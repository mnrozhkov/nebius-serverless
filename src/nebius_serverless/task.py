"""``ns.Task``: run a Python function or a bash script as a Nebius job.

A ``Task`` wraps a function or a script. Calling it starts a job,
waits for it, and returns the result. Constructing a ``Task`` does not
contact Nebius.

Code and results travel through a bucket in the project,
``generated-tasks-<project id>``, which the first run creates. Jobs
mount it; this machine reads and writes it with your S3 credentials.
The bucket is in ``task_storage``, the code that travels in
``task_libs``, and what runs in the job in ``task_runner``.

Besides ``Task`` and ``TaskRun``, the functions at the end of this
module are for code that runs tasks on a user's behalf, such as an
orchestrator integration: they start a run in a given folder, and turn
a task into a JSON record and back.
"""

import hashlib
import inspect
import json
import os
import re
import tempfile
import textwrap
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import task_runner, task_storage
from .errors import Error, TaskFailed, ValidationError
from .image import Image
from .job import Job
from .job_spec import JobSpec
from .output import Progress, call_progress, report
from .profile import resolve_location
from .recipe import INJECT_FILE_MAX_BYTES, validate_image
from .status import Status
from .task_libs import (
    covered,
    function_source,
    pack_libs,
    upload_libs,
    validate_libs,
)
from .task_storage import Bucket, SdkTaskBackend
from .transport import Transport

_LOG = "nebius_serverless.task"


_OP = "Task"


# The console address of a job, with {project} and {job} in it. When
# set, ``console_url`` links to a run's job.
CONSOLE_ENV = "NS_CONSOLE_JOB_URL"


DEFAULT_TIMEOUT = "24h"


# A result can appear in the bucket a moment after the job ends.
RESULT_TRIES = 5


RESULT_PAUSE = 2.0


_INPUT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


_NAME_MAX = 40


class Task:
    """A function or a bash script that runs as a Nebius job.

    Pass a function, or ``command=`` with a bash script. Calling the
    task starts a job, waits for it, and returns the result. ``submit``
    starts the job and returns a ``TaskRun`` at once.

    A function gets its arguments as JSON values and must return a JSON
    value. If it comes from a package listed in ``include_libs``, the
    job imports it by name. Otherwise its source is sent, so it must be
    self-contained: imports go inside the body, and it must not use
    names from the surrounding file. The image needs ``python3``.

    A script reads its inputs from the bash array ``inputs`` and returns
    values by setting keys in the array ``outputs``. The call returns
    ``outputs`` as a dict of strings. The script stops at the first
    failing command. The image needs bash 4 or newer.

    A task can be given the run of another task as an argument. Its job
    then starts when that run has finished, and gets the run's result.

    Both kinds can write files to ``$NS_OUTPUT_DIR``. That folder is in
    the project's task bucket, which every task's job mounts at the same
    path, so a path one task returns can be opened by another.

    Reading results on this machine uses your S3 credentials:
    ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``, or the profile
    in ``~/.aws/credentials``.

    Args:
        function: The function to run.
        command: A bash script to run instead of a function.
        inputs: The names of a script's inputs.
        image: An ``ns.Image``, built on the first run, or an image
            reference.
        include_libs: Import names of local packages or modules to send
            with the task, such as ``"tasks"`` or ``"tasks.training"``.
        name: The task's name. Defaults to the function's name. Required
            for a script. Each run's job is named ``<name>-<UTC time>``.
        **job_options: Options for the job, as on ``JobSpec``: ``gpu``,
            ``cpu``, ``pricing``, ``timeout`` (default 24h), ``disk``,
            ``volumes``, ``env``, ``secrets`` and the rest.

    Example::

        import nebius_serverless as ns

        def double(x):
            return 2 * x

        task = ns.Task(double, image="python:3.12-slim", timeout="10m")
        task(x=21)  # 42

        hello = ns.Task(
            name="hello",
            image="python:3.12-slim",
            inputs=["who"],
            command='outputs[greeting]="hello ${inputs[who]}"',
        )
        hello(who="world")  # {"greeting": "hello world"}
    """

    __signature__: inspect.Signature

    def __init__(
        self,
        function: Callable[..., Any] | None = None,
        *,
        command: str | None = None,
        inputs: Sequence[str] | None = None,
        image: Image | str,
        include_libs: Sequence[str] = (),
        name: str | None = None,
        **job_options: Any,
    ) -> None:
        if (function is None) == (command is None):
            raise ValidationError(
                "Pass exactly one of a function or command.",
                operation=_OP,
            )
        self.image: Image | str = (
            image if isinstance(image, Image) else validate_image(image)
        )
        self.include_libs = validate_libs(include_libs)
        self._options = validate_options(job_options)
        self.function = function
        self.command: str | None = None
        self.inputs: tuple[str, ...] = ()
        self._module: str | None = None
        self._qualname = ""
        self._source: str | None = None
        # Set on a task made from a record: its image is built and its
        # libraries are uploaded already.
        self._fixed = False
        self._libs_key: str | None = None
        if function is not None:
            if inputs is not None:
                raise ValidationError(
                    "inputs is for a command. A function's inputs are "
                    "its arguments.",
                    operation=_OP,
                )
            self._init_function(function)
            default_name = function.__name__
            self.__signature__ = inspect.signature(function)
            self.__doc__ = function.__doc__
        else:
            assert command is not None
            self._init_command(command, inputs)
            if name is None:
                raise ValidationError(
                    "A command task needs a name.", operation=_OP
                )
            default_name = name
            self.__signature__ = _keyword_signature(self.inputs)
        self.name = plain_name(name if name is not None else default_name)
        self.__name__ = self.name
        self._built: tuple[str, str] | None = None
        self._build_lock = threading.Lock()

    def __repr__(self) -> str:
        kind = "command" if self.command is not None else "function"
        return f"Task({self.name!r}, {kind})"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run the task and return its result.

        Starts a job, waits for it, and returns the function's return
        value or the script's ``outputs``. A failure raises
        ``TaskFailed``. Interrupting the wait with Ctrl-C cancels the
        job.
        """
        run = self.submit(*args, **kwargs)
        try:
            return run.result()
        except KeyboardInterrupt:
            run.cancel()
            raise

    def submit(self, *args: Any, **kwargs: Any) -> "TaskRun":
        """Start a job for this task and return its run at once.

        The first run builds the image when it is an ``ns.Image`` that
        has not been built, and uploads ``include_libs`` when they
        changed. It does not wait for the job. Call ``result()`` on the
        run for that.

        An argument may be a run of another task, or a list or dict
        that holds runs. ``submit`` still returns at once. The job
        starts when those runs have finished, with their results in
        their place. If one of them fails, this run is not started and
        its ``result()`` raises ``TaskFailed``. A run that is still
        waiting for its inputs when the script ends is not started.
        """
        return self._start(args, kwargs, folder=None)

    def _start(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        folder: str | None,
    ) -> "TaskRun":
        """``submit``, with the pipeline run's folder when there is one."""
        if _has_runs((args, kwargs)):
            return self._submit_later(args, kwargs, folder=folder)

        def work(progress: Progress) -> "TaskRun":
            return self._submit(args, kwargs, progress, folder=folder)

        return call_progress(quiet=False, logger_name=_LOG, work=work)

    def _submit_later(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        folder: str | None = None,
    ) -> "TaskRun":
        """Return a run now, and start its job when its inputs are done."""
        self._check_names(args, kwargs)
        run = TaskRun._waiting(self.name)

        def start() -> None:
            try:
                ready_args = tuple(_resolved(args))
                ready_kwargs = _resolved(kwargs)
                if run._ready.is_set():
                    return

                def work(progress: Progress) -> "TaskRun":
                    return self._submit(
                        ready_args, ready_kwargs, progress, folder=folder
                    )

                run._started(
                    call_progress(quiet=False, logger_name=_LOG, work=work)
                )
            except BaseException as exc:
                run._not_started(exc)

        # A daemon thread: after Ctrl-C, or when the script ends, a run
        # that is still waiting must not start a job.
        threading.Thread(
            target=start, name=f"ns-task-{self.name}", daemon=True
        ).start()
        return run

    def _init_function(self, function: Callable[..., Any]) -> None:
        if not inspect.isfunction(function):
            raise ValidationError(
                "The task's function must be a plain Python function.",
                operation=_OP,
            )
        if inspect.iscoroutinefunction(function):
            raise ValidationError(
                "An async function cannot be a task.", operation=_OP
            )
        module = function.__module__ or ""
        self._qualname = function.__qualname__
        if covered(module, self.include_libs):
            if "<locals>" in self._qualname:
                raise ValidationError(
                    f"{function.__name__} is defined inside another "
                    "function, so the job cannot import it.",
                    operation=_OP,
                )
            self._module = module
            return
        self._qualname = function.__name__
        self._source = function_source(function)

    def _init_command(
        self, command: object, inputs: Sequence[str] | None
    ) -> None:
        if not isinstance(command, str) or command.strip() == "":
            raise ValidationError(
                "command must be a nonempty bash script.", operation=_OP
            )
        self.command = textwrap.dedent(command).strip("\n")
        if inputs is None:
            return
        if isinstance(inputs, str) or not isinstance(inputs, Sequence):
            raise ValidationError(
                "inputs must be a list of names.", operation=_OP
            )
        names: list[object] = list(inputs)
        for item in names:
            if not isinstance(item, str) or not _INPUT_NAME.fullmatch(item):
                raise ValidationError(
                    f"The input name {item!r} must be letters, digits "
                    "and underscores, not starting with a digit.",
                    operation=_OP,
                )
        if len(set(inputs)) != len(inputs):
            raise ValidationError(
                "inputs must not repeat a name.", operation=_OP
            )
        self.inputs = tuple(inputs)

    def _submit(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
        *,
        run_name: str | None = None,
        job_name: str | None = None,
        key: str | None = None,
        folder: str | None = None,
    ) -> "TaskRun":
        """Start one run. ``run_name``, ``job_name`` and ``key`` replace
        the run's time stamp, the job's ``<task>-<time>`` name, and the
        run's folder in the bucket. ``folder`` is the pipeline run's
        folder that the run's own folder goes in."""
        bound = self._bind(args, kwargs)
        options = dict(self._options)
        project, region = resolve_location(
            profile=options.get("profile"),
            project=options.get("project"),
            region=None,
        )
        backend = task_storage.backend(options.get("profile"))
        bucket, created = backend.ensure_bucket(project)
        if created:
            say(progress, f"Created bucket {bucket.name} ({bucket.id}).")
        image, libs_key = self._prepare(progress, backend, bucket)
        if run_name is None:
            run_name = task_runner.run_name(self.name)
        job_name = job_name or f"{self.name}-{run_name}"
        if key is None:
            key = task_runner.run_key(self.name, run_name, job_name, folder)
        command, files, payload = self._job_files(bound, key, libs_key)
        if payload is not None:
            backend.put(bucket, f"{key}/payload.json", payload)
        volumes = [
            f"{bucket.id}:{task_runner.MOUNT}:rw",
            *options.pop("volumes", ()),
        ]
        with _staged(files) as staged:
            spec = JobSpec(
                command,
                image=image,
                name=job_name,
                volumes=volumes,
                inject_files=[
                    f"{staged / item}:{task_runner.JOB_DIR}/{item}"
                    for item in files
                ],
                **options,
            )
            job = spec._create(
                dry_run=False,
                region=region,
                project=project,
                transport=job_transport(),
                progress=progress,
                idempotency_key=None,
            )
        assert isinstance(job, Job)
        return TaskRun(
            task=self.name,
            name=run_name,
            job=job,
            inputs=bound.inputs,
            bucket=bucket,
            key=key,
            scripted=self.command is not None,
            backend=backend,
            project=project,
        )

    def _prepare(
        self, progress: Progress, backend: SdkTaskBackend, bucket: Bucket
    ) -> tuple[str, str | None]:
        """Build the image and upload the libraries, when they are needed.

        Returns the image reference and the key of the library archive.
        """
        if self._fixed:
            assert isinstance(self.image, str)
            return self.image, self._libs_key
        libs = pack_libs(self.include_libs)
        image = self._image(progress)
        return image, upload_libs(libs, backend, bucket, progress)

    def _record(
        self, progress: Progress, backend: SdkTaskBackend, bucket: Bucket
    ) -> dict[str, Any]:
        """Prepare the task and describe it as JSON.

        The record holds the built image's reference, the key of the
        uploaded libraries, the function's source or import name, and
        the job options: enough to start the task's job where its code
        and Dockerfile are not present. ``_from_record`` reverses it.
        """
        image, libs_key = self._prepare(progress, backend, bucket)
        return {
            "name": self.name,
            "doc": self.__doc__,
            "image": image,
            "libs": libs_key,
            "module": self._module,
            "qualname": self._qualname,
            "source": self._source,
            "command": self.command,
            "inputs": list(self.inputs),
            "parameters": _describe_signature(self.__signature__),
            "options": plain_options(self._options),
        }

    @classmethod
    def _from_record(cls, record: Mapping[str, Any]) -> "Task":
        """A task made from a record. It needs no local code to submit."""
        task = cls.__new__(cls)
        task.image = str(record["image"])
        task.include_libs = ()
        task._options = dict(record["options"])
        task.function = None
        task.command = record.get("command")
        task.inputs = tuple(record.get("inputs") or ())
        task._module = record.get("module")
        task._qualname = str(record.get("qualname") or "")
        task._source = record.get("source")
        task._fixed = True
        task._libs_key = record.get("libs")
        task.name = str(record["name"])
        task.__name__ = task.name
        task.__doc__ = record.get("doc")
        task.__signature__ = _signature_from(record.get("parameters"))
        task._built = None
        task._build_lock = threading.Lock()
        return task

    def _check_names(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        """Check that the arguments fit, whatever their values are."""
        if self.command is not None:
            if args:
                raise ValidationError(
                    f"Pass {self.name}'s inputs by name.", operation=_OP
                )
            missing = [item for item in self.inputs if item not in kwargs]
            extra = [item for item in kwargs if item not in self.inputs]
            if missing or extra:
                problems = [
                    f"{label}: {', '.join(names)}"
                    for label, names in (
                        ("missing", missing),
                        ("unknown", extra),
                    )
                    if names
                ]
                raise ValidationError(
                    f"{self.name} takes the inputs "
                    f"{', '.join(self.inputs) or '(none)'}. "
                    f"{'; '.join(problems).capitalize()}.",
                    operation=_OP,
                )
            return
        try:
            self.__signature__.bind(*args, **kwargs)
        except TypeError as exc:
            raise ValidationError(
                f"{self.name}: {exc}.", operation=_OP
            ) from None

    def _bind(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> "_Bound":
        self._check_names(args, kwargs)
        if self.command is not None:
            return _Bound((), {}, {item: kwargs[item] for item in self.inputs})
        bound = self.__signature__.bind(*args, **kwargs)
        try:
            json.dumps([args, kwargs], allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"{self.name}'s arguments must be JSON values: {exc}.",
                operation=_OP,
            ) from None
        return _Bound(list(args), dict(kwargs), dict(bound.arguments))

    def _image(self, progress: Progress) -> str:
        if isinstance(self.image, str):
            return self.image
        with self._build_lock:
            tag = self.image.tag
            if self._built is not None and self._built[0] == tag:
                return self._built[1]
            ref = build_image(self.image, progress)
            self._built = (tag, ref)
            return ref

    def _job_files(
        self, bound: "_Bound", key: str, libs_key: str | None
    ) -> tuple[list[str], dict[str, bytes], bytes | None]:
        """The job's command, its injected files, and its payload.

        A function's payload goes to the bucket, so its size is not
        limited. The job is given a pointer to it with its checksum,
        and refuses a payload that does not match.
        """
        if self.command is not None:
            script = task_runner.bash_script(
                self.command, bound.inputs, key, libs_key
            )
            if len(script.encode()) > INJECT_FILE_MAX_BYTES:
                raise ValidationError(
                    f"{self.name}'s script and inputs are too large to "
                    "send (over 64 KiB). Pass large data as files.",
                    operation=_OP,
                )
            return (
                ["bash", f"{task_runner.JOB_DIR}/run.sh"],
                {"run.sh": script.encode()},
                None,
            )
        payload = json.dumps(
            {
                "libs": libs_key,
                "libs_dir": task_runner.LIBS_DIR,
                "module": self._module,
                "qualname": self._qualname,
                "source": self._source,
                "args": bound.args,
                "kwargs": bound.kwargs,
            }
        ).encode()
        pointer = {
            "root": task_runner.MOUNT,
            "run": key,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        files = {
            "runner.py": task_runner.RUNNER.encode(),
            "pointer.json": json.dumps(pointer).encode(),
        }
        return ["python3", f"{task_runner.JOB_DIR}/runner.py"], files, payload


class TaskRun:
    """One run of a task: its job, its inputs, and where its files are.

    ``result()`` waits for the job and returns the task's result.
    ``name`` is the UTC time the task was called, such as
    ``20261124-153922``. ``output_path`` is the run's file folder as jobs
    see it, and ``output_uri`` is the same folder as an ``s3://``
    address.

    A run whose task was given other runs as arguments has no job until
    those runs have finished. ``job``, ``name``, ``inputs``,
    ``output_path`` and ``output_uri`` then wait for the job to exist,
    and ``started`` tells whether it does.
    """

    _FIELDS = (
        "_name",
        "_job",
        "_inputs",
        "_bucket",
        "_key",
        "_scripted",
        "_backend",
        "_project",
        "_output_path",
        "_output_uri",
    )
    _name: str
    _job: Job
    _inputs: dict[str, Any]
    _bucket: Bucket
    _key: str
    _scripted: bool
    _backend: SdkTaskBackend
    _project: str
    _output_path: str
    _output_uri: str

    def __init__(
        self,
        *,
        task: str,
        name: str,
        job: Job,
        inputs: dict[str, Any],
        bucket: Bucket,
        key: str,
        scripted: bool,
        backend: SdkTaskBackend,
        project: str,
    ) -> None:
        self._begin(task)
        self._name = name
        self._job = job
        self._inputs = inputs
        self._output_path = f"{task_runner.MOUNT}/{key}/files"
        self._output_uri = f"s3://{bucket.name}/{key}/files/"
        self._bucket = bucket
        self._key = key
        self._scripted = scripted
        self._backend = backend
        self._project = project
        self._ready.set()

    def _begin(self, task: str) -> None:
        self.task = task
        # Set once the job exists, or once it is known that it never will.
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._lock = threading.Lock()
        self._outcome: tuple[bool, Any] | None = None

    @classmethod
    def _waiting(cls, task: str) -> "TaskRun":
        """A run that has no job yet, because its inputs are not done."""
        run = cls.__new__(cls)
        run._begin(task)
        return run

    def _started(self, real: "TaskRun") -> None:
        """Become the run of the job that was started for it."""
        if self._ready.is_set():
            return
        for field in self._FIELDS:
            setattr(self, field, getattr(real, field))
        self._ready.set()

    def _not_started(self, error: BaseException) -> None:
        """Record why no job was started."""
        if self._ready.is_set():
            return
        self._error = error
        self._ready.set()

    def _settled(self) -> "TaskRun":
        """Wait until the job exists. Raise when it was never started."""
        self._ready.wait()
        error = self._error
        if isinstance(error, TaskFailed):
            reason = str(error).splitlines()[0]
            raise TaskFailed(
                f"Task {self.task} was not started, because one of its "
                f"inputs failed: {reason}",
                operation="task",
                state=error.state,
            ) from error
        if error is not None:
            raise error
        return self

    def cancel(self) -> None:
        """Request cancellation of the run's job. Never raises.

        A run still waiting for its inputs is dropped and never starts.
        """
        if not self._ready.is_set():
            self._not_started(
                TaskFailed(
                    f"Task {self.task} was dropped before it started.",
                    operation="task",
                )
            )
            return
        if self._error is not None:
            return
        try:
            self._job.cancel(quiet=True)
        except Error:
            pass

    @property
    def project(self) -> str:
        """The project the run's job is in."""
        return self._settled()._project

    @property
    def started(self) -> bool:
        """Whether the run has a job."""
        return self._ready.is_set() and self._error is None

    @property
    def name(self) -> str:
        """The UTC time the job was started, such as ``20261124-153922``."""
        return self._settled()._name

    @property
    def job(self) -> Job:
        """The run's job. Waits for it when the run has not started."""
        return self._settled()._job

    @property
    def inputs(self) -> dict[str, Any]:
        """The inputs by name, with other runs replaced by their results."""
        return self._settled()._inputs

    @inputs.setter
    def inputs(self, value: dict[str, Any]) -> None:
        self._inputs = value

    @property
    def output_path(self) -> str:
        """The run's file folder as jobs see it."""
        return self._settled()._output_path

    @property
    def output_uri(self) -> str:
        """The run's file folder as an ``s3://`` address."""
        return self._settled()._output_uri

    def __repr__(self) -> str:
        if not self._ready.is_set():
            return f"TaskRun({self.task!r}, waiting for its inputs)"
        if self._error is not None:
            return f"TaskRun({self.task!r}, not started)"
        return f"TaskRun({self.task!r}, {self._name!r}, job={self._job.id!r})"

    def result(self) -> Any:
        """Wait for the job, then return the task's result.

        Returns the function's return value, or the script's ``outputs``
        as a dict of strings. Raises ``TaskFailed`` when the function
        raised, the script exited with an error, the job ended without
        a result, or the run was not started because one of its inputs
        failed. Later calls return or raise the same outcome without
        waiting again.
        """
        self._settled()
        with self._lock:
            if self._outcome is None:

                def work(progress: Progress) -> tuple[bool, Any]:
                    return self._collect(progress)

                self._outcome = call_progress(
                    quiet=False, logger_name=_LOG, work=work
                )
            ok, value = self._outcome
        if ok:
            return value
        raise value

    def _collect(self, progress: Progress) -> tuple[bool, Any]:
        record = self.job._wait(None, progress)
        state = _state(record)
        item = "outputs.bin" if self._scripted else "result.json"
        data = self._read(item, tries=RESULT_TRIES if _done(state) else 1)
        if data is None:
            return False, self._failed(
                f"ended {state} without a result", state
            )
        if self._scripted:
            return self._script_outcome(data, state)
        return self._function_outcome(data, state)

    def _read(self, item: str, *, tries: int) -> bytes | None:
        for attempt in range(tries):
            data = self._backend.get(self._bucket, f"{self._key}/{item}")
            if data is not None:
                return data
            if attempt + 1 < tries:
                time.sleep(RESULT_PAUSE)
        return None

    def _function_outcome(self, data: bytes, state: str) -> tuple[bool, Any]:
        try:
            body = json.loads(data)
            ok = body["ok"]
        except (ValueError, KeyError, TypeError):
            return False, self._failed("wrote an unreadable result", state)
        if ok and _done(state):
            return True, body.get("value")
        if ok:
            # A result counts only from a completed job: one that ended
            # otherwise may have been cut short after writing it.
            return False, self._failed(
                f"returned a value, but its job ended {state}", state
            )
        error = body.get("error") or {}
        remote = str(error.get("traceback") or "")
        message = (
            f"raised {error.get('type')}: {error.get('message')}"
            f"\n\nTraceback from the job:\n{remote}"
        )
        return False, self._failed(message, state, remote_traceback=remote)

    def _script_outcome(self, data: bytes, state: str) -> tuple[bool, Any]:
        parts = data.split(b"\0")
        try:
            code = int(parts[0])
        except ValueError:
            return False, self._failed("wrote unreadable outputs", state)
        values = [part.decode(errors="replace") for part in parts[1:-1]]
        outputs = dict(zip(values[0::2], values[1::2]))
        if code == 0 and _done(state):
            return True, outputs
        return False, self._failed(
            f"exited with code {code}",
            state,
            exit_code=code,
            outputs=outputs,
        )

    def _failed(self, what: str, state: str, **extra: Any) -> TaskFailed:
        return TaskFailed(
            f"Task {self.task} run {self.name} (job {self.job.id}) {what}.",
            operation="task",
            project=self._project,
            resource_id=self.job.id,
            state=state,
            recovery=f"See the log: nebius ai job logs {self.job.id}",
            **extra,
        )


@dataclass(frozen=True)
class _Bound:
    """One call's inputs: as sent to the job, and by name."""

    args: Sequence[Any]
    kwargs: Mapping[str, Any]
    inputs: dict[str, Any]


def _has_runs(value: object) -> bool:
    """Whether a run is among the arguments, at any depth."""
    if isinstance(value, TaskRun):
        return True
    if isinstance(value, (list, tuple)):
        return any(_has_runs(item) for item in value)
    if isinstance(value, dict):
        return any(_has_runs(item) for item in value.values())
    return False


def _resolved(value: Any) -> Any:
    """The value with each run replaced by its result. Waits for them.

    A failed run raises here, which is how a task given that run learns
    that it cannot start.
    """
    if isinstance(value, TaskRun):
        return value.result()
    if isinstance(value, (list, tuple)):
        return [_resolved(item) for item in value]
    if isinstance(value, dict):
        return {key: _resolved(item) for key, item in value.items()}
    return value


def job_transport() -> Transport | None:
    """The transport for job calls. ``None`` is the SDK. Tests replace it."""
    return None


def say(progress: Progress, message: str) -> None:
    report(Status(kind="task", message=message), progress)


def _state(record: dict[str, object]) -> str:
    status = record.get("status")
    state = status.get("state") if isinstance(status, dict) else None
    return state if isinstance(state, str) else "UNKNOWN"


def _done(state: str) -> bool:
    return state == "COMPLETED"


def _keyword_signature(names: Sequence[str]) -> inspect.Signature:
    """A signature that takes each name as a keyword argument."""
    kind = inspect.Parameter.KEYWORD_ONLY
    return inspect.Signature([inspect.Parameter(name, kind) for name in names])


def console_url(project: str, job_id: str) -> str | None:
    """The web console address of a job, when a template is set.

    The template is read from ``NS_CONSOLE_JOB_URL``. It may contain
    ``{project}`` and ``{job}``.
    """
    template = os.environ.get(CONSOLE_ENV)
    if not template:
        return None
    return template.replace("{project}", project).replace("{job}", job_id)


def _describe_signature(signature: inspect.Signature) -> list[dict[str, Any]]:
    """A signature as JSON: each parameter's name, kind, and need."""
    return [
        {
            "name": parameter.name,
            "kind": parameter.kind.name,
            "required": parameter.default is inspect.Parameter.empty,
        }
        for parameter in signature.parameters.values()
    ]


def _signature_from(parameters: object) -> inspect.Signature:
    """The signature a record describes, without the default values.

    A parameter that had a default shows ``...``. The real default is
    applied by the function in the job. No description means any
    arguments are taken.
    """
    if not isinstance(parameters, list):
        return inspect.Signature(
            [
                inspect.Parameter("args", inspect.Parameter.VAR_POSITIONAL),
                inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD),
            ]
        )
    built = []
    for item in parameters:
        kind = getattr(inspect.Parameter, item["kind"])
        plain = kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
        default = inspect.Parameter.empty if item["required"] or plain else ...
        built.append(inspect.Parameter(item["name"], kind, default=default))
    return inspect.Signature(built)


def plain_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Job options as JSON values, for a record."""
    plain = dict(options)
    timeout = plain.get("timeout")
    if isinstance(timeout, timedelta):
        plain["timeout"] = timeout.total_seconds()
    try:
        json.dumps(plain, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"The job options cannot be sent with a flow: {exc}.",
            operation=_OP,
        ) from None
    return plain


def plain_name(text: object) -> str:
    """Lowercase letters, digits and hyphens, at most 40 characters."""
    if not isinstance(text, str):
        raise ValidationError("name must be text.", operation=_OP)
    plain = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    plain = plain[:_NAME_MAX].strip("-")
    if plain == "":
        raise ValidationError(
            f"The name {text!r} has no letters or digits.", operation=_OP
        )
    return plain


def validate_options(options: dict[str, Any]) -> dict[str, Any]:
    """Check the job options now, so a mistake does not wait for a run."""
    if "inject_files" in options:
        raise ValidationError(
            "inject_files is not available on a task. Use include_libs "
            "for code, and volumes for data.",
            operation=_OP,
        )
    checked = {"timeout": DEFAULT_TIMEOUT, **options}
    try:
        JobSpec(["true"], image="python:3", **checked)
    except TypeError as exc:
        raise ValidationError(
            f"Unknown job option: {exc}.", operation=_OP
        ) from None
    return checked


@contextmanager
def _staged(files: Mapping[str, bytes]) -> Iterator[Path]:
    """The generated files on disk until the create has read them."""
    with tempfile.TemporaryDirectory(prefix="ns-task-") as folder:
        root = Path(folder)
        for name, content in files.items():
            (root / name).write_bytes(content)
        yield root


def build_image(image: Image | str, progress: Progress) -> str:
    """The reference of ``image``: built first when it is an ``Image``."""
    if isinstance(image, str):
        return image
    ref = image._build(
        progress,
        dry_run=False,
        timeout="1h",
        disk=None,
        transport=job_transport(),
    )
    assert isinstance(ref, str)
    return ref


def start_run(
    task: Task,
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    *,
    folder: str | None = None,
) -> TaskRun:
    """``task.submit(*args, **kwargs)``, with the run kept in ``folder``.

    ``folder`` is a pipeline run's folder in the bucket. The run's own
    folder is ``<folder>/<job name>``. Without it, ``NS_TASK_FOLDER``
    is used when set, and ``tasks/<task>/<UTC time>`` otherwise.
    """
    return task._start(tuple(args), dict(kwargs or {}), folder=folder)


def submit_run(
    task: Task,
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
    progress: Progress,
    *,
    run_name: str | None = None,
    job_name: str | None = None,
    key: str | None = None,
) -> TaskRun:
    """Start one run with its names given: its time stamp, its job's
    name, and its folder in the bucket. The arguments must not hold
    other runs."""
    return task._submit(
        tuple(args),
        dict(kwargs),
        progress,
        run_name=run_name,
        job_name=job_name,
        key=key,
    )


def prepare_task(
    task: Task, progress: Progress, storage: SdkTaskBackend, bucket: Bucket
) -> None:
    """Build the task's image and upload its libraries, when needed."""
    task._prepare(progress, storage, bucket)


def task_record(
    task: Task, progress: Progress, storage: SdkTaskBackend, bucket: Bucket
) -> dict[str, Any]:
    """Prepare the task and describe it as JSON.

    The record holds the built image's reference, the key of the
    uploaded libraries, the function's source or import name, and the
    job options: enough to start the task's job where its code and
    Dockerfile are not present. ``task_from_record`` reverses it.
    """
    return task._record(progress, storage, bucket)


def task_from_record(record: Mapping[str, Any]) -> Task:
    """A task made from a record. It needs no local code to submit."""
    return Task._from_record(record)
