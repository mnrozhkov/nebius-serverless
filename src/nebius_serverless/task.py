"""``ns.Task``: run a Python function or a bash script as a Nebius job.

Prototype. A ``Task`` wraps a function or a script. Calling it starts a
job, waits for it, and returns the result. Constructing a ``Task`` does
not contact Nebius.

Code and results travel through a bucket in the project,
``generated-tasks-<project id>``, which the first run creates. Jobs
mount it; this machine reads and writes it with your S3 credentials.
"""

import ast
import builtins
import configparser
import dis
import gzip
import hashlib
import hmac
import importlib.util
import inspect
import io
import json
import os
import re
import shlex
import sysconfig
import tarfile
import tempfile
import textwrap
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import CodeType
from typing import Any

from .errors import (
    AccessError,
    Error,
    TaskFailed,
    TransportError,
    ValidationError,
)
from .image import Image
from .job import Job
from .job_spec import JobSpec
from .output import Progress, call_progress, report
from .profile import resolve_location
from .recipe import INJECT_FILE_MAX_BYTES, validate_image
from .status import Status
from .transport import Transport

_LOG = "nebius_serverless.task"
_OP = "Task"

BUCKET_PREFIX = "generated-tasks-"
BUCKET_LABELS = {"managed-by": "nebius-serverless"}
# Where every task's job sees the bucket. It is the same in all jobs, so
# a path one task returns can be opened by another.
MOUNT = "/ns"
# Where the generated runner and its inputs are injected.
JOB_DIR = "/ns-task"
# Where include_libs are unpacked in the job.
LIBS_DIR = "/tmp/ns-libs"
# Set in a task's job to where the bucket is mounted there. A task
# started from inside that job reads and writes the bucket as files.
MOUNT_ENV = "NS_TASK_MOUNT"
# Set in a flow job to the folder of the pipeline's run in the bucket,
# <pipeline>/<run>. A task started there keeps its files under it.
FOLDER_ENV = "NS_TASK_FOLDER"
# The folder of the flow job itself, next to the folders of its tasks.
FLOW_FOLDER = "flow"
# The console address of a job, with {project} and {job} in it. When
# set, a Prefect task run links to its job.
CONSOLE_ENV = "NS_CONSOLE_JOB_URL"
# The names of the artifacts a Prefect task run gets.
JOB_ARTIFACT = "nebius-job"
INPUTS_ARTIFACT = "inputs"
OUTPUTS_ARTIFACT = "outputs"
# How much of one input or output value is shown in Prefect.
_SHOWN_MAX = 500
DEFAULT_TIMEOUT = "24h"
# A result can appear in the bucket a moment after the job ends.
RESULT_TRIES = 5
RESULT_PAUSE = 2.0
_S3_TIMEOUT = 60.0

_INPUT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LIB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
_NAME_MAX = 40

# Runs a function in the job. Standard library only, so the image needs
# nothing but python3.
RUNNER = '''\
"""Run one ns.Task function inside the job."""

import hashlib
import importlib
import io
import json
import os
import sys
import tarfile
import traceback


def load(payload, root):
    if payload["libs"]:
        folder = payload["libs_dir"]
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(root, payload["libs"]), "rb") as handle:
            data = handle.read()
        expected = os.path.basename(payload["libs"]).split(".")[0]
        if hashlib.sha256(data).hexdigest()[: len(expected)] != expected:
            raise RuntimeError("The library archive is not what was sent.")
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            try:
                archive.extractall(folder, filter="data")
            except TypeError:
                archive.extractall(folder)
        sys.path.insert(0, folder)
        previous = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = (
            folder + os.pathsep + previous if previous else folder
        )
    if payload["module"]:
        target = importlib.import_module(payload["module"])
        for part in payload["qualname"].split("."):
            target = getattr(target, part)
        return target
    scope = {"__name__": "__ns_task__"}
    exec(compile(payload["source"], "<ns.Task source>", "exec"), scope)
    return scope[payload["qualname"]]


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "pointer.json")) as handle:
        pointer = json.load(handle)
    root = pointer["root"]
    run = os.path.join(root, pointer["run"])
    files = os.path.join(run, "files")
    os.makedirs(files, exist_ok=True)
    os.environ["NS_OUTPUT_DIR"] = files
    os.environ["NS_TASK_MOUNT"] = root
    code = 0
    try:
        with open(os.path.join(run, "payload.json"), "rb") as handle:
            raw = handle.read()
        if hashlib.sha256(raw).hexdigest() != pointer["sha256"]:
            raise RuntimeError("payload.json is not what was submitted.")
        payload = json.loads(raw)
        function = load(payload, root)
        value = function(*payload["args"], **payload["kwargs"])
        body = json.dumps({"ok": True, "value": value})
    except BaseException as exc:
        code = 1
        traceback.print_exc()
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        body = json.dumps({"ok": False, "error": error})
    with open(os.path.join(run, "result.json"), "w") as handle:
        handle.write(body)
    sys.exit(code)


if __name__ == "__main__":
    main()
'''

_lock = threading.Lock()
_buckets: dict[tuple[int, str, str], "Bucket"] = {}
_stamps: dict[str, tuple[str, int]] = {}


@dataclass(frozen=True)
class Bucket:
    """The project's task bucket: what jobs mount and the client reads."""

    id: str
    name: str
    endpoint: str
    region: str


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
        self.include_libs = _validate_libs(include_libs)
        self._options = _validate_options(job_options)
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
        self.name = _plain_name(name if name is not None else default_name)
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
            run._cancel()
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
        if _has_runs((args, kwargs)):
            return self._submit_later(args, kwargs)

        def work(progress: Progress) -> "TaskRun":
            return self._submit(args, kwargs, progress)

        return call_progress(quiet=False, logger_name=_LOG, work=work)

    def _submit_later(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
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
                    return self._submit(ready_args, ready_kwargs, progress)

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

    def to_prefect(self, **options: Any) -> Any:
        """Return this task as a Prefect task.

        Calling the Prefect task in a flow runs this task's job and
        returns its result. A failure raises ``TaskFailed``, so Prefect
        marks the task run failed and its retries apply. Each retry is
        a new run with a new job. ``submit`` and ``map`` on the returned
        object are Prefect's own and run the jobs at the same time.

        Each task run gets three Markdown artifacts in Prefect:
        ``nebius-job`` (the job id, its name, the command for its logs,
        where its files are) and ``inputs`` when the job has started,
        and ``outputs`` when it has ended. The same goes to the task
        run's log. With ``NS_CONSOLE_JOB_URL`` set to the console
        address of a job, with ``{project}`` and ``{job}`` in it,
        ``nebius-job`` links to the job.

        ``options`` go to ``prefect.task``, for example ``retries=2``.
        The Prefect task is named after this task unless ``name`` is
        given. Needs the ``prefect`` package.
        """
        try:
            from prefect import (  # type: ignore[import-not-found]
                task,
            )
        except ImportError:
            raise ValidationError(
                "to_prefect needs the prefect package.",
                operation=_OP,
                recovery="Install it: pip install prefect",
            ) from None

        def run(*args: Any, **kwargs: Any) -> Any:
            started = self.submit(*args, **kwargs)
            _show_started(started)
            try:
                value = started.result()
            except KeyboardInterrupt:
                started._cancel()
                raise
            _show_finished(started, value)
            return value

        run.__name__ = run.__qualname__ = self.name
        run.__doc__ = self.__doc__
        setattr(run, "__signature__", self.__signature__)
        options.setdefault("name", self.name)
        return task(**options)(run)

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
        if _covered(module, self.include_libs):
            if "<locals>" in self._qualname:
                raise ValidationError(
                    f"{function.__name__} is defined inside another "
                    "function, so the job cannot import it.",
                    operation=_OP,
                )
            self._module = module
            return
        self._qualname = function.__name__
        self._source = _function_source(function)

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
    ) -> "TaskRun":
        """Start one run. ``run_name``, ``job_name`` and ``key`` replace
        the run's time stamp, the job's ``<task>-<time>`` name, and the
        run's folder in the bucket."""
        bound = self._bind(args, kwargs)
        options = dict(self._options)
        project, region = resolve_location(
            profile=options.get("profile"),
            project=options.get("project"),
            region=None,
        )
        backend = _backend(options.get("profile"))
        bucket, created = backend.ensure_bucket(project)
        if created:
            _say(progress, f"Created bucket {bucket.name} ({bucket.id}).")
        image, libs_key = self._prepare(progress, backend, bucket)
        if run_name is None:
            run_name = _run_name(self.name)
        job_name = job_name or f"{self.name}-{run_name}"
        if key is None:
            key = _run_key(self.name, run_name, job_name)
        command, files, payload = self._job_files(bound, key, libs_key)
        if payload is not None:
            backend.put(bucket, f"{key}/payload.json", payload)
        volumes = [f"{bucket.id}:{MOUNT}:rw", *options.pop("volumes", ())]
        with _staged(files) as folder:
            spec = JobSpec(
                command,
                image=image,
                name=job_name,
                volumes=volumes,
                inject_files=[
                    f"{folder / item}:{JOB_DIR}/{item}" for item in files
                ],
                **options,
            )
            job = spec._create(
                dry_run=False,
                region=region,
                project=project,
                transport=_transport(),
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
        self, progress: Progress, backend: "SdkTaskBackend", bucket: Bucket
    ) -> tuple[str, str | None]:
        """Build the image and upload the libraries, when they are needed.

        Returns the image reference and the key of the library archive.
        """
        if self._fixed:
            assert isinstance(self.image, str)
            return self.image, self._libs_key
        libs = _pack_libs(self.include_libs)
        image = self._image(progress)
        return image, _upload_libs(libs, backend, bucket, progress)

    def _record(
        self, progress: Progress, backend: "SdkTaskBackend", bucket: Bucket
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
            "options": _plain_options(self._options),
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
            ref = self.image._build(
                progress,
                dry_run=False,
                timeout="1h",
                disk=None,
                transport=_transport(),
            )
            assert isinstance(ref, str)
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
            script = _script(self.command, bound.inputs, key, libs_key)
            if len(script.encode()) > INJECT_FILE_MAX_BYTES:
                raise ValidationError(
                    f"{self.name}'s script and inputs are too large to "
                    "send (over 64 KiB). Pass large data as files.",
                    operation=_OP,
                )
            return (
                ["bash", f"{JOB_DIR}/run.sh"],
                {"run.sh": script.encode()},
                None,
            )
        payload = json.dumps(
            {
                "libs": libs_key,
                "libs_dir": LIBS_DIR,
                "module": self._module,
                "qualname": self._qualname,
                "source": self._source,
                "args": bound.args,
                "kwargs": bound.kwargs,
            }
        ).encode()
        pointer = {
            "root": MOUNT,
            "run": key,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        files = {
            "runner.py": RUNNER.encode(),
            "pointer.json": json.dumps(pointer).encode(),
        }
        return ["python3", f"{JOB_DIR}/runner.py"], files, payload


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
    _backend: "SdkTaskBackend"
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
        backend: "SdkTaskBackend",
        project: str,
    ) -> None:
        self._begin(task)
        self._name = name
        self._job = job
        self._inputs = inputs
        self._output_path = f"{MOUNT}/{key}/files"
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

    def _cancel(self) -> None:
        """Cancel the job, or drop the run if it has none. Never raises."""
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
        if ok:
            return True, body.get("value")
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


@dataclass(frozen=True)
class _Libs:
    """The include_libs archive, its content hash, and its file count."""

    data: bytes
    digest: str
    count: int


class SdkTaskBackend:
    """The task bucket: found or created with the SDK, read with S3."""

    def __init__(self, profile: str | None) -> None:
        self._profile = profile

    def ensure_bucket(self, project: str) -> tuple[Bucket, bool]:
        """Find the task bucket or create it. ``True`` when created."""
        cache_key = (os.getpid(), self._profile or "", project)
        with _lock:
            found = _buckets.get(cache_key)
        if found is not None:
            return found, False
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.storage.v1 import (
            BucketServiceClient,
            BucketSpec,
            CreateBucketRequest,
            GetBucketByNameRequest,
        )

        from .session import get_sdk

        name = bucket_name(project)
        sdk: Any = get_sdk(self._profile)
        client = BucketServiceClient(sdk)
        lookup = GetBucketByNameRequest(parent_id=project, name=name)
        created = False
        try:
            try:
                record = client.get_by_name(lookup).wait()
            except RequestError as exc:
                if _code_of(exc) != "NOT_FOUND":
                    raise
                request = CreateBucketRequest(
                    metadata=ResourceMetadata(
                        parent_id=project, name=name, labels=BUCKET_LABELS
                    ),
                    spec=BucketSpec(),
                )
                try:
                    client.create(request).wait().sync_wait()
                    created = True
                except RequestError as again:
                    # Another run created it first: use that one.
                    if _code_of(again) != "ALREADY_EXISTS":
                        raise
                record = client.get_by_name(lookup).wait()
        except RequestError as exc:
            _raise_bucket(exc, name, project)
        bucket = Bucket(
            id=str(record.metadata.id),
            name=name,
            endpoint=f"https://{record.status.domain_name}",
            region=str(record.status.region),
        )
        with _lock:
            _buckets[cache_key] = bucket
        return bucket, created

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        """Write one object."""
        _s3(bucket, "PUT", key, data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        return _s3(bucket, "GET", key)

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return _s3(bucket, "HEAD", key) is not None


class MountedTaskBackend(SdkTaskBackend):
    """The task bucket where it is already mounted, read as files.

    Used inside a task's job, when that task starts other tasks. The
    job needs no S3 credentials. The bucket is still found with the
    SDK, because a new job mounts it by id.
    """

    def __init__(self, profile: str | None, root: Path) -> None:
        super().__init__(profile)
        self._root = root

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        """Write one object as a file."""
        path = self._root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        try:
            return (self._root / key).read_bytes()
        except (FileNotFoundError, NotADirectoryError):
            return None

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return (self._root / key).is_file()


def _run_key(task: str, run_name: str, job_name: str) -> str:
    """The folder of one run in the bucket.

    In a pipeline: ``<pipeline>/<run>/<task>``, where ``<task>`` is the
    name of the task's job, such as ``train-baseline-20261125-121050``.
    The pipeline's flow job sets where that is. A task run on its own
    has no pipeline: ``tasks/<task name>/<UTC time>``.
    """
    folder = os.environ.get(FOLDER_ENV, "").strip("/")
    if folder:
        return f"{folder}/{job_name}"
    return f"tasks/{task}/{run_name}"


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


def bucket_name(project: str) -> str:
    """The task bucket's name: a fixed prefix plus the project id."""
    return BUCKET_PREFIX + project.removeprefix("project-")


def forget_buckets() -> None:
    """Drop cached bucket lookups (tests, or after a manual delete)."""
    with _lock:
        _buckets.clear()


def _backend(profile: str | None) -> SdkTaskBackend:
    """S3 from outside a job; the mounted folder from inside one."""
    root = os.environ.get(MOUNT_ENV)
    if root and Path(root).is_dir():
        return MountedTaskBackend(profile, Path(root))
    return SdkTaskBackend(profile)


def _transport() -> Transport | None:
    """The transport for job calls. ``None`` is the SDK. Tests replace it."""
    return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _say(progress: Progress, message: str) -> None:
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


def _shown(value: object) -> str:
    """One value as inline code for Markdown, cut when it is long.

    A string is shown as it is; any other value as JSON.
    """
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, default=str)
        except (TypeError, ValueError):
            text = repr(value)
    text = " ".join(text.split())
    if len(text) > _SHOWN_MAX:
        text = text[:_SHOWN_MAX] + " ... (cut)"
    if text == "":
        return "(empty)"
    fence = "``" if "`" in text else "`"
    pad = " " if "`" in text else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def _values_markdown(title: str, value: object) -> str:
    """Inputs or outputs as a Markdown list, one line per name.

    A dict gives one line per key. Any other value gives one line.
    """
    if isinstance(value, dict):
        lines = [f"- {key}: {_shown(item)}" for key, item in value.items()]
        if not lines:
            lines = ["- (none)"]
    else:
        lines = [f"- Value: {_shown(value)}"]
    return "\n".join([f"### {title}", "", *lines])


def _job_markdown(run: "TaskRun") -> str:
    """A run's Nebius job, for the Prefect artifact ``nebius-job``."""
    job = run.job
    url = console_url(run._project, job.id)
    opened = f" ([open in the console]({url}))" if url else ""
    return "\n".join(
        [
            f"### Nebius job of `{run.task}`",
            "",
            f"- Job: `{job.id}`{opened}",
            f"- Name: `{run.task}-{run.name}`",
            f"- Logs: `nebius ai job logs {job.id}`",
            f"- Files: `{run.output_uri}`",
        ]
    )


def _show_started(run: "TaskRun") -> None:
    """Show a run's job and inputs in the Prefect task run.

    Writes the Markdown artifacts ``nebius-job`` and ``inputs``, and
    the same to the log. Does nothing outside a Prefect run, and never
    fails the task.
    """
    try:
        from prefect import (  # type: ignore[import-not-found]
            get_run_logger,
        )
        from prefect.artifacts import (  # type: ignore[import-not-found]
            create_markdown_artifact,
        )

        logger = get_run_logger()
        url = console_url(run._project, run.job.id)
        where = f" {url}" if url else ""
        logger.info("Nebius job %s%s", run.job.id, where)
        logger.info("Inputs: %s", json.dumps(run.inputs, default=str))
        create_markdown_artifact(
            markdown=_job_markdown(run),
            key=JOB_ARTIFACT,
            description=f"Nebius job {run.job.id} of {run.task}",
        )
        create_markdown_artifact(
            markdown=_values_markdown(f"Inputs of `{run.task}`", run.inputs),
            key=INPUTS_ARTIFACT,
            description=f"Inputs of {run.task}",
        )
    except Exception:
        pass


def _show_finished(run: "TaskRun", value: object) -> None:
    """Show a run's outputs in the Prefect task run. Never fails.

    Writes the Markdown artifact ``outputs``, and the same to the log.
    """
    try:
        from prefect import (  # type: ignore[import-not-found]
            get_run_logger,
        )
        from prefect.artifacts import (  # type: ignore[import-not-found]
            create_markdown_artifact,
        )

        get_run_logger().info("Outputs: %s", json.dumps(value, default=str))
        create_markdown_artifact(
            markdown=_values_markdown(f"Outputs of `{run.task}`", value),
            key=OUTPUTS_ARTIFACT,
            description=f"Outputs of {run.task}",
        )
    except Exception:
        pass


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


def _plain_options(options: Mapping[str, Any]) -> dict[str, Any]:
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


def _upload_libs(
    libs: "_Libs | None",
    backend: "SdkTaskBackend",
    bucket: Bucket,
    progress: Progress,
    what: str = "include_libs",
) -> str | None:
    """Upload an archive unless the bucket has it. Returns its key."""
    if libs is None:
        return None
    key = f"libs/{libs.digest}.tar.gz"
    if not backend.exists(bucket, key):
        backend.put(bucket, key, libs.data)
        _say(progress, f"Uploaded {what} ({libs.count} files).")
    return key


def _plain_name(text: object) -> str:
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


def _run_name(task: str) -> str:
    """The UTC time to the second, with a counter for a repeat."""
    stamp = _now().strftime("%Y%m%d-%H%M%S")
    with _lock:
        last, count = _stamps.get(task, ("", 0))
        count = count + 1 if last == stamp else 1
        _stamps[task] = (stamp, count)
    return stamp if count == 1 else f"{stamp}-{count}"


def _validate_options(options: dict[str, Any]) -> dict[str, Any]:
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


def _validate_libs(names: object) -> tuple[str, ...]:
    if isinstance(names, str) or not isinstance(names, Sequence):
        raise ValidationError(
            'include_libs must be a list of import names, such as ["tasks"].',
            operation=_OP,
        )
    for name in names:
        if not isinstance(name, str) or not _LIB_NAME.fullmatch(name):
            raise ValidationError(
                f"include_libs takes import names such as 'tasks' or "
                f"'tasks.training', not {name!r}.",
                operation=_OP,
            )
        _lib_files(name)
    return tuple(names)


def _covered(module: str, libs: Sequence[str]) -> bool:
    return any(
        module == name or module.startswith(name + ".") for name in libs
    )


def _lib_files(name: str) -> list[tuple[str, Path]]:
    """The files one import name stands for, as ``(archive name, path)``.

    The top-level name is found the way an import finds it, and the
    rest is walked on disk, so the library's code is not run.
    """
    top, *rest = name.split(".")
    try:
        spec = importlib.util.find_spec(top)
    except (ImportError, ValueError):
        spec = None
    if spec is None:
        raise ValidationError(
            f"include_libs: {top!r} cannot be imported here. Run from "
            "the folder that contains it, or add that folder to "
            "sys.path.",
            operation=_OP,
        )
    if spec.submodule_search_locations:
        current = Path(list(spec.submodule_search_locations)[0])
    elif spec.origin and spec.origin.endswith(".py"):
        current = Path(spec.origin)
    else:
        raise ValidationError(
            f"include_libs: {top!r} is not a local Python package or module.",
            operation=_OP,
        )
    current = current.resolve()
    if _installed(current):
        raise ValidationError(
            f"include_libs: {top!r} is an installed package. Install it "
            "in the image instead.",
            operation=_OP,
        )
    base = current.parent
    files: list[Path] = []
    for part in rest:
        if not current.is_dir():
            raise ValidationError(
                f"include_libs: {name!r} was not found.", operation=_OP
            )
        init = current / "__init__.py"
        if init.is_file():
            files.append(init)
        folder = current / part
        current = folder if folder.is_dir() else folder.with_suffix(".py")
    if current.is_dir():
        files.extend(
            path
            for path in sorted(current.rglob("*"))
            if path.is_file() and _sendable(path.relative_to(current))
        )
    elif current.is_file():
        files.append(current)
    else:
        raise ValidationError(
            f"include_libs: {name!r} was not found.", operation=_OP
        )
    return [(path.relative_to(base).as_posix(), path) for path in files]


def _installed(path: Path) -> bool:
    parts = set(path.parts)
    if "site-packages" in parts or "dist-packages" in parts:
        return True
    stdlib = sysconfig.get_paths().get("stdlib")
    return bool(stdlib) and Path(str(stdlib)).resolve() in path.parents


def _sendable(relative: Path) -> bool:
    if relative.suffix in (".pyc", ".pyo"):
        return False
    return not any(
        part == "__pycache__" or part.startswith(".")
        for part in relative.parts
    )


def _pack_libs(names: Sequence[str]) -> _Libs | None:
    """One tar.gz of the libs, the same bytes for the same files."""
    if not names:
        return None
    files: dict[str, Path] = {}
    for name in names:
        files.update(_lib_files(name))
    return _pack_files(files)


def _pack_files(files: Mapping[str, Path]) -> _Libs:
    """One tar.gz of ``archive name: path``, in a fixed order."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as archive:
            for arcname in sorted(files):
                content = files[arcname].read_bytes()
                info = tarfile.TarInfo(arcname)
                info.size = len(content)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(content))
    data = buffer.getvalue()
    digest = hashlib.sha256(data).hexdigest()[:16]
    return _Libs(data=data, digest=digest, count=len(files))


def _function_source(
    function: Callable[..., Any],
    *,
    allowed: Iterable[str] = (),
    strip_decorators: bool = False,
) -> str:
    """The function's source, checked to stand on its own in the job.

    ``allowed`` names from the surrounding file are let through; the
    caller sends them another way. ``strip_decorators`` drops the
    decorators instead of refusing them.
    """
    permitted = set(allowed)
    name = function.__name__
    try:
        text = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(text)
    except (OSError, TypeError, SyntaxError):
        raise ValidationError(
            f"The source of {name} cannot be read, so it cannot be "
            "sent. Define it in a file, or put it in a package listed "
            "in include_libs.",
            operation=_OP,
        ) from None
    node = tree.body[0] if tree.body else None
    if not isinstance(node, ast.FunctionDef):
        raise ValidationError(
            f"{name} must be defined with def to be sent as source.",
            operation=_OP,
        )
    if node.decorator_list and strip_decorators:
        node.decorator_list = []
    if node.decorator_list:
        raise ValidationError(
            f"{name} has a decorator, which cannot be sent with it. "
            "Put it in a package listed in include_libs.",
            operation=_OP,
        )
    if getattr(function, "__closure__", None):
        raise ValidationError(
            f"{name} uses variables of an enclosing function, which "
            "are not sent with it.",
            operation=_OP,
        )
    code = getattr(function, "__code__", None)
    outside = sorted(
        item
        for item in (_globals_used(code) if code is not None else ())
        if not hasattr(builtins, item)
        and item != name
        and item not in permitted
    )
    if outside:
        raise ValidationError(
            f"{name} uses names from the surrounding file: "
            f"{', '.join(outside)}. Its source is sent on its own, so "
            "put imports and helpers inside the function, or put the "
            "function in a package listed in include_libs.",
            operation=_OP,
        )
    # Annotations may name things the job does not have.
    node.returns = None
    arguments = node.args
    for argument in (
        *arguments.posonlyargs,
        *arguments.args,
        *arguments.kwonlyargs,
        arguments.vararg,
        arguments.kwarg,
    ):
        if argument is not None:
            argument.annotation = None
    return ast.unparse(node) + "\n"


def _globals_used(code: CodeType) -> set[str]:
    names = {
        str(instruction.argval)
        for instruction in dis.get_instructions(code)
        if instruction.opname == "LOAD_GLOBAL"
    }
    for constant in code.co_consts:
        if isinstance(constant, CodeType):
            names |= _globals_used(constant)
    return names


def _script(
    command: str,
    inputs: Mapping[str, Any],
    key: str,
    libs_key: str | None,
) -> str:
    """The bash script the job runs: the arrays, a trap, then ``command``.

    The trap writes the exit code and ``outputs`` as NUL-separated text
    when the script ends, however it ends.
    """
    pairs = " ".join(
        f"[{name}]={shlex.quote(_shell_text(value))}"
        for name, value in inputs.items()
    )
    lines = [
        "#!/usr/bin/env bash",
        f"declare -A inputs=({pairs})",
        "declare -A outputs=()",
        f"__ns_run={shlex.quote(f'{MOUNT}/{key}')}",
        'export NS_OUTPUT_DIR="$__ns_run/files"',
        f"export {MOUNT_ENV}={shlex.quote(MOUNT)}",
        "set -e",
        'mkdir -p "$NS_OUTPUT_DIR"',
        "__ns_finish() {",
        "    local code=$? key",
        "    {",
        "        printf '%s\\0' \"$code\"",
        '        for key in "${!outputs[@]}"; do',
        '            printf \'%s\\0%s\\0\' "$key" "${outputs[$key]}"',
        "        done",
        '    } > "$__ns_run/outputs.bin"',
        '    exit "$code"',
        "}",
        "trap __ns_finish EXIT",
    ]
    if libs_key is not None:
        folder = shlex.quote(LIBS_DIR)
        lines += [
            f"mkdir -p {folder}",
            f"tar -xzf {shlex.quote(f'{MOUNT}/{libs_key}')} -C {folder}",
            f'export PYTHONPATH={folder}"${{PYTHONPATH:+:$PYTHONPATH}}"',
        ]
    return "\n".join([*lines, "", command, ""])


def _shell_text(value: object) -> str:
    """A string as it is; any other JSON value as JSON text."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"A command's inputs must be JSON values: {exc}.",
            operation=_OP,
        ) from None


@contextmanager
def _staged(files: Mapping[str, bytes]) -> Iterator[Path]:
    """The generated files on disk until the create has read them."""
    with tempfile.TemporaryDirectory(prefix="ns-task-") as folder:
        root = Path(folder)
        for name, content in files.items():
            (root / name).write_bytes(content)
        yield root


def _code_of(exc: object) -> str:
    status = getattr(exc, "status", None)
    return str(getattr(getattr(status, "code", None), "name", "") or "")


def _raise_bucket(exc: object, name: str, project: str) -> None:
    code = _code_of(exc) or None
    if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        raise AccessError(
            f"Not allowed to find or create the bucket {name} in "
            f"project {project}.",
            operation="task",
            project=project,
            recovery=(
                "A task needs rights to manage Object Storage buckets "
                "in this project. Run ns.whoami() to see the account "
                "in use."
            ),
            status_code=code,
        ) from None
    raise TransportError(
        f"Could not find or create the bucket {name} ({code or 'unknown'}).",
        operation="task",
        project=project,
        recovery="Run the task again.",
        status_code=code,
    ) from None


def _s3_credentials() -> tuple[str, str, str | None]:
    """Access key, secret and optional session token. Never logged."""
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access and secret:
        return access, secret, os.environ.get("AWS_SESSION_TOKEN") or None
    path = Path(
        os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")
    ).expanduser()
    profile = os.environ.get("AWS_PROFILE", "default")
    parser = configparser.RawConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        pass
    if parser.has_section(profile):
        access = parser.get(profile, "aws_access_key_id", fallback=None)
        secret = parser.get(profile, "aws_secret_access_key", fallback=None)
        token = parser.get(profile, "aws_session_token", fallback=None)
        if access and secret:
            return access, secret, token or None
    raise AccessError(
        "No S3 credentials were found, so the task bucket cannot be "
        "read or written from this machine.",
        operation="task",
        recovery=(
            "Create an Object Storage access key and put it in "
            f"{path} under [{profile}], or set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY."
        ),
    )


def _s3(
    bucket: Bucket, method: str, key: str, body: bytes = b""
) -> bytes | None:
    """One signed S3 request. ``None`` when the object does not exist."""
    access, secret, token = _s3_credentials()
    host = bucket.endpoint.removeprefix("https://")
    path = "/" + urllib.parse.quote(f"{bucket.name}/{key}", safe="/-_.~")
    now = _now()
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    digest = hashlib.sha256(body).hexdigest()
    headers = {
        "host": host,
        "x-amz-content-sha256": digest,
        "x-amz-date": stamp,
    }
    if token:
        headers["x-amz-security-token"] = token
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [
            method,
            path,
            "",
            *(f"{name}:{headers[name]}" for name in sorted(headers)),
            "",
            signed,
            digest,
        ]
    )
    scope = f"{day}/{bucket.region}/s3/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            stamp,
            scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ]
    )
    signing = f"AWS4{secret}".encode()
    for part in (day, bucket.region, "s3", "aws4_request"):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256)
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature.hexdigest()}"
    )
    del headers["host"]
    request = urllib.request.Request(
        bucket.endpoint + path,
        data=body if method == "PUT" else None,
        method=method,
        headers=headers,
    )
    what = f"{method} s3://{bucket.name}/{key}"
    try:
        with urllib.request.urlopen(request, timeout=_S3_TIMEOUT) as reply:
            return bytes(reply.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        if exc.code in (401, 403):
            raise AccessError(
                f"The S3 credentials on this machine were refused "
                f"({what}, HTTP {exc.code}).",
                operation="task",
                recovery=(
                    "Use an Object Storage access key that can read and "
                    f"write the bucket {bucket.name}."
                ),
            ) from None
        raise TransportError(
            f"The task bucket request failed ({what}, HTTP {exc.code}).",
            operation="task",
            recovery="Run the task again.",
        ) from None
    except (urllib.error.URLError, OSError) as exc:
        raise TransportError(
            f"The task bucket could not be reached ({what}): {exc}.",
            operation="task",
            recovery="Check the network and run the task again.",
        ) from None
