"""``ns.Task``: run a Python function or a bash script as a Nebius job.

A ``Task`` wraps a function or a script. Calling it starts a job,
waits for it, and returns the result. Constructing a ``Task`` does not
contact Nebius.

Code and results travel through a bucket in the project:
``ns-workspace-<project id>``, which the first run creates, or a bucket
you choose with ``bucket=`` or ``ns.configure(bucket=...)``. Jobs mount
it; this machine reads and writes it with your S3 credentials.
The bucket is in ``storage``, the code that travels in ``code``, what
runs in the job in ``runtime``, and a run's handle in ``run``.
"""

import hashlib
import inspect
import json
import re
import sys
import textwrap
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..core.profile import resolve_location
from ..core.recipe import (
    INJECT_FILE_MAX_BYTES,
    validate_image,
    validate_optional_text,
)
from ..errors import ValidationError
from ..images.image import Image
from ..ui.output import Progress, call_progress
from . import record, runtime, storage, submit
from .attempts import Attempts
from .code import (
    covered,
    function_source,
    pack_libs,
    upload_libs,
    validate_libs,
)
from .run import TaskRun
from .storage import Bucket, SdkTaskBackend, chosen_bucket
from .submit import build_image, plain_options, say, validate_options

_LOG = "nebius_serverless.task"


_OP = "Task"


_INPUT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


_NAME_MAX = 40


class Task:
    """A function or a bash script that runs as a Nebius job.

    Pass a function, or ``command=`` with a bash script. Calling the
    task starts a job, waits for it, and returns the result. ``submit``
    starts the job and returns a ``TaskRun`` at once.

    A function gets its arguments as JSON values and must return a JSON
    value. If it comes from a package listed in ``local_libs``, the
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
    the task bucket, which every task's job mounts at the same path, so
    a path one task returns can be opened by another. The bucket is the
    project's ``ns-workspace-<project id>``, created on the first run,
    unless you choose one. Tasks that pass runs to each other must use
    the same bucket.

    Reading results on this machine uses your S3 credentials:
    ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``, or the profile
    in ``~/.aws/credentials``.

    Args:
        function: The function to run.
        command: A bash script to run instead of a function.
        inputs: The names of a script's inputs.
        image: An ``ns.Image``, built on the first run, or an image
            reference.
        local_libs: Import names of local packages or modules to send
            with the task, such as ``"tasks"`` or ``"tasks.training"``.
        name: The task's name. Defaults to the function's name. Required
            for a script. Each run's job is named ``<name>-<UTC time>``.
        bucket: An existing bucket for the task's code, inputs and
            results. It is never created, and is checked for reading and
            writing on first use. The library's files go under ``ns/``
            in it. Defaults to ``ns.configure(bucket=...)``, then the
            ``NS_BUCKET`` variable, then the project's bucket.
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
        local_libs: Sequence[str] = (),
        name: str | None = None,
        bucket: str | None = None,
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
        self.local_libs = validate_libs(local_libs)
        self.bucket = validate_optional_text(bucket, "bucket", operation=_OP)
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
        has not been built, and uploads ``local_libs`` when they
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
        self._check_buckets((args, kwargs))
        run = TaskRun._waiting(self.name, chosen_bucket(self.bucket))

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
        if covered(module, self.local_libs):
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
        options = self._options
        project, region = resolve_location(
            profile=options.get("profile"),
            project=options.get("project"),
            region=None,
        )
        backend = storage.backend(options.get("profile"))
        bucket, created = backend.ensure_bucket(
            project, chosen_bucket(self.bucket)
        )
        if created:
            say(progress, f"Created bucket {bucket.name} ({bucket.id}).")
        image, libs_key = self._prepare(progress, backend, bucket)
        if run_name is None:
            run_name = submit.run_name(self.name)
        job_name = job_name or f"{self.name}-{run_name}"
        if key is None:
            key = submit.run_key(self.name, run_name, job_name, folder)
        payload = self._payload(bound, libs_key and bucket.key(libs_key))
        backend.put(bucket, f"{key}/payload.json", payload)
        definition = self._definition(image, libs_key)
        run = record.new(
            run_id=key,
            task=self.name,
            name=run_name,
            project=project,
            bucket=bucket,
            scripted=self.command is not None,
            definition=definition,
            preemption_retries=0,
        )
        attempts = Attempts(
            task=self,
            run=run,
            backend=backend,
            bucket=bucket,
            payload=payload,
            region=region,
        )
        run["preemption_retries"] = attempts.budget
        # The record exists before the first job does.
        record.write(backend, bucket, run)
        job = attempts.first(job_name, progress)
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
            bucket_choice=chosen_bucket(self.bucket),
            attempts=attempts,
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
        libs = pack_libs(self.local_libs)
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
        return self._definition(image, libs_key)

    def _definition(self, image: str, libs_key: str | None) -> dict[str, Any]:
        """The task as JSON, once its image and libraries are ready."""
        return {
            "name": self.name,
            "doc": self.__doc__,
            "image": image,
            "libs": libs_key,
            "module": self._module,
            "qualname": self._qualname,
            "source": self._source,
            "source_sha256": (
                None
                if self._source is None
                else hashlib.sha256(self._source.encode()).hexdigest()
            ),
            "command": self.command,
            "inputs": list(self.inputs),
            "parameters": _describe_signature(self.__signature__),
            "options": plain_options(self._options),
            "bucket": self.bucket,
        }

    @classmethod
    def _from_record(cls, record: Mapping[str, Any]) -> "Task":
        """A task made from a record. It needs no local code to submit."""
        task = cls.__new__(cls)
        task.image = str(record["image"])
        task.local_libs = ()
        task._options = dict(record["options"])
        task.function = None
        task.command = record.get("command")
        task.inputs = tuple(record.get("inputs") or ())
        task._module = record.get("module")
        task._qualname = str(record.get("qualname") or "")
        task._source = record.get("source")
        task._fixed = True
        task._libs_key = record.get("libs")
        task.bucket = record.get("bucket")
        task.name = str(record["name"])
        task.__name__ = task.name
        task.__doc__ = record.get("doc")
        task.__signature__ = _signature_from(record.get("parameters"))
        task._built = None
        task._build_lock = threading.Lock()
        return task

    def _check_buckets(self, value: object) -> None:
        """Runs given as arguments must be in this task's bucket: their
        jobs pass files through one ``/ns`` mount."""
        mine = chosen_bucket(self.bucket)
        for run in _runs_in(value):
            if run._bucket_choice != mine:
                raise ValidationError(
                    f"Task {self.name} uses "
                    f"{_bucket_label(mine)}, but its input from task "
                    f"{run.task} is in {_bucket_label(run._bucket_choice)}. "
                    "Tasks that pass runs to each other share one bucket, "
                    "because their jobs pass files through one /ns mount.",
                    operation=_OP,
                    recovery=(
                        "Give every task the same bucket=, or set it once "
                        "with ns.configure(bucket=...)."
                    ),
                )

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

    def _payload(self, bound: "_Bound", libs_key: str | None) -> bytes:
        """What one call runs with, for ``payload.json``.

        A function's code pointer, arguments and libraries, or a
        script's inputs. ``libs_key`` is the archive's full key. The
        payload goes to the bucket, so its size is not limited.
        """
        if self.command is not None:
            return json.dumps({"inputs": bound.inputs}).encode()
        return json.dumps(
            {
                "libs": libs_key,
                "libs_dir": runtime.LIBS_DIR,
                "module": self._module,
                "qualname": self._qualname,
                "source": self._source,
                # The job compares it with its own python3.
                "python": "%d.%d" % sys.version_info[:2],
                "args": bound.args,
                "kwargs": bound.kwargs,
            }
        ).encode()

    def _inputs_of(self, payload: bytes) -> dict[str, Any]:
        """A run's inputs by name, read back from its payload."""
        body = json.loads(payload)
        if self.command is not None:
            return dict(body["inputs"])
        try:
            bound = self.__signature__.bind(*body["args"], **body["kwargs"])
        except TypeError:
            return {"args": body["args"], "kwargs": body["kwargs"]}
        return dict(bound.arguments)

    def _attempt_files(
        self,
        payload: bytes,
        key: str,
        libs_key: str | None,
        attempt: int = 1,
    ) -> tuple[list[str], dict[str, bytes]]:
        """One attempt's command and injected files. ``key`` and
        ``libs_key`` are full keys in the bucket.

        A function's job is given a pointer to its payload with its
        checksum, and refuses a payload that does not match. A script
        gets its inputs in the script itself.
        """
        if self.command is not None:
            inputs = json.loads(payload)["inputs"]
            script = runtime.bash_script(
                self.command, inputs, key, libs_key, attempt
            )
            if len(script.encode()) > INJECT_FILE_MAX_BYTES:
                raise ValidationError(
                    f"{self.name}'s script and inputs are too large to "
                    "send (over 64 KiB). Pass large data as files.",
                    operation=_OP,
                )
            return (
                ["bash", f"{runtime.JOB_DIR}/run.sh"],
                {"run.sh": script.encode()},
            )
        pointer = {
            "root": runtime.MOUNT,
            "run": key,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "attempt": attempt,
        }
        files = {
            "runner.py": runtime.RUNNER.encode(),
            "pointer.json": json.dumps(pointer).encode(),
        }
        return ["python3", f"{runtime.JOB_DIR}/runner.py"], files


@dataclass(frozen=True)
class _Bound:
    """One call's inputs: as sent to the job, and by name."""

    args: Sequence[Any]
    kwargs: Mapping[str, Any]
    inputs: dict[str, Any]


def _runs_in(value: object) -> list[TaskRun]:
    """Every run among the arguments, at any depth."""
    if isinstance(value, TaskRun):
        return [value]
    if isinstance(value, (list, tuple)):
        return [run for item in value for run in _runs_in(item)]
    if isinstance(value, dict):
        return [run for item in value.values() for run in _runs_in(item)]
    return []


def _bucket_label(choice: str | None) -> str:
    if choice is None:
        return "the project's default bucket"
    return f"the bucket {choice}"


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


def _keyword_signature(names: Sequence[str]) -> inspect.Signature:
    """A signature that takes each name as a keyword argument."""
    kind = inspect.Parameter.KEYWORD_ONLY
    return inspect.Signature([inspect.Parameter(name, kind) for name in names])


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
