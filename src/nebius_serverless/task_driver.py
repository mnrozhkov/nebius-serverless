"""Run a function that calls tasks: on a job of its own, or here.

Private to this package. It holds what any orchestrator integration
needs to run a pipeline: a plain function that calls ``Task`` objects.

- ``Driver`` checks what the function uses from outside its body:
  tasks, and constants with JSON values. Its source is sent on its own.
- ``Driver.launch`` names one run, and ``Driver.program`` describes it
  as JSON: the source, the arguments, the constants, and the run's
  folder in the bucket.
- ``Driver.submit`` builds the tasks' images, uploads their libraries,
  sends them as records, stores the credentials the job needs to start
  jobs, and starts the driver job. That job runs an entrypoint of this
  package with the program, and the entrypoint calls ``job_tasks`` and
  ``bind``.
- ``Driver.prepare_here`` gets the tasks ready to run the function in
  this process instead.

A run keeps its files in ``flows/<name>/<run>/`` in the task bucket:
``flow/`` for the driver job, and one folder per task job.
"""

import inspect
import json
import os
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import task, task_runner, task_storage
from .errors import AccessError, TransportError, ValidationError
from .image import Image
from .output import Progress
from .profile import resolve_location
from .task import (
    CONSOLE_ENV,
    Task,
    TaskRun,
    plain_name,
    plain_options,
    prepare_task,
    say,
    submit_run,
    task_from_record,
    task_record,
    validate_options,
)
from .task_libs import (
    Libs,
    function_source,
    globals_used,
    pack_files,
    sendable,
    upload_libs,
)
from .task_storage import Bucket, code_of

_OP = "Task"

# Where pipeline runs keep their files: flows/<name>/<run>/<job name>.
RUNS_ROOT = "flows"
# The folder of the driver job itself, next to the folders of its tasks.
DRIVER_FOLDER = "flow"

# The secret that holds the driver job's credentials. Each run adds a
# version; the job reads its own version as environment variables.
SECRET_NAME = "generated-tasks-credentials"
TOKEN_ENV = "NEBIUS_IAM_TOKEN"
_TOKEN_TIMEOUT = 30.0


@dataclass(frozen=True)
class Launch:
    """One run's names and its checked arguments, before it starts."""

    parameters: dict[str, Any]
    stamp: str
    job_name: str
    title: str
    folder: str


class Driver:
    """A function that calls tasks, ready to run as a pipeline.

    ``operation`` names the caller in errors: the integration's class.
    ``name`` names the runs' folder, ``flows/<name>``, and ``run_name``
    each run: ``<UTC launch time>-<run_name>``.
    """

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        name: str,
        run_name: str,
        image: Image | str,
        job_options: Mapping[str, Any],
        operation: str,
    ) -> None:
        if not inspect.isfunction(function):
            raise ValidationError(
                "The pipeline must be a Python function.", operation=operation
            )
        if inspect.iscoroutinefunction(function):
            raise ValidationError(
                "An async function cannot be a pipeline.", operation=operation
            )
        self.function = function
        self.operation = operation
        self.name = plain_name(name)
        self.run_name = run_name
        plain_name(run_name)
        self.image = image
        self.options = validate_options(dict(job_options))
        self.tasks, self.constants = used_names(function, operation)
        self.source = function_source(
            function,
            allowed=[*self.tasks, *self.constants],
            strip_decorators=True,
        )
        self.signature = inspect.signature(function)
        self._built: tuple[str, str] | None = None
        self._build_lock = threading.Lock()

    def launch(
        self, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> Launch:
        """Check the arguments and name the run. Fails before anything
        is built or started."""
        try:
            bound = self.signature.bind(*args, **kwargs)
        except TypeError as exc:
            raise ValidationError(
                f"{self.function.__name__}: {exc}.", operation=self.operation
            ) from None
        try:
            json.dumps([args, kwargs], allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"{self.function.__name__}'s arguments must be JSON "
                f"values: {exc}.",
                operation=self.operation,
            ) from None
        # One launch time names the run everywhere: its folder in the
        # bucket, the driver job, and the run in the orchestrator.
        stamp = task_runner.run_name(self.name)
        job_name = f"{stamp}-{plain_name(self.run_name)}"
        return Launch(
            parameters=dict(bound.arguments),
            stamp=stamp,
            job_name=job_name,
            title=f"{stamp}-{self.run_name}",
            folder=f"{RUNS_ROOT}/{self.name}/{job_name}",
        )

    def program(
        self,
        launch: Launch,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        """What runs the function, as JSON. ``extra`` is the caller's."""
        return {
            "function": self.function.__name__,
            "source": self.source,
            "args": list(args),
            "kwargs": dict(kwargs),
            "parameters": launch.parameters,
            "constants": self.constants,
            "folder": launch.folder,
            **extra,
        }

    def location(self) -> tuple[str, str]:
        """The project and the region the jobs go to."""
        return resolve_location(
            profile=self.options.get("profile"),
            project=self.options.get("project"),
            region=None,
        )

    def bucket(self, progress: Progress) -> tuple[Bucket, Any]:
        """The task bucket, and the storage that reads it."""
        project, _ = self.location()
        storage = task_storage.backend(self.options.get("profile"))
        bucket, created = storage.ensure_bucket(project)
        if created:
            say(progress, f"Created bucket {bucket.name} ({bucket.id}).")
        return bucket, storage

    def prepare_here(self, progress: Progress) -> Bucket:
        """Get the tasks ready to run the function in this process.

        The images are built and the libraries uploaded now, one task
        after another, and not by tasks that start together.
        """
        bucket, storage = self.bucket(progress)
        for item in self.tasks.values():
            prepare_task(item, progress, storage, bucket)
        return bucket

    def submit(
        self,
        launch: Launch,
        program: Mapping[str, Any],
        progress: Progress,
        *,
        entrypoint: Callable[[dict[str, Any]], Any],
        env: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
    ) -> TaskRun:
        """Start the driver job and return its run at once.

        The job runs ``entrypoint(program)``, a module-level function of
        this package, with the task records, the project and the region
        added to ``program``. ``env`` is added to the job's environment.
        ``secrets`` are stored with the access token in the credentials
        secret, and reach the job as environment variables.
        """
        module = getattr(entrypoint, "__module__", "") or ""
        if not module.startswith(f"{__package__}."):
            raise ValidationError(
                "The entrypoint must be a function of this package.",
                operation=self.operation,
            )
        options = plain_options(self.options)
        profile = options.get("profile")
        project, region = self.location()
        bucket, storage = self.bucket(progress)
        records = {
            alias: task_record(item, progress, storage, bucket)
            for alias, item in self.tasks.items()
        }
        image = self._image(progress)
        package = upload_libs(
            own_package(), storage, bucket, progress, "this package"
        )
        values = {TOKEN_ENV: access_token(profile), **dict(secrets or {})}
        version_id = store_credentials(project, profile, values)
        job_secrets = dict(options.pop("secrets", None) or {})
        job_secrets.update({item: version_id for item in values})
        job_env = dict(options.pop("env", None) or {})
        job_env.update(env or {})
        # The driver job tells its tasks where the run's folder is.
        job_env[task_runner.FOLDER_ENV] = launch.folder
        template = os.environ.get(CONSOLE_ENV)
        if template:
            job_env.setdefault(CONSOLE_ENV, template)
        runner = task_from_record(
            {
                "name": self.name,
                "doc": self.function.__doc__,
                "image": image,
                "libs": package,
                "module": module,
                "qualname": entrypoint.__qualname__,
                "source": None,
                "command": None,
                "inputs": [],
                "parameters": None,
                "options": {**options, "env": job_env, "secrets": job_secrets},
            }
        )
        full = {
            **program,
            "tasks": records,
            "project": project,
            "region": region,
        }
        run = submit_run(
            runner,
            (),
            {"program": full},
            progress,
            run_name=launch.stamp,
            job_name=launch.job_name,
            key=f"{launch.folder}/{DRIVER_FOLDER}",
        )
        run.inputs = launch.parameters
        return run

    def _image(self, progress: Progress) -> str:
        if isinstance(self.image, str):
            return self.image
        with self._build_lock:
            tag = self.image.tag
            if self._built is not None and self._built[0] == tag:
                return self._built[1]
            ref = task.build_image(self.image, progress)
            self._built = (tag, ref)
            return ref


def run_paths(launch_folder: str, bucket: Bucket) -> tuple[str, str]:
    """A run's folder in the bucket: as jobs see it, and as ``s3://``."""
    return (
        f"{task_runner.MOUNT}/{launch_folder}",
        f"s3://{bucket.name}/{launch_folder}/",
    )


def job_tasks(program: Mapping[str, Any]) -> dict[str, Task]:
    """The tasks of a submitted program, made from their records.

    Runs in the driver job. A job has no CLI profile, so the project
    and region that were sent along are configured first.
    """
    from .session import configure

    configure(project=program["project"], region=program["region"])
    return {
        alias: task_from_record(record)
        for alias, record in program["tasks"].items()
    }


def bind(
    program: Mapping[str, Any], tasks: Mapping[str, Any]
) -> Callable[..., Any]:
    """The program's function, with ``tasks`` and its constants bound
    to the names it uses. ``tasks`` may be the tasks themselves, or
    what an orchestrator wraps them in."""
    scope: dict[str, Any] = {
        "__name__": "__ns_driver__",
        **program["constants"],
        **tasks,
    }
    exec(compile(program["source"], "<ns pipeline source>", "exec"), scope)
    function = scope[program["function"]]
    assert callable(function)
    return function


def used_names(
    function: Callable[..., Any], operation: str
) -> tuple[dict[str, Task], dict[str, Any]]:
    """The tasks and the constants a function takes from its file."""
    import builtins

    scope = getattr(function, "__globals__", {})
    tasks: dict[str, Task] = {}
    constants: dict[str, Any] = {}
    unknown: list[str] = []
    code = getattr(function, "__code__", None)
    names = globals_used(code) if code is not None else set()
    for item in sorted(names):
        if item == function.__name__:
            continue
        if item in scope:
            value = scope[item]
            if isinstance(value, Task):
                tasks[item] = value
            elif _is_json(value):
                constants[item] = value
            else:
                unknown.append(item)
        elif not hasattr(builtins, item):
            unknown.append(item)
    if unknown:
        raise ValidationError(
            f"{function.__name__} uses names that cannot be sent with "
            f"it: {', '.join(unknown)}. A pipeline function may use "
            "ns.Task objects, constants with JSON values, and what it "
            "imports inside its body.",
            operation=operation,
        )
    return tasks, constants


def _is_json(value: object) -> bool:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return False
    return True


def own_package() -> Libs:
    """This package's own files, to send with the driver job."""
    root = Path(__file__).resolve().parent
    files = {
        f"{root.name}/{path.relative_to(root).as_posix()}": path
        for path in sorted(root.rglob("*"))
        if path.is_file() and sendable(path.relative_to(root))
    }
    return pack_files(files)


def access_token(profile: str | None) -> str:
    """A fresh access token of the account in use. Never logged."""
    from .session import get_sdk

    sdk: Any = get_sdk(profile)
    try:
        value = sdk.get_token_sync(timeout=_TOKEN_TIMEOUT).token
    except Exception:
        raise AccessError(
            "Could not get an access token for the driver job.",
            operation=_OP,
        ) from None
    if not isinstance(value, str) or value == "":
        raise AccessError(
            "The credentials in use give no access token, so the driver "
            "job could not start jobs.",
            operation=_OP,
        )
    return value


def store_credentials(
    project: str, profile: str | None, values: dict[str, str]
) -> str:
    """Write ``values`` as a new version of the credentials secret.

    Returns the version's id. Each value is stored under the name of the
    environment variable that the job reads it as. Earlier versions are
    left as they are, and the new one is not made the primary version.
    """
    from nebius.aio.service_error import RequestError
    from nebius.api.nebius.common.v1 import ResourceMetadata
    from nebius.api.nebius.mysterybox.v1 import (
        CreateSecretRequest,
        CreateSecretVersionRequest,
        GetSecretByNameRequest,
        GetSecretRequest,
        Payload,
        SecretServiceClient,
        SecretSpec,
        SecretVersionServiceClient,
        SecretVersionSpec,
    )

    from .session import get_sdk

    sdk: Any = get_sdk(profile)
    payload = [
        Payload(key=key, string_value=value) for key, value in values.items()
    ]
    secrets = SecretServiceClient(sdk)
    lookup = GetSecretByNameRequest(parent_id=project, name=SECRET_NAME)
    version = ""
    try:
        try:
            secret = secrets.get_by_name(lookup).wait()
        except RequestError as exc:
            if code_of(exc) != "NOT_FOUND":
                raise
            created = secrets.create(
                CreateSecretRequest(
                    metadata=ResourceMetadata(
                        parent_id=project, name=SECRET_NAME
                    ),
                    spec=SecretSpec(
                        description=(
                            "Credentials of jobs that start tasks. "
                            "Each run adds a version."
                        ),
                        secret_version=SecretVersionSpec(payload=payload),
                    ),
                )
            ).wait()
            created.sync_wait()
            secret = secrets.get(
                GetSecretRequest(id=created.resource_id)
            ).wait()
            version = str(secret.spec.primary_version_id or "")
        else:
            added = (
                SecretVersionServiceClient(sdk)
                .create(
                    CreateSecretVersionRequest(
                        metadata=ResourceMetadata(
                            parent_id=secret.metadata.id
                        ),
                        spec=SecretVersionSpec(payload=payload),
                    )
                )
                .wait()
            )
            added.sync_wait()
            version = str(getattr(added, "resource_id", "") or "")
    except RequestError as exc:
        code = code_of(exc) or None
        if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
            raise AccessError(
                f"Not allowed to write the secret {SECRET_NAME} in "
                f"project {project}.",
                operation=_OP,
                project=project,
                recovery=(
                    "Starting tasks from a job needs rights to manage "
                    "MysteryBox secrets "
                    "in this project. Run ns.whoami() to see the "
                    "account in use."
                ),
                status_code=code,
            ) from None
        raise TransportError(
            f"Could not write the secret {SECRET_NAME} ({code or 'unknown'}).",
            operation=_OP,
            project=project,
            recovery="Submit again.",
            status_code=code,
        ) from None
    if not version:
        raise TransportError(
            f"The new version of {SECRET_NAME} has no id.",
            operation=_OP,
            project=project,
        )
    return version
