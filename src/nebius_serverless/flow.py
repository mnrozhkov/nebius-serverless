"""``ns.PrefectFlow``: run a pipeline of tasks as a Prefect flow on a job.

Prototype. A ``PrefectFlow`` wraps a function that calls ``ns.Task``
objects. ``submit`` starts one Nebius job that runs the function as a
Prefect flow, and that job starts the tasks' jobs. Constructing a
``PrefectFlow`` does not contact Nebius.
"""

import builtins
import inspect
import json
import os
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import task as _task
from .errors import AccessError, TransportError, ValidationError
from .image import Image
from .output import Progress, call_progress
from .profile import resolve_location
from .recipe import validate_image
from .task import Task, TaskRun

_LOG = "nebius_serverless.flow"
_OP = "PrefectFlow"

# The secret that holds the flow job's credentials. Each run adds a
# version; the job reads its own version as environment variables.
SECRET_NAME = "generated-tasks-credentials"
TOKEN_ENV = "NEBIUS_IAM_TOKEN"
PREFECT_URL_ENV = "PREFECT_API_URL"
PREFECT_AUTH_ENV = "PREFECT_API_AUTH_STRING"
_TOKEN_TIMEOUT = 30.0

# The default image of a flow job: Python, Prefect, and what this
# package needs. Prefect here must not be newer than the Prefect server.
IMAGE_NAME = "prefect-flow"
DOCKERFILE = """\
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system --compile-bytecode \\
    prefect==3.8.8 nebius==0.6.43 "PyYAML>=6" "rich>=13"
"""


def _default_image() -> Image:
    image = Image(dockerfile_string=DOCKERFILE)
    image.name = IMAGE_NAME
    return image


image_prefect = _default_image()


class PrefectFlow:
    """A pipeline of tasks that runs as a Prefect flow on a Nebius job.

    Pass a function that calls ``ns.Task`` objects. ``submit`` starts
    one job that runs the function as a Prefect flow and returns at
    once, so the flow keeps running after your script has ended. That
    job starts the tasks' jobs. Calling the ``PrefectFlow`` starts it
    and waits for the function's result.

    In the job every task is a Prefect task, so ``task(...)`` and
    ``task.submit(...)`` each make a Prefect task run. The function
    needs no ``@flow`` decorator, and Prefect is not needed on your
    machine.

    The function's source is sent to the job. From outside its own body
    it may use ``ns.Task`` objects, constants with JSON values, and
    what it imports inside its body. Its arguments and its return value
    must be JSON values.

    The tasks' images are built and their ``include_libs`` uploaded on
    your machine, when the flow is submitted. The flow job needs none
    of the tasks' code.

    The flow job starts jobs as you. A fresh access token, and the
    Prefect server's login when there is one, are written as a new
    version of the ``generated-tasks-credentials`` secret in MysteryBox
    and reach the job as environment variables. Versions are not
    deleted. The login is read from ``PREFECT_API_AUTH_STRING`` or from
    your local Prefect settings.

    Args:
        function: The pipeline. A plain function, or one with ``@flow``.
        flow_name: The flow's name in Prefect.
        run_name: A name for the runs. Defaults to ``flow_name``. Each
            run is named ``<launch time>-<run_name>``, in Prefect and
            as the flow job, for example
            ``20261125-091833-mnist-baseline``. The time is UTC.
        api_url: The Prefect server's API address. Defaults to
            ``PREFECT_API_URL``.
        image: An ``ns.Image`` or an image reference for the flow job.
            Defaults to a slim Python image with Prefect, built once
            per project.
        **job_options: Options for the flow job, as on ``JobSpec``:
            ``timeout`` (default 24h), ``cpu``, ``disk`` and the rest.

    Example::

        import nebius_serverless as ns

        def pipeline(epochs=5):
            data = prepare(dataset="mnist")
            runs = [train.submit(data=data, lr=lr) for lr in RATES]
            return [run.result() for run in runs]

        flow = ns.PrefectFlow(
            pipeline,
            flow_name="mnist-baseline",
            api_url="https://prefect.example.com/api",
            timeout="3h",
        )
        run = flow.submit(epochs=5)
    """

    __signature__: inspect.Signature
    default_image = image_prefect

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        flow_name: str,
        run_name: str | None = None,
        api_url: str | None = None,
        image: Image | str | None = None,
        **job_options: Any,
    ) -> None:
        target: Any = function
        if not inspect.isfunction(target):
            # A function with @flow is a Prefect object around it.
            target = getattr(function, "fn", None)
        if not inspect.isfunction(target):
            raise ValidationError(
                "The flow must be a Python function.", operation=_OP
            )
        if inspect.iscoroutinefunction(target):
            raise ValidationError(
                "An async function cannot be a flow.", operation=_OP
            )
        self.function = target
        for label, value in (("flow_name", flow_name), ("run_name", run_name)):
            given = value is not None or label == "flow_name"
            if given and (not isinstance(value, str) or not value.strip()):
                raise ValidationError(
                    f"{label} must be a nonempty name.", operation=_OP
                )
        self.flow_name = flow_name
        self.run_name = run_name if run_name is not None else flow_name
        # The plain form names the run's folder in the bucket.
        self.name = _task._plain_name(flow_name)
        _task._plain_name(self.run_name)
        api = api_url or os.environ.get(PREFECT_URL_ENV)
        if not api:
            raise ValidationError(
                "Pass api_url, the Prefect server's API address, or set "
                f"{PREFECT_URL_ENV}.",
                operation=_OP,
            )
        self.api_url = api
        self.image: Image | str
        if image is None:
            self.image = image_prefect
        elif isinstance(image, Image):
            self.image = image
        else:
            self.image = validate_image(image, operation=_OP)
        self._options = _task._validate_options(job_options)
        self._tasks, self._constants = _used(target)
        self._source = _task._function_source(
            target,
            allowed=[*self._tasks, *self._constants],
            strip_decorators=True,
        )
        self.__signature__ = inspect.signature(target)
        self.__name__ = self.name
        self.__doc__ = target.__doc__
        self._built: tuple[str, str] | None = None
        self._build_lock = threading.Lock()

    def __repr__(self) -> str:
        return f"PrefectFlow({self.flow_name!r}, tasks={sorted(self._tasks)})"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Start the flow job, wait for it, and return the result.

        Interrupting the wait with Ctrl-C cancels the flow job. The
        tasks' jobs it started are not cancelled.
        """
        run = self.submit(*args, **kwargs)
        try:
            return run.result()
        except KeyboardInterrupt:
            run._cancel()
            raise

    def submit(self, *args: Any, **kwargs: Any) -> TaskRun:
        """Start the flow job and return its run at once.

        Builds the images and uploads the libraries of the tasks the
        function uses, when they changed, and writes the credentials.
        It does not wait for the flow. ``result()`` on the run waits
        and returns the function's result.
        """

        def work(progress: Progress) -> TaskRun:
            return self._submit(args, kwargs, progress)

        return call_progress(quiet=False, logger_name=_LOG, work=work)

    def _submit(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
    ) -> TaskRun:
        try:
            bound = self.__signature__.bind(*args, **kwargs)
        except TypeError as exc:
            raise ValidationError(
                f"{self.flow_name}: {exc}.", operation=_OP
            ) from None
        try:
            json.dumps([args, kwargs], allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"{self.flow_name}'s arguments must be JSON values: {exc}.",
                operation=_OP,
            ) from None
        options = _task._plain_options(self._options)
        profile = options.get("profile")
        project, region = resolve_location(
            profile=profile, project=options.get("project"), region=None
        )
        backend = _task._backend(profile)
        bucket, created = backend.ensure_bucket(project)
        if created:
            _task._say(
                progress, f"Created bucket {bucket.name} ({bucket.id})."
            )
        records = {
            alias: item._record(progress, backend, bucket)
            for alias, item in self._tasks.items()
        }
        image = self._image(progress)
        package = _task._upload_libs(
            _own_package(), backend, bucket, progress, "this package"
        )
        values = {TOKEN_ENV: _access_token(profile)}
        login = _prefect_login()
        if login:
            values[PREFECT_AUTH_ENV] = login
        version = _store_credentials(project, profile, values)
        secrets = dict(options.pop("secrets", None) or {})
        secrets.update({item: version for item in values})
        env = dict(options.pop("env", None) or {})
        env[PREFECT_URL_ENV] = self.api_url
        template = os.environ.get(_task.CONSOLE_ENV)
        if template:
            env.setdefault(_task.CONSOLE_ENV, template)
        runner = Task._from_record(
            {
                "name": self.name,
                "doc": self.__doc__,
                "image": image,
                "libs": package,
                "module": __name__,
                "qualname": _run_flow.__name__,
                "source": None,
                "command": None,
                "inputs": [],
                "parameters": None,
                "options": {**options, "env": env, "secrets": secrets},
            }
        )
        # One launch time names the run everywhere: its folder in the
        # bucket, the flow job, and the flow run in Prefect.
        launched = _task._run_name(self.name)
        flow = {
            "title": self.flow_name,
            "run_title": f"{launched}-{self.run_name}",
            "function": self.function.__name__,
            "source": self._source,
            "args": list(args),
            "kwargs": dict(kwargs),
            "constants": self._constants,
            "tasks": records,
            "project": project,
            "region": region,
        }
        run = runner._submit(
            (),
            {"flow": flow},
            progress,
            run_name=launched,
            job_name=f"{launched}-{_task._plain_name(self.run_name)}",
        )
        run.inputs = dict(bound.arguments)
        return run

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
                transport=_task._transport(),
            )
            assert isinstance(ref, str)
            self._built = (tag, ref)
            return ref


def _run_flow(flow: dict[str, Any]) -> Any:
    """Run a submitted function as a Prefect flow. Runs in the flow job.

    The tasks come back from their records as Prefect tasks and are
    bound, with the constants, to the names the function uses.
    """
    from prefect import (  # type: ignore[import-not-found]
        flow as prefect_flow,
    )

    from .session import configure

    # A job has no CLI profile: the project and region were sent along.
    configure(project=flow["project"], region=flow["region"])
    scope: dict[str, Any] = {"__name__": "__ns_flow__", **flow["constants"]}
    for alias, record in flow["tasks"].items():
        scope[alias] = Task._from_record(record).to_prefect()
    exec(compile(flow["source"], "<ns.PrefectFlow source>", "exec"), scope)
    run_title = str(flow["run_title"])
    # A function, so that braces in the name are not read as a template.
    wrapped = prefect_flow(
        name=flow["title"], flow_run_name=lambda: run_title, log_prints=True
    )(scope[flow["function"]])
    return wrapped(*flow["args"], **flow["kwargs"])


def _used(
    function: Callable[..., Any],
) -> tuple[dict[str, Task], dict[str, Any]]:
    """The tasks and the constants a flow function takes from its file."""
    scope = getattr(function, "__globals__", {})
    tasks: dict[str, Task] = {}
    constants: dict[str, Any] = {}
    unknown: list[str] = []
    code = getattr(function, "__code__", None)
    names = _task._globals_used(code) if code is not None else set()
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
            f"it: {', '.join(unknown)}. A flow function may use ns.Task "
            "objects, constants with JSON values, and what it imports "
            "inside its body.",
            operation=_OP,
        )
    return tasks, constants


def _is_json(value: object) -> bool:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return False
    return True


def _own_package() -> "_task._Libs":
    """This package's own files, to send with the flow job."""
    root = Path(_task.__file__).resolve().parent
    files = {
        f"{root.name}/{path.relative_to(root).as_posix()}": path
        for path in sorted(root.rglob("*"))
        if path.is_file() and _task._sendable(path.relative_to(root))
    }
    return _task._pack_files(files)


def _access_token(profile: str | None) -> str:
    """A fresh access token of the account in use. Never logged."""
    from .session import get_sdk

    sdk: Any = get_sdk(profile)
    try:
        value = sdk.get_token_sync(timeout=_TOKEN_TIMEOUT).token
    except Exception:
        raise AccessError(
            "Could not get an access token for the flow job.",
            operation=_OP,
        ) from None
    if not isinstance(value, str) or value == "":
        raise AccessError(
            "The credentials in use give no access token, so the flow "
            "job could not start jobs.",
            operation=_OP,
        )
    return value


def _prefect_login() -> str | None:
    """The Prefect server's ``user:password``, when one is set here."""
    value: object = os.environ.get(PREFECT_AUTH_ENV)
    if not value:
        try:
            from prefect.settings import (  # type: ignore[import-not-found]
                PREFECT_API_AUTH_STRING,
            )

            value = PREFECT_API_AUTH_STRING.value()
        except Exception:
            value = None
    reveal = getattr(value, "get_secret_value", None)
    if callable(reveal):
        value = reveal()
    return value if isinstance(value, str) and value else None


def _store_credentials(
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
            if _task._code_of(exc) != "NOT_FOUND":
                raise
            created = secrets.create(
                CreateSecretRequest(
                    metadata=ResourceMetadata(
                        parent_id=project, name=SECRET_NAME
                    ),
                    spec=SecretSpec(
                        description=(
                            "Credentials of ns.PrefectFlow jobs. Each "
                            "run adds a version."
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
        code = _task._code_of(exc) or None
        if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
            raise AccessError(
                f"Not allowed to write the secret {SECRET_NAME} in "
                f"project {project}.",
                operation=_OP,
                project=project,
                recovery=(
                    "A flow needs rights to manage MysteryBox secrets "
                    "in this project. Run ns.whoami() to see the "
                    "account in use."
                ),
                status_code=code,
            ) from None
        raise TransportError(
            f"Could not write the secret {SECRET_NAME} ({code or 'unknown'}).",
            operation=_OP,
            project=project,
            recovery="Submit the flow again.",
            status_code=code,
        ) from None
    if not version:
        raise TransportError(
            f"The new version of {SECRET_NAME} has no id.",
            operation=_OP,
            project=project,
        )
    return version
