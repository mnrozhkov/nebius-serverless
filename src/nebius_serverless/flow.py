"""``ns.PrefectFlow``: run a pipeline of tasks as a Prefect flow on a job.

A ``PrefectFlow`` wraps a function that calls ``ns.Task`` objects.
``submit`` starts one Nebius job that runs the function as a Prefect
flow, and that job starts the tasks' jobs. With ``remote_job=False``
the flow runs in this process instead. Either way the flow run is
created in Prefect at submit, so its link is known at once.
Constructing a ``PrefectFlow`` does not contact Nebius.
"""

import base64
import builtins
import inspect
import json
import os
import re
import threading
import urllib.error
import urllib.request
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
# The Prefect UI's address. Without it, the API address without its
# trailing /api.
PREFECT_UI_ENV = "PREFECT_UI_URL"
# Where the Prefect server's login is read from when it is not in the
# environment or in Prefect's settings: one line, ``user:password``.
PREFECT_AUTH_FILE = Path("~/.config/prefect/auth")
_PREFECT_TIMEOUT = 10.0
_TOKEN_TIMEOUT = 30.0

# The default image of a flow job: Python, Prefect, and what this
# package needs. Prefect here must not be newer than the Prefect server;
# submit checks that.
IMAGE_NAME = "prefect-flow"
DOCKERFILE = """\
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system --compile-bytecode \\
    prefect==3.9.5 nebius==0.6.43 "PyYAML>=6" "rich>=13"
"""


def _default_image() -> Image:
    image = Image(dockerfile_string=DOCKERFILE)
    image.name = IMAGE_NAME
    return image


image_prefect = _default_image()

_VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")


class PrefectFlow:
    """A pipeline of tasks that runs as a Prefect flow.

    Pass a function that calls ``ns.Task`` objects. ``submit`` starts
    one Nebius job that runs the function as a Prefect flow and returns
    at once, so the flow keeps running after your script has ended.
    That job starts the tasks' jobs. Calling the ``PrefectFlow`` starts
    it and waits for the function's result.

    With ``remote_job=False`` the flow runs in this process, and the
    tasks' jobs are started from here with your Nebius profile. This
    needs the ``prefect`` package here. ``submit`` runs the flow in a
    thread and returns at once, but the script does not end before the
    flow does. Calling the ``PrefectFlow`` runs the flow and returns
    its result.

    Every task is a Prefect task, so ``task(...)`` and
    ``task.submit(...)`` each make a Prefect task run. The function
    needs no ``@flow`` decorator, and with the flow job Prefect is not
    needed on your machine.

    The flow run is created in Prefect at submit and stays Pending
    until the flow starts. Its link is logged, and is on the returned
    run as ``flow_run_url``. When the Prefect server cannot be reached
    from here, there is no link, and the flow creates its run when it
    starts. ``submit`` stops when the server runs an older Prefect than
    the flow would: the one in the default image, or the one installed
    here with ``remote_job=False``.

    The function's source is sent to the job. From outside its own body
    it may use ``ns.Task`` objects, constants with JSON values, and
    what it imports inside its body. Its arguments and its return value
    must be JSON values.

    The tasks' images are built and their ``include_libs`` uploaded on
    your machine, when the flow is submitted. The flow job needs none
    of the tasks' code.

    A flow job starts jobs as you. A fresh access token, and the
    Prefect server's login when there is one, are written as a new
    version of the ``generated-tasks-credentials`` secret in MysteryBox
    and reach the job as environment variables. Versions are not
    deleted. The login is read from ``PREFECT_API_AUTH_STRING``, from
    your local Prefect settings, or from the file
    ``~/.config/prefect/auth`` (one line, ``user:password``). ``submit``
    checks it with the server first, so a missing or wrong login fails
    at once and not inside the job.

    Args:
        function: The pipeline. A plain function, or one with ``@flow``.
        flow_name: The flow's name in Prefect.
        run_name: A name for the runs. Defaults to ``flow_name``. Each
            run is named ``<launch time>-<run_name>``, in Prefect and
            as the flow job, for example
            ``20261125-091833-mnist-baseline``. The time is UTC.
        api_url: The Prefect server's API address. Defaults to
            ``PREFECT_API_URL``.
        remote_job: Run the flow on a Nebius job (the default), or in
            this process with ``False``.
        image: An ``ns.Image`` or an image reference for the flow job.
            Defaults to a slim Python image with Prefect, built once
            per project. Not used with ``remote_job=False``.
        **job_options: Options for the flow job, as on ``JobSpec``:
            ``timeout`` (default 24h), ``cpu``, ``disk`` and the rest.
            With ``remote_job=False`` only ``profile`` and ``project``
            are used: they choose where the tasks' images are built.

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
        remote_job: bool = True,
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
        self.remote_job = bool(remote_job)
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
        """Run the flow, wait for it, and return the result.

        Interrupting the wait with Ctrl-C cancels the flow job, or
        stops the flow with ``remote_job=False``. The tasks' jobs it
        started are not cancelled.
        """
        if not self.remote_job:

            def prepare(progress: Progress) -> tuple["FlowRun", dict]:
                return self._prepare_here(args, kwargs, progress)

            handle, flow = call_progress(
                quiet=False, logger_name=_LOG, work=prepare
            )
            return self._run_here(flow, handle)

        def work(progress: Progress) -> TaskRun:
            return self._submit(args, kwargs, progress)

        run = call_progress(quiet=False, logger_name=_LOG, work=work)
        try:
            return run.result()
        except KeyboardInterrupt:
            run._cancel()
            raise

    def submit(self, *args: Any, **kwargs: Any) -> "TaskRun | FlowRun":
        """Start the flow and return its run at once.

        Builds the images and uploads the libraries of the tasks the
        function uses, when they changed, and creates the flow run in
        Prefect. With the flow job it also writes the credentials and
        returns the job's ``TaskRun``. With ``remote_job=False`` it
        returns a ``FlowRun`` while the flow runs in a thread. It does
        not wait for the flow. ``result()`` on the run waits and
        returns the function's result.
        """
        if not self.remote_job:

            def prepare(progress: Progress) -> tuple["FlowRun", dict]:
                return self._prepare_here(args, kwargs, progress)

            handle, flow = call_progress(
                quiet=False, logger_name=_LOG, work=prepare
            )
            handle._start(lambda: self._run_here(flow, handle))
            return handle

        def work(progress: Progress) -> TaskRun:
            return self._submit(args, kwargs, progress)

        return call_progress(quiet=False, logger_name=_LOG, work=work)

    def _begin(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        version: str | None,
        where: str,
    ) -> "_Start":
        """Check the arguments and the Prefect server, and name the run.

        ``version`` is the Prefect that will run the flow, ``where`` says
        where it is. Fails before anything is built or started.
        """
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
        login = _prefect_login()
        server = _check_prefect(self.api_url, login)
        _check_versions(self.api_url, server, version, where)
        # One launch time names the run everywhere: its folder in the
        # bucket, the flow job, and the flow run in Prefect.
        launched = _task._run_name(self.name)
        job_name = f"{launched}-{_task._plain_name(self.run_name)}"
        return _Start(
            parameters=dict(bound.arguments),
            login=login,
            launched=launched,
            job_name=job_name,
            run_title=f"{launched}-{self.run_name}",
            # The bucket is laid out as flows/<pipeline>/<run>/<task>.
            folder=f"{_task.FLOWS_ROOT}/{self.name}/{job_name}",
        )

    def _flow_run(self, start: "_Start", progress: Progress) -> str | None:
        """Create the flow run in Prefect and log its link."""
        run_id = _create_flow_run(
            self.api_url,
            start.login,
            self.flow_name,
            start.run_title,
            start.parameters,
        )
        if run_id is None:
            _task._say(
                progress,
                f"Could not create the flow run at {self.api_url} from "
                "here. The flow creates it when it starts.",
            )
        else:
            _task._say(
                progress,
                f"Prefect flow run: {_ui_url(self.api_url, run_id)}",
            )
        return run_id

    def _flow(
        self,
        start: "_Start",
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        run_id: str | None,
    ) -> dict[str, Any]:
        """What runs the flow: the same in the job and here."""
        return {
            "title": self.flow_name,
            "run_title": start.run_title,
            "function": self.function.__name__,
            "source": self._source,
            "args": list(args),
            "kwargs": dict(kwargs),
            "parameters": start.parameters,
            "constants": self._constants,
            "folder": start.folder,
            "flow_run_id": run_id,
        }

    def _prepare_here(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
    ) -> tuple["FlowRun", dict[str, Any]]:
        """Get a flow ready to run in this process."""
        version = _local_prefect()
        start = self._begin(args, kwargs, version, "installed here")
        # The tasks' images are built and their libraries uploaded now,
        # one after another, and not by tasks that start together.
        options = _task._plain_options(self._options)
        profile = options.get("profile")
        project, _ = resolve_location(
            profile=profile, project=options.get("project"), region=None
        )
        backend = _task._backend(profile)
        bucket, created = backend.ensure_bucket(project)
        if created:
            _task._say(
                progress, f"Created bucket {bucket.name} ({bucket.id})."
            )
        for item in self._tasks.values():
            item._prepare(progress, backend, bucket)
        run_id = self._flow_run(start, progress)
        handle = FlowRun(
            name=start.launched,
            title=start.run_title,
            inputs=start.parameters,
            flow_run_id=run_id,
            flow_run_url=_ui_url(self.api_url, run_id) if run_id else None,
            output_path=f"{_task.MOUNT}/{start.folder}",
            output_uri=f"s3://{bucket.name}/{start.folder}/",
        )
        handle._login = start.login
        return handle, self._flow(start, args, kwargs, run_id)

    def _run_here(self, flow: dict[str, Any], handle: "FlowRun") -> Any:
        """Run the flow in this process, with this flow's server."""
        from prefect.settings import (  # type: ignore[import-not-found]
            PREFECT_API_AUTH_STRING,
            PREFECT_API_URL,
            temporary_settings,
        )

        updates: dict[Any, Any] = {PREFECT_API_URL: self.api_url}
        if handle._login:
            updates[PREFECT_API_AUTH_STRING] = handle._login
        with temporary_settings(updates=updates):
            return _execute(flow, self._tasks)

    def _submit(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
    ) -> TaskRun:
        version = None
        if self.image is image_prefect:
            version = _pinned_prefect()
        start = self._begin(args, kwargs, version, "in the flow job's image")
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
        if start.login:
            values[PREFECT_AUTH_ENV] = start.login
        version_id = _store_credentials(project, profile, values)
        secrets = dict(options.pop("secrets", None) or {})
        secrets.update({item: version_id for item in values})
        env = dict(options.pop("env", None) or {})
        env[PREFECT_URL_ENV] = self.api_url
        # The flow job tells its tasks where the run's folder is.
        env[_task.FOLDER_ENV] = start.folder
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
        run_id = self._flow_run(start, progress)
        flow = {
            **self._flow(start, args, kwargs, run_id),
            "tasks": records,
            "project": project,
            "region": region,
        }
        try:
            run = runner._submit(
                (),
                {"flow": flow},
                progress,
                run_name=start.launched,
                job_name=start.job_name,
                key=f"{start.folder}/{_task.FLOW_FOLDER}",
            )
        except BaseException as exc:
            if run_id is not None:
                _mark_crashed(
                    self.api_url,
                    start.login,
                    run_id,
                    f"The flow job did not start: {exc}",
                )
            raise
        run.inputs = start.parameters
        if run_id is not None:
            run.flow_run_id = run_id
            run.flow_run_url = _ui_url(self.api_url, run_id)
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


class _Start:
    """A run's names and the checked arguments, before it starts."""

    def __init__(
        self,
        *,
        parameters: dict[str, Any],
        login: str | None,
        launched: str,
        job_name: str,
        run_title: str,
        folder: str,
    ) -> None:
        self.parameters = parameters
        self.login = login
        self.launched = launched
        self.job_name = job_name
        self.run_title = run_title
        self.folder = folder


class FlowRun:
    """A ``PrefectFlow`` run in this process (``remote_job=False``).

    ``result()`` waits for the flow and returns the function's result,
    or raises what the flow raised. ``name`` is the UTC launch time,
    and ``title`` the flow run's name in Prefect. ``flow_run_url`` is
    the flow run's page in the Prefect UI, when the run could be
    created at submit. ``output_path`` and ``output_uri`` are the run's
    folder in the bucket, with one folder per task job in it. A flow in
    this process has no job, so ``job`` is ``None``.
    """

    job = None

    def __init__(
        self,
        *,
        name: str,
        title: str,
        inputs: dict[str, Any],
        flow_run_id: str | None,
        flow_run_url: str | None,
        output_path: str,
        output_uri: str,
    ) -> None:
        self.name = name
        self.title = title
        self.inputs = inputs
        self.flow_run_id = flow_run_id
        self.flow_run_url = flow_run_url
        self.output_path = output_path
        self.output_uri = output_uri
        self._login: str | None = None
        self._thread: threading.Thread | None = None
        self._value: Any = None
        self._error: BaseException | None = None

    def __repr__(self) -> str:
        return f"FlowRun({self.title!r}, url={self.flow_run_url!r})"

    def _start(self, work: Callable[[], Any]) -> None:
        def target() -> None:
            try:
                self._value = work()
            except BaseException as exc:
                self._error = exc

        # Not a daemon: the script waits for the flow before it ends.
        self._thread = threading.Thread(
            target=target, name=f"ns-flow-{self.title}"
        )
        self._thread.start()

    def result(self) -> Any:
        """Wait for the flow and return the function's result."""
        if self._thread is not None:
            self._thread.join()
        if self._error is not None:
            raise self._error
        return self._value


def _execute(flow: dict[str, Any], tasks: dict[str, Task]) -> Any:
    """Run a flow function as a Prefect flow, here or in the flow job.

    The tasks become Prefect tasks that keep their files in the run's
    folder. They are bound, with the constants, to the names the
    function uses. When the flow run was created at submit, this run
    becomes it.
    """
    from prefect import (  # type: ignore[import-not-found]
        flow as prefect_flow,
    )

    folder = flow.get("folder")
    scope: dict[str, Any] = {"__name__": "__ns_flow__", **flow["constants"]}
    for alias, item in tasks.items():
        scope[alias] = item.to_prefect(folder=folder)
    exec(compile(flow["source"], "<ns.PrefectFlow source>", "exec"), scope)
    run_title = str(flow["run_title"])
    # A function, so that braces in the name are not read as a template.
    # Prefect renames a run it is given after it, so it is the same.
    wrapped = prefect_flow(
        name=flow["title"], flow_run_name=lambda: run_title, log_prints=True
    )(scope[flow["function"]])
    run_id = flow.get("flow_run_id")
    if not run_id:
        return wrapped(*flow["args"], **flow["kwargs"])

    from uuid import UUID

    from prefect.client.orchestration import (  # type: ignore[import-not-found]
        get_client,
    )
    from prefect.flow_engine import (  # type: ignore[import-not-found]
        run_flow,
    )

    with get_client(sync_client=True) as client:
        flow_run = client.read_flow_run(UUID(str(run_id)))
    return run_flow(wrapped, flow_run=flow_run, parameters=flow["parameters"])


def _run_flow(flow: dict[str, Any]) -> Any:
    """Run a submitted function as a Prefect flow. Runs in the flow job.

    The tasks come back from their records.
    """
    from .session import configure

    # A job has no CLI profile: the project and region were sent along.
    configure(project=flow["project"], region=flow["region"])
    tasks = {
        alias: Task._from_record(record)
        for alias, record in flow["tasks"].items()
    }
    return _execute(flow, tasks)


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
    """The Prefect server's ``user:password``, when one is set here.

    Looked for in ``PREFECT_API_AUTH_STRING``, then in the local Prefect
    settings, then in the file ``~/.config/prefect/auth``.
    """
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
    if not value:
        try:
            value = PREFECT_AUTH_FILE.expanduser().read_text().strip()
        except OSError:
            value = None
    return value if isinstance(value, str) and value else None


def _request(
    api_url: str,
    path: str,
    login: str | None,
    body: object = None,
) -> urllib.request.Request:
    """A request to the Prefect API, with the login when there is one."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        api_url.rstrip("/") + path,
        data=data,
        method="GET" if data is None else "POST",
    )
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if login:
        token = base64.b64encode(login.encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    return request


def _check_prefect(api_url: str, login: str | None) -> str | None:
    """Fail now if the Prefect server will refuse the flow.

    Asks the server for its version with the login the flow would get,
    and returns it. A refusal raises, before any image is built or job
    started. A server that cannot be reached from this machine is not
    an error: the job may still reach it. The version is then None.
    """
    request = _request(api_url, "/admin/version", login)
    try:
        with urllib.request.urlopen(
            request, timeout=_PREFECT_TIMEOUT
        ) as reply:
            try:
                version = json.loads(reply.read())
            except (AttributeError, ValueError):
                return None
            return version if isinstance(version, str) else None
    except urllib.error.HTTPError as exc:
        if exc.code not in (401, 403):
            return None
    except (urllib.error.URLError, OSError, ValueError):
        return None
    where = (
        f"Set {PREFECT_AUTH_ENV} to user:password, or put it on one "
        f"line in {PREFECT_AUTH_FILE}."
    )
    if login:
        raise AccessError(
            f"The Prefect server at {api_url} refused the login.",
            operation=_OP,
            recovery=where,
        )
    raise AccessError(
        f"The Prefect server at {api_url} asks for a login, and none "
        "was found on this machine.",
        operation=_OP,
        recovery=where,
    )


def _version(text: str | None) -> tuple[int, int, int] | None:
    """``major.minor.patch`` of a version, or None when there is none."""
    found = _VERSION.match(text.strip()) if text else None
    if found is None:
        return None
    major, minor, patch = (int(part) for part in found.groups())
    return major, minor, patch


def _pinned_prefect() -> str | None:
    """The Prefect version in the default flow job image."""
    found = re.search(r"prefect==(\S+)", DOCKERFILE)
    return found.group(1) if found else None


def _local_prefect() -> str:
    """The Prefect version installed here. Fails when there is none."""
    try:
        import prefect  # type: ignore[import-not-found]
    except ImportError:
        raise ValidationError(
            "A flow with remote_job=False needs the prefect package on "
            "this machine.",
            operation=_OP,
            recovery="Install it: pip install prefect",
        ) from None
    return str(getattr(prefect, "__version__", ""))


def _check_versions(
    api_url: str, server: str | None, client: str | None, where: str
) -> None:
    """Fail when the server runs an older Prefect than the flow would.

    Nothing is checked when either version is unknown.
    """
    have, need = _version(server), _version(client)
    if have is None or need is None or have >= need:
        return
    raise ValidationError(
        f"The Prefect server at {api_url} runs Prefect {server}, older "
        f"than the Prefect {client} {where}.",
        operation=_OP,
        recovery=(
            f"Upgrade the Prefect server to {client} or later, or run the "
            f"flow with Prefect {server}."
        ),
    )


def _ui_url(api_url: str, run_id: str) -> str:
    """The flow run's page in the Prefect UI."""
    base = os.environ.get(PREFECT_UI_ENV, "").strip()
    if not base:
        base = api_url.rstrip("/")
        if base.endswith("/api"):
            base = base[: -len("/api")]
    return f"{base.rstrip('/')}/runs/flow-run/{run_id}"


def _post(
    api_url: str, path: str, login: str | None, body: object
) -> dict[str, Any]:
    request = _request(api_url, path, login, body)
    with urllib.request.urlopen(request, timeout=_PREFECT_TIMEOUT) as reply:
        loaded = json.loads(reply.read())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} returned {type(loaded).__name__}")
    return loaded


def _create_flow_run(
    api_url: str,
    login: str | None,
    flow_name: str,
    run_title: str,
    parameters: dict[str, Any],
) -> str | None:
    """Create a Pending flow run and return its id.

    The flow is found by its name, or created. Returns None when the
    server cannot be reached or refuses: the flow then creates its run
    when it starts.
    """
    try:
        found = _post(api_url, "/flows/", login, {"name": flow_name})
        created = _post(
            api_url,
            "/flow_runs/",
            login,
            {
                "flow_id": found["id"],
                "name": run_title,
                "parameters": parameters,
                "state": {"type": "PENDING", "name": "Pending"},
            },
        )
        return str(created["id"])
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        return None


def _mark_crashed(
    api_url: str, login: str | None, run_id: str, message: str
) -> None:
    """Mark a flow run Crashed, when its flow job did not start."""
    try:
        _post(
            api_url,
            f"/flow_runs/{run_id}/set_state",
            login,
            {
                "state": {
                    "type": "CRASHED",
                    "name": "Crashed",
                    "message": message[:500],
                },
                "force": True,
            },
        )
    except (urllib.error.URLError, OSError, ValueError):
        pass


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
