"""``PrefectFlow``: run a pipeline of tasks as a Prefect flow."""

import inspect
import os
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ...errors import ValidationError
from ...image import Image
from ...output import Progress, call_progress
from ...recipe import validate_image
from ...task import TaskRun, say
from ...task_driver import Driver, Launch, run_paths
from . import login
from .adapter import run_flow, run_in_job

_LOG = "nebius_serverless.prefect"
_OP = "PrefectFlow"

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

        from nebius_serverless.integrations.prefect import PrefectFlow

        def pipeline(epochs=5):
            data = prepare(dataset="mnist")
            runs = [train.submit(data=data, lr=lr) for lr in RATES]
            return [run.result() for run in runs]

        flow = PrefectFlow(
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
            target = getattr(function, "fn", function)
        for label, value in (("flow_name", flow_name), ("run_name", run_name)):
            given = value is not None or label == "flow_name"
            if given and (not isinstance(value, str) or not value.strip()):
                raise ValidationError(
                    f"{label} must be a nonempty name.", operation=_OP
                )
        api = api_url or os.environ.get(login.PREFECT_URL_ENV)
        if not api:
            raise ValidationError(
                "Pass api_url, the Prefect server's API address, or set "
                f"{login.PREFECT_URL_ENV}.",
                operation=_OP,
            )
        self.image: Image | str
        if image is None:
            self.image = image_prefect
        elif isinstance(image, Image):
            self.image = image
        else:
            self.image = validate_image(image, operation=_OP)
        self.flow_name = flow_name
        self.run_name = run_name if run_name is not None else flow_name
        self.api_url = api
        self.remote_job = bool(remote_job)
        self._driver = Driver(
            target,
            name=flow_name,
            run_name=self.run_name,
            image=self.image,
            job_options=job_options,
            operation=_OP,
        )
        self.function = self._driver.function
        # The plain form names the runs' folder in the bucket.
        self.name = self._driver.name
        self.__signature__ = self._driver.signature
        self.__name__ = self.name
        self.__doc__ = self.function.__doc__

    def __repr__(self) -> str:
        tasks = sorted(self._driver.tasks)
        return f"PrefectFlow({self.flow_name!r}, tasks={tasks})"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run the flow, wait for it, and return the result.

        Interrupting the wait with Ctrl-C cancels the flow job, or
        stops the flow with ``remote_job=False``. The tasks' jobs it
        started are not cancelled.
        """
        if not self.remote_job:
            here = self._here(args, kwargs)
            return self._run_here(here.program, here.login)

        def work(progress: Progress) -> FlowRun:
            return self._submit(args, kwargs, progress)

        run = call_progress(quiet=False, logger_name=_LOG, work=work)
        try:
            return run.result()
        except KeyboardInterrupt:
            run.cancel()
            raise

    def submit(self, *args: Any, **kwargs: Any) -> "FlowRun":
        """Start the flow and return its run at once.

        Creates the flow run in Prefect, builds the images and uploads
        the libraries of the tasks the function uses, when they changed.
        With the flow job it also writes the credentials and starts the
        job. With ``remote_job=False`` the flow runs in a thread. It
        does not wait for the flow. ``result()`` on the run waits and
        returns the function's result.
        """
        if not self.remote_job:
            here = self._here(args, kwargs)
            return self._handle(
                here.launch,
                here.run_id,
                here.paths,
                work=lambda: self._run_here(here.program, here.login),
            )

        def work(progress: Progress) -> FlowRun:
            return self._submit(args, kwargs, progress)

        return call_progress(quiet=False, logger_name=_LOG, work=work)

    def _launch(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        version: str | None,
        where: str,
    ) -> tuple[Launch, str | None]:
        """Check the arguments and the Prefect server, and name the run.

        ``version`` is the Prefect that will run the flow, ``where`` says
        where it is. Fails before anything is built or started. Returns
        the run's names and the server's login.
        """
        launch = self._driver.launch(args, kwargs)
        found = login.find_login()
        server = login.check_server(self.api_url, found)
        login.check_versions(self.api_url, server, version, where)
        return launch, found

    def _flow_run(
        self, launch: Launch, secret: str | None, progress: Progress
    ) -> str | None:
        """Create the flow run in Prefect and log its link."""
        run_id = login.create_flow_run(
            self.api_url,
            secret,
            self.flow_name,
            launch.title,
            launch.parameters,
        )
        if run_id is None:
            say(
                progress,
                f"Could not create the flow run at {self.api_url} from "
                "here. The flow creates it when it starts.",
            )
        else:
            say(
                progress,
                f"Prefect flow run: {login.ui_url(self.api_url, run_id)}",
            )
        return run_id

    def _program(
        self,
        launch: Launch,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        run_id: str | None,
    ) -> dict[str, Any]:
        """What runs the flow: the same in the job and here."""
        return self._driver.program(
            launch,
            args,
            kwargs,
            title=self.flow_name,
            run_title=launch.title,
            flow_run_id=run_id,
        )

    def _handle(
        self,
        launch: Launch,
        run_id: str | None,
        paths: tuple[str, str],
        *,
        job_run: TaskRun | None = None,
        work: Callable[[], Any] | None = None,
    ) -> "FlowRun":
        return FlowRun(
            name=launch.stamp,
            title=launch.title,
            inputs=launch.parameters,
            flow_run_id=run_id,
            flow_run_url=login.ui_url(self.api_url, run_id)
            if run_id
            else None,
            output_path=paths[0],
            output_uri=paths[1],
            job_run=job_run,
            work=work,
        )

    def _here(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> "_Here":
        def prepare(progress: Progress) -> _Here:
            return self._prepare_here(args, kwargs, progress)

        return call_progress(quiet=False, logger_name=_LOG, work=prepare)

    def _prepare_here(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
    ) -> "_Here":
        """Get a flow ready to run in this process."""
        version = login.local_version()
        launch, secret = self._launch(args, kwargs, version, "installed here")
        bucket = self._driver.prepare_here(progress)
        run_id = self._flow_run(launch, secret, progress)
        return _Here(
            launch=launch,
            run_id=run_id,
            paths=run_paths(launch.folder, bucket),
            program=self._program(launch, args, kwargs, run_id),
            login=secret,
        )

    def _run_here(self, program: dict[str, Any], secret: str | None) -> Any:
        """Run the flow in this process, with this flow's server."""
        from prefect.settings import (  # type: ignore[import-not-found]
            PREFECT_API_AUTH_STRING,
            PREFECT_API_URL,
            temporary_settings,
        )

        updates: dict[Any, Any] = {PREFECT_API_URL: self.api_url}
        if secret:
            updates[PREFECT_API_AUTH_STRING] = secret
        with temporary_settings(updates=updates):
            return run_flow(program, self._driver.tasks)

    def _submit(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Progress,
    ) -> "FlowRun":
        version = None
        if self.image is image_prefect:
            version = _pinned_version()
        launch, secret = self._launch(
            args, kwargs, version, "in the flow job's image"
        )
        run_id = self._flow_run(launch, secret, progress)
        try:
            run = self._driver.submit(
                launch,
                self._program(launch, args, kwargs, run_id),
                progress,
                entrypoint=run_in_job,
                env={login.PREFECT_URL_ENV: self.api_url},
                secrets={login.PREFECT_AUTH_ENV: secret} if secret else None,
            )
        except BaseException as exc:
            if run_id is not None:
                login.mark_crashed(
                    self.api_url,
                    secret,
                    run_id,
                    f"The flow job did not start: {exc}",
                )
            raise
        bucket, _ = self._driver.bucket(progress)
        paths = run_paths(launch.folder, bucket)
        return self._handle(launch, run_id, paths, job_run=run)


@dataclass(frozen=True)
class _Here:
    """A flow ready to run in this process."""

    launch: Launch
    run_id: str | None
    paths: tuple[str, str]
    program: dict[str, Any]
    login: str | None


class FlowRun:
    """One run of a ``PrefectFlow``.

    ``result()`` waits for the flow and returns the function's result,
    or raises what the flow raised. ``name`` is the UTC launch time,
    and ``title`` the flow run's name in Prefect. ``flow_run_url`` is
    the flow run's page in the Prefect UI, when the run could be
    created at submit. ``output_path`` and ``output_uri`` are the run's
    folder in the bucket, with one folder per task job in it, and
    ``flow/`` for the flow job. ``job`` is the flow job, or ``None``
    for a flow in this process.
    """

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
        job_run: TaskRun | None = None,
        work: Callable[[], Any] | None = None,
    ) -> None:
        self.name = name
        self.title = title
        self.inputs = inputs
        self.flow_run_id = flow_run_id
        self.flow_run_url = flow_run_url
        self.output_path = output_path
        self.output_uri = output_uri
        self.job_run = job_run
        self._thread: threading.Thread | None = None
        self._value: Any = None
        self._error: BaseException | None = None
        if work is not None:
            self._start(work)

    @property
    def job(self) -> Any:
        """The flow job, or ``None`` for a flow in this process."""
        return self.job_run.job if self.job_run is not None else None

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
        if self.job_run is not None:
            return self.job_run.result()
        if self._thread is not None:
            self._thread.join()
        if self._error is not None:
            raise self._error
        return self._value

    def cancel(self) -> None:
        """Cancel the flow job. A flow in this process is not stopped."""
        if self.job_run is not None:
            self.job_run.cancel()


def _pinned_version() -> str | None:
    """The Prefect version in the default flow job image."""
    found = re.search(r"prefect==(\S+)", DOCKERFILE)
    return found.group(1) if found else None
