"""``Job``: one job on Nebius, created once from a ``JobSpec``."""

import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, overload

from ..core import poll
from ..core.loop import reject_running_loop
from ..core.poll import Timeout, state_of, wait_seconds, wait_until
from ..core.transport import (
    Transport,
    call_dry_run,
    call_transport,
    validate_idempotency_key,
)
from ..errors import Error, ValidationError
from ..ui.output import (
    Progress,
    call_progress,
    call_progress_async,
    elapsed_ms,
    log_transport,
    report,
    show_record,
)
from ..ui.status import Status
from . import request
from .spec import Duration, JobSpec

_LOG = "nebius_serverless.job"
_OP = "Job"

# Creates the job again under a new name and key; returns (id, state).
Resubmit = Callable[[str, str, Progress], tuple[str, str | None]]

# state_details.code of a preempted job: the same code Nebius uses for
# a preempted endpoint.
PREEMPTED_CODE = "Preempted"
# Resubmits allowed by restart_attempts=-1.
RESUBMIT_CAP = 10

TERMINAL_STATES = frozenset(
    {"COMPLETED", "FAILED", "CANCELLED", "ERROR", "DELETING"}
)
POLL_SECONDS = 10.0


class Job:
    """One job on Nebius: built from a spec, created once, then waited on.

    ``ns.Job(spec)`` or ``ns.Job(command, image=..., ...)`` with the
    ``JobSpec`` fields makes the object. Nothing contacts Nebius until
    ``create()``, which sends the job once and sets ``id``. ``get``,
    ``wait``, ``cancel`` and ``display`` need a created job.

    ``ns.jobs.get(id)`` returns a ``Job`` for a job that already exists.
    Its ``spec`` is not known, so ``spec``, ``dry_run``, ``to_cli`` and
    ``create`` raise on it. Use ``record``.

    Property reads do not perform I/O. ``wait`` polls until the state is
    terminal or the local timeout expires. It does not cancel the job.

    Example::

        import nebius_serverless as ns

        job = ns.Job(
            ["nvidia-smi"],
            image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
            gpu="h100",
            timeout="10m",
        )
        job.dry_run()  # the request as data; nothing is created
        job.create()   # once; sets job.id
        job.wait()
        job.display()
    """

    @overload
    def __init__(self, spec: JobSpec, /) -> None: ...

    @overload
    def __init__(
        self,
        command: Sequence[str],
        /,
        *,
        image: str,
        timeout: Duration,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        pricing: str = "on-demand",
        restart_attempts: int = 0,
        preemption_retries: int | None = None,
        volumes: Sequence[str] | None = None,
        inject_files: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        name: str | None = None,
        disk: str | None = None,
        shm: str | None = None,
        working_dir: str | None = None,
        registry_secret: str | None = None,
        spot_policy: str | None = None,
        subnet_id: str | None = None,
        profile: str | None = None,
        project: str | None = None,
    ) -> None: ...

    def __init__(
        self, spec: JobSpec | Sequence[str], /, **fields: Any
    ) -> None:
        if isinstance(spec, JobSpec):
            if fields:
                raise ValidationError(
                    "Pass a JobSpec or the job's fields, not both.",
                    operation=_OP,
                    recovery="Use spec.clone(...) to change a spec.",
                )
            self._spec: JobSpec | None = spec
        else:
            self._spec = JobSpec(command=spec, **fields)
        self._id: str | None = None
        self._name: str | None = self._spec.name
        self._transport: Transport | None = None
        self._create_state: str | None = None
        self._record: dict[str, object] | None = None
        self._resubmit: Resubmit | None = None
        self._budget = 0
        self._base_name = ""
        self._attempts: list[str] = []
        self._lock = threading.Lock()

    @classmethod
    def _attach(
        cls,
        job_id: str,
        *,
        name: str,
        transport: Transport,
        record: dict[str, object] | None = None,
    ) -> "Job":
        """A handle to a job that exists. Its spec is not known."""
        job = cls.__new__(cls)
        job._spec = None
        job._id = job_id
        job._name = name
        job._transport = transport
        job._create_state = None
        job._record = record
        job._resubmit = None
        job._budget = 0
        job._base_name = name
        job._attempts = [job_id]
        job._lock = threading.Lock()
        return job

    def __repr__(self) -> str:
        return (
            f"Job(id={self._id!r}, name={self._name!r}, "
            f"status={self.status!r})"
        )

    @property
    def spec(self) -> JobSpec:
        """The spec this job was built from.

        A job loaded with ``jobs.get`` has none: the original arguments
        are not known. Its ``record`` holds what Nebius stored.
        """
        if self._spec is None:
            raise ValidationError(
                f"Job {self._id} was loaded with jobs.get, so its spec "
                "is not known.",
                operation="spec",
                resource_id=self._id,
                recovery="Read job.record for what Nebius stored.",
            )
        return self._spec

    @property
    def id(self) -> str | None:
        """Nebius job id, or ``None`` before ``create()``.

        After a preemption resubmit, the id of the newest attempt.
        """
        return self._id

    @property
    def name(self) -> str | None:
        """Display name. Set by ``create()`` when the spec has none."""
        return self._name

    @property
    def attempts(self) -> list[str]:
        """Every job id this handle created, oldest first.

        Empty before ``create()``. More than one when a preempted spot
        job was created again while ``wait()`` was running. ``id`` is
        the newest.
        """
        return list(self._attempts)

    @property
    def preemptions(self) -> int:
        """How many times ``wait()`` created the job again."""
        return max(0, len(self._attempts) - 1)

    @property
    def record(self) -> dict[str, object] | None:
        """Last fetched JSON, or None before the first successful fetch."""
        return self._record

    @property
    def status(self) -> str | None:
        """Raw state from the last fetch, else the create response."""
        if self._record is not None:
            return state_of(self._record)
        return self._create_state

    def dry_run(
        self,
        *,
        server: bool = False,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
    ) -> dict[str, object]:
        """The create request as data. Creates nothing.

        ``server=False`` (default) builds it here, with env values left
        out. It resolves the project and region, and makes no create
        call. ``server=True`` also sends the request to Nebius with
        ``dry_run`` set: Nebius validates it and creates nothing. Its
        answer is under ``"server"``. A rejection raises what
        ``create()`` would raise.
        """
        spec = self.spec

        def work(progress: Progress) -> dict[str, object]:
            where = request.plan(spec, region=region, project=project)
            name = spec.name or request.name_from_key(str(uuid.uuid4()))
            description = request.spec_description(spec, where, name)
            if server:
                chosen = transport or _sdk_transport(
                    spec.profile, where.project
                )
                body = request.spec_body(spec, where, name)
                description["server"] = call_dry_run(chosen, body)
            return description

        return call_progress(quiet=True, logger_name=_LOG, work=work)

    def to_cli(self, *, project: str | None = None) -> str:
        """The equivalent ``nebius ai job create`` command.

        Does not contact Nebius. Without ``name=``, a fresh name is
        generated, as ``create()`` does. ``project`` (or the spec's)
        becomes ``--parent-id``; without one, the CLI uses its profile's
        project. Env values print as ``KEY="$KEY"`` placeholders and
        secrets by id; a token is never printed.
        """
        from .cli_command import job_command

        spec = self.spec
        name = spec.name or request.name_from_key(str(uuid.uuid4()))
        return job_command(spec, name=name, project=project)

    def create(
        self,
        *,
        region: str | None = None,
        project: str | None = None,
        idempotency_key: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> "Job":
        """Create the job on Nebius, once. Returns this ``Job``.

        Sets ``id`` once Nebius has accepted the job, and does not wait
        for the container. A second call raises: build a new ``Job`` for
        another job. With an ``idempotency_key`` Nebius already knows,
        this object attaches to the job that key made. Region comes from
        ``region=`` or the profile when either names one, otherwise from
        the project's ``spec.region``. Prints one line once the id is
        known. ``quiet=True`` suppresses it for this call only.
        """
        reject_running_loop("create_async")

        def work(progress: Progress) -> Job:
            return self._create(
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def create_async(
        self,
        *,
        region: str | None = None,
        project: str | None = None,
        idempotency_key: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> "Job":
        """Await the same create as ``create``.

        Cancelling this await does not cancel the cloud job.
        ``quiet=True`` suppresses progress for this call only.
        """

        def work(progress: Progress) -> Job:
            return self._create(
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
        )

    def get(self, *, quiet: bool = False) -> dict[str, object]:
        """Fetch the job once and return its JSON.

        Prints one snapshot. ``quiet=True`` suppresses it for this
        call only and is not stored on the job.
        """
        reject_running_loop("get_async")
        self._created("get")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_get(progress)

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def get_async(self, *, quiet: bool = False) -> dict[str, object]:
        """Await the same fetch as ``get``.

        ``quiet=True`` suppresses progress for this call only.
        """
        self._created("get")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_get(progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
        )

    def wait(
        self,
        timeout: Timeout = None,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Poll ``get`` until the state is terminal.

        ``timeout=0`` is one fetch. A local timeout leaves the cloud
        job running. Failed and cancelled states are returned as data.
        A preempted spot job is created again, up to the spec's
        ``preemption_retries``. Progress shows the observation and the
        outcome. ``quiet=True`` suppresses it for this call only.
        """
        reject_running_loop("wait_async")
        self._created("wait")

        def work(progress: Progress) -> dict[str, object]:
            return self._wait(timeout, progress)

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def wait_async(
        self,
        timeout: Timeout = None,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same poll as ``wait``.

        Cancelling this await does not cancel the cloud job. It closes
        the progress handle, so the worker prints nothing further and
        stops polling once a ``get`` already in flight returns.
        """
        self._created("wait")

        def work(progress: Progress) -> dict[str, object]:
            return self._wait(timeout, progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
        )

    def cancel(self, *, quiet: bool = False) -> dict[str, object]:
        """Request cancellation and return when it is accepted.

        The job is usually ``CANCELLING`` at that point. The payload
        does not claim the container has stopped; poll ``get`` for
        ``CANCELLED``.
        ``quiet=True`` suppresses the acceptance line for this call.
        """
        reject_running_loop("cancel_async")
        self._created("cancel")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_cancel(progress)

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def cancel_async(
        self,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same cancellation request as ``cancel``."""
        self._created("cancel")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_cancel(progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
        )

    def display(self) -> None:
        """Print the last fetched record.

        Does not fetch. Before the first successful ``get`` or ``wait``,
        prints the id and name. Color follows the terminal and
        ``NO_COLOR``.
        """
        job_id, _transport = self._created("display")
        show_record(
            self._record,
            job_id=job_id,
            name=self._name or "",
            stream=sys.stdout,
        )

    @property
    def _known_id(self) -> str:
        """The id of a created resource."""
        return self._created("get")[0]

    def _created(self, operation: str) -> tuple[str, Transport]:
        """The id and transport. Raises before ``create()``."""
        if self._id is None or self._transport is None:
            raise ValidationError(
                "This job is not created yet. Call create() first.",
                operation=operation,
                recovery=f"Call job.create(), then job.{operation}().",
            )
        return self._id, self._transport

    def _create(
        self,
        *,
        region: str | None,
        project: str | None,
        transport: Transport | None,
        progress: Progress,
        idempotency_key: str | None,
    ) -> "Job":
        """Send the create. Runs once per object."""
        spec = self.spec
        with self._lock:
            if self._id is not None:
                raise ValidationError(
                    f"Job {self._id} is already created. A Job is created "
                    "once.",
                    operation="create",
                    resource_id=self._id,
                    recovery="Build a new ns.Job(spec) for another job.",
                )
            where = request.plan(spec, region=region, project=project)
            key = validate_idempotency_key(idempotency_key)
            name = spec.name or request.name_from_key(key)
            chosen = transport or _sdk_transport(
                spec.profile, where.project
            )

            def submit(
                attempt_name: str,
                attempt_key: str,
                handle: Progress,
            ) -> tuple[str, str | None]:
                body = request.spec_body(spec, where, attempt_name)
                return _send(chosen, body, attempt_name, attempt_key, handle)

            job_id, state = submit(name, key, progress)
            budget = resubmit_budget(
                spec.pricing, spec.restart_attempts, spec.preemption_retries
            )
            self._id = job_id
            self._name = name
            self._base_name = name
            self._transport = chosen
            self._create_state = state
            self._resubmit = submit if budget else None
            self._budget = budget
            self._attempts = [job_id]
        return self

    def _get(self) -> dict[str, object]:
        job_id, transport = self._created("get")
        record = transport.get(job_id)
        self._record = record
        return record

    def _present_get(self, progress: Progress) -> dict[str, object]:
        started = time.perf_counter()
        try:
            record = self._get()
        except Error as exc:
            log_transport(
                progress,
                job_id=self._id,
                attempt=1,
                duration_ms=elapsed_ms(started),
                request_id=exc.request_id,
            )
            raise
        report(
            Status(
                kind="snapshot",
                job_id=self._known_id,
                state=state_of(record),
                attempt=1,
                duration_ms=elapsed_ms(started),
            ),
            progress,
        )
        return record

    def _cancel(self) -> dict[str, object]:
        job_id, transport = self._created("cancel")
        return transport.cancel(job_id)

    def _present_cancel(self, progress: Progress) -> dict[str, object]:
        started = time.perf_counter()
        try:
            payload = self._cancel()
        except Error as exc:
            log_transport(
                progress,
                job_id=self._id,
                attempt=1,
                duration_ms=elapsed_ms(started),
                request_id=exc.request_id,
            )
            raise
        duration_ms = elapsed_ms(started)
        log_transport(
            progress,
            job_id=self._id,
            attempt=1,
            duration_ms=duration_ms,
        )
        report(
            Status(
                kind="cancel_accepted",
                job_id=self._known_id,
                duration_ms=duration_ms,
            ),
            progress,
        )
        return payload

    def _wait(
        self,
        timeout: Timeout,
        progress: Progress,
    ) -> dict[str, object]:
        job_id, _transport = self._created("wait")
        seconds = wait_seconds(timeout, job_id)
        # One deadline across resubmits; the first wait is unchanged.
        tracked = seconds is not None and self._resubmit is not None
        started = poll.monotonic() if tracked else 0.0
        left: Timeout = timeout
        while True:
            record = wait_until(
                self._get,
                lambda record: state_of(record) in TERMINAL_STATES,
                timeout=left,
                progress=progress,
                resource_id=self._known_id,
                noun="job",
                poll_seconds=POLL_SECONDS,
            )
            if not self._resubmits(record):
                return record
            self._submit_again(progress)
            if seconds is not None:
                left = max(0.0, seconds - (poll.monotonic() - started))

    def _resubmits(self, record: dict[str, object]) -> bool:
        return (
            self._resubmit is not None
            and preempted(record)
            and self.preemptions < self._budget
        )

    def _submit_again(self, progress: Progress) -> None:
        assert self._resubmit is not None
        attempt = len(self._attempts) + 1
        report(
            Status(
                kind="resubmitted",
                job_id=self._known_id,
                message=(
                    f"preempted; resubmitting ({attempt - 1}/{self._budget})"
                ),
            ),
            progress,
        )
        name = f"{self._base_name}-r{attempt}"
        job_id, state = self._resubmit(name, str(uuid.uuid4()), progress)
        self._attempts.append(job_id)
        self._id = job_id
        self._name = name
        self._record = None
        self._create_state = state


def _send(
    transport: Transport,
    body: dict[str, object],
    name: str,
    key: str,
    progress: Progress,
) -> tuple[str, str | None]:
    """One create, with its progress line. Returns the id and state."""
    started = time.perf_counter()
    try:
        result = call_transport(transport, body, idempotency_key=key)
    except Error as exc:
        log_transport(
            progress,
            job_id=None,
            attempt=1,
            duration_ms=elapsed_ms(started),
            request_id=exc.request_id,
        )
        raise
    duration_ms = elapsed_ms(started)
    log_transport(
        progress,
        job_id=result.resource_id,
        attempt=1,
        duration_ms=duration_ms,
        request_id=result.request_id,
    )
    report(
        Status(
            kind="submitted",
            job_id=result.resource_id,
            name=name,
            state=result.state,
            duration_ms=duration_ms,
            request_id=result.request_id,
        ),
        progress,
    )
    return result.resource_id, result.state


def _sdk_transport(
    profile: str | None,
    project: str | None,
) -> Transport:
    from ..core.sdk_transport import SdkTransport

    return SdkTransport(profile=profile, project=project)


def preempted(record: dict[str, object]) -> bool:
    """Whether a job record ended in ``ERROR`` because of preemption."""
    if state_of(record) != "ERROR":
        return False
    status = record.get("status")
    details = status.get("state_details") if isinstance(status, dict) else None
    code = details.get("code") if isinstance(details, dict) else None
    return isinstance(code, str) and code.lower() == PREEMPTED_CODE.lower()


def resubmit_budget(
    pricing: str,
    restart_attempts: int,
    preemption_retries: int | None = None,
) -> int:
    """How many preemption resubmits ``wait()`` may make.

    Only spot jobs are resubmitted. ``preemption_retries`` sets the
    number when given. Without it, the number comes from
    ``restart_attempts``, with ``-1`` meaning ``RESUBMIT_CAP``.
    """
    if pricing != "spot":
        return 0
    if preemption_retries is not None:
        return preemption_retries
    if restart_attempts == 0:
        return 0
    return RESUBMIT_CAP if restart_attempts < 0 else restart_attempts
