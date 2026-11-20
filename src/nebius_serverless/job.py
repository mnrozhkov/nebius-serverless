"""One cloud job, and the collection addressed by id."""

import sys
import time
import uuid
from collections.abc import Callable

from . import poll
from .errors import Error
from .loop import reject_running_loop
from .output import (
    Progress,
    call_progress,
    call_progress_async,
    elapsed_ms,
    log_transport,
    report,
    show_record,
)
from .poll import Timeout, state_of, wait_seconds, wait_until
from .status import Status
from .transport import Transport

_LOG = "nebius_serverless.job"

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
    """One cloud job, addressed by its id (``aijob-...``).

    You get a ``Job`` from ``JobSpec.create()`` or ``jobs.get()``.
    ``get``, ``wait``, and ``cancel`` need that id. Property reads do
    not perform I/O. ``wait`` polls until the state is terminal or the
    local timeout expires. It does not cancel the cloud job.
    """

    def __init__(
        self,
        job_id: str,
        *,
        name: str,
        transport: Transport,
        create_state: str | None = None,
        record: dict[str, object] | None = None,
        resubmit: Resubmit | None = None,
        resubmit_budget: int = 0,
    ) -> None:
        self._id = job_id
        self._name = name
        self._transport = transport
        self._create_state = create_state
        self._record = record
        self._resubmit = resubmit
        self._budget = resubmit_budget if resubmit is not None else 0
        self._base_name = name
        self._attempts = [job_id]

    @property
    def attempts(self) -> list[str]:
        """Every job id this handle created, oldest first.

        More than one when a preempted spot job was created again while
        ``wait()`` was running. ``id`` is the newest.
        """
        return list(self._attempts)

    @property
    def preemptions(self) -> int:
        """How many times ``wait()`` created the job again."""
        return len(self._attempts) - 1

    @property
    def id(self) -> str:
        """Nebius job id. Reading it does not perform I/O."""
        return self._id

    @property
    def name(self) -> str:
        """Display name. Lookup does not use it."""
        return self._name

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

    def get(self, *, quiet: bool = False) -> dict[str, object]:
        """Fetch the job once and return its JSON.

        Prints one snapshot. ``quiet=True`` suppresses it for this
        call only and is not stored on the job.
        """
        reject_running_loop("get_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_get(progress)

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def get_async(self, *, quiet: bool = False) -> dict[str, object]:
        """Await the same fetch as ``get``.

        ``quiet=True`` suppresses progress for this call only.
        """

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
        Progress shows the observation and the outcome. ``quiet=True``
        suppresses it for this call only.
        """
        reject_running_loop("wait_async")

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

        def work(progress: Progress) -> dict[str, object]:
            return self._present_cancel(progress)

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def cancel_async(
        self,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same cancellation request as ``cancel``."""

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
        show_record(
            self._record,
            job_id=self._id,
            name=self._name,
            stream=sys.stdout,
        )

    def _get(self) -> dict[str, object]:
        record = self._transport.get(self._id)
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
                job_id=self._id,
                state=state_of(record),
                attempt=1,
                duration_ms=elapsed_ms(started),
            ),
            progress,
        )
        return record

    def _cancel(self) -> dict[str, object]:
        return self._transport.cancel(self._id)

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
                job_id=self._id,
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
        seconds = wait_seconds(timeout, self._id)
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
                resource_id=self._id,
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
                job_id=self._id,
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


class Jobs:
    """Cloud jobs addressed by a saved id string."""

    def get(
        self,
        job_id: str,
        *,
        profile: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
    ) -> Job:
        """Fetch ``job_id`` once and return a ``Job``. Does not create.

        Prints one snapshot. ``quiet=True`` suppresses it for this call.
        """
        reject_running_loop("get_async")

        def work(progress: Progress) -> Job:
            return self._get(
                job_id,
                profile=profile,
                project=project,
                transport=transport,
                progress=progress,
            )

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    async def get_async(
        self,
        job_id: str,
        *,
        profile: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
    ) -> Job:
        """Await the same fetch as ``get``.

        ``quiet=True`` suppresses progress for this call only.
        """

        def work(progress: Progress) -> Job:
            return self._get(
                job_id,
                profile=profile,
                project=project,
                transport=transport,
                progress=progress,
            )

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
        )

    def _get(
        self,
        job_id: str,
        *,
        profile: str | None,
        project: str | None,
        transport: Transport | None,
        progress: Progress,
    ) -> Job:
        chosen = transport or _sdk_transport(profile, project)
        started = time.perf_counter()
        try:
            record = chosen.get(job_id)
        except Error as exc:
            log_transport(
                progress,
                job_id=job_id,
                attempt=1,
                duration_ms=elapsed_ms(started),
                request_id=exc.request_id,
            )
            raise
        report(
            Status(
                kind="snapshot",
                job_id=job_id,
                state=state_of(record),
                attempt=1,
                duration_ms=elapsed_ms(started),
            ),
            progress,
        )
        name = record_name(record)
        return Job(
            job_id,
            name=name,
            transport=chosen,
            record=record,
        )


jobs = Jobs()


def _sdk_transport(
    profile: str | None,
    project: str | None,
) -> Transport:
    from .sdk_transport import SdkTransport

    return SdkTransport(profile=profile, project=project)


def preempted(record: dict[str, object]) -> bool:
    """Whether a job record ended in ``ERROR`` because of preemption."""
    if state_of(record) != "ERROR":
        return False
    status = record.get("status")
    details = status.get("state_details") if isinstance(status, dict) else None
    code = details.get("code") if isinstance(details, dict) else None
    return isinstance(code, str) and code.lower() == PREEMPTED_CODE.lower()


def resubmit_budget(pricing: str, restart_attempts: int) -> int:
    """How many preemption resubmits ``wait()`` may make."""
    if pricing != "spot" or restart_attempts == 0:
        return 0
    return RESUBMIT_CAP if restart_attempts < 0 else restart_attempts


def record_name(record: dict[str, object]) -> str:
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    name = metadata.get("name")
    if isinstance(name, str):
        return name
    return ""
