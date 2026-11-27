"""Jobs: the recipe, the cloud job, and lookup by id.

``ns.jobs.get(id)`` reloads a job from a saved id. ``ns.JobSpec`` and
``ns.Job`` are the supported names; the modules in this package are
implementation.
"""

import time

from ..core.loop import reject_running_loop
from ..core.poll import state_of
from ..core.records import record_name
from ..core.transport import Transport
from ..errors import Error
from ..ui.output import (
    Progress,
    call_progress,
    call_progress_async,
    elapsed_ms,
    log_transport,
    report,
)
from ..ui.status import Status
from .job import _LOG, Job, _sdk_transport

__all__ = ["get", "get_async"]


def get(
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
        return _get(
            job_id,
            profile=profile,
            project=project,
            transport=transport,
            progress=progress,
        )

    return call_progress(quiet=quiet, logger_name=_LOG, work=work)


async def get_async(
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
        return _get(
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
