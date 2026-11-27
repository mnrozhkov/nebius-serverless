"""Endpoints: the spec, the endpoint, and lookup by id.

``ns.endpoints.get(id)`` reloads an endpoint from a saved id.
``ns.EndpointSpec`` and ``ns.Endpoint`` are the supported names; the
modules in this package are implementation.
"""

import time

from ..core.loop import reject_running_loop
from ..core.poll import state_of
from ..core.records import record_name
from ..core.transport import EndpointTransport
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
from .endpoint import _LOG, _NOUN, Endpoint, _sdk_transport

__all__ = ["get", "get_async"]


def get(
    endpoint_id: str,
    *,
    profile: str | None = None,
    project: str | None = None,
    transport: EndpointTransport | None = None,
    quiet: bool = False,
) -> Endpoint:
    """Fetch ``endpoint_id`` once and return a ``Endpoint``.

    Does not create. ``token`` on the result is ``None``.
    """
    reject_running_loop("endpoints.get_async")

    def work(progress: Progress) -> Endpoint:
        return _get(
            endpoint_id,
            profile=profile,
            project=project,
            transport=transport,
            progress=progress,
        )

    return call_progress(
        quiet=quiet,
        logger_name=_LOG,
        work=work,
        noun="endpoint",
    )


async def get_async(
    endpoint_id: str,
    *,
    profile: str | None = None,
    project: str | None = None,
    transport: EndpointTransport | None = None,
    quiet: bool = False,
) -> Endpoint:
    """Await the same fetch as ``get``."""

    def work(progress: Progress) -> Endpoint:
        return _get(
            endpoint_id,
            profile=profile,
            project=project,
            transport=transport,
            progress=progress,
        )

    return await call_progress_async(
        quiet=quiet,
        logger_name=_LOG,
        work=work,
        noun="endpoint",
    )


def _get(
    endpoint_id: str,
    *,
    profile: str | None,
    project: str | None,
    transport: EndpointTransport | None,
    progress: Progress,
) -> Endpoint:
    chosen = transport or _sdk_transport(profile, project)
    started = time.perf_counter()
    try:
        record = chosen.get(endpoint_id)
    except Error as exc:
        log_transport(
            progress,
            job_id=endpoint_id,
            attempt=1,
            duration_ms=elapsed_ms(started),
            request_id=exc.request_id,
        )
        raise
    report(
        Status(
            kind="snapshot",
            job_id=endpoint_id,
            state=state_of(record),
            attempt=1,
            duration_ms=elapsed_ms(started),
            noun=_NOUN,
        ),
        progress,
    )
    return Endpoint._attach(
        endpoint_id,
        name=record_name(record),
        transport=chosen,
        record=record,
    )
