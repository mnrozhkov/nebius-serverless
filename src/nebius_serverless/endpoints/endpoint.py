"""One cloud endpoint, and the collection addressed by id."""

import asyncio
import sys
import time
from typing import Literal

from ..core import poll
from ..core.loop import reject_running_loop
from ..core.poll import Timeout, state_of, wait_seconds, wait_until
from ..core.transport import EndpointTransport
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
from ..ui.status import Kind, Status

_LOG = "nebius_serverless.endpoint"
_NOUN = "endpoint"

READY_STATE = "RUNNING"
# A ready-wait ends on these too: the endpoint will not become ready.
NOT_READY_STATES = frozenset({"ERROR", "STOPPED", "DELETING"})
# A stopped-wait ends on these.
STOPPED_TARGETS = frozenset({"STOPPED", "ERROR", "DELETING"})
WAIT_TARGETS = ("ready", "stopped")
POLL_SECONDS = 5.0
# Covers Nebius's 30-minute capacity search, about 5 minutes of
# creation, and an image pull with model load. The boto3 SageMaker
# endpoint waiter also stops at 60 minutes.
WAIT_SECONDS = 3600.0


def open_url(request: object, timeout: float) -> object:
    """``urllib.request.urlopen``; tests replace it."""
    import urllib.request

    assert isinstance(request, urllib.request.Request)
    return urllib.request.urlopen(request, timeout=timeout)


def http_status(url: str, token: str | None) -> int | None:
    """GET ``url``; the HTTP status, or ``None`` without a connection.

    The token goes only into the ``Authorization`` header.
    """
    import urllib.error
    import urllib.request

    headers = {} if token is None else {"Authorization": f"Bearer {token}"}
    request = urllib.request.Request(url, headers=headers)
    try:
        with open_url(request, 10.0) as response:  # type: ignore[attr-defined]
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except (urllib.error.URLError, TimeoutError, OSError):
        return None


def urls_of(record: dict[str, object] | None) -> list[str]:
    """Every entry in ``status.public_endpoints``, in API order."""
    if record is None:
        return []
    status = record.get("status")
    if not isinstance(status, dict):
        return []
    items = status.get("public_endpoints")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, str)]


def url_of(record: dict[str, object] | None) -> str | None:
    """The first ``https://`` entry in ``status.public_endpoints``."""
    for item in urls_of(record):
        if item.startswith("https://"):
            return item
    return None


def _protocols_of(record: dict[str, object]) -> set[str] | None:
    """Protocols in ``spec.ports``; ``None`` when the record has none."""
    spec = record.get("spec")
    ports = spec.get("ports") if isinstance(spec, dict) else None
    if not isinstance(ports, list) or not ports:
        return None
    return {
        str(item.get("protocol") or "HTTP").upper()
        for item in ports
        if isinstance(item, dict)
    }


def has_ready_urls(record: dict[str, object]) -> bool:
    """A managed URL exists for each protocol that gets one.

    HTTP ports get ``https://`` and TCP ports ``tls://``; UDP gets no
    managed URL. Without port info, an ``https://`` URL is required.
    """
    urls = urls_of(record)
    protocols = _protocols_of(record) or {"HTTP"}
    if "HTTP" in protocols and not any(u.startswith("https://") for u in urls):
        return False
    if "TCP" in protocols and not any(u.startswith("tls://") for u in urls):
        return False
    return True


class Endpoint:
    """One cloud endpoint, addressed by its id (``aiendpoint-...``).

    You get an ``Endpoint`` from ``EndpointSpec.create()`` or
    ``endpoints.get()``. Property reads do not perform I/O. ``wait``
    polls until the endpoint is ready or will not become ready. Nothing
    here stops the endpoint except ``stop``, and nothing deletes it
    except ``delete``.
    """

    def __init__(
        self,
        endpoint_id: str,
        *,
        name: str,
        transport: EndpointTransport,
        create_state: str | None = None,
        record: dict[str, object] | None = None,
        token: str | None = None,
    ) -> None:
        self._id = endpoint_id
        self._name = name
        self._transport = transport
        self._create_state = create_state
        self._record = record
        self._token = token
        self._url = url_of(record)
        # Set by start(): the next fetches may still show STOPPED.
        self._starting = False

    def __repr__(self) -> str:
        return (
            f"Endpoint(id={self._id!r}, name={self._name!r}, "
            f"status={self.status!r}, url={self._url!r})"
        )

    @property
    def id(self) -> str:
        """Nebius endpoint id. Reading it does not perform I/O."""
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

    @property
    def url(self) -> str | None:
        """Managed ``https://`` URL, or None until a fetch has seen one."""
        return self._url

    @property
    def urls(self) -> list[str]:
        """Every managed URL from the last fetch, in API order.

        ``https://`` for HTTP ports and ``tls://`` for TCP ports.
        """
        return urls_of(self._record)

    @property
    def token(self) -> str | None:
        """Bearer token from this create call, or from ``reveal_token``.

        ``None`` when ``auth='none'``, with ``token_secret``, and on an
        endpoint loaded by ``endpoints.get`` until ``reveal_token()``.
        """
        return self._token

    def reveal_token(self) -> str | None:
        """Fetch the endpoint's bearer token from Nebius and return it.

        Nebius returns ``spec.auth_token`` to anyone allowed to read the
        endpoint, so a handle from ``endpoints.get`` can recover the
        token after a restart. Also sets ``token``. ``None`` when the
        endpoint is public or reads its token from MysteryBox
        (``token_secret``). One fetch; the value is never logged or
        kept in ``record``.
        """
        reject_running_loop("reveal_token_async")
        return self._reveal()

    async def reveal_token_async(self) -> str | None:
        """Await the same fetch as ``reveal_token``."""
        return await asyncio.to_thread(self._reveal)

    def _reveal(self) -> str | None:
        token = self._transport.reveal_token(self._id)
        if token is not None:
            self._token = token
        return token

    def get(self, *, quiet: bool = False) -> dict[str, object]:
        """Fetch the endpoint once and return its JSON.

        ``ERROR`` and ``STOPPED`` are data. ``quiet=True`` suppresses
        the snapshot line for this call only.
        """
        reject_running_loop("get_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_get(progress)

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    async def get_async(self, *, quiet: bool = False) -> dict[str, object]:
        """Await the same fetch as ``get``."""

        def work(progress: Progress) -> dict[str, object]:
            return self._present_get(progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def wait(
        self,
        timeout: Timeout = WAIT_SECONDS,
        *,
        until: Literal["ready", "stopped"] = "ready",
        probe: str | None = None,
        token: str | None = None,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Poll ``get`` until the endpoint reaches ``until``.

        ``until="ready"`` (default) waits for ``RUNNING`` with a managed
        URL. ``ERROR``, ``STOPPED`` and ``DELETING`` end it as data.
        After ``start()`` on this handle, the ``STOPPED`` left over from
        before the start is skipped. This does not probe the
        application's own route.

        ``until="stopped"`` waits for ``STOPPED``, for example after
        ``stop()``. ``ERROR`` and ``DELETING`` end it as data.

        ``probe="/v1/models"`` then GETs ``url + probe`` until it answers
        2xx, within the same deadline, sending the bearer token when this
        handle knows it (or ``token=``). Connection errors, 404 and 5xx
        keep polling; 401 and 403 raise ``AccessError`` at once.

        ``timeout`` defaults to 3600 seconds. Then ``WaitTimeout`` is
        raised, naming the last state; the endpoint is not stopped and
        may still be billing. ``timeout=None`` waits without a limit,
        and ``timeout=0`` is one fetch. Job ``wait`` has no default
        limit because the job's own server-side timeout ends it; an
        endpoint has no such deadline.
        """
        reject_running_loop("wait_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._wait(timeout, until, probe, token, progress)

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    async def wait_async(
        self,
        timeout: Timeout = WAIT_SECONDS,
        *,
        until: Literal["ready", "stopped"] = "ready",
        probe: str | None = None,
        token: str | None = None,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same poll as ``wait``, with the same 3600 s default.

        Cancelling this await does not delete the endpoint. The worker
        stops polling once a ``get`` already in flight returns.
        """

        def work(progress: Progress) -> dict[str, object]:
            return self._wait(timeout, until, probe, token, progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def stop(self, *, quiet: bool = False) -> dict[str, object]:
        """Request a stop and return when the request is accepted.

        The endpoint keeps its id and configuration; ``start`` runs it
        again and only ``delete`` removes it. A stopped endpoint is not
        billed for compute or its disk, but mounted volumes still bill
        (docs.nebius.com/serverless/pricing-quotas).

        The state passes through ``STOPPING`` to ``STOPPED``. Call
        ``wait(until="stopped")`` to block until then; a plain ``wait()``
        can return on the ``RUNNING`` left over from before the stop.
        The payload does not claim it has stopped.
        """
        reject_running_loop("stop_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("stop", progress)

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    async def stop_async(
        self,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same stop request as ``stop``."""

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("stop", progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def start(self, *, quiet: bool = False) -> dict[str, object]:
        """Request a start of a stopped endpoint; return when accepted.

        The endpoint comes back at the same URL (the boot disk is not
        preserved). A preempted spot endpoint is ``STOPPED`` with code
        ``Preempted``; ``start`` resumes it. Call ``wait()`` to block
        until it is ``RUNNING`` with its URL again.
        """
        reject_running_loop("start_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("start", progress)

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    async def start_async(
        self,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same start request as ``start``."""

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("start", progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def delete(self, *, quiet: bool = False) -> dict[str, object]:
        """Request deletion and return when the request is accepted.

        The payload does not claim the machine has stopped or that
        billing has ended.
        """
        reject_running_loop("delete_async")

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("delete", progress)

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    async def delete_async(
        self,
        *,
        quiet: bool = False,
    ) -> dict[str, object]:
        """Await the same deletion request as ``delete``."""

        def work(progress: Progress) -> dict[str, object]:
            return self._present_accepted("delete", progress)

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def display(self) -> None:
        """Print the last fetched record, including the URL.

        Does not fetch. Before the first successful ``get`` or ``wait``,
        prints the id and name.
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
        # The URL always reflects the latest fetch, including none.
        self._url = url_of(record)
        if self._starting and state_of(record) != "STOPPED":
            self._starting = False
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
                noun=_NOUN,
            ),
            progress,
        )
        return record

    def _present_accepted(
        self,
        verb: Literal["stop", "start", "delete"],
        progress: Progress,
    ) -> dict[str, object]:
        started = time.perf_counter()
        transport = self._transport
        calls = {
            "stop": transport.stop,
            "start": transport.start,
            "delete": transport.delete,
        }
        call = calls[verb]
        try:
            payload = call(self._id)
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
        if verb == "start":
            self._starting = True
        log_transport(
            progress,
            job_id=self._id,
            attempt=1,
            duration_ms=duration_ms,
        )
        kinds: dict[str, Kind] = {
            "stop": "stop_accepted",
            "start": "start_accepted",
            "delete": "delete_accepted",
        }
        report(
            Status(
                kind=kinds[verb],
                job_id=self._id,
                duration_ms=duration_ms,
                noun=_NOUN,
            ),
            progress,
        )
        return payload

    def _settled(self, record: dict[str, object], until: str) -> bool:
        state = state_of(record)
        if until == "stopped":
            return state in STOPPED_TARGETS
        if state == "STOPPED" and self._starting:
            return False
        if state in NOT_READY_STATES:
            return True
        return state == READY_STATE and has_ready_urls(record)

    def _wait(
        self,
        timeout: Timeout,
        until: str,
        probe: str | None,
        token: str | None,
        progress: Progress,
    ) -> dict[str, object]:
        if until not in WAIT_TARGETS:
            raise ValidationError(
                "until must be 'ready' or 'stopped'.",
                operation="wait",
                resource_id=self._id,
            )
        if probe is not None and (
            not isinstance(probe, str) or not probe.startswith("/")
        ):
            raise ValidationError(
                "probe must be a path such as '/v1/models'.",
                operation="wait",
                resource_id=self._id,
            )
        seconds = wait_seconds(timeout, self._id)
        started = poll.monotonic()
        record = self._wait_state(timeout, until, progress)
        if probe is None or until != "ready" or state_of(record) != "RUNNING":
            return record
        url = url_of(record)
        if url is None:
            raise ValidationError(
                "probe needs an https:// URL, and this endpoint has none.",
                operation="wait",
                resource_id=self._id,
            )
        bearer = token or self._token
        spent = poll.monotonic() - started
        poll.probe_until(
            lambda: http_status(url.rstrip("/") + probe, bearer),
            seconds=None if seconds is None else seconds - spent,
            progress=progress,
            resource_id=self._id,
            path=probe,
            poll_seconds=POLL_SECONDS,
            token_known=bearer is not None,
        )
        return record

    def _wait_state(
        self,
        timeout: Timeout,
        until: str,
        progress: Progress,
    ) -> dict[str, object]:
        return wait_until(
            self._get,
            lambda record: self._settled(record, until),
            timeout=timeout,
            progress=progress,
            resource_id=self._id,
            noun=_NOUN,
            poll_seconds=POLL_SECONDS,
            detail=lambda record: (
                " ".join(urls_of(record)) or None if until == "ready" else None
            ),
        )


def _sdk_transport(
    profile: str | None,
    project: str | None,
) -> EndpointTransport:
    from ..core.sdk_transport import SdkEndpointTransport

    return SdkEndpointTransport(profile=profile, project=project)
