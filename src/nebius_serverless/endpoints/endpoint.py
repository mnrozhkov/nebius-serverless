"""``Endpoint``: one endpoint on Nebius, created once from a spec."""

import asyncio
import sys
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Literal, overload

from ..core import poll
from ..core.poll import Timeout, state_of, wait_seconds, wait_until
from ..core.transport import (
    EndpointTransport,
    call_dry_run,
    call_transport,
    validate_idempotency_key,
)
from ..errors import Error, ValidationError
from ..ui.loop import off_loop
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
from . import request
from .request import generate_token
from .spec import EndpointSpec

_LOG = "nebius_serverless.endpoint"
_NOUN = "endpoint"
_OP = "Endpoint"

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
    """One endpoint on Nebius: built from a spec, created once, then used.

    ``ns.Endpoint(spec)`` or ``ns.Endpoint(image=..., port=..., ...)``
    with the ``EndpointSpec`` fields makes the object. Nothing contacts
    Nebius until ``create()``, which sends the endpoint once and sets
    ``id`` and ``token``. The other calls need a created endpoint.

    ``ns.endpoints.get(id)`` returns an ``Endpoint`` for one that
    already exists. Its ``spec`` is not known, so ``spec``, ``dry_run``,
    ``to_cli`` and ``create`` raise on it. Use ``record``.

    Property reads do not perform I/O. ``wait`` polls until the endpoint
    is ready or will not become ready. Nothing here stops the endpoint
    except ``stop``, and nothing deletes it except ``delete``.
    """

    @overload
    def __init__(self, spec: EndpointSpec, /) -> None: ...

    @overload
    def __init__(
        self,
        command: Sequence[str] | None = None,
        /,
        *,
        image: str,
        port: int | None = None,
        ports: Sequence[int | str] | None = None,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        pricing: str = "on-demand",
        auth: str = "token",
        token: str | None = None,
        token_secret: str | None = None,
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
        self,
        spec: EndpointSpec | Sequence[str] | None = None,
        /,
        **fields: Any,
    ) -> None:
        if isinstance(spec, EndpointSpec):
            if fields:
                raise ValidationError(
                    "Pass an EndpointSpec or the endpoint's fields, not "
                    "both.",
                    operation=_OP,
                    recovery="Use spec.clone(...) to change a spec.",
                )
            self._spec: EndpointSpec | None = spec
        else:
            self._spec = EndpointSpec(command=spec, **fields)
        self._id: str | None = None
        self._name: str | None = self._spec.name
        self._transport: EndpointTransport | None = None
        self._create_state: str | None = None
        self._record: dict[str, object] | None = None
        self._token: str | None = None
        self._url: str | None = None
        # Set by start(): the next fetches may still show STOPPED.
        self._starting = False
        self._lock = threading.Lock()

    @classmethod
    def _attach(
        cls,
        endpoint_id: str,
        *,
        name: str,
        transport: EndpointTransport,
        record: dict[str, object] | None = None,
    ) -> "Endpoint":
        """A handle to an endpoint that exists. Its spec is not known."""
        endpoint = cls.__new__(cls)
        endpoint._spec = None
        endpoint._id = endpoint_id
        endpoint._name = name
        endpoint._transport = transport
        endpoint._create_state = None
        endpoint._record = record
        endpoint._token = None
        endpoint._url = url_of(record)
        endpoint._starting = False
        endpoint._lock = threading.Lock()
        return endpoint

    def __repr__(self) -> str:
        return (
            f"Endpoint(id={self._id!r}, name={self._name!r}, "
            f"status={self.status!r}, url={self._url!r})"
        )

    @property
    def spec(self) -> EndpointSpec:
        """The spec this endpoint was built from.

        An endpoint loaded with ``endpoints.get`` has none: the original
        arguments are not known. Its ``record`` holds what Nebius stored.
        """
        if self._spec is None:
            raise ValidationError(
                f"Endpoint {self._id} was loaded with endpoints.get, so "
                "its spec is not known.",
                operation="spec",
                resource_id=self._id,
                recovery="Read endpoint.record for what Nebius stored.",
            )
        return self._spec

    @property
    def id(self) -> str | None:
        """Nebius endpoint id, or ``None`` before ``create()``."""
        return self._id

    @property
    def name(self) -> str | None:
        """Display name. Set by ``create()`` when the spec has none."""
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

    def dry_run(
        self,
        *,
        server: bool = False,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
    ) -> dict[str, object]:
        """The create request as data. Creates nothing.

        ``server=False`` (default) builds it here, with env values and
        the token left out. It resolves the project and region, and
        makes no create call. ``server=True`` also sends the request to
        Nebius with ``dry_run`` set: Nebius validates it and creates
        nothing. Its answer is under ``"server"``. A rejection raises
        what ``create()`` would raise.
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
                token = None
                if spec.auth == "token" and spec.token_secret is None:
                    token = spec.token_text or generate_token()
                body = request.spec_body(spec, where, name, token)
                description["server"] = call_dry_run(
                    chosen, body, noun=_NOUN
                )
            return description

        return call_progress(
            quiet=True, logger_name=_LOG, work=work, noun=_NOUN
        )

    def to_cli(self, *, project: str | None = None) -> str:
        """The equivalent ``nebius ai endpoint create`` command.

        Does not contact Nebius. Without ``name=``, a fresh name is
        generated, as ``create()`` does. ``project`` (or the spec's)
        becomes ``--parent-id``; without one, the CLI uses its profile's
        project. Env values print as ``KEY="$KEY"`` placeholders and
        secrets by id; a token is never printed.
        """
        from .cli_command import endpoint_command

        spec = self.spec
        name = spec.name or request.name_from_key(str(uuid.uuid4()))
        return endpoint_command(spec, name=name, project=project)

    def create(
        self,
        *,
        region: str | None = None,
        project: str | None = None,
        idempotency_key: str | None = None,
        quiet: bool = False,
        transport: EndpointTransport | None = None,
    ) -> "Endpoint":
        """Create the endpoint on Nebius, once. Returns this ``Endpoint``.

        Sets ``id`` and ``token`` once Nebius has accepted the endpoint.
        It does not wait for the URL: acceptance is not readiness. A
        second call raises: build a new ``Endpoint`` for another one.
        Without ``token`` or ``token_secret`` on the spec, a random token
        is generated and a notice is printed. ``quiet=True`` suppresses
        progress for this call only.
        """

        def work(progress: Progress) -> Endpoint:
            return self._create(
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return call_progress(
            quiet=quiet, logger_name=_LOG, work=work, noun=_NOUN
        )

    async def create_async(
        self,
        *,
        region: str | None = None,
        project: str | None = None,
        idempotency_key: str | None = None,
        quiet: bool = False,
        transport: EndpointTransport | None = None,
    ) -> "Endpoint":
        """Await the same create as ``create``.

        Cancelling this await leaves the endpoint running when create
        was already accepted.
        """

        def work(progress: Progress) -> Endpoint:
            return self._create(
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return await call_progress_async(
            quiet=quiet, logger_name=_LOG, work=work, noun=_NOUN
        )

    def reveal_token(self) -> str | None:
        """Fetch the endpoint's bearer token from Nebius and return it.

        Nebius returns ``spec.auth_token`` to anyone allowed to read the
        endpoint, so a handle from ``endpoints.get`` can recover the
        token after a restart. Also sets ``token``. ``None`` when the
        endpoint is public or reads its token from MysteryBox
        (``token_secret``). One fetch; the value is never logged or
        kept in ``record``.
        """
        self._created("reveal_token")
        return off_loop(self._reveal)

    async def reveal_token_async(self) -> str | None:
        """Await the same fetch as ``reveal_token``."""
        self._created("reveal_token")
        return await asyncio.to_thread(self._reveal)

    def _reveal(self) -> str | None:
        endpoint_id, transport = self._created("reveal_token")
        token = transport.reveal_token(endpoint_id)
        if token is not None:
            self._token = token
        return token

    def get(self, *, quiet: bool = False) -> dict[str, object]:
        """Fetch the endpoint once and return its JSON.

        ``ERROR`` and ``STOPPED`` are data. ``quiet=True`` suppresses
        the snapshot line for this call only.
        """

        self._created("get")
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

        self._created("get")
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

        self._created("wait")
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

        self._created("wait")
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

        self._created("stop")
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

        self._created("stop")
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

        self._created("start")
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

        self._created("start")
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

        self._created("delete")
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

        self._created("delete")
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
        endpoint_id, _transport = self._created("display")
        show_record(
            self._record,
            job_id=endpoint_id,
            name=self._name or "",
            stream=sys.stdout,
        )

    @property
    def _known_id(self) -> str:
        """The id of a created resource."""
        return self._created("get")[0]

    def _created(self, operation: str) -> tuple[str, EndpointTransport]:
        """The id and transport. Raises before ``create()``."""
        if self._id is None or self._transport is None:
            raise ValidationError(
                "This endpoint is not created yet. Call create() first.",
                operation=operation,
                recovery=(
                    f"Call endpoint.create(), then endpoint.{operation}()."
                ),
            )
        return self._id, self._transport

    def _create(
        self,
        *,
        region: str | None,
        project: str | None,
        transport: EndpointTransport | None,
        progress: Progress,
        idempotency_key: str | None,
    ) -> "Endpoint":
        """Send the create. Runs once per object."""
        spec = self.spec
        with self._lock:
            if self._id is not None:
                raise ValidationError(
                    f"Endpoint {self._id} is already created. An Endpoint "
                    "is created once.",
                    operation="create",
                    resource_id=self._id,
                    recovery="Build a new ns.Endpoint(spec) for another one.",
                )
            where = request.plan(spec, region=region, project=project)
            if (
                idempotency_key is not None
                and spec.auth == "token"
                and spec.token is None
                and spec.token_secret is None
            ):
                raise ValidationError(
                    "idempotency_key needs token= or token_secret=: a token "
                    "generated in another process differs, so a reconnected "
                    "endpoint would not match this one's token.",
                    operation="create",
                )
            key = validate_idempotency_key(idempotency_key)
            name = spec.name or request.name_from_key(key)
            chosen = transport or _sdk_transport(spec.profile, where.project)
            token = None
            if spec.auth == "token" and spec.token_secret is None:
                token = spec.token_text or generate_token()
            body = request.spec_body(spec, where, name, token)
            started = time.perf_counter()
            try:
                result = call_transport(
                    chosen, body, noun=_NOUN, idempotency_key=key
                )
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
            endpoint_id = result.resource_id
            log_transport(
                progress,
                job_id=endpoint_id,
                attempt=1,
                duration_ms=duration_ms,
                request_id=result.request_id,
            )
            report(
                Status(
                    kind="submitted",
                    job_id=endpoint_id,
                    name=name,
                    state=result.state,
                    duration_ms=duration_ms,
                    request_id=result.request_id,
                    noun=_NOUN,
                ),
                progress,
            )
            if token is not None and spec.token is None:
                report(
                    Status(
                        kind="token_generated",
                        job_id=endpoint_id,
                        noun=_NOUN,
                    ),
                    progress,
                )
            self._id = endpoint_id
            self._name = name
            self._transport = chosen
            self._create_state = result.state
            self._token = token
        return self

    def _get(self) -> dict[str, object]:
        endpoint_id, transport = self._created("get")
        record = transport.get(endpoint_id)
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
                job_id=self._known_id,
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
        endpoint_id, transport = self._created(verb)
        started = time.perf_counter()
        calls = {
            "stop": transport.stop,
            "start": transport.start,
            "delete": transport.delete,
        }
        call = calls[verb]
        try:
            payload = call(endpoint_id)
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
                job_id=self._known_id,
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
        seconds = wait_seconds(timeout, self._known_id)
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
            resource_id=self._known_id,
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
            resource_id=self._known_id,
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
