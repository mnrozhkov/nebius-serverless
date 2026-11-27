"""Create, get, cancel, stop, and delete behind a small transport object."""

import re
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol

from ..errors import (
    AccessError,
    CapacityError,
    Error,
    KeyConflict,
    SubmissionUncertain,
    TransportError,
    ValidationError,
    exposed_request_id,
    quota_text,
    recovery_for,
)


@dataclass(frozen=True)
class CreateResult:
    """What one create call did, without a second attempt."""

    resource_id: str | None = None
    accepted: bool = False
    state: str | None = None
    failure: str | None = None
    conflict_id: str | None = None
    request_id: str | None = None
    code: str | None = None
    message: str | None = None
    trace_id: str | None = None
    violations: tuple[dict[str, object], ...] = ()


@dataclass(frozen=True)
class Created:
    """A create the API accepted. ``resource_id`` is checked, never empty."""

    resource_id: str
    state: str | None = None
    request_id: str | None = None


class Transport(Protocol):
    """The job calls the client makes after a recipe is valid."""

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str,
    ) -> CreateResult:
        """Send one create with this ``x-idempotency-key``. Do not retry."""

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Send the create with ``dry_run`` set. Nebius creates nothing."""

    def get(self, job_id: str) -> dict[str, object]:
        """Fetch one job."""

    def cancel(self, job_id: str) -> dict[str, object]:
        """Request cancellation once."""


class EndpointTransport(Protocol):
    """The endpoint calls the client makes after a recipe is valid."""

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str,
    ) -> CreateResult:
        """Send one create with this ``x-idempotency-key``. Do not retry."""

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Send the create with ``dry_run`` set. Nebius creates nothing."""

    def get(self, endpoint_id: str) -> dict[str, object]:
        """Fetch one endpoint, without ``spec.auth_token``."""

    def reveal_token(self, endpoint_id: str) -> str | None:
        """Fetch one endpoint's bearer token, or ``None``."""

    def stop(self, endpoint_id: str) -> dict[str, object]:
        """Request a stop once."""

    def start(self, endpoint_id: str) -> dict[str, object]:
        """Request a start once."""

    def delete(self, endpoint_id: str) -> dict[str, object]:
        """Request deletion once."""


class _DryRuns(Protocol):
    def dry_run(self, body: Mapping[str, object]) -> CreateResult: ...


class _Creates(Protocol):
    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str,
    ) -> CreateResult: ...


# An ambiguous create is sent again with the same key: Nebius returns
# the resource it already made (checked live, 19 Nov 2026).
CREATE_ATTEMPTS = 3
RETRY_PAUSES = (2.0, 5.0)


def pause(seconds: float) -> None:
    """Wait before resending a create. Tests replace this."""
    time.sleep(seconds)


_KEY = re.compile(r"^[\x21-\x7e]{1,128}$")


def validate_idempotency_key(key: str | None) -> str:
    """The caller's key, or a new UUID4 for this create call."""
    if key is None:
        return str(uuid.uuid4())
    if not isinstance(key, str) or _KEY.fullmatch(key) is None:
        raise ValidationError(
            "idempotency_key must be 1 to 128 printable characters "
            "without spaces.",
            operation="create",
        )
    return key


def finish_create(
    result: CreateResult,
    *,
    noun: str = "job",
    idempotency_key: str | None = None,
) -> str:
    """Return the resource id, or raise the error this result stands for.

    A lost id raises ``SubmissionUncertain`` carrying the key a safe
    retry needs.
    """
    failure = result.failure
    request_id = result.request_id
    if failure == "access":
        raise AccessError(
            "Authentication or permission failed.",
            operation="create",
            request_id=request_id,
            trace_id=result.trace_id,
            status_code=result.code,
        )
    if failure == "capacity":
        raise CapacityError(
            capacity_text(noun, result.violations),
            operation="create",
            request_id=request_id,
            violations=result.violations,
            trace_id=result.trace_id,
            status_code=result.code,
        )
    if failure == "conflict":
        if result.conflict_id:
            raise KeyConflict(
                "The API reports an existing resource for this create.",
                operation="create",
                resource_id=result.conflict_id,
                request_id=request_id,
                recovery=recovery_for(KeyConflict.code, noun),
            )
        raise _uncertain(request_id, noun, idempotency_key)
    if failure == "rejected":
        reason = f" ({result.code})" if result.code else ""
        detail = f": {result.message}" if result.message else "."
        raise ValidationError(
            f"Nebius rejected the {noun}{reason}{detail}",
            operation="create",
            request_id=request_id,
            trace_id=result.trace_id,
            status_code=result.code,
            recovery=f"Nothing was created. Fix the request and create "
            f"the {noun} again.",
        )
    if failure == "transport":
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
            request_id=request_id,
            recovery=recovery_for(TransportError.code, noun),
        )
    if _ambiguous(result):
        raise _uncertain(request_id, noun, idempotency_key)
    if not result.resource_id:
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
            request_id=request_id,
            recovery=recovery_for(TransportError.code, noun),
        )
    return result.resource_id


def call_transport(
    transport: _Creates,
    body: Mapping[str, object],
    *,
    noun: str = "job",
    idempotency_key: str,
) -> Created:
    """Create, resending an ambiguous outcome with the same key.

    A non-definite failure, or an accepted operation without an id, is
    sent again up to ``CREATE_ATTEMPTS`` times with the same
    ``idempotency_key`` and body, so Nebius returns the resource it may
    already have made instead of making a second one. Definite errors
    are not retried. Returns the id, or raises what ``finish_create``
    maps.
    """
    for attempt in range(CREATE_ATTEMPTS):
        try:
            result = scrub_result(
                transport.create(body, idempotency_key), body
            )
        except Error:
            raise
        except Exception as exc:
            raise TransportError(
                "The call failed before create was accepted.",
                operation="create",
                request_id=exposed_request_id(exc),
                recovery=recovery_for(TransportError.code, noun),
            ) from None
        if not _ambiguous(result) or attempt == CREATE_ATTEMPTS - 1:
            break
        pause(RETRY_PAUSES[min(attempt, len(RETRY_PAUSES) - 1)])
    return Created(
        resource_id=finish_create(
            result, noun=noun, idempotency_key=idempotency_key
        ),
        state=result.state,
        request_id=result.request_id,
    )


def call_dry_run(
    transport: _DryRuns,
    body: Mapping[str, object],
    *,
    noun: str = "job",
) -> dict[str, object]:
    """Ask Nebius to validate ``body`` without creating anything.

    Returns the server's answer. A rejection raises what a create would
    raise: ``ValidationError``, ``AccessError`` or ``CapacityError``.
    """
    try:
        result = scrub_result(transport.dry_run(body), body)
    except Error:
        raise
    except Exception as exc:
        raise TransportError(
            f"The server dry run for the {noun} failed. Nothing was "
            "created.",
            operation="dry_run",
            request_id=exposed_request_id(exc),
            recovery="Run dry_run(server=True) again.",
        ) from None
    if result.failure in ("access", "capacity", "rejected"):
        finish_create(result, noun=noun)
    if result.failure is not None or not result.accepted:
        raise TransportError(
            f"The server dry run for the {noun} did not complete "
            f"({result.code or result.failure or 'unknown'}). Nothing was "
            "created.",
            operation="dry_run",
            request_id=result.request_id,
            recovery="Run dry_run(server=True) again.",
        )
    return {"accepted": True, "request_id": result.request_id}


def capacity_text(
    noun: str,
    violations: Sequence[dict[str, object]],
) -> str:
    """Name each exceeded quota, or say quota or capacity in general."""
    if violations:
        return " ".join(quota_text(item) for item in violations)
    return f"The API rejected the {noun} for quota or capacity."


def scrub_result(
    result: CreateResult,
    body: Mapping[str, object],
) -> CreateResult:
    """Hide env values, tokens and file contents the server echoed."""
    if not result.message:
        return result
    message = result.message
    for value in _secret_values(body):
        message = message.replace(value, "<hidden>")
    return replace(result, message=message)


def _secret_values(body: Mapping[str, object]) -> list[str]:
    spec = body.get("spec")
    if not isinstance(spec, Mapping):
        return []
    values: list[str] = []
    token = spec.get("auth_token")
    if isinstance(token, str):
        values.append(token)
    for item in spec.get("environment_variables") or ():
        if isinstance(item, Mapping) and isinstance(item.get("value"), str):
            values.append(str(item["value"]))
    for item in spec.get("injected_files") or ():
        if isinstance(item, Mapping):
            content = item.get("content")
            if isinstance(content, str):
                values.append(content)
    # Longest first, so a value that contains another is hidden whole.
    return sorted((v for v in values if len(v) >= 3), key=len, reverse=True)


def _ambiguous(result: CreateResult) -> bool:
    if result.failure == "uncertain":
        return True
    lost = result.accepted and not result.resource_id
    return result.failure is None and lost


def _uncertain(
    request_id: str | None,
    noun: str,
    idempotency_key: str | None,
) -> SubmissionUncertain:
    if idempotency_key is None:
        return SubmissionUncertain(
            "Create may have been accepted and the id is unknown.",
            operation="create",
            request_id=request_id,
            recovery=recovery_for(SubmissionUncertain.code, noun),
        )
    return SubmissionUncertain(
        f"Create may have been accepted and the id is unknown, after "
        f"{CREATE_ATTEMPTS} tries with idempotency key {idempotency_key!r}.",
        operation="create",
        request_id=request_id,
        idempotency_key=idempotency_key,
        recovery=(
            f"Call create(idempotency_key={idempotency_key!r}) on the same "
            f"recipe with the same arguments: Nebius returns the {noun} it "
            "made, if any, instead of making a second one. If you have its "
            f"id, reload it with {noun}s.get."
        ),
    )
