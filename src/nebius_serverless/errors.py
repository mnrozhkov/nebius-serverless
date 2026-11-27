"""Errors for the cloud Jobs and Endpoints client.

They carry identity, never env values or tokens. ``code`` is an SDK
category, not a backend reason code. ``request_id`` is set only when
the Nebius exception already exposes one.
"""

from collections.abc import Iterable, Mapping

_DEFAULT_RECOVERY = (
    "If you have a job id, reload it with jobs.get. "
    "Do not send a second create."
)
_ACCESS_RECOVERY = (
    "Check the Nebius profile and the project permissions, then retry. "
    "Run ns.whoami() to see the account and project in use."
)
_CAPACITY_RECOVERY = "Free quota or capacity, then retry the call."
_CONFLICT_RECOVERY = (
    "Reload the existing job with jobs.get. "
    "Do not send a second create."
)
_WAIT_RECOVERY = "Reload it with jobs.get(id), or call cancel."

ENDPOINT_RECOVERY = (
    "If you have an endpoint id, reload it with endpoints.get. "
    "Do not send a second create."
)
ENDPOINT_CONFLICT_RECOVERY = (
    "Reload the existing endpoint with endpoints.get. "
    "Do not send a second create."
)
ENDPOINT_WAIT_RECOVERY = (
    "Call wait() to keep waiting, or delete() to stop it. "
    "From another process, reload it with endpoints.get(id)."
)


class Error(Exception):
    """Base error for the Jobs client."""

    code = "error"
    _recovery = _DEFAULT_RECOVERY

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        project: str | None = None,
        key: str | None = None,
        resource_id: str | None = None,
        recovery: str | None = None,
        diff: Iterable[object] = (),
        request_id: str | None = None,
        trace_id: str | None = None,
        status_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.project = project
        self.key = key
        self.resource_id = resource_id
        self.recovery = self._recovery if recovery is None else recovery
        self.diff = tuple(diff)
        self.request_id = _clean_request_id(request_id)
        self.trace_id = _clean_request_id(trace_id)
        self.status_code = status_code or None
        self.code = type(self).code

    def __str__(self) -> str:
        """The message, then the ids Nebius support asks for."""
        text = super().__str__()
        ids = [
            f"{label} {value}"
            for label, value in (
                ("request id", self.request_id),
                ("trace id", self.trace_id),
            )
            if value
        ]
        return f"{text} ({', '.join(ids)})" if ids else text


class ValidationError(Error):
    """The call was rejected before Nebius was contacted.

    ``details`` lists each problem for tools, as dicts with ``loc``
    (the field path), ``msg`` and ``type``. It never holds the values
    that were passed.
    """

    code = "validation"

    def __init__(
        self,
        message: str,
        *,
        details: Iterable[Mapping[str, object]] = (),
        **kwargs: object,
    ) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.details = [dict(item) for item in details]


class UnsupportedFeature(ValidationError):
    """The call asks for something this slice does not implement."""

    code = "unsupported"


class AccessError(Error):
    """Authentication or permission failed."""

    code = "access"
    _recovery = _ACCESS_RECOVERY


class CapacityError(Error):
    """The API rejected the job for quota or capacity.

    ``violations`` lists each exceeded quota as a dict with ``quota``,
    ``requested``, ``limit``, ``region`` and ``message``.
    """

    code = "capacity"
    _recovery = _CAPACITY_RECOVERY

    def __init__(
        self,
        message: str,
        *,
        violations: Iterable[dict[str, object]] = (),
        **kwargs: object,
    ) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.violations = [dict(item) for item in violations]


class TransportError(Error):
    """The call failed and create was not accepted."""

    code = "transport"


class SubmissionUncertain(Error):
    """The outcome of create is unknown, even after same-key retries.

    ``idempotency_key`` is the key those creates carried. A create with
    that key and the same recipe returns the resource Nebius made, if
    any, instead of making a second one.
    """

    code = "submission_uncertain"

    def __init__(
        self,
        message: str,
        *,
        idempotency_key: str | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.idempotency_key = idempotency_key


class KeyConflict(Error):
    """The API reports an existing resource and its id is known."""

    code = "key_conflict"
    _recovery = _CONFLICT_RECOVERY


class WaitTimeout(Error):
    """Local waiting stopped. The cloud job was not cancelled.

    No cancellation was requested. The job may still be running.
    """

    code = "wait_timeout"
    _recovery = _WAIT_RECOVERY


class BuildFailed(Error):
    """An image build job ended in a state other than ``COMPLETED``.

    ``resource_id`` is the build job id and ``state`` its final state.
    """

    code = "build_failed"

    def __init__(
        self,
        message: str,
        *,
        state: str | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.state = state


class TaskFailed(Error):
    """A task's job, function or script failed.

    ``resource_id`` is the job id and ``state`` its final state.
    ``remote_traceback`` is the traceback from the job when a function
    raised. For a shell task, ``exit_code`` is the script's exit code
    and ``outputs`` holds what it had set in ``outputs`` before it
    stopped.
    """

    code = "task_failed"

    def __init__(
        self,
        message: str,
        *,
        state: str | None = None,
        remote_traceback: str | None = None,
        exit_code: int | None = None,
        outputs: dict[str, str] | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.state = state
        self.remote_traceback = remote_traceback
        self.exit_code = exit_code
        self.outputs = dict(outputs or {})


def recovery_for(code: str, noun: str) -> str | None:
    """Recovery text that names ``noun``. ``None`` keeps the default.

    Job errors keep the class default. Endpoint errors name
    ``endpoints.get`` and ``delete`` instead of ``jobs.get`` and
    ``cancel``.
    """
    if noun != "endpoint":
        return None
    if code == "key_conflict":
        return ENDPOINT_CONFLICT_RECOVERY
    if code == "wait_timeout":
        return ENDPOINT_WAIT_RECOVERY
    if code in {"access", "capacity"}:
        return None
    return ENDPOINT_RECOVERY


def exposed_request_id(exc: object) -> str | None:
    """Return a request id already present on ``exc``, if there is one."""
    return _exposed(exc, "request_id")


def exposed_trace_id(exc: object) -> str | None:
    """Return a trace id already present on ``exc``, if there is one."""
    return _exposed(exc, "trace_id")


def quota_text(violation: dict[str, object]) -> str:
    """One violation as a sentence, for example::

        Quota exceeded: compute.gpu.h100 requested 8, limit 4 (eu-north1).
    """
    quota = violation.get("quota") or "a quota"
    text = f"Quota exceeded: {quota}"
    if violation.get("requested") not in (None, ""):
        text += f" requested {violation['requested']}"
    if violation.get("limit") not in (None, ""):
        text += f", limit {violation['limit']}"
    if violation.get("region"):
        text += f" ({violation['region']})"
    return text + "."


def _exposed(exc: object, name: str) -> str | None:
    status = getattr(exc, "status", None)
    for owner in (status, exc):
        if owner is None:
            continue
        found = _clean_request_id(getattr(owner, name, None))
        if found is not None:
            return found
    return None


def _clean_request_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text == "":
        return None
    return text
