"""One wait loop for jobs and endpoints.

The loop owns the clock, the deadline, the zero-timeout single fetch,
the clamped pause, cancellation of an awaited wait, and ``WaitTimeout``.
A handle supplies only how to fetch, when a record is settled, and how
often to poll. Tests replace ``monotonic`` and ``sleep`` here.
"""

import re
import threading
import time
from collections.abc import Callable
from datetime import timedelta

from ..errors import (
    AccessError,
    Error,
    ValidationError,
    WaitTimeout,
    recovery_for,
)
from ..ui.output import (
    Follow,
    Progress,
    elapsed_ms,
    log_transport,
    report,
    state_problem,
    timeout_text,
)
from ..ui.status import Status

Record = dict[str, object]
Timeout = float | str | timedelta | None

_DURATION = re.compile(r"^(?:([0-9]+)h)?(?:([0-9]+)m)?(?:([0-9]+)s)?$")
_NUMBER = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")


def duration_seconds(value: object) -> int | float | None:
    """Seconds for ``"1h30m"``, ``"45m"``, ``"90"``, a number, or a
    ``timedelta``; ``None`` when ``value`` is none of these.

    Shared by job ``timeout=`` and ``wait(timeout=)``. It does not
    check a range.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, timedelta):
        seconds: int | float = value.total_seconds()
    elif isinstance(value, (int, float)):
        seconds = value
    elif isinstance(value, str):
        parsed = _parse_duration(value)
        if parsed is None:
            return None
        seconds = parsed
    else:
        return None
    if isinstance(seconds, float) and seconds == int(seconds):
        seconds = int(seconds)
    return seconds


def _parse_duration(value: str) -> int | float | None:
    match = _DURATION.fullmatch(value)
    if match is not None and any(match.groups()):
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds
    if _NUMBER.fullmatch(value) is None:
        return None
    if "." in value:
        return float(value)
    return int(value)


def monotonic() -> float:
    """Seconds used to decide when a local wait is over."""
    return time.monotonic()


def sleep(seconds: float, stop: threading.Event | None = None) -> None:
    """Pause between polls. Return early when ``stop`` is set."""
    if stop is None:
        time.sleep(seconds)
        return
    stop.wait(seconds)


def state_of(record: Record | None) -> str | None:
    """Return the raw ``status.state`` string, when the record has one."""
    if record is None:
        return None
    status = record.get("status")
    if not isinstance(status, dict):
        return None
    state = status.get("state")
    if not isinstance(state, str) or state == "":
        return None
    return state


def wait_until(
    fetch: Callable[[], Record],
    settled: Callable[[Record], bool],
    *,
    timeout: Timeout,
    progress: Progress,
    resource_id: str,
    noun: str,
    poll_seconds: float,
    detail: Callable[[Record], str | None] | None = None,
) -> Record:
    """Fetch until ``settled(record)``; return that record.

    ``timeout`` is seconds, a duration string such as ``"45m"``, or
    a ``timedelta``. ``None`` waits without a limit and ``0`` is one
    fetch. Running out of time raises ``WaitTimeout`` and changes
    nothing in the cloud. ``detail`` adds a line under the final one,
    such as an endpoint URL.
    """
    seconds = _seconds(timeout, resource_id)
    deadline = None if seconds is None else monotonic() + seconds
    one_fetch = seconds is not None and seconds <= 0
    progress.begin_wait()
    started = monotonic()
    follow = Follow(progress, resource_id, noun=noun)
    attempt = 0
    while True:
        attempt += 1
        began = time.perf_counter()
        try:
            record = fetch()
        except Error as exc:
            log_transport(
                progress,
                job_id=resource_id,
                attempt=attempt,
                duration_ms=elapsed_ms(began),
                request_id=exc.request_id,
            )
            raise
        done = settled(record)
        note = detail(record) if done and detail is not None else None
        if done and state_of(record) in ("ERROR", "FAILED"):
            note = " | ".join(state_problem(record)) or note
        follow.poll(
            state=state_of(record),
            elapsed=monotonic() - started,
            attempt=attempt,
            duration_ms=elapsed_ms(began),
            terminal=done,
            detail=note,
        )
        if done:
            return record
        if one_fetch or _expired(deadline):
            _give_up(follow, seconds, resource_id, noun, state_of(record))
        pause = poll_seconds
        if deadline is not None:
            pause = max(0.0, min(pause, deadline - monotonic()))
        sleep(pause, progress.stopped)
        # A cancelled await closes the handle: stop polling here.
        progress.raise_if_abandoned()
        if _expired(deadline):
            _give_up(follow, seconds, resource_id, noun, state_of(record))


def probe_until(
    status: Callable[[], int | None],
    *,
    seconds: float | None,
    progress: Progress,
    resource_id: str,
    path: str,
    poll_seconds: float,
    token_known: bool,
) -> None:
    """Call ``status()`` until it returns a 2xx HTTP code.

    ``None`` (no connection), 404, 502, 503 and other codes keep polling.
    401 and 403 raise ``AccessError`` at once. Running out of ``seconds``
    raises ``WaitTimeout``; ``None`` waits without a limit.
    """
    deadline = None if seconds is None else monotonic() + max(seconds, 0.0)
    last: object = object()
    while True:
        code = status()
        if code is not None and 200 <= code < 300:
            return
        if code in (401, 403):
            reason = (
                "rejected the bearer token"
                if token_known
                else "needs a bearer token"
            )
            raise AccessError(
                f"Endpoint {resource_id} answered {code} at {path}: it "
                f"{reason}.",
                operation="wait",
                resource_id=resource_id,
                recovery="Call reveal_token() first, or pass wait(token=...).",
            )
        if code != last:
            shown = "no connection" if code is None else str(code)
            report(
                Status(
                    kind="probe",
                    job_id=resource_id,
                    message=f"app not answering yet at {path}: {shown}",
                    noun="endpoint",
                ),
                progress,
            )
            last = code
        if deadline is not None and monotonic() >= deadline:
            raise WaitTimeout(
                f"Endpoint {resource_id} is RUNNING, but {path} did not "
                "answer with 2xx in time. It may be billing.",
                operation="wait",
                resource_id=resource_id,
                recovery=(
                    "Call wait(probe=...) to keep waiting, or delete() to "
                    "stop it."
                ),
            )
        pause = poll_seconds
        if deadline is not None:
            pause = max(0.0, min(pause, deadline - monotonic()))
        sleep(pause, progress.stopped)
        progress.raise_if_abandoned()


def wait_seconds(timeout: Timeout, resource_id: str) -> float | None:
    """A validated ``timeout`` in seconds; ``None`` means no limit."""
    return _seconds(timeout, resource_id)


def _seconds(timeout: Timeout, resource_id: str) -> float | None:
    if timeout is None:
        return None
    seconds = duration_seconds(timeout)
    if seconds is None:
        raise ValidationError(
            "wait timeout must be seconds, a duration such as '45m', "
            "a timedelta, or None.",
            operation="wait",
            resource_id=resource_id,
        )
    if seconds < 0:
        raise ValidationError(
            "wait timeout must be zero or greater.",
            operation="wait",
            resource_id=resource_id,
        )
    return float(seconds)


def _expired(deadline: float | None) -> bool:
    return deadline is not None and monotonic() >= deadline


def _give_up(
    follow: Follow,
    seconds: float | None,
    resource_id: str,
    noun: str,
    state: str | None,
) -> None:
    seconds = 0.0 if seconds is None else seconds
    follow.timed_out(seconds)
    raise WaitTimeout(
        timeout_text(resource_id, seconds, noun, state=state),
        operation="wait",
        resource_id=resource_id,
        recovery=recovery_for(WaitTimeout.code, noun),
    )
