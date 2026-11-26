"""The Prefect server: its login, a check before submit, and flow runs.

Talks to the server's REST API with the standard library, so a flow
can be submitted from a machine without Prefect.
"""

import base64
import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from ...errors import AccessError, ValidationError

_OP = "PrefectFlow"

PREFECT_URL_ENV = "PREFECT_API_URL"


PREFECT_AUTH_ENV = "PREFECT_API_AUTH_STRING"


# The Prefect UI's address. Without it, the API address without its
# trailing /api.
PREFECT_UI_ENV = "PREFECT_UI_URL"


# Where the Prefect server's login is read from when it is not in the
# environment or in Prefect's settings: one line, ``user:password``.
PREFECT_AUTH_FILE = Path("~/.config/prefect/auth")


_PREFECT_TIMEOUT = 10.0


_VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")


def find_login() -> str | None:
    """The Prefect server's ``user:password``, when one is set here.

    Looked for in ``PREFECT_API_AUTH_STRING``, then in the local Prefect
    settings, then in the file ``~/.config/prefect/auth``.
    """
    value: object = os.environ.get(PREFECT_AUTH_ENV)
    if not value:
        try:
            from prefect.settings import (  # type: ignore[import-not-found]
                PREFECT_API_AUTH_STRING,
            )

            value = PREFECT_API_AUTH_STRING.value()
        except Exception:
            value = None
    reveal = getattr(value, "get_secret_value", None)
    if callable(reveal):
        value = reveal()
    if not value:
        try:
            value = PREFECT_AUTH_FILE.expanduser().read_text().strip()
        except OSError:
            value = None
    return value if isinstance(value, str) and value else None


def _request(
    api_url: str,
    path: str,
    login: str | None,
    body: object = None,
) -> urllib.request.Request:
    """A request to the Prefect API, with the login when there is one."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        api_url.rstrip("/") + path,
        data=data,
        method="GET" if data is None else "POST",
    )
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if login:
        token = base64.b64encode(login.encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    return request


def check_server(api_url: str, login: str | None) -> str | None:
    """Fail now if the Prefect server will refuse the flow.

    Asks the server for its version with the login the flow would get,
    and returns it. A refusal raises, before any image is built or job
    started. A server that cannot be reached from this machine is not
    an error: the job may still reach it. The version is then None.
    """
    request = _request(api_url, "/admin/version", login)
    try:
        with urllib.request.urlopen(
            request, timeout=_PREFECT_TIMEOUT
        ) as reply:
            try:
                version = json.loads(reply.read())
            except (AttributeError, ValueError):
                return None
            return version if isinstance(version, str) else None
    except urllib.error.HTTPError as exc:
        if exc.code not in (401, 403):
            return None
    except (urllib.error.URLError, OSError, ValueError):
        return None
    where = (
        f"Set {PREFECT_AUTH_ENV} to user:password, or put it on one "
        f"line in {PREFECT_AUTH_FILE}."
    )
    if login:
        raise AccessError(
            f"The Prefect server at {api_url} refused the login.",
            operation=_OP,
            recovery=where,
        )
    raise AccessError(
        f"The Prefect server at {api_url} asks for a login, and none "
        "was found on this machine.",
        operation=_OP,
        recovery=where,
    )


def _version(text: str | None) -> tuple[int, int, int] | None:
    """``major.minor.patch`` of a version, or None when there is none."""
    found = _VERSION.match(text.strip()) if text else None
    if found is None:
        return None
    major, minor, patch = (int(part) for part in found.groups())
    return major, minor, patch


def local_version() -> str:
    """The Prefect version installed here. Fails when there is none."""
    try:
        import prefect  # type: ignore[import-not-found]
    except ImportError:
        raise ValidationError(
            "A flow with remote_job=False needs the prefect package on "
            "this machine.",
            operation=_OP,
            recovery='Install it: pip install "nebius-serverless[prefect]"',
        ) from None
    return str(getattr(prefect, "__version__", ""))


def check_versions(
    api_url: str, server: str | None, client: str | None, where: str
) -> None:
    """Fail when the server runs an older Prefect than the flow would.

    Nothing is checked when either version is unknown.
    """
    have, need = _version(server), _version(client)
    if have is None or need is None or have >= need:
        return
    raise ValidationError(
        f"The Prefect server at {api_url} runs Prefect {server}, older "
        f"than the Prefect {client} {where}.",
        operation=_OP,
        recovery=(
            f"Upgrade the Prefect server to {client} or later, or run the "
            f"flow with Prefect {server}."
        ),
    )


def ui_url(api_url: str, run_id: str) -> str:
    """The flow run's page in the Prefect UI."""
    base = os.environ.get(PREFECT_UI_ENV, "").strip()
    if not base:
        base = api_url.rstrip("/")
        if base.endswith("/api"):
            base = base[: -len("/api")]
    return f"{base.rstrip('/')}/runs/flow-run/{run_id}"


def _post(
    api_url: str, path: str, login: str | None, body: object
) -> dict[str, Any]:
    request = _request(api_url, path, login, body)
    with urllib.request.urlopen(request, timeout=_PREFECT_TIMEOUT) as reply:
        loaded = json.loads(reply.read())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} returned {type(loaded).__name__}")
    return loaded


def create_flow_run(
    api_url: str,
    login: str | None,
    flow_name: str,
    run_title: str,
    parameters: dict[str, Any],
) -> str | None:
    """Create a Pending flow run and return its id.

    The flow is found by its name, or created. Returns None when the
    server cannot be reached or refuses: the flow then creates its run
    when it starts.
    """
    try:
        found = _post(api_url, "/flows/", login, {"name": flow_name})
        created = _post(
            api_url,
            "/flow_runs/",
            login,
            {
                "flow_id": found["id"],
                "name": run_title,
                "parameters": parameters,
                "state": {"type": "PENDING", "name": "Pending"},
            },
        )
        return str(created["id"])
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        return None


def mark_crashed(
    api_url: str, login: str | None, run_id: str, message: str
) -> None:
    """Mark a flow run Crashed, when its flow job did not start."""
    try:
        _post(
            api_url,
            f"/flow_runs/{run_id}/set_state",
            login,
            {
                "state": {
                    "type": "CRASHED",
                    "name": "Crashed",
                    "message": message[:500],
                },
                "force": True,
            },
        )
    except (urllib.error.URLError, OSError, ValueError):
        pass
