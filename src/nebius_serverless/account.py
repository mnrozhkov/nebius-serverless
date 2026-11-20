"""``ns.whoami()``: who the credentials belong to, and where jobs go."""

import json

from .errors import (
    AccessError,
    Error,
    ValidationError,
    exposed_request_id,
    exposed_trace_id,
)
from .loop import reject_running_loop
from .output import Progress, call_progress, call_progress_async, report
from .status import Status

_LOG = "nebius_serverless.account"
_RECOVERY = (
    "Run `nebius profile create` or `nebius profile activate`, or check "
    "NEBIUS_IAM_TOKEN / ns.configure(credentials_file=...)."
)


def whoami(
    *,
    profile: str | None = None,
    project: str | None = None,
    quiet: bool = False,
) -> dict[str, object]:
    """Check the credentials and where ``create()`` would put a job.

    One read-only call (IAM ``GetProfile``) through the shared SDK, then
    the same project and region resolution as ``create()``. Nothing is
    created, listed or changed. Returns::

        {"account": {"kind": "user" | "service_account" | "anonymous",
                     "id": ..., "name": ..., "email": ...},
         "auth": {"source": "profile" | "env" | "credentials_file" | "sdk",
                  "detail": ...},
         "project": ..., "region": ..., "problem": None | "..."}

    and prints a one-line summary on stderr (``quiet=True`` suppresses
    it). Failed authentication raises ``AccessError`` with what to do.
    No token or credential appears in the result or the output.
    """
    reject_running_loop("whoami_async")

    def work(progress: Progress) -> dict[str, object]:
        return _whoami(profile, project, progress)

    return call_progress(quiet=quiet, logger_name=_LOG, work=work)


async def whoami_async(
    *,
    profile: str | None = None,
    project: str | None = None,
    quiet: bool = False,
) -> dict[str, object]:
    """Await the same check as ``whoami``."""

    def work(progress: Progress) -> dict[str, object]:
        return _whoami(profile, project, progress)

    return await call_progress_async(
        quiet=quiet,
        logger_name=_LOG,
        work=work,
    )


def _whoami(
    profile: str | None,
    project: str | None,
    progress: Progress,
) -> dict[str, object]:
    from .profile import resolve_location
    from .session import auth_source, get_sdk, settings

    chosen = profile if profile is not None else settings().profile
    kind, detail = auth_source(chosen)
    account = _account(get_sdk(chosen))
    found: dict[str, object] = {
        "account": account,
        "auth": {"source": kind, "detail": detail},
        "project": None,
        "region": None,
        "problem": None,
    }
    try:
        project_id, region_id = resolve_location(
            profile=chosen, project=project, region=None
        )
        found["project"] = project_id
        found["region"] = region_id
    except ValidationError as exc:
        found["problem"] = str(exc)
    report(Status(kind="account", message=_summary(found)), progress)
    return found


def _account(sdk: object) -> dict[str, object]:
    try:
        from nebius.aio.service_error import RequestError
    except ImportError:
        raise ValidationError(
            "The Nebius SDK is required to check the account.",
            operation="whoami",
        ) from None
    try:
        response = sdk.whoami().wait()  # type: ignore[attr-defined]
        payload = json.loads(
            response.to_json(preserving_proto_field_name=True)
        )
    except RequestError as exc:
        status = getattr(exc, "status", None)
        code = getattr(getattr(status, "code", None), "name", None)
        raise AccessError(
            "Nebius did not accept these credentials.",
            operation="whoami",
            request_id=exposed_request_id(exc),
            trace_id=exposed_trace_id(exc),
            status_code=code,
            recovery=_RECOVERY,
        ) from None
    except Error:
        raise
    except Exception as exc:
        raise AccessError(
            "Could not authenticate with Nebius.",
            operation="whoami",
            request_id=exposed_request_id(exc),
            recovery=_RECOVERY,
        ) from None
    return _parse_account(payload if isinstance(payload, dict) else {})


def _parse_account(payload: dict[str, object]) -> dict[str, object]:
    user = payload.get("user_profile")
    if isinstance(user, dict):
        attributes = user.get("attributes")
        attrs = attributes if isinstance(attributes, dict) else {}
        return {
            "kind": "user",
            "id": user.get("id"),
            "name": attrs.get("name") or attrs.get("preferred_username"),
            "email": attrs.get("email"),
        }
    service = payload.get("service_account_profile")
    if isinstance(service, dict):
        info = service.get("info")
        metadata = info.get("metadata") if isinstance(info, dict) else None
        meta = metadata if isinstance(metadata, dict) else {}
        return {
            "kind": "service_account",
            "id": meta.get("id"),
            "name": meta.get("name"),
            "email": None,
        }
    return {"kind": "anonymous", "id": None, "name": None, "email": None}


def _summary(found: dict[str, object]) -> str:
    account = found["account"]
    auth = found["auth"]
    assert isinstance(account, dict) and isinstance(auth, dict)
    parts = [account.get("name"), account.get("email")]
    who = " ".join(dict.fromkeys(str(part) for part in parts if part))
    ident = f" ({account['id']})" if account.get("id") else ""
    kind = str(account.get("kind")).replace("_", " ")
    line = " ".join(f"Signed in as {kind} {who}{ident}".split())
    line += f" via {_via(auth)}."
    if found["project"]:
        line += f" Jobs go to {found['project']} in {found['region']}."
    else:
        line += f" {found['problem']}"
    return line


def _via(auth: dict[str, object]) -> str:
    source, detail = auth["source"], auth["detail"]
    if source == "profile":
        if detail == "active profile":
            return "the active CLI profile"
        return f"CLI profile {detail!r}"
    if source == "env":
        return str(detail)
    if source == "credentials_file":
        return f"credentials file {detail}"
    return "a configured SDK"
