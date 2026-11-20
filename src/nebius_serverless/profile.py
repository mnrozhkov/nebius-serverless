"""Project and region from the active Nebius SDK profile."""

import re
from pathlib import Path

from .aliases import REGIONS
from .errors import ValidationError

_MISSING = (
    "Project or region configuration is missing or ambiguous."
)

_NO_PROJECT = (
    "No project: there is no Nebius CLI profile to read one from. Pass "
    "project= or call ns.configure(project=...)."
)

_REGION_IDS = sorted(
    REGIONS,
    key=len,
    reverse=True,
)


def load_profile(
    profile: str | None = None,
    *,
    config_file: str | Path | None = None,
) -> tuple[str, str | None]:
    """Return ``(project_id, region)`` from the SDK profile.

    ``region`` is set when the profile or its endpoint names one. A
    profile that only has a project id returns ``None`` for the region;
    the caller then reads ``Project.spec.region``. A missing or
    ambiguous project raises ``ValidationError``. The message does not
    include credential material.
    """
    try:
        from nebius.aio.cli_config import Config
    except ImportError:
        raise ValidationError(
            "The Nebius SDK is required to resolve a profile.",
            operation="create",
        ) from None
    kwargs: dict[str, object] = {"profile": profile}
    if config_file is not None:
        kwargs["config_file"] = config_file
        kwargs["no_env"] = True
    try:
        config = Config(**kwargs)
        project = config.parent_id
    except Exception:
        raise ValidationError(_MISSING, operation="create") from None
    region = _region_of(config)
    if not isinstance(project, str) or project.strip() == "":
        raise ValidationError(_MISSING, operation="create")
    return project, region


def resolve_location(
    *,
    profile: str | None,
    project: str | None,
    region: str | None,
) -> tuple[str, str]:
    """Return ``(project_id, region)`` for one create.

    Explicit values win, then ``ns.configure`` defaults, then the CLI
    profile. Without a region, the project's ``spec.region`` is read.
    Without a CLI config file (an env token or a credentials file) the
    project must come from the call or from ``configure``.
    """
    from .session import has_cli_config, settings

    defaults = settings()
    if profile is None:
        profile = defaults.profile
    project_id = project if project is not None else defaults.project
    region_id = region if region is not None else defaults.region
    if project_id is None or region_id is None:
        if has_cli_config():
            loaded_project, loaded_region = load_profile(profile)
            if project_id is None:
                project_id = loaded_project
            if region_id is None:
                region_id = loaded_region
        elif project_id is None:
            raise ValidationError(_NO_PROJECT, operation="create")
    if region_id is None:
        from .sdk_transport import project_region

        region_id = project_region(profile, project_id)
    return project_id, region_id


def _region_of(config: object) -> str | None:
    profile = getattr(config, "_profile", {})
    if not isinstance(profile, dict):
        raise ValidationError(_MISSING, operation="create")
    raw = profile.get("region")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if raw not in (None, ""):
        raise ValidationError(_MISSING, operation="create")
    endpoint = ""
    endpoint_of = getattr(config, "endpoint", None)
    if callable(endpoint_of):
        endpoint = endpoint_of() or ""
    found = [
        region
        for region in _REGION_IDS
        if re.search(
            rf"(?<![a-z0-9]){re.escape(region)}(?![a-z0-9])",
            endpoint,
        )
    ]
    if len(found) > 1:
        raise ValidationError(_MISSING, operation="create")
    if len(found) == 1:
        return found[0]
    return None
