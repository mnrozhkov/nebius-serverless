"""One Nebius SDK per process and auth source, opened on first use.

Each ``SDK`` owns an event-loop thread, an executor, and gRPC channels,
and the Nebius SDK rejects one that a child process inherits. So calls
share one SDK per ``(process, auth source)``: it opens on first use and
closes once at exit, or on ``close()``. A forked child opens its own.
"""

import atexit
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..errors import ValidationError

TOKEN_ENV = "NEBIUS_IAM_TOKEN"
CONFIG_FILE = Path("~/.nebius/config.yaml")

_lock = threading.Lock()
_sdks: dict[tuple[int, str], object] = {}
_regions: dict[tuple[int, str], str] = {}
_hooked = False


@dataclass(frozen=True)
class Settings:
    """Process defaults set by ``configure``."""

    sdk: object | None = None
    credentials_file: Path | None = None
    profile: str | None = None
    project: str | None = None
    region: str | None = None


_settings = Settings()


def configure(
    *,
    sdk: object | None = None,
    credentials_file: str | Path | None = None,
    profile: str | None = None,
    project: str | None = None,
    region: str | None = None,
) -> None:
    """Set how this process authenticates, and default project/region.

    - ``sdk``: a ``nebius.sdk.SDK`` you built. It is used as-is and never
      closed by this package.
    - ``credentials_file``: a service-account credentials file, the
      server-to-server way to authenticate in CI.
    - ``profile``: the CLI profile to use when a call names none.
    - ``project`` / ``region``: defaults for calls that pass none. With
      no CLI profile (an env token or a credentials file), ``project``
      must come from here or from the call.

    Without ``configure``, ``NEBIUS_IAM_TOKEN`` is used when it is set,
    and otherwise the active CLI profile. Each call replaces the previous
    settings, and ``configure()`` with no arguments restores the
    defaults. SDKs this package opened for the old settings are closed.
    """
    global _settings
    if sdk is not None and credentials_file is not None:
        raise ValidationError(
            "Pass sdk or credentials_file, not both.",
            operation="configure",
        )
    path = None
    if credentials_file is not None:
        path = Path(credentials_file).expanduser()
        if not path.is_file():
            raise ValidationError(
                f"The credentials file {str(path)!r} does not exist.",
                operation="configure",
            )
    close()
    _settings = Settings(
        sdk=sdk,
        credentials_file=path,
        profile=profile,
        project=project,
        region=region,
    )


def settings() -> Settings:
    """The current ``configure`` settings."""
    return _settings


def auth_source(profile: str | None = None) -> tuple[str, str]:
    """``(kind, label)`` for the credentials the next call will use.

    ``kind`` is ``sdk``, ``credentials_file``, ``env`` or ``profile``.
    The label names a file, a variable or a profile, never a secret.
    """
    current = _settings
    if current.sdk is not None:
        return "sdk", "configured SDK"
    if current.credentials_file is not None:
        return "credentials_file", str(current.credentials_file)
    if os.environ.get(TOKEN_ENV):
        return "env", TOKEN_ENV
    chosen = profile or current.profile or os.environ.get("NEBIUS_PROFILE")
    return "profile", chosen or "active profile"


def has_cli_config() -> bool:
    """Whether the Nebius CLI config file exists."""
    return CONFIG_FILE.expanduser().is_file()


def get_sdk(profile: str | None) -> object:
    """The shared SDK for ``profile`` (``None``: the configured default)."""
    current = _settings
    if current.sdk is not None:
        return current.sdk
    chosen = profile if profile is not None else current.profile
    kind, label = auth_source(chosen)
    key = (os.getpid(), f"{kind}:{label}:{chosen or ''}")
    with _lock:
        sdk = _sdks.get(key)
        if sdk is None:
            sdk = _open(chosen, current.credentials_file)
            _sdks[key] = sdk
            _hook()
        return sdk


def project_region(project_id: str, lookup: Callable[[], str]) -> str:
    """``lookup()`` once per project per process, then the cached value."""
    key = (os.getpid(), project_id)
    with _lock:
        found = _regions.get(key)
    if found is not None:
        return found
    region = lookup()
    with _lock:
        _regions[key] = region
    return region


def close() -> None:
    """Close every SDK this process opened. The next call reopens one.

    Long-running services can call it to release connections early.
    It is also registered once to run at interpreter exit.
    """
    pid = os.getpid()
    with _lock:
        mine = [key for key in _sdks if key[0] == pid]
        opened = [_sdks.pop(key) for key in mine]
        for key in [key for key in _regions if key[0] == pid]:
            del _regions[key]
    for sdk in opened:
        closer = getattr(sdk, "sync_close", None)
        if callable(closer):
            try:
                closer()
            except Exception:  # closing at exit must not raise
                pass


def _open(profile: str | None, credentials_file: Path | None) -> object:
    from .sdk_transport import open_sdk

    return open_sdk(profile, credentials_file=credentials_file)


def _hook() -> None:
    global _hooked
    if not _hooked:
        atexit.register(close)
        _hooked = True
