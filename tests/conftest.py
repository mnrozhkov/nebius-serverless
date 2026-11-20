"""Shared test setup."""

import pytest


@pytest.fixture(autouse=True)
def _no_local_nebius_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Start every test as CI does: no CLI config, env token, or profile.

    Otherwise a test passes on a machine with ~/.nebius/config.yaml and
    fails in CI. A test that needs a CLI config says so itself.
    """
    monkeypatch.setenv("HOME", str(tmp_path_factory.mktemp("home")))
    monkeypatch.delenv("NEBIUS_IAM_TOKEN", raising=False)
    monkeypatch.delenv("NEBIUS_PROFILE", raising=False)


@pytest.fixture(autouse=True)
def _no_create_retry_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same-key create retries wait seconds in real use; not in tests."""
    monkeypatch.setattr(
        "nebius_serverless.transport.pause", lambda _seconds: None
    )
