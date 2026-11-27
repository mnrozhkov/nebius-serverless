"""Auth without a CLI login: env token, credentials file, ns.configure."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import nebius_serverless as ns
from nebius_serverless.core import session
from nebius_serverless.core.sdk_transport import SdkTransport, open_sdk

TOKEN = "iam-token-value-never-shown"


class FakeSdk:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.closed = 0

    def sync_close(self) -> None:
        self.closed += 1


@pytest.fixture
def made(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> list[FakeSdk]:
    """Empty HOME, fresh session state, and a recording SDK constructor."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("NEBIUS_IAM_TOKEN", raising=False)
    monkeypatch.delenv("NEBIUS_PROFILE", raising=False)
    monkeypatch.setattr(session, "_sdks", {})
    monkeypatch.setattr(session, "_regions", {})
    monkeypatch.setattr(session, "_settings", session.Settings())
    sdks: list[FakeSdk] = []

    def construct(**kwargs: object) -> FakeSdk:
        sdk = FakeSdk(**kwargs)
        sdks.append(sdk)
        return sdk

    import nebius.sdk

    monkeypatch.setattr(nebius.sdk, "SDK", construct)

    class FakeProjects:
        def __init__(self, sdk: object) -> None:
            pass

        def get(self, request: object) -> object:
            project = SimpleNamespace(spec=SimpleNamespace(region="eu-north1"))
            return SimpleNamespace(wait=lambda: project)

    import nebius.api.nebius.iam.v2 as iam

    monkeypatch.setattr(iam, "ProjectServiceClient", FakeProjects)
    return sdks


def _spec() -> ns.JobSpec:
    return ns.JobSpec(["true"], image="busybox:1.37", timeout="10m")


def test_an_env_token_works_without_a_cli_config(
    made: list[FakeSdk], monkeypatch: pytest.MonkeyPatch
) -> None:
    from nebius.aio.token.static import EnvBearer

    monkeypatch.setenv("NEBIUS_IAM_TOKEN", TOKEN)
    described = _spec().create(dry_run=True, project="project-test-0001")
    assert described["region"] == "eu-north1"
    assert len(made) == 1
    assert isinstance(made[0].kwargs["credentials"], EnvBearer)
    assert "config_reader" not in made[0].kwargs
    assert session.auth_source() == ("env", "NEBIUS_IAM_TOKEN")


def test_without_a_profile_the_project_must_be_given(
    made: list[FakeSdk], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NEBIUS_IAM_TOKEN", TOKEN)
    with pytest.raises(ns.ValidationError, match="configure") as caught:
        _spec().create(dry_run=True)
    assert TOKEN not in str(caught.value)
    ns.configure(project="project-test-0001", region="eu-west1")
    described = _spec().create(dry_run=True)
    assert (described["project"], described["region"]) == (
        "project-test-0001",
        "eu-west1",
    )


def test_an_env_token_beats_a_federation_profile(
    made: list[FakeSdk], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = tmp_path / ".nebius" / "config.yaml"
    config.parent.mkdir()
    config.write_text(
        "default: fed\n"
        "profiles:\n"
        "  fed:\n"
        "    endpoint: api.eu-north1.nebius.cloud\n"
        "    auth-type: federation\n"
        "    parent-id: project-test-0001\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("NEBIUS_IAM_TOKEN", TOKEN)
    open_sdk(None)
    assert "config_reader" in made[0].kwargs
    assert "credentials" not in made[0].kwargs


def test_a_configured_sdk_is_used_and_never_closed(
    made: list[FakeSdk],
) -> None:
    mine = FakeSdk()
    ns.configure(sdk=mine)
    assert SdkTransport(profile="anything")._session() is mine
    assert session.get_sdk(None) is mine
    ns.close()
    assert mine.closed == 0
    assert made == []
    assert session.auth_source() == ("sdk", "configured SDK")


def test_a_credentials_file(made: list[FakeSdk], tmp_path: Path) -> None:
    path = tmp_path / "sa.json"
    path.write_text('{"private": "never-shown"}', encoding="utf-8")
    ns.configure(credentials_file=path, project="project-test-0001")
    session.get_sdk(None)
    assert made[0].kwargs["credentials_file_name"] == str(path)
    assert session.auth_source() == ("credentials_file", str(path))


def test_a_missing_credentials_file_names_the_path_only(
    made: list[FakeSdk], tmp_path: Path
) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(ns.ValidationError, match="nope.json") as caught:
        ns.configure(credentials_file=missing)
    assert caught.value.operation == "configure"
    with pytest.raises(ns.ValidationError, match="not both"):
        ns.configure(sdk=FakeSdk(), credentials_file=missing)


def test_no_credential_value_leaks(
    made: list[FakeSdk], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NEBIUS_IAM_TOKEN", TOKEN)
    ns.configure(project="project-test-0001")
    assert TOKEN not in str(session.auth_source())
    assert TOKEN not in str(ns.diagnostics())
    assert TOKEN not in repr(session.settings())
