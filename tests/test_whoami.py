"""ns.whoami(): the account, the auth source, and where jobs would go."""

import json
from types import SimpleNamespace

import pytest
from grpc import StatusCode
from nebius.aio.service_error import RequestError, RequestStatusExtended

import nebius_serverless as ns
from nebius_serverless import profile, session

TOKEN = "env-token-never-shown"
USER = {
    "user_profile": {
        "id": "useraccount-test-0001",
        "attributes": {"name": "Ada", "email": "ada@example.com"},
    }
}
SERVICE = {
    "service_account_profile": {
        "info": {"metadata": {"id": "serviceaccount-test-0001", "name": "ci"}}
    }
}


class FakeSdk:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.calls: list[str] = []

    def whoami(self) -> object:
        self.calls.append("whoami")
        if isinstance(self.payload, Exception):
            raise self.payload
        text = json.dumps(self.payload)
        response = SimpleNamespace(to_json=lambda **_kw: text)
        return SimpleNamespace(wait=lambda: response)


class Err:
    """stderr captured by capsys (pytest swaps sys.stderr per phase)."""

    def __init__(self, capsys: pytest.CaptureFixture[str]) -> None:
        self.capsys = capsys

    def getvalue(self) -> str:
        return self.capsys.readouterr().err


@pytest.fixture
def stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> Err:
    monkeypatch.setattr(session, "_settings", session.Settings())
    monkeypatch.setattr(
        profile,
        "resolve_location",
        lambda **_kw: ("project-test-0001", "eu-north1"),
    )
    return Err(capsys)


def _use(monkeypatch: pytest.MonkeyPatch, payload: object) -> FakeSdk:
    sdk = FakeSdk(payload)
    monkeypatch.setattr(session, "get_sdk", lambda _profile: sdk)
    return sdk


def test_a_user(monkeypatch: pytest.MonkeyPatch, stderr: Err) -> None:
    monkeypatch.delenv("NEBIUS_IAM_TOKEN", raising=False)
    sdk = _use(monkeypatch, USER)
    found = ns.whoami(profile="work")
    assert found == {
        "account": {
            "kind": "user",
            "id": "useraccount-test-0001",
            "name": "Ada",
            "email": "ada@example.com",
        },
        "auth": {"source": "profile", "detail": "work"},
        "project": "project-test-0001",
        "region": "eu-north1",
        "problem": None,
    }
    assert sdk.calls == ["whoami"]
    line = stderr.getvalue()
    assert "Signed in as user Ada ada@example.com" in line
    assert "CLI profile 'work'" in line
    assert "project-test-0001 in eu-north1" in line


def test_a_service_account_via_env_token(
    monkeypatch: pytest.MonkeyPatch, stderr: Err
) -> None:
    monkeypatch.setenv("NEBIUS_IAM_TOKEN", TOKEN)
    _use(monkeypatch, SERVICE)
    found = ns.whoami()
    assert found["account"]["kind"] == "service_account"  # type: ignore
    assert found["auth"] == {"source": "env", "detail": "NEBIUS_IAM_TOKEN"}
    assert TOKEN not in stderr.getvalue() and TOKEN not in str(found)


def test_rejected_credentials_raise_access_error_with_a_hint(
    monkeypatch: pytest.MonkeyPatch, stderr: Err
) -> None:
    error = RequestError(
        RequestStatusExtended(
            code=StatusCode.UNAUTHENTICATED,
            message="token expired",
            details=[],
            request_id="req-9",
            trace_id="trace-9",
            service_errors=[],
        )
    )
    _use(monkeypatch, error)
    with pytest.raises(ns.AccessError) as caught:
        ns.whoami()
    assert caught.value.request_id == "req-9"
    assert "nebius profile create" in caught.value.recovery


def test_quiet_returns_the_same_dict_silently(
    monkeypatch: pytest.MonkeyPatch, stderr: Err
) -> None:
    _use(monkeypatch, USER)
    loud = ns.whoami()
    assert "Signed in" in stderr.getvalue()
    assert ns.whoami(quiet=True) == loud
    assert stderr.getvalue() == ""


def test_a_missing_project_is_reported_not_raised(
    monkeypatch: pytest.MonkeyPatch, stderr: Err
) -> None:
    def no_project(**_kw: object) -> tuple[str, str]:
        raise ns.ValidationError("No project: pass project=.")

    monkeypatch.setattr(profile, "resolve_location", no_project)
    _use(monkeypatch, USER)
    found = ns.whoami()
    assert found["project"] is None
    assert found["problem"] == "No project: pass project=."


def test_access_errors_point_at_whoami() -> None:
    assert "ns.whoami()" in ns.AccessError("x").recovery
