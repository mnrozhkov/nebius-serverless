"""wait(probe="/v1/models"): ready, then the app's own route answers 2xx."""

import logging
import urllib.error

import pytest
from fakes import FakeClock, FakeEndpointTransport

import nebius_serverless as ns

URL = "https://ep.example.nebius.cloud"
WHERE = {"region": "eu-north1", "project": "project-test-0001"}


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    return clock


class _Ctx:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> "_Ctx":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def _opener(
    monkeypatch: pytest.MonkeyPatch, codes: list[int | None]
) -> list[object]:
    seen: list[object] = []

    def opener(request: object, timeout: float) -> object:
        seen.append(request)
        code = codes.pop(0) if len(codes) > 1 else codes[0]
        if code is None:
            raise urllib.error.URLError("refused")
        if code >= 300:
            raise urllib.error.HTTPError(
                getattr(request, "full_url", ""), code, "no", {}, None
            )
        return _Ctx(code)

    monkeypatch.setattr(
        "nebius_serverless.endpoints.endpoint.open_url", opener
    )
    return seen


def _endpoint(fake: FakeEndpointTransport, **kwargs: object) -> ns.Endpoint:
    spec = ns.EndpointSpec(
        image="vllm/vllm-openai:v0.19.1",
        port=8000,
        **kwargs,  # type: ignore[arg-type]
    )
    return ns.Endpoint(spec).create(transport=fake, quiet=True, **WHERE)


def test_waits_until_the_app_answers(
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="nebius_serverless")
    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING", "RUNNING"]
    fake.urls = [None, URL]
    endpoint = _endpoint(fake)
    seen = _opener(monkeypatch, [None, 502, 502, 200])
    record = endpoint.wait(probe="/v1/models")
    assert record["status"]["state"] == "RUNNING"
    assert len(seen) == 4
    request = seen[-1]
    assert request.full_url == URL + "/v1/models"  # type: ignore[attr-defined]
    assert request.get_header("Authorization") == f"Bearer {endpoint.token}"  # type: ignore[attr-defined]
    err = capsys.readouterr().err
    assert "app not answering yet at /v1/models: no connection" in err
    assert "app not answering yet at /v1/models: 502" in err
    assert err.count(": 502") == 1  # one line per change
    assert endpoint.token not in err
    assert all(
        str(endpoint.token) not in str(r.__dict__) for r in caplog.records
    )


def test_a_rejected_token_raises_at_once(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeEndpointTransport()
    endpoint = _endpoint(fake)
    seen = _opener(monkeypatch, [401])
    with pytest.raises(ns.AccessError, match="rejected the bearer token"):
        endpoint.wait(probe="/v1/models", quiet=True)
    assert len(seen) == 1
    reloaded = ns.endpoints.get(endpoint.id, transport=fake, quiet=True)
    with pytest.raises(ns.AccessError, match="needs a bearer token") as e:
        reloaded.wait(probe="/v1/models", quiet=True)
    assert "reveal_token" in e.value.recovery


def test_the_deadline_covers_both_phases(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING"] * 120 + ["RUNNING"]  # 600 s
    fake.urls = [None] * 120 + [URL]
    endpoint = _endpoint(fake, auth="none")
    _opener(monkeypatch, [503])
    with pytest.raises(ns.WaitTimeout, match="did not answer"):
        endpoint.wait(timeout="15m", probe="/health", quiet=True)
    assert clock.now == 900


def test_probe_is_only_for_a_ready_wait(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeEndpointTransport()
    fake.states = ["STOPPED"]
    fake.urls = [None]
    seen = _opener(monkeypatch, [200])
    endpoint = _endpoint(fake)
    endpoint.wait(until="stopped", probe="/v1/models", quiet=True)
    endpoint.wait(probe="/v1/models", quiet=True)  # STOPPED: returned
    assert seen == []
    with pytest.raises(ns.ValidationError, match="path"):
        endpoint.wait(probe="v1/models", quiet=True)
