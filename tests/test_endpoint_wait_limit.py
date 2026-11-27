"""Endpoint wait() is bounded by default; job wait() is not."""

import inspect

import pytest
from fakes import FakeClock, FakeEndpointTransport

import nebius_serverless as ns

URL = "https://ep.example.nebius.cloud"
WHERE = {"region": "eu-north1", "project": "project-test-0001"}


def _endpoint(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakeEndpointTransport,
) -> tuple[ns.Endpoint, FakeClock]:
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    spec = ns.EndpointSpec(image="vllm/vllm-openai:v0.19.1", port=8000)
    return spec.create(transport=fake, quiet=True, **WHERE), clock


def test_default_wait_gives_up_after_an_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING"]
    fake.urls = [None]
    endpoint, clock = _endpoint(monkeypatch, fake)
    with pytest.raises(ns.WaitTimeout) as caught:
        endpoint.wait(quiet=True)
    assert clock.now == 3600
    text = str(caught.value)
    assert endpoint.id in text
    assert "PROVISIONING" in text
    assert "billing" in text
    assert "wait()" in text
    assert "delete()" in text
    assert "delete()" in caught.value.recovery
    assert fake.deletes == [] and fake.stops == []


def test_timeout_none_waits_without_a_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polls = int(10 * 3600 / 5)
    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING"] * polls + ["RUNNING"]
    fake.urls = [None] * polls + [URL]
    endpoint, clock = _endpoint(monkeypatch, fake)
    record = endpoint.wait(timeout=None, quiet=True)
    assert record["status"]["state"] == "RUNNING"
    assert endpoint.url == URL
    assert clock.now >= 10 * 3600


def test_the_defaults_differ_on_purpose() -> None:
    def default(method: object) -> object:
        return inspect.signature(method).parameters["timeout"].default

    assert default(ns.Endpoint.wait) == 3600
    assert default(ns.Endpoint.wait_async) == 3600
    assert default(ns.Job.wait) is None
    assert default(ns.Job.wait_async) is None


def test_wait_accepts_duration_strings_and_timedelta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import timedelta

    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING"]
    fake.urls = [None]
    endpoint, clock = _endpoint(monkeypatch, fake)
    with pytest.raises(ns.WaitTimeout) as caught:
        endpoint.wait(timeout="45m", quiet=True)
    assert clock.now == 2700
    assert "45m" in str(caught.value)
    start = clock.now
    with pytest.raises(ns.WaitTimeout):
        endpoint.wait(timeout=timedelta(minutes=2), quiet=True)
    assert clock.now - start == 120


@pytest.mark.parametrize("bad", ["soon", "-5m", True, [1]])
def test_wait_rejects_a_bad_timeout(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    endpoint, _clock = _endpoint(monkeypatch, FakeEndpointTransport())
    with pytest.raises(ns.ValidationError, match="wait timeout"):
        endpoint.wait(timeout=bad, quiet=True)  # type: ignore[arg-type]


def test_job_wait_accepts_a_duration_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fakes import FakeTransport

    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    fake = FakeTransport()
    fake.states = ["RUNNING"]
    spec = ns.JobSpec(["true"], image="busybox:1.37", timeout="1h")
    job = spec.create(transport=fake, quiet=True, **WHERE)
    with pytest.raises(ns.WaitTimeout):
        job.wait(timeout="1m30s", quiet=True)
    assert clock.now == 90
