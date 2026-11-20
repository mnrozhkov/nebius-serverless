"""start(), wait(until=...), DELETING, and a url that tracks the last fetch."""

import asyncio
import io
import sys

import pytest
from fakes import FakeClock, FakeEndpointTransport, FakeTransport

import nebius_serverless as ns

URL = "https://ep.example.nebius.cloud"
WHERE = {"region": "eu-north1", "project": "project-test-0001"}


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr("nebius_serverless.poll.monotonic", clock.monotonic)
    monkeypatch.setattr("nebius_serverless.poll.sleep", clock.sleep)
    return clock


def _endpoint(fake: FakeEndpointTransport) -> ns.Endpoint:
    spec = ns.EndpointSpec(image="vllm/vllm-openai:v0.19.1", port=8000)
    return spec.create(transport=fake, quiet=True, **WHERE)


def test_stop_then_start_round_trip() -> None:
    fake = FakeEndpointTransport()
    fake.states = [
        "RUNNING",  # first wait
        "RUNNING",  # left over right after stop
        "STOPPING",
        "STOPPED",
        "STOPPED",  # left over right after start
        "STARTING",
        "IMAGE_PULLING",
        "RUNNING",
    ]
    fake.urls = [URL, URL, URL, None, None, None, None, URL]
    endpoint = _endpoint(fake)
    assert endpoint.wait(quiet=True)["status"]["state"] == "RUNNING"

    endpoint.stop(quiet=True)
    record = endpoint.wait(until="stopped", quiet=True)
    assert record["status"]["state"] == "STOPPED"
    assert len(fake.gets) == 4
    assert endpoint.url is None

    payload = endpoint.start(quiet=True)
    assert payload == {"id": endpoint.id, "resource_id": endpoint.id}
    assert fake.starts == [endpoint.id]
    assert len(fake.gets) == 4  # start() does not wait
    record = endpoint.wait(quiet=True)
    assert record["status"]["state"] == "RUNNING"
    assert len(fake.gets) == 8
    assert endpoint.url == URL


def test_stopped_still_ends_a_ready_wait_without_a_start() -> None:
    fake = FakeEndpointTransport()
    fake.states = ["STOPPED"]
    fake.urls = [None]
    record = _endpoint(fake).wait(quiet=True)
    assert record["status"]["state"] == "STOPPED"


def test_start_async_calls_the_transport_once() -> None:
    fake = FakeEndpointTransport()
    endpoint = _endpoint(fake)
    asyncio.run(endpoint.start_async(quiet=True))
    assert fake.starts == [endpoint.id]
    assert fake.gets == []


def test_url_follows_the_latest_fetch() -> None:
    fake = FakeEndpointTransport()
    fake.states = ["RUNNING", "STOPPING"]
    fake.urls = [URL, None]
    endpoint = _endpoint(fake)
    endpoint.get(quiet=True)
    assert endpoint.url == URL
    endpoint.get(quiet=True)
    assert endpoint.url is None


def test_deleting_ends_both_waits_as_data() -> None:
    fake = FakeEndpointTransport()
    fake.states = ["DELETING"]
    endpoint = _endpoint(fake)
    assert endpoint.wait(quiet=True)["status"]["state"] == "DELETING"
    stopped = endpoint.wait(until="stopped", quiet=True)
    assert stopped["status"]["state"] == "DELETING"

    jobs = FakeTransport()
    jobs.states = ["RUNNING", "DELETING"]
    spec = ns.JobSpec(["true"], image="busybox:1.37", timeout="1h")
    job = spec.create(transport=jobs, quiet=True, **WHERE)
    assert job.wait(quiet=True)["status"]["state"] == "DELETING"


def test_until_must_be_a_known_target() -> None:
    endpoint = _endpoint(FakeEndpointTransport())
    with pytest.raises(ns.ValidationError, match="until"):
        endpoint.wait(until="soon", quiet=True)  # type: ignore[arg-type]


def test_start_progress_line(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = io.StringIO()
    endpoint = _endpoint(FakeEndpointTransport())
    monkeypatch.setattr(sys, "stderr", stream)
    endpoint.start()
    assert stream.getvalue() == f"Start accepted for {endpoint.id}\n"
