"""stop/start/delete retry an operation conflict; other failures say what
happened instead of the create-only "may have been accepted" text."""

from types import SimpleNamespace

import pytest
from grpc import StatusCode
from nebius.aio.service_error import RequestError, RequestStatusExtended

import nebius_serverless as ns
from nebius_serverless import sdk_transport
from nebius_serverless.sdk_transport import SdkEndpointTransport

ID = "aiendpoint-test-0001"


def _error(code: StatusCode, message: str = "") -> RequestError:
    return RequestError(
        RequestStatusExtended(
            code=code,
            message=message,
            details=[],
            request_id="req-1",
            trace_id="trace-1",
            service_errors=[],
        )
    )


class FakeClient:
    def __init__(self, failures: list[RequestError]) -> None:
        self.failures = failures
        self.calls = 0

    def _call(self, request: object) -> object:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return SimpleNamespace(wait=lambda: SimpleNamespace(resource_id=ID))

    start = stop = delete = _call


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    fake = FakeClient([])
    monkeypatch.setattr(SdkEndpointTransport, "_client", lambda self: fake)
    monkeypatch.setattr(sdk_transport.time, "sleep", lambda _s: None)
    return fake


@pytest.mark.parametrize("verb", ["start", "stop", "delete"])
def test_an_operation_conflict_is_retried(
    client: FakeClient, verb: str
) -> None:
    client.failures = [_error(StatusCode.ABORTED, "operation conflict")] * 2
    transport = SdkEndpointTransport(profile=None)
    assert getattr(transport, verb)(ID) == {"id": ID, "resource_id": ID}
    assert client.calls == 3


def test_a_lasting_conflict_gives_up_with_an_honest_message(
    client: FakeClient,
) -> None:
    attempts = sdk_transport.CONFLICT_ATTEMPTS
    client.failures = [_error(StatusCode.ABORTED)] * attempts
    with pytest.raises(ns.TransportError) as caught:
        SdkEndpointTransport(profile=None).start(ID)
    assert client.calls == attempts
    text = str(caught.value)
    assert "start call" in text and "ABORTED" in text
    assert "Create" not in text
    assert caught.value.request_id == "req-1"


def test_a_definite_error_is_not_retried(client: FakeClient) -> None:
    client.failures = [_error(StatusCode.FAILED_PRECONDITION)]
    with pytest.raises(ns.TransportError, match="FAILED_PRECONDITION"):
        SdkEndpointTransport(profile=None).start(ID)
    assert client.calls == 1
