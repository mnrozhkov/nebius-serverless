"""Errors carry request/trace ids and codes; quota failures name quotas;
ERROR records show their reason."""

import io
import sys

import pytest
from fakes import FakeClock, FakeTransport
from grpc import StatusCode
from nebius.aio.service_error import RequestError, RequestStatusExtended
from nebius.api.nebius.common.v1 import QuotaFailure, ServiceError

import nebius_serverless as ns
from nebius_serverless.core.sdk_transport import (
    SdkEndpointTransport,
    SdkTransport,
)

SECRET = "env-value-never-shown"
WHERE = {"region": "eu-north1", "project": "project-test-0001"}


def _error(code: StatusCode, *, quota: bool = False) -> RequestError:
    errors = []
    if quota:
        errors.append(
            ServiceError(
                quota_failure=QuotaFailure(
                    violations=[
                        QuotaFailure.Violation(
                            quota="compute.gpu.h100",
                            requested="8",
                            limit="4",
                            region="eu-north1",
                            message="too many GPUs",
                        ),
                        QuotaFailure.Violation(
                            quota="compute.disk.ssd",
                            requested="4000Gi",
                            limit="2000Gi",
                        ),
                    ]
                )
            )
        )
    return RequestError(
        RequestStatusExtended(
            code=code,
            message="",
            details=[],
            request_id="req-1",
            trace_id="trace-1",
            service_errors=errors,
        )
    )


class Raising:
    def __init__(self, error: RequestError) -> None:
        self.error = error

    def _raise(self, *_args: object, **_kwargs: object) -> object:
        raise self.error

    create = get = stop = start = delete = cancel = _raise


def _job(**kwargs: object) -> ns.JobSpec:
    return ns.JobSpec(
        command=["nvidia-smi"],
        image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
        gpu="h100:8",
        timeout="1h",
        env={"TOKEN": SECRET},
        **kwargs,  # type: ignore[arg-type]
    )


def test_a_quota_failure_names_each_quota(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = _error(StatusCode.RESOURCE_EXHAUSTED, quota=True)
    monkeypatch.setattr(SdkTransport, "_client", lambda self: Raising(error))
    with pytest.raises(ns.CapacityError) as caught:
        ns.Job(_job()).create(
            transport=SdkTransport(profile=None), quiet=True, **WHERE
        )
    exc = caught.value
    assert [v["quota"] for v in exc.violations] == [
        "compute.gpu.h100",
        "compute.disk.ssd",
    ]
    assert exc.violations[0]["limit"] == "4"
    text = str(exc)
    assert (
        "Quota exceeded: compute.gpu.h100 requested 8, limit 4 (eu-north1)."
        in text
    )
    assert "compute.disk.ssd requested 4000Gi, limit 2000Gi." in text
    assert text.endswith("(request id req-1, trace id trace-1)")
    assert (exc.request_id, exc.trace_id) == ("req-1", "trace-1")
    assert exc.status_code == "RESOURCE_EXHAUSTED"
    assert SECRET not in text and SECRET not in repr(exc)


@pytest.mark.parametrize(
    ("transport", "call"),
    [
        (SdkTransport, "get"),
        (SdkTransport, "cancel"),
        (SdkEndpointTransport, "get"),
        (SdkEndpointTransport, "stop"),
        (SdkEndpointTransport, "delete"),
    ],
)
def test_every_call_carries_the_ids(
    monkeypatch: pytest.MonkeyPatch, transport: type, call: str
) -> None:
    error = _error(StatusCode.PERMISSION_DENIED)
    monkeypatch.setattr(transport, "_client", lambda self: Raising(error))
    with pytest.raises(ns.AccessError) as caught:
        getattr(transport(profile=None), call)("aijob-test-0001")
    assert caught.value.trace_id == "trace-1"
    assert caught.value.status_code == "PERMISSION_DENIED"
    assert "trace id trace-1" in str(caught.value)


ERROR_RECORD = {
    "metadata": {"id": "aijob-test-0001", "name": "big"},
    "spec": {"image": "busybox:1.37"},
    "status": {
        "state": "ERROR",
        "state_details": {
            "code": "NotEnoughResources",
            "message": "No capacity for 1gpu-16vcpu-200gb",
            "service_error": {
                "quota_failure": {
                    "violations": [
                        {
                            "quota": "compute.gpu.h100",
                            "requested": "8",
                            "limit": "4",
                        }
                    ]
                }
            },
        },
    },
}


def test_an_error_record_shows_its_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    fake = FakeTransport()
    fake.get = lambda _id: ERROR_RECORD  # type: ignore[method-assign]
    job = ns.Job(_job()).create(transport=fake, quiet=True, **WHERE)
    err = io.StringIO()
    monkeypatch.setattr(sys, "stderr", err)
    job.wait()
    progress = err.getvalue()
    assert "NotEnoughResources: No capacity for 1gpu-16vcpu-200gb" in progress
    assert "Quota exceeded: compute.gpu.h100 requested 8, limit 4." in progress
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setenv("NO_COLOR", "1")
    job.display()
    shown = out.getvalue()
    assert "NotEnoughResources: No capacity for 1gpu-16vcpu-200gb" in shown
    assert "Quota exceeded: compute.gpu.h100 requested 8, limit 4." in shown
