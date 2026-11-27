"""Specs are data; Job and Endpoint create once on the handle."""

from pathlib import Path

import pytest
from fakes import FakeClock, FakeEndpointTransport, FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.sdk_transport import (
    build_create_request,
    build_endpoint_request,
)
from nebius_serverless.jobs.job import PREEMPTED_CODE

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
SECRET = "env-value-not-shown"
TOKEN = "token-value-not-shown"
CUDA = "nvidia/cuda:13.1.1-runtime-ubuntu24.04"


def _spec(**changes: object) -> ns.JobSpec:
    fields: dict[str, object] = {
        "command": ["python", "train.py", "--epochs", "3"],
        "image": CUDA,
        "gpu": "h100",
        "timeout": "2h",
        "env": {"WANDB_KEY": SECRET},
        "secrets": {"HF_TOKEN": "mbsec-test-0001"},
        "volumes": ["s3://data:/workspace/data:ro"],
    }
    fields.update(changes)
    return ns.JobSpec(**fields)  # type: ignore[arg-type]


def _endpoint_spec(**changes: object) -> ns.EndpointSpec:
    fields: dict[str, object] = {
        "image": "vllm/vllm-openai:v0.19.1",
        "command": ["vllm", "serve", "Qwen/Qwen3-0.6B"],
        "gpu": "h100",
        "port": 8000,
        "token": TOKEN,
        "env": {"HF_HOME": SECRET},
    }
    fields.update(changes)
    return ns.EndpointSpec(**fields)  # type: ignore[arg-type]


def test_a_spec_and_a_job_make_no_transport_call() -> None:
    fake = FakeTransport()
    spec = _spec()
    job = ns.Job(spec)
    short = ns.Job(["nvidia-smi"], image=CUDA, timeout="10m")
    assert fake.bodies == [] and fake.gets == [] and fake.dry_runs == []
    assert job.id is None and short.id is None
    assert job.spec is spec
    assert short.spec.command == ("nvidia-smi",)


def test_specs_are_frozen_and_compare_by_value() -> None:
    spec = _spec()
    with pytest.raises(Exception, match="frozen"):
        spec.gpu = "h200"  # type: ignore[misc]
    assert spec == _spec()
    assert spec != _spec(gpu="h200")


def test_clone_copies_exactly_and_validates_changes() -> None:
    spec = _spec()
    assert spec.clone() == spec
    assert spec.clone() is not spec
    longer = spec.clone(timeout="3h")
    assert longer.timeout == 3 * 3600
    assert longer.clone(timeout="2h") == spec
    with pytest.raises(ns.ValidationError, match="between 5 minutes"):
        spec.clone(timeout="1m")
    with pytest.raises(ns.ValidationError, match="no field nope"):
        spec.clone(nope=1)


def test_a_spec_and_keyword_fields_together_raise() -> None:
    with pytest.raises(ns.ValidationError, match="not both"):
        ns.Job(_spec(), gpu="h200")  # type: ignore[call-overload]


def test_field_errors_keep_their_message_and_add_details() -> None:
    with pytest.raises(ns.ValidationError) as caught:
        _spec(timeout="1m")
    assert "between 5 minutes and 168 hours" in str(caught.value)
    assert caught.value.details == [
        {
            "loc": ("timeout",),
            "msg": str(caught.value),
            "type": "validation",
        }
    ]
    with pytest.raises(ns.ValidationError, match="not a shell string"):
        _spec(command="python train.py")


def test_model_validate_raises_the_library_error() -> None:
    data = {"command": ["x"], "image": "python:3", "timeout": 600}
    assert ns.JobSpec.model_validate(data).timeout == 600
    with pytest.raises(ns.ValidationError, match="no field gpus") as caught:
        ns.JobSpec.model_validate({**data, "gpus": "h100"})
    assert caught.value.details[0]["loc"] == ("gpus",)
    with pytest.raises(ns.ValidationError, match="timeout is required"):
        ns.JobSpec.model_validate({"command": ["x"], "image": "python:3"})
    loaded = ns.JobSpec.model_validate_json(
        '{"command": ["x"], "image": "python:3", "timeout": "1h"}'
    )
    assert loaded.timeout == 3600
    schema = ns.JobSpec.model_json_schema()
    assert {"command", "image", "timeout"} <= set(schema["required"])


def test_env_values_stay_out_of_repr_dry_run_and_to_dict() -> None:
    spec = _spec()
    job = ns.Job(spec)
    assert SECRET not in repr(spec)
    assert SECRET not in repr(job)
    described = job.dry_run(**WHERE)
    assert SECRET not in str(described)
    assert described["env"] == ["WANDB_KEY"]
    assert spec.to_dict()["env"] == {"WANDB_KEY": "<hidden>"}
    assert spec.to_dict(redact=False)["env"] == {"WANDB_KEY": SECRET}
    assert SECRET not in spec.to_yaml()


def test_dry_run_matches_the_create_description() -> None:
    described = ns.Job(_spec(name="train")).dry_run(**WHERE)
    assert described == {
        "image": CUDA,
        "command": ["python", "train.py", "--epochs", "3"],
        "args": "train.py --epochs 3",
        "project": "project-test-0001",
        "region": "eu-north1",
        "gpu": "h100",
        "timeout": 7200,
        "pricing": "on-demand",
        "volumes": ["s3://data:/workspace/data:ro"],
        "env": ["WANDB_KEY"],
        "name": "train",
        "disk": "250Gi",
        "shm": "16Gi",
        "secrets": {"HF_TOKEN": "mbsec-test-0001"},
    }


def test_server_dry_run_sends_dry_run_and_creates_nothing() -> None:
    fake = FakeTransport()
    job = ns.Job(_spec(name="train"))
    described = job.dry_run(server=True, transport=fake, **WHERE)
    assert described["server"] == {"accepted": True, "request_id": None}
    assert fake.bodies == []
    assert len(fake.dry_runs) == 1
    assert fake.dry_runs[0]["metadata"] == {
        "parent_id": "project-test-0001",
        "name": "train",
    }
    assert job.id is None


def test_a_rejected_server_dry_run_raises() -> None:
    fake = FakeTransport()
    fake.failure = "rejected"
    with pytest.raises(ns.ValidationError, match="Nebius rejected the job"):
        ns.Job(_spec()).dry_run(server=True, transport=fake, **WHERE)
    assert fake.bodies == []


def test_the_sdk_request_carries_dry_run() -> None:
    fake = FakeTransport()
    ns.Job(_spec(env={})).dry_run(server=True, transport=fake, **WHERE)
    body = fake.dry_runs[0]
    assert build_create_request(body, dry_run=True).dry_run is True
    assert build_create_request(body).dry_run is False
    endpoint_fake = FakeEndpointTransport()
    ns.Endpoint(_endpoint_spec(env={})).dry_run(
        server=True, transport=endpoint_fake, **WHERE
    )
    endpoint_body = endpoint_fake.dry_runs[0]
    request = build_endpoint_request(endpoint_body, dry_run=True)
    assert request.dry_run is True


def test_create_returns_self_and_runs_once() -> None:
    fake = FakeTransport()
    job = ns.Job(_spec())
    assert job.id is None and job.attempts == []
    assert job.create(transport=fake, quiet=True, **WHERE) is job
    assert job.id == "job-1"
    assert job.name is not None and job.name.startswith("job-")
    with pytest.raises(ns.ValidationError, match="already created"):
        job.create(transport=fake, quiet=True, **WHERE)
    assert len(fake.bodies) == 1


def test_each_job_from_one_spec_gets_its_own_name() -> None:
    fake = FakeTransport()
    spec = _spec()
    first = ns.Job(spec).create(transport=fake, quiet=True, **WHERE)
    second = ns.Job(spec).create(transport=fake, quiet=True, **WHERE)
    assert (first.id, second.id) == ("job-1", "job-2")
    assert first.name != second.name


@pytest.mark.parametrize("call", ["get", "wait", "cancel", "display"])
def test_calls_before_create_ask_for_create(call: str) -> None:
    job = ns.Job(_spec())
    with pytest.raises(ns.ValidationError, match=r"Call create\(\)"):
        getattr(job, call)()


def test_a_loaded_job_has_no_spec() -> None:
    fake = FakeTransport()
    fake.states = ["RUNNING"]
    job = ns.jobs.get("aijob-test-0001", transport=fake, quiet=True)
    assert job.id == "aijob-test-0001"
    assert job.status == "RUNNING"
    for call in (
        lambda: job.spec,
        lambda: job.dry_run(),
        lambda: job.to_cli(),
        lambda: job.create(transport=fake),
    ):
        with pytest.raises(ns.ValidationError):
            call()
    assert fake.bodies == []


def test_preemption_retries_needs_spot() -> None:
    with pytest.raises(ns.ValidationError, match="needs pricing='spot'"):
        _spec(preemption_retries=2)
    assert _spec(preemption_retries=0).preemption_retries == 0
    with pytest.raises(ns.ValidationError, match="0 or more"):
        _spec(pricing="spot", preemption_retries=-1)


def test_preemption_retries_sets_the_resubmit_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    preempted = f"ERROR:{PREEMPTED_CODE}"
    fake = FakeTransport()
    fake.states = [preempted, preempted, "COMPLETED"]
    spec = _spec(pricing="spot", preemption_retries=1, restart_attempts=0)
    job = ns.Job(spec).create(transport=fake, quiet=True, **WHERE)
    record = job.wait(quiet=True)
    assert record["status"] == {
        "state": "ERROR",
        "state_details": {"code": PREEMPTED_CODE},
    }
    assert job.attempts == ["job-1", "job-2"]


def test_yaml_round_trip(tmp_path: Path) -> None:
    spec = _spec(pricing="spot", preemption_retries=2, name="train")
    path = tmp_path / "job.yaml"
    path.write_text(spec.to_yaml(redact=False), encoding="utf-8")
    assert ns.JobSpec.from_yaml(path) == spec
    text = spec.to_yaml()
    assert "timeout: 2h" in text
    assert "<hidden>" in text


def test_endpoint_spec_is_data_with_a_secret_token() -> None:
    spec = _endpoint_spec()
    assert TOKEN not in repr(spec)
    assert SECRET not in repr(spec)
    assert spec.token_text == TOKEN
    assert spec.clone() == spec
    assert spec.clone(port=9000).port == 9000
    assert spec.to_dict()["token"] == "<hidden>"
    assert spec.to_dict(redact=False)["token"] == TOKEN
    assert TOKEN not in spec.to_yaml(redact=False)
    with pytest.raises(ns.ValidationError, match="exactly one of port"):
        spec.clone(ports=[8000, "6379/tcp"])


def test_endpoint_creates_once_and_loaded_has_no_spec() -> None:
    fake = FakeEndpointTransport()
    endpoint = ns.Endpoint(_endpoint_spec())
    assert endpoint.id is None and endpoint.token is None
    with pytest.raises(ns.ValidationError, match=r"Call create\(\)"):
        endpoint.wait()
    described = endpoint.dry_run(**WHERE)
    assert TOKEN not in str(described) and SECRET not in str(described)
    assert endpoint.create(transport=fake, quiet=True, **WHERE) is endpoint
    assert (endpoint.id, endpoint.token) == ("endpoint-1", TOKEN)
    with pytest.raises(ns.ValidationError, match="already created"):
        endpoint.create(transport=fake, quiet=True, **WHERE)
    assert len(fake.bodies) == 1
    loaded = ns.endpoints.get("endpoint-1", transport=fake, quiet=True)
    with pytest.raises(ns.ValidationError, match="spec is not known"):
        loaded.dry_run()
    with pytest.raises(ns.ValidationError, match="spec is not known"):
        loaded.create(transport=fake)
