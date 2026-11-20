"""Shared memory: 16Gi on GPU, none on CPU, and shm= overrides both."""

from pathlib import Path

import pytest
from fakes import FakeEndpointTransport, FakeTransport

import nebius_serverless as ns
from nebius_serverless.sdk_transport import (
    build_create_request,
    build_endpoint_request,
)

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
GIB = 1024**3


def _job(**kwargs: object) -> ns.JobSpec:
    return ns.JobSpec(
        ["nvidia-smi"],
        image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
        timeout="1h",
        **kwargs,  # type: ignore[arg-type]
    )


def _endpoint(**kwargs: object) -> ns.EndpointSpec:
    return ns.EndpointSpec(
        image="vllm/vllm-openai:v0.19.1",
        port=8000,
        **kwargs,  # type: ignore[arg-type]
    )


CASES = [
    (_job, FakeTransport, build_create_request),
    (_endpoint, FakeEndpointTransport, build_endpoint_request),
]


@pytest.mark.parametrize(("build", "fake_type", "to_proto"), CASES)
@pytest.mark.parametrize(
    ("kwargs", "label", "sent"),
    [
        ({}, None, None),
        ({"gpu": "h100"}, "16Gi", 16 * GIB),
        ({"shm": "1Gi"}, "1Gi", GIB),
        ({"gpu": "h100", "shm": "1Gi"}, "1Gi", GIB),
        ({"gpu": "h100", "shm": "0"}, "0", 0),
    ],
)
def test_shm_default_and_override(
    build: object,
    fake_type: type,
    to_proto: object,
    kwargs: dict[str, object],
    label: str | None,
    sent: int | None,
) -> None:
    recipe = build(**kwargs)  # type: ignore[operator]
    described = recipe.create(dry_run=True, **WHERE)
    assert described.get("shm") == label
    fake = fake_type()
    recipe.create(transport=fake, quiet=True, **WHERE)
    body = fake.bodies[0]
    assert body["spec"].get("shm_size_bytes") == sent
    proto = to_proto(body).spec  # type: ignore[operator]
    assert proto.shm_size_bytes == (sent or 0)


@pytest.mark.parametrize("build", [_job, _endpoint])
@pytest.mark.parametrize("value", ["lots", "-1", 1.5, True])
def test_shm_must_be_a_size(build: object, value: object) -> None:
    with pytest.raises(ns.ValidationError, match="shm"):
        build(shm=value)  # type: ignore[operator]


def test_yaml_accepts_shm(tmp_path: Path) -> None:
    job = tmp_path / "job.yaml"
    job.write_text(
        "schema_version: 1\nkind: job\nimage: a:b\ncommand: [x]\n"
        "timeout: 1h\nshm: 64Gi\n",
        encoding="utf-8",
    )
    assert ns.JobSpec.from_yaml(job).shm == "64Gi"
    endpoint = tmp_path / "endpoint.yaml"
    endpoint.write_text(
        "schema_version: 1\nkind: endpoint\nimage: a:b\nport: 80\nshm: 0\n",
        encoding="utf-8",
    )
    assert ns.EndpointSpec.from_yaml(endpoint).shm == "0"
