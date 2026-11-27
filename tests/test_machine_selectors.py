"""gpu= / cpu= are sent as the API's selectors; Nebius picks the machine."""

from pathlib import Path

import pytest
from fakes import FakeEndpointTransport, FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.sdk_transport import (
    build_create_request,
    build_endpoint_request,
)

WHERE = {"region": "eu-north1", "project": "project-test-0001"}


def _job(**kwargs: object) -> ns.JobSpec:
    return ns.JobSpec(
        command=["nvidia-smi"],
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
    (_job, ns.Job, FakeTransport, build_create_request),
    (_endpoint, ns.Endpoint, FakeEndpointTransport, build_endpoint_request),
]


@pytest.mark.parametrize(("build", "handle", "fake_type", "to_proto"), CASES)
@pytest.mark.parametrize(
    ("kwargs", "field", "value", "shm"),
    [
        ({"gpu": "h100:8"}, "gpu", "h100:8", 16 * 1024**3),
        ({"gpu": "h100"}, "gpu", "h100", 16 * 1024**3),
        ({"cpu": "amd:16"}, "cpu", "amd:16", None),
    ],
)
def test_selectors_are_sent_without_platform_or_preset(
    build: object,
    handle: type,
    fake_type: type,
    to_proto: object,
    kwargs: dict[str, object],
    field: str,
    value: str,
    shm: int | None,
) -> None:
    recipe = build(**kwargs)  # type: ignore[operator]
    described = handle(recipe).dry_run(**WHERE)
    assert described[field] == value
    assert "platform" not in described and "preset" not in described
    fake = fake_type()
    handle(recipe).create(transport=fake, quiet=True, **WHERE)
    spec = fake.bodies[0]["spec"]
    assert spec[field] == [value]
    assert "platform" not in spec and "preset" not in spec
    assert spec.get("shm_size_bytes") == shm
    proto = to_proto(fake.bodies[0]).spec  # type: ignore[operator]
    assert list(getattr(proto, field)) == [value]
    assert proto.platform == "" and proto.preset == ""


@pytest.mark.parametrize("build", [_job, _endpoint])
@pytest.mark.parametrize(
    "kwargs",
    [
        {"gpu": "h100", "cpu": "amd:16"},
        {"gpu": "h100", "platform": "gpu-h100-sxm"},
        {"cpu": "amd:16", "preset": "2vcpu-8gb"},
        {"gpu": "h100:0"},
        {"gpu": ":8"},
        {"cpu": "amd"},
        {"cpu": "amd:0"},
        {"gpu": 100},
    ],
)
def test_bad_selectors_raise(build: object, kwargs: dict[str, object]) -> None:
    with pytest.raises(ns.ValidationError):
        build(**kwargs)  # type: ignore[operator]


@pytest.mark.parametrize("build", [_job, _endpoint])
def test_spot_on_a_cpu_selector_is_unsupported(build: object) -> None:
    with pytest.raises(ns.UnsupportedFeature, match="Spot"):
        build(cpu="amd:16", pricing="spot")  # type: ignore[operator]
    assert build(gpu="h100", pricing="spot").gpu == "h100"  # type: ignore


def test_yaml_takes_cpu(tmp_path: Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text(
        "schema_version: 1\nkind: job\nimage: a:b\ncommand: [x]\n"
        "timeout: 1h\ncpu: amd:16\n",
        encoding="utf-8",
    )
    assert ns.JobSpec.from_yaml(path).cpu == "amd:16"
