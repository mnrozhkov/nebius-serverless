"""Default names are generated per create; explicit names are kept."""

import re

import pytest
from fakes import FakeEndpointTransport, FakeTransport

import nebius_serverless as ns

WHERE = {"region": "eu-north1", "project": "project-test-0001"}


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


def _sent(body: dict[str, object]) -> str:
    metadata = body["metadata"]
    assert isinstance(metadata, dict)
    return str(metadata["name"])


@pytest.mark.parametrize(
    ("build", "fake_type", "prefix"),
    [
        (_job, FakeTransport, "job"),
        (_endpoint, FakeEndpointTransport, "endpoint"),
    ],
)
def test_each_create_sends_a_fresh_default_name(
    build: object, fake_type: type, prefix: str
) -> None:
    recipe = build()  # type: ignore[operator]
    assert recipe.name is None
    fake = fake_type()
    first = recipe.create(transport=fake, quiet=True, **WHERE)
    second = recipe.create(transport=fake, quiet=True, **WHERE)
    names = [_sent(body) for body in fake.bodies]
    assert names[0] != names[1]
    for name in names:
        assert re.fullmatch(rf"{prefix}-[0-9a-f]{{12}}", name)
    assert [first.name, second.name] == names
    assert recipe.name is None


@pytest.mark.parametrize(
    ("build", "fake_type"),
    [(_job, FakeTransport), (_endpoint, FakeEndpointTransport)],
)
def test_an_explicit_name_is_sent_on_every_create(
    build: object, fake_type: type
) -> None:
    recipe = build(name="x")  # type: ignore[operator]
    fake = fake_type()
    handles = [
        recipe.create(transport=fake, quiet=True, **WHERE) for _ in range(2)
    ]
    assert [_sent(body) for body in fake.bodies] == ["x", "x"]
    assert [handle.name for handle in handles] == ["x", "x"]
