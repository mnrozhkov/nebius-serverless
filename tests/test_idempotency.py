"""Create is safe to retry: one idempotency key per call, resent with the
same key on an ambiguous outcome, reusable across processes."""

from types import SimpleNamespace

import pytest
from fakes import FakeEndpointTransport, FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.sdk_transport import SdkTransport
from nebius_serverless.core.transport import CreateResult

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
UNCERTAIN = CreateResult(accepted=True, failure="uncertain")
LOST_ID = CreateResult(accepted=True, resource_id=None)


def _job(**kwargs: object) -> ns.JobSpec:
    return ns.JobSpec(
        ["true"],
        image="busybox:1.37",
        timeout="10m",
        **kwargs,  # type: ignore[arg-type]
    )


def _name(body: dict[str, object]) -> str:
    metadata = body["metadata"]
    assert isinstance(metadata, dict)
    return str(metadata["name"])


def test_each_call_has_its_own_key() -> None:
    fake = FakeTransport()
    _job().create(transport=fake, quiet=True, **WHERE)
    _job().create(transport=fake, quiet=True, **WHERE)
    assert len(set(fake.keys)) == 2


@pytest.mark.parametrize("first", [UNCERTAIN, LOST_ID])
def test_an_ambiguous_create_is_resent_with_the_same_key(
    first: CreateResult,
) -> None:
    fake = FakeTransport()
    fake.outcomes = [first]
    job = _job().create(transport=fake, quiet=True, **WHERE)
    assert job.id == "job-1"
    assert len(fake.bodies) == 2
    assert fake.bodies[0] == fake.bodies[1]
    assert fake.keys[0] == fake.keys[1]


def test_three_ambiguous_outcomes_say_how_to_retry_safely() -> None:
    fake = FakeTransport()
    fake.outcomes = [UNCERTAIN, UNCERTAIN, UNCERTAIN]
    with pytest.raises(ns.SubmissionUncertain) as caught:
        _job().create(transport=fake, quiet=True, **WHERE)
    key = fake.keys[0]
    assert caught.value.idempotency_key == key
    assert fake.keys == [key, key, key]
    assert "failed before create was accepted" not in str(caught.value)
    assert f"create(idempotency_key={key!r})" in caught.value.recovery


@pytest.mark.parametrize("failure", ["rejected", "capacity", "access"])
def test_definite_failures_are_not_resent(failure: str) -> None:
    fake = FakeTransport()
    fake.failure = failure
    with pytest.raises(ns.Error):
        _job().create(transport=fake, quiet=True, **WHERE)
    assert len(fake.bodies) == 1


def test_a_caller_key_is_sent_and_names_the_resource() -> None:
    fake = FakeTransport()
    for _ in range(2):
        _job().create(transport=fake, quiet=True, idempotency_key="k", **WHERE)
    assert fake.keys == ["k", "k"]
    assert _name(fake.bodies[0]) == _name(fake.bodies[1])
    assert _name(fake.bodies[0]).startswith("job-")
    described = _job().create(dry_run=True, idempotency_key="k", **WHERE)
    assert described["name"] == _name(fake.bodies[0])
    named = _job(name="mine").create(
        transport=fake, quiet=True, idempotency_key="k2", **WHERE
    )
    assert named.name == "mine"


@pytest.mark.parametrize("bad", ["", "has space", "x" * 129, 5])
def test_a_bad_key_is_rejected(bad: object) -> None:
    with pytest.raises(ns.ValidationError, match="idempotency_key"):
        _job().create(
            transport=FakeTransport(),
            quiet=True,
            idempotency_key=bad,  # type: ignore[arg-type]
            **WHERE,
        )


def test_a_key_on_a_generated_token_endpoint_needs_a_known_token() -> None:
    def spec(**kwargs: object) -> ns.EndpointSpec:
        return ns.EndpointSpec(
            image="vllm/vllm-openai:v0.19.1",
            port=8000,
            **kwargs,  # type: ignore[arg-type]
        )

    fake = FakeEndpointTransport()
    with pytest.raises(ns.ValidationError, match="token_secret"):
        spec().create(
            transport=fake, quiet=True, idempotency_key="k", **WHERE
        )
    assert fake.bodies == []
    known = (
        {"token": "chosen"},
        {"token_secret": "mbsec-x"},
        {"auth": "none"},
    )
    for ok in known:
        spec(**ok).create(
            transport=fake, quiet=True, idempotency_key="k", **WHERE
        )
    spec().create(transport=fake, quiet=True, **WHERE)  # auto key is fine


def test_the_sdk_transport_sends_the_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[object] = []

    class Client:
        def create(self, request: object, **kwargs: object) -> object:
            sent.append(kwargs.get("metadata"))
            operation = SimpleNamespace(resource_id="aijob-test-0001")
            return SimpleNamespace(wait=lambda: operation)

    monkeypatch.setattr(SdkTransport, "_client", lambda self: Client())
    job = _job().create(
        transport=SdkTransport(profile=None),
        quiet=True,
        idempotency_key="k-1",
        **WHERE,
    )
    assert job.id == "aijob-test-0001"
    assert sent == [[("x-idempotency-key", "k-1")]]
