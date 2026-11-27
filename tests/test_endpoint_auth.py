"""Endpoints default to token auth; a generated token gets a notice."""

import io
import logging
import sys

import pytest
from fakes import FakeEndpointTransport

import nebius_serverless as ns

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
VLLM = "vllm/vllm-openai:v0.19.1"
NOTICE = "Token generated for endpoint-1"


def _spec(**kwargs: object) -> ns.EndpointSpec:
    return ns.EndpointSpec(
        image=VLLM,
        port=8000,
        **kwargs,  # type: ignore[arg-type]
    )


def test_the_default_dry_run_asks_for_a_generated_token() -> None:
    described = ns.Endpoint(_spec()).dry_run(**WHERE)
    assert described["auth"] == "token"
    assert described["token"] == "generated"


def test_auth_none_is_an_explicit_public_url() -> None:
    described = ns.Endpoint(_spec(auth="none")).dry_run(**WHERE)
    assert described["auth"] == "none"
    assert "token" not in described
    fake = FakeEndpointTransport()
    endpoint = ns.Endpoint(_spec(auth="none")).create(
        transport=fake, quiet=True, **WHERE
    )
    assert endpoint.token is None
    body_spec = fake.bodies[0]["spec"]
    assert isinstance(body_spec, dict)
    assert "auth_token" not in body_spec


def test_a_generated_token_is_announced_once_without_its_value(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    caplog.set_level(logging.DEBUG, logger="nebius_serverless")
    endpoint = ns.Endpoint(_spec()).create(
        transport=FakeEndpointTransport(), **WHERE
    )
    token = endpoint.token
    assert token
    text = stream.getvalue()
    assert text.count(NOTICE) == 1
    assert "endpoint.token" in text
    assert "token_secret=" in text
    assert token not in text
    assert any(
        getattr(r, "event", None) == "token_generated" for r in caplog.records
    )
    assert all(token not in r.getMessage() for r in caplog.records)
    assert all(token not in str(r.__dict__) for r in caplog.records)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"token": "chosen-by-the-user"},
        {"token_secret": "mbsec-test-0001"},
        {"auth": "none"},
    ],
)
def test_no_notice_when_nothing_was_generated(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, object]
) -> None:
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    ns.Endpoint(_spec(**kwargs)).create(
        transport=FakeEndpointTransport(), **WHERE
    )
    assert "Token generated" not in stream.getvalue()


def test_quiet_suppresses_the_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    ns.Endpoint(_spec()).create(
        transport=FakeEndpointTransport(), quiet=True, **WHERE
    )
    assert stream.getvalue() == ""


def test_reveal_token_recovers_it_on_a_reloaded_handle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="nebius_serverless")
    fake = FakeEndpointTransport()
    created = ns.Endpoint(_spec()).create(
        transport=fake, quiet=True, **WHERE
    )
    reloaded = ns.endpoints.get(created.id, transport=fake, quiet=True)
    assert reloaded.token is None
    assert reloaded.reveal_token() == created.token
    assert reloaded.token == created.token
    assert fake.reveals == [created.id]
    record = reloaded.get(quiet=True)
    assert "auth_token" not in str(record)
    assert all(created.token not in str(r.__dict__) for r in caplog.records)


def test_reveal_token_is_none_without_a_plain_token() -> None:
    fake = FakeEndpointTransport()
    public = ns.Endpoint(_spec(auth="none")).create(
        transport=fake, quiet=True, **WHERE
    )
    assert public.reveal_token() is None
    secret = ns.Endpoint(_spec(token_secret="mbsec-test-0001")).create(
        transport=fake, quiet=True, **WHERE
    )
    assert secret.reveal_token() is None


def test_reveal_token_async_twin() -> None:
    import asyncio

    fake = FakeEndpointTransport()
    created = ns.Endpoint(_spec()).create(
        transport=fake, quiet=True, **WHERE
    )
    reloaded = ns.endpoints.get(created.id, transport=fake, quiet=True)
    assert asyncio.run(reloaded.reveal_token_async()) == created.token
