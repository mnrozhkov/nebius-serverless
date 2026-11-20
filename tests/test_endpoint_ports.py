"""ports= with protocols, the token-auth port rule, urls, and readiness."""

from pathlib import Path

import pytest
from fakes import FakeClock, FakeEndpointTransport

import nebius_serverless as ns
from nebius_serverless.sdk_transport import build_endpoint_request

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
HTTPS = "https://port8000-x.tunnel.example"
TLS = "tls://port6379-x.tunnel.example:443"


def _spec(**kwargs: object) -> ns.EndpointSpec:
    return ns.EndpointSpec(
        image="vllm/vllm-openai:v0.19.1",
        **kwargs,  # type: ignore[arg-type]
    )


def _body_ports(recipe: ns.EndpointSpec) -> list[dict[str, object]]:
    fake = FakeEndpointTransport()
    recipe.create(transport=fake, quiet=True, **WHERE)
    spec = fake.bodies[0]["spec"]
    assert isinstance(spec, dict)
    ports = spec["ports"]
    assert isinstance(ports, list)
    return ports


def test_five_ports_map_to_the_api() -> None:
    recipe = _spec(
        ports=[8000, "8001/http", "6379/tcp", "9000/UDP", "80:8080"],
        auth="none",
    )
    assert recipe.ports == (
        "8000/http",
        "8001/http",
        "6379/tcp",
        "9000/udp",
        "80:8080/http",
    )
    body = _body_ports(recipe)
    assert body == [
        {"container_port": 8000, "protocol": "HTTP"},
        {"container_port": 8001, "protocol": "HTTP"},
        {"container_port": 6379, "protocol": "TCP"},
        {"container_port": 9000, "protocol": "UDP"},
        {"container_port": 8080, "host_port": 80, "protocol": "HTTP"},
    ]
    fake = FakeEndpointTransport()
    recipe.create(transport=fake, quiet=True, **WHERE)
    proto = build_endpoint_request(fake.bodies[0]).spec.ports
    got = [(p.container_port, p.host_port, p.protocol.name) for p in proto]
    assert got == [
        (8000, 0, "HTTP"),
        (8001, 0, "HTTP"),
        (6379, 0, "TCP"),
        (9000, 0, "UDP"),
        (8080, 80, "HTTP"),
    ]
    described = recipe.create(dry_run=True, **WHERE)
    assert described["ports"] == list(recipe.ports)
    assert "port" not in described


def test_port_and_a_one_item_ports_list_send_the_same_request() -> None:
    assert _body_ports(_spec(port=8000)) == _body_ports(_spec(ports=[8000]))
    assert _spec(port=8000).port == 8000
    assert _spec(ports=[8000]).port is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"port": 8000, "ports": [8000]},
        {"port": 0},
        {"port": 65536},
        {"ports": ["8000/sctp"], "auth": "none"},
        {"ports": [], "auth": "none"},
        {"ports": "8000", "auth": "none"},
        {"ports": [8000, "8000/http"], "auth": "none"},
    ],
)
def test_bad_port_arguments_raise(kwargs: dict[str, object]) -> None:
    with pytest.raises(ns.ValidationError):
        _spec(**kwargs)


@pytest.mark.parametrize(
    "ports",
    [
        [8000, 8001],
        ["6379/tcp"],
        ["9000/udp"],
        # Live, 19 Nov 2026: "unsupported port protocol for auth/metrics
        # proxy: TCP" (and the same for UDP).
        [8000, "6379/tcp"],
        [8000, "9000/udp"],
    ],
)
def test_token_auth_needs_exactly_one_http_port(ports: list[object]) -> None:
    for kwargs in ({}, {"auth": "token"}, {"token_secret": "mbsec-x"}):
        with pytest.raises(ns.ValidationError, match="exactly one port"):
            _spec(ports=ports, **kwargs)
    assert _spec(ports=ports, auth="none").ports


def test_one_http_port_is_fine_with_token_auth() -> None:
    assert _spec(ports=["80:8080"]).auth == "token"


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr("nebius_serverless.poll.monotonic", clock.monotonic)
    monkeypatch.setattr("nebius_serverless.poll.sleep", clock.sleep)
    return clock


@pytest.mark.parametrize(
    ("protocols", "urls", "ready_at"),
    [
        (["TCP"], [None, TLS], 2),
        (["UDP"], [None], 1),
        (["HTTP", "TCP"], [HTTPS, f"{HTTPS} {TLS}"], 2),
        (["HTTP"], [None, HTTPS], 2),
    ],
)
def test_ready_means_a_url_for_each_protocol_that_gets_one(
    clock: FakeClock,
    protocols: list[str],
    urls: list[str | None],
    ready_at: int,
) -> None:
    fake = FakeEndpointTransport()
    fake.spec_ports = [
        {"container_port": 1000 + i, "protocol": p}
        for i, p in enumerate(protocols)
    ]
    fake.states = ["RUNNING"]
    fake.urls = urls
    endpoint = _spec(port=8000, auth="none").create(
        transport=fake, quiet=True, **WHERE
    )
    record = endpoint.wait(quiet=True)
    assert record["status"]["state"] == "RUNNING"
    assert len(fake.gets) == ready_at


def test_urls_lists_every_managed_url(clock: FakeClock) -> None:
    fake = FakeEndpointTransport()
    fake.urls = [f"{TLS} {HTTPS}"]
    endpoint = _spec(port=8000).create(transport=fake, quiet=True, **WHERE)
    assert endpoint.urls == []
    endpoint.get(quiet=True)
    assert endpoint.urls == [TLS, HTTPS]
    assert endpoint.url == HTTPS


def test_yaml_ports(tmp_path: Path) -> None:
    path = tmp_path / "endpoint.yaml"
    path.write_text(
        "schema_version: 1\nkind: endpoint\nimage: a:b\nauth: none\n"
        "ports: [8000, 6379/tcp]\n",
        encoding="utf-8",
    )
    assert ns.EndpointSpec.from_yaml(path).ports == ("8000/http", "6379/tcp")
    path.write_text(
        "schema_version: 1\nkind: endpoint\nimage: a:b\n"
        "port: 8000\nports: [8001]\n",
        encoding="utf-8",
    )
    with pytest.raises(ns.ValidationError, match="exactly one of port"):
        ns.EndpointSpec.from_yaml(path)
