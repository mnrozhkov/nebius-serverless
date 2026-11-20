"""Endpoint YAML rejects a plaintext bearer token and never echoes it."""

from pathlib import Path

import pytest

import nebius_serverless as ns

BASE = (
    "schema_version: 1\n"
    "kind: endpoint\n"
    "image: vllm/vllm-openai:v0.19.1\n"
    "port: 8000\n"
    "auth: token\n"
)


def test_a_plaintext_token_is_rejected_without_echoing_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "endpoint.yaml"
    path.write_text(BASE + "token: abc123\n", encoding="utf-8")
    with pytest.raises(ns.ValidationError, match="token_secret") as caught:
        ns.EndpointSpec.from_yaml(path)
    assert "abc123" not in str(caught.value)
    assert "abc123" not in repr(caught.value)
    assert caught.value.operation == "from_yaml"


def test_token_secret_still_loads(tmp_path: Path) -> None:
    path = tmp_path / "endpoint.yaml"
    path.write_text(BASE + "token_secret: mbsec-test-0001\n", encoding="utf-8")
    spec = ns.EndpointSpec.from_yaml(path)
    assert spec.token_secret == "mbsec-test-0001"
    assert spec.token is None
