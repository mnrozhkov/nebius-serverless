"""Serve a small model with vLLM on an endpoint, call it, then delete it.

``gpu="h100"`` with token auth. Uses the active Nebius profile. The same
recipe as YAML is ``endpoint.yaml`` in this folder.

The endpoint bills for compute while it runs. This script deletes it in
``finally``, including when a step fails or you press Ctrl-C.
"""

import json
import urllib.request

import nebius_serverless as ns

MODEL = "Qwen/Qwen3-0.6B"


def main() -> None:
    """Create the endpoint, wait for the model, send one request, delete."""

    # Describe what should serve. Omit command to keep the image entrypoint.
    spec = ns.EndpointSpec(
        command=[
            "bash",
            "-c",
            "python3 -m vllm.entrypoints.openai.api_server "
            f"--model {MODEL} --host 0.0.0.0 --port 8000",
        ],
        image="vllm/vllm-openai:v0.19.1",
        gpu="h100",
        port=8000,
        disk="500Gi",
        name="qwen-tiny",
    )

    # Create one endpoint. Its generated token is endpoint.token.
    endpoint = ns.Endpoint(spec).create()
    try:
        # RUNNING with a URL, then /v1/models answers: the model is loaded.
        endpoint.wait(probe="/v1/models")
        # ERROR and STOPPED end the wait as data. An ERROR record can still
        # list the URL, so check the state, not the URL.
        if endpoint.status != "RUNNING":
            raise SystemExit(f"{endpoint.id} ended {endpoint.status}")
        print(_chat(endpoint, "Say hello in five words."))
    finally:
        endpoint.delete()


def _chat(endpoint: ns.Endpoint, prompt: str) -> str:
    assert endpoint.url is not None
    body = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 64,
    }
    request = urllib.request.Request(
        endpoint.url.rstrip("/") + "/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {endpoint.token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    return payload["choices"][0]["message"]["content"]


if __name__ == "__main__":
    main()
