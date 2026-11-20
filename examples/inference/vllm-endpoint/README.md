# Serve a model with vLLM

Serves `Qwen/Qwen3-0.6B` with vLLM's OpenAI-compatible server on one H100 endpoint behind a bearer token, sends one chat request, and deletes the endpoint.

## What this example does

- [`serve.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/inference/vllm-endpoint/serve.py) creates the endpoint, waits until `/v1/models` answers, sends one request to `/v1/chat/completions`, prints the reply, and deletes the endpoint in `finally`, also when a step fails or you press Ctrl-C.
- [`endpoint.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/inference/vllm-endpoint/endpoint.yaml) is the same recipe as YAML.
- The endpoint uses token auth, the default: `create()` generates a random token, keeps it in `endpoint.token`, and callers send `Authorization: Bearer <token>`.

## Requirements

- A Nebius profile that can create endpoints: see [Install and authenticate](https://mnrozhkov.github.io/nebius-serverless/quickstarts/install/).
- H100 quota in the project's region.

## Runtime and compute

| | |
| --- | --- |
| Machine | `gpu="h100"`, on-demand |
| Port | `8000`, served at a managed `https://` URL |
| Disk | `500Gi` |
| Image | `vllm/vllm-openai:v0.19.1` |

An endpoint bills for compute from create until it is stopped or deleted. Closing the process does neither.

## Run it

```bash
uv run python examples/inference/vllm-endpoint/serve.py
```

The same recipe from YAML:

```python
import nebius_serverless as ns

spec = ns.EndpointSpec.from_yaml("examples/inference/vllm-endpoint/endpoint.yaml")
print(spec.create(dry_run=True))  # the create request; nothing is created
```

## Expected output

`create()` prints the endpoint id and a notice that a token was generated. `wait(probe="/v1/models")` prints each state, then the URL under `RUNNING`, then the probe's answers until the model responds. Pulling the image and loading the model take a few minutes. The script then prints the model's reply, and `delete()` prints that deletion was accepted.

## Call it yourself

This client does not wrap inference. Send HTTP to `endpoint.url` with the token:

```python
import urllib.request

request = urllib.request.Request(
    endpoint.url.rstrip("/") + "/v1/models",
    headers={"Authorization": f"Bearer {endpoint.token}"},
)
with urllib.request.urlopen(request) as response:
    print(response.status)
```

## Keep it, stop it, start it again

Save `endpoint.id` to come back later. `endpoints.get` fetches the endpoint and creates nothing; `reveal_token()` fetches its token:

```python
endpoint = ns.endpoints.get(saved_id)
token = endpoint.reveal_token()
```

A stopped endpoint is not billed for compute or its disk; mounted volumes still bill. It comes back at the same URL:

```python
endpoint.stop()
endpoint.wait(until="stopped")
endpoint.start()
endpoint.wait(probe="/v1/models")  # the model loads again
```

## How to adapt it

- Serve another model: change `--model` in the command, and `MODEL` in `serve.py`.
- Spot pricing: `pricing="spot"`. A preempted spot endpoint goes to `STOPPED`; `start()` resumes it.
- Keep the token in MysteryBox for production: `token_secret="mbsec-…"`, whose payload has `AUTH_TOKEN`. Clients then read the token from MysteryBox.
- `auth="none"` makes a public URL that anyone who has it can call, at your cost. Use it only on purpose.

## Troubleshooting

- **The endpoint ends in `ERROR` before `RUNNING`.** Usually there was no capacity for the machine. An endpoint in `ERROR` is not billed; `display()` shows the reason. Delete it, then retry later or in another region.
- **`RUNNING`, but requests fail.** `RUNNING` with a URL means the tunnel is up, not that vLLM has loaded the model. Wait with `probe=`, as `serve.py` does.
- **`AccessError` from the probe.** The endpoint answered 401 or 403: the handle has no token or the wrong one. Call `reveal_token()` or pass `token=` to `wait`.
- **`WaitTimeout` after 60 minutes.** The endpoint is unchanged and may still be billing. Call `wait()` again, or `delete()`.
