# nebius-serverless

Python client for Nebius Serverless AI Jobs and Endpoints.

> **Community project — not an official Nebius SDK.** For the official SDK, see [github.com/nebius/pysdk](https://github.com/nebius/pysdk).
>
> **Beta.** 0.x releases may change the API.

## Installation

Python 3.10 or later:

```bash
pip install nebius-serverless
```

It depends on `nebius>=0.6.43,<0.7`, `PyYAML>=6`, and `rich>=13`. The import name is `nebius_serverless`.

To work on the client or run the examples, use [uv](https://docs.astral.sh/uv/) from a checkout of this repo:

```bash
uv sync
```

`uv sync` installs this package with the locked dependencies (`nebius==0.6.43`, `PyYAML==6.0.3`, `rich==15.0.0`) and the locked test and lint tools.

Run the client and the example scripts with `uv run`:

```bash
uv run python
uv run python examples/job_quickstart.py
uv run python examples/from_yaml/nvidia_smi_yaml.py --config examples/from_yaml/nvidia_smi.yaml
```

`ns.whoami()` shows the account and the project and region `create()` will use. A Nebius CLI profile, or for CI a service account or `NEBIUS_IAM_TOKEN` (see [`ns.configure`](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/reference.md#configure)), is required for `create()` and `jobs.get()`. Configure one with [`nebius profile create`](https://docs.nebius.com/cli/configure). The active profile supplies the project id. When that profile does not name a region, `create()` reads it from the project (`spec.region` in the Nebius Python SDK). `profile=`, `project=`, and `region=` on `JobSpec` select or override them. Constructing a `JobSpec` does not read that profile.

`gpu="h100"` (or `"h100:8"`, or `cpu="amd:16"`) asks Nebius for a machine, and Nebius picks the platform and preset in the project's region; see [Machine selection](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/reference.md#machine-selection). Logs are `nebius ai job logs <id>`. This library does not stream logs.

`create()`, `get()`, `wait()`, and `cancel()` print progress on stderr. `quiet=True` suppresses progress for that call only. `ns.configure_logging()` turns diagnostics on separately, and `ns.diagnostics()` returns local metadata, including whether stderr is `plain` or `interactive`. A local timeout does not cancel the remote job.

[`examples/job_quickstart.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/job_quickstart.py) is the recipe below. [`examples/from_yaml/nvidia_smi_yaml.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/from_yaml/nvidia_smi_yaml.py) `--config` loads [`examples/from_yaml/nvidia_smi.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/from_yaml/nvidia_smi.yaml). The other example settings live in the Python file.

```python
import nebius_serverless as ns

spec = ns.JobSpec(
    ["nvidia-smi"],
    image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
    gpu="h100",
    timeout="1h",
)
job = spec.create()
job.wait()
job.display()
```

`quiet=True` on `create` and `wait` suppresses progress for those calls. Stdout still prints the id and the final state. The same pattern is in [Quiet progress](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/examples.md#quiet-progress):

```python
job = spec.create(quiet=True)
print(job.id)
record = job.wait(quiet=True)
print(job.id, record["status"]["state"])
```

`job.get()`, `job.cancel()`, and `ns.jobs.get(id)` are in [docs/examples.md](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/examples.md).

The same recipe can be loaded from YAML:

```python
spec = ns.JobSpec.from_yaml("job.yaml")
job = spec.create()
```

`create(dry_run=True)` returns the create request and does not create a job. The example scripts call `create()` and use the active profile.

## Endpoints

An endpoint is a container that stays up and answers on a managed `https://` URL. It bills for compute while it runs, until you stop or delete it. `ns.EndpointSpec` is the recipe, `Endpoint` is the created endpoint, and `ns.endpoints.get(id)` reloads one. GPU aliases, pricing, volumes, env, profiles, progress, and errors work as they do for jobs.

```python
spec = ns.EndpointSpec(
    image="vllm/vllm-openai:v0.19.1",
    command=["bash", "-c", "vllm serve Qwen/Qwen3-0.6B --port 8000"],
    gpu="h100",
    port=8000,
)
endpoint = spec.create()
endpoint.wait(probe="/v1/models")  # RUNNING, and the server answers
print(endpoint.url, endpoint.token)
endpoint.delete()
```

`command` is optional and keeps the image entrypoint when omitted. Endpoints require a bearer token by default (`auth="token"`): callers send `Authorization: Bearer {endpoint.token}`. Without `token=`, a random token is generated on create and kept in `endpoint.token`; `endpoint.reveal_token()` fetches it again later, for example on a handle from `ns.endpoints.get(id)`. For production, keep it in MysteryBox with `token_secret=`. `auth="none"` makes a public URL that anyone who has it can call, at your cost. `EndpointSpec.from_yaml` loads a `kind: endpoint` document. [`examples/endpoint_quickstart.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/endpoint_quickstart.py) serves a model, calls it, and deletes it. The full surface is in [docs/endpoints.md](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/endpoints.md).

## Documentation

The documentation site is [mnrozhkov.github.io/nebius-serverless](https://mnrozhkov.github.io/nebius-serverless/). The same pages are in `docs/`:

- [docs/reference.md](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/reference.md) — classes and methods on the current jobs surface
- [docs/examples.md](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/examples.md) — worked examples for the scripts under `examples/`
- [docs/endpoints.md](https://github.com/mnrozhkov/nebius-serverless/blob/main/docs/endpoints.md) — `EndpointSpec`, `Endpoint`, and `endpoints`

## License

Apache-2.0. Copyright 2026 Mikhail Rozhkov and Evgeny Zholkovskiy. See [LICENSE](https://github.com/mnrozhkov/nebius-serverless/blob/main/LICENSE) and [NOTICE](https://github.com/mnrozhkov/nebius-serverless/blob/main/NOTICE).
