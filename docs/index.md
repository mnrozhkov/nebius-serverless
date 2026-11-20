---
hide:
  - navigation
---

# nebius-serverless

Python client for Nebius Serverless AI Jobs and Endpoints. Describe a container workload in Python or YAML, create it on a Nebius GPU or CPU, and wait for it from a script, a notebook, or CI.

!!! warning "Community project, not an official Nebius SDK"
    For the official SDK, see [github.com/nebius/pysdk](https://github.com/nebius/pysdk).

!!! note "Beta"
    0.x releases may change the API.

<div class="grid cards" markdown>

-   **[Reference](reference.md)**

    `JobSpec`, `Job`, `jobs`, machine selection, auth for CI, errors, and the CLI and API map.

-   **[Endpoints](endpoints.md)**

    `EndpointSpec`, `Endpoint`, and `endpoints`: long-running containers on a managed `https://` URL.

-   **[Examples](examples.md)**

    Worked examples: quickstart, fine-tuning, a parallel sweep, batch jobs, secrets, and notebooks.

</div>

## Install

Python 3.10 or later. The import name is `nebius_serverless`.

=== "pip"

    ```bash
    pip install nebius-serverless
    ```

=== "uv"

    ```bash
    uv add nebius-serverless
    ```

It depends on `nebius>=0.6.43,<0.7`, `PyYAML>=6`, and `rich>=13`.

## Authenticate

`create()` and `jobs.get()` need a Nebius CLI profile, or for CI a service account or `NEBIUS_IAM_TOKEN` (see [`ns.configure`](reference.md#configure)). Configure a profile with [`nebius profile create`](https://docs.nebius.com/cli/configure). The active profile supplies the project id. When that profile does not name a region, `create()` reads it from the project. `profile=`, `project=`, and `region=` on `JobSpec` select or override them.

`ns.whoami()` shows the account and the project and region `create()` will use, without creating anything.

## Run a job

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

`gpu="h100"` (or `"h100:8"`, or `cpu="amd:16"`) asks Nebius for a machine, and Nebius picks the platform and preset in the project's region; see [Machine selection](reference.md#machine-selection). `create(dry_run=True)` returns the create request and does not create a job. The same recipe loads from YAML with `ns.JobSpec.from_yaml("job.yaml")`.

`create()`, `get()`, `wait()`, and `cancel()` print progress on stderr; `quiet=True` suppresses it for that call ([Quiet progress](examples.md#quiet-progress)). A local timeout does not cancel the remote job. Logs are `nebius ai job logs <id>`: this library does not stream logs.

## Serve an endpoint

An endpoint is a container that stays up and answers on a managed `https://` URL. It bills for compute while it runs, until you stop or delete it.

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

Endpoints require a bearer token by default (`auth="token"`): callers send `Authorization: Bearer {endpoint.token}`. Without `token=`, a random token is generated on create; `endpoint.reveal_token()` fetches it again later. For production, keep it in MysteryBox with `token_secret=`. `auth="none"` makes a public URL that anyone who has it can call, at your cost. See [Endpoints](endpoints.md).

## Work on the client

From a checkout of the [repository](https://github.com/mnrozhkov/nebius-serverless), [uv](https://docs.astral.sh/uv/) installs the package with the locked dependencies and the test and lint tools:

```bash
uv sync
uv run python examples/job_quickstart.py
```

## License

Apache-2.0. Copyright 2026 Mikhail Rozhkov and Evgeny Zholkovskiy. See [LICENSE](https://github.com/mnrozhkov/nebius-serverless/blob/main/LICENSE) and [NOTICE](https://github.com/mnrozhkov/nebius-serverless/blob/main/NOTICE).
