# nebius-serverless

[![PyPI](https://img.shields.io/pypi/v/nebius-serverless)](https://pypi.org/project/nebius-serverless/)
[![Python](https://img.shields.io/pypi/pyversions/nebius-serverless)](https://pypi.org/project/nebius-serverless/)
[![Docs](https://img.shields.io/badge/docs-mnrozhkov.github.io-3f51b5)](https://mnrozhkov.github.io/nebius-serverless/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/mnrozhkov/nebius-serverless/blob/main/LICENSE)

Unofficial Python client for Nebius Serverless AI Jobs and Endpoints. Describe a container workload in Python or YAML, create it on a Nebius GPU or CPU, and wait for it from a script, a notebook, or CI.

> **Unofficial package.** Not an official Nebius product, and Nebius does not support it. For the official Python SDK, see [github.com/nebius/pysdk](https://github.com/nebius/pysdk). Report problems in this project's [GitHub issues](https://github.com/mnrozhkov/nebius-serverless/issues).
>
> **Beta.** 0.x releases may change the API.

<!-- --8<-- [start:install] -->
## Install

```bash
pip install nebius-serverless
```

Python 3.10 or later. It depends on `nebius>=0.6.43,<0.7`, `PyYAML>=6`, and `rich>=13`. The import name is `nebius_serverless`.

`create()` and `jobs.get()` need a Nebius CLI profile ([`nebius profile create`](https://docs.nebius.com/cli/configure)), or in CI a service account or `NEBIUS_IAM_TOKEN` ([Run in CI](https://mnrozhkov.github.io/nebius-serverless/guides/ci/)). `ns.whoami()` shows the account, and the project and region `create()` will use, without creating anything.
<!-- --8<-- [end:install] -->

<!-- --8<-- [start:job] -->
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

`gpu="h100"` asks Nebius for one H100, and Nebius picks the platform and preset in the project's region. `create()` returns once Nebius has accepted the job. `job.wait()` polls until the job ends and prints each new state. `job.get()` fetches once, `job.cancel()` requests cancellation, and `ns.jobs.get(id)` reloads a job from a saved id. `create(dry_run=True)` returns the request and creates nothing. The same recipe loads from YAML with `ns.JobSpec.from_yaml("job.yaml")`. Logs are `nebius ai job logs <id>`: this library does not stream them.
<!-- --8<-- [end:job] -->

<!-- --8<-- [start:endpoint] -->
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

Endpoints require a bearer token by default: callers send `Authorization: Bearer {endpoint.token}`. A random token is generated on create, and `endpoint.reveal_token()` fetches it again later. For production, keep it in MysteryBox with `token_secret=`. `auth="none"` makes a public URL that anyone who has it can call, at your cost.
<!-- --8<-- [end:endpoint] -->

## Documentation

The documentation site is [mnrozhkov.github.io/nebius-serverless](https://mnrozhkov.github.io/nebius-serverless/):

- [🚀 Quickstarts](https://mnrozhkov.github.io/nebius-serverless/quickstarts/): install, first job, first endpoint, first image, notebooks
- [🏋️ Training](https://mnrozhkov.github.io/nebius-serverless/training/): fine-tuning with Axolotl, a parallel sweep
- [⚡ Inference](https://mnrozhkov.github.io/nebius-serverless/inference/): serve a model with vLLM
- [📖 Guides](https://mnrozhkov.github.io/nebius-serverless/guides/): machines, storage, secrets, retries, CI, YAML and the CLI
- [📚 Reference](https://mnrozhkov.github.io/nebius-serverless/reference/): every class, function, and error

The runnable scripts are in [`examples/`](https://github.com/mnrozhkov/nebius-serverless/tree/main/examples), and the pages themselves in [`docs/`](https://github.com/mnrozhkov/nebius-serverless/tree/main/docs).

## Work on the client

From a checkout, [uv](https://docs.astral.sh/uv/) installs the package with the locked dependencies (`nebius==0.6.43`, `PyYAML==6.0.3`, `rich==15.0.0`) and the test and lint tools:

```bash
uv sync
uv run pytest
uv run python examples/quickstarts/job_quickstart.py
uv run --group docs mkdocs serve   # the documentation site, locally
```

## License

Apache-2.0. Copyright 2026 Mikhail Rozhkov and Evgeny Zholkovskiy. See [LICENSE](https://github.com/mnrozhkov/nebius-serverless/blob/main/LICENSE) and [NOTICE](https://github.com/mnrozhkov/nebius-serverless/blob/main/NOTICE).
