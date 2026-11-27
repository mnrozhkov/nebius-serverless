# Notebooks

Two notebooks walk through each lifecycle one cell at a time. Synchronous calls such as `job.wait()` work in a notebook cell; the notebooks use the awaited twins (`create_async`, `wait_async`, …), which keep the kernel responsive while they wait. See [Async and notebooks](../guides/async.md).

| Notebook | What it does |
| --- | --- |
| [`job_lifecycle.ipynb`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/quickstarts/notebooks/job_lifecycle.ipynb) | Runs one GPU job: checks the account, describes the job, dry-runs it, creates it, checks its status without blocking, waits, reads the logs, reloads it by id, and cancels it. |
| [`endpoint_quickstart.ipynb`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/quickstarts/notebooks/endpoint_quickstart.ipynb) | The vLLM endpoint from [First endpoint](first-endpoint.md): checks the account, creates the endpoint, waits until the model answers, sends a chat request, reloads it by id, stops and starts it, and deletes it. |

Both were run end to end on an H100, and GitHub shows them with their outputs.

## Run them

From a checkout of the repository:

```bash
uv sync
uv run --with jupyter jupyter lab examples/quickstarts/notebooks
```

You need a Nebius CLI profile and quota for one on-demand H100. The endpoint notebook bills from create until its last cell deletes the endpoint: interrupting a cell does not delete it.
