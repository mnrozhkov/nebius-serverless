# Prefect

`PrefectFlow` runs a function that calls [tasks](../reference/tasks.md) as a [Prefect](https://www.prefect.io/) flow. By default the flow runs on its own small Nebius job, which starts the tasks' jobs, so it keeps going after your script ends. With `remote_job=False` it runs in your process. Either way, every task call is a Prefect task run and a Nebius job.

```python
from nebius_serverless.integrations.prefect import PrefectFlow
```

Importing `nebius_serverless` does not load this integration. Running a flow on a Nebius job needs nothing more on your machine. Running it here (`remote_job=False`) needs Prefect:

```bash
pip install "nebius-serverless[prefect]"
```

## PrefectFlow

```python
PrefectFlow(
    function,
    *,
    flow_name,
    run_name=None,
    api_url=None,
    remote_job=True,
    image=None,
    **job_options,
)
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `function` | yes | The pipeline: a plain function, not `async`. A function with Prefect's `@flow` is accepted too, and the decorator is dropped. |
| `flow_name` | yes | The flow's name in Prefect. |
| `run_name` | no | A name for the runs. Defaults to `flow_name`. Each run is named `<UTC launch time>-<run_name>`, such as `20261125-091833-mnist-baseline`, in Prefect and as the flow job. |
| `api_url` | no | The Prefect server's API address, such as `https://prefect.example.com/api`. Defaults to `PREFECT_API_URL`; one of the two is required. |
| `remote_job` | no | `True` (default) runs the flow on a Nebius job. `False` runs it in this process. |
| `image` | no | An [`ns.Image`](../reference/images.md) or an image reference for the flow job. Defaults to a slim Python image with Prefect, built once per project. Not used with `remote_job=False`. |
| `**job_options` | no | Options of the flow job, as on [`JobSpec`](../reference/jobs.md#jobspec): `timeout` (default `24h`), `cpu`, `disk`, and the rest. Without `gpu` or `cpu` it runs on the smallest CPU machine. With `remote_job=False` only `profile` and `project` are used. |

Constructing a `PrefectFlow` does not contact Nebius. After construction these attributes are readable: `function`, `flow_name`, `run_name`, `api_url`, `remote_job`, `image`.

### What the function may use

The function's source is sent to the flow job, so it is checked when the `PrefectFlow` is made. From outside its own body it may use:

- `ns.Task` objects. Inside the flow each one is a Prefect task: `task(...)` waits for the result, `task.submit(...)` returns a Prefect future, and a future passed to another task is resolved by Prefect.
- Constants with JSON values, such as a list of learning rates.
- What it imports inside its body.

Its arguments and its return value must be JSON values.

```python
def pipeline(epochs: int = 5) -> dict:
    data = prepare.submit(dataset="mnist")
    runs = [train.submit(data=data, lr=lr, epochs=epochs) for lr in RATES]
    return pick(runs=runs)

flow = PrefectFlow(pipeline, flow_name="mnist-baseline", timeout="3h")
run = flow.submit(epochs=5)
```

### `PrefectFlow.submit`

```python
flow.submit(*args, **kwargs) -> FlowRun
```

Start the flow and return its run at once.

1. Checks the arguments, and that the Prefect server answers and accepts the login. It stops if the server runs an older Prefect than the flow would use.
2. Creates the flow run in Prefect, and prints its link. The run stays Pending until the flow starts. When the server cannot be reached from here, there is no link, and the flow creates its run when it starts.
3. Builds the images and uploads the `include_libs` of the tasks the function uses, when they changed. This happens here, so the flow job needs none of the tasks' code.
4. With `remote_job=True`, stores the credentials and starts the flow job. If that fails, the flow run is marked Crashed. With `remote_job=False`, runs the flow in a thread; the script does not end before the flow does.

### `PrefectFlow.__call__`

```python
flow(*args, **kwargs) -> Any
```

Run the flow, wait for it, and return the function's result. Ctrl-C cancels the flow job. Task jobs it already started are not cancelled.

## FlowRun

One run of a flow, returned by `submit`.

| Member | Meaning |
| --- | --- |
| `result()` | Wait for the flow and return the function's result. Raises `TaskFailed` when the flow job failed, or what the flow raised here. |
| `cancel()` | Cancel the flow job. A flow in this process is not stopped. |
| `name` | The UTC launch time. |
| `title` | The flow run's name in Prefect. |
| `inputs` | The function's arguments by name. |
| `flow_run_id`, `flow_run_url` | The flow run in Prefect and its page in the UI, or `None` when it could not be created at submit. |
| `output_path`, `output_uri` | The run's folder in the task bucket: `flow/` for the flow job, and one folder per task job. |
| `job` | The flow job, a [`Job`](../reference/jobs.md#job), or `None` for a flow in this process. `job_run` is its [`TaskRun`](../reference/tasks.md#taskrun). |

## Task runs in Prefect

Each task call is a Prefect task run. A failure raises `TaskFailed`, so Prefect marks the task run failed. Each task run gets three Markdown artifacts:

- `nebius-job`: the job id, its name, the command for its logs, and where its files are.
- `inputs`, when the job has started.
- `outputs`, when it has ended.

The same goes to the task run's log.

## Server and login

| Variable | Meaning |
| --- | --- |
| `PREFECT_API_URL` | The server's API address, when `api_url` is not passed. |
| `PREFECT_API_AUTH_STRING` | The server's login, `user:password`. Also read from your local Prefect settings, or from one line in `~/.config/prefect/auth`. |
| `PREFECT_UI_URL` | The Prefect UI's address, for links. Defaults to the API address without `/api`. |
| `NS_CONSOLE_JOB_URL` | The console address of a job, with `{project}` and `{job}` in it. When set, each task run's `nebius-job` artifact links to its job. |

The flow job starts jobs as you. Each run stores a fresh access token, and the Prefect login when there is one, as a new version of the `generated-tasks-credentials` MysteryBox secret. The job reads its own version as environment variables. Versions are not deleted.

## Files

A run keeps its files in `flows/<flow>/<run>/` in the task bucket: `flow/` for the flow job, and one folder per task job, named after the job.

See [First Prefect flow](../quickstarts/first-flow.md) for a runnable example, and [Pipeline of tasks](../training/task-pipeline.md) for a full one.
