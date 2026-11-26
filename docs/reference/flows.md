# Prefect flows

`ns.PrefectFlow` runs a function that calls [tasks](tasks.md) as a [Prefect](https://www.prefect.io/) flow. By default the flow runs on its own small Nebius job, which starts the tasks' jobs, so it keeps going after your script ends. With `remote_job=False` it runs in your process. Either way, every task call is a Prefect task run and a Nebius job.

## PrefectFlow

```python
ns.PrefectFlow(
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
| `remote_job` | no | `True` (default) runs the flow on a Nebius job. `False` runs it in this process, which needs the `prefect` package here. |
| `image` | no | An [`ns.Image`](images.md) or an image reference for the flow job. Defaults to a slim Python image with Prefect, built once per project. Not used with `remote_job=False`. |
| `**job_options` | no | Options of the flow job, as on [`JobSpec`](jobs.md#jobspec): `timeout` (default `24h`), `cpu`, `disk`, and the rest. Without `gpu` or `cpu` it runs on the smallest CPU machine. With `remote_job=False` only `profile` and `project` are used. |

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

flow = ns.PrefectFlow(pipeline, flow_name="mnist-baseline", timeout="3h")
run = flow.submit(epochs=5)
```

### `PrefectFlow.submit`

```python
flow.submit(*args, **kwargs) -> TaskRun | FlowRun
```

Start the flow and return its run at once.

1. Checks the arguments, and that the Prefect server answers and accepts the login. It stops if the server runs an older Prefect than the flow would use.
2. Builds the images and uploads the `include_libs` of the tasks the function uses, when they changed. This happens here, so the flow job needs none of the tasks' code.
3. Creates the flow run in Prefect, and prints its link. The run stays Pending until the flow starts. When the server cannot be reached from here, there is no link, and the flow creates its run when it starts.
4. With `remote_job=True`, stores the credentials and starts the flow job, then returns that job's [`TaskRun`](tasks.md#taskrun). With `remote_job=False`, runs the flow in a thread and returns a `FlowRun`; the script does not end before the flow does.

`result()` on either run waits and returns the function's result. The flow run's link is on `run.flow_run_url`.

### `PrefectFlow.__call__`

```python
flow(*args, **kwargs) -> Any
```

Run the flow, wait for it, and return the function's result. Ctrl-C cancels the flow job, or stops the flow with `remote_job=False`. Task jobs it already started are not cancelled.

## FlowRun

The run of a flow in this process (`remote_job=False`). The flow job's run is a [`TaskRun`](tasks.md#taskrun) with the same `flow_run_id` and `flow_run_url`.

| Member | Meaning |
| --- | --- |
| `result()` | Wait for the flow and return the function's result, or raise what the flow raised. |
| `name` | The UTC launch time. |
| `title` | The flow run's name in Prefect. |
| `inputs` | The function's arguments by name. |
| `flow_run_id`, `flow_run_url` | The flow run in Prefect and its page in the UI, or `None` when it could not be created at submit. |
| `output_path`, `output_uri` | The run's folder in the task bucket, with one folder per task job in it. |
| `job` | Always `None`: a flow in this process has no job. |

## Prefect server and login

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
