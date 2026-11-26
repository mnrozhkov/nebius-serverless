# First Prefect flow

Run a pipeline of tasks as a [Prefect](https://www.prefect.io/) flow. The flow itself runs on a small Nebius job, so it keeps going after your script ends, and every task is a Prefect task run with a link to its job:

```python title="examples/quickstarts/flow_quickstart.py"
--8<-- "examples/quickstarts/flow_quickstart.py"
```

```bash
export PREFECT_API_URL=https://prefect.example.com/api
uv run python examples/quickstarts/flow_quickstart.py
```

You need a Prefect server, and the same Object Storage credentials as in [First task](first-task.md). Prefect does not have to be installed on your machine.

## What happens

1. `pipeline` is a plain function that calls tasks. It needs no `@flow` decorator.
2. `ns.PrefectFlow(pipeline, flow_name=..., timeout="1h")` checks that `pipeline` uses only tasks, constants with JSON values, and what it imports inside its body. Nothing contacts Nebius yet.
3. `submit(n=3)` checks the Prefect server and its login, builds the tasks' images and uploads their libraries, and creates the flow run in Prefect. It prints the link to the run. The first submit in a project also builds the flow job's image, Python with Prefect, once.
4. It then starts the flow job on the smallest CPU machine and returns its run. The flow job runs `pipeline`, and starts one job per task call.
5. `run.result()` waits for the flow job and returns what `pipeline` returned.

The run is named by its launch time, such as `20261125-091833-sum-of-squares`, in Prefect, as the flow job, and as its folder `flows/sum-of-squares/<run>/` in the task bucket.

## Prefect login

When the server asks for a login, set `PREFECT_API_AUTH_STRING` to `user:password`, or put that line in `~/.config/prefect/auth`. `submit` checks it with the server first, so a wrong login fails at once and not inside the job.

The flow job starts jobs as you. A short-lived access token, and the Prefect login when there is one, are stored as a new version of the `generated-tasks-credentials` MysteryBox secret and reach the job as environment variables.

## Run the flow here

`remote_job=False` runs the flow in your process instead of on a job. The tasks still run on Nebius. This needs `pip install prefect` locally, and the script stays open until the flow ends:

```python
flow = ns.PrefectFlow(pipeline, flow_name="sum-of-squares", remote_job=False)
run = flow.submit(n=3)
print(run.flow_run_url)
print(run.result())
```

## Next

See a full [pipeline of tasks](../training/task-pipeline.md) in all three modes, or the [Prefect flows reference](../reference/flows.md).
