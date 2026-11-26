# Tasks

`ns.Task` runs a Python function or a bash script as a Nebius job. Calling the task starts the job, waits for it, and returns the result. `submit` returns a `TaskRun` at once, and a run can be passed to another task, which then starts when the run has finished.

## Task

```python
ns.Task(
    function=None,
    *,
    command=None,
    inputs=None,
    image,
    local_libs=(),
    name=None,
    **job_options,
)
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `function` | one of these two | A plain Python function, not `async`. Its arguments and return value must be JSON values. |
| `command` | one of these two | A bash script, run with `set -e`. The image needs bash 4 or newer. |
| `inputs` | no | A script's input names: letters, digits, and underscores. Not used with a function, whose inputs are its arguments. |
| `image` | yes | An [`ns.Image`](images.md), built on the first run and reused after that, or an image reference such as `python:3.12-slim-bookworm`. A function task needs `python3` in the image. |
| `local_libs` | no | Import names of local packages or modules to send with the task, such as `["mylib"]` or `["mylib.training"]`. |
| `name` | for a script | The task's name. Defaults to the function's name. Lowercased, with other characters turned into hyphens, at most 40 characters. Each run's job is named `<name>-<UTC time>`. |
| `**job_options` | no | Options of the job, as on [`JobSpec`](jobs.md#jobspec): `gpu`, `cpu`, `pricing`, `timeout` (default `24h`), `disk`, `volumes`, `env`, `secrets`, `profile`, `project`, and the rest. They are checked here, so a mistake fails at once. `inject_files` is not available: use `local_libs` for code and `volumes` for data. |

Constructing a `Task` does not contact Nebius. After construction these attributes are readable: `name`, `function`, `command`, `inputs`, `image`, `local_libs`.

### Functions

A function is sent in one of two ways:

- **By name.** When it comes from a package in `local_libs`, the package is uploaded and the job imports the function. It must be defined at module level.
- **As source.** Otherwise its source is sent on its own. It must be self-contained: imports and helpers go inside its body, and it must not use names from the surrounding file, decorators, or variables of an enclosing function. Type annotations are dropped, so they may name anything.

The function gets its arguments as JSON values and must return a JSON value.

### Scripts

A script reads its inputs from the bash array `inputs`, and returns values by setting keys in the bash array `outputs`. The call returns `outputs` as a `dict[str, str]`. An input that is not a string arrives as JSON text. Pass inputs by name.

```python
hello = ns.Task(
    name="hello",
    image="python:3.12-slim-bookworm",
    inputs=["who"],
    command='outputs[greeting]="hello ${inputs[who]}"',
)
hello(who="world")  # {"greeting": "hello world"}
```

The script, with its inputs, must be at most 64 KiB. Pass large data as files.

### Files and the task bucket

Code, arguments, and results travel through a bucket in the project, `generated-tasks-<project>`, which the first run creates. Every task's job mounts it at the same path, `/ns`, so a path one task returns can be opened by another.

Each run has its own folder. A task called on its own uses `tasks/<task>/<UTC time>/`; inside a [Prefect flow](../integrations/prefect.md), `flows/<flow>/<run>/<job name>/`. A job can write files to `$NS_OUTPUT_DIR`, the `files/` folder of its run.

Reading results on your machine uses S3: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or the profile in `~/.aws/credentials` (`AWS_PROFILE`, default `default`). Creating the bucket needs rights to manage Object Storage in the project, otherwise `AccessError`.

### `Task.__call__`

```python
task(*args, **kwargs) -> Any
```

Start a job, wait for it, and return the function's return value or the script's `outputs`. A failure raises `TaskFailed`. Ctrl-C while waiting cancels the job.

### `Task.submit`

```python
task.submit(*args, **kwargs) -> TaskRun
```

Start a job and return its run at once. The first run builds the image when it is an `ns.Image`, and uploads `local_libs` when they changed.

An argument may be a `TaskRun`, or a list or dict that holds runs. `submit` still returns at once: the job starts when those runs have finished, with their results in their place. If one of them fails, this run is not started and its `result()` raises `TaskFailed`. A run still waiting for its inputs when the script ends is never started.

```python
data = prepare.submit(dataset="mnist")
runs = [train.submit(data=data, lr=lr) for lr in (0.1, 0.01)]  # start together, after prepare
best = pick.submit(runs=runs)  # starts when both have finished
print(best.result())
```

## TaskRun

One run of a task, returned by `submit`.

| Member | Meaning |
| --- | --- |
| `result()` | Wait for the job and return the result. Raises `TaskFailed` when the function raised, the script exited non-zero, the job ended without a result, or the run was not started because an input failed. Later calls return or raise the same outcome without waiting. |
| `cancel()` | Request cancellation of the run's job. A run still waiting for its inputs is dropped and never starts. Never raises. |
| `job` | The run's [`Job`](jobs.md#job): `job.id`, and the rest. |
| `project` | The project the run's job is in. |
| `name` | The UTC time the run started, such as `20261124-153922`. |
| `task` | The task's name. |
| `inputs` | The inputs by name, with other runs replaced by their results. |
| `output_path` | The run's files folder as jobs see it, under `/ns`. |
| `output_uri` | The same folder as an `s3://` address. |
| `started` | Whether the run has a job. A run waiting for its inputs has none yet; `job`, `name`, `inputs`, `output_path`, and `output_uri` then wait for it. |

## Errors

`TaskFailed` is an `ns.Error`. Its `resource_id` is the job id and `state` the job's final state. A function's traceback from the job is in `remote_traceback`. For a script, `exit_code` is its exit code and `outputs` what it had set before it stopped. Its `recovery` names the logs command: `nebius ai job logs <id>`.

See [First task](../quickstarts/first-task.md) for a runnable example.
