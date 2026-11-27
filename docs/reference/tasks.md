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
    bucket=None,
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
| `bucket` | no | An existing bucket for the task's code, inputs and results, instead of the project's `ns-workspace-<project id>`. Never created. See [Files and the task bucket](#files-and-the-task-bucket). |
| `**job_options` | no | Options of the job, as on [`JobSpec`](jobs.md#jobspec): `gpu`, `cpu`, `pricing`, `timeout` (default `24h`), `disk`, `volumes`, `env`, `secrets`, `profile`, `project`, and the rest. They are checked here, so a mistake fails at once. `inject_files` is not available: use `local_libs` for code and `volumes` for data. |

Constructing a `Task` does not contact Nebius. After construction these attributes are readable: `name`, `function`, `command`, `inputs`, `image`, `local_libs`, `bucket`.

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

Code, arguments, and results travel through one bucket. Every task's job mounts it at the same path, `/ns`, so a path one task returns can be opened by another.

By default it is the project's bucket, `ns-workspace-<project id without "project-">`. The first run creates it with the label `managed-by: nebius-serverless`, which needs rights to manage Object Storage in the project. `ns` matches the package alias, the `/ns` mount and the `NS_*` variables; `workspace` says it is a working area a tool manages, for runs, code archives and caches.

To use a bucket you already have, choose it. The first that is set wins:

1. `bucket=` on the task: `ns.Task(generate, image=..., bucket="santa-cards-course")`.
2. `ns.configure(bucket="santa-cards-course")` for this process.
3. The `NS_BUCKET` environment variable, for CI.
4. Otherwise, the project's bucket.

A chosen bucket must exist: the package never creates it. On first use it writes and reads one small object, `ns/tasks/.access-check`, and raises `AccessError` naming the bucket and the missing right when it cannot. Tasks that pass runs to each other must use the same bucket, because their jobs pass files through one `/ns` mount; otherwise `submit` raises `ValidationError`.

The package writes only under its own folders:

```
<bucket>/
├── libs/<hash>.tar.gz           task code archives
├── tasks/<task>/<run>/          a task called on its own
└── flows/<flow>/<run>/<job>/    tasks inside a Prefect flow
```

In a chosen bucket these folders sit under `ns/`, so your own files stay apart: `santa-cards-course/ns/tasks/...`. `ns.configure(prefix="")` puts them at the bucket root, and `prefix="other"` under another folder.

Each run has its own folder, with its payload, its result, and `files/`. A job can write files to `$NS_OUTPUT_DIR`, the `files/` folder of its run. `run.output_uri` is that folder as an `s3://` address.

Reading results on your machine uses S3: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or the profile in `~/.aws/credentials` (`AWS_PROFILE`, default `default`).

!!! note "Buckets from earlier versions"
    Before 0.2.0, tasks used a bucket named `generated-tasks-<project id>`. It is left alone. When you no longer need its runs, empty it with an S3 client, then delete it in the web console or with `nebius storage bucket delete`.

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

One run of a task, returned by `submit` and by `ns.tasks.get`. One call of a task is one run. A run has one or more attempts, one Nebius job each: the first, and one more after each preemption.

| Member | Meaning |
| --- | --- |
| `result(timeout=None)` | Wait for the job and return the result. Raises `TaskFailed` when the function raised, the script exited non-zero, the job ended without a result, or the run was not started because an input failed. Later calls return or raise the same outcome without waiting. `timeout` is a local deadline, such as `"30m"` or seconds: when it passes, `WaitTimeout` names the latest job id, and the job keeps running. |
| `cancel()` | Request cancellation of the run's job. A run still waiting for its inputs is dropped and never starts. Never raises. |
| `id` | The run id: the run's folder in the bucket, such as `tasks/train/20261127-152011`. It stays the same across attempts. Store it to reconnect. |
| `uri` | The run's folder as an `s3://` address, for other tools. |
| `job` | The latest attempt's [`Job`](jobs.md#job): `job.id`, and the rest. |
| `project` | The project the run's job is in. |
| `name` | The UTC time the run started, such as `20261124-153922`. |
| `task` | The task's name. |
| `inputs` | The inputs by name, with other runs replaced by their results. |
| `output_path` | The run's files folder as jobs see it, under `/ns`. |
| `output_uri` | The same folder as an `s3://` address. |
| `started` | Whether the run has a job. A run waiting for its inputs has none yet; `job`, `name`, `inputs`, `output_path`, and `output_uri` then wait for it. |

## `tasks.get`

```python
ns.tasks.get(run_id, *, bucket=None, profile=None, project=None) -> TaskRun
```

Reconnect to a run after a kernel restart or a crashed script, and read the result you already paid for:

```python
run = train.submit(epochs=3)
run_id = run.id  # "tasks/train/20261127-152011": store this

# later, in another process
run = ns.tasks.get(run_id)
value = run.result(timeout="30m")
```

`run_id` is a run id, or the job id (`aijob-...`) of any of the run's attempts. **Store the run id**: it is the run's identity and works without a job read. A job id identifies one attempt; it is accepted because it is what progress output and the console show.

`ns.tasks.get` never starts or resubmits a job. The run it returns follows the latest attempt. When `result()` finds that attempt was preempted and the run's `preemption_retries` budget is not used up, it starts the next attempt.

`bucket=` is the bucket the run is in when you chose one, with the same defaults as `Task(bucket=)`. A job id finds its bucket and project from the job. Only one process should wait on a run: two processes waiting on the same preempted run can both start a new attempt.

Each run keeps a record next to its files, `<run id>/run.json`: the task's definition (image, libraries, the function's import name or source, job options), and its attempts with their job ids. The arguments stay in `payload.json`. Runs from before run records existed cannot be reattached.

## Errors

`TaskFailed` is an `ns.Error`. Its `resource_id` is the job id and `state` the job's final state. A function's traceback from the job is in `remote_traceback`. For a script, `exit_code` is its exit code and `outputs` what it had set before it stopped. Its `recovery` names the logs command: `nebius ai job logs <id>`.

See [First task](../quickstarts/first-task.md) for a runnable example.
