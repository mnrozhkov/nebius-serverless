# Proposal: `ns.PrefectFlow`

Status: a prototype is implemented in `src/nebius_serverless/flow.py`.
See "What exists" for what it covers and what was checked.
Companion to `task_proposal.md` in this folder.

## Summary

`ns.PrefectFlow` runs a pipeline of tasks as a Prefect flow on a Nebius
job, in the background, or with `remote_job=False` in the user's own
process. The user has tasks, with their images and
local libraries, and a function that calls them. `PrefectFlow` takes
that function and hides the rest: the credentials, the bucket, the
project, Prefect's setup, and getting the tasks into the job.

```python
import nebius_serverless as ns
from tasks import EXPERIMENT, LEARNING_RATES, evaluate, prepare, train


def baseline(epochs: int = 5) -> dict[str, object]:
    data = prepare(dataset="mnist")
    training = [
        train.submit(
            exp_name=EXPERIMENT,
            data_dir=data["data_dir"],
            lr=lr,
            epochs=epochs,
        )
        for lr in LEARNING_RATES
    ]
    trained = [run.result() for run in training]
    evaluation = [
        evaluate.submit(
            checkpoint=run["checkpoint"], data_dir=data["data_dir"]
        )
        for run in trained
    ]
    scores = [run.result() for run in evaluation]
    best, score = max(
        zip(trained, scores), key=lambda pair: pair[1]["accuracy"]
    )
    return {"lr": best["lr"], "accuracy": score["accuracy"]}


flow = ns.PrefectFlow(
    baseline,
    flow_name="mnist-baseline",
    api_url="https://<prefect server>/api",
    timeout="3h",
)

run = flow.submit(epochs=5)   # returns at once; the flow runs on a CPU job
```

```
user's machine: flow.submit(), then free to leave
job (CPU, on-demand): the Prefect flow "mnist-baseline"
  ├─ job: prepare-data
  ├─ job: train-baseline x4
  └─ job: evaluate x4
```

## What is decided and what is not

Decided in discussion:

- The flow runs on an on-demand CPU job, in the background.
- It is recorded on a remote Prefect server.
- Every run does every step. No result of an earlier run is reused.
- The flow job has its own image: a slim Python image with Prefect.
- `include_libs` come with the tasks that use them. The flow job does
  not carry the libraries of the tasks it starts.
- Credentials, the bucket and the other plumbing are hidden from the
  user. Running a flow is one object and one call.
- The function does not need the `@flow` decorator.
- `flow_name` is required. `run_name` defaults to it. A run is named
  `<launch time>-<run_name>`.
- The data the flow job needs is passed through the managed bucket,
  not as an injected file, so its size is not limited to 64 KiB.
- The flow job's credentials go through MysteryBox and reach the job
  as environment variables. This is the way the Nebius documentation
  describes.
- Each run writes a new secret version. Earlier versions are not
  deleted, and other jobs are not checked.
- With `remote_job=False` the same flow runs in the user's process,
  recorded on the same server. It needs Prefect on the user's machine.
- The flow run is created in Prefect at submit, so its link is shown at
  once, before the flow starts.
- All of this is inside `ns.Task` and `ns.PrefectFlow`. The user
  writes no code for credentials, the bucket or the project.
- For now the flow job acts as the user, with the user's access token.
- `image` has a default: a slim Python image with Prefect, defined in
  the library as a Dockerfile string.

Proposed here, not yet confirmed:

- The function goes to the constructor and its arguments to `submit`.
- Tasks are found automatically and travel as records.
- The run handle and what `cancel()` does.

## The interface

```python
ns.PrefectFlow(
    function,               # the pipeline: a plain function, or one with @flow
    *,
    flow_name=...,          # the flow's name in Prefect. Required
    run_name=None,          # a name for the runs. Default: flow_name
    api_url=None,           # the Prefect server. Default: PREFECT_API_URL
    remote_job=True,        # False: run the flow in this process
    image=image_prefect,    # ns.Image or a reference. Default: see "The image"
    timeout="24h",          # of the flow job
    **job_options,          # as on JobSpec: cpu, disk, env, secrets, ...
)
```

| Call | Behavior |
|---|---|
| `flow.submit(**arguments)` | Start the flow job and return its run at once. With `remote_job=False`, start the flow in a thread here and return a `FlowRun` at once. |
| `flow(**arguments)` | Start it, wait, and return the function's result. With `remote_job=False`, run the flow in this thread. |

The run returned by `submit`:

| Member | Meaning |
|---|---|
| `run.name` | The run's launch time, in UTC. |
| `run.job` | The flow job. `None` with `remote_job=False`. |
| `run.flow_run_url` | The flow run's page in the Prefect UI, or `None` when it could not be created at submit. `run.flow_run_id` is its id. |
| `run.result()` | Wait, then return the function's result. |
| `run.cancel()` | Stop the flow job and the task jobs it started. Not built. |

Why the function and its arguments are given separately: in
`PrefectFlow(flow=baseline(5))` Python calls `baseline(5)` first, on
the user's machine, so the whole pipeline would run there before
`PrefectFlow` sees anything.

## The flow function

The function's source is sent to the job, like a task's function. It
may use, from outside its own body:

- `ns.Task` objects, such as `prepare` and `train`. Each one travels
  as a record.
- Constants with JSON values, such as `EXPERIMENT` and
  `LEARNING_RATES`. They are sent by value.
- Anything it imports inside its body, if the flow image has it.

`PrefectFlow(...)` checks this and fails with the names of anything
else, such as a helper function from the surrounding file.

Calls to tasks mean the same on the user's machine and in the job:
`task(...)` waits and returns the result, and `task.submit(...)`
returns something with `result()`. In the job each task is a Prefect
task, so each call is a Prefect task run. One function can therefore
be run as a plain script, as a local flow, and as a flow job.

## What happens at `submit`

On the user's machine:

1. The tasks the function uses are found.
2. The Prefect server is asked for its version, with the login the
   flow will use. A refused login, or a server older than the Prefect
   in the default image, stops here, before anything is built.
3. For each task: its image is built if needed, its `include_libs` are
   uploaded if they changed, and a record is made.
4. Credentials are written as a new version of a secret.
5. The flow run is created in Prefect, Pending, under the run's name,
   with the arguments as its parameters. Its link is logged. When the
   server cannot be reached from the user's machine, this is skipped
   and the flow creates its run when it starts.
6. The payload is written to the run's folder in the managed bucket:
   the function's source, the arguments, the constants, the task
   records, the run's folder and the flow run's id.
7. The flow job is created. Only the runner and a pointer to the
   payload are injected into it. If it cannot be created, the flow run
   is marked Crashed.

In the flow job:

1. The runner reads the payload through the bucket mount and checks it
   against the checksum in the pointer.
2. It rebuilds each task from its record and wraps it as a Prefect
   task that keeps its files in the run's folder.
3. It wraps the function as a Prefect flow named `flow_name` and runs
   it with the arguments, with the tasks and constants bound to the
   names the function uses. With a flow run id, Prefect runs that
   flow run: Pending becomes Running, and no second run is made.
4. The function's return value is written as the run's result.

## Running the flow here (`remote_job=False`)

```python
flow = ns.PrefectFlow(
    baseline, flow_name="mnist-baseline", api_url=..., remote_job=False
)
run = flow.submit(epochs=5)   # the flow runs in a thread of this process
print(run.flow_run_url)
print(run.result())
```

The flow is the same, and so is what Prefect shows. What changes:

- It needs the `prefect` package on the user's machine, no older than
  the server's. `submit` checks both before anything is built.
- There is no flow job, no flow image, no copy of this package and no
  credentials secret. The tasks' jobs are started from the user's
  process with the user's Nebius profile. The Prefect address and
  login are set for the flow only, so `~/.config/prefect/auth` works
  here too.
- The tasks' images are built and their libraries uploaded at submit,
  one after another, so tasks that start together do not build the
  same image at once.
- The function runs from its source, as in the job, so the same rules
  apply: a function that runs here also runs on a flow job.
- `submit` returns at once, but the flow's thread is not a daemon: the
  script ends when the flow does. `flow(...)` runs it in the calling
  thread, so Ctrl-C stops it. The task jobs already started keep
  running.
- `image` and the job options are for the flow job. Only `profile` and
  `project` are used here.

## Task records

A record is what is left of a task once its image is built and its
libraries are uploaded: enough to start its job without its code or
Dockerfile.

```json
{
  "name": "train-baseline",
  "image": "cr.<region>.nebius.cloud/<registry>/baseline:sha-...",
  "libs": "libs/<hash>.tar.gz",
  "function": {"name": "train_step", "source": "def train_step(...): ..."},
  "parameters": ["exp_name", "data_dir", "lr", "epochs"],
  "options": {"gpu": "h100", "pricing": "spot", "timeout": "2h"}
}
```

- An imported function has its import name instead of its source:
  `{"module": "src.data.mnist", "name": "prepare_data"}`.
- A script task has `command` and `inputs` instead of `function` and
  `parameters`.
- A record holds no library code, no image, no credentials and no
  pickled objects.

Records are made per run and travel with it. A flow that is running
is not affected by a later edit of a task.

## How the data travels

| Data | Route | When |
|---|---|---|
| Task images | the project's registry | when an image is built |
| Task libraries | the managed bucket | at `submit`, once per content |
| The payload: source, arguments, constants, records | the managed bucket, in the run's folder | at `submit` |
| A pointer to the payload, with its checksum | injected into the flow job | at `submit` |
| Credentials | a MysteryBox secret | at `submit` |
| Results | the managed bucket | when a job ends |

- The payload goes through the bucket for every run, large or small,
  so there is one path and no size limit.
- The checksum in the injected pointer keeps the job running exactly
  what was submitted, even if someone else can write to the bucket.
  Library archives get the same check.
- The payload stays in the run's folder as the record of what the run
  used.
- The same change applies to ordinary tasks, whose arguments are
  limited to 64 KiB today.

## Credentials

The flow job needs two secrets. `PrefectFlow` passes both, and the user
writes nothing for them.

| Secret | Variable in the job | Where it comes from | Read in the job by |
|---|---|---|---|
| A Nebius access token | `NEBIUS_IAM_TOKEN` | the user's current login | the Nebius SDK |
| The Prefect server's login | `PREFECT_API_AUTH_STRING` | the same variable on the user's machine, or the local Prefect settings | Prefect |

At `submit`, on the user's machine:

1. A secret named `generated-tasks-credentials` is found in the
   project, or created the first time.
2. A new version is added to it. It holds both values, each under the
   name of its variable.
3. The flow job refers to that version by its id. The job's
   definition holds the id, not the values.

When the container starts, Nebius reads the version and sets the two
variables. The job does not call MysteryBox.

What is deliberately left out:

- No version is deleted, by the script or by the job.
- No earlier version is looked at, and no other job is checked.
- A new version is not marked as the secret's primary version.

What follows from that:

- The secret gains one version per run, and they stay.
- A stored token stops working when it expires. A stored Prefect
  login does not expire. Both can be read by people who have the role
  that allows viewing secret values; the documentation says that role
  is part of `admin` and not of `editor`.
- A flow that runs longer than the token lives fails partway. How
  long that is has not been measured.
- The task jobs get neither secret. Only the flow job talks to Nebius
  and to Prefect.

A Prefect server without a login needs no second secret; then only the
token is stored.

Not chosen for now: a service account, which is what a flow that must
run for many hours or on a schedule would need.

## The image

`image` has a default, so most flows do not name one. The default is
defined in the library as a Dockerfile string:

```python
image_prefect = ns.Image(dockerfile_string="""\
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system --compile-bytecode \\
    prefect==3.9.5 nebius==0.6.43 "PyYAML>=6" "rich>=13"
""")
```

It is a slim Python image with Prefect and what this package depends
on. It has nothing of the tasks in it.

- **Prefect's version.** Prefect in the image must not be newer than
  the Prefect server. The default pins one version, which is then the
  oldest server it works with. `submit` reads the server's version and
  stops when it is older. A flow for an older server passes its own
  image, whose Prefect is not checked.
- **This package.** The flow starts jobs with it, so the job needs
  it. `PrefectFlow` sends its own copy with the flow job, the way
  `include_libs` sends a library. The image needs only the package's
  dependencies, and works before the package is released.
- **More packages.** A flow function that imports something else in
  its body needs an image that has it. The default's text is public,
  so a user copies it, adds a line, and passes the result as `image`.
- **Its name.** In the registry it is named `prefect-flow`, not
  `image`, so it can be told from other images built from strings.

The image is built once. `ns.Image` tags an image with a hash of the
Dockerfile text and looks for that tag in the project's registry
before building:

- The first `submit` in a project builds it, about two minutes.
- Every later `submit` finds it and builds nothing. This holds across
  sessions and machines, and for everyone in the project, because
  they all use the same default text.
- A release of this package that changes the default text causes one
  new build.
- What is not pinned in the text is fixed at the first build.
  `uv:latest` and `PyYAML>=6` stay at the versions that build got,
  until the text changes.

Possible later: build the default image with exactly the server's
Prefect version, which `submit` already reads.

## Behavior

- **Result.** The function's return value, a JSON value, is the run's
  result. It can be read later, from another session, by the run's
  name.
- **Failure.** A failed flow fails the job. `run.result()` raises
  `ns.TaskFailed` with the flow's traceback, and the Prefect server
  shows the flow run as failed.
- **Logs.** The flow's own output goes to the Prefect server, so it
  can be followed live. The task jobs' logs stay in Nebius.
- **Cancelling.** `run.cancel()` stops the flow job, and the runner
  cancels the task jobs it started.
- **Names.** A run is named `<launch time>-<run_name>`, for example
  `20261125-091833-mnist-baseline`. The flow run in Prefect and the
  flow job in Nebius both carry that name, so one can be found from
  the other. The time is UTC, to the second. `run_name` is the same
  for every run of a `PrefectFlow` and defaults to `flow_name`. The
  job's name uses the plain form of `run_name`: lowercase letters,
  digits and dashes. The task jobs are named as any task run:
  `<task name>-<UTC time>`.
- **Layout in the bucket.** `flows/<pipeline>/<run>/<task>/`. The
  pipeline is the plain form of `flow_name`, the run is the run's
  name, and the task is the name of the task's job:

  ```
  flows/
    mnist-baseline/
      20261125-120614-mnist-baseline/
        flow/                                  # the flow job itself
        prepare-data-20261125-120752/
        train-baseline-20261125-121050/
        train-baseline-20261125-121050-2/
        aggregate-results-20261125-121746/
  ```

  Each folder holds that job's `payload.json`, its `result.json` and
  its `files/`. Each task is given the run's folder when it is wrapped
  as a Prefect task. The flow job also sets `NS_TASK_FOLDER` to it.
  With `remote_job=False` the layout is the same, without `flow/`.
  Library archives stay in `libs/`.
- **A link to the flow run.** The flow run is created at submit, so
  its link is known before the flow starts: the API address without
  `/api`, then `/runs/flow-run/<id>`. `PREFECT_UI_URL` replaces the
  first part when the UI is elsewhere.
- **What Prefect shows for a task.** Each task run has three Markdown
  artifacts: `nebius-job` (the job's id, its name, the command for
  its logs, where its files are) and `inputs`, written when the job
  has started, and `outputs`, written when it has ended. The same
  goes to the task run's log. The flow run shows the function's
  arguments as its parameters. The three names are the same for every
  task, so on the server's Artifacts page each is one entry whose
  versions come from all task runs.
- **A link to the job.** The documentation does not give the console
  address of a job's page. When `NS_CONSOLE_JOB_URL` holds it, with
  `{project}` and `{job}` in it, `nebius-job` links to the job, and
  the flow job passes the setting on.

## What exists

In `ns.Task` (`task.py`):

- Task records: `_record` and `_from_record`.
- The payload through the bucket. Only the runner and a pointer with
  the payload's checksum are injected. The job refuses a payload or a
  library archive that does not match. This also lifts the 64 KiB
  limit on a task's arguments.
- Inside a job, the bucket is read through the mount
  (`NS_TASK_MOUNT`).
- A Prefect task run shows its job, inputs and outputs.

In `ns.PrefectFlow` (`flow.py`):

- Finding the tasks and constants a function uses, and the check.
- The default image.
- Writing the credentials as a new secret version.
- The flow job: the function, the records and this package are sent,
  and the function runs as a Prefect flow with project and region
  set.
- `flow.submit(...)` and `flow(...)`.
- The flow run created at submit, its link, and the check of the
  server's Prefect version.
- `remote_job=False`: the flow in the user's process, and `FlowRun`.

Checked with a temporary Prefect 3.9.5 server in the tests: a flow run
created at submit over the REST API is the one the flow runs in, it
ends Completed, and the task jobs' files are in the run's folder.

Not built: `run.cancel()` stopping the task jobs, Prefect options per
task (such as retries), a service account.

The earlier hand-written `credentials.py`, `train_prefect_job.py` and
`images/flow/Dockerfile` are removed.

## Open questions

1. How long a caller token lives.
2. Whether a task started inside a job sees a result file as soon as
   the other job has written it. The prototype retries for 10 seconds.
3. Whether the secret may keep growing by one version per run, or
   needs a limit later.
4. The console address of a job's page, to make the link a default.
5. Where Prefect options for one task, such as retries, are given.
6. Whether `PrefectFlow` without `api_url` should run with no Prefect
   at all, or be a separate class. For now the plain function is that:
   `train.py` runs it as is with `--mode local`.
7. Whether a Prefect worker on Nebius, for schedules and runs started
   from the Prefect UI, is wanted later.
