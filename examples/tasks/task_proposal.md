# Proposal: `ns.Task`

Status: a prototype is implemented in `src/nebius_serverless/task.py`
and was run against a real project. See "Prototype" for what it
covers and "To do" for what is left.
Companion to `examples/images/interface_proposal.py`.

## Summary

`ns.Task` wraps a Nebius serverless job so that a Python function or a
shell script can be called like a local function. The image build, code
upload, job submission, waiting, and result retrieval are hidden.

```python
import nebius_serverless as ns

image = ns.Image(dockerfile_string=DOCKERFILE)

def fun1(size1, size2, coeff):
    import torch
    a = torch.rand(size1, size2) * coeff
    return a.sum().cpu().item()

task1 = ns.Task(fun1, image=image, include_libs=["mylib"], gpu="h100")

task2 = ns.Task(
    name="axolotl-train",
    image=image,
    gpu="h100",
    pricing="spot",
    volumes=["s3://fine-tuning-axolotl:/workspace/data:rw"],
    inputs=["exp_name", "model"],
    command='''
    axolotl train /workspace/data/configs/${inputs[model]}.yaml
    dst=/workspace/data/output/${inputs[exp_name]}
    mkdir -p "$dst" && cp -r /workspace/output/. "$dst"
    outputs[checkpoint_path]="s3://fine-tuning-axolotl/output/${inputs[exp_name]}"
    ''',
)

result1 = task1(size1=3, size2=4, coeff=17)             # 104.2
result2 = task2(exp_name="exp1", model="qwen_0_5b")     # {"checkpoint_path": "s3://..."}
```

## What is decided and what is not

Decided in discussion:

- `Task` is a job wrapper that runs a function or a shell script and
  hides everything around it.
- Arguments and results are JSON.
- `include_libs` is part of the first version. Its entries are import
  names such as `"src"` or `"src.training"`, not paths.
- `Task` has a name. A run is named by the date and time the task was
  called.
- `Task` can be turned into a Prefect task.
- In command mode, inputs arrive as a bash associative array `inputs`,
  and the script returns values by setting keys in an array `outputs`.
- Results travel through a managed bucket.
- The client reads and writes that bucket with the user's default S3
  credentials. The library creates no service account and no access
  key.

Proposed here, not yet confirmed:

- `task.submit()`, `task.local()`, `task.with_options()`.
- `$NS_OUTPUT_DIR` and `run.download()` for files.
- `ns.TaskFailed`.
- The `task.to_prefect()` adapter shape.

## Constructor

```python
ns.Task(
    function=None,          # a Python function (function mode), or
    *,
    command=None,           # a bash script (command mode); exactly one of the two
    inputs=None,            # command mode: names of the inputs
    image=...,              # ns.Image or an image reference string
    include_libs=(),          # import names of local packages or modules
    name=None,              # task name
    **job_options,          # same keywords as JobSpec: gpu, cpu, pricing, timeout,
                            # disk, volumes, env, secrets, restart_attempts, ...
)
```

Constructing a `Task` does not contact Nebius. It validates what it can
locally: names, the function's self-containment, the `inputs` list, the
job options.

## Calling a task

| Call | Behavior |
|---|---|
| `task(**inputs)` | Submit, wait, return the result. Raises on failure. |
| `task.submit(**inputs)` | Submit and return a run handle at once. Several runs in parallel: `submit` in a loop, then `result()` on each. |
| `task.local(**inputs)` | Run on this machine, no job. For debugging and tests. |
| `task.with_options(gpu="h200")` | A copy of the task with other job options. |
| `await task.call_async(**inputs)` | Async twin, like the rest of the library. |

The run handle returned by `submit`:

| Member | Meaning |
|---|---|
| `run.result()` | Wait, then return the result. Raises on failure. |
| `run.job` | The underlying `ns.Job` (id, status, `cancel()`). |
| `run.inputs` | The inputs as a dict. |
| `run.output_uri` | `s3://...` location of the run's folder. |
| `run.output_path` | The same folder as a path inside a job. |
| `run.download(dir)` | Copy the run's files to a local folder. |

A run can be picked up from another session by its name:
`task.run("20261124-153922").result()`. `task.last_run()` returns
the most recent one.

## Function mode

- Arguments are bound to the function's signature and must be JSON
  values. The return value must be a JSON value.
- How the function reaches the job depends on where it is defined:
  - **In a package listed in `include_libs`** (for example
    `from src.training.baseline import train_baseline`): the job
    imports it by its module path and name. Helpers, constants and
    other modules of the package work as usual.
  - **In the script or notebook itself:** the function's source is
    sent. It must be self-contained: imports go inside the body, and
    it must not use helpers, constants, or imports from the
    surrounding file. `Task(...)` checks this and fails with the names
    that would be missing.
- A module in `include_libs` is also imported on the client, to get the
  function. Imports of packages that exist only in the image (such as
  `torch`) therefore go inside the functions, not at the top of the
  module.
- The image needs `python3`. It does not need this package.
- If the function raises, the call raises `ns.TaskFailed` with the
  remote traceback.

## Command mode

`Task` wraps the script in a generated bash script:

```bash
declare -A inputs=([exp_name]='exp1' [model]='qwen_0_5b')   # generated, quoted
declare -A outputs=()
trap '<write outputs and exit code to the run folder>' EXIT   # generated
# ... the user's script, inlined in the same shell ...
```

- The call returns `outputs` as a Python dict.
- The script stops at the first failing command (`set -e`).
- The trap also runs when the script fails or calls `exit`, so partial
  outputs and the exit code are kept. A nonzero exit raises
  `ns.TaskFailed`.
- Values are strings. `age=22` arrives as `"22"`. Nested values are
  passed as JSON text.
- The image needs bash 4 or newer. Alpine images do not ship bash.
- `outputs` must be set in the main shell. Assignments in a subshell,
  a pipeline, or a child script are lost. Child processes cannot see
  `inputs`; pass values on as arguments:
  `python train.py --model "${inputs[model]}"`.

## Outputs

| Kind | How it comes back |
|---|---|
| Return value (function) | `task(...)` / `run.result()` |
| `outputs` array (command) | `task(...)` / `run.result()`, as a dict |
| Files | Written to `$NS_OUTPUT_DIR` in the job; `run.download()` locally |
| Errors | `ns.TaskFailed` with traceback or exit code |
| Logs | Not returned. The tail of stdout/stderr is saved to the run folder. Full logs: `nebius ai job logs <id>` |

Large artifacts (checkpoints, datasets) go to `$NS_OUTPUT_DIR` or to a
volume of your own; the task returns their location, not the data.

What the run folder accepts: the bucket is mounted with mountpoint-s3.
Tested on a job (2026-11-24): writing a file from start to end (64 MB),
overwriting, renaming and deleting work. Appending to a file and
writing after a seek fail. Code should therefore write a file to local
disk and copy it into `$NS_OUTPUT_DIR`; the prototype library's
`src.common.save_torch` does this for `torch.save`.

Passing files between tasks: the managed bucket is mounted at the
same path in every task's job. A path under `$NS_OUTPUT_DIR` that one
task returns can therefore be given to another task as an input and
opened there as is. `train.py` in this folder does this with a
dataset folder and with checkpoints.

## `include_libs`

```python
ns.Task(train, image=image, include_libs=["src"])
ns.Task(train, image=image, include_libs=["src.training", "src.common"])
```

- Each entry is an import name, written as in an `import` statement:
  a package (`"src"`), a subpackage (`"src.training"`), or a
  module (`"src.common"`). Paths are not accepted.
- A name is found the way Python finds an import, so it does not
  depend on the current folder. If `import src` works in the script,
  `include_libs=["src"]` works. Finding a name does not run the
  library's code.
- `"src"` sends the whole package. `"src.training"` sends that
  subpackage and the `__init__.py` of each parent, so the job imports
  it under the same name, `src.training`.
- When only a part is sent, the other parts it imports must be listed
  too. In the prototype, `src.training.baseline` imports
  `src.common`, so `["src.training"]` alone is not enough.
  Proposed: `Task(...)` reads the imports of the files it sends and
  fails, naming what is missing from the same package.
- `Task(...)` also fails when a name cannot be imported on this
  machine, or when it is an installed package (in `site-packages`),
  which belongs in the Dockerfile.
- A function imported from a module that `include_libs` does not
  cover is sent as source, under the self-contained rule.
- On submit the files are packed into one archive (without
  `__pycache__`), hashed, and uploaded once per hash.
- In the job the archive is unpacked and put on `sys.path` and
  `PYTHONPATH`, so it works in both modes.
- An edit takes effect on the next call. The image is not rebuilt.
- Pure Python only. The dependencies of these libs belong in the
  Dockerfile.
- `src/` in this folder is a prototype of such a library, and
  `tasks.py` shows how it is used.

## Naming

- `name` is the stable name of the task. Default: the function's
  `__name__`. Required in command mode.
- A run's name is the date and time the task was called, in UTC, to
  the second: `20261124-153922`. It is the time of the call on the
  client, not the time the job starts in the cloud.
- The job is named `<name>-<run name>`, for example
  `axolotl-train-20261124-153922`, so jobs sort by time in the console
  and stay recognisable. The run folder is `tasks/<name>/<run name>/`.
- Runs of one task that start in the same second get a counter
  appended, from the second one on: `20261124-153922-2`. This covers
  `submit` in a loop.
- Every call is a new run with a new name, including a retry of the
  same call. A retry does not reattach to an earlier job.
- Names are converted to what the platform allows. A name that cannot
  be made valid fails at construction.
- The name is used for the job label in the console, the run folder,
  log lines, and the Prefect task name.

## Image

- `image=` takes an `ns.Image` or a reference string.
- An `ns.Image` is built on the first call and reused after that (the
  tag is a hash of the Dockerfile, as today).
- The first `submit` builds the image before it returns, so a loop of
  `submit` calls builds it once, not once per run.
- Proposed project layout: one folder per image under `images/`, each
  with a `Dockerfile`, for example `images/baseline/Dockerfile`.
  `ns.Image("images/baseline/Dockerfile")` already names the image
  after its folder, so the folder name is the image's name in the
  registry. Tasks that need the same environment share one image.

## Failure, cancellation, preemption

- A blocking call that is interrupted (Ctrl-C, Prefect cancel or
  timeout) cancels the job.
- `timeout` gets a default, so a forgotten job cannot run forever.
- A process that dies without cancelling leaves the job running.
  `task.runs()` lists a task's jobs so they can be found and cancelled.
- The library already resubmits preempted spot jobs. A task may
  therefore run more than once:
  - the result is written only at the very end of a successful attempt;
  - `$NS_OUTPUT_DIR` is the same across attempts, so training code can
    resume from its own checkpoint.

## Prefect

Prefect is an optional extra (`nebius-serverless[prefect]`). The core
has no Prefect dependency; the adapter returns a real Prefect task.

```python
p_task1 = task1.to_prefect(retries=2)

@flow
def pipeline():
    a = p_task1(size1=3, size2=4, coeff=17)
    futures = p_task1.map(size1=[1, 2, 3], size2=4, coeff=17)   # Prefect's own map
```

What the core `Task` provides to make this thin:

- a real `__name__`, `__doc__` and `__signature__` (built from `inputs`
  in command mode);
- exceptions on remote failure, so Prefect retries work;
- JSON inputs and results, which Prefect can persist and cache;
- an async twin for async flows;
- cancellation that reaches the job;
- a hash of source, libs, image tag and inputs, saved in the run's
  manifest, which serves as the Prefect cache key. A Prefect retry
  starts a new run;
- `name` maps to the Prefect task name.

Each task runs as a Nebius job; the flow runs wherever it is launched.
A Prefect work pool on Nebius is out of scope.

The flow can itself run in a job, in the background. That is
`ns.PrefectFlow`, described in `flow_job_proposal.md`.

## How it works

Into the job:

- The generated runner (function source or bash script) and the inputs
  are sent with the existing file injection, at most 64 KiB per file.
- `include_libs` archives are uploaded to a managed bucket, which the
  job mounts.

Out of the job:

- Results, errors, the log tail and files are written to the run's
  folder in the same bucket and read by the client.

```
<managed bucket>/
  libs/<hash>.tar.gz
  tasks/<task name>/<run name>/
    manifest.json     # source or script, libs hash, image tag, inputs
    result.json       # return value or outputs; written last
    error.json        # traceback or exit code
    stdout.log        # tail
    files/            # $NS_OUTPUT_DIR
```

The bucket is created on first use, like the `generated-images`
registry. Its name is `generated-tasks-<project id without "project-">`,
so it can be derived without a lookup, and it carries the label
`managed-by: nebius-serverless`.

Verified against a real project (2026-11-24):

- The bucket can be created with the SDK using the caller's own
  credentials.
- A job can mount it read-write and a later job can read the files
  back. No credentials are needed inside the job.
- The client can list, read and write objects with the default S3
  credentials on the machine, at the regional endpoint
  `https://storage.<region>.nebius.cloud`. A file written by a job
  was read back locally.
- The mount source must be the bucket ID (`storagebucket-...`). The
  API rejects `s3://<bucket name>` as a volume source for a bucket in
  the project, including for buckets that already existed. `Task` must
  mount by ID. The `s3://bucket:/path` form documented for `JobSpec`
  is affected by the same rejection.

## Open questions

1. **Client access to the bucket.** Settled: the client uses the
   user's own S3 credentials. The prototype signs the requests itself
   with the standard library, so there is no new dependency. It reads
   `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or the profile in
   `~/.aws/credentials`. Other credential sources that `boto3` knows
   (SSO, config-file roles) are not supported. Still open: keep this,
   or depend on `boto3`.
2. **`include_libs` without S3 credentials.** Not needed now that client
   access works. If it is ever wanted, an archive can be split over
   several injected files; whether the platform limits the total
   injected size is untested.
3. **Duplicate job names.** Unknown whether the platform rejects two
   jobs with one name. Run names are unique by construction, so this
   only matters if two machines call the same task in the same
   second.
4. **Live logs.** The SDK has no call to read job logs, so output is
   not visible from Python while a job runs.
5. **Retention.** Results and lib archives accumulate in the bucket and
   need a cleanup rule.
6. **Decorator form.** `@ns.task(image=image, gpu="h100")` as a second
   way to define a function task.


## Prototype

`src/nebius_serverless/task.py`, exported as `ns.Task` and
`ns.TaskFailed`, with tests in `tests/test_task.py`.

Implemented:

- Function tasks, sent as source or imported by name from
  `include_libs`.
- Command tasks with the `inputs` and `outputs` arrays.
- `task(...)` and `task.submit(...)`; `run.result()`, `run.job`,
  `run.inputs`, `run.name`, `run.output_path`, `run.output_uri`.
- `include_libs` by import name, packed, hashed and uploaded once per
  content.
- `$NS_OUTPUT_DIR`, and passing a path from one task to another.
- The managed bucket, created on first use and mounted by ID at `/ns`.
- Names: `<task name>-<UTC time>`, with a counter for a repeat in the
  same second.
- `image=` as an `ns.Image` (built once) or a reference.
- `ns.TaskFailed` with the job's traceback, or a script's exit code
  and the outputs it had set.
- Ctrl-C during a blocking call cancels the job.
- Job options checked when the `Task` is constructed. `timeout`
  defaults to 24h.
- `task.to_prefect(**options)`: the task as a Prefect task, named after
  it, with the same signature.

Checked on real CPU jobs (2026-11-24): a function sent as source that
imports a local library inside its body; a function imported by name;
a function that raises; a script with inputs, outputs, a file in
`$NS_OUTPUT_DIR` and an import of the local library; a script that
fails partway (exit code and earlier outputs kept); a path returned by
the script task and opened by a function task.

Checked on GPU jobs (2026-11-24) with `train.py` in this
folder (its tasks are defined in `tasks.py`): a script task running `nvidia-smi` on an H100, a dataset step
on CPU, four spot H100 training jobs at the same time, four evaluation
jobs, and paths passed from one step to the next. The image was an
`ns.Image`, built beforehand and reused by every task.

Checked with Prefect (2026-11-24) with an earlier `train_prefect.py`
that used `to_prefect()` and ran the flow on the user's machine: the
same pipeline as a flow on a remote Prefect server (3.8.8). Each task
run is one job, parallel steps use Prefect's `submit`, and a failing
task shows as a failed task run with the job's traceback.

Not checked: a spot preemption and the resume after it, an image build
started by a task, and files larger than 64 MB in `$NS_OUTPUT_DIR`.

Observed:

- A `submit` takes about 6 seconds before the job exists.
- A job takes 2 to 3 minutes to start, most of it pulling the image.
  For short steps this is nearly all of the run time.
- Interrupting a Prefect flow does not cancel its jobs, and the
  process waits for them. The jobs had to be cancelled by hand.

## To do

Design changes that were proposed and postponed:

1. A way for parallel runs to fail without losing the rest:
   `run.wait()` that does not raise, `run.status`, `run.error`,
   `run.cancel()`.
2. Include an imported function's package automatically, so
   `include_libs` is needed only when the import is inside the
   function.
3. `task.local()` for function tasks only. A script needs bash 4, and
   a stock Mac has bash 3.2.
4. `command` from a script file.
5. Report runs that are still going when the script exits, and
   cancel the job when a Prefect task run is stopped.
6. Decide what a second run of a pipeline does: repeat every step, or
   reuse a finished run with the same code, image and inputs.
7. `task.map()` and a concurrency limit. Until then, parallel runs use
   `submit` in a loop, which starts all of them at once.
8. Send a task's function and arguments through the bucket, with a
   checksum, instead of as an injected file. This removes the 64 KiB
   limit. See `flow_job_proposal.md`.

To test:

- A spot preemption, and the resume from the checkpoint after it.
- A task whose image is an `ns.Image` that has to be built.

Designed but not in the prototype:

- `task.with_options()`, the async twin, `run.download()`,
  `task.run(name)` and `task.last_run()`.
- The run manifest and the saved tail of the log.
- The check for imports missing from a partly sent package.
- For Prefect: the content hash as cache key, and cancelling the
  job when the task run is cancelled.
- Documentation pages.

Open decisions:

- The default `timeout` (24h in the prototype).
- A retention rule for the bucket.
- The built-in S3 client, or `boto3`.
- Whether the `s3://bucket` volume form of `JobSpec`, which the API
  rejects, is fixed as part of this work.
- How a task that starts tasks gets Nebius credentials: a forwarded
  personal access token, or a service account.

## Order of work

1. `image=` accepts an `ns.Image` in `JobSpec`.
2. `Task` in command mode: `inputs` array, naming, `submit`,
   `local`, cancel on interrupt. No outputs yet. Needs no bucket.
3. The managed bucket, created on first use and mounted by ID.
4. Outputs in both modes, function mode, `include_libs`.
5. `to_prefect`.
