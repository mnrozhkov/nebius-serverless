# First task

Run a Python function and a bash script on Nebius as if they were local calls, and pass the result of one to the next:

```python title="examples/quickstarts/task_quickstart.py"
--8<-- "examples/quickstarts/task_quickstart.py"
```

```bash
uv run python examples/quickstarts/task_quickstart.py
```

!!! note "Object Storage credentials"
    Results come back through a bucket in your project, and this machine reads it over S3. Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to a Nebius Object Storage access key, or put one in `~/.aws/credentials`.

## What happens

1. `ns.Task(count_words, image=..., timeout="10m")` wraps the function. The other options are the ones `JobSpec` takes: `gpu`, `cpu`, `pricing`, `env`, `secrets`, `volumes`, and the rest. Nothing contacts Nebius yet.
2. `count(text=TEXT)` starts a job, waits for it, and returns what the function returned. The function's source is sent to the job, so it must be self-contained: imports go inside its body.
3. `hello(who="world")` runs a bash script. Its inputs arrive in the bash array `inputs`, and what it sets in the array `outputs` comes back as a dict of strings.
4. `count.submit(...)` returns a run at once. `top.submit(counts=counts)` gets that run as an argument: its job starts when `counts` has finished, with the result in its place.
5. `word.result()` waits for the job and returns the result.

The first run creates the bucket `generated-tasks-<project>` in the project. Every task's job mounts it at the same path, and each run keeps its arguments, result, and files in `tasks/<task>/<time>/`.

## Arguments and results

Arguments and return values are JSON values: numbers, strings, lists, dicts, `None`. For anything larger, write files to `$NS_OUTPUT_DIR` in the job and return their path. The next task can open that path, because every job sees the bucket in the same place.

A function that raises, a script that exits non-zero, or a job that ends without a result raises `ns.TaskFailed`. It carries the job id, the traceback from the job (`remote_traceback`), and for a script its `exit_code` and the `outputs` set so far.

## Your own code

A function defined in your script is sent as source. To use a package from your project, list it in `local_libs`: it is uploaded once per change, and the job imports the function by name.

```python
from mylib.training import train_model

train = ns.Task(train_model, image=image, local_libs=["mylib"], gpu="h100", timeout="2h")
```

`image` can be an [`ns.Image`](first-image.md): it is built on the first run and reused after that.

## Next

Run tasks as a [Prefect flow](first-flow.md), or see a full [pipeline of tasks](../training/task-pipeline.md) that trains and evaluates a model. Every option is in the [Tasks reference](../reference/tasks.md).
