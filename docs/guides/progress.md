# Progress, logs, and diagnostics

## Progress

`create`, `get`, `wait`, `cancel`, and `jobs.get`, plus their awaited twins, print progress on stderr. An interactive terminal shows a Rich spinner. The job id is cyan, an in-progress state is yellow, success is green, and failure is red. The words stay in the line, so color is not the only signal. A notebook, a CI run (`CI`, `GITHUB_ACTIONS`, or `GITLAB_CI`), a pipe, or a dumb terminal gets append-only lines with no color. `NO_COLOR` turns color off and keeps the spinner. `create(dry_run=True)` prints nothing.

Endpoint calls print the same way, and `wait` ends with the URL under the `RUNNING` line.

## Quiet calls

`quiet=True` is keyword-only. It suppresses progress for that call only. It is not stored on the `Job`, it does not suppress exceptions or diagnostic logs, and it is not sent to Nebius. Stdout still gets what your code prints:

```python
job = spec.create(quiet=True)
print(job.id)
record = job.wait(quiet=True)
print(job.id, record["status"]["state"])
```

```python
job = spec.create()      # submission line
job.wait()               # state, spinner, outcome

job = spec.create(quiet=True)
job.wait()               # progress is visible again
job.wait(quiet=True)
```

## Container logs

Progress shows states, not your program's output. Container logs stay on the CLI:

```bash
nebius ai job logs <id>
```

This package does not stream job or endpoint logs.

## Diagnostic logs

`ns.configure_logging(level="DEBUG")` turns on the package's own logs, separately from progress. With debug logging on, progress stays append-only so log lines remain readable. `ns.diagnostics()` returns the package version and how stderr is presented. Both are described under [Session and auth](../reference/session.md#configure_logging).
