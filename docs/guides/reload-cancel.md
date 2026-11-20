# Reload, cancel, and retry safely

## Reload a saved id

The id is the handle. Save it, and fetch the resource again later: after a restart, from another script, or for something created with the CLI. A fetch never creates anything.

=== "Jobs"

    ```python
    saved_id = job.id
    loaded = ns.jobs.get(saved_id)
    print(loaded.status)
    ```

=== "Endpoints"

    ```python
    saved_id = endpoint.id
    loaded = ns.endpoints.get(saved_id)
    print(loaded.status, loaded.url)
    token = loaded.reveal_token()  # the reloaded handle has no token until this
    ```

## Cancel a job

`cancel` returns when the request is accepted, with the job usually `CANCELLING`. Stopping the container can take minutes. `wait()` returns once it is `CANCELLED`.

```python
payload = job.cancel()
print(payload)
print(job.get()["status"]["state"])   # usually CANCELLING
record = job.wait()
print(record["status"]["state"])      # CANCELLED
```

An endpoint has no `cancel`: `stop()` it to pause billing for compute, or `delete()` it.

## Local timeouts do not cancel

A local timeout stops waiting only. No cancellation is requested, and the job may still be running. Reload it with `jobs.get(id)` or call `cancel`.

```python
try:
    job.wait(timeout=0)
except ns.WaitTimeout as exc:
    print(exc.resource_id)
```

Closing the process and cancelling an awaited call do not cancel the job or delete the endpoint either.

## Retry a create without making two

Every create sends an idempotency key: a new one per call, or the one you pass. When the outcome is unclear, the client sends the same request with the same key up to 3 times, and Nebius returns what it already made instead of making a second job.

Pass your own key to re-run a notebook cell or a script and reconnect to what it already created:

```python
job = spec.create(idempotency_key="lr-3e-4")
```

If the outcome is still unknown after 3 tries, `create` raises `SubmissionUncertain`. Retry with the same recipe and the key it carries:

```python
try:
    job = spec.create()
except ns.SubmissionUncertain as exc:
    job = spec.create(idempotency_key=exc.idempotency_key)
```

A key you pass on an endpoint with token auth needs `token=` or `token_secret=`, because a token generated in another process would differ. How long Nebius remembers a key is not documented. Details are under [`JobSpec.create`](../reference/jobs.md#jobspeccreate).
