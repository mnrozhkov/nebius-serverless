# Jobs

The jobs surface: `JobSpec` is the recipe, `Job` is one Nebius job after create, and `ns.jobs` fetches a job from a saved id. Import the package as `ns`.

## JobSpec

Recipe for one `nebius ai job create`. A second `create()` starts another cloud job and returns another `Job`.

```python
ns.JobSpec(
    command,
    *,
    image,
    gpu=None,
    platform=None,
    preset=None,
    timeout,
    pricing="on-demand",
    subnet_id=None,
    volumes=None,
    env=None,
    name=None,
    disk=None,
    profile=None,
    project=None,
    restart_attempts=0,
    working_dir=None,
    secrets=None,
    registry_secret=None,
    spot_policy=None,
    shm=None,
)
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `command` | yes | Nonempty list of strings. A shell string is rejected. The first element is the container command. Further elements are POSIX-quoted and joined into the single `args` string. A one-element command omits `args`. |
| `image` | yes | `name:tag` or `registry/path@sha256:<64 hex>`. Sent unchanged. The client does not resolve a tag to a digest. |
| `gpu` | no | GPU selector `MODEL[:COUNT]`, such as `"h100"` or `"h100:8"`; Nebius picks the platform and preset. See [Machine selection](../guides/machines.md). `gpu="cpu"` raises `ValidationError`. |
| `cpu` | no | CPU selector `FAMILY:VCPUS`, such as `"amd:16"`. Excludes `gpu`. Omit both, and `platform`, for the smallest CPU machine. |
| `platform` | no | Explicit platform id. Excludes `gpu` and `cpu`. |
| `preset` | no | Explicit preset id. When omitted, the client sends the minimal preset for the chosen platform. An unknown platform id requires `preset`. |
| `timeout` | yes | Job deadline: a duration string such as `"30m"` or `"24h"`, a number of seconds, or a `timedelta`. Normalized to seconds. Below 5 minutes or above 168 hours raises `ValidationError` (the CLI's range). Nothing is rounded or clamped. It is required so every job has an explicit cost ceiling; Nebius's own defaults differ by entry point (24 hours for `job create` and the API, 1 hour for `job run`). Nebius checks it asynchronously after provisioning, so a job can run briefly past it. A timed-out job is returned as data with `status.state_details.code == "TimeoutExceeded"`. |
| `pricing` | no | `"on-demand"` (default) or `"spot"`. `"spot"` is preemptible and follows the current price. `"spot"` on CPU raises `UnsupportedFeature`. Any other value raises `ValidationError`. |
| `subnet_id` | no | Sent when set. Omit it when the project has a single subnet. |
| `volumes` | no | Mount strings, passed through: `source:container_path[:mode]` or `s3://bucket:/container_path[:mode]`. Mode is `ro` or `rw` (default `rw`). An `s3://` string with a fourth `:profile` part raises `UnsupportedFeature`. The client does not verify the objects. |
| `inject_files` | no | CLI [`--inject-file`](https://docs.nebius.com/cli/reference/ai/create) strings: `local_path:container_path`. Each local file is read on create (not on `JobSpec` construction), sent as container content, and must be at most 64 KiB. Dry-run lists the strings and does not read the files. |
| `env` | no | `dict[str, str]`. Values are sent on create and left out of dry-run descriptions and exception text. Use `secrets` for tokens. |
| `name` | no | Console label (`metadata.name`), sent unchanged on every create. When omitted, each `create()` generates a new `job-` plus 12 hex characters and the returned handle's `name` is the one sent, so a loop over one recipe gives distinct names. A dry run generates one too, so it cannot predict the name a later create sends. The recipe's `name` attribute stays `None`. It is not a lookup key. |
| `disk` | no | CLI size string, passed through, such as `500Gi`. When omitted, the request uses `250Gi`. |
| `profile` | no | Nebius SDK profile name. When omitted, the active profile is used. |
| `project` | no | Project id. Overrides the profile's project. |
| `restart_attempts` | no | Container restarts on the same VM after the command fails, sent as `restart_attempts`. `0` (default) runs once and leaves the field unset. `N` allows up to `N` restarts after the first run. `-1` restarts without limit, like the CLI's `--restart-policy on-failure`. A successful exit is never restarted. Nebius never restarts after preemption, lack of capacity, an unexpected VM stop, a startup failure, or a timeout: the job ends in `ERROR`. For a spot job this client then creates it again while `wait()` runs, up to `restart_attempts` times (10 for `-1`); see [Job.wait](#jobwait). Below `-1`, a bool, or a non-integer raises `ValidationError`. This is not the CLI's global `--retries`, which counts API request retries. |
| `working_dir` | no | Container working directory, at most 128 characters. When omitted, the image `WORKDIR` applies. |
| `secrets` | no | `dict[str, str]` of env name to MysteryBox id: a secret id (`mbsec-…`, latest version) or a version id (`mbsecver-…`). The container gets the payload entry whose key matches the name, or the first entry. Names follow `[A-Za-z_][A-Za-z0-9_]*` and must not repeat an `env` key. Ids are shown in dry-run descriptions; values never reach the client. |
| `registry_secret` | no | MysteryBox secret version id (`mbsecver-…`) for a private image registry. The payload must have `REGISTRY_USERNAME` and `REGISTRY_PASSWORD`. A secret id (`mbsec-…`) raises `ValidationError`, unlike in `secrets`: the API field is `registry_credentials.mysterybox_secret_version`, so registry credentials are pinned to one version. The CLI resolves a secret name or id to a version for you (`--registry-secret`); this client does not yet. |
| `spot_policy` | no | Pricing policy id that caps the spot price. Requires `pricing="spot"`, otherwise `ValidationError`. Sent as `pricing_model.spot_pricing_policy` in place of `follows_spot_price`: the VM is preempted when the spot price exceeds the cap. |
| `shm` | no | `/dev/shm` size in the `disk` syntax, such as `64Gi`, or `0`. An integer is taken as bytes. When omitted, a GPU machine gets `16Gi` and a CPU machine sends none, matching the CLI's `--shm-size` default. Sent as `shm_size_bytes`. |

After construction these attributes are readable: `command`, `image`, `gpu`, `platform`, `preset`, `timeout_seconds`, `pricing`, `subnet_id`, `volumes`, `inject_files`, `env`, `name`, `disk`, `profile`, `project`, `restart_attempts`, `working_dir`, `secrets`, `registry_secret`, `spot_policy`, `shm`. `env` and `secrets` are tuples of `(key, value)` pairs. `timeout_seconds` is the normalized deadline.

### `JobSpec.from_yaml`

```python
ns.JobSpec.from_yaml(path) -> JobSpec
```

Load a schema-version-1 command document. Relative strings inside the document are not expanded.

```yaml
schema_version: 1
kind: job
image: nvidia/cuda:13.1.1-runtime-ubuntu24.04
command: ["nvidia-smi"]
gpu: h100
timeout: 1h
pricing: on-demand
```

Allowed keys: `schema_version`, `kind`, `image`, `command`, `gpu`, `platform`, `preset`, `timeout`, `pricing`, `subnet_id`, `volumes`, `env`, `name`, `disk`, `restart_attempts`, `working_dir`, `secrets`, `registry_secret`, `spot_policy`, `shm`.

`schema_version` must be `1` and `kind` must be `job`. `command` is a list of strings. `gpu` or `cpu` selects the machine and may be omitted for the CPU default. `gpu: cpu` is rejected. A document whose kind or `execution` is `function` raises `UnsupportedFeature`. Unknown keys, duplicate keys, aliases, anchors, tags, merge keys, and a document over 1 MiB are rejected.

### `JobSpec.to_cli`

```python
spec.to_cli(*, project=None) -> str
```

Returns the `nebius ai job create` command that sends the same request as `create()`, one flag per line, for a shell or the Nebius docs. It does not contact Nebius. Without `name=`, a fresh name is generated, as `create()` does. `project` (or the recipe's) becomes `--parent-id`; without one, the CLI uses its profile's project. With no machine argument it prints the CPU platform explicitly, because the CLI's own default is a GPU.

Nothing secret is printed: env values become `--env KEY="$KEY"` placeholders for the shell to fill, secrets appear by id, and a token never appears. `restart_attempts` above 0 has no CLI flag and is printed as a `#` note. The flags come from the same table as the [CLI and API map](cli-api-map.md), and a test keeps the two in step.

### `JobSpec.create`

```python
spec.create(
    dry_run=False,
    *,
    region=None,
    project=None,
    quiet=False,
    idempotency_key=None,
) -> Job
```

```python
spec.create(
    dry_run=True,
    *,
    region=None,
    project=None,
    quiet=False,
    idempotency_key=None,
) -> dict
```

`create()` blocks until Nebius has accepted the job and the id is known, then returns a `Job`. It does not wait for the container to finish. It prints one submission line. `quiet=True` suppresses that line for this call only. `dry_run=True` stays silent either way.

`create(dry_run=True)` builds the request, returns that description, and does not call create. Project and region come from `project=` / `region=` when passed, otherwise from the active SDK profile (`profile=` on the `JobSpec` selects it). When the project id is known and the profile does not name a region, the region is `spec.region` from IAM v2 `ProjectService.get`. That read is not a job create. A missing or ambiguous project, or a project with no region, raises `ValidationError` before create.

The description includes `image`, `command`, `args` when present, `project`, `region`, `platform`, `preset`, `timeout` (seconds), `pricing`, `subnet_id` when set, `volumes` when set, `inject_files` when set, `env` (key names only), `name`, `disk`, `shm` when sent, and, when set, `restart_attempts` (when not `0`), `working_dir`, `secrets` (env name to MysteryBox id), `registry_secret`, and `spot_policy`. Env values, secret values, and inject-file contents are not included.

`region` is the project region shown in the description. Pass it to skip the project lookup. Nebius resolves `gpu`/`cpu` in the project's region at create. `project` on this call overrides `JobSpec.project`.

An optional `transport=` keyword replaces the Nebius SDK client. Tests use it. Scripts leave it unset.

`idempotency_key=` makes a create safe to retry. Every create sends one `x-idempotency-key`: this argument, or a new UUID for the call. When the outcome is ambiguous (a non-definite gRPC code, or an accepted operation without an id), the client sends the same request again with the same key, up to 3 times, and Nebius returns the resource it already made instead of making a second one (checked live: same key and body return the same id; same key with a different body is rejected with `INVALID_ARGUMENT`). Only then does it raise `SubmissionUncertain`, whose `idempotency_key` and recovery say how to retry. Pass your own key, such as `"lr-3e-4"`, to re-run a notebook cell or a script and reconnect to what it already created. Without `name=`, the default name is derived from the key, so a same-key retry sends an identical request; a create without a key still gets a fresh name. How long Nebius remembers a key is not documented. A dry run sends nothing.

### `JobSpec.create_async`

```python
await spec.create_async(
    dry_run=False,
    *,
    region=None,
    project=None,
    quiet=False,
)
```

Same call as `create`, including `quiet`. Cancelling the await leaves the cloud job running.

## Job

One Nebius job id. You get a `Job` from `JobSpec.create()` or `jobs.get()`. Do not construct it yourself.

Property reads do not perform I/O.

| Property | Meaning |
| --- | --- |
| `id` | Nebius job id. |
| `name` | Display name from the recipe, or from the fetched metadata. Lookup ignores it. |
| `status` | Raw `status.state` string. Before the first successful fetch, this is the state from the create response, or `None` when create did not include one. |
| `record` | Last fetched job JSON, or `None` before the first successful `get` or `wait`. |
| `attempts` | Every job id this handle created, oldest first. More than one after a preempted spot job was created again. |
| `preemptions` | How many times `wait()` created the job again after a preemption. |

### `Job.get`

```python
job.get(*, quiet=False) -> dict
```

One `nebius ai job get`. Returns the job JSON: state plus the submitted params. Stores that JSON on `record` and `status`. Prints one snapshot line. `quiet=True` suppresses it for this call only.

`FAILED`, `CANCELLED`, `ERROR`, and a platform timeout are data. `get` does not raise because the container exited badly.

### `Job.wait`

```python
job.wait(timeout=None, *, quiet=False) -> dict
```

Polls `get` about every 10 seconds until `status.state` is terminal, then returns that JSON. Terminal states are `COMPLETED`, `FAILED`, `CANCELLED`, `ERROR`, and `DELETING`. The raw string is preserved. Each new state is one line, with the seconds since the wait started. The last line is the outcome. An interactive terminal also keeps a spinner on the current state. `quiet=True` suppresses that progress for this call only.

`timeout` is seconds, a duration string such as `"45m"`, or a `timedelta`. `wait(timeout=0)` performs one fetch. When the state is not terminal it raises `WaitTimeout` with the job id. `wait(timeout=seconds)` stops the local poll at that deadline. Neither form cancels the cloud job. The job may still be running. Reload it with `jobs.get(id)` or call `cancel`.

#### Spot preemption

A spot job created by this handle with `restart_attempts` other than `0` is created again when `wait()` sees it end in `ERROR` with `state_details.code` `"Preempted"`. The new job has the same recipe, a new idempotency key, and the name plus `-r2`, `-r3`, …; progress says `preempted; resubmitting (1/2)`. `wait()` then follows the new job, within the same local deadline, and returns its final record. `id` and `name` move to the newest attempt and `attempts` lists every id. The budget is `restart_attempts` resubmits, counted apart from container restarts, and at most 10 for `-1`. Other failures, on-demand jobs, and jobs loaded with `jobs.get` are never resubmitted.

**This happens only while a process is running `wait()`.** If the notebook or script stops, nothing is resubmitted. Each attempt starts from scratch with the full `timeout`, so save checkpoints to a mounted volume and resume from them ([Storage and files](../guides/storage.md)).

### `Job.cancel`

```python
job.cancel(*, quiet=False) -> dict
```

Requests cancellation and returns when the request is accepted. It does not wait for the container to stop: the job is usually `CANCELLING` at that point, and stopping can take minutes. The payload does not claim the container has stopped. `wait()` returns once the state is `CANCELLED`. Progress says the request was accepted. `quiet=True` suppresses that line for this call only. A later `get` reports the cancelled state when Nebius has finished cancelling.

### `Job.display`

```python
job.display() -> None
```

Prints the last fetched record as a short summary on stdout: id, name, state, image, command, machine, disk, timings, and one line per instance. Bytes and durations are human-readable. The state word is colored on a terminal. `NO_COLOR` and a non-terminal stdout leave the text plain. `record` stays the raw JSON.

`display` does not fetch. Before the first successful `get` or `wait` it prints the id, the name, and `no record yet`.

### Awaited twins

```python
await job.get_async(*, quiet=False)
await job.wait_async(timeout=None, *, quiet=False)
await job.cancel_async(*, quiet=False)
```

Same calls as the synchronous methods, including `quiet`. The synchronous methods also work inside a running event loop, such as a Jupyter cell: there they run on a worker thread and block until they end. The twins keep the loop responsive. Cancelling `wait_async` or `get_async` does not cancel the cloud job. Cancelling `wait_async` also stops its progress display and its polling, so `asyncio.run` can return.

## jobs

Module-level collection, `ns.jobs`. It matches the CLI noun `nebius ai job`.

### `jobs.get`

```python
ns.jobs.get(job_id, *, profile=None, project=None, quiet=False) -> Job
```

One `nebius ai job get`, starting from an id string. Returns a `Job` whose `record` is already filled. Does not create a job. Prints one snapshot. `quiet=True` suppresses it for this call only.

`profile` and `project` select the SDK profile for this fetch. `transport=` is the same test injection as on `JobSpec.create`.

### `jobs.get_async`

```python
await ns.jobs.get_async(
    job_id,
    *,
    profile=None,
    project=None,
    quiet=False,
    idempotency_key=None,
) -> Job
```

Same fetch as `jobs.get`, including `quiet`.
