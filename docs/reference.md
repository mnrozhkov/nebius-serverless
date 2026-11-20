# Reference

User-facing surface of `nebius_serverless`. Import it as `ns`.

```python
import nebius_serverless as ns
```

| Name | Role |
| --- | --- |
| [`JobSpec`](#jobspec) | Local recipe. No cloud id. |
| [`Job`](#job) | One Nebius job after create. |
| [`jobs`](#jobs) | Fetch a job from a saved id. |
| [`EndpointSpec`, `Endpoint`, `endpoints`](endpoints.md) | Long-running endpoints on a managed URL. Documented in [endpoints.md](endpoints.md). |
| [`configure_logging`](#configure_logging) | Opt in to diagnostic logs. |
| [`diagnostics`](#diagnostics) | Local version and presentation metadata. |
| [`whoami`](#whoami) | Check the account, auth source, project and region before creating anything. |
| [`configure`](#configure) | Authenticate without a CLI login (CI), and set default project and region. |
| [`close`](#close) | Release the shared Nebius SDK early. |
| [Errors](#errors) | Failures raised by this package. |
| [CLI and API map](#cli-and-api-map) | Each argument's CLI flag and API field. |

Constructing a `JobSpec`, loading YAML, or importing the package does not authenticate, upload, or create. The Nebius SDK is imported on the call that resolves a profile or contacts Nebius.

Logs stay on the CLI: `nebius ai job logs <id>`. This package does not stream them.

Synchronous methods are the script API. Inside a running event loop they raise `ValidationError` and name the awaited twin (`create_async`, `get_async`, `wait_async`, `cancel_async`, `jobs.get_async`). Cancelling an await does not cancel the cloud job. Only `cancel` / `cancel_async` does.

## Progress and diagnostics

`create`, `get`, `wait`, `cancel`, and `jobs.get`, plus their awaited twins, print progress on stderr. An interactive terminal shows a Rich spinner. The job id is cyan, an in-progress state is yellow, success is green, and failure is red. The words stay in the line, so color is not the only signal. A notebook, a CI run (`CI`, `GITHUB_ACTIONS`, or `GITLAB_CI`), a pipe, or a dumb terminal gets append-only lines with no color. `NO_COLOR` turns color off and keeps the spinner. `create(dry_run=True)` prints nothing.

`quiet=True` is keyword-only. It suppresses progress for that call only. It is not stored on the `Job`, it does not suppress exceptions or diagnostic logs, and it is not sent to Nebius.

```python
job = spec.create()      # submission line
job.wait()               # state, spinner, outcome

job = spec.create(quiet=True)
job.wait()               # progress is visible again
job.wait(quiet=True)
```

With debug logging enabled, progress stays append-only so log lines remain readable.

`configure_logging(level="INFO")` sets the `nebius_serverless` logger level. When the application has not already attached a handler, it adds one stderr `StreamHandler` and turns propagation off. Call it again to change the level; the helper keeps a single handler of its own. An application handler is left in place, including its level, filters, and the current propagation setting. Applications that already configure logging should set the `nebius_serverless` logger themselves.

Debug lines can append `event`, `operation_id`, `request_id`, `job_id`, `attempt`, `duration_ms`, and `status_code` when those values exist. `operation_id` identifies the local call. It is not a server request id. Records do not include environment values, credentials, headers, or request bodies.

```python
ns.configure_logging(level="DEBUG")
job.wait(quiet=True)  # no progress; DEBUG records still emit
```

`diagnostics()` returns a JSON-serializable dict and prints nothing:

| Key | Meaning |
| --- | --- |
| `version` | Package version. |
| `python` | Python version. |
| `platform` | OS platform. |
| `presentation` | `plain` or `interactive` for the current stderr. |

It does not report an ambient quiet switch. A call can still pass `quiet=True`. No profile, credentials, environment values, or job records are included.

A local timeout stops waiting only. No cancellation was requested, and the job may still be running. Reload it with `jobs.get(id)` or call `cancel`. Cancelling `wait_async` stops the progress display and the polling, once a `get` already in flight returns. It does not cancel the cloud job.

SDK progress and these diagnostics are separate from container logs. Those remain `nebius ai job logs <id>`.

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
| `gpu` | no | GPU selector `MODEL[:COUNT]`, such as `"h100"` or `"h100:8"`; Nebius picks the platform and preset. See [Machine selection](#machine-selection). `gpu="cpu"` raises `ValidationError`. |
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

Nothing secret is printed: env values become `--env KEY="$KEY"` placeholders for the shell to fill, secrets appear by id, and a token never appears. `restart_attempts` above 0 has no CLI flag and is printed as a `#` note. The flags come from the same table as the [CLI and API map](#cli-and-api-map), and a test keeps the two in step.

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

**This happens only while a process is running `wait()`.** If the notebook or script stops, nothing is resubmitted. Each attempt starts from scratch with the full `timeout`, so save checkpoints to a mounted volume and resume from them, as [examples/train/axolotl_lora.py](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/train/axolotl_lora.py) does with `s3://axolotl-artifacts`.

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

Same calls as the synchronous methods, including `quiet`. Cancelling `wait_async` or `get_async` does not cancel the cloud job. Cancelling `wait_async` also stops its progress display and its polling, so `asyncio.run` can return.

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

## Machine selection

`gpu` and `cpu` are the API's machine selectors, sent as they are. Nebius picks the platform and preset in the project's region at create time, and reads (`get`, `wait`, `display`) show the platform and preset it chose.

| Argument | Example | Sent as | Machine |
| --- | --- | --- | --- |
| `gpu="MODEL"` | `gpu="h100"` | `spec.gpu = ["h100"]` | One GPU of that model |
| `gpu="MODEL:COUNT"` | `gpu="h100:8"` | `spec.gpu = ["h100:8"]` | COUNT GPUs, at least 1 |
| `cpu="FAMILY:VCPUS"` | `cpu="amd:16"` | `spec.cpu = ["amd:16"]` | A CPU machine with that many vCPUs |
| `platform=`, `preset=` | `platform="gpu-h100-sxm"` | `platform`, `preset` | Exactly that platform and preset |
| none of them | | `platform="cpu-d3"`, `preset="2vcpu-8gb"` | The smallest CPU machine |

The client checks only the shape (`h100:0`, `:8` and `gpu="cpu"` raise `ValidationError`). `gpu` and `cpu` exclude each other, and either excludes `platform` and `preset`. A model, count or family that the region does not offer is Nebius's call: create raises `ValidationError` with the code and Nebius's reason, or `CapacityError`. `platform=` without `preset=` fills the smallest preset for known platforms; the CLI marks `--platform`/`--preset` "will be deprecated soon". Spot pricing on a CPU machine (`cpu=`, a `cpu-…` platform, or no machine argument) raises `UnsupportedFeature`.

### `configure_logging`

```python
ns.configure_logging(*, level="INFO") -> None
```

Described under [Progress and diagnostics](#progress-and-diagnostics). `level` is a logging name such as `"DEBUG"` or `"INFO"`, or a numeric level.

### `whoami`

```python
ns.whoami(*, profile=None, project=None, quiet=False) -> dict
await ns.whoami_async(*, profile=None, project=None, quiet=False) -> dict
```

A cheap, read-only check: one IAM `GetProfile` call through the shared SDK, then the same project and region resolution `create()` uses, so the two cannot disagree. It creates, lists and changes nothing. It prints one line on stderr, such as `Signed in as user Ada ada@example.com (useraccount-…) via the active CLI profile. Jobs go to project-… in eu-north1.`, and returns:

| Key | Meaning |
| --- | --- |
| `account` | `kind` (`user`, `service_account` or `anonymous`), `id`, and `name` / `email` when the API returns them. |
| `auth` | `source` (`profile`, `env`, `credentials_file` or `sdk`) and a `detail` that names a profile, variable or file, never a secret. |
| `project`, `region` | Where `create()` would put a job. `None` when they cannot be resolved; `problem` then says why. |

Credentials that Nebius rejects raise `AccessError` with the request id and what to do next. Every `AccessError` suggests running `ns.whoami()`. `quiet=True` suppresses the line and returns the same dict.

### `configure`

```python
ns.configure(*, sdk=None, credentials_file=None, profile=None, project=None, region=None) -> None
```

Sets how this process authenticates, and the default project and region for calls that pass none. Each call replaces the previous settings; `ns.configure()` with no arguments restores the defaults. Per-call `profile=` and `project=` still win.

The first source that applies is used:

| Source | How | Project and region |
| --- | --- | --- |
| `configure(sdk=…)` | A `nebius.sdk.SDK` you built, used as-is and never closed by this package | From the call or `configure` |
| `configure(credentials_file=…)` | A service-account credentials file. A missing file raises `ValidationError` naming the path | From the call or `configure` (required without a CLI profile) |
| `NEBIUS_IAM_TOKEN` | An IAM token in the environment. With no CLI config file it is the only credential; with one, it still comes before the profile's own login, as in the CLI | From the call, `configure`, or the CLI profile if there is one |
| CLI profile | `~/.nebius/config.yaml`: `profile=`, `configure(profile=…)`, `NEBIUS_PROFILE`, or the active profile | From the profile; the region from the project when the profile has none |

Without a CLI config file there is no profile to read a project from, so `create()` raises `ValidationError` until `project=` or `configure(project=…)` names one. No credential value appears in errors, logs, `repr`, or `diagnostics()`.

#### Run in CI

Use a service account. Create an authorized key for it, then store the credentials file as a CI secret:

```yaml
# .github/workflows/train.yml
jobs:
  train:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install nebius-serverless
      - run: echo "$NEBIUS_SA_CREDENTIALS" > "$RUNNER_TEMP/sa.json"
        env:
          NEBIUS_SA_CREDENTIALS: ${{ secrets.NEBIUS_SA_CREDENTIALS }}
      - run: python train.py
        env:
          NEBIUS_CREDENTIALS_FILE: ${{ runner.temp }}/sa.json
          NEBIUS_PROJECT: ${{ vars.NEBIUS_PROJECT }}
```

```python
# train.py
import os

import nebius_serverless as ns

ns.configure(
    credentials_file=os.environ["NEBIUS_CREDENTIALS_FILE"],
    project=os.environ["NEBIUS_PROJECT"],
)
job = ns.JobSpec(["python", "train.py"], image="…", gpu="h100", timeout="2h").create()
job.wait()
```

A short-lived token works too: set `NEBIUS_IAM_TOKEN` and pass `project=`; no `configure` call is needed.

### `close`

```python
ns.close() -> None
```

All calls in one process share one Nebius SDK per profile: it opens on the first call that needs it and closes at interpreter exit. A forked child opens its own, and a project's region is looked up once per process. `close()` releases the SDK early, for example in a long-running service; the next call opens a new one.

### `diagnostics`

```python
ns.diagnostics() -> dict
```

Described under [Progress and diagnostics](#progress-and-diagnostics). Prints nothing and does not contact Nebius.

## CLI and API map

Each recipe argument, the `nebius ai job create` / `nebius ai endpoint create` flag that does the same, and the API field it fills. Checked against Nebius CLI 0.12.309 `--help` and the `nebius` Python SDK 0.6.43 descriptors. `spec.to_cli()` prints these flags for a recipe. `tests/test_flag_map.py` fails when a `JobSpec` or `EndpointSpec` argument has no row here.

| SDK argument | CLI flag | API field | Note |
| --- | --- | --- | --- |
| `command=[...]` | `--container-command` + `--args` | `container_command`, `args` | Intended: the SDK takes one list and splits it, so quoting is done for you. |
| `image=` | `--image` | `image` | Same. |
| `port=`, `ports=` (endpoint) | `--container-port HOST[:CONTAINER][/http\|tcp\|udp]`, repeatable | `ports[]` | Same syntax: `ports=[8000, "6379/tcp", "80:8080"]`; `port=8000` is the one-HTTP-port shortcut. Token auth needs exactly one port, an HTTP one. Jobs: not exposed. |
| `gpu=` | `--gpu MODEL[:COUNT]` | `gpu[]` | Same; Nebius picks the platform and preset. Intended: with no machine argument, the SDK picks the smallest CPU machine and the CLI picks a GPU. |
| `cpu=` | `--cpu FAMILY:VCPUS` | `cpu[]` | Same. |
| `platform=`, `preset=` | `--platform`, `--preset` | `platform`, `preset` | Same. The CLI says these will be deprecated in favour of `--gpu`/`--cpu`. |
| `pricing="spot"` | `--preemptible` + `--follows-spot-price` | `preemptible`, `pricing_model` | Intended: one argument for the two flags that always go together. |
| `spot_policy=` | `--spot-pricing-policy-id` | `pricing_model.spot_pricing_policy.id` | Rename only. Requires `pricing="spot"`. |
| `auth=` (endpoint) | `--auth none\|token` | presence of `auth_token` or `auth_token_mysterybox_secret` | Intended: the SDK defaults to `"token"` and the CLI to `none`, so a billing GPU URL is public only when the code says so. |
| `token=`, `token_secret=` (endpoint) | `--token`, `--token-secret SELECTOR` | `auth_token`, `auth_token_mysterybox_secret` | The SDK takes a secret id or version id; the CLI also resolves names and `ID@VERSION`. |
| `subnet_id=` | `--subnet-id` | `subnet_id` | Same. |
| `volumes=` | `--volume SOURCE:PATH[:MODE[:PROFILE]]` | `volumes[]` | The SDK rejects the `:PROFILE` and `PROFILE@SECRET` S3 credential forms. |
| `inject_files=` | `--inject-file` | `injected_files` | Same, at most 64 KiB each. |
| `env=` | `--env KEY=VALUE` | `environment_variables[].value` | Same. |
| `secrets={}` | `--env-secret KEY=SELECTOR` | `environment_variables[].mysterybox_secret` | The SDK takes a secret id or version id; the CLI also resolves names and `ID@VERSION`. |
| `name=` | `--name` | `metadata.name` | Same. Generated per create when omitted. |
| `disk=` | `--disk-size` | `disk.size_bytes` | Rename only. Default `250Gi` in both. |
| `shm=` | `--shm-size` | `shm_size_bytes` | Same default: `16Gi` on GPU, none on CPU. |
| `project=` | `--parent-id` | `metadata.parent_id` | Rename only. |
| `profile=` | global `--profile` | n/a | Same. |
| `working_dir=` | `--working-dir` | `working_dir` | Same, at most 128 characters. |
| `registry_secret=` | `--registry-secret SELECTOR` | `registry_credentials.mysterybox_secret_version` | The SDK takes a version id only, because the API pins registry credentials to one version; the CLI resolves a name or secret id to a version. |
| `restart_attempts=` (job) | `--restart-policy never\|on-failure` | `restart_attempts` | `0` is `never` and `-1` is `on-failure`; the SDK also takes a count. Not the CLI's global `--retries`, which counts API request retries. |
| `timeout=` (job) | `--timeout` | `timeout` | Same range, 5 minutes to 168 hours. Intended: required in the SDK, an explicit cost ceiling; the CLI defaults to 24 hours. |
| n/a | `--public` | `public_ip` | Not exposed. The managed `https://` URL does not need it. |
| n/a | `--ssh-key` | `ssh_authorized_keys` | Not exposed. |
| n/a | `--registry-username`, `--registry-password` | `registry_credentials.username`, `.password` | Not exposed. Use `registry_secret`. |
| `create(dry_run=True)` | `--dry-run` | n/a | The SDK builds the request locally and does not call create. The CLI describes its flag as "Validate the request without creating a resource". |
| `create()` | `create` (`--async` returns early) | `Create` | The SDK returns once the id is known, not when the job or endpoint is ready. |

| Resource | In the SDK | CLI only |
| --- | --- | --- |
| job | `create`, `get`, `wait` (with spot resubmit), `cancel`, `to_cli` | `delete`, `list`, `get-by-name`, `restart`, `logs`, `ssh`, `run` (beta) |
| endpoint | `create`, `get`, `wait` (with `probe=`), `stop`, `start`, `delete`, `reveal_token`, `to_cli` | `restart`, `list`, `get-by-name`, `logs`, `ssh` |

## Errors

Every error is an `Error`. `UnsupportedFeature` is a `ValidationError`. Exception text includes the operation and the job id when those are known. It does not include env values, tokens, or registry passwords.

| Attribute | Meaning |
| --- | --- |
| `operation` | Call that failed, such as `"create"` or `"wait"`. |
| `resource_id` | The job or endpoint id, when it is known. |
| `project` | Project id when it was recorded. |
| `key` | Resource key when it was recorded. |
| `recovery` | Short hint. The default is to reload with `jobs.get` and not to send a second create. |
| `diff` | Optional tuple of extra detail. |
| `code` | Stable SDK category: `validation`, `unsupported`, `access`, `capacity`, `transport`, `submission_uncertain`, `key_conflict`, or `wait_timeout`. |
| `request_id` | Server request id when the Nebius exception already exposed one. Otherwise `None`. |
| `trace_id` | Server trace id, when Nebius sent one. `str(exc)` ends with `(request id …, trace id …)`: quote that line to Nebius support. |
| `status_code` | The gRPC status code name, such as `"RESOURCE_EXHAUSTED"`, when the error came from a Nebius call. Branch on it instead of parsing text. |
| `violations` | `CapacityError` only: each exceeded quota as a dict with `quota`, `requested`, `limit`, `region` and `message`. The message names them too, for example `Quota exceeded: compute.gpu.h100 requested 8, limit 4 (eu-north1).` |

| Type | When |
| --- | --- |
| `ValidationError` | Bad command, image, timeout, machine selector, YAML, a create Nebius rejected (with its code and reason), missing project or region, or a sync call inside a running event loop. |
| `UnsupportedFeature` | Spot on CPU, function YAML, and other commands excluded from this slice. |
| `AccessError` | Authentication or permission failure from the SDK. |
| `CapacityError` | The API rejected the job for quota or capacity. |
| `TransportError` | The call failed and there is no reason to think create was accepted. |
| `SubmissionUncertain` | The outcome of create is still unknown after the same request was sent 3 times with one idempotency key. `idempotency_key` holds that key; `create(idempotency_key=…)` with the same recipe returns the job Nebius made, if any, without making a second one. |
| `KeyConflict` | The API reports an existing resource and its id is known (`resource_id`). Reload that resource. Do not send a second create. |
| `WaitTimeout` | Local waiting stopped. No cancellation was requested. The job may still be running. |

Closing the process, a local `WaitTimeout`, and cancelling an awaited twin do not cancel the cloud job.

A job or endpoint that ends in `ERROR` or `FAILED` is returned as data. Its reason, `status.state_details.code` and `message` plus any quota violations, is shown in the final progress line of `wait()` and in `display()`. Errors never chain the Nebius SDK exception (`raise … from None`), because a server message can echo request fields; the attributes above carry what support needs.
