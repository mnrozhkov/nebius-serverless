# Endpoints

Reference for the endpoints surface of `nebius_serverless`. The jobs surface is in [reference.md](reference.md).

An endpoint is a container that stays up and answers on a managed URL. A job runs a command and finishes. Creating an endpoint allocates the machine, and it bills while it runs, until it is stopped or deleted. Closing the process does neither.

Import the package as `ns`. Machine selection (`gpu`, `cpu`), pricing, volumes, env handling, and the error types are the same as for [JobSpec](reference.md#jobspec).

## Examples

### Serve an image

`command` is optional. Omit it to keep the image entrypoint. Pass a list of strings to override it, with the same quoting rules as `JobSpec`.

```python
import nebius_serverless as ns

spec = ns.EndpointSpec(
    image="vllm/vllm-openai:v0.19.1",
    command=[
        "bash",
        "-c",
        "python3 -m vllm.entrypoints.openai.api_server "
        "--model Qwen/Qwen3-0.6B --host 0.0.0.0 --port 8000",
    ],
    gpu="h100",
    port=8000,
    disk="500Gi",
    pricing="spot",
    name="qwen-tiny",
)
print(spec.create(dry_run=True))

endpoint = spec.create()
print(endpoint.id, endpoint.token)
record = endpoint.wait()
print(endpoint.url, record["status"]["state"])
```

Endpoints require a bearer token by default (`auth="token"`). `endpoint.token` is the token sent on create. Without `token=`, the client generates a random token for each create, as `nebius ai endpoint create --auth token` does, and prints a notice. `endpoints.get` leaves the token out of `record`; `endpoint.reveal_token()` fetches it from Nebius again, which works for anyone allowed to read the endpoint. For a token you manage yourself, use `token_secret` (below). `endpoint.url` is the managed `https://` URL. `wait` returns when that URL is present, or when the endpoint has stopped or failed. `RUNNING` means the platform workload is up. It does not mean the model answers yet. Poll the application's own route before sending traffic.

Callers send HTTP themselves. This client does not wrap inference.

```python
import urllib.request

request = urllib.request.Request(
    endpoint.url.rstrip("/") + "/v1/models",
    headers={"Authorization": f"Bearer {endpoint.token}"},
)
with urllib.request.urlopen(request) as response:
    print(response.status)
```

For production, keep the token in MysteryBox. Create a secret whose payload has `AUTH_TOKEN`, then pass its id. No token is generated, `endpoint.token` is `None`, and clients read the token from MysteryBox:

```python
spec = ns.EndpointSpec(
    image="vllm/vllm-openai:v0.19.1",
    gpu="h100",
    port=8000,
    token_secret="mbsec-...",
)
```

`auth="none"` makes a public URL: anyone who has it can call the endpoint, and you pay for it. Use it only on purpose.

Delete when the endpoint should stop billing:

```python
endpoint.delete()
```

The same recipe as YAML:

```yaml
schema_version: 1
kind: endpoint
image: vllm/vllm-openai:v0.19.1
command:
  - bash
  - -c
  - python3 -m vllm.entrypoints.openai.api_server --model Qwen/Qwen3-0.6B --host 0.0.0.0 --port 8000
gpu: h100
port: 8000
disk: 500Gi
pricing: spot
name: qwen-tiny
```

```python
spec = ns.EndpointSpec.from_yaml("endpoint.yaml")
print(spec.create(dry_run=True))
```

### Reload a saved id

`endpoint.id` is the handle. `endpoints.get` performs one get and does not create again. `token` on a reloaded endpoint is `None` until `reveal_token()` fetches it.

```python
saved_id = endpoint.id
loaded = ns.endpoints.get(saved_id)
print(loaded.status, loaded.url)
```

### Inside an event loop

```python
endpoint = await spec.create_async()
record = await endpoint.wait_async()
await endpoint.delete_async()
```

Calling `spec.create()` from inside a running event loop raises `ValidationError` and names `create_async`.

A runnable version of this recipe, which also polls `/v1/models`, sends one chat request, and deletes the endpoint in `finally`, is [`examples/endpoint_quickstart.py`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/endpoint_quickstart.py). The YAML is [`examples/from_yaml/vllm_endpoint.yaml`](https://github.com/mnrozhkov/nebius-serverless/blob/main/examples/from_yaml/vllm_endpoint.yaml).

## Contract

| Name | Role |
| --- | --- |
| `EndpointSpec` | Local recipe. No cloud id. |
| `Endpoint` | One Nebius endpoint after create. |
| `endpoints` | Fetch an endpoint from a saved id. |

Constructing an `EndpointSpec`, loading YAML, or importing the package does not authenticate, upload, or create. A second `create()` starts another endpoint and returns another `Endpoint`. Each one bills for compute while it runs. A stopped endpoint is not billed for compute or its disk; mounted volumes still bill ([pricing](https://docs.nebius.com/serverless/pricing-quotas)).

Synchronous methods are the script API. Inside a running event loop they raise `ValidationError` and name the awaited twin (`create_async`, `get_async`, `wait_async`, `stop_async`, `start_async`, `delete_async`, `endpoints.get_async`). Cancelling an await does not delete the endpoint. Only `delete` / `delete_async` requests deletion.

This package does not stream endpoint logs. Log access stays on the CLI and the web console.

`create`, `get`, `wait`, `delete`, and `endpoints.get`, plus their awaited twins, print progress on stderr the same way as the job calls ([Progress and diagnostics](reference.md#progress-and-diagnostics)). `wait` ends with the URL under the `RUNNING` line. `quiet=True` is keyword-only and suppresses progress for that call only. `create(dry_run=True)` prints nothing.

### `EndpointSpec`

```python
ns.EndpointSpec(
    command=None,
    *,
    image,
    port=None,
    ports=None,
    gpu=None,
    platform=None,
    preset=None,
    pricing="on-demand",
    auth="none",
    token=None,
    subnet_id=None,
    volumes=None,
    inject_files=None,
    env=None,
    name=None,
    disk=None,
    profile=None,
    project=None,
    token_secret=None,
    working_dir=None,
    secrets=None,
    registry_secret=None,
    spot_policy=None,
    shm=None,
)
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `command` | no | `None` keeps the image entrypoint. A nonempty list of strings overrides it. The first element is the container command. Further elements are POSIX-quoted and joined into the single `args` string. A one-element command omits `args`. A shell string is rejected. |
| `image` | yes | `name:tag` or `registry/path@sha256:<64 hex>`. Sent unchanged. The client does not resolve a tag to a digest. |
| `port` | one of | One HTTP container port, an integer from 1 to 65535. The platform serves it at a managed `https://` URL. Shortcut for `ports=[port]`. |
| `ports` | one of | Several ports, in the CLI's `--container-port` syntax: an int is an HTTP container port, and a string is `HOST[:CONTAINER][/http\|tcp\|udp]`, for example `[8000, "6379/tcp", "80:8080"]`. The protocol defaults to http and is case-insensitive. An HTTP port gets an `https://` URL, a TCP port a `tls://HOST:443` URL, and a UDP port no managed URL. Pass exactly one of `port` and `ports`. A repeated container port and protocol raises `ValidationError`. Token auth (the default) needs exactly one port, and it must be HTTP: Nebius rejects an extra TCP or UDP port behind its auth proxy (checked live). Other port sets need `auth="none"`. |
| `gpu` | no | GPU selector `MODEL[:COUNT]`, such as `"h100"`; Nebius picks the platform and preset. See [Machine selection](reference.md#machine-selection). |
| `cpu` | no | CPU selector `FAMILY:VCPUS`, such as `"amd:16"`. Excludes `gpu`. |
| `platform` | no | Explicit platform id. Excludes `gpu` and `cpu`. |
| `preset` | no | Explicit preset id. When omitted, the client sends the minimal preset for the chosen platform. An unknown platform id requires `preset`. |
| `pricing` | no | `"on-demand"` (default) or `"spot"`. `"spot"` is preemptible and follows the current price. `"spot"` on CPU raises `UnsupportedFeature`. Any other value raises `ValidationError`. |
| `auth` | no | `"token"` (default) or `"none"`. With `"token"`, callers send `Authorization: Bearer <token>`. `"none"` is a public URL: anyone who has it can call the endpoint, and you pay for it. The default differs from the CLI's `--auth none` on purpose and matches Nebius's own endpoint templates. Token auth needs exactly one HTTP port, which this contract already requires. Any other value raises `ValidationError`. |
| `token` | no | Bearer token sent when `auth="token"`. Setting it with `auth="none"` raises `ValidationError`. When omitted, the client generates a random token on each create, sets `Endpoint.token`, and prints a notice. `Endpoint.reveal_token()` fetches it again later. The value is left out of `repr`, dry-run descriptions, and exception text. |
| `subnet_id` | no | Sent when set. Omit it when the project has a single subnet. |
| `volumes` | no | Mount strings, passed through, same form as `JobSpec`. The `s3://` `:profile` suffix raises `UnsupportedFeature`. |
| `inject_files` | no | `local_path:container_path` strings, same form and 64 KiB limit as `JobSpec`. Files are read on create. Not a YAML key, as for `JobSpec`. |
| `env` | no | `dict[str, str]`. Values are sent on create and left out of dry-run descriptions and exception text. Use `secrets` for tokens. |
| `name` | no | Console label (`metadata.name`), sent unchanged on every create. When omitted, each `create()` generates a new `endpoint-` plus 12 hex characters and the returned handle's `name` is the one sent, so a loop over one recipe gives distinct names. A dry run generates one too, so it cannot predict the name a later create sends. The recipe's `name` attribute stays `None`. It is not a lookup key. |
| `disk` | no | CLI size string, passed through, such as `500Gi`. When omitted, the request uses `250Gi`. |
| `profile` | no | Nebius SDK profile name. When omitted, the active profile is used. |
| `project` | no | Project id. Overrides the profile's project. |
| `token_secret` | no | MysteryBox secret id (`mbsec-…`) or version id (`mbsecver-…`) whose payload has `AUTH_TOKEN`. Sent as `auth_token_mysterybox_secret`. Requires `auth="token"` and cannot be combined with `token`; either mistake raises `ValidationError`. No token is generated, `Endpoint.token` is `None`, and the dry-run description shows `"token": "secret"` with the id. |
| `working_dir` | no | Container working directory, at most 128 characters. When omitted, the image `WORKDIR` applies. |
| `secrets` | no | `dict[str, str]` of env name to MysteryBox id: a secret id (`mbsec-…`, latest version) or a version id (`mbsecver-…`). The container gets the payload entry whose key matches the name, or the first entry. Names follow `[A-Za-z_][A-Za-z0-9_]*` and must not repeat an `env` key. Ids are shown in dry-run descriptions; values never reach the client. |
| `registry_secret` | no | MysteryBox secret version id (`mbsecver-…`) for a private image registry. The payload must have `REGISTRY_USERNAME` and `REGISTRY_PASSWORD`. A secret id (`mbsec-…`) raises `ValidationError`, unlike in `secrets`: the API field is `registry_credentials.mysterybox_secret_version`, so registry credentials are pinned to one version. The CLI resolves a secret name or id to a version for you (`--registry-secret`); this client does not yet. |
| `spot_policy` | no | Pricing policy id that caps the spot price. Requires `pricing="spot"`, otherwise `ValidationError`. Sent as `pricing_model.spot_pricing_policy` in place of `follows_spot_price`: the VM is preempted when the spot price exceeds the cap. |
| `shm` | no | `/dev/shm` size in the `disk` syntax, such as `64Gi`, or `0`. An integer is taken as bytes. When omitted, a GPU machine gets `16Gi` and a CPU machine sends none, matching the CLI's `--shm-size` default. Sent as `shm_size_bytes`. |

There is no `timeout` or `restart_attempts` parameter. An endpoint has no job deadline, and the endpoint API has no restart count. `shm` follows the job rule: `16Gi` on GPU, none on CPU, unless set.

After construction these attributes are readable: `command`, `image`, `port` (`None` when `ports` was used), `ports` (normalized strings such as `"8000/http"`), `gpu`, `platform`, `preset`, `pricing`, `auth`, `token`, `subnet_id`, `volumes`, `inject_files`, `env`, `name`, `disk`, `profile`, `project`, `token_secret`, `working_dir`, `secrets`, `registry_secret`, `spot_policy`, `shm`. `token` is the value passed in, or `None` when it will be generated or read from `token_secret`. `command` is `None` when the image entrypoint is kept. `env` and `secrets` are tuples of `(key, value)` pairs.

### `EndpointSpec.from_yaml`

```python
ns.EndpointSpec.from_yaml(path) -> EndpointSpec
```

Load a schema-version-1 endpoint document. Relative strings inside the document are not expanded.

Allowed keys: `schema_version`, `kind`, `image`, `command`, `port`, `ports`, `gpu`, `platform`, `preset`, `pricing`, `auth`, `subnet_id`, `volumes`, `env`, `name`, `disk`, `token_secret`, `working_dir`, `secrets`, `registry_secret`, `spot_policy`, `shm`.

`schema_version` must be `1` and `kind` must be `endpoint`. A `kind: job` document is rejected and points at `JobSpec.from_yaml`; `JobSpec.from_yaml` does the same for `kind: endpoint`. `command` may be omitted. When present it is a list of strings. `port` is an integer; `ports` is a list in the Python form, and only one of them may be set. `gpu` may be omitted for CPU. `gpu: cpu` is rejected. Unknown keys, duplicate keys, aliases, anchors, tags, merge keys, and a document over 1 MiB are rejected. `env` values are treated like the Python values: sent on create, omitted from dry-run descriptions and exception text. A `token` key is rejected with `ValidationError`, because YAML files are often committed: store the token in MysteryBox and set `token_secret`, or pass `token=` in Python. The error never contains the value.

### `EndpointSpec.create`

```python
spec.create(dry_run=False, *, region=None, project=None, quiet=False, idempotency_key=None) -> Endpoint
spec.create(dry_run=True, *, region=None, project=None, quiet=False) -> dict
```

`create()` blocks until Nebius has accepted the endpoint and the id is known, then returns an `Endpoint`. It does not wait until the URL is ready. Acceptance is not readiness.

`create(dry_run=True)` builds the request, returns that description, and does not call create. Project and region come from `project=` / `region=` when passed, otherwise from the active SDK profile (`profile=` on the `EndpointSpec` selects it). A missing or ambiguous project or region raises `ValidationError` before create.

The description includes `image`, `command` and `args` when a command was set, `ports` (normalized strings such as `["8000/http", "6379/tcp"]`), `project`, `region`, `platform`, `preset`, `pricing`, `auth`, `token` when `auth="token"`, `subnet_id` when set, `volumes` when set, `inject_files` when set, `env` (key names only), `name`, `disk`, and `shm` when sent. `token` is the word `"provided"` or `"generated"`, never the value. Env values and inject-file contents are not included.

`region` is the project region shown in the description; Nebius resolves `gpu`/`cpu` there at create. `project` on this call overrides `EndpointSpec.project`.

An optional `transport=` keyword replaces the Nebius SDK client. Tests use it. Scripts leave it unset.

`idempotency_key=` works as for jobs ([reference](reference.md#jobspeccreate)). With a key you pass, token auth needs `token=` or `token_secret=`: a token generated in another process would differ, so a reconnected endpoint would not match it.

### `EndpointSpec.to_cli`

```python
spec.to_cli(*, project=None) -> str
```

The `nebius ai endpoint create` command for this recipe, as for [`JobSpec.to_cli`](reference.md#jobspecto_cli): `--container-port` per port, `--auth`, and `--token-secret` by id. A `token=` value is never printed; a `#` note says to add `--token` yourself.

### `EndpointSpec.create_async`

```python
await spec.create_async(dry_run=False, *, region=None, project=None, quiet=False)
```

Same call as `create`. Cancelling the await leaves the endpoint running when create was already accepted.

## Endpoint

One Nebius endpoint id. You get an `Endpoint` from `EndpointSpec.create()` or `endpoints.get()`. Do not construct it yourself.

Property reads do not perform I/O.

| Property | Meaning |
| --- | --- |
| `id` | Nebius endpoint id. |
| `name` | Display name from the recipe, or from the fetched metadata. Lookup ignores it. |
| `status` | Raw `status.state` string. Before the first successful fetch, this is the state from the create response, or `None` when create did not include one. |
| `record` | Last fetched endpoint JSON, or `None` before the first successful `get` or `wait`. `spec.auth_token` is removed from fetched JSON. |
| `url` | The first managed `https://` URL in the last fetch, or `None` when it has none. |
| `urls` | Every managed URL in the last fetch, in API order: `https://` for HTTP ports, `tls://` for TCP ports. |
| `token` | Bearer token from this create call, or `None` on an endpoint loaded by `endpoints.get` and when `auth="none"`. |

### `Endpoint.get`

```python
endpoint.get(*, quiet=False) -> dict
```

One `nebius ai endpoint get`. Returns the endpoint JSON. Stores that JSON on `record` and `status`, and sets `url` when `status.public_endpoints` contains an `https://` URL. Does not change `token`.

`ERROR` and `STOPPED` are data. `get` does not raise because the workload failed. A missing id raises `Error` and includes that id.

### `Endpoint.wait`

```python
endpoint.wait(timeout=3600, *, until="ready", probe=None, token=None, quiet=False) -> dict
```

Polls `get` about every 5 seconds until the endpoint reaches the target in `until`, or will not reach it, then returns that JSON.

`until="ready"` (default): ready means `status.state` is `RUNNING` and the fetched record has a managed URL for each protocol that gets one: `https://` when the endpoint exposes an HTTP port and `tls://` when it exposes a TCP port. A UDP-only endpoint is ready at `RUNNING`. `ERROR`, `STOPPED`, and `DELETING` end the wait as data. The caller reads `endpoint.status` and `endpoint.url`. `PROVISIONING`, `STARTING`, `IMAGE_PULLING`, and `STOPPING` keep the poll going, and so does `RUNNING` without a URL. After `start()` on this handle, the `STOPPED` left over from before the start keeps the poll going until another state appears.

An endpoint in `ERROR` is not billed; it keeps its id until `delete`.

`until="stopped"`: ends on `STOPPED`, or on `ERROR` or `DELETING` as data. Use it after `stop()`: a plain `wait()` right after a stop can return on the `RUNNING` that is still reported. Any other `until` raises `ValidationError`.

`endpoint.url` always reflects the latest fetch, and is `None` when that record has no URL. An `ERROR` or `DELETING` record can still list its URL.

By default the wait gives up after 3600 seconds (60 minutes) and raises `WaitTimeout`. The message names the endpoint id and its last state, says it may be billing, and names `wait()` and `delete()` as the next calls. Sixty minutes covers Nebius's capacity search (up to 30 minutes), creation, and an image pull with model load. `timeout` is seconds, a duration string such as `"45m"` or `"1h30m"`, or a `timedelta`; anything else raises `ValidationError`. `wait(timeout=None)` waits without a limit. `wait(timeout=0)` performs one fetch. `wait(timeout=seconds)` stops the local poll at that deadline. No form deletes or stops the endpoint. Job `wait` has no default limit, because the job's own `timeout` ends it on the server. An endpoint has no such deadline.

`RUNNING` with a URL means the tunnel is up, not that the server inside answers: a model can still be loading. `probe="/v1/models"` then GETs `url + probe` every 5 seconds until it answers 2xx, within the same deadline. It sends `Authorization: Bearer <token>` when the handle knows the token (from `create`, `reveal_token()`, or `token=`). A refused connection, 404, 502, 503 and other codes keep polling, with one progress line per new answer; 401 or 403 raises `AccessError` at once. Running out of time raises `WaitTimeout`. The token goes only into the request header and is never logged.

### `Endpoint.stop`

```python
endpoint.stop(*, quiet=False) -> dict
```

Requests a stop and returns `{"id": ..., "resource_id": ...}` when the request is accepted. The endpoint keeps its id and configuration; only `delete` removes it. While it is stopped, Nebius does not bill compute or the disk; mounted volumes still bill ([pricing](https://docs.nebius.com/serverless/pricing-quotas)). `start` runs it again. The state passes through `STOPPING` to `STOPPED`; `wait(until="stopped")` blocks until then.

### `Endpoint.start`

```python
endpoint.start(*, quiet=False) -> dict
```

Requests a start of a stopped endpoint and returns `{"id": ..., "resource_id": ...}` when the request is accepted. It does not wait: call `wait()` to block until the endpoint is `RUNNING` with its URL. The endpoint comes back at the same URL and IP address; the boot disk is not preserved ([stop and start](https://docs.nebius.com/serverless/maintenance/stop-start)). A preempted spot endpoint goes to `STOPPED` with code `Preempted`; `start` resumes it.

```python
endpoint.stop()
endpoint.wait(until="stopped")
endpoint.start()
endpoint.wait()            # RUNNING with the same URL
```

### `Endpoint.delete`

```python
endpoint.delete(*, quiet=False) -> dict
```

Requests deletion and returns `{"id": ..., "resource_id": ...}` when the request is accepted. The payload does not claim the machine has stopped or that billing has ended. A later `get` reports `DELETING` or a missing id once Nebius has finished.

There is no `cancel`. Cancellation is the job verb. An endpoint is removed with `delete`.

### `Endpoint.reveal_token`

```python
endpoint.reveal_token() -> str | None
await endpoint.reveal_token_async() -> str | None
```

Fetches the endpoint once and returns its bearer token, and sets `endpoint.token`. Nebius's `Get` returns `spec.auth_token` to anyone allowed to read the endpoint (checked live on 19 November 2026), so a handle loaded with `endpoints.get` can recover the token after a restart. Returns `None` for a public endpoint and for one that reads its token from MysteryBox (`token_secret`). The value is never logged, and `record` never holds it.

### `Endpoint.display`

```python
endpoint.display() -> None
```

Prints the last fetched record on stdout, including the managed URL, image, platform, and instances. Does not fetch. Before the first successful `get` or `wait`, prints the id and name.

### Awaited twins

```python
await endpoint.get_async(*, quiet=False)
await endpoint.wait_async(timeout=3600, *, until="ready", probe=None, token=None, quiet=False)
await endpoint.stop_async(*, quiet=False)
await endpoint.start_async(*, quiet=False)
await endpoint.delete_async(*, quiet=False)
```

Same calls as the synchronous methods. Cancelling `wait_async` or `get_async` does not delete the endpoint. A cancelled `wait_async` stops polling once a `get` already in flight returns.

## endpoints

Module-level collection, `ns.endpoints`. It matches the CLI noun `nebius ai endpoint`.

### `endpoints.get`

```python
ns.endpoints.get(endpoint_id, *, profile=None, project=None, quiet=False) -> Endpoint
```

One `nebius ai endpoint get`, starting from an id string. Returns an `Endpoint` whose `record` is already filled. Does not create an endpoint. `token` is `None`; call `reveal_token()` to fetch it.

`profile` and `project` select the SDK profile for this fetch. `transport=` is the same test injection as on `EndpointSpec.create`.

### `endpoints.get_async`

```python
await ns.endpoints.get_async(endpoint_id, *, profile=None, project=None, quiet=False) -> Endpoint
```

Same fetch as `endpoints.get`.

## Errors

Every error is an `Error`, the same types as [reference.md](reference.md#errors). The endpoint id is stored on `resource_id` when it is known, so existing handlers see a resource id. Get and delete failures include that id in the text. `operation` is `EndpointSpec` for constructor errors and the method name otherwise. `recovery` names `endpoints.get` and `delete` instead of `jobs.get` and `cancel`. Text does not include env values or tokens.

| Type | When |
| --- | --- |
| `ValidationError` | Bad command, image, port, auth, machine selector, YAML, a create Nebius rejected (with its code and reason), missing project or region, or a sync call inside a running event loop. |
| `UnsupportedFeature` | Spot on CPU, and the excluded shapes in the next section. |
| `AccessError` | Authentication or permission failure from the SDK. |
| `CapacityError` | The API rejected the endpoint for quota or capacity. |
| `TransportError` | The call failed and there is no reason to think create was accepted. |
| `SubmissionUncertain` | Create may have been accepted and the id is unknown, after 3 tries with one idempotency key. `idempotency_key` holds it; a create with that key and the same recipe returns the endpoint Nebius made, if any. |
| `KeyConflict` | The API reports an existing resource and its id is known. |
| `WaitTimeout` | The local wait ended before a final state. The endpoint is unchanged and may still be billing; `wait()` again or `delete()`. |

Closing the process, a local `WaitTimeout`, and cancelling an awaited twin do not delete the endpoint.

## Out of scope

- HTTP helpers, inference clients, and application health probes. The caller uses `endpoint.url`.
- A context manager that deletes on exit. Process death would leave the endpoint billing, and the API has no remote lifetime to enforce.
- `--public` / a public IP. The managed `https://` URL does not need it.
- `restart`, `list`, `delete` of a recipe that was never created, `get-by-name`, and `wait(until="deleted")`. `stop` and `start` are supported.
- Secret selectors by name or `ID@VERSION`, which the CLI resolves. `secrets`, `token_secret`, and `registry_secret` take MysteryBox ids.
- Source upload and Python function serving.
- DevLabs, scaling, and extra replicas. One preset is one machine.

## Names

This surface names each type after the API. `EndpointSpec` is the recipe, as in the API's `EndpointSpec`. `Endpoint` is the cloud resource with an `aiendpoint-…` id, as in the API, the CLI (`nebius ai endpoint`), the console, and the `endpoints.get` collection. Jobs follow the same rule: `JobSpec` is the recipe and `Job` is the `aijob-…` resource. One recipe can create many resources, because each `create()` makes a new id. `create` and `delete` match `nebius ai endpoint create` and `nebius ai endpoint delete`. `auth` defaults to `"token"`, unlike the CLI's `--auth none`: a billing GPU URL is public only when the code says `auth="none"`. `pricing` defaults to `"on-demand"`, matching `JobSpec`.
