# CLI and API map

Each recipe argument, the `nebius ai job create` / `nebius ai endpoint create` flag that does the same, and the API field it fills. Checked against Nebius CLI 0.12.309 `--help` and the `nebius` Python SDK 0.6.43 descriptors. `spec.to_cli()` prints these flags for a recipe. `tests/test_flag_map.py` fails when a `JobSpec` or `EndpointSpec` argument has no row here.

## Arguments

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
| `preemption_retries=` (job) | n/a | n/a | Intended: `wait()` creates a preempted spot job again, up to this many times. The CLI has no such retry. |
| `timeout=` (job) | `--timeout` | `timeout` | Same range, 5 minutes to 168 hours. Intended: required in the SDK, an explicit cost ceiling; the CLI defaults to 24 hours. |
| n/a | `--public` | `public_ip` | Not exposed. The managed `https://` URL does not need it. |
| n/a | `--ssh-key` | `ssh_authorized_keys` | Not exposed. |
| n/a | `--registry-username`, `--registry-password` | `registry_credentials.username`, `.password` | Not exposed. Use `registry_secret`. |
| `dry_run()` | n/a | n/a | The SDK builds the request locally and does not call create. |
| `dry_run(server=True)` | `--dry-run` | `dry_run` | Same: Nebius validates the request and creates nothing. The CLI describes its flag as "Validate the request without creating a resource". |
| `create()` | `create` (`--async` returns early) | `Create` | The SDK returns once the id is known, not when the job or endpoint is ready. `ns.Job(spec).create()` and `ns.Endpoint(spec).create()` create once. |

## Commands

| Resource | In the SDK | CLI only |
| --- | --- | --- |
| job | `create`, `get`, `wait` (with spot resubmit), `cancel`, `to_cli` | `delete`, `list`, `get-by-name`, `restart`, `logs`, `ssh`, `run` (beta) |
| endpoint | `create`, `get`, `wait` (with `probe=`), `stop`, `start`, `delete`, `reveal_token`, `to_cli` | `restart`, `list`, `get-by-name`, `logs`, `ssh` |
