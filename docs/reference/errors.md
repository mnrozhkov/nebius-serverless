# Errors

Every error is an `Error`. `UnsupportedFeature` is a `ValidationError`. Exception text includes the operation and the job id when those are known. It does not include env values, tokens, or registry passwords.

| Attribute | Meaning |
| --- | --- |
| `operation` | Call that failed, such as `"create"` or `"wait"`. |
| `resource_id` | The job or endpoint id, when it is known. |
| `project` | Project id when it was recorded. |
| `key` | Resource key when it was recorded. |
| `recovery` | Short hint. The default is to reload with `jobs.get` and not to send a second create. |
| `diff` | Optional tuple of extra detail. |
| `code` | Stable SDK category: `validation`, `unsupported`, `access`, `capacity`, `transport`, `submission_uncertain`, `key_conflict`, `wait_timeout`, `build_failed`, or `task_failed`. |
| `request_id` | Server request id when the Nebius exception already exposed one. Otherwise `None`. |
| `trace_id` | Server trace id, when Nebius sent one. `str(exc)` ends with `(request id …, trace id …)`: quote that line to Nebius support. |
| `status_code` | The gRPC status code name, such as `"RESOURCE_EXHAUSTED"`, when the error came from a Nebius call. Branch on it instead of parsing text. |
| `state` | `BuildFailed` and `TaskFailed` only: the job's final state. |
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
| `BuildFailed` | An [image build](images.md#imagebuild) job did not end `COMPLETED`. `resource_id` is the build job; `nebius ai job logs <id>` shows why. |
| `TaskFailed` | A [task](tasks.md)'s function raised, its script exited non-zero, its job ended without a result, or it was not started because an input failed. `resource_id` is the job id. `remote_traceback` holds a function's traceback from the job; `exit_code` and `outputs` a script's exit code and the outputs it had set. |

Closing the process, a local `WaitTimeout`, and cancelling an awaited twin do not cancel the cloud job.

A job or endpoint that ends in `ERROR` or `FAILED` is returned as data. Its reason, `status.state_details.code` and `message` plus any quota violations, is shown in the final progress line of `wait()` and in `display()`. Errors never chain the Nebius SDK exception (`raise … from None`), because a server message can echo request fields; the attributes above carry what support needs.

## Endpoints

The same types as above. The endpoint id is stored on `resource_id` when it is known, so existing handlers see a resource id. Get and delete failures include that id in the text. `operation` is `EndpointSpec` for constructor errors and the method name otherwise. `recovery` names `endpoints.get` and `delete` instead of `jobs.get` and `cancel`. Text does not include env values or tokens.

| Type | When |
| --- | --- |
| `ValidationError` | Bad command, image, port, auth, machine selector, YAML, a create Nebius rejected (with its code and reason), missing project or region, or a sync call inside a running event loop. |
| `UnsupportedFeature` | Spot on CPU, and the shapes under [Out of scope](endpoints.md#out-of-scope). |
| `AccessError` | Authentication or permission failure from the SDK. |
| `CapacityError` | The API rejected the endpoint for quota or capacity. |
| `TransportError` | The call failed and there is no reason to think create was accepted. |
| `SubmissionUncertain` | Create may have been accepted and the id is unknown, after 3 tries with one idempotency key. `idempotency_key` holds it; a create with that key and the same recipe returns the endpoint Nebius made, if any. |
| `KeyConflict` | The API reports an existing resource and its id is known. |
| `WaitTimeout` | The local wait ended before a final state. The endpoint is unchanged and may still be billing; `wait()` again or `delete()`. |

Closing the process, a local `WaitTimeout`, and cancelling an awaited twin do not delete the endpoint.
