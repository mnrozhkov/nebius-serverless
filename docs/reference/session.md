# Session and auth

Calls that apply to the whole process: who it is signed in as, how it authenticates, the shared Nebius SDK, and diagnostic logs. Import the package as `ns`.

## `whoami`

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

## `configure`

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

To authenticate a CI run, see [Run in CI](../guides/ci.md).

## `close`

```python
ns.close() -> None
```

All calls in one process share one Nebius SDK per profile: it opens on the first call that needs it and closes at interpreter exit. A forked child opens its own, and a project's region is looked up once per process. `close()` releases the SDK early, for example in a long-running service; the next call opens a new one.

## `configure_logging`

```python
ns.configure_logging(*, level="INFO") -> None
```

`level` is a logging name such as `"DEBUG"` or `"INFO"`, or a numeric level.

`configure_logging(level="INFO")` sets the `nebius_serverless` logger level. When the application has not already attached a handler, it adds one stderr `StreamHandler` and turns propagation off. Call it again to change the level; the helper keeps a single handler of its own. An application handler is left in place, including its level, filters, and the current propagation setting. Applications that already configure logging should set the `nebius_serverless` logger themselves.

Debug lines can append `event`, `operation_id`, `request_id`, `job_id`, `attempt`, `duration_ms`, and `status_code` when those values exist. `operation_id` identifies the local call. It is not a server request id. Records do not include environment values, credentials, headers, or request bodies.

```python
ns.configure_logging(level="DEBUG")
job.wait(quiet=True)  # no progress; DEBUG records still emit
```

## `diagnostics`

```python
ns.diagnostics() -> dict
```

`diagnostics()` returns a JSON-serializable dict and prints nothing:

| Key | Meaning |
| --- | --- |
| `version` | Package version. |
| `python` | Python version. |
| `platform` | OS platform. |
| `presentation` | `plain` or `interactive` for the current stderr. |

It does not report an ambient quiet switch. A call can still pass `quiet=True`. No profile, credentials, environment values, or job records are included.

It does not contact Nebius.
