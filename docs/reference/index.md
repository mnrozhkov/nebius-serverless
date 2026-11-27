# 📚 Reference

The user-facing surface of `nebius_serverless`. Import it as `ns`.

```python
import nebius_serverless as ns
```

<div class="grid cards" markdown>

-   :material-briefcase-outline: **[Jobs](jobs.md)**

    ---

    `JobSpec`, what to run, as data; `Job`, one Nebius job, created once with `create()`; `ns.jobs`, fetch a job from a saved id.

-   :material-web: **[Endpoints](endpoints.md)**

    ---

    `EndpointSpec`, `Endpoint`, and `ns.endpoints`: long-running containers on a managed URL. Created once with `ns.Endpoint(spec).create()`.

-   :material-docker: **[Images](images.md)**

    ---

    `Image`: build a Dockerfile on Nebius and push it to the project's registry.

-   :material-function-variant: **[Tasks](tasks.md)**

    ---

    `Task` and `TaskRun`: a Python function or a bash script as a job; `ns.tasks`, reconnect to a run from its id.

-   :material-account-key-outline: **[Session and auth](session.md)**

    ---

    `whoami`, `configure`, `close`, `configure_logging`, and `diagnostics`.

-   :material-console: **[CLI and API map](cli-api-map.md)**

    ---

    Each spec field's `nebius ai` flag and API field.

-   :material-alert-circle-outline: **[Errors](errors.md)**

    ---

    The exceptions this package raises, and what to do about each.

</div>

## Everywhere

Constructing a `JobSpec`, `EndpointSpec`, `Job`, `Endpoint`, `Image`, or `Task`, loading YAML, or importing the package does not authenticate, upload, or create. Only `create()` creates a job or an endpoint. The Nebius SDK is imported on the call that resolves a profile or contacts Nebius.

Logs stay on the CLI: `nebius ai job logs <id>`. This package does not stream them.

Synchronous methods also work inside a running event loop, such as a Jupyter cell: there they run on a worker thread and block until they end. The awaited twins (`create_async`, `get_async`, `wait_async`, `cancel_async`, `jobs.get_async`) keep the loop responsive. Cancelling an await does not cancel the cloud job. Only `cancel` / `cancel_async` does.

## Supported imports

These imports are supported, and changes to them are called out in the release notes:

| Import | For |
| --- | --- |
| `import nebius_serverless as ns`, and every name in `ns.__all__` | Everyone. `ns.JobSpec`, `ns.Task`, `ns.jobs.get(id)`, `ns.tasks.get(id)`, and the rest on these pages. |
| `nebius_serverless.errors` | The same exceptions as `ns.Error` and its subclasses. |
| `nebius_serverless.integrations.<name>` | An integration, such as `nebius_serverless.integrations.prefect`. |
| `nebius_serverless.tasks.driver` | Writing an integration: the names in its `__all__`. |

The package is organized by feature: `jobs/`, `endpoints/`, `images/`, `tasks/`, shared code in `core/`, and progress output in `ui/`. Those modules are importable, but they are implementation: a function there is not supported because it can be imported, and it may move or change in any release. `tests/test_boundaries.py` checks which package may import which, and `tests/test_public_api.py` checks that the names above do not change by accident.

