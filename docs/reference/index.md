# 📚 Reference

The user-facing surface of `nebius_serverless`. Import it as `ns`.

```python
import nebius_serverless as ns
```

<div class="grid cards" markdown>

-   :material-briefcase-outline: **[Jobs](jobs.md)**

    ---

    `JobSpec`, the recipe; `Job`, one Nebius job after create; `ns.jobs`, fetch a job from a saved id.

-   :material-web: **[Endpoints](endpoints.md)**

    ---

    `EndpointSpec`, `Endpoint`, and `ns.endpoints`: long-running containers on a managed URL.

-   :material-docker: **[Images](images.md)**

    ---

    `Image`: build a Dockerfile on Nebius and push it to the project's registry.

-   :material-function-variant: **[Tasks](tasks.md)**

    ---

    `Task` and `TaskRun`: a Python function or a bash script as a job.

-   :material-sitemap-outline: **[Prefect flows](flows.md)**

    ---

    `PrefectFlow` and `FlowRun`: a pipeline of tasks as a Prefect flow.

-   :material-account-key-outline: **[Session and auth](session.md)**

    ---

    `whoami`, `configure`, `close`, `configure_logging`, and `diagnostics`.

-   :material-console: **[CLI and API map](cli-api-map.md)**

    ---

    Each recipe argument's `nebius ai` flag and API field.

-   :material-alert-circle-outline: **[Errors](errors.md)**

    ---

    The exceptions this package raises, and what to do about each.

</div>

## Everywhere

Constructing a `JobSpec`, `EndpointSpec`, `Image`, `Task`, or `PrefectFlow`, loading YAML, or importing the package does not authenticate, upload, or create. The Nebius SDK is imported on the call that resolves a profile or contacts Nebius.

Logs stay on the CLI: `nebius ai job logs <id>`. This package does not stream them.

Synchronous methods are the script API. Inside a running event loop they raise `ValidationError` and name the awaited twin (`create_async`, `get_async`, `wait_async`, `cancel_async`, `jobs.get_async`). Cancelling an await does not cancel the cloud job. Only `cancel` / `cancel_async` does.
