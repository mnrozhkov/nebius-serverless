# 🔌 Integrations

Run [tasks](../reference/tasks.md) from an orchestrator. Each integration is a subpackage of `nebius_serverless.integrations` with its own extra. It imports only the package root, `nebius_serverless.errors`, and `nebius_serverless.tasks.driver`, the interface for integrations. `import nebius_serverless` does not load any of them.

<div class="grid cards" markdown>

-   :material-sitemap-outline: **[Prefect](prefect.md)**

    ---

    `PrefectFlow`: a pipeline of tasks as a Prefect flow, on a Nebius job or in your process. `pip install "nebius-serverless[prefect]"`.

</div>
