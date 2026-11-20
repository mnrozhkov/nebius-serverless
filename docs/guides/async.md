# Async and notebooks

Synchronous methods are the script API. Inside a running event loop, such as Jupyter or an async web service, they raise `ValidationError` and name the awaited twin to use instead: `create_async`, `get_async`, `wait_async`, `cancel_async`, `jobs.get_async`, and for endpoints `stop_async`, `start_async`, `delete_async`, `endpoints.get_async`.

=== "Jobs"

    ```python
    import asyncio

    import nebius_serverless as ns

    async def main() -> None:
        spec = ns.JobSpec(
            ["nvidia-smi"],
            image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
            gpu="h100",
            timeout="1h",
        )
        job = await spec.create_async()
        record = await job.wait_async()
        print(job.id, record["status"]["state"])

    asyncio.run(main())
    ```

=== "Endpoints"

    ```python
    endpoint = await spec.create_async()
    record = await endpoint.wait_async()
    await endpoint.delete_async()
    ```

In a notebook, `await` the twins directly in a cell. The [notebooks](../quickstarts/notebooks.md) do it step by step.

!!! warning "Cancelling an await does not cancel the cloud resource"
    Interrupting a cell or cancelling `wait_async` stops the local poll and its progress display. The job keeps running, and the endpoint keeps billing. Only `cancel` / `cancel_async` cancels a job, and only `delete` / `delete_async` deletes an endpoint.
