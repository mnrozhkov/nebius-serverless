# Async and notebooks

Synchronous methods work everywhere, including inside a running event loop such as a Jupyter cell. There, a synchronous call runs its work on a worker thread and blocks until it ends, as `time.sleep` would. Ctrl-C in the cell stops the local poll within one poll interval.

The awaited twins keep the event loop responsive while they wait: `create_async`, `get_async`, `wait_async`, `cancel_async`, `jobs.get_async`, and for endpoints `stop_async`, `start_async`, `delete_async`, `endpoints.get_async`. Use them in an async web service, or to wait on several jobs at once.

=== "Jobs"

    ```python
    import asyncio

    import nebius_serverless as ns

    async def main() -> None:
        spec = ns.JobSpec(
            command=["nvidia-smi"],
            image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
            gpu="h100",
            timeout="1h",
        )
        job = await ns.Job(spec).create_async()
        record = await job.wait_async()
        print(job.id, record["status"]["state"])

    asyncio.run(main())
    ```

=== "Endpoints"

    ```python
    endpoint = await ns.Endpoint(spec).create_async()
    record = await endpoint.wait_async()
    await endpoint.delete_async()
    ```

In a notebook, call the synchronous methods, or `await` the twins directly in a cell. The [notebooks](../quickstarts/notebooks.md) do it step by step.

!!! warning "Cancelling an await does not cancel the cloud resource"
    Interrupting a cell, a synchronous `wait()`, or cancelling `wait_async` stops the local poll and its progress display. The job keeps running, and the endpoint keeps billing. Only `cancel` / `cancel_async` cancels a job, and only `delete` / `delete_async` deletes an endpoint.
