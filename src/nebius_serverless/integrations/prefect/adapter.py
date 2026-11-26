"""Tasks as Prefect tasks, and a pipeline as a Prefect flow.

Used by ``PrefectFlow``: in this process with ``remote_job=False``, and
in the flow job, where ``run_in_job`` is the entrypoint.
"""

from collections.abc import Mapping
from typing import Any

from ...errors import ValidationError
from ...task import Task, start_run
from ...task_driver import bind, job_tasks
from .artifacts import show_finished, show_started

_OP = "PrefectFlow"
INSTALL = 'Install it: pip install "nebius-serverless[prefect]"'


def prefect_task(
    task: Task, *, folder: str | None = None, **options: Any
) -> Any:
    """Return ``task`` as a Prefect task.

    Calling the Prefect task in a flow runs the task's job and returns
    its result. A failure raises ``TaskFailed``, so Prefect marks the
    task run failed and its retries apply. Each retry is a new run with
    a new job. ``submit`` and ``map`` on the returned object are
    Prefect's own and run the jobs at the same time.

    Each task run gets the artifacts in ``artifacts``. ``folder`` is the
    flow run's folder in the bucket, ``flows/<flow>/<run>``. Each job's
    files then go under ``<folder>/<job name>``. ``options`` go to
    ``prefect.task``, for example ``retries=2``. The Prefect task is
    named after the task unless ``name`` is given.
    """
    try:
        from prefect import task as make_task  # type: ignore[import-not-found]
    except ImportError:
        raise ValidationError(
            "A Prefect task needs the prefect package.",
            operation=_OP,
            recovery=INSTALL,
        ) from None

    def run(*args: Any, **kwargs: Any) -> Any:
        started = start_run(task, args, kwargs, folder=folder)
        show_started(started)
        try:
            value = started.result()
        except KeyboardInterrupt:
            started.cancel()
            raise
        show_finished(started, value)
        return value

    run.__name__ = run.__qualname__ = task.name
    run.__doc__ = task.__doc__
    setattr(run, "__signature__", task.__signature__)
    options.setdefault("name", task.name)
    return make_task(**options)(run)


def run_flow(program: Mapping[str, Any], tasks: Mapping[str, Task]) -> Any:
    """Run a pipeline as a Prefect flow, here or in the flow job.

    The tasks become Prefect tasks that keep their files in the run's
    folder. When the flow run was created at submit, this run becomes
    it.
    """
    from prefect import flow  # type: ignore[import-not-found]

    folder = program.get("folder")
    function = bind(
        program,
        {
            alias: prefect_task(item, folder=folder)
            for alias, item in tasks.items()
        },
    )
    run_title = str(program["run_title"])
    # A function, so that braces in the name are not read as a template.
    # Prefect renames a run it is given after it, so it is the same.
    wrapped = flow(
        name=program["title"], flow_run_name=lambda: run_title, log_prints=True
    )(function)
    run_id = program.get("flow_run_id")
    if not run_id:
        return wrapped(*program["args"], **program["kwargs"])

    from uuid import UUID

    from prefect.client.orchestration import (  # type: ignore[import-not-found]
        get_client,
    )
    from prefect.flow_engine import (  # type: ignore[import-not-found]
        run_flow as run_prefect_flow,
    )

    with get_client(sync_client=True) as client:
        flow_run = client.read_flow_run(UUID(str(run_id)))
    return run_prefect_flow(
        wrapped, flow_run=flow_run, parameters=program["parameters"]
    )


def run_in_job(program: dict[str, Any]) -> Any:
    """The flow job's entrypoint: the tasks come back from their records."""
    return run_flow(program, job_tasks(program))
