"""What a Prefect task run shows about its Nebius job.

Each task run gets three Markdown artifacts: ``nebius-job`` (the job
id, its name, the command for its logs, where its files are) and
``inputs`` when the job has started, and ``outputs`` when it has ended.
The same goes to the task run's log. With ``NS_CONSOLE_JOB_URL`` set to
the console address of a job, with ``{project}`` and ``{job}`` in it,
``nebius-job`` links to the job.
"""

import json

from ...task import TaskRun, console_url

# The names of the artifacts a Prefect task run gets.
JOB_ARTIFACT = "nebius-job"


INPUTS_ARTIFACT = "inputs"


OUTPUTS_ARTIFACT = "outputs"


# How much of one input or output value is shown in Prefect.
_SHOWN_MAX = 500


def _shown(value: object) -> str:
    """One value as inline code for Markdown, cut when it is long.

    A string is shown as it is; any other value as JSON.
    """
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, default=str)
        except (TypeError, ValueError):
            text = repr(value)
    text = " ".join(text.split())
    if len(text) > _SHOWN_MAX:
        text = text[:_SHOWN_MAX] + " ... (cut)"
    if text == "":
        return "(empty)"
    fence = "``" if "`" in text else "`"
    pad = " " if "`" in text else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def values_markdown(title: str, value: object) -> str:
    """Inputs or outputs as a Markdown list, one line per name.

    A dict gives one line per key. Any other value gives one line.
    """
    if isinstance(value, dict):
        lines = [f"- {key}: {_shown(item)}" for key, item in value.items()]
        if not lines:
            lines = ["- (none)"]
    else:
        lines = [f"- Value: {_shown(value)}"]
    return "\n".join([f"### {title}", "", *lines])


def job_markdown(run: TaskRun) -> str:
    """A run's Nebius job, for the Prefect artifact ``nebius-job``."""
    job = run.job
    url = console_url(run.project, job.id)
    opened = f" ([open in the console]({url}))" if url else ""
    return "\n".join(
        [
            f"### Nebius job of `{run.task}`",
            "",
            f"- Job: `{job.id}`{opened}",
            f"- Name: `{run.task}-{run.name}`",
            f"- Logs: `nebius ai job logs {job.id}`",
            f"- Files: `{run.output_uri}`",
        ]
    )


def show_started(run: TaskRun) -> None:
    """Show a run's job and inputs in the Prefect task run.

    Writes the Markdown artifacts ``nebius-job`` and ``inputs``, and
    the same to the log. Does nothing outside a Prefect run, and never
    fails the task.
    """
    try:
        from prefect import (  # type: ignore[import-not-found]
            get_run_logger,
        )
        from prefect.artifacts import (  # type: ignore[import-not-found]
            create_markdown_artifact,
        )

        logger = get_run_logger()
        url = console_url(run.project, run.job.id)
        where = f" {url}" if url else ""
        logger.info("Nebius job %s%s", run.job.id, where)
        logger.info("Inputs: %s", json.dumps(run.inputs, default=str))
        create_markdown_artifact(
            markdown=job_markdown(run),
            key=JOB_ARTIFACT,
            description=f"Nebius job {run.job.id} of {run.task}",
        )
        create_markdown_artifact(
            markdown=values_markdown(f"Inputs of `{run.task}`", run.inputs),
            key=INPUTS_ARTIFACT,
            description=f"Inputs of {run.task}",
        )
    except Exception:
        pass


def show_finished(run: TaskRun, value: object) -> None:
    """Show a run's outputs in the Prefect task run. Never fails.

    Writes the Markdown artifact ``outputs``, and the same to the log.
    """
    try:
        from prefect import (  # type: ignore[import-not-found]
            get_run_logger,
        )
        from prefect.artifacts import (  # type: ignore[import-not-found]
            create_markdown_artifact,
        )

        get_run_logger().info("Outputs: %s", json.dumps(value, default=str))
        create_markdown_artifact(
            markdown=values_markdown(f"Outputs of `{run.task}`", value),
            key=OUTPUTS_ARTIFACT,
            description=f"Outputs of {run.task}",
        )
    except Exception:
        pass
