"""Tasks: ``ns.Task`` runs a Python function or a bash script as a job.

``ns.tasks.get(id)`` reconnects to a run from its run id, or from the
job id of any of its attempts. The modules in this package are
implementation.
"""

import base64
import json
import re
import shlex
from typing import TYPE_CHECKING, Any

from ..core.profile import resolve_location
from ..core.transport import Transport
from ..errors import ValidationError
from ..ui.output import Progress, call_progress
from . import record as run_record
from . import runtime, storage, submit
from .attempts import RUN_ENV, Attempts

if TYPE_CHECKING:
    from .run import TaskRun

__all__ = ["get"]

_LOG = "nebius_serverless.task"
_OP = "tasks.get"
JOB_PREFIX = "aijob-"
_SCRIPT_RUN = re.compile(r"^__ns_run=(.+)$", re.MULTILINE)


def get(
    run_id: str,
    *,
    bucket: str | None = None,
    profile: str | None = None,
    project: str | None = None,
) -> "TaskRun":
    """Reconnect to a task run. Does not start or resubmit a job.

    ``run_id`` is the run's id (``run.id``), such as
    ``tasks/train/20261127-152011``, or the job id (``aijob-...``) of
    any of its attempts. A run id needs no job read. The returned run
    follows the latest attempt; its ``result()`` waits for it and
    returns the task's result. When the latest attempt was preempted
    and the run's budget allows, ``result()`` starts the next attempt.

    ``bucket`` is the bucket the run is in when you chose one; it
    defaults as for ``Task(bucket=)``. A job id finds its bucket itself.
    """

    def work(progress: Progress) -> "TaskRun":
        return _get(run_id, bucket=bucket, profile=profile, project=project)

    return call_progress(quiet=False, logger_name=_LOG, work=work)


def _get(
    given: str,
    *,
    bucket: str | None,
    profile: str | None,
    project: str | None,
) -> "TaskRun":
    if not isinstance(given, str) or not given.strip("/ "):
        raise ValidationError(
            "Pass a run id, such as tasks/train/20261127-152011, or the "
            "job id of one of its attempts.",
            operation=_OP,
        )
    transport = submit.job_transport() or _sdk_transport(profile, project)
    chosen = storage.chosen_bucket(bucket)
    root: str | None = None
    job_id: str | None = None
    if given.startswith(JOB_PREFIX):
        job_id = given
        found = _run_of_job(transport.get(given), given, bucket)
        run_id, chosen, root, job_project = found
        project = project or job_project
    else:
        run_id = given.strip("/")
    project_id, _region = resolve_location(
        profile=profile, project=project, region=None
    )
    backend = storage.backend(profile)
    where, _created = backend.ensure_bucket(
        project_id, chosen, root=root, find_only=True
    )
    run = run_record.read(backend, where, run_id, job_id=job_id)
    return reattached(run, backend, where, transport)


def reattached(
    run: dict[str, Any],
    backend: storage.SdkTaskBackend,
    bucket: storage.Bucket,
    transport: Transport,
) -> "TaskRun":
    """A run built from its record, following its latest attempt."""
    from .run import TaskRun
    from .task import Task

    if not run.get("attempts"):
        raise ValidationError(
            f"Task run {run['id']} has no job yet: its process stopped "
            "before the first job was created.",
            operation=_OP,
            recovery="Start the task again.",
        )
    task = Task._from_record(run["definition"])
    payload = backend.get(bucket, f"{run['id']}/payload.json")
    attempts = Attempts(
        task=task,
        run=run,
        backend=backend,
        bucket=bucket,
        payload=payload,
    )
    inputs = {} if payload is None else task._inputs_of(payload)
    return TaskRun(
        task=str(run["task"]),
        name=str(run["name"]),
        job=attempts.attach(transport),
        inputs=inputs,
        bucket=bucket,
        key=str(run["id"]),
        scripted=run["result"]["format"] == "script",
        backend=backend,
        project=str(run["project"]),
        bucket_choice=bucket.name if bucket.chosen else None,
        attempts=attempts,
    )


def _run_of_job(
    job: dict[str, object], job_id: str, bucket: str | None
) -> tuple[str, str | None, str | None, str | None]:
    """The run id, bucket choice, root and project a task job names.

    The job's ``NS_TASK_RUN`` variable gives the run id. Without it, the
    run folder is read from the injected ``pointer.json`` or ``run.sh``.
    An explicit ``bucket`` wins over the bucket the job names.
    """
    spec = job.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    env = {
        str(item.get("name")): item.get("value")
        for item in spec.get("environment_variables") or ()
        if isinstance(item, dict)
    }
    metadata = job.get("metadata")
    project = (
        metadata.get("parent_id") if isinstance(metadata, dict) else None
    )
    chosen = bucket or _text(env.get(storage.BUCKET_ENV))
    root = _text(env.get(storage.ROOT_ENV))
    if root is None and env.get(storage.ROOT_ENV) == "":
        root = ""
    run_id = _text(env.get(RUN_ENV))
    if run_id is None:
        folder = _injected_folder(spec)
        if folder is not None:
            prefix = (root if root is not None else storage.chosen_root())
            if chosen and prefix and folder.startswith(f"{prefix}/"):
                folder = folder.removeprefix(f"{prefix}/")
            run_id = folder
    if run_id is None:
        raise ValidationError(
            f"Job {job_id} is not a task job: it has no {RUN_ENV} "
            "variable and no task runner files.",
            operation=_OP,
            resource_id=job_id,
            recovery=(
                "Reload a plain job with ns.jobs.get(id). For a task run, "
                "pass its run id."
            ),
        )
    return run_id, chosen, root, _text(project)


def _injected_folder(spec: dict[str, Any]) -> str | None:
    """The run folder from ``pointer.json`` or ``run.sh``, if sent."""
    for item in spec.get("injected_files") or ():
        if not isinstance(item, dict):
            continue
        path = str(item.get("container_path") or "")
        text = _content(item.get("content"))
        if text is None:
            continue
        if path.endswith("/pointer.json"):
            try:
                folder = json.loads(text).get("run")
            except (ValueError, AttributeError):
                continue
            if isinstance(folder, str):
                return folder.strip("/")
        if path.endswith("/run.sh"):
            match = _SCRIPT_RUN.search(text)
            if match:
                (value,) = shlex.split(match.group(1))
                return value.removeprefix(runtime.MOUNT).strip("/")
    return None


def _content(value: object) -> str | None:
    """Injected file content: bytes, or base64 text from the API."""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True).decode()
        except (ValueError, UnicodeDecodeError):
            return value
    return None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _sdk_transport(profile: str | None, project: str | None) -> Transport:
    from ..core.sdk_transport import SdkTransport

    return SdkTransport(profile=profile, project=project)
