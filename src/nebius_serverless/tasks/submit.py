"""Start task runs: names, images, job options, and records.

These functions are for code that runs tasks on a user's behalf, such
as an integration: they start a run in a given folder, and turn a task
into a JSON record and back. ``tasks.driver`` is the supported
interface to them.
"""

import json
import os
import threading
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ..core.transport import Transport
from ..errors import ValidationError
from ..images.image import Image
from ..jobs.spec import JobSpec
from ..ui.output import Progress, report
from ..ui.status import Status
from . import runtime
from .run import TaskRun
from .storage import Bucket, SdkTaskBackend

if TYPE_CHECKING:
    from .task import Task

_OP = "Task"


DEFAULT_TIMEOUT = "24h"


_lock = threading.Lock()
_stamps: dict[str, tuple[str, int]] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def run_name(task: str) -> str:
    """The UTC time to the second, with a counter for a repeat."""
    stamp = _now().strftime("%Y%m%d-%H%M%S")
    with _lock:
        last, count = _stamps.get(task, ("", 0))
        count = count + 1 if last == stamp else 1
        _stamps[task] = (stamp, count)
    return stamp if count == 1 else f"{stamp}-{count}"


def run_key(
    task: str, run_name: str, job_name: str, folder: str | None = None
) -> str:
    """The folder of one run in the bucket.

    In a pipeline: ``flows/<pipeline>/<run>/<task>``, where ``<task>``
    is the name of the task's job, such as
    ``train-baseline-20261125-121050``.
    The pipeline passes its run's folder, or its driver job sets it in
    ``NS_TASK_FOLDER``. A task run on its own has no pipeline:
    ``tasks/<task name>/<UTC time>``.
    """
    if folder is None:
        folder = os.environ.get(runtime.FOLDER_ENV, "")
    folder = folder.strip("/")
    if folder:
        return f"{folder}/{job_name}"
    return f"tasks/{task}/{run_name}"


def job_transport() -> Transport | None:
    """The transport for job calls. ``None`` is the SDK. Tests replace it."""
    return None


def say(progress: Progress, message: str) -> None:
    report(Status(kind="task", message=message), progress)


def plain_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Job options as JSON values, for a record."""
    plain = dict(options)
    timeout = plain.get("timeout")
    if isinstance(timeout, timedelta):
        plain["timeout"] = timeout.total_seconds()
    try:
        json.dumps(plain, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"The job options cannot be sent with a flow: {exc}.",
            operation=_OP,
        ) from None
    return plain


def validate_options(options: dict[str, Any]) -> dict[str, Any]:
    """Check the job options now, so a mistake does not wait for a run."""
    if "inject_files" in options:
        raise ValidationError(
            "inject_files is not available on a task. Use local_libs "
            "for code, and volumes for data.",
            operation=_OP,
        )
    checked = {"timeout": DEFAULT_TIMEOUT, **options}
    try:
        JobSpec(command=["true"], image="python:3", **checked)
    except TypeError as exc:
        raise ValidationError(
            f"Unknown job option: {exc}.", operation=_OP
        ) from None
    return checked


def build_image(image: Image | str, progress: Progress) -> str:
    """The reference of ``image``: built first when it is an ``Image``."""
    if isinstance(image, str):
        return image
    ref = image._build(
        progress,
        dry_run=False,
        timeout="1h",
        disk=None,
        transport=job_transport(),
    )
    assert isinstance(ref, str)
    return ref


def start_run(
    task: "Task",
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    *,
    folder: str | None = None,
) -> TaskRun:
    """``task.submit(*args, **kwargs)``, with the run kept in ``folder``.

    ``folder`` is a pipeline run's folder in the bucket. The run's own
    folder is ``<folder>/<job name>``. Without it, ``NS_TASK_FOLDER``
    is used when set, and ``tasks/<task>/<UTC time>`` otherwise.
    """
    return task._start(tuple(args), dict(kwargs or {}), folder=folder)


def submit_run(
    task: "Task",
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
    progress: Progress,
    *,
    run_name: str | None = None,
    job_name: str | None = None,
    key: str | None = None,
) -> TaskRun:
    """Start one run with its names given: its time stamp, its job's
    name, and its folder in the bucket. The arguments must not hold
    other runs."""
    return task._submit(
        tuple(args),
        dict(kwargs),
        progress,
        run_name=run_name,
        job_name=job_name,
        key=key,
    )


def prepare_task(
    task: "Task",
    progress: Progress,
    storage: SdkTaskBackend,
    bucket: Bucket,
) -> None:
    """Build the task's image and upload its libraries, when needed."""
    task._prepare(progress, storage, bucket)


def task_record(
    task: "Task",
    progress: Progress,
    storage: SdkTaskBackend,
    bucket: Bucket,
) -> dict[str, Any]:
    """Prepare the task and describe it as JSON.

    The record holds the built image's reference, the key of the
    uploaded libraries, the function's source or import name, and the
    job options: enough to start the task's job where its code and
    Dockerfile are not present. ``task_from_record`` reverses it.
    """
    return task._record(progress, storage, bucket)


def task_from_record(record: Mapping[str, Any]) -> "Task":
    """A task made from a record. It needs no local code to submit."""
    from .task import Task

    return Task._from_record(record)
