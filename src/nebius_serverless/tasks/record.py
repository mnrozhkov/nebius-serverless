"""The run record: ``<run id>/run.json`` in the task bucket.

One task call is one run, with a stable run id: its folder in the
bucket, relative to the library's root, such as
``tasks/train/20261127-152011``. A run has one or more attempts, one
Nebius job each: the first, then one per preemption.

The record holds what is needed to find the run's result and to start
another attempt without the caller's code: the task's definition (the
image, the libraries' archive, the function's import name or source,
and the job options) and the attempts. The arguments stay in
``payload.json``. The process that creates a job appends its attempt;
one writer per run is assumed.
"""

import json
from datetime import datetime, timezone
from typing import Any

from ..errors import Error
from .storage import Bucket, SdkTaskBackend

RUN_FILE = "run.json"
SCHEMA_VERSION = 1


def new(
    *,
    run_id: str,
    task: str,
    name: str,
    project: str,
    bucket: Bucket,
    scripted: bool,
    definition: dict[str, Any],
    preemption_retries: int,
) -> dict[str, Any]:
    """A record with no attempts yet."""
    return {
        "schema_version": SCHEMA_VERSION,
        "id": run_id,
        "task": task,
        "name": name,
        "project": project,
        "bucket": bucket.name,
        "root": bucket.root,
        "created_at": _now(),
        "result": {
            "item": "outputs.bin" if scripted else "result.json",
            "format": "script" if scripted else "function",
        },
        "definition": definition,
        "attempts": [],
        "preemption_retries": preemption_retries,
    }


def add_attempt(
    record: dict[str, Any], *, job_id: str, name: str, reason: str
) -> int:
    """Append one attempt and return its number, from 1."""
    attempts = record["attempts"]
    number = len(attempts) + 1
    attempts.append(
        {
            "n": number,
            "job_id": job_id,
            "name": name,
            "created_at": _now(),
            "reason": reason,
        }
    )
    return number


def write(
    backend: SdkTaskBackend, bucket: Bucket, record: dict[str, Any]
) -> None:
    """Write the record to ``<run id>/run.json``."""
    data = json.dumps(record, indent=1, sort_keys=True).encode()
    backend.put(bucket, f"{record['id']}/{RUN_FILE}", data)


def read(
    backend: SdkTaskBackend,
    bucket: Bucket,
    run_id: str,
    *,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Read a run's record. Raises ``Error`` when there is none."""
    data = backend.get(bucket, f"{run_id}/{RUN_FILE}")
    where = f"s3://{bucket.name}/{bucket.key(run_id)}/"
    if data is None:
        hint = (
            f" Its job may still be on Nebius: nebius ai job get {job_id}"
            if job_id
            else ""
        )
        raise Error(
            f"No task run {run_id} was found at {where}.",
            operation="tasks.get",
            resource_id=job_id,
            recovery=(
                "Check the run id and the bucket: bucket=, "
                "ns.configure(bucket=...) or NS_BUCKET. Runs created "
                "before run records existed, or whose folder was "
                f"deleted, cannot be reattached.{hint}"
            ),
        )
    try:
        record = json.loads(data)
        version = record["schema_version"]
    except (ValueError, KeyError, TypeError):
        raise Error(
            f"The record of task run {run_id} at {where} is not readable.",
            operation="tasks.get",
            recovery="Check that the folder holds a task run.",
        ) from None
    if not isinstance(version, int) or version > SCHEMA_VERSION:
        raise Error(
            f"The record of task run {run_id} has schema version "
            f"{version}; this package reads version {SCHEMA_VERSION}.",
            operation="tasks.get",
            recovery="Upgrade nebius-serverless to read it.",
        )
    if not isinstance(record, dict):
        raise Error(
            f"The record of task run {run_id} at {where} is not readable.",
            operation="tasks.get",
        )
    return record


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
