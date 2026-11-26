"""What runs inside a task's job, and where its files are in the bucket.

A run's folder in the task bucket, as every job sees it under ``MOUNT``::

    <run key>/
        payload.json   a function's code pointer, arguments and libraries
        result.json    a function's outcome: {"ok": true, "value": ...}
                       or {"ok": false, "error": {...}}
        outputs.bin    a script's exit code and outputs, NUL-separated
        files/         the run's own files: $NS_OUTPUT_DIR in the job

The run key is ``tasks/<task>/<UTC time>`` for a task called on its own,
and ``<folder>/<job name>`` for a task started in a pipeline run's
folder. Library archives are kept in ``libs/<hash>.tar.gz``.

The job runs ``RUNNER`` for a function and ``bash_script`` for a
script. Both use only what the image has: python3, or bash 4.
"""

import json
import os
import shlex
import threading
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .errors import ValidationError

_OP = "Task"

# Where every task's job sees the bucket. It is the same in all jobs, so
# a path one task returns can be opened by another.
MOUNT = "/ns"


# Where the generated runner and its inputs are injected.
JOB_DIR = "/ns-task"


# Where include_libs are unpacked in the job.
LIBS_DIR = "/tmp/ns-libs"


# Set in a task's job to where the bucket is mounted there. A task
# started from inside that job reads and writes the bucket as files.
MOUNT_ENV = "NS_TASK_MOUNT"


# Set in a driver job to the folder of the pipeline's run in the bucket,
# <pipeline>/<run>. A task started there keeps its files under it.
FOLDER_ENV = "NS_TASK_FOLDER"

# Runs a function in the job. Standard library only, so the image needs
# nothing but python3.
RUNNER = '''\
"""Run one ns.Task function inside the job."""

import hashlib
import importlib
import io
import json
import os
import sys
import tarfile
import traceback


def load(payload, root):
    if payload["libs"]:
        folder = payload["libs_dir"]
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(root, payload["libs"]), "rb") as handle:
            data = handle.read()
        expected = os.path.basename(payload["libs"]).split(".")[0]
        if hashlib.sha256(data).hexdigest()[: len(expected)] != expected:
            raise RuntimeError("The library archive is not what was sent.")
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            try:
                archive.extractall(folder, filter="data")
            except TypeError:
                archive.extractall(folder)
        sys.path.insert(0, folder)
        previous = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = (
            folder + os.pathsep + previous if previous else folder
        )
    if payload["module"]:
        target = importlib.import_module(payload["module"])
        for part in payload["qualname"].split("."):
            target = getattr(target, part)
        return target
    scope = {"__name__": "__ns_task__"}
    exec(compile(payload["source"], "<ns.Task source>", "exec"), scope)
    return scope[payload["qualname"]]


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "pointer.json")) as handle:
        pointer = json.load(handle)
    root = pointer["root"]
    run = os.path.join(root, pointer["run"])
    files = os.path.join(run, "files")
    os.makedirs(files, exist_ok=True)
    os.environ["NS_OUTPUT_DIR"] = files
    os.environ["NS_TASK_MOUNT"] = root
    code = 0
    try:
        with open(os.path.join(run, "payload.json"), "rb") as handle:
            raw = handle.read()
        if hashlib.sha256(raw).hexdigest() != pointer["sha256"]:
            raise RuntimeError("payload.json is not what was submitted.")
        payload = json.loads(raw)
        function = load(payload, root)
        value = function(*payload["args"], **payload["kwargs"])
        body = json.dumps({"ok": True, "value": value})
    except BaseException as exc:
        code = 1
        traceback.print_exc()
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        body = json.dumps({"ok": False, "error": error})
    with open(os.path.join(run, "result.json"), "w") as handle:
        handle.write(body)
    sys.exit(code)


if __name__ == "__main__":
    main()
'''

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
        folder = os.environ.get(FOLDER_ENV, "")
    folder = folder.strip("/")
    if folder:
        return f"{folder}/{job_name}"
    return f"tasks/{task}/{run_name}"


def bash_script(
    command: str,
    inputs: Mapping[str, Any],
    key: str,
    libs_key: str | None,
) -> str:
    """The bash script the job runs: the arrays, a trap, then ``command``.

    The trap writes the exit code and ``outputs`` as NUL-separated text
    when the script ends, however it ends.
    """
    pairs = " ".join(
        f"[{name}]={shlex.quote(_shell_text(value))}"
        for name, value in inputs.items()
    )
    lines = [
        "#!/usr/bin/env bash",
        f"declare -A inputs=({pairs})",
        "declare -A outputs=()",
        f"__ns_run={shlex.quote(f'{MOUNT}/{key}')}",
        'export NS_OUTPUT_DIR="$__ns_run/files"',
        f"export {MOUNT_ENV}={shlex.quote(MOUNT)}",
        "set -e",
        'mkdir -p "$NS_OUTPUT_DIR"',
        "__ns_finish() {",
        "    local code=$? key",
        "    {",
        "        printf '%s\\0' \"$code\"",
        '        for key in "${!outputs[@]}"; do',
        '            printf \'%s\\0%s\\0\' "$key" "${outputs[$key]}"',
        "        done",
        '    } > "$__ns_run/outputs.bin"',
        '    exit "$code"',
        "}",
        "trap __ns_finish EXIT",
    ]
    if libs_key is not None:
        folder = shlex.quote(LIBS_DIR)
        lines += [
            f"mkdir -p {folder}",
            f"tar -xzf {shlex.quote(f'{MOUNT}/{libs_key}')} -C {folder}",
            f'export PYTHONPATH={folder}"${{PYTHONPATH:+:$PYTHONPATH}}"',
        ]
    return "\n".join([*lines, "", command, ""])


def _shell_text(value: object) -> str:
    """A string as it is; any other JSON value as JSON text."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"A command's inputs must be JSON values: {exc}.",
            operation=_OP,
        ) from None
