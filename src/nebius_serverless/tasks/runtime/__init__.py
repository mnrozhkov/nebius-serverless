"""What runs inside a task's job, and where its files are in the bucket.

A run's folder in the task bucket, as every job sees it under ``MOUNT``::

    <run key>/
        payload.json   a function's code pointer, arguments and libraries
        result.json    a function's outcome: {"ok": true, "value": ...}
                       or {"ok": false, "error": {...}}
        outputs.bin    a script's exit code and outputs, NUL-separated
        files/         the run's own files: $NS_OUTPUT_DIR in the job

The run key (``submit.run_key``) is ``tasks/<task>/<UTC time>`` for a
task called on its own, and ``<folder>/<job name>`` for a task started
in a pipeline run's folder. Library archives are kept in
``libs/<hash>.tar.gz``.

The job runs ``RUNNER`` (the text of ``runner.py``) for a function and
``bash_script`` for a script. Both use only what the image has: python3,
or bash 4. ``runner.py`` must import nothing outside the standard
library; ``tests/test_boundaries.py`` checks it.
"""

import json
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ...errors import ValidationError

_OP = "Task"

# Where every task's job sees the bucket. It is the same in all jobs, so
# a path one task returns can be opened by another.
MOUNT = "/ns"


# Where the generated runner and its inputs are injected.
JOB_DIR = "/ns-task"


# Where local_libs are unpacked in the job.
LIBS_DIR = "/tmp/ns-libs"


# Set in a task's job to where the bucket is mounted there. A task
# started from inside that job reads and writes the bucket as files.
MOUNT_ENV = "NS_TASK_MOUNT"


# Set in a driver job to the folder of the pipeline's run in the bucket,
# <pipeline>/<run>. A task started there keeps its files under it.
FOLDER_ENV = "NS_TASK_FOLDER"

# Runs a function in the job: ``runner.py`` next to this file, sent as
# text. Standard library only, so the image needs nothing but python3.
RUNNER = (Path(__file__).parent / "runner.py").read_text()

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
