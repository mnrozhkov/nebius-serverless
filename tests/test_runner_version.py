"""The task runner names both Python versions when they differ."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

from nebius_serverless.tasks import runtime

HERE = "%d.%d" % sys.version_info[:2]


def _run(tmp_path: Path, source: str, python: str | None) -> tuple[dict, str]:
    """Run the runner in a bare interpreter; the result and stderr."""
    runner = tmp_path / "runner.py"
    runner.write_text(runtime.RUNNER)
    run = tmp_path / "bucket" / "tasks" / "f" / "1"
    run.mkdir(parents=True)
    body: dict[str, object] = {
        "libs": None,
        "libs_dir": str(tmp_path / "libs"),
        "module": None,
        "qualname": "f",
        "source": source,
        "args": [2],
        "kwargs": {},
    }
    if python is not None:
        body["python"] = python
    payload = json.dumps(body).encode()
    (run / "payload.json").write_bytes(payload)
    pointer = {
        "root": str(tmp_path / "bucket"),
        "run": "tasks/f/1",
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (tmp_path / "pointer.json").write_text(json.dumps(pointer))
    done = subprocess.run(
        [sys.executable, "-I", "-S", str(runner)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    return json.loads((run / "result.json").read_text()), done.stderr


def test_the_same_version_prints_nothing(tmp_path: Path) -> None:
    result, err = _run(tmp_path, "def f(x):\n    return x + 1\n", HERE)
    assert result["ok"] is True and result["value"] == 3
    assert "Python" not in err


def test_another_version_warns_in_the_job_log(tmp_path: Path) -> None:
    result, err = _run(tmp_path, "def f(x):\n    return x + 1\n", "3.9")
    assert result["value"] == 3
    assert f"sent from Python 3.9, and this job runs Python {HERE}" in err


def test_newer_syntax_fails_naming_both_versions(tmp_path: Path) -> None:
    # Syntax this interpreter cannot compile, sent from a "newer" one.
    source = "def f(x):\n    return x ?? 1\n"
    result, _err = _run(tmp_path, source, "3.99")
    assert result["ok"] is False
    error = result["error"]
    assert error["type"] == "SyntaxError"
    assert "written on Python 3.99" in error["message"]
    assert f"runs Python {HERE}" in error["message"]
    assert "Python 3.99" in error["traceback"]
