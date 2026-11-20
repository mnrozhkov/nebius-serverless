"""No tracked file publishes a real Nebius resource id or tunnel host.

Real ids are a prefix, ``-e00`` and 15 to 19 more characters, e.g. a
project, subnet, job, endpoint, secret or compute instance. Placeholders
such as ``project-e00…``, ``mbsec-...`` and ``aijob-test-0001`` do not
match. Failures name the file, line and id prefix, never the id.
"""

import random
import re
import shutil
import string
import subprocess
from collections.abc import Iterable
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCANNED = ("src", "tests", "examples", "docs", "README.md")
TEXT_SUFFIXES = frozenset(
    {".py", ".md", ".ipynb", ".yaml", ".yml", ".toml", ".txt", ".json"}
)
RESOURCE_ID = re.compile(r"\b([a-z]+)-e00[a-z0-9]{12,}\b")
TUNNEL_HOST = re.compile(r"\bport\d+-[a-z0-9]{8,}\.tunnel\.applications\.")


def find_ids(paths: Iterable[Path], root: Path) -> list[str]:
    """Return ``file:line prefix`` for each match, without the id."""
    found: list[str] = []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        where = path.relative_to(root)
        for number, line in enumerate(text.splitlines(), start=1):
            for match in RESOURCE_ID.finditer(line):
                found.append(f"{where}:{number} {match.group(1)}-e00…")
            if TUNNEL_HOST.search(line):
                found.append(f"{where}:{number} tunnel host")
    return found


def tracked_files(root: Path) -> list[Path]:
    """Tracked text files under ``SCANNED``; a walk when git is absent."""
    git = shutil.which("git")
    if git is not None and (root / ".git").exists():
        listed = subprocess.run(
            [git, "ls-files", "--", *SCANNED],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        candidates = [root / name for name in listed]
    else:
        candidates = []
        for name in SCANNED:
            base = root / name
            candidates.extend(base.rglob("*") if base.is_dir() else [base])
    return [
        path
        for path in candidates
        if path.is_file()
        and ".venv" not in path.parts
        and path.suffix in TEXT_SUFFIXES | {""}
    ]


def _fake_id(prefix: str) -> str:
    rng = random.Random(7)
    tail = "".join(rng.choices(string.ascii_lowercase + string.digits, k=16))
    return f"{prefix}-e00{tail}"


def test_tracked_files_hold_no_resource_ids() -> None:
    files = tracked_files(ROOT)
    assert any(path.suffix == ".ipynb" for path in files)
    found = find_ids(files, ROOT)
    assert found == [], "Replace these ids with placeholders: " + "; ".join(
        found
    )


@pytest.mark.parametrize(
    ("name", "template"),
    [
        ("leak.ipynb", '{{"cells": [{{"source": ["{id}"]}}]}}\n'),
        ("test_leak.py", 'PROJECT = "{id}"\n'),
    ],
)
def test_a_synthetic_id_is_caught_without_echoing_it(
    tmp_path: Path, name: str, template: str
) -> None:
    fake = _fake_id("project")
    path = tmp_path / name
    path.write_text(template.format(id=fake), encoding="utf-8")
    found = find_ids([path], tmp_path)
    assert found == [f"{name}:1 project-e00…"]
    assert fake not in found[0]


def test_placeholders_pass(tmp_path: Path) -> None:
    path = tmp_path / "ok.md"
    path.write_text(
        "project-e00… mbsec-... mbsecver-... aijob-test-0001 "
        "https://port8000-….tunnel.applications.eu-north1.nebius.cloud\n",
        encoding="utf-8",
    )
    assert find_ids([path], tmp_path) == []
