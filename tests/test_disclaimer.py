"""Every place that describes the package says it is unofficial."""

import re
from pathlib import Path

import pytest

import nebius_serverless as ns

ROOT = Path(__file__).resolve().parents[1]
DISCLAIMER = "Not an official Nebius product, and Nebius does not support it."


def _flat(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "name",
    ["README.md", "NOTICE", "docs/index.md", "overrides/main.html"],
)
def test_the_disclaimer_is_shown(name: str) -> None:
    assert DISCLAIMER in _flat(ROOT / name)


def test_the_pypi_summary_and_docstring_say_unofficial() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    summary = re.search(r'^description = "(.*)"$', pyproject, re.MULTILINE)
    assert summary is not None
    assert summary.group(1).startswith("Unofficial ")
    assert (ns.__doc__ or "").startswith("Unofficial ")
