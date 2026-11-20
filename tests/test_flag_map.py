"""The CLI and API map gives every recipe argument a flag and an API field."""

import inspect
import re
from pathlib import Path

import pytest

import nebius_serverless as ns

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "docs" / "reference" / "cli-api-map.md"
ARGUMENT = re.compile(r"`(\w+)=")


def _map_section() -> str:
    text = REFERENCE.read_text(encoding="utf-8")
    start = text.index("\n## Arguments\n")
    end = text.index("\n## Commands\n", start)
    return text[:end]


def mapped_arguments(section: str) -> set[str]:
    """Names written as `name=` in the first column of the map table."""
    names: set[str] = set()
    for line in section.splitlines():
        if not line.startswith("| ") or line.startswith("| ---"):
            continue
        first = line.split(" | ", 1)[0]
        names.update(ARGUMENT.findall(first))
    return names


def test_the_map_names_the_cli_version() -> None:
    assert "CLI 0.12.309" in _map_section()


@pytest.mark.parametrize("recipe", [ns.JobSpec, ns.EndpointSpec])
def test_every_recipe_argument_has_a_row(recipe: type) -> None:
    parameters = set(inspect.signature(recipe.__init__).parameters)
    parameters.discard("self")
    missing = sorted(parameters - mapped_arguments(_map_section()))
    assert missing == [], f"Add rows to the CLI and API map for {missing}"


def test_a_new_argument_without_a_row_is_caught() -> None:
    section = _map_section()
    assert "brand_new" not in mapped_arguments(section)
    row = "| `brand_new=` | `--brand-new` | `brand_new` | Same. |"
    assert "brand_new" in mapped_arguments(section + "\n" + row)


def test_intended_differences_say_why() -> None:
    for line in _map_section().splitlines():
        if "Intended" in line:
            note = line.rstrip(" |").rsplit(" | ", 1)[-1]
            assert len(note) > len("Intended."), line
