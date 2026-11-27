"""The core knows no orchestrator, and integrations use its public API.

The core is every module of ``nebius_serverless`` outside
``integrations/``. It must not import or mention an orchestrator. An
integration must not import a private (``_``) name from the core, nor
read a private attribute of anything but itself. Importing the package
must not load an integration.
"""

import ast
import re
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "nebius_serverless"
INTEGRATIONS = PACKAGE / "integrations"
ORCHESTRATORS = re.compile(r"prefect|airflow|dagster|nextflow", re.IGNORECASE)


def _core_modules() -> list[Path]:
    return [
        path
        for path in sorted(PACKAGE.rglob("*.py"))
        if INTEGRATIONS not in path.parents
    ]


def _integration_modules() -> list[Path]:
    return sorted(INTEGRATIONS.rglob("*.py"))


def _private(name: str) -> bool:
    return name.startswith("_") and not (
        name.startswith("__") and name.endswith("__")
    )


def test_the_core_does_not_mention_an_orchestrator() -> None:
    found = [
        f"{path.relative_to(PACKAGE)}:{number}"
        for path in _core_modules()
        for number, line in enumerate(path.read_text().splitlines(), 1)
        if ORCHESTRATORS.search(line)
    ]
    assert found == []


def _core_module(node: ast.ImportFrom, path: Path) -> bool:
    """Whether an import names a module of the core."""
    if node.level == 0:
        module = node.module or ""
        return module == "nebius_serverless" or (
            module.startswith("nebius_serverless.")
            and not module.startswith("nebius_serverless.integrations")
        )
    # The number of packages between the module and nebius_serverless:
    # 2 for integrations/prefect/flow.py. A relative import climbs one
    # package per dot past the first, so it reaches nebius_serverless
    # with more dots than that.
    depth = len(path.relative_to(PACKAGE).parts) - 1
    return node.level > depth and not (node.module or "").startswith(
        "integrations"
    )


def test_integrations_use_only_the_public_api_of_the_core() -> None:
    found: list[str] = []
    for path in _integration_modules():
        where = path.relative_to(PACKAGE)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and _core_module(node, path):
                parts = (node.module or "").split(".")
                names = [alias.name for alias in node.names]
                for name in [*parts, *names]:
                    if _private(name):
                        found.append(f"{where}:{node.lineno} imports {name}")
            elif isinstance(node, ast.Attribute) and _private(node.attr):
                owner = node.value
                if not (isinstance(owner, ast.Name) and owner.id == "self"):
                    found.append(f"{where}:{node.lineno} reads .{node.attr}")
    assert found == []


def test_importing_the_package_loads_no_integration() -> None:
    code = (
        "import sys, nebius_serverless; "
        "loaded = [name for name in sys.modules "
        "if name.split('.')[0] == 'prefect' "
        "or name.startswith('nebius_serverless.integrations')]; "
        "print(loaded)"
    )
    done = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "[]"
