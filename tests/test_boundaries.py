"""The package keeps its layers, and integrations use the supported interface.

- The core (every module outside ``integrations/``) does not import or
  mention an orchestrator.
- ``core/`` and ``ui/`` import no feature package. Feature packages
  import only what their layer allows: ``images`` and ``tasks`` may use
  ``jobs``, ``tasks`` may use ``images``, ``endpoints`` uses neither.
- An integration imports only the package root, ``errors``, and
  ``tasks.driver`` (the names in its ``__all__``), and reads no private
  attribute of anything but itself.
- The task runner that runs in a job uses only the standard library.
- Importing the package does not load an integration.
"""

import ast
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

from nebius_serverless.tasks import driver

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "nebius_serverless"
INTEGRATIONS = PACKAGE / "integrations"
RUNNER = PACKAGE / "tasks" / "runtime" / "runner.py"
ORCHESTRATORS = re.compile(r"prefect|airflow|dagster|nextflow", re.IGNORECASE)
NAME = "nebius_serverless"

# Which packages each layer may import. Root modules (``errors``,
# ``_version``) are open to every layer.
ALLOWED = {
    "ui": {"ui"},
    "core": {"core", "ui"},
    "jobs": {"core", "ui", "jobs"},
    "endpoints": {"core", "ui", "endpoints"},
    "images": {"core", "ui", "jobs", "images"},
    "tasks": {"core", "ui", "jobs", "images", "tasks"},
}
ROOT_MODULES = {"errors", "_version"}
INTEGRATION_INTERFACE = {
    NAME,
    f"{NAME}.errors",
    f"{NAME}.tasks.driver",
}


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


def _package_of(path: Path) -> list[str]:
    """The dotted package a module is in, as parts."""
    return [NAME, *path.relative_to(PACKAGE).parts[:-1]]


def _imports(path: Path) -> list[tuple[int, str, list[str]]]:
    """Each import of the package: line, absolute module, and names.

    ``from . import a`` is reported once per name, as module ``pkg.a``.
    """
    found = []
    package = _package_of(path)
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == NAME:
                    found.append((node.lineno, alias.name, []))
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = (node.module or "").split(".")
            else:
                base = package[: len(package) - (node.level - 1)]
                base += (node.module or "").split(".") if node.module else []
            if base[0] != NAME:
                continue
            names = [alias.name for alias in node.names]
            module = ".".join(base)
            if node.module is None:
                for name in names:
                    found.append((node.lineno, f"{module}.{name}", []))
            else:
                found.append((node.lineno, module, names))
    return found


def _layer(module: str) -> str | None:
    parts = module.split(".")
    return parts[1] if len(parts) > 1 else None


def test_the_core_does_not_mention_an_orchestrator() -> None:
    found = [
        f"{path.relative_to(PACKAGE)}:{number}"
        for path in _core_modules()
        for number, line in enumerate(path.read_text().splitlines(), 1)
        if ORCHESTRATORS.search(line)
    ]
    assert found == []


def test_each_layer_imports_only_what_it_may() -> None:
    found = []
    for path in _core_modules():
        where = path.relative_to(PACKAGE)
        own = where.parts[0] if len(where.parts) > 1 else None
        if own is None:
            continue  # the root: __init__.py, errors.py, _version.py
        allowed = ALLOWED[own] | ROOT_MODULES
        for line, module, _names in _imports(path):
            layer = _layer(module)
            if layer is not None and layer not in allowed:
                found.append(f"{where}:{line} imports {module}")
    assert found == []


def test_integrations_use_only_the_supported_interface() -> None:
    interface = set(driver.__all__)
    found: list[str] = []
    for path in _integration_modules():
        where = path.relative_to(PACKAGE)
        own = ".".join([NAME, *where.parts[:2]])  # integrations.<name>
        for line, module, names in _imports(path):
            if module == own or module.startswith(f"{own}."):
                continue
            if module not in INTEGRATION_INTERFACE:
                found.append(f"{where}:{line} imports {module}")
            elif module == f"{NAME}.tasks.driver":
                found += [
                    f"{where}:{line} imports {name} from tasks.driver"
                    for name in names
                    if name not in interface
                ]
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute) and _private(node.attr):
                owner = node.value
                if not (isinstance(owner, ast.Name) and owner.id == "self"):
                    found.append(f"{where}:{node.lineno} reads .{node.attr}")
    assert found == []


def test_the_task_runner_uses_only_the_standard_library() -> None:
    found = []
    for node in ast.walk(ast.parse(RUNNER.read_text())):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                found.append(f"{node.lineno}: relative import")
                continue
            names = [node.module or ""]
        else:
            continue
        found += [
            f"{node.lineno}: {name}"
            for name in names
            if name.split(".")[0] not in sys.stdlib_module_names
        ]
    assert found == []


def test_the_task_runner_runs_without_the_package(tmp_path: Path) -> None:
    """The runner runs in a bare interpreter, outside this package."""
    runner = tmp_path / "runner.py"
    runner.write_text(RUNNER.read_text())
    root = tmp_path / "bucket"
    run = root / "tasks" / "add" / "1"
    run.mkdir(parents=True)
    payload = json.dumps(
        {
            "libs": None,
            "libs_dir": str(tmp_path / "libs"),
            "module": None,
            "qualname": "add",
            "source": "def add(a, b):\n    return a + b\n",
            "args": [2, 3],
            "kwargs": {},
        }
    ).encode()
    (run / "payload.json").write_bytes(payload)
    pointer = {
        "root": str(root),
        "run": "tasks/add/1",
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (tmp_path / "pointer.json").write_text(json.dumps(pointer))
    done = subprocess.run(
        [sys.executable, "-I", "-S", str(runner)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert done.returncode == 0, done.stderr
    result = json.loads((run / "result.json").read_text())
    assert result == {"ok": True, "value": 5, "attempt": 1}


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
