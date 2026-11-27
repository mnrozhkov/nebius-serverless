"""The supported public surface does not change by accident.

``public_api.json`` records every name in ``nebius_serverless.__all__``
with its parameters: names, kinds and defaults, not annotations, so
the record is the same on every supported Python version. A change to
the public API updates the file in the same commit, on purpose.

Regenerate it with ``python tests/test_public_api.py > tests/public_api.json``.
"""

import inspect
import json
from pathlib import Path
from typing import Any

import nebius_serverless as ns
from nebius_serverless.tasks import driver

RECORD = Path(__file__).with_name("public_api.json")

# The interface integrations may use: see ``tasks/driver.py``.
DRIVER_INTERFACE = [
    "Driver",
    "Image",
    "Launch",
    "Progress",
    "Task",
    "TaskRun",
    "bind",
    "call_progress",
    "console_url",
    "job_tasks",
    "run_paths",
    "say",
    "start_run",
    "validate_image",
]


def _parameters(obj: Any) -> list[str] | None:
    try:
        signature = inspect.signature(obj)
    except (TypeError, ValueError):
        return None
    shown = []
    for parameter in signature.parameters.values():
        text = f"{parameter.name}:{parameter.kind.name}"
        if parameter.default is not inspect.Parameter.empty:
            text += f"={parameter.default!r}"
        shown.append(text)
    return shown


def _fields(model: Any) -> list[str]:
    """A spec's fields in order, each with its default when it has one."""
    shown = []
    for name, info in model.model_fields.items():
        if info.is_required():
            shown.append(name)
            continue
        factory = info.default_factory
        default = factory() if factory is not None else info.default
        shown.append(f"{name}={default!r}")
    return shown


def _describe(obj: Any) -> dict[str, Any]:
    if inspect.isclass(obj):
        # A spec also shows what it inherits from the library's base
        # model, and its fields; not Pydantic's own methods.
        is_spec = hasattr(obj, "model_fields")
        owners = (
            [
                owner
                for owner in reversed(obj.__mro__)
                if owner.__module__.startswith("nebius_serverless")
            ]
            if is_spec
            else [obj]
        )
        members: dict[str, Any] = {}
        for owner in owners:
            for name, value in vars(owner).items():
                if name.startswith("_") and name != "__init__":
                    continue
                if isinstance(value, property):
                    members[name] = "property"
                elif isinstance(value, (staticmethod, classmethod)):
                    members[name] = _parameters(value.__func__)
                elif callable(value):
                    members[name] = _parameters(value)
        described: dict[str, Any] = {
            "kind": "class",
            "members": dict(sorted(members.items())),
        }
        if is_spec:
            described["fields"] = _fields(obj)
        return described
    if inspect.isroutine(obj):
        return {"kind": "function", "parameters": _parameters(obj)}
    # ns.jobs and ns.endpoints: lookup by id.
    return {
        "kind": "lookup",
        "members": {
            name: _parameters(getattr(obj, name))
            for name in ("get", "get_async")
        },
    }


def surface() -> dict[str, Any]:
    return {name: _describe(getattr(ns, name)) for name in sorted(ns.__all__)}


def test_the_public_surface_matches_the_record() -> None:
    assert surface() == json.loads(RECORD.read_text())


def test_jobs_and_endpoints_keep_their_lookup_calls() -> None:
    for namespace in (ns.jobs, ns.endpoints):
        assert callable(namespace.get)
        assert inspect.iscoroutinefunction(namespace.get_async)


def test_the_integration_interface_is_listed() -> None:
    assert sorted(driver.__all__) == DRIVER_INTERFACE
    for name in driver.__all__:
        assert hasattr(driver, name), name


if __name__ == "__main__":
    print(json.dumps(surface(), indent=2, sort_keys=True))
