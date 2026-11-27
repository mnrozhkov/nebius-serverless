"""Read a schema-version-1 job document into ``JobSpec`` arguments."""

from collections.abc import Mapping
from pathlib import Path

from ..core.yaml_loader import load_document
from ..errors import UnsupportedFeature, ValidationError

ALLOWED = frozenset(
    {
        "schema_version",
        "kind",
        "image",
        "command",
        "gpu",
        "cpu",
        "platform",
        "preset",
        "timeout",
        "pricing",
        "subnet_id",
        "volumes",
        "env",
        "name",
        "disk",
        "restart_attempts",
        "preemption_retries",
        "working_dir",
        "secrets",
        "registry_secret",
        "spot_policy",
        "shm",
    }
)


def load_yaml_fields(path: str | Path) -> dict[str, object]:
    """Read one command document into ``JobSpec`` constructor arguments.

    Relative strings inside the document are not expanded. A document
    over 1 MiB is rejected before it is parsed.
    """
    return _fields(load_document(path))


def _fields(document: Mapping[object, object]) -> dict[str, object]:
    if (
        document.get("kind") == "function"
        or document.get("execution") == "function"
    ):
        raise UnsupportedFeature(
            "Function YAML is not supported. Use ns.Task for a function.",
            operation="from_yaml",
        )
    if document.get("kind") == "endpoint":
        raise ValidationError(
            "kind is endpoint. Load it with EndpointSpec.from_yaml.",
            operation="from_yaml",
        )
    unknown = sorted(
        str(key) for key in document if key not in ALLOWED
    )
    if unknown:
        names = ", ".join(unknown)
        raise ValidationError(
            f"Unknown YAML keys: {names}.",
            operation="from_yaml",
        )
    if document.get("schema_version") != 1:
        raise ValidationError(
            "schema_version must be 1.",
            operation="from_yaml",
        )
    if document.get("kind") != "job":
        raise ValidationError(
            "kind must be job.",
            operation="from_yaml",
        )
    fields: dict[str, object] = {
        "command": document.get("command"),
        "image": document.get("image"),
        "timeout": document.get("timeout"),
    }
    for name in (
        "gpu",
        "cpu",
        "platform",
        "preset",
        "pricing",
        "subnet_id",
        "volumes",
        "env",
        "name",
        "disk",
        "restart_attempts",
        "preemption_retries",
        "working_dir",
        "secrets",
        "registry_secret",
        "spot_policy",
        "shm",
    ):
        if name in document and document[name] is not None:
            fields[name] = document[name]
    return fields
