"""Read a schema-version-1 endpoint document into ``EndpointSpec`` args."""

from collections.abc import Mapping
from pathlib import Path

from ..core.yaml_loader import load_document
from ..errors import ValidationError

ENDPOINT_ALLOWED = frozenset(
    {
        "schema_version",
        "kind",
        "image",
        "command",
        "port",
        "ports",
        "gpu",
        "cpu",
        "platform",
        "preset",
        "pricing",
        "auth",
        "subnet_id",
        "volumes",
        "env",
        "name",
        "disk",
        "token_secret",
        "working_dir",
        "secrets",
        "registry_secret",
        "spot_policy",
        "shm",
    }
)
_TOKEN_IN_YAML = (
    "token is not accepted in YAML, because YAML files are often "
    "committed. Store the token in MysteryBox and set token_secret, or "
    "pass token= in Python."
)


def load_endpoint_yaml_fields(path: str | Path) -> dict[str, object]:
    """Read one endpoint document into ``EndpointSpec`` constructor arguments.

    The same strict rules as job documents apply. ``kind`` must be
    ``endpoint``.
    """
    return _endpoint_fields(load_document(path))


def _endpoint_fields(
    document: Mapping[object, object],
) -> dict[str, object]:
    if document.get("kind") == "job":
        raise ValidationError(
            "kind is job. Load it with JobSpec.from_yaml.",
            operation="from_yaml",
        )
    if "token" in document:
        # The value is never echoed.
        raise ValidationError(_TOKEN_IN_YAML, operation="from_yaml")
    unknown = sorted(
        str(key) for key in document if key not in ENDPOINT_ALLOWED
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
    if document.get("kind") != "endpoint":
        raise ValidationError(
            "kind must be endpoint.",
            operation="from_yaml",
        )
    fields: dict[str, object] = {"image": document.get("image")}
    for name in (
        "port",
        "ports",
        "command",
        "gpu",
        "cpu",
        "platform",
        "preset",
        "pricing",
        "auth",
        "subnet_id",
        "volumes",
        "env",
        "name",
        "disk",
        "token_secret",
        "working_dir",
        "secrets",
        "registry_secret",
        "spot_policy",
        "shm",
    ):
        if name in document and document[name] is not None:
            fields[name] = document[name]
    return fields
