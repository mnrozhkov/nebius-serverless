"""Strict YAML loading shared by job and endpoint documents.

``load_document`` reads one document with the strict rules. The
job and endpoint packages check its keys and turn it into recipe
arguments.
"""

from pathlib import Path

import yaml
from yaml.events import (
    AliasEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceStartEvent,
)

from ..errors import ValidationError

_LIMIT = 1024 * 1024
_PLAIN_TAGS = frozenset(
    {
        "tag:yaml.org,2002:str",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:null",
        "tag:yaml.org,2002:seq",
        "tag:yaml.org,2002:map",
    }
)


class _StrictLoader(yaml.SafeLoader):
    """Safe loader that rejects aliases, anchors, tags, and duplicates."""

    def compose_node(self, parent, index):  # type: ignore[no-untyped-def]
        if self.check_event(AliasEvent):
            raise ValidationError(
                "YAML aliases are rejected.",
                operation="from_yaml",
            )
        event = self.peek_event()
        if getattr(event, "anchor", None):
            raise ValidationError(
                "YAML anchors are rejected.",
                operation="from_yaml",
            )
        if _explicit_tag(event):
            raise ValidationError(
                "YAML tags are rejected.",
                operation="from_yaml",
            )
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):  # type: ignore[no-untyped-def]
        mapping: dict[object, object] = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                raise ValidationError(
                    "YAML merge keys are rejected.",
                    operation="from_yaml",
                )
            key = self.construct_object(key_node, deep=deep)
            if key == "<<":
                raise ValidationError(
                    "YAML merge keys are rejected.",
                    operation="from_yaml",
                )
            if key in mapping:
                raise ValidationError(
                    f"Duplicate YAML key {key!r}.",
                    operation="from_yaml",
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _explicit_tag(event: object) -> bool:
    if not isinstance(
        event,
        (ScalarEvent, SequenceStartEvent, MappingStartEvent),
    ):
        return False
    tag = event.tag
    if tag is None or tag in _PLAIN_TAGS and _implicit(event):
        return False
    if tag not in _PLAIN_TAGS:
        return True
    return not _implicit(event)


def _implicit(
    event: ScalarEvent | SequenceStartEvent | MappingStartEvent,
) -> bool:
    implicit = getattr(event, "implicit", True)
    if isinstance(implicit, tuple):
        return any(implicit)
    return bool(implicit)


def load_document(path: str | Path) -> dict[object, object]:
    source = Path(path)
    try:
        data = source.read_bytes()
    except OSError:
        raise ValidationError(
            "The YAML document was rejected.",
            operation="from_yaml",
        ) from None
    if len(data) > _LIMIT:
        raise ValidationError(
            "The YAML document is over 1 MiB.",
            operation="from_yaml",
        )
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise ValidationError(
            "The YAML document was rejected.",
            operation="from_yaml",
        ) from None
    try:
        document = yaml.load(text, Loader=_StrictLoader)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "The YAML document was rejected.",
            operation="from_yaml",
        ) from None
    if not isinstance(document, dict):
        raise ValidationError(
            "The YAML document must be a mapping.",
            operation="from_yaml",
        )
    return document
