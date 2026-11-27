"""Fields read from a job or endpoint record."""


def record_name(record: dict[str, object]) -> str:
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    name = metadata.get("name")
    if isinstance(name, str):
        return name
    return ""
