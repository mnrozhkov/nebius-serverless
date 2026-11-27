"""The base of ``JobSpec`` and ``EndpointSpec``: frozen Pydantic data.

A spec is data. It makes no Nebius calls. Each field is checked by the
same functions as before (``core/recipe.py``), so the error messages
stay the same. Those checks raise ``ns.ValidationError`` themselves;
an error Pydantic finds on its own, such as an unknown field passed to
``model_validate``, is converted to ``ns.ValidationError`` too.
"""

import json
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel, ConfigDict, SecretStr, field_validator
from pydantic import ValidationError as PydanticError

from ..errors import ValidationError
from .machines import check_machine, validate_cpu, validate_gpu
from .recipe import (
    reject_spot_on_cpu,
    validate_disk,
    validate_env,
    validate_image,
    validate_inject_files,
    validate_optional_text,
    validate_pricing,
    validate_registry_secret,
    validate_secrets,
    validate_shm,
    validate_volumes,
    validate_working_dir,
)

# How a hidden value is written by ``to_dict`` and ``to_yaml``.
HIDDEN = "<hidden>"

_S = TypeVar("_S", bound="SpecModel")

Check = Callable[[object], object]


class SpecModel(BaseModel):
    """Frozen, strict data with the library's own field checks."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # The ``operation`` errors name, such as ``JobSpec``.
    _operation: ClassVar[str] = "spec"

    @classmethod
    def _checks(cls) -> Mapping[str, Check]:
        """The check for each field. A field without one is not checked
        beyond its type."""
        return {}

    @field_validator("*", mode="before")
    @classmethod
    def _check_field(cls, value: object, info: Any) -> object:
        check = cls._checks().get(info.field_name)
        if check is None:
            return value
        try:
            return check(value)
        except ValidationError as exc:
            if not exc.details:
                exc.details = [
                    {
                        "loc": (info.field_name,),
                        "msg": str(exc),
                        "type": exc.code,
                    }
                ]
            raise

    def _init_fields(self, fields: dict[str, Any]) -> None:
        """Validate ``fields`` and set them. Called by ``__init__``."""
        try:
            BaseModel.__init__(self, **fields)
        except PydanticError as exc:
            raise converted(exc, self._operation) from None

    @classmethod
    def model_validate(  # type: ignore[override]
        cls: type[_S], obj: Any, *args: Any, **kwargs: Any
    ) -> _S:
        """Pydantic's ``model_validate``, raising ``ns.ValidationError``."""
        if isinstance(obj, Mapping):
            cls._check_names(obj)
        try:
            return super().model_validate(obj, *args, **kwargs)
        except PydanticError as exc:
            raise converted(exc, cls._operation) from None

    @classmethod
    def model_validate_json(  # type: ignore[override]
        cls: type[_S],
        json_data: str | bytes | bytearray,
        *args: Any,
        **kwargs: Any,
    ) -> _S:
        """Pydantic's ``model_validate_json``, raising
        ``ns.ValidationError``."""
        try:
            data = json.loads(json_data)
        except ValueError:
            raise ValidationError(
                f"{cls._operation} JSON is not valid.",
                operation=cls._operation,
            ) from None
        return cls.model_validate(data, *args, **kwargs)

    @classmethod
    def _check_names(cls, data: Mapping[Any, Any]) -> None:
        """Unknown and missing fields, named before any value is read."""
        fields = cls.model_fields
        unknown = sorted(str(name) for name in data if name not in fields)
        if unknown:
            raise ValidationError(
                f"{cls._operation} has no field {', '.join(unknown)}.",
                operation=cls._operation,
                details=[
                    {"loc": (name,), "msg": "unknown field", "type": "extra"}
                    for name in unknown
                ],
            )
        missing = [
            name
            for name, info in fields.items()
            if info.is_required() and name not in data
        ]
        if missing:
            verb = "is" if len(missing) == 1 else "are"
            raise ValidationError(
                f"{', '.join(missing)} {verb} required.",
                operation=cls._operation,
                details=[
                    {"loc": (name,), "msg": "required", "type": "missing"}
                    for name in missing
                ],
            )

    def clone(self: _S, **changes: Any) -> _S:
        """A copy with ``changes`` applied, checked again.

        With no arguments it is an exact copy: ``spec.clone() == spec``.
        A change is validated like the original arguments, so an
        invalid value raises ``ValidationError``.
        """
        type(self)._check_names({**type(self).model_fields, **changes})
        fields = type(self).model_fields
        values = {name: getattr(self, name) for name in fields}
        values.update(changes)
        return type(self)(**values)

    def to_dict(self, *, redact: bool = True) -> dict[str, Any]:
        """The spec as JSON values.

        ``redact=True`` (default) replaces env values and tokens with
        ``"<hidden>"``, so the result is safe for logs and files.
        ``redact=False`` keeps them.
        """
        data: dict[str, Any] = {}
        for name in type(self).model_fields:
            value = getattr(self, name)
            if isinstance(value, SecretStr):
                value = HIDDEN if redact else value.get_secret_value()
            elif name == "env" and redact:
                value = {key: HIDDEN for key in value}
            data[name] = _plain(value)
        return data

    def _document(
        self, kind: str, allowed: frozenset[str], redact: bool
    ) -> dict[str, Any]:
        """A schema-version-1 document with the fields YAML accepts.

        Fields at their defaults are left out.
        """
        data = self.to_dict(redact=redact)
        document: dict[str, Any] = {"schema_version": 1, "kind": kind}
        for name, info in type(self).model_fields.items():
            if name not in allowed:
                continue
            value = data[name]
            default = (
                info.default_factory()  # type: ignore[call-arg]
                if info.default_factory is not None
                else info.default
            )
            if info.is_required() or _plain(default) != value:
                document[name] = value
        return document


def _plain(value: object) -> object:
    """Tuples as lists and mappings as dicts, for JSON and YAML."""
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    return value


def converted(exc: PydanticError, operation: str) -> ValidationError:
    """Pydantic's error as ``ns.ValidationError``. Inputs are not kept."""
    details = [
        {"loc": tuple(item["loc"]), "msg": item["msg"], "type": item["type"]}
        for item in exc.errors(
            include_url=False, include_input=False, include_context=False
        )
    ]
    first = details[0]
    loc = first["loc"]
    assert isinstance(loc, tuple)
    field = ".".join(str(part) for part in loc) or "the spec"
    if first["type"] == "missing":
        message = f"{field} is required."
    elif first["type"] == "extra_forbidden":
        message = f"{operation} has no field {field}."
    else:
        message = f"{field}: {first['msg']}."
    return ValidationError(message, operation=operation, details=details)


def shared_checks(operation: str) -> dict[str, Check]:
    """Field checks that jobs and endpoints share."""

    def text(field: str) -> Check:
        return lambda value: validate_optional_text(
            value, field, operation=operation
        )

    return {
        "image": lambda value: validate_image(value, operation=operation),
        "gpu": lambda value: validate_gpu(value, operation=operation),
        "cpu": lambda value: validate_cpu(value, operation=operation),
        "platform": text("platform"),
        "preset": text("preset"),
        "pricing": lambda value: validate_pricing(
            value, operation=operation
        ),
        "volumes": lambda value: validate_volumes(
            value, operation=operation
        ),
        "inject_files": lambda value: validate_inject_files(
            value, operation=operation
        ),
        "env": lambda value: dict(validate_env(value, operation=operation)),
        "secrets": lambda value: dict(
            validate_secrets(value, (), operation=operation)
        ),
        "name": text("name"),
        "disk": lambda value: validate_disk(
            validate_optional_text(value, "disk", operation=operation),
            operation=operation,
        ),
        "shm": lambda value: validate_shm(value, operation=operation),
        "working_dir": lambda value: validate_working_dir(
            value, operation=operation
        ),
        "registry_secret": lambda value: validate_registry_secret(
            value, operation=operation
        ),
        "spot_policy": text("spot_policy"),
        "subnet_id": text("subnet_id"),
        "profile": text("profile"),
        "project": text("project"),
    }


def check_combinations(
    *,
    gpu: str | None,
    cpu: str | None,
    platform: str | None,
    preset: str | None,
    pricing: str,
    spot_policy: str | None,
    env: Mapping[str, str],
    secrets: Mapping[str, str],
    operation: str,
) -> None:
    """The checks that need more than one field."""
    check_machine(gpu, cpu, platform, preset, operation=operation)
    reject_spot_on_cpu(pricing, gpu, platform, cpu=cpu, operation=operation)
    if spot_policy is not None and pricing != "spot":
        raise ValidationError(
            "spot_policy requires pricing='spot'.",
            operation=operation,
        )
    for key in secrets:
        if key in env:
            raise ValidationError(
                f"{key} is set in both env and secrets.",
                operation=operation,
            )
