"""Validate an endpoint recipe and map it to a create request."""

import re
import secrets
from collections.abc import Sequence
from dataclasses import dataclass

from ..core.recipe import (
    DISK_DEFAULT,
    SHM_LABEL,
    describe_extras,
    disk_bytes,
    machine_fields,
    parse_volume,
    quoted_args,
    secret_ref,
    spec_extras,
    validate_command,
    validate_secret_ref,
)
from ..errors import ValidationError

AUTH_MODES = ("none", "token")
TOKEN_GENERATED = "generated"
TOKEN_PROVIDED = "provided"
TOKEN_SECRET = "secret"
_OPERATION = "EndpointSpec"


def validate_optional_command(command: object) -> tuple[str, ...] | None:
    """``None`` keeps the image entrypoint. A list overrides it."""
    if command is None:
        return None
    return validate_command(command, operation=_OPERATION)


PROTOCOLS = ("http", "tcp", "udp")
_PORT = re.compile(r"^([0-9]+)(?::([0-9]+))?(?:/([A-Za-z]+))?$")
_TOKEN_PORTS = (
    "Token auth needs exactly one port, and it must be HTTP: Nebius "
    "rejects TCP and UDP ports behind its auth proxy. Pass auth='none' "
    "to expose these ports without a token."
)


@dataclass(frozen=True)
class PortSpec:
    """One exposed port: container port, host port, and protocol."""

    container: int
    host: int
    protocol: str

    def label(self) -> str:
        """The CLI form, e.g. ``8000/http`` or ``80:8080/tcp``."""
        if self.host == self.container:
            return f"{self.container}/{self.protocol}"
        return f"{self.host}:{self.container}/{self.protocol}"

    def body(self) -> dict[str, object]:
        """The ``spec.ports[]`` entry for the create body."""
        entry: dict[str, object] = {
            "container_port": self.container,
            "protocol": self.protocol.upper(),
        }
        if self.host != self.container:
            entry["host_port"] = self.host
        return entry


def validate_port(port: object) -> int:
    """One HTTP container port, an integer from 1 to 65535."""
    if not _port_number(port):
        raise ValidationError(
            "port must be an integer from 1 to 65535.",
            operation=_OPERATION,
        )
    assert isinstance(port, int)
    return port


def validate_ports(port: object, ports: object) -> tuple[PortSpec, ...]:
    """Exactly one of ``port`` (one HTTP port) or ``ports``.

    A ``ports`` item is an int (an HTTP container port) or the CLI form
    ``HOST[:CONTAINER][/http|tcp|udp]``; the protocol defaults to http.
    """
    if (port is None) == (ports is None):
        raise ValidationError(
            "Pass exactly one of port= (one HTTP port) or ports=.",
            operation=_OPERATION,
        )
    if port is not None:
        number = validate_port(port)
        return (PortSpec(number, number, "http"),)
    if isinstance(ports, (str, bytes)) or not isinstance(ports, Sequence):
        raise ValidationError(
            "ports must be a list such as [8000, '6379/tcp'].",
            operation=_OPERATION,
        )
    if not ports:
        raise ValidationError(
            "ports must not be empty.",
            operation=_OPERATION,
        )
    parsed = tuple(_parse_port(item) for item in ports)
    seen: set[tuple[int, str]] = set()
    for item in parsed:
        key = (item.container, item.protocol)
        if key in seen:
            raise ValidationError(
                f"ports repeats container port {item.container}/"
                f"{item.protocol}.",
                operation=_OPERATION,
            )
        seen.add(key)
    return parsed


def check_token_ports(auth: str, ports: Sequence[PortSpec]) -> None:
    """Token auth allows exactly one port, an HTTP one.

    The API states "exactly one HTTP port"; a live create on 19 Nov 2026
    also rejected an extra TCP or UDP port ("unsupported port protocol
    for auth/metrics proxy").
    """
    if auth != "token":
        return
    if len(ports) != 1 or ports[0].protocol != "http":
        raise ValidationError(_TOKEN_PORTS, operation=_OPERATION)


def _parse_port(item: object) -> PortSpec:
    if isinstance(item, int) and not isinstance(item, bool):
        number = validate_port(item)
        return PortSpec(number, number, "http")
    match = _PORT.fullmatch(item) if isinstance(item, str) else None
    if match is None:
        raise ValidationError(
            "Each port is an int or HOST[:CONTAINER][/http|tcp|udp], "
            "such as 8000, '6379/tcp' or '80:8080'.",
            operation=_OPERATION,
        )
    first, second, protocol = match.groups()
    host = int(first)
    container = int(second) if second is not None else host
    proto = (protocol or "http").lower()
    if proto not in PROTOCOLS:
        raise ValidationError(
            "A port protocol must be http, tcp or udp.",
            operation=_OPERATION,
        )
    if not (_port_number(host) and _port_number(container)):
        raise ValidationError(
            "Port numbers must be from 1 to 65535.",
            operation=_OPERATION,
        )
    return PortSpec(container, host, proto)


def _port_number(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1 <= value <= 65535
    )


def validate_auth(auth: object) -> str:
    """Accept ``none`` or ``token``."""
    if auth not in AUTH_MODES:
        raise ValidationError(
            "auth must be 'none' or 'token'.",
            operation=_OPERATION,
        )
    return auth


def validate_token(token: object, auth: str) -> str | None:
    """A bearer token only makes sense with ``auth='token'``.

    The value is never echoed in the error text.
    """
    if token is None:
        return None
    if not isinstance(token, str) or token == "":
        raise ValidationError(
            "token must be a nonempty string.",
            operation=_OPERATION,
        )
    if auth != "token":
        raise ValidationError(
            "token requires auth='token'.",
            operation=_OPERATION,
        )
    return token


def validate_token_secret(
    token_secret: object,
    auth: str,
    token: str | None,
) -> str | None:
    """A MysteryBox secret holding ``AUTH_TOKEN``. Excludes ``token``."""
    if token_secret is None:
        return None
    value = validate_secret_ref(
        token_secret, "token_secret", operation=_OPERATION
    )
    if auth != "token":
        raise ValidationError(
            "token_secret requires auth='token'.",
            operation=_OPERATION,
        )
    if token is not None:
        raise ValidationError(
            "Set token or token_secret, not both.",
            operation=_OPERATION,
        )
    return value


def generate_token() -> str:
    """A random bearer token, as the CLI makes for ``--auth token``."""
    return secrets.token_urlsafe(32)


def describe_endpoint(
    *,
    command: Sequence[str] | None,
    image: str,
    ports: Sequence[PortSpec],
    project: str,
    region: str,
    platform: str | None,
    preset: str | None,
    pricing: str,
    auth: str,
    token_supplied: bool,
    subnet_id: str | None,
    volumes: Sequence[str],
    inject_files: Sequence[str],
    env_keys: Sequence[str],
    name: str,
    disk: str | None,
    token_secret: str | None = None,
    working_dir: str | None = None,
    secrets: Sequence[tuple[str, str]] = (),
    registry_secret: str | None = None,
    spot_policy: str | None = None,
    shm: str | None = SHM_LABEL,
    gpu: str | None = None,
    cpu: str | None = None,
) -> dict[str, object]:
    """Build the dry-run description. Env values and tokens are left out."""
    description: dict[str, object] = {"image": image}
    if command is not None:
        description["command"] = list(command)
        args = quoted_args(command)
        if args is not None:
            description["args"] = args
    description["ports"] = [item.label() for item in ports]
    description["project"] = project
    description["region"] = region
    machine_fields(
        description,
        platform=platform,
        preset=preset,
        gpu=gpu,
        cpu=cpu,
        body=False,
    )
    description["pricing"] = pricing
    description["auth"] = auth
    if token_secret is not None:
        description["token"] = TOKEN_SECRET
        description["token_secret"] = token_secret
    elif auth == "token":
        description["token"] = (
            TOKEN_PROVIDED if token_supplied else TOKEN_GENERATED
        )
    if subnet_id is not None:
        description["subnet_id"] = subnet_id
    if volumes:
        description["volumes"] = list(volumes)
    if inject_files:
        description["inject_files"] = list(inject_files)
    description["env"] = list(env_keys)
    description["name"] = name
    description["disk"] = disk if disk is not None else DISK_DEFAULT
    if shm is not None:
        description["shm"] = shm
    describe_extras(
        description,
        working_dir=working_dir,
        secrets=secrets,
        registry_secret=registry_secret,
        spot_policy=spot_policy,
    )
    return description


def endpoint_request_body(
    *,
    command: Sequence[str] | None,
    image: str,
    ports: Sequence[PortSpec],
    project: str,
    platform: str | None,
    preset: str | None,
    pricing: str,
    token: str | None,
    subnet_id: str | None,
    volumes: Sequence[str],
    inject_files: Sequence[dict[str, object]],
    env: Sequence[tuple[str, str]],
    name: str,
    disk: str | None,
    token_secret: str | None = None,
    working_dir: str | None = None,
    secrets: Sequence[tuple[str, str]] = (),
    registry_secret: str | None = None,
    spot_policy: str | None = None,
    shm: str | None = SHM_LABEL,
    gpu: str | None = None,
    cpu: str | None = None,
) -> dict[str, object]:
    """Map a recipe to the create body the endpoint transport sends."""
    spec: dict[str, object] = {"image": image}
    if command is not None:
        spec["container_command"] = command[0]
        args = quoted_args(command)
        if args is not None:
            spec["args"] = args
    spec["ports"] = [item.body() for item in ports]
    machine_fields(
        spec, platform=platform, preset=preset, gpu=gpu, cpu=cpu, body=True
    )
    spec["disk"] = disk if disk is not None else DISK_DEFAULT
    if shm is not None:
        spec["shm_size_bytes"] = disk_bytes(shm, operation=_OPERATION)
    if subnet_id is not None:
        spec["subnet_id"] = subnet_id
    if volumes:
        spec["volumes"] = [parse_volume(item) for item in volumes]
    if inject_files:
        spec["injected_files"] = list(inject_files)
    spec_extras(
        spec,
        pricing=pricing,
        env=env,
        working_dir=working_dir,
        secrets=secrets,
        registry_secret=registry_secret,
        spot_policy=spot_policy,
    )
    if token is not None:
        spec["auth_token"] = token
    if token_secret is not None:
        spec["auth_token_mysterybox_secret"] = secret_ref(token_secret)
    return {
        "metadata": {"parent_id": project, "name": name},
        "spec": spec,
    }
