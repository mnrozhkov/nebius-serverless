"""``EndpointSpec``: what an endpoint serves, as data. No Nebius calls."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import ClassVar, Literal

import yaml
from pydantic import Field, SecretStr, model_validator

from ..core.spec_model import (
    Check,
    SpecModel,
    check_combinations,
    shared_checks,
)
from .request import (
    PortSpec,
    check_token_ports,
    validate_auth,
    validate_optional_command,
    validate_ports,
    validate_token,
    validate_token_secret,
)
from .yaml import ENDPOINT_ALLOWED, load_endpoint_yaml_fields

_OP = "EndpointSpec"


class EndpointSpec(SpecModel):
    """What one ``nebius ai endpoint create`` serves: plain, frozen data.

    An endpoint is a container that stays up and answers on a managed
    ``https://`` URL. An ``EndpointSpec`` makes no Nebius calls.
    ``ns.Endpoint(spec).create()`` creates an endpoint from it. Each
    created endpoint bills for compute while it runs, until it is
    stopped or deleted. Use ``spec.clone(...)`` to change fields. All
    arguments are keyword arguments.

    Args:
        image: ``name:tag`` or ``registry/path@sha256:<digest>``, e.g.
            ``"vllm/vllm-openai:v0.19.1"``. Sent unchanged.
        command: Optional list of strings that overrides the image
            entrypoint, e.g. ``["vllm", "serve", "Qwen/Qwen3-0.6B"]``.
            ``None`` (default) keeps the entrypoint. The first item is
            the container command; the rest are POSIX-quoted into one
            ``args`` string. A shell string is rejected.
        port: One HTTP container port, 1 to 65535, e.g. ``8000``.
            Served on a managed ``https://`` URL (``Endpoint.url``).
        ports: Instead of ``port``: a list of ports, each an int (an
            HTTP port) or ``HOST[:CONTAINER][/http|tcp|udp]``, e.g.
            ``[8000, "6379/tcp"]``. Stored as labels such as
            ``"8000/http"``.
        gpu: GPU alias such as ``"h100"``, ``"h200"``, ``"b200"``,
            ``"b300"``, ``"l40s"`` or ``"rtx"``. Picks the smallest
            one-GPU preset in the project region. Omit it for CPU.
        cpu: CPU selector such as ``"amd:16"``. Excludes ``gpu``.
        platform: Explicit platform id, e.g. ``"gpu-h100-sxm"``.
            Overrides ``gpu``.
        preset: Explicit preset id, e.g. ``"1gpu-16vcpu-200gb"``. When
            omitted, the smallest preset for the platform is used.
        pricing: ``"on-demand"`` (default) or ``"spot"``. Spot is
            preemptible and not available on CPU.
        auth: ``"token"`` (default): callers must send
            ``Authorization: Bearer <token>``. ``"none"``: a public URL;
            anyone who has it can call the endpoint, and you pay for it.
        token: Bearer token for ``auth="token"``, stored as a
            ``SecretStr``. When omitted, a random token is generated on
            each ``create`` and returned as ``Endpoint.token``.
            ``Endpoint.reveal_token()`` fetches it again later. Use
            ``token_secret`` to manage the token in MysteryBox.
        token_secret: MysteryBox secret id or version id whose payload
            has ``AUTH_TOKEN``. Requires ``auth="token"`` and excludes
            ``token``. No token is generated.
        volumes: Mount strings ``source:/container/path[:ro|rw]`` or
            ``s3://bucket:/container/path[:ro|rw]`` (no ``:profile``
            suffix), e.g. ``["s3://my-models:/models:ro"]``. Mode
            defaults to ``rw``.
        inject_files: ``local_path:/container/path`` strings, e.g.
            ``["./config.yaml:/app/config.yaml"]``. Each file is read on
            ``create`` and must be at most 64 KiB.
        env: Plain environment variables, e.g.
            ``{"WANDB_MODE": "offline"}``. Values are hidden from
            ``repr``, dry-run output, error text and
            ``to_dict(redact=True)``. Use ``secrets`` for tokens.
        secrets: Environment variables read from MysteryBox, e.g.
            ``{"HF_TOKEN": "mbsec-..."}``. Each value is a secret id
            (``mbsec-``, latest version) or a version id
            (``mbsecver-``). Names must not repeat ``env``.
        name: Console label, sent unchanged on every create. When
            omitted, each ``create`` (and each dry run) generates
            ``endpoint-`` plus 12 hex chars. It is not a lookup key.
        disk: Boot disk size, e.g. ``"500Gi"``. Defaults to ``"250Gi"``.
        shm: ``/dev/shm`` size in the ``disk`` syntax, e.g. ``"64Gi"``,
            or ``"0"``. When omitted, a GPU machine gets ``16Gi`` and a
            CPU machine sends none, as the CLI's ``--shm-size`` does.
        working_dir: Container working directory, e.g. ``"/workspace"``.
            At most 128 characters. Defaults to the image ``WORKDIR``.
        registry_secret: MysteryBox secret version id (``mbsecver-...``)
            for a private image registry. The payload must have
            ``REGISTRY_USERNAME`` and ``REGISTRY_PASSWORD``.
        spot_policy: Pricing policy id that caps the spot price, e.g.
            ``"pricingpolicy-..."``. Requires ``pricing="spot"``.
        subnet_id: Subnet id, e.g. ``"vpcsubnet-..."``. Only needed
            when the project has more than one subnet.
        profile: Nebius CLI profile name. Defaults to the active profile.
        project: Project id, e.g. ``"project-..."``. Overrides the
            profile's project.

    Example::

        import nebius_serverless as ns

        spec = ns.EndpointSpec(
            image="vllm/vllm-openai:v0.19.1",
            command=["vllm", "serve", "Qwen/Qwen3-0.6B", "--port", "8000"],
            gpu="h100",
            port=8000,
            secrets={"HF_TOKEN": "mbsec-..."},  # read from MysteryBox
        )
        endpoint = ns.Endpoint(spec).create()
        endpoint.wait(probe="/v1/models")
        print(endpoint.url, endpoint.token)
        endpoint.delete()
    """

    _operation: ClassVar[str] = _OP

    image: str
    command: tuple[str, ...] | None = None
    port: int | None = None
    ports: tuple[str, ...] | None = None
    gpu: str | None = None
    cpu: str | None = None
    platform: str | None = None
    preset: str | None = None
    pricing: Literal["on-demand", "spot"] = "on-demand"
    auth: Literal["token", "none"] = "token"
    token: SecretStr | None = None
    token_secret: str | None = None
    volumes: tuple[str, ...] = ()
    inject_files: tuple[str, ...] = ()
    env: dict[str, str] = Field(default_factory=dict, repr=False)
    secrets: dict[str, str] = Field(default_factory=dict)
    name: str | None = None
    disk: str | None = None
    shm: str | None = None
    working_dir: str | None = None
    registry_secret: str | None = None
    spot_policy: str | None = None
    subnet_id: str | None = None
    profile: str | None = None
    project: str | None = None

    def __init__(
        self,
        *,
        image: str,
        command: Sequence[str] | None = None,
        port: int | None = None,
        ports: Sequence[int | str] | None = None,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        pricing: str = "on-demand",
        auth: str = "token",
        token: str | SecretStr | None = None,
        token_secret: str | None = None,
        volumes: Sequence[str] | None = None,
        inject_files: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        name: str | None = None,
        disk: str | None = None,
        shm: str | None = None,
        working_dir: str | None = None,
        registry_secret: str | None = None,
        spot_policy: str | None = None,
        subnet_id: str | None = None,
        profile: str | None = None,
        project: str | None = None,
    ) -> None:
        self._init_fields(
            {
                "image": image,
                "command": command,
                "port": port,
                "ports": ports,
                "gpu": gpu,
                "cpu": cpu,
                "platform": platform,
                "preset": preset,
                "pricing": pricing,
                "auth": auth,
                "token": token,
                "token_secret": token_secret,
                "volumes": volumes,
                "inject_files": inject_files,
                "env": env,
                "secrets": secrets,
                "name": name,
                "disk": disk,
                "shm": shm,
                "working_dir": working_dir,
                "registry_secret": registry_secret,
                "spot_policy": spot_policy,
                "subnet_id": subnet_id,
                "profile": profile,
                "project": project,
            }
        )

    @classmethod
    def _checks(cls) -> Mapping[str, Check]:
        return _CHECKS

    @model_validator(mode="after")
    def _check_combinations(self) -> "EndpointSpec":
        port_specs = validate_ports(self.port, self.ports)
        check_combinations(
            gpu=self.gpu,
            cpu=self.cpu,
            platform=self.platform,
            preset=self.preset,
            pricing=self.pricing,
            spot_policy=self.spot_policy,
            env=self.env,
            secrets=self.secrets,
            operation=_OP,
        )
        validate_token(self.token_text, self.auth)
        validate_token_secret(self.token_secret, self.auth, self.token_text)
        check_token_ports(self.auth, port_specs)
        return self

    @property
    def token_text(self) -> str | None:
        """The bearer token as plain text, or ``None``. Never logged."""
        return None if self.token is None else self.token.get_secret_value()

    def _port_specs(self) -> tuple[PortSpec, ...]:
        """The exposed ports, parsed."""
        return validate_ports(self.port, self.ports)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "EndpointSpec":
        """Load a schema-version-1 endpoint document.

        Relative strings inside the document are not expanded.
        """
        return cls(**load_endpoint_yaml_fields(path))  # type: ignore[arg-type]

    def to_yaml(self, *, redact: bool = True) -> str:
        """The spec as a schema-version-1 YAML document.

        ``from_yaml`` reads it back. ``token`` is never written: store it
        in MysteryBox and set ``token_secret``. ``profile``, ``project``
        and ``inject_files`` are not part of the YAML schema and are left
        out. ``redact=True`` (default) writes env values as
        ``"<hidden>"``; pass ``redact=False`` to write them.
        """
        allowed = ENDPOINT_ALLOWED - {"token"}
        document = self._document("endpoint", allowed, redact)
        return yaml.safe_dump(document, sort_keys=False)


def _token(value: object) -> object:
    """A nonempty string or ``SecretStr``. The value is never echoed."""
    if isinstance(value, SecretStr):
        value = value.get_secret_value()
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        validate_token(value, "token")
    return SecretStr(value)  # type: ignore[arg-type]


def _ports(value: object) -> object:
    if value is None:
        return None
    return tuple(item.label() for item in validate_ports(None, value))


def _port(value: object) -> object:
    if value is None:
        return None
    (item,) = validate_ports(value, None)
    return item.container


_CHECKS: dict[str, Check] = {
    **shared_checks(_OP),
    "command": validate_optional_command,
    "port": _port,
    "ports": _ports,
    "auth": validate_auth,
    "token": _token,
    "token_secret": lambda value: value,
}
