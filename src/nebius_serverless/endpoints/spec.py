"""A local endpoint recipe. Constructing it does not contact Nebius."""

import hashlib
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, overload

from ..core.loop import reject_running_loop
from ..core.machines import resolve_machine
from ..core.profile import resolve_location
from ..core.recipe import (
    check_identity,
    load_inject_files,
    reject_spot_on_cpu,
    resolve_shm,
    validate_disk,
    validate_env,
    validate_image,
    validate_inject_files,
    validate_optional_text,
    validate_pricing,
    validate_registry_secret,
    validate_secrets,
    validate_shm,
    validate_spot_policy,
    validate_volumes,
    validate_working_dir,
)
from ..core.transport import (
    EndpointTransport,
    call_transport,
    validate_idempotency_key,
)
from ..errors import Error, UnsupportedFeature, ValidationError
from ..ui.output import (
    Progress,
    call_progress,
    call_progress_async,
    elapsed_ms,
    log_transport,
    report,
)
from ..ui.status import Status
from .endpoint import Endpoint
from .request import (
    check_token_ports,
    describe_endpoint,
    endpoint_request_body,
    generate_token,
    validate_auth,
    validate_optional_command,
    validate_ports,
    validate_token,
    validate_token_secret,
)
from .yaml import load_endpoint_yaml_fields

_LOG = "nebius_serverless.endpoint_spec"
_OPERATION = "EndpointSpec"


class EndpointSpec:
    """Recipe for one ``nebius ai endpoint create``.

    An endpoint is a container that stays up and answers on a managed
    ``https://`` URL. ``create()`` returns an ``Endpoint``, the cloud
    endpoint with the new id. Constructing an ``EndpointSpec``, loading
    YAML, or importing the package does not authenticate, upload, or
    create.
    Each created endpoint bills for compute while it runs, until
    it is stopped or deleted.

    Args:
        command: Optional list of strings that overrides the image
            entrypoint, e.g. ``["bash", "-c", "vllm serve ..."]``.
            ``None`` (default) keeps the entrypoint. The first item is
            the container command; the rest are POSIX-quoted into one
            ``args`` string. A shell string is rejected.
        image: ``name:tag`` or ``registry/path@sha256:<digest>``, e.g.
            ``"vllm/vllm-openai:v0.19.1"``. Sent unchanged.
        port: Container HTTP port, 1 to 65535, e.g. ``8000``. Served on
            a managed ``https://`` URL (``Endpoint.url``).
        gpu: GPU alias such as ``"h100"``, ``"h200"``, ``"b200"``,
            ``"b300"``, ``"l40s"`` or ``"rtx"``. Picks the smallest
            one-GPU preset in the project region. Omit it for CPU.
        platform: Explicit platform id, e.g. ``"gpu-h100-sxm"``. Overrides
            ``gpu``.
        preset: Explicit preset id, e.g. ``"1gpu-16vcpu-200gb"``. When
            omitted, the smallest preset for the platform is used.
        pricing: ``"on-demand"`` (default) or ``"spot"``. Spot is
            preemptible and not available on CPU.
        auth: ``"token"`` (default): callers must send
            ``Authorization: Bearer <token>``. ``"none"``: a public URL;
            anyone who has it can call the endpoint, and you pay for it.
        token: Bearer token for ``auth="token"``. When omitted, a random
            token is generated on each ``create`` and returned as
            ``Endpoint.token``, and ``create`` prints a notice.
            ``Endpoint.reveal_token()`` fetches it again later. Use
            ``token_secret`` to manage the token in MysteryBox.
            Setting ``token`` with ``auth="none"`` raises.
        subnet_id: Subnet id, e.g. ``"vpcsubnet-..."``. Only needed
            when the project has more than one subnet.
        volumes: Mount strings ``source:/container/path[:ro|rw]`` or
            ``s3://bucket:/container/path[:ro|rw]`` (no ``:profile``
            suffix), e.g.
            ``["s3://my-models:/models:ro"]``. Mode defaults to ``rw``.
        inject_files: ``local_path:/container/path`` strings, e.g.
            ``["./config.yaml:/app/config.yaml"]``. Each file is read on
            ``create`` and must be at most 64 KiB.
        env: Plain environment variables, e.g.
            ``{"WANDB_MODE": "offline"}``. Values are hidden from dry-run
            output and error text. Use ``secrets`` for tokens.
        name: Console label, sent unchanged on every create. When
            omitted, each ``create`` (and each dry run) generates
            ``endpoint-`` plus 12 hex chars, and ``name`` stays
            ``None``. It is not a lookup key.
        disk: Boot disk size, e.g. ``"500Gi"``. Defaults to ``"250Gi"``.
        profile: Nebius CLI profile name. Defaults to the active profile.
        project: Project id, e.g. ``"project-..."``. Overrides the
            profile's project.
        token_secret: MysteryBox secret id or version id whose payload
            has ``AUTH_TOKEN``. Requires ``auth="token"`` and excludes
            ``token``. No token is generated and ``Endpoint.token`` is
            ``None``.
        working_dir: Container working directory, e.g. ``"/workspace"``.
            At most 128 characters. Defaults to the image ``WORKDIR``.
        secrets: Environment variables read from MysteryBox, e.g.
            ``{"HF_TOKEN": "mbsec-..."}``. Each value is a secret id
            (``mbsec-``, latest version) or a version id
            (``mbsecver-``). The variable gets the payload entry whose
            key matches its name, or the first entry. Names must not
            repeat ``env``.
        registry_secret: MysteryBox secret version id (``mbsecver-...``)
            for a private image registry. The payload must have
            ``REGISTRY_USERNAME`` and ``REGISTRY_PASSWORD``. Unlike
            ``secrets``, a secret id (``mbsec-``) is rejected: the API
            pins registry credentials to one version. The CLI resolves a
            name or secret id to a version; this client does not yet.
        spot_policy: Pricing policy id that caps the spot price, e.g.
            ``"pricingpolicy-..."``. Requires ``pricing="spot"``. The
            VM is preempted when the spot price goes above the cap.
        shm: ``/dev/shm`` size in the ``disk`` syntax, e.g. ``"64Gi"``,
            or ``"0"``. When omitted, a GPU machine gets ``16Gi`` and a
            CPU machine sends none, as the CLI's ``--shm-size`` does.

    Example::

        import nebius_serverless as ns

        spec = ns.EndpointSpec(
            command=[
                "bash",
                "-c",
                "python3 -m vllm.entrypoints.openai.api_server "
                "--model Qwen/Qwen3-0.6B --host 0.0.0.0 --port 8000",
            ],
            image="vllm/vllm-openai:v0.19.1",
            port=8000,
            gpu="h100",
            disk="500Gi",
            secrets={"HF_TOKEN": "mbsec-..."},  # read from MysteryBox
            name="qwen-tiny",
        )
        print(spec.create(dry_run=True))  # inspect the request only
        endpoint = spec.create()
        endpoint.wait()
        print(endpoint.url, endpoint.token)
        endpoint.delete()
    """

    def __init__(
        self,
        command: Sequence[str] | None = None,
        *,
        image: str,
        port: int | None = None,
        ports: Sequence[int | str] | None = None,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        pricing: str = "on-demand",
        auth: str = "token",
        token: str | None = None,
        subnet_id: str | None = None,
        volumes: Sequence[str] | None = None,
        inject_files: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        name: str | None = None,
        disk: str | None = None,
        profile: str | None = None,
        project: str | None = None,
        token_secret: str | None = None,
        working_dir: str | None = None,
        secrets: Mapping[str, str] | None = None,
        registry_secret: str | None = None,
        spot_policy: str | None = None,
        shm: str | None = None,
    ) -> None:
        op = _OPERATION
        self.command = validate_optional_command(command)
        self.image = validate_image(image, operation=op)
        self._ports = validate_ports(port, ports)
        self.port = self._ports[0].container if port is not None else None
        self.ports = tuple(item.label() for item in self._ports)
        self.platform = validate_optional_text(
            platform, "platform", operation=op
        )
        self.preset = validate_optional_text(preset, "preset", operation=op)
        self.gpu, self.cpu = check_identity(
            gpu, self.platform, self.preset, cpu=cpu, operation=op
        )
        self.pricing = validate_pricing(pricing, operation=op)
        reject_spot_on_cpu(
            self.pricing,
            self.gpu,
            self.platform,
            cpu=self.cpu,
            operation=op,
        )
        self.auth = validate_auth(auth)
        self.token = validate_token(token, self.auth)
        self.token_secret = validate_token_secret(
            token_secret, self.auth, self.token
        )
        check_token_ports(self.auth, self._ports)
        self.subnet_id = validate_optional_text(
            subnet_id, "subnet_id", operation=op
        )
        self.volumes = validate_volumes(volumes, operation=op)
        self.inject_files = validate_inject_files(inject_files, operation=op)
        self.env = validate_env(env, operation=op)
        supplied = validate_optional_text(name, "name", operation=op)
        self.name = supplied
        self.disk = validate_disk(
            validate_optional_text(disk, "disk", operation=op),
            operation=op,
        )
        self.profile = validate_optional_text(profile, "profile", operation=op)
        self.project = validate_optional_text(project, "project", operation=op)
        self.working_dir = validate_working_dir(working_dir, operation=op)
        self.secrets = validate_secrets(secrets, self.env, operation=op)
        self.registry_secret = validate_registry_secret(
            registry_secret, operation=op
        )
        self.spot_policy = validate_spot_policy(
            spot_policy, self.pricing, operation=op
        )
        self.shm = validate_shm(shm, operation=op)

    def __repr__(self) -> str:
        token = "" if self.token is None else ", token=<hidden>"
        return (
            f"EndpointSpec(image={self.image!r}, ports={self.ports!r}, "
            f"name={self.name!r}, auth={self.auth!r}{token})"
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "EndpointSpec":
        """Load a schema-version-1 endpoint document.

        Relative strings inside the document are not expanded.
        """
        fields = load_endpoint_yaml_fields(path)
        return cls(**fields)  # type: ignore[arg-type]

    def to_cli(self, *, project: str | None = None) -> str:
        """The equivalent ``nebius ai endpoint create`` command.

        Does not contact Nebius. Without ``name=``, a fresh name is
        generated, as ``create()`` does. ``project`` (or the recipe's)
        becomes ``--parent-id``; without one, the CLI uses its profile's
        project. Env values print as ``KEY="$KEY"`` placeholders and
        secrets by id; a token is never printed.
        """
        from .cli_command import endpoint_command

        name = self.name or _name_from_key(str(uuid.uuid4()))
        return endpoint_command(self, name=name, project=project)

    @overload
    def create(
        self,
        dry_run: Literal[False] = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint: ...

    @overload
    def create(
        self,
        dry_run: Literal[True],
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> dict[str, object]: ...

    @overload
    def create(
        self,
        dry_run: bool,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint | dict[str, object]: ...

    def create(
        self,
        dry_run: bool = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint | dict[str, object]:
        """Create a cloud endpoint, or return the request when ``dry_run``.

        A live call returns a ``Endpoint`` once Nebius has accepted the
        endpoint and the id is known. It does not wait for the URL.
        Acceptance is not readiness. A second call starts another
        endpoint. ``quiet=True`` suppresses progress for this call only.
        """
        reject_running_loop("create_async")

        def work(progress: Progress) -> Endpoint | dict[str, object]:
            return self._create(
                dry_run=dry_run,
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return call_progress(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    @overload
    async def create_async(
        self,
        dry_run: Literal[False] = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint: ...

    @overload
    async def create_async(
        self,
        dry_run: Literal[True],
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> dict[str, object]: ...

    @overload
    async def create_async(
        self,
        dry_run: bool,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint | dict[str, object]: ...

    async def create_async(
        self,
        dry_run: bool = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: EndpointTransport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Endpoint | dict[str, object]:
        """Await the same create as ``create``.

        Cancelling this await leaves the endpoint running when create
        was already accepted.
        """

        def work(progress: Progress) -> Endpoint | dict[str, object]:
            return self._create(
                dry_run=dry_run,
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return await call_progress_async(
            quiet=quiet,
            logger_name=_LOG,
            work=work,
            noun="endpoint",
        )

    def _create(
        self,
        *,
        dry_run: bool,
        region: str | None,
        project: str | None,
        transport: EndpointTransport | None,
        progress: Progress,
        idempotency_key: str | None,
    ) -> Endpoint | dict[str, object]:
        project_id, region_id = resolve_location(
            profile=self.profile,
            project=project if project is not None else self.project,
            region=region,
        )
        machine = resolve_machine(
            self.gpu, self.cpu, self.platform, self.preset
        )
        if self.pricing == "spot" and machine.cpu:
            raise UnsupportedFeature(
                "Spot pricing is not supported on CPU.",
                operation="create",
            )
        if (
            idempotency_key is not None
            and self.auth == "token"
            and self.token is None
            and self.token_secret is None
        ):
            raise ValidationError(
                "idempotency_key needs token= or token_secret=: a token "
                "generated in another process differs, so a reconnected "
                "endpoint would not match this one's token.",
                operation="create",
            )
        key = validate_idempotency_key(idempotency_key)
        name = self.name or _name_from_key(key)
        shm = resolve_shm(self.shm, cpu=machine.cpu)
        description = describe_endpoint(
            command=self.command,
            image=self.image,
            ports=self._ports,
            project=project_id,
            region=region_id,
            platform=machine.platform,
            preset=machine.preset,
            gpu=machine.gpu,
            cpu=machine.cpu_selector,
            pricing=self.pricing,
            auth=self.auth,
            token_supplied=self.token is not None,
            subnet_id=self.subnet_id,
            volumes=self.volumes,
            inject_files=self.inject_files,
            env_keys=[key for key, _value in self.env],
            name=name,
            disk=self.disk,
            shm=shm,
            token_secret=self.token_secret,
            working_dir=self.working_dir,
            secrets=self.secrets,
            registry_secret=self.registry_secret,
            spot_policy=self.spot_policy,
        )
        if dry_run:
            return description
        chosen = transport or _sdk_transport(self.profile, project_id)
        token = None
        if self.auth == "token" and self.token_secret is None:
            token = self.token or generate_token()
        body = endpoint_request_body(
            command=self.command,
            image=self.image,
            ports=self._ports,
            project=project_id,
            platform=machine.platform,
            preset=machine.preset,
            gpu=machine.gpu,
            cpu=machine.cpu_selector,
            pricing=self.pricing,
            token=token,
            subnet_id=self.subnet_id,
            volumes=self.volumes,
            inject_files=load_inject_files(self.inject_files),
            env=self.env,
            name=name,
            disk=self.disk,
            shm=shm,
            token_secret=self.token_secret,
            working_dir=self.working_dir,
            secrets=self.secrets,
            registry_secret=self.registry_secret,
            spot_policy=self.spot_policy,
        )
        started = time.perf_counter()
        try:
            result = call_transport(
                chosen, body, noun="endpoint", idempotency_key=key
            )
        except Error as exc:
            log_transport(
                progress,
                job_id=None,
                attempt=1,
                duration_ms=elapsed_ms(started),
                request_id=exc.request_id,
            )
            raise
        duration_ms = elapsed_ms(started)
        endpoint_id = result.resource_id
        log_transport(
            progress,
            job_id=endpoint_id,
            attempt=1,
            duration_ms=duration_ms,
            request_id=result.request_id,
        )
        report(
            Status(
                kind="submitted",
                job_id=endpoint_id,
                name=name,
                state=result.state,
                duration_ms=duration_ms,
                request_id=result.request_id,
                noun="endpoint",
            ),
            progress,
        )
        if token is not None and self.token is None:
            report(
                Status(
                    kind="token_generated",
                    job_id=endpoint_id,
                    noun="endpoint",
                ),
                progress,
            )
        return Endpoint(
            endpoint_id,
            name=name,
            transport=chosen,
            create_state=result.state,
            token=token,
        )


def _sdk_transport(profile: str | None, project: str) -> EndpointTransport:
    from ..core.sdk_transport import SdkEndpointTransport

    return SdkEndpointTransport(profile=profile, project=project)


def _name_from_key(key: str) -> str:
    """The default name: one key, one name, so a same-key retry sends an
    identical body; a fresh key per create gives a fresh name."""
    return "endpoint-" + hashlib.sha256(key.encode()).hexdigest()[:12]
