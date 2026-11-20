"""A local job recipe. Constructing it does not contact Nebius."""

import hashlib
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Literal, overload

from .aliases import resolve_machine
from .errors import Error, UnsupportedFeature
from .job import Job, resubmit_budget
from .loop import reject_running_loop
from .output import (
    Progress,
    call_progress,
    call_progress_async,
    elapsed_ms,
    log_transport,
    report,
)
from .profile import resolve_location
from .recipe import (
    check_identity,
    describe,
    load_inject_files,
    reject_spot_on_cpu,
    request_body,
    resolve_shm,
    timeout_seconds,
    validate_command,
    validate_disk,
    validate_env,
    validate_image,
    validate_inject_files,
    validate_optional_text,
    validate_pricing,
    validate_registry_secret,
    validate_restart_attempts,
    validate_secrets,
    validate_shm,
    validate_spot_policy,
    validate_volumes,
    validate_working_dir,
)
from .status import Status
from .transport import Transport, call_transport, validate_idempotency_key
from .yaml_job import load_yaml_fields

_LOG = "nebius_serverless.job_spec"


class JobSpec:
    """Recipe for one ``nebius ai job create``.

    ``create()`` returns a ``Job``, the cloud job with the new id. One
    recipe can create many jobs. Constructing a ``JobSpec``, loading
    YAML, or importing the package does not authenticate, upload, or
    create. The Nebius SDK is imported on the call that needs a profile
    or a live create.

    Args:
        command: Nonempty list of strings, e.g. ``["python", "train.py",
            "--epochs", "3"]``. The first item is the container command;
            the rest are POSIX-quoted into one ``args`` string. A shell
            string is rejected; wrap shell syntax as
            ``["bash", "-lc", "a && b"]``.
        image: ``name:tag`` or ``registry/path@sha256:<digest>``, e.g.
            ``"pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime"``. Sent
            unchanged.
        gpu: GPU alias such as ``"h100"``, ``"h200"``, ``"b200"``,
            ``"b300"``, ``"l40s"`` or ``"rtx"``. Picks the smallest
            one-GPU preset in the project region. Omit it for CPU.
        platform: Explicit platform id, e.g. ``"gpu-h100-sxm"``.
            Overrides ``gpu``.
        preset: Explicit preset id, e.g. ``"1gpu-16vcpu-200gb"``. When
            omitted, the smallest preset for the platform is used.
        timeout: Required job deadline: ``"30m"``, ``"24h"``, seconds
            (``3600``), or a ``timedelta``, from 5 minutes to 168 hours.
            It is required so every job has an explicit cost ceiling.
            Nebius checks it asynchronously after provisioning, so a job
            can run briefly past it. A timed-out job is returned as data
            with ``status.state_details.code == "TimeoutExceeded"``.
        pricing: ``"on-demand"`` (default) or ``"spot"``. Spot is
            preemptible and not available on CPU.
        subnet_id: Subnet id, e.g. ``"vpcsubnet-..."``. Only needed
            when the project has more than one subnet.
        volumes: Mount strings ``source:/container/path[:ro|rw]`` or
            ``s3://bucket:/container/path[:ro|rw]`` (no ``:profile``
            suffix), e.g.
            ``["s3://my-data:/workspace/data:ro"]``. Mode defaults to
            ``rw``.
        inject_files: ``local_path:/container/path`` strings, e.g.
            ``["./config.yaml:/app/config.yaml"]``. Each file is read on
            ``create`` and must be at most 64 KiB.
        env: Plain environment variables, e.g.
            ``{"WANDB_MODE": "offline"}``. Values are hidden from dry-run
            output and error text. Use ``secrets`` for tokens.
        name: Console label, sent unchanged on every create. When
            omitted, each ``create`` (and each dry run) generates
            ``job-`` plus 12 hex chars, and ``name`` stays ``None``. It
            is not a lookup key.
        disk: Boot disk size, e.g. ``"500Gi"``. Defaults to ``"250Gi"``.
        profile: Nebius CLI profile name. Defaults to the active profile.
        project: Project id, e.g. ``"project-..."``. Overrides the
            profile's project.
        restart_attempts: Container restarts on the same VM after the
            command fails. ``0`` (default) runs once, ``N`` allows up to
            ``N`` restarts after the first run, and ``-1`` restarts
            without limit (CLI ``--restart-policy on-failure``). A
            successful exit is never restarted. Preemption, capacity
            loss, an unexpected VM stop, startup failures, and timeouts
            are never restarted either: those end in ``ERROR``. For
            preemption recovery, use on-demand pricing or resubmit from
            your own loop.
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

        spec = ns.JobSpec(
            ["bash", "-lc", "axolotl train /workspace/data/config.yaml"],
            image="docker.io/axolotlai/axolotl:main-20260309-py3.11-cu128-2.9.1",
            gpu="h100",
            pricing="spot",
            restart_attempts=2,  # after a crash, not preemption
            timeout="24h",
            disk="500Gi",
            volumes=[
                "s3://axolotl-data:/workspace/data:ro",
                "s3://axolotl-artifacts:/workspace/artifacts:rw",
            ],
            env={"WANDB_MODE": "offline"},
            secrets={"HF_TOKEN": "mbsec-..."},  # read from MysteryBox
            name="axolotl-lora",
        )
        print(spec.create(dry_run=True))  # inspect the request only
        job = spec.create()
        job.wait()
        job.display()
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        image: str,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        timeout: str | int | float | timedelta,
        pricing: str = "on-demand",
        subnet_id: str | None = None,
        volumes: Sequence[str] | None = None,
        inject_files: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        name: str | None = None,
        disk: str | None = None,
        profile: str | None = None,
        project: str | None = None,
        restart_attempts: int = 0,
        working_dir: str | None = None,
        secrets: Mapping[str, str] | None = None,
        registry_secret: str | None = None,
        spot_policy: str | None = None,
        shm: str | None = None,
    ) -> None:
        self.command = validate_command(command)
        self.image = validate_image(image)
        self.platform = validate_optional_text(platform, "platform")
        self.preset = validate_optional_text(preset, "preset")
        self.gpu, self.cpu = check_identity(
            gpu, self.platform, self.preset, cpu=cpu
        )
        self.timeout_seconds = timeout_seconds(timeout)
        self.pricing = validate_pricing(pricing)
        reject_spot_on_cpu(
            self.pricing, self.gpu, self.platform, cpu=self.cpu
        )
        self.subnet_id = validate_optional_text(subnet_id, "subnet_id")
        self.volumes = validate_volumes(volumes)
        self.inject_files = validate_inject_files(inject_files)
        self.env = validate_env(env)
        supplied = validate_optional_text(name, "name")
        self.name = supplied
        self.disk = validate_disk(validate_optional_text(disk, "disk"))
        self.profile = validate_optional_text(profile, "profile")
        self.project = validate_optional_text(project, "project")
        self.restart_attempts = validate_restart_attempts(
            restart_attempts
        )
        self.working_dir = validate_working_dir(working_dir)
        self.secrets = validate_secrets(secrets, self.env)
        self.registry_secret = validate_registry_secret(registry_secret)
        self.spot_policy = validate_spot_policy(spot_policy, self.pricing)
        self.shm = validate_shm(shm)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "JobSpec":
        """Load a schema-version-1 command document.

        Function documents are rejected. Relative paths inside the
        document are not expanded.
        """
        fields = load_yaml_fields(path)
        return cls(**fields)  # type: ignore[arg-type]

    def to_cli(self, *, project: str | None = None) -> str:
        """The equivalent ``nebius ai job create`` command.

        Does not contact Nebius. Without ``name=``, a fresh name is
        generated, as ``create()`` does. ``project`` (or the recipe's)
        becomes ``--parent-id``; without one, the CLI uses its profile's
        project. Env values print as ``KEY="$KEY"`` placeholders and
        secrets by id; a token is never printed.
        """
        from .cli import job_command

        name = self.name or _name_from_key(str(uuid.uuid4()))
        return job_command(self, name=name, project=project)

    @overload
    def create(
        self,
        dry_run: Literal[False] = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job: ...

    @overload
    def create(
        self,
        dry_run: Literal[True],
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
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
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job | dict[str, object]: ...

    def create(
        self,
        dry_run: bool = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job | dict[str, object]:
        """Create a cloud job, or return the request when ``dry_run``.

        ``dry_run=True`` resolves project and region and does not call
        create. Region comes from ``region=`` or the profile when either
        names one, otherwise from the project's ``spec.region``. A live
        call returns a ``Job`` after the id is known and does not wait
        for the container to finish. A second call starts another cloud
        job. The live call prints one line once the id is known.
        ``quiet=True`` suppresses that line for this call only.
        ``dry_run=True`` stays silent. ``quiet`` is not sent to Nebius.
        """
        reject_running_loop("create_async")

        def work(progress: Progress) -> Job | dict[str, object]:
            return self._create(
                dry_run=dry_run,
                region=region,
                project=project,
                transport=transport,
                progress=progress,
                idempotency_key=idempotency_key,
            )

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    @overload
    async def create_async(
        self,
        dry_run: Literal[False] = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job: ...

    @overload
    async def create_async(
        self,
        dry_run: Literal[True],
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
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
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job | dict[str, object]: ...

    async def create_async(
        self,
        dry_run: bool = False,
        *,
        region: str | None = None,
        project: str | None = None,
        transport: Transport | None = None,
        quiet: bool = False,
        idempotency_key: str | None = None,
    ) -> Job | dict[str, object]:
        """Await the same create as ``create``.

        Cancelling this await does not cancel the cloud job.
        ``quiet=True`` suppresses progress for this call only.
        """

        def work(progress: Progress) -> Job | dict[str, object]:
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
        )

    def _create(
        self,
        *,
        dry_run: bool,
        region: str | None,
        project: str | None,
        transport: Transport | None,
        progress: Progress,
        idempotency_key: str | None,
    ) -> Job | dict[str, object]:
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
        key = validate_idempotency_key(idempotency_key)
        name = self.name or _name_from_key(key)
        shm = resolve_shm(self.shm, cpu=machine.cpu)
        description = describe(
            command=self.command,
            image=self.image,
            project=project_id,
            region=region_id,
            platform=machine.platform,
            preset=machine.preset,
            gpu=machine.gpu,
            cpu=machine.cpu_selector,
            timeout=self.timeout_seconds,
            pricing=self.pricing,
            subnet_id=self.subnet_id,
            volumes=self.volumes,
            inject_files=self.inject_files,
            env_keys=[key for key, _value in self.env],
            name=name,
            disk=self.disk,
            shm=shm,
            restart_attempts=self.restart_attempts,
            working_dir=self.working_dir,
            secrets=self.secrets,
            registry_secret=self.registry_secret,
            spot_policy=self.spot_policy,
        )
        if dry_run:
            return description
        chosen = transport or _sdk_transport(self.profile, project_id)

        def submit(
            attempt_name: str,
            attempt_key: str,
            handle: Progress,
        ) -> tuple[str, str | None]:
            body = request_body(
                command=self.command,
                image=self.image,
                project=project_id,
                platform=machine.platform,
                preset=machine.preset,
                gpu=machine.gpu,
                cpu=machine.cpu_selector,
                timeout=self.timeout_seconds,
                pricing=self.pricing,
                subnet_id=self.subnet_id,
                volumes=self.volumes,
                inject_files=load_inject_files(self.inject_files),
                env=self.env,
                name=attempt_name,
                disk=self.disk,
                shm=shm,
                restart_attempts=self.restart_attempts,
                working_dir=self.working_dir,
                secrets=self.secrets,
                registry_secret=self.registry_secret,
                spot_policy=self.spot_policy,
            )
            started = time.perf_counter()
            try:
                result = call_transport(
                    chosen, body, idempotency_key=attempt_key
                )
            except Error as exc:
                log_transport(
                    handle,
                    job_id=None,
                    attempt=1,
                    duration_ms=elapsed_ms(started),
                    request_id=exc.request_id,
                )
                raise
            duration_ms = elapsed_ms(started)
            log_transport(
                handle,
                job_id=result.resource_id,
                attempt=1,
                duration_ms=duration_ms,
                request_id=result.request_id,
            )
            report(
                Status(
                    kind="submitted",
                    job_id=result.resource_id,
                    name=attempt_name,
                    state=result.state,
                    duration_ms=duration_ms,
                    request_id=result.request_id,
                ),
                handle,
            )
            return result.resource_id, result.state

        job_id, state = submit(name, key, progress)
        budget = resubmit_budget(self.pricing, self.restart_attempts)
        return Job(
            job_id,
            name=name,
            transport=chosen,
            create_state=state,
            resubmit=submit if budget else None,
            resubmit_budget=budget,
        )


def _sdk_transport(profile: str | None, project: str) -> Transport:
    from .sdk_transport import SdkTransport

    return SdkTransport(profile=profile, project=project)


def _name_from_key(key: str) -> str:
    """The default name: one key, one name, so a same-key retry sends an
    identical body; a fresh key per create gives a fresh name."""
    return "job-" + hashlib.sha256(key.encode()).hexdigest()[:12]
