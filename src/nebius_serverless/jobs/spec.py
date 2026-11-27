"""``JobSpec``: what a job runs, as data. It makes no Nebius calls."""

from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import ClassVar, Literal

import yaml
from pydantic import Field, model_validator

from ..core.cli_flags import duration
from ..core.recipe import (
    timeout_seconds,
    validate_command,
    validate_restart_attempts,
)
from ..core.spec_model import (
    Check,
    SpecModel,
    check_combinations,
    shared_checks,
)
from ..errors import ValidationError
from .yaml import ALLOWED, load_yaml_fields

_OP = "JobSpec"

Duration = str | int | float | timedelta


class JobSpec(SpecModel):
    """What one ``nebius ai job create`` runs: plain, frozen data.

    A ``JobSpec`` makes no Nebius calls. ``ns.Job(spec).create()``
    creates a job from it. Use one spec for many jobs, and
    ``spec.clone(...)`` to change fields: a spec cannot be changed in
    place. All arguments are keyword arguments.

    Args:
        command: Nonempty list of strings, e.g. ``["python", "train.py",
            "--epochs", "3"]``. The first item is the container command;
            the rest are POSIX-quoted into one ``args`` string. A shell
            string is rejected; wrap shell syntax as
            ``["bash", "-lc", "a && b"]``.
        image: ``name:tag`` or ``registry/path@sha256:<digest>``, e.g.
            ``"pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime"``. Sent
            unchanged.
        timeout: Required job deadline: ``"30m"``, ``"24h"``, seconds
            (``3600``), or a ``timedelta``, from 5 minutes to 168 hours.
            Stored as whole seconds. It is required so every job has an
            explicit cost ceiling. Nebius checks it asynchronously after
            provisioning, so a job can run briefly past it. A timed-out
            job is returned as data with
            ``status.state_details.code == "TimeoutExceeded"``.
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
        restart_attempts: Container restarts on the same VM after the
            command fails. ``0`` (default) runs once, ``N`` allows up to
            ``N`` restarts after the first run, and ``-1`` restarts
            without limit (CLI ``--restart-policy on-failure``). A
            successful exit is never restarted. Preemption, capacity
            loss, an unexpected VM stop, startup failures, and timeouts
            are never restarted either: those end in ``ERROR``.
        preemption_retries: How many times ``Job.wait()`` creates a
            preempted spot job again. Spot only. ``None`` (default)
            takes the number from ``restart_attempts``.
        volumes: Mount strings ``source:/container/path[:ro|rw]`` or
            ``s3://bucket:/container/path[:ro|rw]`` (no ``:profile``
            suffix), e.g. ``["s3://my-data:/workspace/data:ro"]``. Mode
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
            (``mbsecver-``). The variable gets the payload entry whose
            key matches its name, or the first entry. Names must not
            repeat ``env``.
        name: Console label, sent unchanged on every create. When
            omitted, each ``create`` (and each dry run) generates
            ``job-`` plus 12 hex chars. It is not a lookup key.
        disk: Boot disk size, e.g. ``"500Gi"``. Defaults to ``"250Gi"``.
        shm: ``/dev/shm`` size in the ``disk`` syntax, e.g. ``"64Gi"``,
            or ``"0"``. When omitted, a GPU machine gets ``16Gi`` and a
            CPU machine sends none, as the CLI's ``--shm-size`` does.
        working_dir: Container working directory, e.g. ``"/workspace"``.
            At most 128 characters. Defaults to the image ``WORKDIR``.
        registry_secret: MysteryBox secret version id (``mbsecver-...``)
            for a private image registry. The payload must have
            ``REGISTRY_USERNAME`` and ``REGISTRY_PASSWORD``. Unlike
            ``secrets``, a secret id (``mbsec-``) is rejected: the API
            pins registry credentials to one version.
        spot_policy: Pricing policy id that caps the spot price, e.g.
            ``"pricingpolicy-..."``. Requires ``pricing="spot"``. The
            VM is preempted when the spot price goes above the cap.
        subnet_id: Subnet id, e.g. ``"vpcsubnet-..."``. Only needed
            when the project has more than one subnet.
        profile: Nebius CLI profile name. Defaults to the active profile.
        project: Project id, e.g. ``"project-..."``. Overrides the
            profile's project.

    Example::

        import nebius_serverless as ns

        spec = ns.JobSpec(
            command=["python", "train.py"],
            image="pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime",
            gpu="h100",
            timeout="2h",
        )
        job = ns.Job(spec)
        print(job.dry_run())  # the request, without creating anything
        job.create()
        job.wait()
    """

    _operation: ClassVar[str] = _OP

    command: tuple[str, ...]
    image: str
    gpu: str | None = None
    cpu: str | None = None
    platform: str | None = None
    preset: str | None = None
    timeout: int
    pricing: Literal["on-demand", "spot"] = "on-demand"
    restart_attempts: int = 0
    preemption_retries: int | None = None
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
        command: Sequence[str],
        image: str,
        timeout: Duration,
        gpu: str | None = None,
        cpu: str | None = None,
        platform: str | None = None,
        preset: str | None = None,
        pricing: str = "on-demand",
        restart_attempts: int = 0,
        preemption_retries: int | None = None,
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
                "command": command,
                "image": image,
                "gpu": gpu,
                "cpu": cpu,
                "platform": platform,
                "preset": preset,
                "timeout": timeout,
                "pricing": pricing,
                "restart_attempts": restart_attempts,
                "preemption_retries": preemption_retries,
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
    def _check_combinations(self) -> "JobSpec":
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
        if self.preemption_retries and self.pricing != "spot":
            raise ValidationError(
                "preemption_retries needs pricing='spot': only a spot "
                "job is preempted.",
                operation=_OP,
            )
        return self

    @classmethod
    def from_yaml(cls, path: str | Path) -> "JobSpec":
        """Load a schema-version-1 command document.

        Function documents are rejected. Relative paths inside the
        document are not expanded.
        """
        return cls(**load_yaml_fields(path))  # type: ignore[arg-type]

    def to_yaml(self, *, redact: bool = True) -> str:
        """The spec as a schema-version-1 YAML document.

        ``from_yaml`` reads it back. ``profile``, ``project`` and
        ``inject_files`` are not part of the YAML schema and are left
        out. ``redact=True`` (default) writes env values as
        ``"<hidden>"``; pass ``redact=False`` to write them.
        """
        document = self._document("job", ALLOWED, redact)
        document["timeout"] = duration(self.timeout)
        return yaml.safe_dump(document, sort_keys=False)


def validate_preemption_retries(value: object) -> int | None:
    """``None``, or a count of 0 or more."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(
            "preemption_retries must be a whole number, 0 or more.",
            operation=_OP,
        )
    return value


def whole_seconds(value: object) -> int:
    """A job timeout in whole seconds, between 5 minutes and 168 hours."""
    return int(round(timeout_seconds(value)))


_CHECKS: dict[str, Check] = {
    **shared_checks(_OP),
    "command": lambda value: validate_command(value, operation=_OP),
    "timeout": whole_seconds,
    "restart_attempts": validate_restart_attempts,
    "preemption_retries": validate_preemption_retries,
}
