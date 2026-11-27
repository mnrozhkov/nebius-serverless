"""Map a ``JobSpec`` to its dry-run description and its create request.

``plan`` resolves where the job goes and on what machine. ``describe``
and ``request_body`` then build the two forms from the spec and the
plan. None of this creates anything.
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

from ..core.machines import Machine, resolve_machine
from ..core.profile import resolve_location
from ..core.recipe import (
    DISK_DEFAULT,
    SHM_LABEL,
    describe_extras,
    disk_bytes,
    load_inject_files,
    machine_fields,
    parse_volume,
    quoted_args,
    resolve_shm,
    spec_extras,
)
from ..errors import UnsupportedFeature
from .spec import JobSpec


@dataclass(frozen=True)
class Plan:
    """Where one create goes and on what machine."""

    project: str
    region: str
    machine: Machine
    shm: str | None


def plan(
    spec: JobSpec, *, region: str | None, project: str | None
) -> Plan:
    """Resolve the project, region, machine and shm for ``spec``.

    Region comes from ``region=`` or the profile when either names one,
    otherwise from the project's ``spec.region``.
    """
    project_id, region_id = resolve_location(
        profile=spec.profile,
        project=project if project is not None else spec.project,
        region=region,
    )
    machine = resolve_machine(spec.gpu, spec.cpu, spec.platform, spec.preset)
    if spec.pricing == "spot" and machine.cpu:
        raise UnsupportedFeature(
            "Spot pricing is not supported on CPU.",
            operation="create",
        )
    return Plan(
        project=project_id,
        region=region_id,
        machine=machine,
        shm=resolve_shm(spec.shm, cpu=machine.cpu),
    )


def spec_description(
    spec: JobSpec, where: Plan, name: str
) -> dict[str, object]:
    """The dry-run description of ``spec``. Env values are left out."""
    machine = where.machine
    return describe(
        command=spec.command,
        image=spec.image,
        project=where.project,
        region=where.region,
        platform=machine.platform,
        preset=machine.preset,
        gpu=machine.gpu,
        cpu=machine.cpu_selector,
        timeout=spec.timeout,
        pricing=spec.pricing,
        subnet_id=spec.subnet_id,
        volumes=spec.volumes,
        inject_files=spec.inject_files,
        env_keys=list(spec.env),
        name=name,
        disk=spec.disk,
        shm=where.shm,
        restart_attempts=spec.restart_attempts,
        working_dir=spec.working_dir,
        secrets=tuple(spec.secrets.items()),
        registry_secret=spec.registry_secret,
        spot_policy=spec.spot_policy,
    )


def spec_body(spec: JobSpec, where: Plan, name: str) -> dict[str, object]:
    """The create request for ``spec``. Reads the inject files."""
    machine = where.machine
    return request_body(
        command=spec.command,
        image=spec.image,
        project=where.project,
        platform=machine.platform,
        preset=machine.preset,
        gpu=machine.gpu,
        cpu=machine.cpu_selector,
        timeout=spec.timeout,
        pricing=spec.pricing,
        subnet_id=spec.subnet_id,
        volumes=spec.volumes,
        inject_files=load_inject_files(spec.inject_files),
        env=tuple(spec.env.items()),
        name=name,
        disk=spec.disk,
        shm=where.shm,
        restart_attempts=spec.restart_attempts,
        working_dir=spec.working_dir,
        secrets=tuple(spec.secrets.items()),
        registry_secret=spec.registry_secret,
        spot_policy=spec.spot_policy,
    )


def name_from_key(key: str) -> str:
    """The default name: one key, one name, so a same-key retry sends an
    identical body; a fresh key per create gives a fresh name."""
    return "job-" + hashlib.sha256(key.encode()).hexdigest()[:12]


def describe(
    *,
    command: Sequence[str],
    image: str,
    project: str,
    region: str,
    platform: str | None,
    preset: str | None,
    timeout: int | float,
    pricing: str,
    subnet_id: str | None,
    volumes: Sequence[str],
    inject_files: Sequence[str],
    env_keys: Sequence[str],
    name: str,
    disk: str | None,
    restart_attempts: int = 0,
    working_dir: str | None = None,
    secrets: Sequence[tuple[str, str]] = (),
    registry_secret: str | None = None,
    spot_policy: str | None = None,
    shm: str | None = SHM_LABEL,
    gpu: str | None = None,
    cpu: str | None = None,
) -> dict[str, object]:
    """Build the dry-run description. Env values are not included."""
    description: dict[str, object] = {
        "image": image,
        "command": list(command),
    }
    args = quoted_args(command)
    if args is not None:
        description["args"] = args
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
    description["timeout"] = timeout
    description["pricing"] = pricing
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
    if restart_attempts:
        description["restart_attempts"] = restart_attempts
    describe_extras(
        description,
        working_dir=working_dir,
        secrets=secrets,
        registry_secret=registry_secret,
        spot_policy=spot_policy,
    )
    return description


def request_body(
    *,
    command: Sequence[str],
    image: str,
    project: str,
    platform: str | None,
    preset: str | None,
    timeout: int | float,
    pricing: str,
    subnet_id: str | None,
    volumes: Sequence[str],
    inject_files: Sequence[dict[str, object]],
    env: Sequence[tuple[str, str]],
    name: str,
    disk: str | None,
    restart_attempts: int = 0,
    working_dir: str | None = None,
    secrets: Sequence[tuple[str, str]] = (),
    registry_secret: str | None = None,
    spot_policy: str | None = None,
    shm: str | None = SHM_LABEL,
    gpu: str | None = None,
    cpu: str | None = None,
) -> dict[str, object]:
    """Map a recipe to the create body the transport sends."""
    spec: dict[str, object] = {
        "image": image,
        "container_command": command[0],
    }
    machine_fields(
        spec, platform=platform, preset=preset, gpu=gpu, cpu=cpu, body=True
    )
    spec |= {
        "timeout_seconds": timeout,
        "disk": disk if disk is not None else DISK_DEFAULT,
    }
    if shm is not None:
        spec["shm_size_bytes"] = disk_bytes(shm)
    args = quoted_args(command)
    if args is not None:
        spec["args"] = args
    if subnet_id is not None:
        spec["subnet_id"] = subnet_id
    if volumes:
        spec["volumes"] = [parse_volume(item) for item in volumes]
    if inject_files:
        spec["injected_files"] = list(inject_files)
    if restart_attempts:
        # 0 and unset are the same request; -1 is sent as -1.
        spec["restart_attempts"] = restart_attempts
    spec_extras(
        spec,
        pricing=pricing,
        env=env,
        working_dir=working_dir,
        secrets=secrets,
        registry_secret=registry_secret,
        spot_policy=spot_policy,
    )
    return {
        "metadata": {"parent_id": project, "name": name},
        "spec": spec,
    }
