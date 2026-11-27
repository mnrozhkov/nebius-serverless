"""Map a job recipe to its dry-run description and its create request."""

from collections.abc import Sequence

from ..core.recipe import (
    DISK_DEFAULT,
    SHM_LABEL,
    describe_extras,
    disk_bytes,
    machine_fields,
    parse_volume,
    quoted_args,
    spec_extras,
)


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
