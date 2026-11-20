"""Validate a recipe and map a job to its create request."""

import re
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path

from .aliases import (
    check_machine,
    known_cpu,
    validate_cpu,
    validate_gpu,
)
from .errors import UnsupportedFeature, ValidationError
from .poll import duration_seconds

# The CLI's documented range. On 19 Nov 2026 the server accepted
# 30m and even 4m (stored as 1800s and 240s), so 1h was too strict.
MIN_TIMEOUT = 5 * 60
MAX_TIMEOUT = 168 * 3600
DISK_DEFAULT = "250Gi"
SHM_LABEL = "16Gi"
INJECT_FILE_MAX_BYTES = 64 * 1024
WORKING_DIR_MAX = 128

_IMAGE = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
    r"|@sha256:[0-9a-f]{64})$"
)
_DISK = re.compile(r"^([0-9]+)(Ki|Mi|Gi|Ti|K|M|G|T)?$")
_S3_VOLUME = re.compile(
    r"^s3://([^/:]+):(/[^:\s]+)(?::(ro|rw)(?::([^:\s]+))?)?$"
)
_MOUNT = re.compile(r"^([^:\s]+):(/[^:\s]+)(?::(ro|rw))?$")
_INJECT_FILE = re.compile(r"^(.+):(/[^:\s]+)$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SECRET_ID_PREFIX = "mbsec-"
_SECRET_VERSION_PREFIX = "mbsecver-"
_UNITS = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}


def validate_command(
    command: object,
    *,
    operation: str = "JobSpec",
) -> tuple[str, ...]:
    """Return a nonempty command list. A shell string is rejected."""
    if isinstance(command, str) or not isinstance(command, Sequence):
        raise ValidationError(
            "command must be a nonempty list of strings, "
            "not a shell string.",
            operation=operation,
        )
    if len(command) == 0:
        raise ValidationError(
            "command must be a nonempty list of strings.",
            operation=operation,
        )
    parts: list[str] = []
    for item in command:
        if not isinstance(item, str) or item == "":
            raise ValidationError(
                "command must be a nonempty list of strings, "
                "not a shell string.",
                operation=operation,
            )
        parts.append(item)
    return tuple(parts)


def quoted_args(command: Sequence[str]) -> str | None:
    """POSIX-quote every argument after the executable."""
    if len(command) < 2:
        return None
    return " ".join(shlex.quote(part) for part in command[1:])


def validate_image(image: object, *, operation: str = "JobSpec") -> str:
    """Accept a tag or a sha256 digest and return it unchanged."""
    if not isinstance(image, str) or _IMAGE.fullmatch(image) is None:
        raise ValidationError(
            "image must be registry/path:tag or "
            "registry/path@sha256:<64 hex>.",
            operation=operation,
        )
    return image


def timeout_seconds(timeout: object) -> int | float:
    """Normalize a duration to seconds. Do not round or clamp."""
    seconds = duration_seconds(timeout)
    if seconds is None:
        raise ValidationError(
            "timeout must be a duration string, seconds, or timedelta.",
            operation="JobSpec",
        )
    if seconds < MIN_TIMEOUT or seconds > MAX_TIMEOUT:
        raise ValidationError(
            "timeout must be between 5 minutes and 168 hours "
            f"({seconds} seconds).",
            operation="JobSpec",
        )
    return seconds


def validate_pricing(pricing: object, *, operation: str = "JobSpec") -> str:
    """Accept on-demand or spot."""
    if pricing not in ("on-demand", "spot"):
        raise ValidationError(
            "pricing must be 'on-demand' or 'spot'.",
            operation=operation,
        )
    return pricing


def reject_spot_on_cpu(
    pricing: str,
    gpu: str | None,
    platform: str | None,
    *,
    cpu: str | None = None,
    operation: str = "JobSpec",
) -> None:
    """Spot on a known CPU machine is outside this slice."""
    if pricing == "spot" and known_cpu(gpu, cpu, platform):
        raise UnsupportedFeature(
            "Spot pricing is not supported on CPU.",
            operation=operation,
        )


def validate_optional_text(
    value: object,
    field: str,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """Return a nonempty string, or None when the field was omitted."""
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        raise ValidationError(
            f"{field} must be a nonempty string.",
            operation=operation,
        )
    return value


def disk_bytes(disk: str, *, operation: str = "JobSpec") -> int:
    """Parse a CLI disk size such as ``500Gi`` into bytes."""
    match = _DISK.fullmatch(disk)
    if match is None:
        raise ValidationError(
            "disk must be an integer size with an optional unit, "
            "such as 500Gi.",
            operation=operation,
        )
    amount = int(match.group(1))
    unit = match.group(2)
    scale = 1 if unit is None else _UNITS[unit]
    return amount * scale


def validate_disk(
    disk: str | None,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """Pass a disk string through after checking it parses."""
    if disk is None:
        return None
    disk_bytes(disk, operation=operation)
    return disk


def validate_shm(
    shm: object,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """A ``/dev/shm`` size in the ``disk`` syntax, e.g. ``64Gi``, or ``0``.

    An integer is taken as bytes.
    """
    if shm is None:
        return None
    if isinstance(shm, int) and not isinstance(shm, bool) and shm >= 0:
        return str(shm)
    if not isinstance(shm, str) or _DISK.fullmatch(shm) is None:
        raise ValidationError(
            "shm must be a size with an optional unit, such as 64Gi, or 0.",
            operation=operation,
        )
    return shm


def resolve_shm(shm: str | None, *, cpu: bool) -> str | None:
    """The shm size to send: the user's, else 16Gi on GPU, else none.

    This matches the CLI's ``--shm-size`` default (16Gi for GPU
    platforms, 0 for CPU).
    """
    if shm is not None:
        return shm
    return None if cpu else SHM_LABEL


def validate_volumes(
    volumes: object,
    *,
    operation: str = "JobSpec",
) -> tuple[str, ...]:
    """Pass volume strings through. Objects are not looked up."""
    if volumes is None:
        return ()
    if isinstance(volumes, str) or not isinstance(volumes, Sequence):
        raise ValidationError(
            "volumes must be a list of mount strings.",
            operation=operation,
        )
    checked: list[str] = []
    for item in volumes:
        if not isinstance(item, str) or not _volume_ok(item):
            raise ValidationError(
                "volumes must be source:container_path[:mode] or "
                "s3://bucket:/container_path[:mode].",
                operation=operation,
            )
        s3_match = _S3_VOLUME.fullmatch(item)
        if s3_match is not None and s3_match.group(4):
            raise UnsupportedFeature(
                "The :profile suffix on s3:// volumes is not supported. "
                "Remove it to mount a bucket in the project.",
                operation=operation,
            )
        checked.append(item)
    return tuple(checked)


def _volume_ok(item: str) -> bool:
    return (
        _S3_VOLUME.fullmatch(item) is not None
        or _MOUNT.fullmatch(item) is not None
    )


def parse_volume(item: str) -> dict[str, str]:
    """Split one volume string into source, path, and mode."""
    s3_match = _S3_VOLUME.fullmatch(item)
    if s3_match is not None:
        return {
            "source": f"s3://{s3_match.group(1)}",
            "container_path": s3_match.group(2),
            "mode": s3_match.group(3) or "rw",
        }
    mount = _MOUNT.fullmatch(item)
    if mount is None:
        raise ValidationError(
            "volumes must be source:container_path[:mode] or "
            "s3://bucket:/container_path[:mode].",
            operation="JobSpec",
        )
    return {
        "source": mount.group(1),
        "container_path": mount.group(2),
        "mode": mount.group(3) or "rw",
    }


def validate_env(
    env: object,
    *,
    operation: str = "JobSpec",
) -> tuple[tuple[str, str], ...]:
    """Copy env pairs. Values stay on the recipe and off the plan."""
    if env is None:
        return ()
    if not isinstance(env, Mapping):
        raise ValidationError(
            "env must be a dict of strings.",
            operation=operation,
        )
    pairs: list[tuple[str, str]] = []
    for key, value in env.items():
        if not isinstance(key, str) or key == "":
            raise ValidationError(
                "env keys must be nonempty strings.",
                operation=operation,
            )
        if not isinstance(value, str):
            raise ValidationError(
                "env values must be strings.",
                operation=operation,
            )
        pairs.append((key, value))
    return tuple(pairs)


def validate_restart_attempts(
    restart_attempts: object,
    *,
    operation: str = "JobSpec",
) -> int:
    """Container restarts after a failure: ``0``, a count, or ``-1``.

    ``-1`` means no limit, as the API and CLI ``on-failure`` define it.
    """
    if (
        isinstance(restart_attempts, bool)
        or not isinstance(restart_attempts, int)
        or restart_attempts < -1
    ):
        raise ValidationError(
            "restart_attempts must be an integer: 0 (run once), a "
            "positive count, or -1 (restart without limit).",
            operation=operation,
        )
    return restart_attempts


def validate_working_dir(
    working_dir: object,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """A nonempty container path of at most 128 characters."""
    value = validate_optional_text(
        working_dir, "working_dir", operation=operation
    )
    if value is not None and len(value) > WORKING_DIR_MAX:
        raise ValidationError(
            f"working_dir must be at most {WORKING_DIR_MAX} characters.",
            operation=operation,
        )
    return value


def validate_secret_ref(
    value: object,
    field: str,
    *,
    operation: str = "JobSpec",
) -> str:
    """Accept a MysteryBox secret id or secret version id."""
    if not isinstance(value, str) or not (
        value.startswith(_SECRET_ID_PREFIX)
        or value.startswith(_SECRET_VERSION_PREFIX)
    ):
        raise ValidationError(
            f"{field} must be a MysteryBox secret id (mbsec-...) "
            "or secret version id (mbsecver-...).",
            operation=operation,
        )
    return value


def validate_registry_secret(
    registry_secret: object,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """A MysteryBox secret version with the registry username and
    password."""
    if registry_secret is None:
        return None
    if not isinstance(registry_secret, str) or not registry_secret.startswith(
        _SECRET_VERSION_PREFIX
    ):
        raise ValidationError(
            "registry_secret must be a MysteryBox secret version id "
            "(mbsecver-...). The API pins registry credentials to one "
            "version. The CLI resolves a name or secret id to a version "
            "for you; this client does not yet.",
            operation=operation,
        )
    return registry_secret


def secret_ref(value: str) -> dict[str, str]:
    """Map a MysteryBox id to the ``MysteryBoxSecretRef`` field it fills."""
    if value.startswith(_SECRET_VERSION_PREFIX):
        return {"version_id": value}
    return {"secret_id": value}


def validate_secrets(
    secrets: object,
    env: Sequence[tuple[str, str]],
    *,
    operation: str = "JobSpec",
) -> tuple[tuple[str, str], ...]:
    """Copy env-name to MysteryBox-id pairs. Names must not repeat env."""
    if secrets is None:
        return ()
    if not isinstance(secrets, Mapping):
        raise ValidationError(
            "secrets must be a dict of env names to MysteryBox ids.",
            operation=operation,
        )
    env_keys = {key for key, _value in env}
    pairs: list[tuple[str, str]] = []
    for key, value in secrets.items():
        if not isinstance(key, str) or _ENV_NAME.fullmatch(key) is None:
            raise ValidationError(
                "secrets keys must be env names such as HF_TOKEN.",
                operation=operation,
            )
        if key in env_keys:
            raise ValidationError(
                f"{key} is set in both env and secrets.",
                operation=operation,
            )
        ref = validate_secret_ref(value, "secrets values", operation=operation)
        pairs.append((key, ref))
    return tuple(pairs)


def validate_spot_policy(
    spot_policy: object,
    pricing: str,
    *,
    operation: str = "JobSpec",
) -> str | None:
    """A pricing policy id caps the spot price. It requires spot."""
    value = validate_optional_text(
        spot_policy, "spot_policy", operation=operation
    )
    if value is not None and pricing != "spot":
        raise ValidationError(
            "spot_policy requires pricing='spot'.",
            operation=operation,
        )
    return value


def describe_extras(
    description: dict[str, object],
    *,
    working_dir: str | None,
    secrets: Sequence[tuple[str, str]],
    registry_secret: str | None,
    spot_policy: str | None,
) -> None:
    """Add the optional fields a dry run shows only when they are set."""
    if working_dir is not None:
        description["working_dir"] = working_dir
    if secrets:
        description["secrets"] = dict(secrets)
    if registry_secret is not None:
        description["registry_secret"] = registry_secret
    if spot_policy is not None:
        description["spot_policy"] = spot_policy


def spec_extras(
    spec: dict[str, object],
    *,
    pricing: str,
    env: Sequence[tuple[str, str]],
    working_dir: str | None,
    secrets: Sequence[tuple[str, str]],
    registry_secret: str | None,
    spot_policy: str | None,
) -> None:
    """Fill env, pricing, and the optional fields shared by jobs and
    endpoints."""
    variables: list[dict[str, object]] = [
        {"name": key, "value": value} for key, value in env
    ]
    variables.extend(
        {"name": key, "mysterybox_secret": secret_ref(ref)}
        for key, ref in secrets
    )
    if variables:
        spec["environment_variables"] = variables
    if working_dir is not None:
        spec["working_dir"] = working_dir
    if registry_secret is not None:
        spec["registry_credentials"] = {
            "mysterybox_secret_version": registry_secret
        }
    if pricing == "spot":
        spec["preemptible"] = True
        if spot_policy is not None:
            spec["spot_pricing_policy"] = spot_policy
        else:
            spec["follows_spot_price"] = True


def check_identity(
    gpu: object,
    platform: str | None,
    preset: str | None,
    *,
    cpu: object = None,
    operation: str = "JobSpec",
) -> tuple[str | None, str | None]:
    """Validate the selectors and their exclusions; return gpu, cpu."""
    gpu_selector = validate_gpu(gpu, operation=operation)
    cpu_selector = validate_cpu(cpu, operation=operation)
    check_machine(
        gpu_selector, cpu_selector, platform, preset, operation=operation
    )
    return gpu_selector, cpu_selector


def machine_fields(
    target: dict[str, object],
    *,
    platform: str | None,
    preset: str | None,
    gpu: str | None,
    cpu: str | None,
    body: bool,
) -> None:
    """Add the selector, or the platform and preset, to ``target``.

    A request body sends selectors as one-entry lists, as the API takes
    them; a dry-run description shows them as strings.
    """
    if gpu is not None:
        target["gpu"] = [gpu] if body else gpu
    elif cpu is not None:
        target["cpu"] = [cpu] if body else cpu
    else:
        target["platform"] = platform
        target["preset"] = preset


def validate_inject_files(
    inject_files: object,
    *,
    operation: str = "JobSpec",
) -> tuple[str, ...]:
    """Pass inject-file strings through. Files are read on create."""
    if inject_files is None:
        return ()
    if isinstance(inject_files, str) or not isinstance(
        inject_files, Sequence
    ):
        raise ValidationError(
            "inject_files must be a list of "
            "local_path:container_path strings.",
            operation=operation,
        )
    items: list[str] = []
    for item in inject_files:
        if not isinstance(item, str) or _INJECT_FILE.fullmatch(item) is None:
            raise ValidationError(
                "inject_files must be local_path:container_path, "
                "matching the CLI --inject-file form.",
                operation=operation,
            )
        items.append(item)
    return tuple(items)


def parse_inject_file(item: str) -> tuple[Path, str]:
    """Split one inject-file string into a local path and container path."""
    match = _INJECT_FILE.fullmatch(item)
    if match is None:
        raise ValidationError(
            "inject_files must be local_path:container_path, "
            "matching the CLI --inject-file form.",
            operation="JobSpec",
        )
    return Path(match.group(1)), match.group(2)


def load_inject_files(
    inject_files: Sequence[str],
) -> list[dict[str, object]]:
    """Read local files for create. Each file must be at most 64 KiB."""
    loaded: list[dict[str, object]] = []
    for item in inject_files:
        local_path, container_path = parse_inject_file(item)
        try:
            content = local_path.read_bytes()
        except OSError as exc:
            raise ValidationError(
                f"Could not read inject file {local_path}.",
                operation="create",
            ) from exc
        if len(content) > INJECT_FILE_MAX_BYTES:
            raise ValidationError(
                "inject_files entries must be at most 64 KiB.",
                operation="create",
            )
        loaded.append(
            {
                "container_path": container_path,
                "content": content,
            }
        )
    return loaded


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
