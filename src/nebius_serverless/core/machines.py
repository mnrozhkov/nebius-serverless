"""Machine selection: GPU/CPU selectors, an explicit platform, or CPU.

``gpu="h100"`` / ``"h100:8"`` and ``cpu="amd:16"`` are sent as the API's
selectors, and Nebius picks the platform and preset in the project's
region. ``platform=`` / ``preset=`` stay as explicit overrides. With none
of them the client sends the smallest CPU machine.
"""

import re
from dataclasses import dataclass

from ..errors import ValidationError

# Public regions, used to read a region out of a profile endpoint.
REGIONS = (
    "eu-north1",
    "eu-north2",
    "eu-south1",
    "eu-west1",
    "eu-west2",
    "me-west1",
    "uk-south1",
    "uk-south2",
    "us-central1",
    "us-north1",
)

CPU_PLATFORM = "cpu-d3"
CPU_PRESET = "2vcpu-8gb"

# The smallest preset of known platforms, used for platform= without
# preset=. Nebius validates both on create.
PRESETS: dict[str, str] = {
    "gpu-h100-sxm": "1gpu-16vcpu-200gb",
    "gpu-h200-sxm": "1gpu-16vcpu-200gb",
    "gpu-b300-sxm": "1gpu-24vcpu-346gb",
    "gpu-b200-sxm": "1gpu-20vcpu-224gb",
    "gpu-b200-sxm-a": "1gpu-20vcpu-224gb",
    "gpu-l40s-a": "1gpu-8vcpu-32gb",
    "gpu-rtx6000": "1gpu-24vcpu-218gb",
    "gpu-rtx6000-a": "1gpu-24vcpu-218gb",
    CPU_PLATFORM: CPU_PRESET,
}

_NAME = r"[A-Za-z0-9][A-Za-z0-9._-]*"
_GPU = re.compile(rf"^({_NAME})(?::([0-9]+))?$")
_CPU = re.compile(rf"^({_NAME}):([0-9]+)$")


@dataclass(frozen=True)
class Machine:
    """What create sends: a selector, or a platform and preset."""

    platform: str | None
    preset: str | None
    gpu: str | None = None
    cpu_selector: str | None = None
    cpu: bool = False


def validate_gpu(gpu: object, *, operation: str = "JobSpec") -> str | None:
    """``MODEL[:COUNT]``, e.g. ``"h100"`` or ``"h100:8"``; count >= 1."""
    if gpu is None:
        return None
    if gpu == "cpu":
        raise ValidationError(
            "gpu='cpu' is invalid. Omit gpu for the default CPU machine, "
            "or pass cpu='amd:16'.",
            operation=operation,
        )
    match = _GPU.fullmatch(gpu) if isinstance(gpu, str) else None
    count = None if match is None else match.group(2)
    if match is None or (count is not None and int(count) < 1):
        raise ValidationError(
            "gpu must be MODEL or MODEL:COUNT with COUNT of at least 1, "
            "such as 'h100' or 'h100:8'.",
            operation=operation,
        )
    return match.group(0)


def validate_cpu(cpu: object, *, operation: str = "JobSpec") -> str | None:
    """``FAMILY:VCPUS``, e.g. ``"amd:16"``; vCPUs >= 1."""
    if cpu is None:
        return None
    match = _CPU.fullmatch(cpu) if isinstance(cpu, str) else None
    if match is None or int(match.group(2)) < 1:
        raise ValidationError(
            "cpu must be FAMILY:VCPUS, such as 'amd:16'.",
            operation=operation,
        )
    return match.group(0)


def check_machine(
    gpu: str | None,
    cpu: str | None,
    platform: str | None,
    preset: str | None,
    *,
    operation: str = "JobSpec",
) -> None:
    """Selectors exclude each other and an explicit platform or preset."""
    if gpu is not None and cpu is not None:
        raise ValidationError(
            "Set gpu or cpu, not both.",
            operation=operation,
        )
    if (gpu is not None or cpu is not None) and (
        platform is not None or preset is not None
    ):
        raise ValidationError(
            "gpu and cpu let Nebius pick the platform and preset. "
            "Omit them to set platform and preset yourself.",
            operation=operation,
        )
    if platform is not None and platform not in PRESETS and not preset:
        raise ValidationError(
            f"Unknown platform {platform!r} requires preset.",
            operation=operation,
        )


def known_cpu(
    gpu: str | None,
    cpu: str | None,
    platform: str | None,
) -> bool:
    """Whether the recipe is known to run on a CPU machine."""
    if cpu is not None:
        return True
    if platform is not None:
        return platform.startswith("cpu")
    return gpu is None


def resolve_machine(
    gpu: str | None,
    cpu: str | None,
    platform: str | None,
    preset: str | None,
) -> Machine:
    """The selector, or the platform and preset, that create sends."""
    if gpu is not None:
        return Machine(None, None, gpu=gpu)
    if cpu is not None:
        return Machine(None, None, cpu_selector=cpu, cpu=True)
    if platform is not None:
        return Machine(
            platform,
            preset or PRESETS[platform],
            cpu=platform.startswith("cpu"),
        )
    return Machine(CPU_PLATFORM, preset or CPU_PRESET, cpu=True)
