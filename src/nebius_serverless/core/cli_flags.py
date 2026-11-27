"""The CLI flags shared by ``nebius ai job|endpoint create`` commands.

``FLAGS`` maps each spec field to its CLI flag. The same table is
checked against the CLI and API map in ``docs/reference.md``, so the
printer and the docs cannot drift apart.

The job and endpoint packages add their own flags in ``cli_command``.

Nothing secret is printed: env values become ``"$KEY"`` placeholders,
secrets appear by id, and a token is never written out.
"""

import shlex
from collections.abc import Sequence

from .machines import resolve_machine
from .recipe import DISK_DEFAULT, resolve_shm

FLAGS: dict[str, str] = {
    "command": "--container-command",
    "image": "--image",
    "port": "--container-port",
    "ports": "--container-port",
    "gpu": "--gpu",
    "cpu": "--cpu",
    "platform": "--platform",
    "preset": "--preset",
    "pricing": "--preemptible",
    "spot_policy": "--spot-pricing-policy-id",
    "auth": "--auth",
    "token": "--token",
    "token_secret": "--token-secret",
    "subnet_id": "--subnet-id",
    "volumes": "--volume",
    "inject_files": "--inject-file",
    "env": "--env",
    "secrets": "--env-secret",
    "name": "--name",
    "disk": "--disk-size",
    "shm": "--shm-size",
    "project": "--parent-id",
    "profile": "--profile",
    "working_dir": "--working-dir",
    "registry_secret": "--registry-secret",
    "restart_attempts": "--restart-policy",
    "timeout": "--timeout",
}

Flag = tuple[str, str | None]


def common_flags(
    spec: object, *, name: str, project: str | None
) -> list[Flag]:
    s = spec
    machine = resolve_machine(
        s.gpu, s.cpu, s.platform, s.preset  # type: ignore[attr-defined]
    )
    flags: list[Flag] = []
    if s.profile is not None:  # type: ignore[attr-defined]
        flags.append(("--profile", s.profile))  # type: ignore[attr-defined]
    chosen_project = project or s.project  # type: ignore[attr-defined]
    if chosen_project is not None:
        flags.append(("--parent-id", chosen_project))
    flags.append(("--name", name))
    flags.append(("--image", s.image))  # type: ignore[attr-defined]
    command: Sequence[str] | None = s.command  # type: ignore[attr-defined]
    if command:
        flags.append(("--container-command", command[0]))
        if len(command) > 1:
            flags.append(("--args", shlex.join(command[1:])))
    if machine.gpu is not None:
        flags.append(("--gpu", machine.gpu))
    elif machine.cpu_selector is not None:
        flags.append(("--cpu", machine.cpu_selector))
    else:
        # The CLI's own default is a GPU; say CPU explicitly.
        flags.append(("--platform", machine.platform))
        flags.append(("--preset", machine.preset))
    if s.pricing == "spot":  # type: ignore[attr-defined]
        flags.append(("--preemptible", None))
        if s.spot_policy is not None:  # type: ignore[attr-defined]
            flags.append(("--spot-pricing-policy-id", s.spot_policy))  # type: ignore[attr-defined]
        else:
            flags.append(("--follows-spot-price", None))
    flags.append(("--disk-size", s.disk or DISK_DEFAULT))  # type: ignore[attr-defined]
    shm = resolve_shm(s.shm, cpu=machine.cpu)  # type: ignore[attr-defined]
    if shm is not None:
        flags.append(("--shm-size", shm))
    if s.subnet_id is not None:  # type: ignore[attr-defined]
        flags.append(("--subnet-id", s.subnet_id))  # type: ignore[attr-defined]
    for volume in s.volumes:  # type: ignore[attr-defined]
        flags.append(("--volume", volume))
    for item in s.inject_files:  # type: ignore[attr-defined]
        flags.append(("--inject-file", item))
    for key in s.env:  # type: ignore[attr-defined]
        flags.append(("--env", f'{key}="${key}"'))
    for key, ref in s.secrets.items():  # type: ignore[attr-defined]
        flags.append(("--env-secret", f"{key}={ref}"))
    if s.working_dir is not None:  # type: ignore[attr-defined]
        flags.append(("--working-dir", s.working_dir))  # type: ignore[attr-defined]
    if s.registry_secret is not None:  # type: ignore[attr-defined]
        flags.append(("--registry-secret", s.registry_secret))  # type: ignore[attr-defined]
    return flags


def render(head: str, flags: list[Flag], notes: list[str]) -> str:
    lines = [head]
    for flag, value in flags:
        if value is None:
            lines.append(f"  {flag}")
        elif flag == "--env":
            # KEY="$KEY": the shell expands the value; it is never printed.
            lines.append(f"  {flag} {value}")
        else:
            lines.append(f"  {flag} {shlex.quote(str(value))}")
    text = " \\\n".join(lines)
    for note in notes:
        text += f"\n# {note}"
    return text + "\n"


def duration(seconds: float) -> str:
    whole = int(round(seconds))
    hours, rest = divmod(whole, 3600)
    minutes, secs = divmod(rest, 60)
    text = "".join(
        f"{value}{unit}"
        for value, unit in ((hours, "h"), (minutes, "m"), (secs, "s"))
        if value
    )
    return text or "0s"
