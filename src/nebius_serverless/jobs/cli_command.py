"""Print the ``nebius ai job create`` command a ``JobSpec`` stands for."""

from ..core.cli_flags import common_flags, duration, render


def job_command(
    spec: object,
    *,
    name: str,
    project: str | None,
) -> str:
    """The ``nebius ai job create`` command for a ``JobSpec``."""
    flags = common_flags(spec, name=name, project=project)
    after_image = 1 + next(
        i for i, (flag, _value) in enumerate(flags) if flag == "--image"
    )
    timeout = duration(spec.timeout_seconds)  # type: ignore[attr-defined]
    flags.insert(after_image, ("--timeout", timeout))
    notes: list[str] = []
    restarts = spec.restart_attempts  # type: ignore[attr-defined]
    if restarts == -1:
        flags.append(("--restart-policy", "on-failure"))
    elif restarts > 0:
        notes.append(
            f"restart_attempts={restarts} has no CLI flag: "
            "--restart-policy on-failure restarts without limit."
        )
    return render("nebius ai job create", flags, notes)
