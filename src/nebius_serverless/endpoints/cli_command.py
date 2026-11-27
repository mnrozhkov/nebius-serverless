"""Print the ``nebius ai endpoint create`` command for an endpoint."""

from ..core.cli_flags import common_flags, render


def endpoint_command(
    spec: object,
    *,
    name: str,
    project: str | None,
) -> str:
    """The ``nebius ai endpoint create`` command for an ``EndpointSpec``."""
    flags = common_flags(spec, name=name, project=project)
    for port in spec.ports:  # type: ignore[attr-defined]
        flags.append(("--container-port", port))
    flags.append(("--auth", spec.auth))  # type: ignore[attr-defined]
    notes: list[str] = []
    if spec.token_secret is not None:  # type: ignore[attr-defined]
        flags.append(("--token-secret", spec.token_secret))  # type: ignore[attr-defined]
    elif spec.token is not None:  # type: ignore[attr-defined]
        notes.append(
            "token= is not printed: add --token yourself, or the CLI "
            "generates one."
        )
    return render("nebius ai endpoint create", flags, notes)
