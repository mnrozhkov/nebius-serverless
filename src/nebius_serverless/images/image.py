"""``ns.Image``: a Dockerfile built on a Nebius job and pushed to Container
Registry. Constructing one does not contact Nebius."""

import hashlib
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from ..core.profile import resolve_location
from ..core.recipe import INJECT_FILE_MAX_BYTES
from ..core.transport import Transport
from ..errors import BuildFailed, ValidationError
from ..jobs.job import Job
from ..ui.output import Progress, call_progress, call_progress_async, report
from ..ui.status import Status

if TYPE_CHECKING:
    from ..jobs.spec import JobSpec
    from .registry import Registry, SdkImageBackend

_LOG = "nebius_serverless.image"
_OP = "Image"

# The debug tag has a shell, which the build script needs.
KANIKO = "gcr.io/kaniko-project/executor:v1.23.2-debug"

# Writes the registry login for kaniko, then builds and pushes. The
# password arrives from MysteryBox as REGISTRY_PASSWORD. RUN layers are
# cached in <repository>/cache, so a build after a small Dockerfile edit
# reruns only the changed step and the steps below it.
BUILD_SCRIPT = """\
set -eu
mkdir -p /kaniko/.docker
printf '{"auths":{"%s":{"username":"iam","password":"%s"}}}' \\
  "$REGISTRY_HOST" "$REGISTRY_PASSWORD" > /kaniko/.docker/config.json
unset REGISTRY_PASSWORD
exec /kaniko/executor \\
  --context dir:///workspace \\
  --dockerfile /workspace/Dockerfile \\
  --destination "$IMAGE" \\
  --cache=true \\
  --cache-repo "${IMAGE%:*}/cache" \\
  --snapshot-mode=redo \\
  --compressed-caching=false
"""

_COMPONENT = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$")
_DEFAULT_NAME = "image"

Duration = str | int | float | timedelta


class Image:
    """A Dockerfile that ``build()`` turns into an image in the project.

    Pass exactly one of ``dockerfile`` (a path) or ``dockerfile_string``
    (the Dockerfile text). Constructing an ``Image`` does not contact
    Nebius.

    ``build()`` runs kaniko on a CPU serverless job and pushes to the
    project's ``generated-images`` Container Registry, which the first
    build creates. The tag is a hash of the Dockerfile, so a Dockerfile
    that was built once is reused instead of built again. Jobs in the
    same project pull the image without credentials.

    The push uses your own credentials, the ones that create jobs: each
    build stores a short-lived access token as a new version of the
    ``generated-images-push`` MysteryBox secret and deletes that version
    when the build ends. ``RUN`` steps share the build container, so
    build only Dockerfiles you trust.

    The build context is the Dockerfile alone. ``COPY`` of local files is
    not supported yet; ``COPY --from=<image>`` works. Project and region
    come from ``ns.configure`` or the active CLI profile.

    Args:
        dockerfile: Path to a local Dockerfile, at most 64 KiB. The
            repository name is the Dockerfile's folder name.
        dockerfile_string: The Dockerfile text, at most 64 KiB. The
            repository name is ``image``.

    Example::

        import nebius_serverless as ns

        image = ns.Image(dockerfile_string=\"\"\"
        FROM python:3.12-slim
        RUN pip install requests
        \"\"\")
        ref = image.build()  # builds once; later calls reuse the image
        job = ns.Job(
            ["python", "-c", "import requests; print(requests.__version__)"],
            image=ref,
            timeout="10m",
        ).create()
    """

    def __init__(
        self,
        dockerfile: str | Path | None = None,
        *,
        dockerfile_string: str | None = None,
    ) -> None:
        if (dockerfile is None) == (dockerfile_string is None):
            raise ValidationError(
                "Pass exactly one of dockerfile or dockerfile_string.",
                operation=_OP,
            )
        self.dockerfile: Path | None = None
        self.dockerfile_string: str | None = None
        if dockerfile_string is not None:
            if not isinstance(dockerfile_string, str) or (
                dockerfile_string.strip() == ""
            ):
                raise ValidationError(
                    "dockerfile_string must be nonempty Dockerfile text.",
                    operation=_OP,
                )
            self.dockerfile_string = dockerfile_string
            self.name = _DEFAULT_NAME
        else:
            if not isinstance(dockerfile, (str, Path)) or str(dockerfile) in (
                "",
                ".",
            ):
                raise ValidationError(
                    "dockerfile must be a path to a local Dockerfile.",
                    operation=_OP,
                )
            path = Path(dockerfile).expanduser().resolve()
            if not path.is_file():
                raise ValidationError(
                    f"The Dockerfile {str(path)!r} does not exist or is "
                    "not a file.",
                    operation=_OP,
                )
            self.dockerfile = path
            self.name = default_name(path)
        self._content()

    def __repr__(self) -> str:
        source = (
            repr(str(self.dockerfile))
            if self.dockerfile is not None
            else "dockerfile_string=..."
        )
        return f"Image({source}, tag={self.tag!r})"

    @property
    def tag(self) -> str:
        """``sha-`` plus 12 hex chars of the Dockerfile's SHA-256.

        Read from the file each time, so an edited Dockerfile gets a new
        tag and is built again.
        """
        digest = hashlib.sha256(self._content()).hexdigest()
        return f"sha-{digest[:12]}"

    @overload
    def build(
        self,
        dry_run: Literal[False] = False,
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> str: ...

    @overload
    def build(
        self,
        dry_run: Literal[True],
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> dict[str, object]: ...

    def build(
        self,
        dry_run: bool = False,
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> str | dict[str, object]:
        """Make sure the image is in the registry; return its reference.

        The reference is ``<registry host>/<registry>/<name>:<tag>``, for
        ``JobSpec(image=...)``. When the registry already has this tag,
        nothing is built. Otherwise this creates the ``generated-images``
        registry if needed, writes a fresh push token, runs the build
        job, and waits for it. A build that does not end ``COMPLETED``
        raises ``BuildFailed`` with the job id.

        ``timeout`` and ``disk`` are the build job's, as on ``JobSpec``.
        ``dry_run=True`` creates nothing and returns the reference, whether
        it would be reused, and the build job request. ``quiet=True``
        suppresses progress.
        """

        def work(progress: Progress) -> str | dict[str, object]:
            return self._build(
                progress,
                dry_run=dry_run,
                timeout=timeout,
                disk=disk,
                transport=transport,
            )

        return call_progress(quiet=quiet, logger_name=_LOG, work=work)

    @overload
    async def build_async(
        self,
        dry_run: Literal[False] = False,
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> str: ...

    @overload
    async def build_async(
        self,
        dry_run: Literal[True],
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> dict[str, object]: ...

    async def build_async(
        self,
        dry_run: bool = False,
        *,
        timeout: Duration = "1h",
        disk: str | None = None,
        quiet: bool = False,
        transport: Transport | None = None,
    ) -> str | dict[str, object]:
        """Await the same build as ``build``.

        Cancelling this await does not cancel a running build job.
        """

        def work(progress: Progress) -> str | dict[str, object]:
            return self._build(
                progress,
                dry_run=dry_run,
                timeout=timeout,
                disk=disk,
                transport=transport,
            )

        return await call_progress_async(
            quiet=quiet, logger_name=_LOG, work=work
        )

    def _build(
        self,
        progress: Progress,
        *,
        dry_run: bool,
        timeout: Duration,
        disk: str | None,
        transport: Transport | None,
    ) -> str | dict[str, object]:
        from .registry import PUSH_SECRET_NAME, REGISTRY_NAME

        content = self._content()
        tag = _tag(content)
        project_id, region_id = resolve_location(
            profile=None, project=None, region=None
        )
        backend = _backend()
        if dry_run:
            found = backend.find_registry(project_id)
            built = found is not None and backend.has_image(
                found, self.name, tag
            )
            if found is None:
                ref = f"<{REGISTRY_NAME}>/{self.name}:{tag}"
                host = "<created on build>"
            else:
                ref, host = self._ref(found, tag), found.host
            with _staged(content) as path:
                spec = self._spec(
                    path,
                    ref,
                    host,
                    "mbsecver-created-on-build",
                    tag,
                    timeout,
                    disk,
                )
                request = Job(spec).dry_run(
                    region=region_id, project=project_id
                )
            return {
                "image": ref,
                "reuse": built,
                "registry": {
                    "name": REGISTRY_NAME,
                    "id": found.id if found else None,
                    "created_on_build": found is None,
                },
                "push_secret": PUSH_SECRET_NAME,
                "job": None if built else request,
            }
        registry, created = backend.ensure_registry(project_id)
        if created:
            _say(
                progress,
                f"Created registry {REGISTRY_NAME} ({registry.id}).",
            )
        ref = self._ref(registry, tag)
        if not created and backend.has_image(registry, self.name, tag):
            _say(progress, f"Reusing {ref}.")
            return ref
        version = backend.push_secret_version(project_id)
        _say(progress, f"Building {ref}.")
        try:
            with _staged(content) as path:
                spec = self._spec(
                    path, ref, registry.host, version, tag, timeout, disk
                )
                job = Job(spec)._create(
                    region=region_id,
                    project=project_id,
                    transport=transport,
                    progress=progress,
                    idempotency_key=None,
                )
            record = job._wait(None, progress)
        finally:
            backend.delete_secret_version(version)
        state = _state(record)
        if state != "COMPLETED":
            raise BuildFailed(
                f"Build job {job.id} for {ref} ended {state}.",
                operation="build",
                project=project_id,
                resource_id=job.id,
                state=state,
                recovery=f"See the build log: nebius ai job logs {job.id}",
            )
        _say(progress, f"Pushed {ref}.")
        return ref

    def _ref(self, registry: "Registry", tag: str) -> str:
        return f"{registry.path}/{self.name}:{tag}"

    def _spec(
        self,
        dockerfile: Path,
        ref: str,
        host: str,
        secret: str,
        tag: str,
        timeout: Duration,
        disk: str | None,
    ) -> "JobSpec":
        from ..jobs.spec import JobSpec

        return JobSpec(
            command=["/busybox/sh", "-c", BUILD_SCRIPT],
            image=KANIKO,
            timeout=timeout,
            disk=disk,
            inject_files=[f"{dockerfile}:/workspace/Dockerfile"],
            env={"IMAGE": ref, "REGISTRY_HOST": host},
            secrets={"REGISTRY_PASSWORD": secret},
            name=_job_name(self.name, tag),
        )

    def _content(self) -> bytes:
        if self.dockerfile_string is not None:
            data = self.dockerfile_string.encode()
        else:
            assert self.dockerfile is not None
            try:
                data = self.dockerfile.read_bytes()
            except OSError:
                raise ValidationError(
                    f"The Dockerfile {str(self.dockerfile)!r} cannot be read.",
                    operation=_OP,
                ) from None
        if len(data) > INJECT_FILE_MAX_BYTES:
            raise ValidationError(
                "The Dockerfile must be at most 64 KiB.",
                operation=_OP,
            )
        return data


@contextmanager
def _staged(content: bytes) -> Iterator[Path]:
    """The exact bytes that were hashed, as a file the build job injects.

    A Dockerfile on disk could change between hashing and the create, so
    it is copied to a temporary file that lives until the create returns.
    """
    with tempfile.TemporaryDirectory(prefix="ns-image-") as folder:
        path = Path(folder) / "Dockerfile"
        path.write_bytes(content)
        yield path


def default_name(dockerfile: Path) -> str:
    """The Dockerfile's folder name as a registry repository name."""
    text = dockerfile.parent.name.lower()
    text = re.sub(r"[^a-z0-9._-]+", "-", text)
    text = re.sub(r"[._-]*([._-])[._-]*", r"\1", text).strip("._-")
    return text if _COMPONENT.fullmatch(text) else _DEFAULT_NAME


def _job_name(name: str, tag: str) -> str:
    """``build-<name>-<tag>`` with a plain, bounded name part."""
    plain = re.sub(r"[^a-z0-9]+", "-", name).strip("-")[:40].strip("-")
    return f"build-{plain or _DEFAULT_NAME}-{tag}"


def _tag(content: bytes) -> str:
    return f"sha-{hashlib.sha256(content).hexdigest()[:12]}"


def _state(record: dict[str, object]) -> str:
    status = record.get("status")
    state = status.get("state") if isinstance(status, dict) else None
    return state if isinstance(state, str) else "UNKNOWN"


def _say(progress: Progress, message: str) -> None:
    report(Status(kind="image", message=message), progress)


def _backend() -> "SdkImageBackend":
    from .registry import open_backend

    return open_backend(None)
