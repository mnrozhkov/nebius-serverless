"""ns.Image: inputs, the build job request, reuse, and the push token."""

import asyncio
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from fakes import FakeTransport

import nebius_serverless as ns
from nebius_serverless.images.image import BUILD_SCRIPT, KANIKO, default_name
from nebius_serverless.images.registry import Registry

DOCKERFILE = "FROM python:3.12-slim\nRUN pip install requests\n"
EDITED = DOCKERFILE + "RUN true\n"
HOST = "cr.eu-north1.nebius.cloud"
REGISTRY = Registry(id="registry-e00abc", host=HOST)


def _tag(text: str = DOCKERFILE) -> str:
    return "sha-" + hashlib.sha256(text.encode()).hexdigest()[:12]


class FakeBackend:
    """Registry, image and secret calls, recorded. Holds no token."""

    def __init__(self) -> None:
        self.registry: Registry | None = REGISTRY
        self.images: set[tuple[str, str]] = set()
        self.created: list[str] = []
        self.versions: list[str] = []
        self.deleted: list[str] = []

    def find_registry(self, project: str) -> Registry | None:
        return self.registry

    def ensure_registry(self, project: str) -> tuple[Registry, bool]:
        if self.registry is not None:
            return self.registry, False
        self.created.append(project)
        self.registry = REGISTRY
        return self.registry, True

    def has_image(self, registry: Registry, name: str, tag: str) -> bool:
        return (name, tag) in self.images

    def push_secret_version(self, project: str) -> str:
        version = f"mbsecver-{len(self.versions) + 1}"
        self.versions.append(version)
        return version

    def delete_secret_version(self, version_id: str) -> None:
        self.deleted.append(version_id)


@pytest.fixture(autouse=True)
def _location() -> Iterator[None]:
    ns.configure(project="project-1", region="eu-north1")
    yield
    ns.configure()


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend()
    monkeypatch.setattr(
        "nebius_serverless.images.image._backend", lambda: fake
    )
    return fake


@pytest.fixture
def transport() -> FakeTransport:
    fake = FakeTransport()
    fake.states = ["COMPLETED"]
    return fake


def _dockerfile(tmp_path: Path, text: str = DOCKERFILE) -> Path:
    folder = tmp_path / "Torch_Images"
    folder.mkdir(exist_ok=True)
    path = folder / "Dockerfile"
    path.write_text(text)
    return path


def test_a_path_names_the_repository_after_its_folder(
    tmp_path: Path,
) -> None:
    path = _dockerfile(tmp_path)
    image = ns.Image(path)
    assert image.name == "torch_images"
    assert image.tag == _tag()
    path.write_text(EDITED)
    assert image.tag == _tag(EDITED)


def test_a_string_is_named_image_and_tagged_by_content() -> None:
    image = ns.Image(dockerfile_string=DOCKERFILE)
    assert image.name == "image"
    assert image.tag == _tag()
    assert ns.Image(dockerfile_string=DOCKERFILE).tag == image.tag


@pytest.mark.parametrize(
    ("folder", "expected"),
    [
        ("My Images", "my-images"),
        ("--weird..name__", "weird.name"),
        ("...", "image"),
    ],
)
def test_default_name_is_a_valid_repository(
    folder: str, expected: str
) -> None:
    assert default_name(Path("/x") / folder / "Dockerfile") == expected


def test_a_dockerfile_at_the_root_is_named_image() -> None:
    assert default_name(Path("/Dockerfile")) == "image"


def test_exactly_one_source(tmp_path: Path) -> None:
    with pytest.raises(ns.ValidationError, match="exactly one"):
        ns.Image()
    with pytest.raises(ns.ValidationError, match="exactly one"):
        ns.Image(_dockerfile(tmp_path), dockerfile_string=DOCKERFILE)


def test_construction_rejects_bad_input(tmp_path: Path) -> None:
    with pytest.raises(ns.ValidationError, match="does not exist"):
        ns.Image(tmp_path / "missing")
    with pytest.raises(ns.ValidationError, match="nonempty"):
        ns.Image(dockerfile_string="  \n")
    with pytest.raises(ns.ValidationError, match="64 KiB"):
        ns.Image(dockerfile_string="#" * (64 * 1024 + 1))
    with pytest.raises(ns.ValidationError, match="64 KiB"):
        ns.Image(_dockerfile(tmp_path, "#" * (64 * 1024 + 1)))


def test_build_runs_kaniko_and_returns_the_reference(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    ref = ns.Image(dockerfile_string=DOCKERFILE).build(
        transport=transport, quiet=True
    )
    assert ref == f"{HOST}/e00abc/image:{_tag()}"
    body = transport.bodies[0]
    spec = body["spec"]
    assert isinstance(spec, dict)
    assert spec["image"] == KANIKO
    assert spec["container_command"] == "/busybox/sh"
    assert "--destination" in BUILD_SCRIPT
    assert '--cache-repo "${IMAGE%:*}/cache"' in BUILD_SCRIPT
    assert "gpu" not in spec
    assert spec["timeout_seconds"] == 3600
    env = {item["name"]: item for item in spec["environment_variables"]}
    assert env["IMAGE"]["value"] == ref
    assert env["REGISTRY_HOST"]["value"] == HOST
    assert env["REGISTRY_PASSWORD"]["mysterybox_secret"] == {
        "version_id": "mbsecver-1"
    }
    assert body["metadata"] == {
        "parent_id": "project-1",
        "name": f"build-image-{_tag()}",
    }
    assert backend.deleted == ["mbsecver-1"]


def test_the_build_injects_the_hashed_dockerfile(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    ns.Image(dockerfile_string=DOCKERFILE).build(
        transport=transport, quiet=True
    )
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["injected_files"] == [
        {
            "container_path": "/workspace/Dockerfile",
            "content": DOCKERFILE.encode(),
        }
    ]


def test_an_image_built_once_is_reused(
    tmp_path: Path,
    backend: FakeBackend,
    transport: FakeTransport,
    capsys: pytest.CaptureFixture[str],
) -> None:
    backend.images.add(("torch_images", _tag()))
    ref = ns.Image(_dockerfile(tmp_path)).build(transport=transport)
    assert ref == f"{HOST}/e00abc/torch_images:{_tag()}"
    assert transport.bodies == []
    assert backend.versions == []
    assert f"Reusing {ref}." in capsys.readouterr().err


def test_the_job_name_is_plain_and_bounded(
    tmp_path: Path, backend: FakeBackend, transport: FakeTransport
) -> None:
    folder = tmp_path / ("My_Long.Folder-" + "x" * 80)
    folder.mkdir()
    (folder / "Dockerfile").write_text(DOCKERFILE)
    ns.Image(folder / "Dockerfile").build(transport=transport, quiet=True)
    metadata = transport.bodies[0]["metadata"]
    assert isinstance(metadata, dict)
    name = str(metadata["name"])
    assert name.startswith("build-my-long-folder-x")
    assert name.endswith(f"-{_tag()}")
    assert len(name) <= len("build-") + 40 + 1 + len(_tag())


def test_an_edited_dockerfile_is_built_again(
    tmp_path: Path, backend: FakeBackend, transport: FakeTransport
) -> None:
    backend.images.add(("torch_images", _tag()))
    path = _dockerfile(tmp_path)
    image = ns.Image(path)
    path.write_text(EDITED)
    ref = image.build(transport=transport, quiet=True)
    assert ref.endswith(_tag(EDITED))
    assert len(transport.bodies) == 1


def test_first_build_creates_the_registry(
    backend: FakeBackend,
    transport: FakeTransport,
    capsys: pytest.CaptureFixture[str],
) -> None:
    backend.registry = None
    ns.Image(dockerfile_string=DOCKERFILE).build(transport=transport)
    assert backend.created == ["project-1"]
    err = capsys.readouterr().err
    assert "Created registry generated-images (registry-e00abc)." in err
    assert "Pushed " in err
    assert "mbsecver" not in err


def test_failed_build_raises_and_still_deletes_the_token(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    transport.states = ["FAILED"]
    with pytest.raises(ns.BuildFailed) as info:
        ns.Image(dockerfile_string=DOCKERFILE).build(
            transport=transport, quiet=True
        )
    assert info.value.resource_id == "job-1"
    assert info.value.state == "FAILED"
    assert "nebius ai job logs job-1" in (info.value.recovery or "")
    assert backend.deleted == ["mbsecver-1"]


def test_rejected_create_deletes_the_token(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    transport.failure = "access"
    with pytest.raises(ns.AccessError):
        ns.Image(dockerfile_string=DOCKERFILE).build(
            transport=transport, quiet=True
        )
    assert backend.deleted == ["mbsecver-1"]


def test_build_options_reach_the_job(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    ns.Image(dockerfile_string=DOCKERFILE).build(
        timeout="2h", disk="500Gi", transport=transport, quiet=True
    )
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["timeout_seconds"] == 7200
    assert spec["disk"] == "500Gi"


def test_dry_run_creates_nothing(backend: FakeBackend) -> None:
    backend.registry = None
    described = ns.Image(dockerfile_string=DOCKERFILE).build(dry_run=True)
    assert described["image"] == f"<generated-images>/image:{_tag()}"
    assert described["reuse"] is False
    assert described["registry"] == {
        "name": "generated-images",
        "id": None,
        "created_on_build": True,
    }
    assert isinstance(described["job"], dict)
    assert backend.created == []
    assert backend.versions == []


def test_dry_run_reports_reuse(backend: FakeBackend) -> None:
    backend.images.add(("image", _tag()))
    described = ns.Image(dockerfile_string=DOCKERFILE).build(dry_run=True)
    assert described["reuse"] is True
    assert described["job"] is None


def test_build_async(backend: FakeBackend, transport: FakeTransport) -> None:
    image = ns.Image(dockerfile_string=DOCKERFILE)
    ref = asyncio.run(image.build_async(transport=transport, quiet=True))
    assert ref == f"{HOST}/e00abc/image:{_tag()}"


def test_sync_build_works_inside_a_running_loop(
    backend: FakeBackend, transport: FakeTransport
) -> None:
    image = ns.Image(dockerfile_string=DOCKERFILE)

    async def main() -> str:
        return image.build(transport=transport, quiet=True)

    ref = asyncio.run(main())
    assert ref == f"{HOST}/e00abc/image:{_tag()}"
    assert len(transport.bodies) == 1
