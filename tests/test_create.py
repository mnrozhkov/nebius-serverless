"""Create mapping, lost ids, and profile resolution."""

from pathlib import Path

import pytest
from fakes import FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.sdk_transport import (
    _result_when_id_known,
    build_create_request,
)

CUDA = "nvidia/cuda:13.1.1-runtime-ubuntu24.04"
SECRET = "super-secret-value"


def _job(**kwargs: object) -> ns.JobSpec:
    fields: dict[str, object] = {
        "image": CUDA,
        "gpu": "h100",
        "timeout": "1h",
        "env": {"TOKEN": SECRET},
    }
    fields.update(kwargs)
    fields.setdefault("command", ["nvidia-smi"])
    return ns.JobSpec(**fields)  # type: ignore[arg-type]


def _spec(body: dict[str, object]) -> dict[str, object]:
    spec = body["spec"]
    assert isinstance(spec, dict)
    return spec


def test_nvidia_smi_body_and_a_second_create() -> None:
    fake = FakeTransport()
    recipe = _job(name="my-job", command=["bash", "-c", "echo hello world"])
    run = ns.Job(recipe).create(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    assert isinstance(run, ns.Job)
    assert run.id == "job-1"
    assert run.name == "my-job"
    assert run.status == "PROVISIONING"
    assert run.record is None
    spec = _spec(fake.bodies[0])
    assert spec["image"] == CUDA
    assert spec["container_command"] == "bash"
    assert spec["args"] == "-c 'echo hello world'"
    assert spec["gpu"] == ["h100"]
    assert "platform" not in spec and "preset" not in spec
    assert spec["timeout_seconds"] == 3600
    assert spec["disk"] == "250Gi"
    assert spec["shm_size_bytes"] == 16 * 1024**3
    assert "preemptible" not in spec
    assert "follows_spot_price" not in spec
    metadata = fake.bodies[0]["metadata"]
    assert metadata == {"parent_id": "project-1", "name": "my-job"}
    second = ns.Job(recipe).create(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    assert isinstance(second, ns.Job)
    assert second.id == "job-2"
    assert len(fake.bodies) == 2


def test_spot_flags_and_dry_run_skips_create() -> None:
    fake = FakeTransport()
    job = ns.Job(
        _job(pricing="spot", disk="500Gi", volumes=["data:/work:ro"])
    )
    described = job.dry_run(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    assert isinstance(described, dict)
    assert described["pricing"] == "spot"
    assert described["disk"] == "500Gi"
    assert described["env"] == ["TOKEN"]
    assert SECRET not in str(described)
    assert fake.bodies == []
    job.create(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    spec = _spec(fake.bodies[0])
    assert spec["preemptible"] is True
    assert spec["follows_spot_price"] is True
    assert "pricing_policy" not in str(spec)
    assert spec["disk"] == "500Gi"
    variables = spec["environment_variables"]
    assert variables == [{"name": "TOKEN", "value": SECRET}]
    assert spec["volumes"] == [
        {"source": "data", "container_path": "/work", "mode": "ro"}
    ]


def test_inject_files_read_on_create(tmp_path: Path) -> None:
    local = tmp_path / "config.yaml"
    local.write_text("base_model: demo\n", encoding="utf-8")
    inject = f"{local}:/workspace/config.yaml"
    fake = FakeTransport()
    described = ns.Job(_job(inject_files=[inject])).dry_run(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    assert isinstance(described, dict)
    assert described["inject_files"] == [inject]
    assert fake.bodies == []

    ns.Job(_job(inject_files=[inject], env={})).create(
        region="eu-north1",
        project="project-1",
        transport=fake,
    )
    spec = _spec(fake.bodies[0])
    assert spec["injected_files"] == [
        {
            "container_path": "/workspace/config.yaml",
            "content": b"base_model: demo\n",
        }
    ]

    request = build_create_request(fake.bodies[0])
    payload = request.to_json(preserving_proto_field_name=True)
    assert "injected_files" in payload or "injectedFiles" in payload
    assert "/workspace/config.yaml" in payload

    too_big = tmp_path / "big.bin"
    too_big.write_bytes(b"x" * (64 * 1024 + 1))
    big = _job(inject_files=[f"{too_big}:/workspace/big.bin"], env={})
    with pytest.raises(ns.ValidationError, match="64 KiB"):
        ns.Job(big).create(
            region="eu-north1",
            project="project-1",
            transport=fake,
        )


def test_a_lost_id_is_resent_with_one_key_a_definite_failure_is_not() -> (
    None
):
    lost = FakeTransport()
    lost.drop_id = True
    spec = _job()
    with pytest.raises(ns.SubmissionUncertain) as uncertain:
        ns.Job(spec).create(
            region="eu-north1",
            project="project-1",
            transport=lost,
        )
    assert len(lost.bodies) == 3
    assert lost.bodies[0] == lost.bodies[1] == lost.bodies[2]
    assert len(set(lost.keys)) == 1
    assert uncertain.value.idempotency_key == lost.keys[0]
    failed = FakeTransport()
    failed.failure = "transport"
    with pytest.raises(ns.TransportError) as caught:
        ns.Job(spec).create(
            region="eu-north1",
            project="project-1",
            transport=failed,
        )
    assert len(failed.bodies) == 1
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize(
    ("failure", "error", "conflict_id"),
    [
        ("access", ns.AccessError, None),
        ("capacity", ns.CapacityError, None),
        ("conflict", ns.KeyConflict, "job-existing"),
        ("conflict", ns.SubmissionUncertain, None),
        ("uncertain", ns.SubmissionUncertain, None),
    ],
)
def test_create_errors_hide_env_values(
    failure: str,
    error: type[Exception],
    conflict_id: str | None,
) -> None:
    fake = FakeTransport()
    fake.failure = failure
    fake.conflict_id = conflict_id
    with pytest.raises(error) as caught:
        ns.Job(_job()).create(
            region="eu-north1",
            project="project-1",
            transport=fake,
        )
    assert SECRET not in str(caught.value)
    if conflict_id is not None:
        assert caught.value.resource_id == conflict_id
    # Only an ambiguous outcome is resent, with the same key.
    assert len(fake.bodies) == (3 if failure == "uncertain" else 1)
    assert len(set(fake.keys)) == 1


def test_explicit_location_skips_the_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(profile: str | None = None, **kwargs: object) -> tuple[str, str]:
        raise AssertionError(profile)

    monkeypatch.setattr("nebius_serverless.core.profile.load_profile", boom)
    described = ns.Job(_job()).dry_run(
        region="eu-north1",
        project="project-1",
    )
    assert isinstance(described, dict)
    assert described["region"] == "eu-north1"


def test_missing_profile_fails_before_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(
        profile: str | None = None,
        **kwargs: object,
    ) -> tuple[str, str]:
        raise ns.ValidationError("missing", operation="create")

    monkeypatch.setattr("nebius_serverless.core.profile.load_profile", missing)
    fake = FakeTransport()
    with pytest.raises(ns.ValidationError):
        ns.Job(_job()).create(transport=fake)
    assert fake.bodies == []


def test_profile_file_supplies_project_and_region(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "\n".join(
            [
                "default: test",
                "profiles:",
                "  test:",
                "    parent-id: project-123",
                "    endpoint: api.eu-north1.nebius.cloud",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    from nebius_serverless.core.profile import load_profile

    assert load_profile(config_file=path) == ("project-123", "eu-north1")
    empty = tmp_path / "empty.yaml"
    empty.write_text(
        "default: test\nprofiles:\n  test:\n    endpoint: api.nebius.cloud\n",
        encoding="utf-8",
    )
    with pytest.raises(ns.ValidationError):
        load_profile(config_file=empty)
    ambiguous = tmp_path / "ambiguous.yaml"
    ambiguous.write_text(
        "profiles:\n  a:\n    parent-id: p1\n  b:\n    parent-id: p2\n",
        encoding="utf-8",
    )
    with pytest.raises(ns.ValidationError):
        load_profile(config_file=ambiguous)
    global_endpoint = tmp_path / "global.yaml"
    global_endpoint.write_text(
        "\n".join(
            [
                "default: test",
                "profiles:",
                "  test:",
                "    parent-id: project-123",
                "    endpoint: api.nebius.cloud",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert load_profile(config_file=global_endpoint) == (
        "project-123",
        None,
    )


def _yes() -> bool:
    """Stands in for has_cli_config on a machine with a CLI profile."""
    return True


def test_project_region_fills_a_global_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def load(
        profile: str | None = None,
        **kwargs: object,
    ) -> tuple[str, str | None]:
        return ("project-123", None)

    def region(profile: str | None, project_id: str) -> str:
        assert profile is None
        assert project_id == "project-123"
        return "eu-north1"

    monkeypatch.setattr("nebius_serverless.core.session.has_cli_config", _yes)
    monkeypatch.setattr("nebius_serverless.core.profile.load_profile", load)
    monkeypatch.setattr(
        "nebius_serverless.core.sdk_transport.project_region",
        region,
    )
    described = ns.Job(_job()).dry_run()
    assert isinstance(described, dict)
    assert described["region"] == "eu-north1"
    assert described["project"] == "project-123"
    assert described["gpu"] == "h100"


def test_explicit_region_skips_the_project_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def load(
        profile: str | None = None,
        **kwargs: object,
    ) -> tuple[str, str | None]:
        return ("project-123", None)

    def region(profile: str | None, project_id: str) -> str:
        raise AssertionError(project_id)

    monkeypatch.setattr("nebius_serverless.core.session.has_cli_config", _yes)
    monkeypatch.setattr("nebius_serverless.core.profile.load_profile", load)
    monkeypatch.setattr(
        "nebius_serverless.core.sdk_transport.project_region",
        region,
    )
    described = ns.Job(_job()).dry_run(region="eu-north1")
    assert isinstance(described, dict)
    assert described["region"] == "eu-north1"


def test_sdk_request_omits_on_demand_flags() -> None:
    from nebius_serverless.core.sdk_transport import (
        build_create_request,
        classify_code,
    )
    from nebius_serverless.jobs.request import request_body

    body = request_body(
        command=["nvidia-smi"],
        image=CUDA,
        project="project-1",
        platform="gpu-h100-sxm",
        preset="1gpu-16vcpu-200gb",
        timeout=3600,
        pricing="on-demand",
        subnet_id=None,
        volumes=(),
        inject_files=(),
        env=(("TOKEN", SECRET),),
        name="my-job",
        disk=None,
    )
    request = build_create_request(body)
    payload = request.to_json(preserving_proto_field_name=True)
    assert "preemptible" not in payload
    assert "followsSpotPrice" not in payload
    assert "follows_spot_price" not in payload
    assert "250" in payload or "268435456000" in payload
    assert SECRET in payload
    spot = request_body(
        command=["nvidia-smi"],
        image=CUDA,
        project="project-1",
        platform="gpu-h100-sxm",
        preset="1gpu-16vcpu-200gb",
        timeout=3600,
        pricing="spot",
        subnet_id=None,
        volumes=(),
        inject_files=(),
        env=(),
        name="my-job",
        disk="500Gi",
    )
    spot_request = build_create_request(spot)
    spot_payload = spot_request.to_json(preserving_proto_field_name=True)
    assert "follows_spot_price" in spot_payload or "followsSpotPrice" in (
        spot_payload
    )
    assert "spot_pricing_policy" not in spot_payload
    assert "SpotPricingPolicy" not in spot_payload
    access = classify_code(
        "UNAUTHENTICATED",
        sent=False,
        conflict_id=None,
        capacity=False,
        access=False,
    )
    assert access.failure == "access"
    conflict = classify_code(
        "ALREADY_EXISTS",
        sent=True,
        conflict_id="job-existing",
        capacity=False,
        access=False,
    )
    assert conflict.failure == "conflict"
    assert conflict.conflict_id == "job-existing"
    lost = classify_code(
        "UNAVAILABLE",
        sent=True,
        conflict_id=None,
        capacity=False,
        access=False,
    )
    assert lost.failure == "uncertain"


class _Operation:
    """Create operation that can gain an id before it is terminal."""

    def __init__(self, resource_id: str = "", *, done: bool = False) -> None:
        self.resource_id = resource_id
        self._done = done
        self.updates = 0
        self.waited = False

    def done(self) -> bool:
        return self._done

    def sync_update(self) -> None:
        self.updates += 1
        if self.updates == 1:
            self.resource_id = "aijob-early"

    def sync_wait(self) -> None:
        self.waited = True
        raise AssertionError("create waited for the container")


def test_create_returns_when_the_id_is_already_known() -> None:
    operation = _Operation("aijob-known")
    result = _result_when_id_known(operation)
    assert result.resource_id == "aijob-known"
    assert result.accepted is True
    assert result.failure is None
    assert operation.updates == 0
    assert operation.waited is False


def test_create_polls_until_the_id_and_stops_before_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    monkeypatch.setattr(
        "nebius_serverless.core.sdk_transport.time.sleep",
        slept.append,
    )
    operation = _Operation("")
    result = _result_when_id_known(operation)
    assert result.resource_id == "aijob-early"
    assert operation.updates == 1
    assert operation.waited is False
    assert slept == []


def test_finished_operation_without_an_id_is_uncertain() -> None:
    operation = _Operation("", done=True)
    result = _result_when_id_known(operation)
    assert result.resource_id is None
    assert result.failure == "uncertain"
    assert operation.updates == 0
    assert operation.waited is False
