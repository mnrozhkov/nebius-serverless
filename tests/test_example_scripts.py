"""The quickstart and Axolotl example scripts build the documented recipe."""

from __future__ import annotations

import sys
from importlib.machinery import ModuleSpec
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pytest

import nebius_serverless as ns

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"


def _load_module(path: Path) -> ModuleType:
    spec: ModuleSpec | None = spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_quickstart_creates_and_waits(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module(EXAMPLES / "quickstarts" / "job_quickstart.py")
    created: list[ns.JobSpec] = []

    class _FakeQuickRun:
        def __init__(self) -> None:
            self.id = "job-quick-1"
            self.status = "COMPLETED"
            self.record = {"status": {"state": "COMPLETED"}}

        def wait(self) -> dict[str, object]:
            return self.record

        def display(self) -> None:
            print(f"{self.id}\n  {self.status}")

    def fake_create(self: ns.JobSpec) -> _FakeQuickRun:
        created.append(self)
        return _FakeQuickRun()

    monkeypatch.setattr(module.ns.JobSpec, "create", fake_create)
    module.main()
    assert len(created) == 1
    assert created[0].command == ("nvidia-smi",)
    assert created[0].gpu == "h100"
    assert created[0].timeout_seconds == 3600
    out = capsys.readouterr().out
    assert "job-quick-1" in out
    assert "COMPLETED" in out


def test_axolotl_finetune_creates_and_waits(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module(
        EXAMPLES / "training" / "axolotl-finetune" / "finetune.py"
    )
    created: list[ns.JobSpec] = []

    class _FakeTrainRun:
        def __init__(self) -> None:
            self.id = "job-train-1"
            self.status = "COMPLETED"
            self.record = {"status": {"state": "COMPLETED"}}

        def wait(self) -> dict[str, object]:
            return self.record

        def display(self) -> None:
            print(f"{self.id}\n  {self.status}")

    def fake_create(self: ns.JobSpec) -> _FakeTrainRun:
        created.append(self)
        return _FakeTrainRun()

    monkeypatch.setattr(module.ns.JobSpec, "create", fake_create)
    module.main()
    assert len(created) == 1
    job = created[0]
    assert job.command == (
        "bash",
        "-lc",
        "RUN_ID=run-$(date +%Y%m%d-%H%M%S)"
        " && axolotl train /workspace/data/config.yaml"
        " && mkdir -p /workspace/data/output/$RUN_ID"
        " && cp -r /workspace/output/. /workspace/data/output/$RUN_ID",
    )
    assert (
        job.image
        == "docker.io/axolotlai/axolotl:main-20260309-py3.11-cu128-2.9.1"
    )
    assert job.gpu == "h100"
    assert job.pricing == "spot"
    assert job.disk == "500Gi"
    assert job.timeout_seconds == 24 * 3600
    assert job.volumes == ("s3://fine-tuning-axolotl:/workspace/data:rw",)
    out = capsys.readouterr().out
    assert "job-train-1" in out
    assert "COMPLETED" in out


def test_the_cli_tab_is_what_to_cli_prints() -> None:
    """docs show job_quickstart.sh as the CLI form of job_quickstart.py."""
    module = _load_module(EXAMPLES / "quickstarts" / "job_quickstart.py")
    created: list[ns.JobSpec] = []

    def capture(self: ns.JobSpec) -> None:
        created.append(self)
        raise SystemExit("stop before create")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(module.ns.JobSpec, "create", capture)
        with pytest.raises(SystemExit):
            module.main()

    def command(text: str) -> list[str]:
        start = text.index("nebius ai job create")
        lines = text[start:].strip().splitlines()
        return [line for line in lines if "--name" not in line]

    script = (EXAMPLES / "quickstarts" / "job_quickstart.sh").read_text()
    assert command(script) == command(created[0].to_cli())
