"""ns.Task: inputs, naming, the job request, local_libs, and results."""

import asyncio
import importlib
import inspect
import shutil
import subprocess
import sys
import threading
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fakes import FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.transport import CreateResult
from nebius_serverless.images.image import Image
from nebius_serverless.tasks import run as run_module
from nebius_serverless.tasks import runtime as runner_module
from nebius_serverless.tasks import storage as storage_module
from nebius_serverless.tasks import submit as submit_module
from nebius_serverless.tasks import task as task_module
from nebius_serverless.tasks.storage import Bucket

IMAGE = "python:3.12-slim"
BUCKET = Bucket(
    id="storagebucket-e00abc",
    name="ns-workspace-1",
    endpoint="https://storage.test",
    region="eu-north1",
)
STAMP = "20261124-153922"
FACTOR = 3


class FakeBackend:
    """The task bucket as a folder. The fake job writes into it too."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.puts: list[str] = []
        self.created = False
        # The bucket each run asked for: None is the project default.
        self.chosen: list[str | None] = []

    def ensure_bucket(
        self, project: str, chosen: str | None = None, **_kwargs: object
    ) -> tuple[Bucket, bool]:
        self.chosen.append(chosen)
        if chosen is None:
            return BUCKET, self.created
        bucket = Bucket(
            id=f"storagebucket-{chosen}",
            name=chosen,
            endpoint=BUCKET.endpoint,
            region=BUCKET.region,
            root=storage_module.chosen_root(),
            chosen=True,
        )
        return bucket, False

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        path = self.root / bucket.key(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        self.puts.append(bucket.key(key))

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        path = self.root / bucket.key(key)
        return path.read_bytes() if path.is_file() else None

    def exists(self, bucket: Bucket, key: str) -> bool:
        return (self.root / bucket.key(key)).is_file()


class RunningTransport(FakeTransport):
    """Runs the injected runner or script here when a job is created."""

    def __init__(self, folder: Path) -> None:
        super().__init__()
        self.states = ["COMPLETED"]
        self.folder = folder
        self.run = True
        self.ended: dict[str, str] = {}
        self._guard = threading.Lock()

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str = "",
    ) -> CreateResult:
        with self._guard:
            result = super().create(body, idempotency_key)
            if self.run:
                self._execute(body["spec"])
                self.ended[str(result.resource_id)] = self.states[0]
            return result

    def get(self, job_id: str) -> dict[str, object]:
        record = super().get(job_id)
        if job_id in self.ended:
            record["status"] = {"state": self.ended[job_id]}
        return record

    def _execute(self, spec: object) -> None:
        assert isinstance(spec, dict)
        container = self.folder / f"job-{len(self.bodies)}"
        container.mkdir(parents=True)
        for item in spec["injected_files"]:
            name = Path(item["container_path"]).name
            (container / name).write_bytes(item["content"])
        if spec["container_command"] == "python3":
            command = [sys.executable, str(container / "runner.py")]
        else:
            command = [_bash() or "bash", str(container / "run.sh")]
        done = subprocess.run(command, capture_output=True, env={})
        self.states = ["COMPLETED" if done.returncode == 0 else "FAILED"]


def _bash() -> str | None:
    """A bash with associative arrays (4 or newer), when there is one."""
    found = shutil.which("bash")
    if found is None:
        return None
    major = subprocess.run(
        [found, "-c", "echo ${BASH_VERSINFO[0]}"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return found if major.isdigit() and int(major) >= 4 else None


needs_bash = pytest.mark.skipif(
    _bash() is None, reason="needs bash 4 or newer"
)


@pytest.fixture(autouse=True)
def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    ns.configure(project="project-1", region="eu-north1")
    monkeypatch.setattr(runner_module, "MOUNT", str(tmp_path / "bucket"))
    monkeypatch.setattr(runner_module, "LIBS_DIR", str(tmp_path / "libs"))
    monkeypatch.setattr(run_module, "RESULT_PAUSE", 0.0)
    monkeypatch.setattr(
        submit_module,
        "_now",
        lambda: datetime(2026, 11, 24, 15, 39, 22, tzinfo=timezone.utc),
    )
    submit_module._stamps.clear()
    yield
    ns.configure()


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend(tmp_path / "bucket")
    monkeypatch.setattr(storage_module, "backend", lambda profile: fake)
    return fake


@pytest.fixture
def transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> RunningTransport:
    fake = RunningTransport(tmp_path / "containers")
    monkeypatch.setattr(submit_module, "job_transport", lambda: fake)
    return fake


@pytest.fixture
def package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A local package ``demo_pkg`` that can be imported in this test."""
    root = tmp_path / "src" / "demo_pkg"
    (root / "sub").mkdir(parents=True)
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "junk.pyc").write_bytes(b"junk")
    (root / "__init__.py").write_text("")
    (root / "common.py").write_text("FACTOR = 3\n")
    (root / "mathx.py").write_text(
        "from demo_pkg.common import FACTOR\n\n\n"
        "def triple(x):\n    return _times(x)\n\n\n"
        "def _times(x):\n    return FACTOR * x\n"
    )
    (root / "sub" / "__init__.py").write_text("")
    (root / "sub" / "deep.py").write_text("def one():\n    return 1\n")
    monkeypatch.syspath_prepend(str(root.parent))
    importlib.invalidate_caches()
    yield root
    for name in [key for key in sys.modules if key.startswith("demo_pkg")]:
        del sys.modules[name]


def _libs(backend: FakeBackend) -> list[str]:
    """The library archives written to the bucket."""
    return [key for key in backend.puts if key.startswith("libs/")]


def double(x, times=2):
    return x * times


def boom(x):
    raise ValueError(f"bad {x}")


def uses_helper(x):
    return FACTOR * double(x)


def unpacks(x):
    from demo_pkg.sub.deep import one

    return x + one()


def annotated(x: "Path") -> "CreateResult":
    return x


def wrap(x):
    return {"value": x}


def unwrap(item, extra=0):
    return item["value"] + extra


def total(items, named):
    return sum(item["value"] for item in items) + named["a"]["value"]


def test_exactly_one_of_function_or_command() -> None:
    with pytest.raises(ns.ValidationError, match="exactly one"):
        ns.Task(image=IMAGE)
    with pytest.raises(ns.ValidationError, match="exactly one"):
        ns.Task(double, command="true", image=IMAGE)


def test_a_command_needs_a_name_and_valid_inputs() -> None:
    with pytest.raises(ns.ValidationError, match="needs a name"):
        ns.Task(command="true", image=IMAGE)
    with pytest.raises(ns.ValidationError, match="input name"):
        ns.Task(command="true", name="x", inputs=["a-b"], image=IMAGE)
    with pytest.raises(ns.ValidationError, match="repeat"):
        ns.Task(command="true", name="x", inputs=["a", "a"], image=IMAGE)
    with pytest.raises(ns.ValidationError, match="for a command"):
        ns.Task(double, inputs=["x"], image=IMAGE)


def test_job_options_are_checked_at_construction() -> None:
    with pytest.raises(ns.ValidationError, match="Unknown job option"):
        ns.Task(double, image=IMAGE, gpus="h100")
    with pytest.raises(ns.ValidationError, match="inject_files"):
        ns.Task(double, image=IMAGE, inject_files=["a:/b"])
    with pytest.raises(ns.ValidationError):
        ns.Task(double, image=IMAGE, cpu="2")


def test_a_function_sent_as_source_must_stand_alone() -> None:
    with pytest.raises(ns.ValidationError) as caught:
        ns.Task(uses_helper, image=IMAGE)
    assert "FACTOR, double" in str(caught.value)
    with pytest.raises(ns.ValidationError, match="plain Python function"):
        ns.Task(len, image=IMAGE)
    with pytest.raises(ns.ValidationError, match="def"):
        ns.Task(lambda x: x, image=IMAGE)


def test_names_and_signatures() -> None:
    task = ns.Task(double, image=IMAGE)
    assert task.name == "double"
    assert str(inspect.signature(task)) == "(x, times=2)"
    named = ns.Task(double, name="Train_Baseline v2", image=IMAGE)
    assert named.name == "train-baseline-v2"
    script = ns.Task(
        command="true", name="gpu-check", inputs=["a", "b"], image=IMAGE
    )
    assert str(inspect.signature(script)) == "(*, a, b)"
    with pytest.raises(ns.ValidationError, match="no letters or digits"):
        ns.Task(double, name="---", image=IMAGE)


def test_a_function_runs_and_returns_its_value(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(double, image=IMAGE, timeout="10m")
    assert task(21) == 42
    assert task(x=5, times=3) == 15
    body = transport.bodies[0]
    spec = body["spec"]
    assert isinstance(spec, dict)
    assert body["metadata"] == {
        "parent_id": "project-1",
        "name": f"double-{STAMP}",
    }
    assert spec["image"] == IMAGE
    assert spec["container_command"] == "python3"
    assert spec["timeout_seconds"] == 600
    assert spec["volumes"] == [
        {
            "source": BUCKET.id,
            "container_path": runner_module.MOUNT,
            "mode": "rw",
        }
    ]
    # The second call in the same second gets a counter.
    second = transport.bodies[1]["metadata"]
    assert isinstance(second, dict)
    assert second["name"] == f"double-{STAMP}-2"


def test_a_task_call_submit_and_result_work_inside_a_running_loop(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    """As in a Jupyter cell: the loop is running in this thread."""
    task = ns.Task(double, image=IMAGE, timeout="10m")

    async def cell() -> tuple[object, object]:
        called = task(21)
        run = task.submit(4, times=5)
        return called, run.result()

    assert asyncio.run(cell()) == (42, 20)
    assert len(transport.bodies) == 2


def test_submit_returns_a_run_with_its_inputs_and_folder(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    run = ns.Task(double, image=IMAGE).submit(4, times=5)
    assert run.name == STAMP
    assert run.job.id == "job-1"
    assert run.inputs == {"x": 4, "times": 5}
    key = f"tasks/double/{STAMP}/files"
    assert run.output_path == f"{runner_module.MOUNT}/{key}"
    assert run.output_uri == f"s3://{BUCKET.name}/{key}/"
    assert run.result() == 20
    assert run.result() == 20
    assert transport.gets == ["job-1"]


def test_a_chosen_bucket_keeps_runs_under_its_prefix(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    ns.configure(
        project="project-1", region="eu-north1", bucket="santa-cards-course"
    )
    run = ns.Task(double, image=IMAGE).submit(4, times=5)
    assert backend.chosen == ["santa-cards-course"]
    key = f"ns/tasks/double/{STAMP}"
    assert run.output_path == f"{runner_module.MOUNT}/{key}/files"
    assert run.output_uri == f"s3://santa-cards-course/{key}/files/"
    assert f"{key}/payload.json" in backend.puts
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["volumes"][0]["source"] == "storagebucket-santa-cards-course"
    pointer = next(
        item
        for item in spec["injected_files"]
        if item["container_path"].endswith("pointer.json")
    )
    assert f'"run": "{key}"' in pointer["content"].decode()
    variables = {
        item["name"]: item["value"] for item in spec["environment_variables"]
    }
    assert variables == {
        "NS_BUCKET": "santa-cards-course",
        "NS_BUCKET_PREFIX": "ns",
        "NS_TASK_RUN": f"tasks/double/{STAMP}",
    }
    # The job read its payload and wrote its result under the prefix.
    assert run.result() == 20


def test_the_task_bucket_wins_over_configure(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    ns.configure(project="project-1", region="eu-north1", bucket="shared")
    ns.Task(double, image=IMAGE, bucket="mine").submit(1).result()
    ns.Task(double, image=IMAGE).submit(1).result()
    assert backend.chosen == ["mine", "shared"]


def test_the_default_bucket_adds_only_the_run_variable(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    ns.Task(double, image=IMAGE).submit(1).result()
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["environment_variables"] == [
        {"name": "NS_TASK_RUN", "value": f"tasks/double/{STAMP}"}
    ]
    assert backend.chosen == [None]


def test_tasks_in_one_pipeline_share_one_bucket(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    first = ns.Task(wrap, image=IMAGE, bucket="one").submit(1)
    with pytest.raises(ns.ValidationError, match="share one bucket"):
        ns.Task(unwrap, image=IMAGE, bucket="two").submit(first)
    with pytest.raises(ns.ValidationError, match="default bucket"):
        ns.Task(unwrap, image=IMAGE).submit(item=[first])
    same = ns.Task(unwrap, image=IMAGE, bucket="one").submit(first, extra=1)
    assert same.result() == 2


def test_a_raising_function_fails_with_the_job_traceback(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    with pytest.raises(ns.TaskFailed) as caught:
        ns.Task(boom, image=IMAGE)(7)
    error = caught.value
    assert "raised ValueError: bad 7" in str(error)
    assert error.remote_traceback is not None
    assert "ValueError: bad 7" in error.remote_traceback
    assert error.state == "FAILED"
    assert error.resource_id == "job-1"


def test_bad_arguments_fail_before_a_job_is_created(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(double, image=IMAGE)
    with pytest.raises(ns.ValidationError, match="missing"):
        task()
    with pytest.raises(ns.ValidationError, match="JSON"):
        task(object())
    assert transport.bodies == []


def test_annotations_are_not_sent(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    assert ns.Task(annotated, image=IMAGE)(3) == 3


def test_a_job_without_a_result_fails(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    transport.run = False
    transport.states = ["FAILED"]
    with pytest.raises(ns.TaskFailed, match="ended FAILED without a result"):
        ns.Task(double, image=IMAGE)(1)


def test_a_result_counts_only_from_a_completed_job(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execute = transport._execute

    def then_error(spec: object) -> None:
        execute(spec)
        transport.states = ["ERROR"]

    monkeypatch.setattr(transport, "_execute", then_error)
    with pytest.raises(ns.TaskFailed, match="job ended ERROR") as caught:
        ns.Task(double, image=IMAGE)(1)
    assert caught.value.state == "ERROR"


def test_an_imported_function_is_run_by_name(
    package: Path, backend: FakeBackend, transport: RunningTransport
) -> None:
    triple = importlib.import_module("demo_pkg.mathx").triple
    task = ns.Task(triple, image=IMAGE, local_libs=["demo_pkg"])
    assert task(5) == 15
    assert task(6) == 18
    # The archive is uploaded once and has no cache files.
    assert len(_libs(backend)) == 1
    unpacked = Path(runner_module.LIBS_DIR) / "demo_pkg"
    assert (unpacked / "mathx.py").is_file()
    assert not (unpacked / "__pycache__" / "junk.pyc").exists()


def test_an_edit_uploads_a_new_archive(
    package: Path, backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(unpacks, image=IMAGE, local_libs=["demo_pkg"])
    assert task(1) == 2
    (package / "sub" / "deep.py").write_text("def one():\n    return 10\n")
    assert task(1) == 11
    assert len(_libs(backend)) == 2


def test_a_part_of_a_package_keeps_its_import_name(
    package: Path, backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(unpacks, image=IMAGE, local_libs=["demo_pkg.sub"])
    assert task(1) == 2
    unpacked = Path(runner_module.LIBS_DIR) / "demo_pkg"
    assert (unpacked / "__init__.py").is_file()
    assert (unpacked / "sub" / "deep.py").is_file()
    assert not (unpacked / "mathx.py").exists()


def test_local_libs_takes_local_import_names(package: Path) -> None:
    with pytest.raises(ns.ValidationError, match="import names"):
        ns.Task(double, image=IMAGE, local_libs=["./demo_pkg"])
    with pytest.raises(ns.ValidationError, match="import names"):
        ns.Task(double, image=IMAGE, local_libs="demo_pkg")
    with pytest.raises(ns.ValidationError, match="cannot be imported"):
        ns.Task(double, image=IMAGE, local_libs=["no_such_pkg_xyz"])
    with pytest.raises(ns.ValidationError, match="was not found"):
        ns.Task(double, image=IMAGE, local_libs=["demo_pkg.missing"])
    with pytest.raises(ns.ValidationError, match="installed package"):
        ns.Task(double, image=IMAGE, local_libs=["yaml"])
    with pytest.raises(ns.ValidationError, match="installed package|not a"):
        ns.Task(double, image=IMAGE, local_libs=["json"])


def test_the_script_declares_inputs_and_collects_outputs(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    transport.run = False
    task = ns.Task(
        name="hello",
        image=IMAGE,
        inputs=["who", "count"],
        command="""
        outputs[greeting]="hello ${inputs[who]}"
        """,
    )
    folder = backend.root / "tasks" / "hello" / STAMP
    folder.mkdir(parents=True)
    (folder / "outputs.bin").write_bytes(b"0\0greeting\0hello it's me\0")
    assert task(who="it's me", count=3) == {"greeting": "hello it's me"}
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["container_command"] == "bash"
    script = spec["injected_files"][0]["content"].decode()
    assert "declare -A inputs=([who]='it'\"'\"'s me' [count]=3)" in script
    assert "declare -A outputs=()" in script
    assert "trap __ns_finish EXIT" in script
    assert '\noutputs[greeting]="hello ${inputs[who]}"\n' in script


def test_a_script_takes_exactly_its_inputs(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(command="true", name="s", inputs=["a"], image=IMAGE)
    with pytest.raises(ns.ValidationError, match="Missing: a"):
        task()
    with pytest.raises(ns.ValidationError, match="Unknown: b"):
        task(a=1, b=2)
    with pytest.raises(ns.ValidationError, match="by name"):
        task(1)
    assert transport.bodies == []


def test_a_failing_script_keeps_its_exit_code_and_outputs(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    transport.run = False
    transport.states = ["FAILED"]
    folder = backend.root / "tasks" / "s" / STAMP
    folder.mkdir(parents=True)
    (folder / "outputs.bin").write_bytes(b"3\0partial\0x\0")
    with pytest.raises(ns.TaskFailed, match="exited with code 3") as caught:
        ns.Task(command="exit 3", name="s", image=IMAGE)()
    assert caught.value.exit_code == 3
    assert caught.value.outputs == {"partial": "x"}


@needs_bash
def test_a_script_runs_in_bash(
    package: Path, backend: FakeBackend, transport: RunningTransport
) -> None:
    task = ns.Task(
        name="hello",
        image=IMAGE,
        inputs=["who"],
        local_libs=["demo_pkg"],
        command="""
        outputs[greeting]="hello ${inputs[who]}"
        echo data > "$NS_OUTPUT_DIR/a.txt"
        outputs[libs]=$(ls "${PYTHONPATH%%:*}")
        """,
    )
    run = task.submit(who="a b")
    assert run.result() == {"greeting": "hello a b", "libs": "demo_pkg"}
    assert (Path(run.output_path) / "a.txt").read_text() == "data\n"
    failing = ns.Task(
        name="bad", image=IMAGE, command="outputs[a]=1\nfalse\noutputs[b]=2"
    )
    with pytest.raises(ns.TaskFailed) as caught:
        failing()
    assert caught.value.exit_code == 1
    assert caught.value.outputs == {"a": "1"}


def test_an_image_is_built_once(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builds: list[str] = []

    def build(self: Image, progress: object, **kwargs: object) -> str:
        builds.append(self.tag)
        return f"cr.test/registry/image:{self.tag}"

    monkeypatch.setattr(Image, "_build", build)
    image = ns.Image(dockerfile_string="FROM python:3.12-slim\n")
    task = ns.Task(double, image=image)
    assert task(1) == 2
    assert task(2) == 4
    assert builds == [image.tag]
    spec = transport.bodies[0]["spec"]
    assert isinstance(spec, dict)
    assert spec["image"] == f"cr.test/registry/image:{image.tag}"


def test_ctrl_c_cancels_the_job(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def interrupted(self: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(task_module.TaskRun, "result", interrupted)
    with pytest.raises(KeyboardInterrupt):
        ns.Task(double, image=IMAGE)(1)
    assert transport.cancels == ["job-1"]


def mount_seen() -> str:
    import os

    return os.environ["NS_TASK_MOUNT"]


def test_a_job_knows_where_the_bucket_is_mounted(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    assert ns.Task(mount_seen, image=IMAGE)() == runner_module.MOUNT


def test_in_a_pipeline_a_run_is_kept_under_pipeline_run_task(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = ns.Task(double, image=IMAGE)
    monkeypatch.delenv(runner_module.FOLDER_ENV, raising=False)
    alone = task.submit(1)
    assert alone.output_uri == (
        f"s3://{BUCKET.name}/tasks/double/{STAMP}/files/"
    )
    run_folder = "flows/my-flow/20261125-my-run"
    monkeypatch.setenv(runner_module.FOLDER_ENV, run_folder)
    inside = task.submit(2)
    folder = f"{run_folder}/double-{STAMP}-2"
    assert inside.output_uri == f"s3://{BUCKET.name}/{folder}/files/"
    assert inside.output_path == f"{runner_module.MOUNT}/{folder}/files"
    assert inside.result() == 4
    assert backend.exists(BUCKET, f"{folder}/payload.json")
    assert backend.exists(BUCKET, f"{folder}/result.json")


def test_inside_a_job_the_bucket_is_read_as_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(runner_module.MOUNT_ENV, raising=False)
    outside = storage_module.backend(None)
    assert type(outside) is storage_module.SdkTaskBackend
    monkeypatch.setenv(runner_module.MOUNT_ENV, str(tmp_path))
    inside = storage_module.backend(None)
    assert isinstance(inside, storage_module.MountedTaskBackend)
    assert inside.get(BUCKET, "tasks/a/result.json") is None
    assert not inside.exists(BUCKET, "tasks/a/result.json")
    inside.put(BUCKET, "tasks/a/result.json", b"{}")
    assert inside.get(BUCKET, "tasks/a/result.json") == b"{}"
    assert inside.exists(BUCKET, "tasks/a/result.json")
    assert (tmp_path / "tasks" / "a" / "result.json").read_bytes() == b"{}"


def test_a_task_given_a_run_gets_its_result(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    first = ns.Task(wrap, image=IMAGE).submit(3)
    second = ns.Task(unwrap, image=IMAGE).submit(item=first, extra=1)
    assert second.result() == 4
    assert second.started
    assert second.inputs == {"item": {"value": 3}, "extra": 1}
    assert second.job.id == "job-2"
    assert second.name == STAMP
    # The plain call takes a run too.
    assert ns.Task(unwrap, image=IMAGE)(item=first) == 3


def test_runs_inside_lists_and_dicts_are_replaced(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    wrapping = ns.Task(wrap, image=IMAGE)
    runs = [wrapping.submit(number) for number in (1, 2, 3)]
    summed = ns.Task(total, image=IMAGE).submit(
        items=runs, named={"a": runs[0]}
    )
    assert summed.result() == 7
    assert summed.inputs["items"] == [
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]


def test_a_chain_of_runs(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    wrapping = ns.Task(wrap, image=IMAGE)
    unwrapping = ns.Task(unwrap, image=IMAGE)
    first = unwrapping.submit(item=wrapping.submit(1), extra=1)
    last = unwrapping.submit(item=wrapping.submit(first))
    assert last.result() == 2


def test_a_job_starts_only_when_its_input_has_finished(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    gate = task_module.TaskRun._waiting("gate")
    later = ns.Task(unwrap, image=IMAGE).submit(item=gate)
    # submit returned at once, and nothing was started.
    assert not later.started
    assert repr(later) == "TaskRun('unwrap', waiting for its inputs)"
    assert transport.bodies == []

    gate._started(ns.Task(wrap, image=IMAGE).submit(5))
    assert later.result() == 5
    assert later.started
    assert len(transport.bodies) == 2


def test_names_are_checked_at_once_even_with_a_run(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    first = ns.Task(wrap, image=IMAGE).submit(1)
    with pytest.raises(ns.ValidationError, match="missing a required"):
        ns.Task(unwrap, image=IMAGE).submit(wrong=first)
    with pytest.raises(ns.ValidationError, match="unexpected keyword"):
        ns.Task(unwrap, image=IMAGE).submit(item=first, wrong=first)
    script = ns.Task(command="true", name="s", inputs=["a"], image=IMAGE)
    with pytest.raises(ns.ValidationError, match="Unknown: b"):
        script.submit(a=first, b=first)
    assert len(transport.bodies) == 1


def test_a_failed_input_keeps_the_task_from_starting(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    failed = ns.Task(boom, image=IMAGE).submit(1)
    after = ns.Task(unwrap, image=IMAGE).submit(item=failed)
    with pytest.raises(ns.TaskFailed) as caught:
        after.result()
    message = str(caught.value)
    assert message.startswith(
        "Task unwrap was not started, because one of its inputs failed: "
        "Task boom run"
    )
    assert isinstance(caught.value.__cause__, ns.TaskFailed)
    assert not after.started
    assert repr(after) == "TaskRun('unwrap', not started)"
    # Only the failed input's job was created.
    assert len(transport.bodies) == 1
    # A task that depends on the one that was not started fails too.
    third = ns.Task(wrap, image=IMAGE).submit(after)
    with pytest.raises(ns.TaskFailed, match="was not started"):
        third.result()
    assert len(transport.bodies) == 1


def test_an_input_shared_by_two_tasks_is_waited_for_once(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    first = ns.Task(wrap, image=IMAGE).submit(2)
    unwrapping = ns.Task(unwrap, image=IMAGE)
    one = unwrapping.submit(item=first)
    two = unwrapping.submit(item=first, extra=10)
    assert sorted([one.result(), two.result()]) == [2, 12]
    assert transport.gets.count("job-1") == 1


def test_a_dropped_run_never_starts(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    gate = task_module.TaskRun._waiting("gate")
    later = ns.Task(unwrap, image=IMAGE).submit(item=gate)
    later.cancel()
    with pytest.raises(ns.TaskFailed, match="dropped before it started"):
        later.result()
    gate._started(ns.Task(wrap, image=IMAGE).submit(5))
    for thread in threading.enumerate():
        if thread.name == "ns-task-unwrap":
            thread.join(timeout=10)
    assert len(transport.bodies) == 1


def test_the_bucket_is_named_after_the_project() -> None:
    name = storage_module.bucket_name("project-e00abc")
    assert name == "ns-workspace-e00abc"
