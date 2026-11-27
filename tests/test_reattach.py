"""Reattach a task run by run id or job id, after the process is gone."""

import base64
import copy
import dataclasses
import json
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fakes import FakeClock, FakeTransport
from test_task import (
    BUCKET,
    IMAGE,
    STAMP,
    FakeBackend,
    RunningTransport,
    double,
    needs_bash,
)

import nebius_serverless as ns
from nebius_serverless.core.transport import CreateResult
from nebius_serverless.jobs.job import PREEMPTED_CODE
from nebius_serverless.tasks import run as run_module
from nebius_serverless.tasks import runtime as runner_module
from nebius_serverless.tasks import storage as storage_module
from nebius_serverless.tasks import submit as submit_module

SPOT = {"gpu": "h100", "pricing": "spot", "preemption_retries": 1}


class Nebius(RunningTransport):
    """Job ids as Nebius gives them, and ``get`` returns the job's spec.

    A create whose number is in ``preempt`` does not run its job: the
    job ends ``ERROR`` with code ``Preempted``. One in ``late_preempt``
    runs, writes its result, and is preempted before it completes. One
    in ``fail`` ends ``FAILED`` without running. One in ``idle`` does
    not run, and stays in ``states``.
    """

    def __init__(self, folder: Path) -> None:
        super().__init__(folder)
        self.preempt: set[int] = set()
        self.late_preempt: set[int] = set()
        self.fail: set[int] = set()
        self.idle: set[int] = set()
        self.base64_files = False
        self.hide_env = False

    def create(
        self, body: Mapping[str, object], idempotency_key: str = ""
    ) -> CreateResult:
        number = len(self.bodies) + 1
        self.run = number not in self.preempt | self.idle | self.fail
        result = super().create(body, idempotency_key)
        new_id = f"aijob-{result.resource_id}"
        state = self.ended.pop(str(result.resource_id), None)
        if number in self.preempt | self.late_preempt:
            state = f"ERROR:{PREEMPTED_CODE}"
        if number in self.fail:
            state = "FAILED"
        if state is not None:
            self.ended[new_id] = state
        return dataclasses.replace(result, resource_id=new_id)

    def get(self, job_id: str) -> dict[str, object]:
        record = FakeTransport.get(self, job_id)
        if job_id in self.ended:
            state, _, code = self.ended[job_id].partition(":")
            status: dict[str, object] = {"state": state}
            if code:
                status["state_details"] = {"code": code}
            record["status"] = status
        number = int(job_id.rsplit("-", 1)[1])
        body = self.bodies[number - 1]
        spec = copy.deepcopy(body["spec"])
        assert isinstance(spec, dict)
        if self.hide_env:
            spec.pop("environment_variables", None)
        if self.base64_files:
            for item in spec.get("injected_files", ()):
                item["content"] = base64.b64encode(item["content"]).decode()
        record["spec"] = spec
        record["metadata"] = {**body["metadata"], "id": job_id}  # type: ignore[dict-item]
        return record


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
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)
    submit_module._stamps.clear()
    yield
    ns.configure()


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend(tmp_path / "bucket")
    monkeypatch.setattr(storage_module, "backend", lambda profile: fake)
    return fake


@pytest.fixture
def nebius(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Nebius:
    fake = Nebius(tmp_path / "containers")
    monkeypatch.setattr(submit_module, "job_transport", lambda: fake)
    return fake


def _record(backend: FakeBackend, run_id: str) -> dict[str, object]:
    data = backend.get(BUCKET, f"{run_id}/run.json")
    assert data is not None
    record = json.loads(data)
    assert isinstance(record, dict)
    return record


def test_a_function_run_is_reattached_by_run_id(
    backend: FakeBackend, nebius: Nebius
) -> None:
    run = ns.Task(double, image=IMAGE).submit(4, times=5)
    run_id = run.id
    assert run_id == f"tasks/double/{STAMP}"
    assert run.uri == f"s3://{BUCKET.name}/{run_id}/"
    del run
    gets_before = list(nebius.gets)
    again = ns.tasks.get(run_id)
    # A run id needs no job read.
    assert nebius.gets == gets_before
    assert (again.id, again.task, again.name) == (run_id, "double", STAMP)
    assert again.inputs == {"x": 4, "times": 5}
    assert again.job.id == "aijob-job-1"
    assert again.result() == 20
    assert len(nebius.bodies) == 1


def test_the_record_is_written_before_the_job_and_lists_it(
    backend: FakeBackend, nebius: Nebius
) -> None:
    written: list[int] = []
    original = backend.put

    def put(bucket: object, key: str, data: bytes) -> None:
        if key.endswith("run.json"):
            written.append(len(nebius.bodies))
        original(bucket, key, data)  # type: ignore[arg-type]

    backend.put = put  # type: ignore[method-assign]
    run = ns.Task(double, image=IMAGE).submit(2)
    assert written == [0, 1]
    record = _record(backend, run.id)
    assert record["schema_version"] == 1
    assert record["id"] == run.id
    assert record["task"] == "double"
    assert record["project"] == "project-1"
    assert record["bucket"] == BUCKET.name
    assert record["result"] == {"item": "result.json", "format": "function"}
    definition = record["definition"]
    assert isinstance(definition, dict)
    assert definition["qualname"] == "double"
    attempts = record["attempts"]
    assert isinstance(attempts, list)
    assert [(a["n"], a["job_id"], a["reason"]) for a in attempts] == [
        (1, "aijob-job-1", "first")
    ]
    # The arguments stay in payload.json.
    assert "args" not in json.dumps(record)


def test_a_function_run_is_reattached_by_job_id(
    backend: FakeBackend, nebius: Nebius
) -> None:
    run = ns.Task(double, image=IMAGE).submit(3)
    job_id = run.job.id
    assert job_id is not None
    again = ns.tasks.get(job_id)
    assert again.id == run.id
    assert again.result() == 6


def test_a_job_id_without_the_variable_uses_the_pointer(
    backend: FakeBackend, nebius: Nebius
) -> None:
    run = ns.Task(double, image=IMAGE).submit(3)
    nebius.hide_env = True
    nebius.base64_files = True
    assert ns.tasks.get(str(run.job.id)).id == run.id


@needs_bash
def test_a_script_run_is_reattached_by_run_id_and_job_id(
    backend: FakeBackend, nebius: Nebius
) -> None:
    hello = ns.Task(
        name="hello",
        image=IMAGE,
        inputs=["who"],
        command='outputs[greeting]="hello ${inputs[who]}"',
    )
    run = hello.submit(who="world")
    by_id = ns.tasks.get(run.id)
    assert by_id.inputs == {"who": "world"}
    assert by_id.result() == {"greeting": "hello world"}
    nebius.hide_env = True
    assert ns.tasks.get(str(run.job.id)).result() == {
        "greeting": "hello world"
    }


def test_any_attempt_reaches_the_latest_result(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.preempt = {1}
    run = ns.Task(double, image=IMAGE, **SPOT).submit(7)
    first = run.job.id
    assert run.result() == 14
    assert run.job.attempts == ["aijob-job-1", "aijob-job-2"]
    record = _record(backend, run.id)
    attempts = record["attempts"]
    assert isinstance(attempts, list)
    assert [(a["job_id"], a["reason"]) for a in attempts] == [
        ("aijob-job-1", "first"),
        ("aijob-job-2", "preempted"),
    ]
    assert first is not None
    for given in (run.id, first):
        again = ns.tasks.get(given)
        assert again.job.id == "aijob-job-2"
        assert again.result() == 14
    assert len(nebius.bodies) == 2


def test_get_never_resubmits_and_result_resubmits_once(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.preempt = {1}
    run_id = ns.Task(double, image=IMAGE, **SPOT).submit(5).id
    # The process that submitted the run is gone.
    again = ns.tasks.get(run_id)
    assert len(nebius.bodies) == 1
    assert again.result() == 10
    assert len(nebius.bodies) == 2
    assert nebius.bodies[1]["metadata"]["name"] == (  # type: ignore[index]
        f"double-{STAMP}-r2"
    )


def test_a_timeout_leaves_the_job_running(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.idle = {1}
    nebius.states = ["RUNNING"]
    run = ns.Task(double, image=IMAGE).submit(1)
    with pytest.raises(ns.WaitTimeout) as caught:
        run.result(timeout=0)
    assert caught.value.resource_id == "aijob-job-1"
    assert run.id in caught.value.recovery
    assert nebius.cancels == []


def test_a_run_in_a_pipeline_folder_reattaches(
    backend: FakeBackend, nebius: Nebius
) -> None:
    folder = "flows/demo/20261124-153922-nightly"
    run = submit_module.start_run(
        ns.Task(double, image=IMAGE), (6,), folder=folder
    )
    assert run.id == f"{folder}/double-{STAMP}"
    assert ns.tasks.get(run.id).result() == 12


def test_a_plain_job_is_not_a_task_run(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.idle = {1}
    job = ns.Job(["true"], image=IMAGE, timeout="10m").create(
        transport=nebius, quiet=True
    )
    with pytest.raises(ns.ValidationError, match="not a task job"):
        ns.tasks.get(str(job.id))


def test_an_unknown_run_id_names_the_run(
    backend: FakeBackend, nebius: Nebius
) -> None:
    with pytest.raises(ns.Error, match="No task run tasks/x/1") as caught:
        ns.tasks.get("tasks/x/1")
    assert "bucket" in caught.value.recovery


def test_a_newer_record_asks_for_an_upgrade(
    backend: FakeBackend, nebius: Nebius
) -> None:
    run = ns.Task(double, image=IMAGE).submit(1)
    record = _record(backend, run.id)
    record["schema_version"] = 2
    backend.put(BUCKET, f"{run.id}/run.json", json.dumps(record).encode())
    with pytest.raises(ns.Error, match="schema version 2") as caught:
        ns.tasks.get(run.id)
    assert "Upgrade" in caught.value.recovery


# Preemption: attempts, retries, and results from earlier attempts.


def boom(x):
    raise ValueError(f"bad {x}")


def resumes(steps):
    import json
    import os

    path = os.path.join(os.environ["NS_OUTPUT_DIR"], "checkpoint.json")
    start = 0
    if os.path.exists(path):
        with open(path) as handle:
            start = json.load(handle)["step"]
    with open(path, "w") as handle:
        json.dump({"step": steps}, handle)
    return {"attempt": int(os.environ["NS_TASK_ATTEMPT"]), "from": start}


def test_a_task_refuses_restart_attempts() -> None:
    with pytest.raises(ns.ValidationError, match="restart_attempts") as caught:
        ns.Task(double, image=IMAGE, restart_attempts=1)
    assert "preemption_retries" in caught.value.recovery


def test_the_second_attempt_returns_its_own_value(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.preempt = {1}
    run = ns.Task(double, image=IMAGE, **SPOT).submit(21)
    assert run.result() == 42
    assert [(a["n"], a["job_id"], a["reason"]) for a in run.attempts] == [
        (1, "aijob-job-1", "first"),
        (2, "aijob-job-2", "preempted"),
    ]
    assert [a["state"] for a in run.attempts] == ["ERROR", "COMPLETED"]
    record = _record(backend, run.id)
    assert [a["state"] for a in record["attempts"]] == [  # type: ignore[union-attr]
        "ERROR",
        "COMPLETED",
    ]


def test_a_result_from_a_preempted_attempt_is_never_returned(
    backend: FakeBackend, nebius: Nebius
) -> None:
    # Attempt 1 finishes the function and writes ok: true, then is
    # preempted. Attempt 2 fails before it writes anything.
    nebius.late_preempt = {1}
    nebius.fail = {2}
    run = ns.Task(double, image=IMAGE, **SPOT).submit(5)
    stale = backend.get(BUCKET, f"{run.id}/result.json")
    assert stale is not None and json.loads(stale)["ok"] is True
    with pytest.raises(ns.TaskFailed, match="ended FAILED without a result"):
        run.result()
    assert len(nebius.bodies) == 2


@needs_bash
def test_a_script_result_from_a_preempted_attempt_is_never_returned(
    backend: FakeBackend, nebius: Nebius
) -> None:
    hello = ns.Task(
        name="hello",
        image=IMAGE,
        inputs=["who"],
        command='outputs[greeting]="hello ${inputs[who]}"',
        **SPOT,
    )
    nebius.late_preempt = {1}
    nebius.fail = {2}
    with pytest.raises(ns.TaskFailed, match="without a result"):
        hello(who="a")
    nebius.late_preempt = {3}
    nebius.fail = set()
    assert hello(who="b") == {"greeting": "hello b"}


def test_a_raising_function_is_not_retried(
    backend: FakeBackend, nebius: Nebius
) -> None:
    with pytest.raises(ns.TaskFailed, match="raised ValueError"):
        ns.Task(boom, image=IMAGE, **SPOT)(1)
    assert len(nebius.bodies) == 1


def test_files_survive_and_the_attempt_number_is_set(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.late_preempt = {1}
    run = ns.Task(resumes, image=IMAGE, **SPOT).submit(500)
    assert run.result() == {"attempt": 2, "from": 500}
    variables = [
        {
            item["name"]: item["value"]
            for item in body["spec"]["environment_variables"]  # type: ignore[index]
        }
        for body in nebius.bodies
    ]
    assert [item["NS_TASK_ATTEMPT"] for item in variables] == ["1", "2"]
    assert {item["NS_TASK_RUN"] for item in variables} == {run.id}


def test_a_used_up_budget_names_the_preemptions_and_the_run(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.preempt = {1, 2}
    run = ns.Task(double, image=IMAGE, **SPOT).submit(1)
    with pytest.raises(ns.TaskFailed) as caught:
        run.result()
    message = str(caught.value)
    assert "preempted 2 times" in message
    assert run.id in message
    assert "budget (1)" in message
    assert len(nebius.bodies) == 2


def test_without_a_budget_a_preemption_says_how_to_retry(
    backend: FakeBackend, nebius: Nebius
) -> None:
    nebius.preempt = {1}
    run = ns.Task(double, image=IMAGE, gpu="h100", pricing="spot").submit(1)
    with pytest.raises(ns.TaskFailed, match="preempted once") as caught:
        run.result()
    assert "preemption_retries=N" in caught.value.recovery
    assert len(nebius.bodies) == 1
