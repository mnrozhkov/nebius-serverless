"""A preempted spot job is created again while wait() is watching."""

import pytest
from fakes import FakeClock, FakeTransport

import nebius_serverless as ns
from nebius_serverless.jobs.job import PREEMPTED_CODE

WHERE = {"region": "eu-north1", "project": "project-test-0001"}
PREEMPTED = f"ERROR:{PREEMPTED_CODE}"


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(
        "nebius_serverless.core.poll.monotonic", clock.monotonic
    )
    monkeypatch.setattr("nebius_serverless.core.poll.sleep", clock.sleep)


def _create(
    fake: FakeTransport,
    *,
    pricing: str = "spot",
    restart_attempts: int = 2,
    name: str | None = "train",
) -> ns.Job:
    spec = ns.JobSpec(
        command=["python", "train.py"],
        image="pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime",
        gpu="h100",
        timeout="2h",
        pricing=pricing,
        restart_attempts=restart_attempts,
        name=name,
    )
    return ns.Job(spec).create(transport=fake, quiet=True, **WHERE)


def _names(fake: FakeTransport) -> list[str]:
    return [str(body["metadata"]["name"]) for body in fake.bodies]  # type: ignore[index]


def test_a_preempted_spot_job_is_resubmitted(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake = FakeTransport()
    fake.states = ["RUNNING", PREEMPTED, "RUNNING", "COMPLETED"]
    job = _create(fake)
    record = job.wait()
    assert record["status"]["state"] == "COMPLETED"
    assert job.attempts == ["job-1", "job-2"]
    assert (job.id, job.name, job.preemptions) == ("job-2", "train-r2", 1)
    assert _names(fake) == ["train", "train-r2"]
    assert fake.keys[0] != fake.keys[1]
    err = capsys.readouterr().err
    assert "job-1 preempted; resubmitting (1/2)" in err
    assert "Submitted job job-2 train-r2" in err


@pytest.mark.parametrize(
    "end", ["ERROR:NotEnoughResources", "FAILED", "ERROR"]
)
def test_other_failures_are_returned_as_data(end: str) -> None:
    fake = FakeTransport()
    fake.states = ["RUNNING", end]
    job = _create(fake)
    assert job.wait(quiet=True)["status"]["state"] == end.split(":")[0]
    assert job.attempts == ["job-1"]


@pytest.mark.parametrize(
    "kwargs",
    [{"pricing": "on-demand"}, {"restart_attempts": 0}],
)
def test_on_demand_or_no_restarts_never_resubmit(
    kwargs: dict[str, object],
) -> None:
    fake = FakeTransport()
    fake.states = [PREEMPTED]
    job = _create(fake, **kwargs)  # type: ignore[arg-type]
    assert job.wait(quiet=True)["status"]["state"] == "ERROR"
    assert len(fake.bodies) == 1


@pytest.mark.parametrize(("restart_attempts", "cap"), [(2, 2), (-1, 10)])
def test_the_budget_is_enforced(restart_attempts: int, cap: int) -> None:
    fake = FakeTransport()
    fake.states = [PREEMPTED]  # every attempt is preempted
    job = _create(fake, restart_attempts=restart_attempts)
    record = job.wait(quiet=True)
    assert record["status"]["state"] == "ERROR"
    assert job.preemptions == cap
    assert len(fake.bodies) == cap + 1
    assert _names(fake)[-1] == f"train-r{cap + 1}"


def test_a_reloaded_job_is_not_resubmitted() -> None:
    fake = FakeTransport()
    fake.states = [PREEMPTED]
    created = _create(fake)
    reloaded = ns.jobs.get(created.id, transport=fake, quiet=True)
    reloaded.wait(quiet=True)
    assert len(fake.bodies) == 1
