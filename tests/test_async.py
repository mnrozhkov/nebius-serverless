"""Awaited twins, and sync calls inside a running event loop."""

import _thread
import asyncio
import threading
import time

import pytest
from fakes import FakeTransport

import nebius_serverless as ns


def _spec() -> ns.JobSpec:
    return ns.JobSpec(
        command=["nvidia-smi"],
        image="python:3.12-slim",
        gpu="h100",
        timeout="1h",
    )


def _job() -> ns.Job:
    return ns.Job(_spec())


def test_a_dry_run_inside_a_loop_matches_one_outside() -> None:
    spec = _spec().clone(name="same")
    outside = ns.Job(spec).dry_run(region="eu-north1", project="p")

    async def inside() -> dict[str, object]:
        return ns.Job(spec).dry_run(region="eu-north1", project="p")

    assert asyncio.run(inside()) == outside


def test_sync_calls_inside_a_loop_run_off_the_loop() -> None:
    fake = FakeTransport()
    fake.states = ["RUNNING", "COMPLETED"]
    threads: list[str] = []
    original = fake.get

    def get(job_id: str) -> dict[str, object]:
        threads.append(threading.current_thread().name)
        return original(job_id)

    fake.get = get  # type: ignore[method-assign]

    async def inside() -> tuple[ns.Job, str, str]:
        job = _job().create(
            region="eu-north1", project="p", transport=fake, quiet=True
        )
        first = job.get(quiet=True)["status"]["state"]  # type: ignore[index]
        last = job.wait(quiet=True)["status"]["state"]  # type: ignore[index]
        return job, first, last

    job, first, last = asyncio.run(inside())
    assert (job.id, first, last) == ("job-1", "RUNNING", "COMPLETED")
    assert threads and all(name.startswith("ns-sync") for name in threads)


def test_ctrl_c_in_a_loop_stops_polling_and_cancels_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A long poll interval: the worker must stop well before it ends.
    monkeypatch.setattr("nebius_serverless.jobs.job.POLL_SECONDS", 30.0)
    fake = FakeTransport()
    fake.states = ["RUNNING"]
    job = _job().create(
        region="eu-north1", project="p", transport=fake, quiet=True
    )
    original = fake.get

    def get(job_id: str) -> dict[str, object]:
        record = original(job_id)
        _thread.interrupt_main()  # Ctrl-C in the waiting thread
        return record

    fake.get = get  # type: ignore[method-assign]

    async def inside() -> None:
        job.wait(quiet=True)

    # As in a Jupyter kernel: the loop does not handle SIGINT itself.
    loop = asyncio.new_event_loop()
    started = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            loop.run_until_complete(inside())
    finally:
        loop.close()
    workers = [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("ns-sync")
    ]
    for thread in workers:
        thread.join(timeout=5)
    assert not any(thread.is_alive() for thread in workers)
    assert time.monotonic() - started < 5
    assert fake.gets == ["job-1"]
    assert fake.cancels == []


def test_awaited_twins_match_the_sync_calls() -> None:
    spec = _spec()
    sync_fake = FakeTransport()
    awaited_fake = FakeTransport()
    sync = ns.Job(spec).create(
        region="eu-north1", project="p", transport=sync_fake
    )

    async def once() -> ns.Job:
        return await ns.Job(spec).create_async(
            region="eu-north1",
            project="p",
            transport=awaited_fake,
        )

    awaited = asyncio.run(once())
    assert isinstance(awaited, ns.Job)
    assert (awaited.id, sync.id) == ("job-1", "job-1")
    # Each create generates its own default name.
    bodies = [awaited_fake.bodies[0], sync_fake.bodies[0]]
    for body in bodies:
        metadata = body["metadata"]
        assert isinstance(metadata, dict)
        assert str(metadata.pop("name")).startswith("job-")
    assert bodies[0] == bodies[1]


def test_async_get_wait_and_cancel() -> None:
    fake = FakeTransport()
    fake.states = ["RUNNING", "COMPLETED"]
    run = _job().create(
        region="eu-north1",
        project="p",
        transport=fake,
    )
    assert isinstance(run, ns.Job)

    async def follow() -> None:
        first = await run.get_async()
        assert first["status"] == {"state": "RUNNING"}
        finished = await run.wait_async(timeout=0)
        assert finished["status"] == {"state": "COMPLETED"}
        payload = await run.cancel_async()
        assert payload["accepted"] is True
        loaded = await ns.jobs.get_async(run.id, transport=fake)
        assert loaded.id == run.id

    asyncio.run(follow())
    assert fake.cancels == [run.id]


def test_cancelling_wait_does_not_cancel_the_job() -> None:
    fake = FakeTransport()
    started = threading.Event()
    release = threading.Event()

    def blocking_get(job_id: str) -> dict[str, object]:
        started.set()
        release.wait(5)
        return {
            "metadata": {"id": job_id},
            "status": {"state": "RUNNING"},
        }

    fake.get = blocking_get  # type: ignore[method-assign]
    run = _job().create(
        region="eu-north1",
        project="p",
        transport=fake,
    )
    assert isinstance(run, ns.Job)

    async def cancel_wait() -> None:
        task = asyncio.create_task(run.wait_async(timeout=0))
        await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_wait())
    release.set()
    assert fake.cancels == []


_CANCEL_SCRIPT = r"""
import asyncio, sys, time
sys.path.insert(0, "tests")
from fakes import FakeEndpointTransport, FakeTransport
import nebius_serverless as ns

kind = sys.argv[1]
if kind == "job":
    fake = FakeTransport()
    fake.states = ["RUNNING"]
    spec = ns.JobSpec(command=["x"], image="a:b", timeout="1h")
    handle = ns.Job(spec).create(
        region="eu-north1", project="p", transport=fake, quiet=True)
else:
    fake = FakeEndpointTransport()
    fake.states = ["PROVISIONING"]
    spec = ns.EndpointSpec(image="a:b", port=80)
    handle = ns.Endpoint(spec).create(
        region="eu-north1", project="p", transport=fake, quiet=True)

async def main():
    task = asyncio.create_task(handle.wait_async(quiet=True))
    while not fake.gets:
        await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

started = time.monotonic()
asyncio.run(main())
print(len(fake.gets), round(time.monotonic() - started, 1))
"""


@pytest.mark.parametrize("kind", ["job", "endpoint"])
def test_cancelled_wait_async_stops_polling_and_lets_the_process_exit(
    kind: str,
) -> None:
    import subprocess
    import sys

    # The real poll interval (10s / 5s) applies: only the stop signal can
    # end this quickly. Before the fix the child polled until killed.
    out = subprocess.run(
        [sys.executable, "-c", _CANCEL_SCRIPT, kind],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert out.returncode == 0, out.stderr
    gets, seconds = out.stdout.split()
    assert int(gets) == 1
    assert float(seconds) < 2.0
