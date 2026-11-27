"""Static checks for the job and endpoint handles. mypy reads it; pytest
does not.

Run: ``uv run mypy tests/typing``. Nothing here is executed.
"""

from typing import Any

from typing_extensions import assert_type

import nebius_serverless as ns


def job_create(spec: ns.JobSpec) -> None:
    job = ns.Job(spec)
    assert_type(job.id, str | None)
    assert_type(job.dry_run(), dict[str, object])
    assert_type(job.dry_run(server=True), dict[str, object])
    assert_type(job.to_cli(), str)
    assert_type(job.create(), ns.Job)
    assert job.id
    assert_type(job.id.startswith("aijob-"), bool)
    assert_type(job.spec, ns.JobSpec)


def job_shorthand() -> None:
    job = ns.Job(["nvidia-smi"], image="python:3.12-slim", timeout="10m")
    assert_type(job, ns.Job)
    assert_type(ns.Job(["x"], image="python:3", timeout=600).create(), ns.Job)


async def job_create_async(spec: ns.JobSpec) -> None:
    assert_type(await ns.Job(spec).create_async(), ns.Job)


def spec_data(spec: ns.JobSpec) -> None:
    assert_type(spec.clone(), ns.JobSpec)
    assert_type(spec.clone(timeout="2h"), ns.JobSpec)
    assert_type(spec.timeout, int)
    assert_type(spec.command, tuple[str, ...])
    assert_type(spec.to_dict(), dict[str, Any])


def endpoint_create(spec: ns.EndpointSpec) -> None:
    endpoint = ns.Endpoint(spec)
    assert_type(endpoint.id, str | None)
    assert_type(endpoint.dry_run(), dict[str, object])
    assert_type(endpoint.create(), ns.Endpoint)
    assert endpoint.id
    assert_type(endpoint.id.startswith("aiendpoint-"), bool)
    assert_type(spec.clone(port=9000), ns.EndpointSpec)


async def endpoint_create_async(spec: ns.EndpointSpec) -> None:
    assert_type(await ns.Endpoint(spec).create_async(), ns.Endpoint)


def get_by_id() -> None:
    assert_type(ns.jobs.get("aijob-test-0001"), ns.Job)
    assert_type(ns.endpoints.get("aiendpoint-test-0001"), ns.Endpoint)
