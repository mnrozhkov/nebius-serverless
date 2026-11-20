"""Static checks for ``create`` return types. mypy reads it; pytest does not.

Run: ``uv run mypy tests/typing``. Nothing here is executed.
"""

from typing_extensions import assert_type

import nebius_serverless as ns


def job_create(recipe: ns.JobSpec, flag: bool) -> None:
    assert_type(recipe.create(), ns.Job)
    assert_type(recipe.create(dry_run=False), ns.Job)
    assert_type(recipe.create(dry_run=True), dict[str, object])
    assert_type(recipe.create(True), dict[str, object])
    assert_type(recipe.create(dry_run=flag), ns.Job | dict[str, object])


async def job_create_async(recipe: ns.JobSpec, flag: bool) -> None:
    assert_type(await recipe.create_async(), ns.Job)
    assert_type(await recipe.create_async(dry_run=False), ns.Job)
    assert_type(
        await recipe.create_async(dry_run=True),
        dict[str, object],
    )
    assert_type(
        await recipe.create_async(dry_run=flag),
        ns.Job | dict[str, object],
    )


def endpoint_create(recipe: ns.EndpointSpec, flag: bool) -> None:
    assert_type(recipe.create(), ns.Endpoint)
    assert_type(recipe.create(dry_run=False), ns.Endpoint)
    assert_type(recipe.create(dry_run=True), dict[str, object])
    assert_type(recipe.create(True), dict[str, object])
    assert_type(
        recipe.create(dry_run=flag),
        ns.Endpoint | dict[str, object],
    )


async def endpoint_create_async(recipe: ns.EndpointSpec, flag: bool) -> None:
    assert_type(await recipe.create_async(), ns.Endpoint)
    assert_type(await recipe.create_async(dry_run=False), ns.Endpoint)
    assert_type(
        await recipe.create_async(dry_run=True),
        dict[str, object],
    )
    assert_type(
        await recipe.create_async(dry_run=flag),
        ns.Endpoint | dict[str, object],
    )


def get_by_id() -> None:
    assert_type(ns.jobs.get("aijob-test-0001"), ns.Job)
    assert_type(ns.endpoints.get("aiendpoint-test-0001"), ns.Endpoint)
