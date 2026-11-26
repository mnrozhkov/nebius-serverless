"""The Prefect flow quickstart: a pipeline of tasks as a Prefect flow.

``submit()`` starts one CPU job that runs ``pipeline`` as a Prefect flow,
and that job starts the tasks' jobs. The script prints the flow run's
link, then waits for the result.

Set ``PREFECT_API_URL`` to your Prefect server's API address, such as
``https://prefect.example.com/api``, and ``PREFECT_API_AUTH_STRING`` to
``user:password`` if it asks for a login. Prefect itself is not needed
on this machine. Reading the result needs the same Object Storage
credentials as the task quickstart. Uses the active Nebius profile.
"""

import os

import nebius_serverless as ns
from nebius_serverless.integrations.prefect import PrefectFlow

IMAGE = "python:3.12-slim-bookworm"


def square_number(x: int) -> int:
    """Return ``x`` squared."""
    return x * x


def add_numbers(values: list[int]) -> int:
    """Return the sum of ``values``."""
    return sum(values)


square = ns.Task(square_number, name="square", image=IMAGE, timeout="10m")
add = ns.Task(add_numbers, name="add", image=IMAGE, timeout="10m")


def pipeline(n: int = 3) -> int:
    """Square 0 to n-1, one job each at the same time, then add them up."""
    squares = [square.submit(x=x) for x in range(n)]
    return add(values=squares)


def main() -> None:
    """Start the flow on a Nebius job and wait for its result."""
    flow = PrefectFlow(
        pipeline,
        flow_name="sum-of-squares",
        api_url=os.environ.get("PREFECT_API_URL"),
        timeout="1h",  # the flow job's limit
    )
    run = flow.submit(n=3)
    print(f"sum of squares: {run.result()}")


if __name__ == "__main__":
    main()
