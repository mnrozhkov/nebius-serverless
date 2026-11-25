"""The baseline pipeline as a Prefect flow that runs on a Nebius job.

Runs on the ``ns.PrefectFlow`` prototype. See ``flow_job_proposal.md``
in this folder.

``baseline`` is a plain function that calls the tasks of ``tasks.py``.
``ns.PrefectFlow`` sends it to an on-demand CPU job and runs it there
as the Prefect flow ``mnist-baseline``. A run is named by its launch
time, for example ``20261125-091833-mnist-baseline``, in Prefect and as
the flow job. Each task call becomes a Prefect task run and one Nebius
job. ``submit`` returns at once, so
the flow keeps running after this script has ended.

Run it from this folder::

    python train_prefect.py

Every run does every step, including the dataset download. The run is
recorded on the Prefect server whose API address is in
``PREFECT_API_URL``, such as ``https://prefect.example.com/api``. Set
it before running. If that server asks for a login, also set
``PREFECT_API_AUTH_STRING`` to ``user:password``.
Prefect is not needed on this machine.

Each task run in Prefect has the artifacts ``nebius-job``, ``inputs``
and ``outputs``. Set ``NS_CONSOLE_JOB_URL`` to the console address of
a job, with ``{project}`` and ``{job}`` in it, to get a link to the
job in ``nebius-job``.
"""

from tasks import (
    EXPERIMENT,
    LEARNING_RATES,
    PREFECT_API_URL,
    aggregate,
    evaluate,
    gpu_check,
    prepare,
    train,
)

import nebius_serverless as ns


def pipeline(epochs: int = 5) -> dict[str, object]:
    """Train with each learning rate, evaluate, and return the best run."""

    # blocks pipeline
    # gpu_check(exp_name=EXPERIMENT, fields="memory.total,driver_version")

    # doesn't block anything
    gpu_check.submit(fields="memory.total,driver_version")

    data = prepare.submit(dataset="mnist")
    data_dir = data.result()["data_dir"]

    # One job per learning rate, all at the same time.
    training = [
        train.submit(
            exp_name=EXPERIMENT, data_dir=data_dir, lr=lr, epochs=epochs
        )
        for lr in LEARNING_RATES
    ]

    evaluation = [
        evaluate.submit(trained=run, data_dir=data_dir) for run in training
    ]

    best = aggregate.submit(trained=training, scores=evaluation)
    result = best.result()

    return result


flow = ns.PrefectFlow(
    pipeline,
    flow_name="mnist-baseline",
    api_url=PREFECT_API_URL,
    timeout="3h",
)


if __name__ == "__main__":
    run = flow.submit(epochs=5)
    print(f"started flow job {run.job.id}, run {run.name}")

    # pipeline(epochs=5)
