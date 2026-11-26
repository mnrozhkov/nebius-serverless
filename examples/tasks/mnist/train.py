"""A baseline experiment as a pipeline of tasks.

Uses ``ns.Task`` and the Prefect integration's ``PrefectFlow``. The
tasks and constants are in ``tasks.py``. The walkthrough is the
"Pipeline of tasks" page of the documentation,
``docs/training/task-pipeline.md``.

The pipeline trains a small image classifier on MNIST with several
learning rates, evaluates each result, and returns the best one:

1. ``gpu_check``: a shell command. Nothing waits for it.
2. ``prepare``: downloads the dataset once, on a CPU job.
3. ``train``: one spot GPU job per learning rate, all at the same time.
4. ``evaluate``: one GPU job per training run, each started as soon as
   its training run has ended.
5. ``aggregate``: picks the best run, on a CPU job.

A path returned by one task is passed to the next one as is. This
relies on every task's job seeing the managed bucket at the same path.

Run it from this folder, in one of three ways::

    python train.py                        # plain Python on this machine
    python train.py --mode prefect-local   # a Prefect flow on this machine
    python train.py --mode prefect-remote  # a Prefect flow on a Nebius job

- ``local`` needs no Prefect. The script waits for the result. Each
  task's files go to ``tasks/<task>/<time>/`` in the managed bucket.
- ``prefect-local`` runs ``pipeline`` as the Prefect flow
  ``mnist-baseline`` in this process (``remote_job=False``). It needs
  Prefect here, ``pip install "nebius-serverless[prefect]"``, and waits
  for the result.
- ``prefect-remote`` sends ``pipeline`` to an on-demand CPU job and
  runs it there as that flow. The script ends once the job has
  started, and the flow keeps running. Prefect is not needed here.

In both Prefect modes:

- The run is recorded on the Prefect server whose API address is in
  ``PREFECT_API_URL``, such as ``https://prefect.example.com/api``.
  Set it before running. If that server asks for a login, also set
  ``PREFECT_API_AUTH_STRING`` to ``user:password``, or put it on one
  line in ``~/.config/prefect/auth``.
- A run is named by its launch time, for example
  ``20261125-091833-mnist-baseline``. Its files go to
  ``flows/mnist-baseline/<run>/`` in the bucket, one folder per task
  job.
- The link to the flow run in the Prefect UI is printed at the start.
  It is the API address without ``/api``; set ``PREFECT_UI_URL`` when
  the UI is elsewhere.
- Each task call is a Prefect task run and one Nebius job, with the
  artifacts ``nebius-job``, ``inputs`` and ``outputs``. Set
  ``NS_CONSOLE_JOB_URL`` to the console address of a job, with
  ``{project}`` and ``{job}`` in it, to get a link to the job in
  ``nebius-job``.

Every run does every step, including the dataset download.
"""

import argparse

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

from nebius_serverless.integrations.prefect import PrefectFlow

MODES = ("local", "prefect-local", "prefect-remote")


def pipeline(epochs: int = 5) -> dict[str, object]:
    """Train with each learning rate, evaluate, and return the best run.

    With Prefect, this function's source is what runs as the flow. From
    outside its body it uses only tasks and constants with JSON values.
    """

    # Nothing waits for it. Calling gpu_check(...) instead would.
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
    return best.result()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the MNIST baseline pipeline."
    )
    parser.add_argument(
        "--mode",
        choices=MODES,
        default="local",
        help=(
            "local: plain Python here; prefect-local: a Prefect flow "
            "here; prefect-remote: a Prefect flow on a Nebius job "
            "(default: %(default)s)"
        ),
    )
    mode = parser.parse_args().mode

    if mode == "local":
        result = pipeline()
        print(result)
        return

    flow = PrefectFlow(
        pipeline,
        flow_name=EXPERIMENT,
        api_url=PREFECT_API_URL,
        remote_job=(mode == "prefect-remote"),
        timeout="3h",  # The flow job's limit. Not used by prefect-local.
    )
    run = flow.submit()
    if run.flow_run_url:
        print(f"Prefect: {run.flow_run_url}")


if __name__ == "__main__":
    main()
