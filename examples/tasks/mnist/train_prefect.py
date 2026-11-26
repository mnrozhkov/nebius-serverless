"""The baseline pipeline of ``train.py`` as a Prefect flow.

Run it from this folder::

    python train_prefect.py          # the flow on a Nebius job
    python train_prefect.py --here   # the flow in this process

- By default ``pipeline`` is sent to an on-demand CPU job and runs there
  as the flow ``mnist-baseline``. The script ends once the job has
  started, and the flow keeps running. Prefect is not needed here.
- With ``--here`` the flow runs in this process, and the script waits
  for the result. This needs ``pip install "nebius-serverless[prefect]"``.

Either way:

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
"""

import argparse
import os

from tasks import EXPERIMENT
from train import pipeline

from nebius_serverless.integrations.prefect import PrefectFlow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the MNIST baseline pipeline as a Prefect flow."
    )
    parser.add_argument(
        "--here",
        action="store_true",
        help="run the flow in this process, not on a Nebius job",
    )
    here = parser.parse_args().here

    flow = PrefectFlow(
        pipeline,
        flow_name=EXPERIMENT,
        api_url=os.environ.get("PREFECT_API_URL"),
        remote_job=not here,
        timeout="3h",  # the flow job's limit
    )
    run = flow.submit()
    if run.flow_run_url:
        print(f"Prefect: {run.flow_run_url}")
    if here:
        print(run.result())


if __name__ == "__main__":
    main()
