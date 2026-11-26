"""The tasks and constants of the baseline experiment.

``train.py`` imports them from here. Import
this module from this folder, so that ``ns.Image`` finds the
Dockerfile. ``include_libs`` takes import names, so ``src`` is
found the same way ``import src`` finds it.

The training tasks share one image, ``images/baseline/Dockerfile``.
``aggregate`` needs only Python and uses a public image.

- ``gpu_check``: a shell command. Inputs arrive in the bash array
  ``inputs``; what it sets in ``outputs`` comes back as a dict.
- ``prepare``: downloads the dataset, on a CPU job.
- ``train``: trains with one learning rate, on a spot GPU job.
- ``evaluate``: measures one checkpoint, on a GPU job.
- ``aggregate``: puts the runs together and picks the best one, on a
  CPU job. Its outputs are the experiment's result.


dont consider this:
# The step functions are defined here and import from ``src`` inside
# their bodies. This script never imports ``src`` itself, so each
# import succeeds only if ``include_libs`` sent the package to the job.
"""


import os

from src.data.mnist import prepare_data

import nebius_serverless as ns

EXPERIMENT = "mnist-baseline"
LEARNING_RATES = [0.3, 0.1, 0.03, 0.01]

# The API address of the Prefect server, such as
# https://prefect.example.com/api. It is read from the environment, so
# that no server address is kept in this file.
PREFECT_API_URL = os.environ.get("PREFECT_API_URL")


DOCKERFILE = """
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system --compile-bytecode torch==2.8.0 torchvision==0.23.0
"""

image_torch = ns.Image(dockerfile_string=DOCKERFILE)

# sh task
gpu_check = ns.Task(
    name="gpu-check",
    image=image_torch,
    gpu="h100",
    timeout="10m",
    inputs=["fields"],
    command="""
    nvidia-smi
    outputs[gpu_count]=$(nvidia-smi --list-gpus | wc -l)
    outputs[gpu_name]=$(
        nvidia-smi --query-gpu=name --format=csv,noheader | head -n 1
    )
    outputs[details]=$(
        nvidia-smi --query-gpu="${inputs[fields]}" --format=csv,noheader \\
            | head -n 1
    )
    """,
)

# imported function task
prepare = ns.Task(
    prepare_data,
    name="prepare-data",
    image=image_torch,
    include_libs=["src"],
    timeout="30m",
)


def train_step(
    exp_name: str, data_dir: str, lr: float, epochs: int
) -> dict[str, object]:
    """Train the baseline model with one learning rate."""
    from src.training.baseline import train_baseline

    return train_baseline(
        exp_name=exp_name, data_dir=data_dir, lr=lr, epochs=epochs
    )


# function import inside task
train = ns.Task(
    train_step,
    name="train-baseline",
    image=image_torch,
    include_libs=["src"],
    gpu="h100",
    pricing="spot",
    timeout="2h",
)


def evaluate_step(
    trained: dict[str, object], data_dir: str
) -> dict[str, object]:
    from src.evaluation.accuracy import evaluate

    return evaluate(checkpoint=trained["checkpoint"], data_dir=data_dir)


evaluate = ns.Task(
    evaluate_step,
    name="evaluate",
    image=image_torch,
    include_libs=["src"],
    gpu="h100",
    timeout="30m",
)


def aggregate_results(
    trained: list[dict[str, object]], scores: list[dict[str, object]]
) -> dict[str, object]:
    runs = list(zip(trained, scores))
    for run, score in runs:
        print(
            f"lr {run['lr']}: loss {run['loss']:.3f}, "
            f"accuracy {score['accuracy']:.3f}"
        )
    best, score = max(runs, key=lambda pair: pair[1]["accuracy"])
    return {
        "lr": best["lr"],
        "accuracy": score["accuracy"],
        "checkpoint": best["checkpoint"],
        "accuracy_by_lr": {
            str(run["lr"]): score["accuracy"] for run, score in runs
        },
    }


# plain function task, on a small CPU job
aggregate = ns.Task(
    aggregate_results,
    name="aggregate-results",
    image="python:3.12-slim-bookworm",
    timeout="10m",
)
