"""The tasks and constants of the MNIST baseline experiment.

``train.py`` imports them from here. Run it from this folder: the tasks
send the local package ``lib`` with ``local_libs=["lib"]``, which is
found the same way ``import lib`` finds it.

The training tasks share one image, built from ``DOCKERFILE`` on the
first run and reused after that. ``aggregate`` needs only Python and
uses a public image.

- ``gpu_check``: a bash script, on a GPU job.
- ``prepare``: downloads the dataset, on a CPU job.
- ``train``: trains with one learning rate, on a spot GPU job.
- ``evaluate``: measures one checkpoint, on a GPU job.
- ``aggregate``: puts the runs together and picks the best one, on a
  CPU job. Its result is the experiment's result.
"""

import os

from lib.data.mnist import prepare_data

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

# A bash script. Its inputs arrive in the bash array ``inputs``; what it
# sets in ``outputs`` comes back as a dict of strings.
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

# A function from the package ``lib``: the job imports it by name.
prepare = ns.Task(
    prepare_data,
    name="prepare-data",
    image=image_torch,
    local_libs=["lib"],
    timeout="30m",
)


def train_step(
    exp_name: str, data_dir: str, lr: float, epochs: int
) -> dict[str, object]:
    """Train the baseline model with one learning rate."""
    from lib.training.baseline import train_baseline

    return train_baseline(
        exp_name=exp_name, data_dir=data_dir, lr=lr, epochs=epochs
    )


# A function defined here: its source is sent, so it imports from ``lib``
# inside its body.
train = ns.Task(
    train_step,
    name="train-baseline",
    image=image_torch,
    local_libs=["lib"],
    gpu="h100",
    pricing="spot",
    # A preempted job is created again, up to twice. Training resumes
    # from checkpoint.pt in the run's folder.
    preemption_retries=2,
    timeout="2h",
)


def evaluate_step(
    trained: dict[str, object], data_dir: str
) -> dict[str, object]:
    from lib.evaluation.accuracy import evaluate

    return evaluate(checkpoint=trained["checkpoint"], data_dir=data_dir)


evaluate = ns.Task(
    evaluate_step,
    name="evaluate",
    image=image_torch,
    local_libs=["lib"],
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


# A self-contained function on a public image and the smallest CPU machine.
aggregate = ns.Task(
    aggregate_results,
    name="aggregate-results",
    image="python:3.12-slim-bookworm",
    timeout="10m",
)
