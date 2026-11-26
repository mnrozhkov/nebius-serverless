"""A baseline experiment as a pipeline of tasks, in plain Python.

The pipeline trains a small image classifier on MNIST with several
learning rates, evaluates each result, and returns the best one:

1. ``gpu_check``: a bash script. Nothing waits for it.
2. ``prepare``: downloads the dataset once, on a CPU job.
3. ``train``: one spot GPU job per learning rate, all at the same time.
4. ``evaluate``: one GPU job per training run, each started as soon as
   its training run has ended.
5. ``aggregate``: picks the best run, on a CPU job.

The tasks are in ``tasks.py``. A path returned by one task is passed to
the next one as is: every task's job sees the task bucket at the same
path. Each task's files go to ``tasks/<task>/<time>/`` in the bucket.

Run it from this folder. It waits for the result and prints it::

    python train.py

``train_prefect.py`` runs the same pipeline as a Prefect flow.
"""

from tasks import (
    EXPERIMENT,
    LEARNING_RATES,
    aggregate,
    evaluate,
    gpu_check,
    prepare,
    train,
)


def pipeline(epochs: int = 5) -> dict[str, object]:
    """Train with each learning rate, evaluate, and return the best run.

    ``train_prefect.py`` runs this same function as a Prefect flow: its
    source is sent on its own, so from outside its body it uses only
    tasks and constants with JSON values.
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
    """Run the pipeline and print the best run."""
    print(pipeline())


if __name__ == "__main__":
    main()
