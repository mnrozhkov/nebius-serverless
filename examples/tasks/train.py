"""A baseline experiment as a pipeline of tasks.

Runs on the ``ns.Task`` prototype. See ``task_proposal.md`` in this
folder. The tasks and constants are in ``tasks.py``. Run it from this
folder.

The pipeline trains a small image classifier on MNIST with several
learning rates, evaluates each result, and names the best one:

1. ``gpu_check``: a shell command.
2. ``prepare``: downloads the dataset once, on a CPU job.
3. ``train``: one GPU job per learning rate, started with ``submit``
   so that they run at the same time.
4. ``evaluate``: one GPU job per checkpoint from step 3.
5. Comparison, on this machine.

A path returned by one task is passed to the next one as is. This
relies on every task's job seeing the managed bucket at the same path.
"""

from tasks import (
    EXPERIMENT,
    LEARNING_RATES,
    evaluate,
    gpu_check,
    prepare,
    train,
)


def main() -> None:
    """Run the pipeline and print the best learning rate."""
    gpu = gpu_check(fields="memory.total,driver_version")
    # A dict of strings, something like:
    # {"exp_name": "mnist-baseline", "gpu_count": "1",
    #  "gpu_name": "NVIDIA H100 ...", "details": "81559 MiB, 570.86.15"}
    print(gpu)

    data = prepare(dataset="mnist")
    # {"data_dir": "<run folder>/mnist", "train": 60000, "test": 10000}

    # One job per learning rate. submit() returns at once, so the jobs
    # run at the same time; result() waits for each one.
    training = [
        train.submit(
            exp_name=EXPERIMENT, data_dir=data["data_dir"], lr=lr, epochs=5
        )
        for lr in LEARNING_RATES
    ]
    trained = [run.result() for run in training]
    # [{"lr": 0.3, "loss": 1.92, "checkpoint": "<run folder>/model.pt"}, ...]

    evaluation = [
        evaluate.submit(trained=run, data_dir=data["data_dir"])
        for run in trained
    ]
    scores = [run.result() for run in evaluation]
    # [{"accuracy": 0.41}, ...]

    print("lr\tloss\taccuracy")
    for run, score in zip(trained, scores):
        print(f"{run['lr']}\t{run['loss']:.3f}\t{score['accuracy']:.3f}")
    best = max(zip(trained, scores), key=lambda pair: pair[1]["accuracy"])
    print(f"best lr: {best[0]['lr']}, checkpoint: {best[0]['checkpoint']}")


if __name__ == "__main__":
    main()
