"""Train the baseline model. Written to run as an ``ns.Task``.

``train_baseline`` takes JSON values and returns a JSON dict, so the
same function can be called on your machine or sent to a job.

It uses the data, model and helper modules of this library. They are
available in a job because the whole ``src`` package is sent with
``include_libs``.

``torch`` is imported inside the functions, not at the top, so the
module can be imported on a machine that does not have ``torch``.
"""

from src.common import output_dir, save_torch
from src.data.mnist import load_split
from src.models.small_cnn import build_model

MOMENTUM = 0.9


def train_baseline(
    exp_name: str,
    data_dir: str,
    lr: float,
    epochs: int = 5,
    batch_size: int = 256,
    seed: int = 0,
    limit: int | None = None,
) -> dict[str, object]:
    """Train on MNIST with one learning rate and save the weights.

    A checkpoint is saved after every epoch in the run's folder. If the
    job is started again, for example after a spot preemption, training
    continues from that checkpoint.

    Args:
        exp_name: Name of the experiment, returned with the result.
        data_dir: The folder ``prepare_data`` returned.
        lr: Learning rate.
        epochs: Number of passes over the training images.
        batch_size: Images per step.
        seed: Random seed.
        limit: Use only the first ``limit`` images. For a quick check.

    Returns:
        The learning rate, the last epoch's mean loss, the device, the
        epoch training resumed from, and the path of the weights.
    """
    import torch

    torch.manual_seed(seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    images, labels = load_split(data_dir, True, device, limit)
    model = build_model().to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=lr, momentum=MOMENTUM)

    folder = output_dir()
    checkpoint = folder / "checkpoint.pt"
    first, loss = 0, float("nan")
    if checkpoint.is_file():
        saved = torch.load(checkpoint, map_location=device)
        model.load_state_dict(saved["model"])
        optimizer.load_state_dict(saved["optimizer"])
        first, loss = saved["epoch"], saved["loss"]

    for epoch in range(first, epochs):
        loss = _train_epoch(model, optimizer, images, labels, batch_size)
        save_torch(
            {
                "epoch": epoch + 1,
                "loss": loss,
                "model": model.state_dict(),
                "optimizer": optimizer.state_dict(),
            },
            checkpoint,
        )

    weights = folder / "model.pt"
    save_torch(model.state_dict(), weights)
    return {
        "exp_name": exp_name,
        "lr": lr,
        "epochs": epochs,
        "loss": loss,
        "device": device,
        "resumed_from": first,
        "checkpoint": str(weights),
    }


def _train_epoch(model, optimizer, images, labels, batch_size) -> float:
    """One pass over the data. Returns the mean loss."""
    import torch

    model.train()
    order = torch.randperm(len(images), device=images.device)
    total = 0.0
    for start in range(0, len(order), batch_size):
        batch = order[start : start + batch_size]
        optimizer.zero_grad()
        loss = torch.nn.functional.cross_entropy(
            model(images[batch]), labels[batch]
        )
        loss.backward()
        optimizer.step()
        total += float(loss.item()) * len(batch)
    return total / len(order)
