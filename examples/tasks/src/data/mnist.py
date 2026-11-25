"""MNIST: download it once, then load it as tensors.

``torch`` and ``torchvision`` are imported inside the functions, so
this module can be imported on a machine that has neither.
"""

import tempfile
from pathlib import Path

from src.common import copy_file, output_dir

FOLDER = "MNIST/raw"
MEAN = 0.1307
STD = 0.3081


def prepare_data(dataset: str = "mnist") -> dict[str, object]:
    """Download the dataset into the run's folder.

    Returns the folder, for ``train_baseline`` and ``evaluate``, and the
    number of training and test images.
    """
    from torchvision.datasets import MNIST

    if dataset != "mnist":
        raise ValueError(f"unknown dataset {dataset!r}; use 'mnist'")
    target = output_dir(dataset)
    # Download and unpack on local disk, then copy the unpacked files.
    with tempfile.TemporaryDirectory() as local:
        train = MNIST(local, train=True, download=True)
        test = MNIST(local, train=False, download=True)
        (target / FOLDER).mkdir(parents=True, exist_ok=True)
        for path in sorted((Path(local) / FOLDER).iterdir()):
            if path.is_file() and path.suffix != ".gz":
                copy_file(path, target / FOLDER / path.name)
    return {"data_dir": str(target), "train": len(train), "test": len(test)}


def load_split(
    data_dir: str, train: bool, device: str, limit: int | None = None
) -> tuple[object, object]:
    """Return normalized images and labels as tensors on ``device``."""
    from torchvision.datasets import MNIST

    data = MNIST(data_dir, train=train, download=False)
    images = data.data[:limit].to(device).unsqueeze(1).float().div(255)
    labels = data.targets[:limit].to(device)
    return (images - MEAN) / STD, labels
