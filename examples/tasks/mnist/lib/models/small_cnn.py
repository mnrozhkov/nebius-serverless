"""The baseline model, shared by training and evaluation."""

CLASSES = 10


def build_model() -> object:
    """A small convolutional network for 28x28 grey images."""
    from torch import nn

    return nn.Sequential(
        nn.Conv2d(1, 32, 3, padding=1),
        nn.ReLU(),
        nn.MaxPool2d(2),
        nn.Conv2d(32, 64, 3, padding=1),
        nn.ReLU(),
        nn.MaxPool2d(2),
        nn.Conv2d(64, 128, 3, padding=1),
        nn.ReLU(),
        nn.AdaptiveAvgPool2d(1),
        nn.Flatten(),
        nn.Linear(128, CLASSES),
    )
