"""Measure a trained model on the test images."""

from src.data.mnist import load_split
from src.models.small_cnn import build_model


def evaluate(
    checkpoint: str, data_dir: str, limit: int | None = None
) -> dict[str, object]:
    """Return the share of test images the weights classify correctly.

    Args:
        checkpoint: The weights path ``train_baseline`` returned.
        data_dir: The folder ``prepare_data`` returned.
        limit: Use only the first ``limit`` images. For a quick check.
    """
    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    images, labels = load_split(data_dir, False, device, limit)
    model = build_model().to(device)
    model.load_state_dict(torch.load(checkpoint, map_location=device))
    model.eval()
    correct = 0
    with torch.no_grad():
        for start in range(0, len(images), 1000):
            scores = model(images[start : start + 1000])
            answers = labels[start : start + 1000]
            correct += int((scores.argmax(dim=1) == answers).sum().item())
    return {"accuracy": correct / len(images), "test": len(images)}
