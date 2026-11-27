"""Helpers shared by the functions in this library."""

import os
import tempfile
from pathlib import Path


def output_dir(*parts: str) -> Path:
    """Return the folder for a run's files, creating it when missing.

    In a job this is under ``$NS_OUTPUT_DIR``, which ``ns.Task`` sets to
    the run's folder. On your machine it is under ``./output``.
    """
    root = Path(os.environ.get("NS_OUTPUT_DIR", "output"))
    path = root.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_file(source: Path, target: Path) -> None:
    """Copy a file with plain sequential writes.

    A run's folder is a mounted bucket. It takes a file written from
    start to end, and refuses appends and writes after a seek.
    """
    with open(source, "rb") as reader, open(target, "wb") as writer:
        while chunk := reader.read(1 << 20):
            writer.write(chunk)


def save_torch(value: object, target: Path) -> None:
    """``torch.save`` to a local file, then copy it to ``target``."""
    import torch

    with tempfile.TemporaryDirectory() as folder:
        local = Path(folder) / target.name
        torch.save(value, local)
        copy_file(local, target)
