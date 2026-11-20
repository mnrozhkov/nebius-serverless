"""The README quickstart.

``["nvidia-smi"]``, ``gpu="h100"``, and timeout ``1h``. Uses the active
Nebius profile.
"""

import nebius_serverless as ns


def main() -> None:
    """Create the README job, wait for it, and print the final record."""

    # Describe what should run.
    spec = ns.JobSpec(
        ["nvidia-smi"],
        image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
        gpu="h100",
        timeout="1h",
    )

    # Start one cloud job.
    job = spec.create()

    # Wait for it to complete.
    job.wait()
    job.display()


if __name__ == "__main__":
    main()
