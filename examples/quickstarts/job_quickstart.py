"""The README quickstart.

``["nvidia-smi"]``, ``gpu="h100"``, and timeout ``1h``. Uses the active
Nebius profile.
"""

import nebius_serverless as ns


def main() -> None:
    """Create the README job, wait for it, and print the final record."""

    # Describe what should run. A spec is data: nothing is created yet.
    spec = ns.JobSpec(
        command=["nvidia-smi"],
        image="nvidia/cuda:13.1.1-runtime-ubuntu24.04",
        gpu="h100",
        timeout="1h",
    )

    # Create one job from the spec.
    job = ns.Job(spec).create()

    # Wait for it to complete.
    job.wait()
    job.display()


if __name__ == "__main__":
    main()
