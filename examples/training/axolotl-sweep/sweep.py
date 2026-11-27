"""Run two Axolotl fine-tunes in parallel and save adapters to one volume.

Upload ``configs/qwen_0_5b.yaml`` and ``configs/qwen_1_5b.yaml`` to
``s3://fine-tuning-axolotl/configs/`` before running. Both jobs mount that
bucket and copy checkpoints into separate folders under
``/workspace/data/output/``. Uses the active Nebius profile.
"""

import nebius_serverless as ns

IMAGE = "docker.io/axolotlai/axolotl:main-20260309-py3.11-cu128-2.9.1"


def train(config: str) -> list[str]:
    """Train one config, then copy its output to the shared volume."""
    return [
        "bash",
        "-lc",
        f"axolotl train /workspace/data/configs/{config}.yaml"
        f" && mkdir -p /workspace/data/output/{config}"
        " && cp -r /workspace/output/."
        f" /workspace/data/output/{config}",
    ]


def main() -> None:
    """Create both jobs, wait for each, and display the final records."""
    base = ns.JobSpec(
        command=train("qwen_0_5b"),
        image=IMAGE,
        gpu="h100",
        pricing="spot",
        disk="500Gi",
        timeout="24h",
        volumes=["s3://fine-tuning-axolotl:/workspace/data:rw"],
        name="axolotl-qwen-0.5b",
    )
    # clone() copies the spec with the changed fields, checked again.
    specs = (
        base,
        base.clone(command=train("qwen_1_5b"), name="axolotl-qwen-1.5b"),
    )

    jobs = [ns.Job(spec).create() for spec in specs]
    for job in jobs:
        job.wait()
        job.display()


if __name__ == "__main__":
    main()
