"""Run a job spec loaded from YAML.

``--config`` is the path to the YAML file. The spec in that file is
the whole configuration. Uses the active Nebius profile.
"""

import argparse

import nebius_serverless as ns


def main() -> None:
    """Create the job described by --config and wait for it."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the job YAML file.",
    )
    config = parser.parse_args().config
    spec = ns.JobSpec.from_yaml(config)
    job = ns.Job(spec).create()
    job.wait()
    job.display()
    if job.status != "COMPLETED":
        raise SystemExit(f"{job.id} ended {job.status}")


if __name__ == "__main__":
    main()
