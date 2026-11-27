"""The image quickstart: build a Dockerfile, reuse it, run a job on it.

The first run builds the image on a CPU job (a few minutes) and pushes it
to the project's ``generated-images`` registry. Later runs, and the second
``build()`` below, find the same tag and reuse it. Uses the active Nebius
profile.
"""

import nebius_serverless as ns

DOCKERFILE = """\
FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir requests==2.32.3
"""


def main() -> None:
    """Build (or reuse) the image, then run one CPU job on it."""

    image = ns.Image(dockerfile_string=DOCKERFILE)

    # Builds the first time; returns the reference either way.
    ref = image.build()

    # Same Dockerfile, same tag: nothing is built.
    assert image.build() == ref

    spec = ns.JobSpec(
        ["python", "-c", "import requests; print(requests.__version__)"],
        image=ref,
        timeout="15m",
    )
    job = spec.create()
    job.wait()
    job.display()


if __name__ == "__main__":
    main()
