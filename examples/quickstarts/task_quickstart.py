"""The task quickstart: run a Python function and a bash script as jobs.

Each task call is one small CPU job. Arguments and results travel
through the project's task bucket, which the first run creates. Reading
the results here needs Object Storage credentials: ``AWS_ACCESS_KEY_ID``
and ``AWS_SECRET_ACCESS_KEY``, or a profile in ``~/.aws/credentials``.
Uses the active Nebius profile.
"""

import nebius_serverless as ns

IMAGE = "python:3.12-slim-bookworm"
TEXT = "to be or not to be that is the question"


def count_words(text: str) -> dict[str, int]:
    """Count each word in a text. Runs in the job, so it imports there."""
    from collections import Counter

    return dict(Counter(text.split()))


def most_common(counts: dict[str, int]) -> str:
    """Return the most frequent word."""
    return max(counts, key=lambda word: counts[word])


count = ns.Task(count_words, image=IMAGE, timeout="10m")
top = ns.Task(most_common, image=IMAGE, timeout="10m")

# A bash script. Its inputs arrive in the bash array ``inputs``; what it
# sets in ``outputs`` comes back as a dict of strings.
hello = ns.Task(
    name="hello",
    image=IMAGE,
    inputs=["who"],
    command="""
    echo "hello ${inputs[who]}"
    outputs[greeting]="hello ${inputs[who]}"
    outputs[python]=$(python3 --version)
    """,
    timeout="10m",
)


def main() -> None:
    """Call each task, then chain two of them."""

    # Calling a task starts its job, waits, and returns the result.
    print(count(text=TEXT))
    print(hello(who="world"))

    # submit() returns at once. A run passed to another task is replaced
    # by its result, and that task's job starts once the run has ended.
    counts = count.submit(text=TEXT)
    word = top.submit(counts=counts)
    print(f"most common: {word.result()}")


if __name__ == "__main__":
    main()
