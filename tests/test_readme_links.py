"""README links are absolute, so they also work on the PyPI project page."""

import re
from pathlib import Path

README = Path(__file__).resolve().parents[1] / "README.md"
REPO = "https://github.com/mnrozhkov/nebius-serverless/blob/main/"


def test_readme_links_are_absolute_and_point_at_files() -> None:
    text = README.read_text(encoding="utf-8")
    targets = re.findall(r"\]\(([^)\s]+)\)", text)
    assert targets
    relative = [t for t in targets if not re.match(r"https?://|#", t)]
    assert relative == [], f"Use absolute links for PyPI: {relative}"
    root = README.parent
    for target in targets:
        if target.startswith(REPO):
            path = target.removeprefix(REPO).split("#", 1)[0]
            assert (root / path).exists(), target
