"""to_cli() prints the CLI command that matches what create() sends."""

import re
import shlex
from pathlib import Path

import pytest
from fakes import FakeEndpointTransport, FakeTransport

import nebius_serverless as ns
from nebius_serverless.core.cli_flags import FLAGS

SECRET = "env-value-never-printed"
TOKEN = "token-never-printed"
WHERE = {"region": "eu-north1", "project": "project-test-0001"}


def _parse(text: str) -> list[tuple[str, str | None]]:
    """``(flag, value)`` pairs from the printed command, notes dropped."""
    command = " ".join(
        line.rstrip(" \\")
        for line in text.splitlines()
        if not line.startswith("#")
    )
    words = shlex.split(command)[4:]  # nebius ai <noun> create
    pairs: list[tuple[str, str | None]] = []
    i = 0
    while i < len(words):
        flag = words[i]
        if i + 1 < len(words) and not words[i + 1].startswith("--"):
            pairs.append((flag, words[i + 1]))
            i += 2
        else:
            pairs.append((flag, None))
            i += 1
    return pairs


def _values(pairs: list[tuple[str, str | None]], flag: str) -> list[str]:
    return [str(v) for f, v in pairs if f == flag]


def _seconds(text: str) -> int:
    parts = re.findall(r"(\d+)([hms])", text)
    return sum(int(n) * {"h": 3600, "m": 60, "s": 1}[u] for n, u in parts)


JOBS = [
    {"command": ["nvidia-smi"], "gpu": "h100", "timeout": "1h"},
    {
        "command": ["bash", "-lc", "axolotl train /data/c.yaml && echo 'ok'"],
        "gpu": "h100:8",
        "pricing": "spot",
        "spot_policy": "pricingpolicy-test",
        "restart_attempts": -1,
        "timeout": "1h30m",
        "disk": "500Gi",
        "shm": "64Gi",
        "volumes": ["s3://d:/data:ro", "vol-1:/cache"],
        "env": {"PLAIN": SECRET},
        "secrets": {"HF_TOKEN": "mbsec-test"},
        "working_dir": "/workspace",
        "registry_secret": "mbsecver-test",
        "subnet_id": "vpcsubnet-test",
    },
    {"command": ["true"], "cpu": "amd:16", "timeout": "5m"},
    {"command": ["true"], "timeout": "10m"},
]


@pytest.mark.parametrize("kwargs", JOBS)
def test_job_command_matches_the_create_body(
    kwargs: dict[str, object],
) -> None:
    spec = ns.JobSpec(image="busybox:1.37", name="fixed", **kwargs)  # type: ignore[arg-type]
    job = ns.Job(spec)
    pairs = _parse(job.to_cli(project="project-test-0001"))
    fake = FakeTransport()
    job.create(transport=fake, quiet=True, **WHERE)
    body = fake.bodies[0]
    sent = body["spec"]
    assert isinstance(sent, dict)
    assert _values(pairs, "--parent-id") == ["project-test-0001"]
    assert _values(pairs, "--name") == [body["metadata"]["name"]]  # type: ignore[index]
    assert _values(pairs, "--image") == [sent["image"]]
    assert _values(pairs, "--container-command") == [sent["container_command"]]
    args = [sent["args"]] if "args" in sent else []
    assert _values(pairs, "--args") == args
    assert _seconds(_values(pairs, "--timeout")[0]) == sent["timeout_seconds"]
    for key in ("gpu", "cpu"):
        assert _values(pairs, f"--{key}") == sent.get(key, [])
    for key in ("platform", "preset"):
        want = [sent[key]] if key in sent else []
        assert _values(pairs, f"--{key}") == want
    assert _values(pairs, "--disk-size") == [sent["disk"]]
    spot = sent.get("preemptible") is True
    assert (("--preemptible", None) in pairs) == spot
    if "spot_pricing_policy" in sent:
        assert _values(pairs, "--spot-pricing-policy-id") == [
            sent["spot_pricing_policy"]
        ]
    elif spot:
        assert ("--follows-spot-price", None) in pairs
    assert _values(pairs, "--volume") == list(kwargs.get("volumes", []))  # type: ignore[call-overload]
    assert len(sent.get("volumes", [])) == len(_values(pairs, "--volume"))  # type: ignore[arg-type]
    env = {e["name"]: e for e in sent.get("environment_variables", [])}  # type: ignore[index]
    for value in _values(pairs, "--env"):
        name, shown = value.split("=", 1)
        assert shown == f"${name}" and "value" in env[name]
    for value in _values(pairs, "--env-secret"):
        name, ref = value.split("=", 1)
        assert env[name]["mysterybox_secret"]  # type: ignore[index]
        assert ref in str(env[name]["mysterybox_secret"])
    policy = _values(pairs, "--restart-policy")
    unlimited = sent.get("restart_attempts") == -1
    assert policy == (["on-failure"] if unlimited else [])


def test_no_secret_value_is_printed() -> None:
    spec = ns.JobSpec(
        command=["true"], image="a:b", timeout="1h", env={"PLAIN": SECRET}
    )
    assert SECRET not in ns.Job(spec).to_cli()
    endpoint = ns.EndpointSpec(image="a:b", port=8000, token=TOKEN)
    text = ns.Endpoint(endpoint).to_cli()
    assert TOKEN not in text
    assert "--auth token" in text
    assert "# token= is not printed" in text


def test_endpoint_command() -> None:
    spec = ns.EndpointSpec(
        image="vllm/vllm-openai:v0.19.1",
        ports=[8000, "6379/tcp", "80:8080"],
        auth="none",
        gpu="h100",
        name="fixed",
    )
    endpoint = ns.Endpoint(spec)
    pairs = _parse(endpoint.to_cli())
    fake = FakeEndpointTransport()
    endpoint.create(transport=fake, quiet=True, **WHERE)
    sent = fake.bodies[0]["spec"]
    assert isinstance(sent, dict)
    assert _values(pairs, "--container-port") == [
        "8000/http",
        "6379/tcp",
        "80:8080/http",
    ]
    assert len(sent["ports"]) == 3  # type: ignore[arg-type]
    assert _values(pairs, "--auth") == ["none"]
    assert _values(pairs, "--gpu") == sent["gpu"]
    assert "--parent-id" not in [f for f, _ in pairs]
    secret = ns.EndpointSpec(image="a:b", port=8000, token_secret="mbsec-t")
    printed = ns.Endpoint(secret).to_cli()
    assert _values(_parse(printed), "--token-secret") == ["mbsec-t"]


def test_a_restart_count_the_cli_cannot_say_is_noted() -> None:
    spec = ns.JobSpec(
        command=["true"], image="a:b", timeout="1h", restart_attempts=3
    )
    text = ns.Job(spec).to_cli()
    command, _, notes = text.partition("\n#")
    assert "--restart-policy" not in command
    assert "restart_attempts=3 has no CLI flag" in notes


def test_the_printer_and_the_flag_map_agree() -> None:
    root = Path(__file__).resolve().parents[1]
    docs = root / "docs" / "reference" / "cli-api-map.md"
    text = docs.read_text(encoding="utf-8")
    section = text[: text.index("\n## Commands\n")]
    rows = [line for line in section.splitlines() if line.startswith("| `")]
    for argument, flag in FLAGS.items():
        row = next(r for r in rows if f"`{argument}=" in r.split(" | ")[0])
        assert flag in row, (argument, flag)
