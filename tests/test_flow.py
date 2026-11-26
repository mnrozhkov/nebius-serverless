"""ns.PrefectFlow: what it sends, task records, and the checksum."""

# The fixtures of test_task are imported and then named as arguments.
# ruff: noqa: F811

import hashlib
import importlib
import inspect
import io
import json
import sys
import tarfile
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from test_task import (  # noqa: F401  (fixtures)
    BUCKET,
    IMAGE,
    STAMP,
    FakeBackend,
    RunningTransport,
    _setup,
    backend,
    double,
    package,
    transport,
    unpacks,
)

import nebius_serverless as ns
from nebius_serverless import flow as flow_module
from nebius_serverless import task as task_module
from nebius_serverless.image import Image

API = "https://prefect.test/api"
# The real check. The credentials fixture replaces it on the module.
CHECK_PREFECT = flow_module._check_prefect
RATES = [0.1, 0.01]
NAME = "experiment"

double_task = ns.Task(double, image=IMAGE, timeout="10m")
hello = ns.Task(
    name="hello",
    image=IMAGE,
    inputs=["who"],
    command='outputs[greeting]="hello ${inputs[who]}"',
)


def pipeline(epochs=5):
    """Run the tasks."""
    runs = [double_task.submit(rate, times=epochs) for rate in RATES]
    greeting = hello(who=NAME)
    return {"runs": [run.result() for run in runs], "hi": greeting}


def helper():
    return 1


def uses_a_helper():
    return helper() + len(inspect.signature(double).parameters)


@pytest.fixture
def credentials(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, str]]:
    """Record what would be written to MysteryBox. Nothing is written."""
    stored: list[dict[str, str]] = []

    def store(project: str, profile: object, values: dict[str, str]) -> str:
        stored.append(dict(values))
        return f"mbsecver-e00test{len(stored)}"

    monkeypatch.setattr(flow_module, "_store_credentials", store)
    monkeypatch.setattr(flow_module, "_access_token", lambda profile: "tok")
    monkeypatch.setattr(flow_module, "_prefect_login", lambda: "me:pw")
    monkeypatch.setattr(flow_module, "_check_prefect", lambda url, login: None)
    monkeypatch.setattr(flow_module, "_create_flow_run", lambda *args: None)
    monkeypatch.setattr(
        Image,
        "_build",
        lambda self, progress, **kwargs: f"cr.test/r/{self.name}:{self.tag}",
    )
    return stored


def _payload(backend: FakeBackend, key: str) -> dict[str, object]:
    data = backend.get(BUCKET, f"{key}/payload.json")
    assert data is not None
    loaded = json.loads(data)
    assert isinstance(loaded, dict)
    return loaded


def test_it_finds_the_tasks_and_constants_a_function_uses() -> None:
    flow = ns.PrefectFlow(pipeline, flow_name="Pipe Line", api_url=API)
    assert sorted(flow._tasks) == ["double_task", "hello"]
    assert flow._constants == {"NAME": NAME, "RATES": RATES}
    assert flow.flow_name == "Pipe Line"
    assert flow.run_name == "Pipe Line"
    assert flow.name == "pipe-line"
    named = ns.PrefectFlow(
        pipeline, flow_name="pipeline", run_name="lr sweep", api_url=API
    )
    assert named.run_name == "lr sweep"
    assert str(inspect.signature(flow)) == "(epochs=5)"
    assert flow.__doc__ == "Run the tasks."
    assert "def pipeline(epochs=5):" in flow._source


def test_other_names_from_the_file_are_refused() -> None:
    with pytest.raises(ns.ValidationError) as caught:
        ns.PrefectFlow(uses_a_helper, flow_name="f", api_url=API)
    assert "double, helper, inspect" in str(caught.value)
    with pytest.raises(ns.ValidationError, match="must be a Python function"):
        ns.PrefectFlow(len, flow_name="f", api_url=API)


def test_it_needs_a_prefect_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    with pytest.raises(ns.ValidationError, match="api_url"):
        ns.PrefectFlow(pipeline, flow_name="f")
    monkeypatch.setenv("PREFECT_API_URL", API)
    assert ns.PrefectFlow(pipeline, flow_name="f").api_url == API


def test_the_flow_name_is_required() -> None:
    with pytest.raises(TypeError, match="flow_name"):
        ns.PrefectFlow(pipeline, api_url=API)  # type: ignore[call-arg]
    with pytest.raises(ns.ValidationError, match="flow_name"):
        ns.PrefectFlow(pipeline, flow_name=" ", api_url=API)
    with pytest.raises(ns.ValidationError, match="run_name"):
        ns.PrefectFlow(pipeline, flow_name="f", run_name="", api_url=API)


def test_the_image_has_a_default() -> None:
    flow = ns.PrefectFlow(pipeline, flow_name="f", api_url=API)
    assert flow.image is ns.PrefectFlow.default_image
    assert flow.image.name == "prefect-flow"
    assert "prefect==" in flow_module.DOCKERFILE
    custom = ns.PrefectFlow(pipeline, flow_name="f", api_url=API, image=IMAGE)
    assert custom.image == IMAGE


def test_submit_sends_the_function_the_tasks_and_the_credentials(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
) -> None:
    transport.run = False
    flow = ns.PrefectFlow(
        pipeline,
        flow_name="My Flow",
        run_name="LR Sweep",
        api_url=API,
        timeout="3h",
        env={"A": "b"},
    )
    run = flow.submit(epochs=3)
    assert run.inputs == {"epochs": 3}
    assert run.name == STAMP

    # One secret version with the token and the login, never the values
    # in the job.
    assert credentials == [
        {"NEBIUS_IAM_TOKEN": "tok", "PREFECT_API_AUTH_STRING": "me:pw"}
    ]
    body = transport.bodies[0]
    spec = body["spec"]
    assert isinstance(spec, dict)
    # The job is named <launch time>-<run_name>, in its plain form.
    assert body["metadata"] == {
        "parent_id": "project-1",
        "name": f"{STAMP}-lr-sweep",
    }
    image = ns.PrefectFlow.default_image
    assert spec["image"] == f"cr.test/r/prefect-flow:{image.tag}"
    assert spec["timeout_seconds"] == 3 * 3600
    variables = {item["name"]: item for item in spec["environment_variables"]}
    assert variables["A"]["value"] == "b"
    assert variables["PREFECT_API_URL"]["value"] == API
    for name in ("NEBIUS_IAM_TOKEN", "PREFECT_API_AUTH_STRING"):
        assert variables[name]["mysterybox_secret"] == {
            "version_id": "mbsecver-e00test1"
        }
    assert "tok" not in json.dumps(spec, default=str)
    assert "me:pw" not in json.dumps(spec, default=str)

    # Only the runner and a pointer are injected. The pointer names the
    # payload in the bucket and carries its checksum.
    injected = {
        Path(item["container_path"]).name: item["content"]
        for item in spec["injected_files"]
    }
    assert sorted(injected) == ["pointer.json", "runner.py"]
    pointer = json.loads(injected["pointer.json"])
    # The run's folder is flows/<pipeline>/<run>, with the flow job's
    # own files under "flow". The job tells its tasks where that is.
    folder = f"flows/my-flow/{STAMP}-lr-sweep"
    key = f"{folder}/flow"
    assert variables["NS_TASK_FOLDER"]["value"] == folder
    assert run.output_uri == f"s3://{BUCKET.name}/{key}/files/"
    assert pointer["run"] == key
    raw = backend.get(BUCKET, f"{key}/payload.json")
    assert raw is not None
    assert pointer["sha256"] == hashlib.sha256(raw).hexdigest()

    # The payload runs this package's flow function with the user's
    # function, its arguments, its constants and the task records.
    payload = _payload(backend, key)
    assert payload["module"] == "nebius_serverless.flow"
    assert payload["qualname"] == "_run_flow"
    sent = payload["kwargs"]["flow"]  # type: ignore[index]
    assert sent["title"] == "My Flow"
    assert sent["run_title"] == f"{STAMP}-LR Sweep"
    assert sent["function"] == "pipeline"
    assert sent["kwargs"] == {"epochs": 3}
    assert sent["constants"] == {"NAME": NAME, "RATES": RATES}
    assert sent["project"] == "project-1"
    assert sent["region"] == "eu-north1"
    record = sent["tasks"]["double_task"]
    assert record["name"] == "double"
    assert record["image"] == IMAGE
    assert record["source"].startswith("def double(x, times=2):")
    assert record["options"] == {"timeout": "10m"}
    assert sent["tasks"]["hello"]["command"].startswith("outputs[greeting]")
    assert sent["tasks"]["hello"]["inputs"] == ["who"]

    # This package travels with the flow job.
    libs = backend.get(BUCKET, str(payload["libs"]))
    assert libs is not None
    names = tarfile.open(fileobj=io.BytesIO(libs)).getnames()
    assert "nebius_serverless/flow.py" in names
    assert "nebius_serverless/task.py" in names
    assert not any(name.endswith(".pyc") for name in names)


def test_a_server_without_a_login_gets_only_the_token(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport.run = False
    monkeypatch.setattr(flow_module, "_prefect_login", lambda: None)
    flow = ns.PrefectFlow(pipeline, flow_name="pipeline", api_url=API)
    flow.submit()
    assert credentials == [{"NEBIUS_IAM_TOKEN": "tok"}]
    # Without run_name, the run is named after the flow.
    name = transport.bodies[0]["metadata"]["name"]  # type: ignore[index]
    assert name == f"{STAMP}-pipeline"


def test_bad_arguments_fail_before_anything_is_sent(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
) -> None:
    flow = ns.PrefectFlow(pipeline, flow_name="f", api_url=API)
    with pytest.raises(ns.ValidationError, match="unexpected keyword"):
        flow.submit(steps=1)
    with pytest.raises(ns.ValidationError, match="JSON"):
        flow.submit(epochs=object())
    assert credentials == []
    assert transport.bodies == []


def test_a_task_from_its_record_runs_without_its_code(
    package: Path, backend: FakeBackend, transport: RunningTransport
) -> None:
    progress = task_module.Progress(quiet=True, logger_name="test")
    original = ns.Task(unpacks, image=IMAGE, include_libs=["demo_pkg"])
    record = json.loads(
        json.dumps(original._record(progress, backend, BUCKET))
    )
    assert record["libs"].startswith("libs/")
    copy = ns.Task._from_record(record)
    assert copy.name == "unpacks"
    assert str(inspect.signature(copy)) == "(x)"
    assert copy(1) == original(1) == 2
    # The copy sent no library of its own.
    assert len([key for key in backend.puts if key.startswith("libs/")]) == 1

    triple = importlib.import_module("demo_pkg.mathx").triple
    by_name = ns.Task(triple, image=IMAGE, include_libs=["demo_pkg"])
    again = ns.Task._from_record(by_name._record(progress, backend, BUCKET))
    assert again(4) == 12


def test_a_record_keeps_which_arguments_are_needed() -> None:
    progress = task_module.Progress(quiet=True, logger_name="test")
    fake = FakeBackend(Path("."))
    record = double_task._record(progress, fake, BUCKET)
    copy = ns.Task._from_record(record)
    assert str(inspect.signature(copy)) == "(x, times=Ellipsis)"
    with pytest.raises(ns.ValidationError, match="missing"):
        copy._bind((), {})


def test_a_changed_payload_is_refused(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = backend.put

    def tampering(bucket: object, key: str, data: bytes) -> None:
        if key.endswith("payload.json"):
            data = data.replace(b'"args": [21]', b'"args": [22]')
        original(bucket, key, data)  # type: ignore[arg-type]

    monkeypatch.setattr(backend, "put", tampering)
    with pytest.raises(ns.TaskFailed, match="not what was submitted"):
        ns.Task(double, image=IMAGE)(21)


def test_the_login_is_found_in_the_environment_or_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "prefect.settings", None)
    monkeypatch.delenv("PREFECT_API_AUTH_STRING", raising=False)
    monkeypatch.setattr(flow_module, "PREFECT_AUTH_FILE", tmp_path / "auth")
    assert flow_module._prefect_login() is None
    (tmp_path / "auth").write_text("file-user:file-pw\n")
    assert flow_module._prefect_login() == "file-user:file-pw"
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "env-user:env-pw")
    assert flow_module._prefect_login() == "env-user:env-pw"


class _Reply:
    def __init__(self, body: object = "3.9.5") -> None:
        self.body = json.dumps(body).encode()

    def __enter__(self) -> "_Reply":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def test_a_refused_login_fails_before_anything_is_started(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str | None] = []

    def refuse(request: urllib.request.Request, timeout: float) -> _Reply:
        seen.append(request.get_header("Authorization"))
        raise urllib.error.HTTPError(API, 401, "Unauthorized", {}, None)  # type: ignore[arg-type]

    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    check = CHECK_PREFECT
    with pytest.raises(ns.AccessError, match="asks for a login") as caught:
        check(API, None)
    assert "~/.config/prefect/auth" in str(caught.value.recovery)
    with pytest.raises(ns.AccessError, match="refused the login"):
        check(API, "me:wrong")
    assert seen == [None, "Basic bWU6d3Jvbmc="]

    # submit makes the check first: nothing is written or started.
    monkeypatch.setattr(flow_module, "_check_prefect", check)
    with pytest.raises(ns.AccessError, match="refused the login"):
        ns.PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
    assert credentials == []
    assert transport.bodies == []


def test_an_accepted_login_or_an_unreachable_server_is_not_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda request, timeout: _Reply()
    )
    assert CHECK_PREFECT(API, "me:pw") == "3.9.5"

    def unreachable(request: object, timeout: float) -> _Reply:
        raise urllib.error.URLError("no route")

    monkeypatch.setattr(urllib.request, "urlopen", unreachable)
    assert CHECK_PREFECT(API, None) is None

    def missing(request: object, timeout: float) -> _Reply:
        raise urllib.error.HTTPError(API, 404, "Not Found", {}, None)  # type: ignore[arg-type]

    monkeypatch.setattr(urllib.request, "urlopen", missing)
    CHECK_PREFECT(API, None)


def test_a_function_with_the_flow_decorator_is_accepted() -> None:
    prefect = pytest.importorskip("prefect")

    @prefect.flow(name="Decorated Flow")
    def decorated(epochs=1):
        return double_task(epochs)

    flow = ns.PrefectFlow(decorated, flow_name="Other Name", api_url=API)
    assert flow.flow_name == "Other Name"
    assert flow._source.startswith("def decorated(epochs=1):")


def test_the_default_image_pins_the_prefect_it_checks() -> None:
    assert flow_module._pinned_prefect() == "3.9.5"
    assert "prefect==3.9.5" in flow_module.DOCKERFILE


def test_an_older_prefect_server_is_refused() -> None:
    check = flow_module._check_versions
    check(API, "3.9.5", "3.9.5", "here")
    check(API, "3.10.0", "3.9.5", "here")
    # Unknown versions are not checked.
    check(API, None, "3.9.5", "here")
    check(API, "3.8.8", None, "here")
    check(API, "dev", "3.9.5", "here")
    with pytest.raises(ns.ValidationError) as caught:
        check(API, "3.8.8", "3.9.5", "in the flow job's image")
    assert "runs Prefect 3.8.8, older than the Prefect 3.9.5" in str(
        caught.value
    )
    assert "Upgrade the Prefect server to 3.9.5" in str(caught.value.recovery)


def test_submit_stops_when_the_server_is_older_than_the_image(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport.run = False
    monkeypatch.setattr(flow_module, "_check_prefect", lambda *args: "3.8.8")
    with pytest.raises(ns.ValidationError, match="older than"):
        ns.PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
    assert credentials == []
    assert transport.bodies == []
    # Another image's Prefect is not known, so it is not checked.
    ns.PrefectFlow(pipeline, flow_name="f", api_url=API, image=IMAGE).submit()
    assert len(transport.bodies) == 1


def test_the_link_goes_to_the_prefect_ui(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    link = flow_module._ui_url
    assert (
        link("https://prefect.test/api", "r-1")
        == "https://prefect.test/runs/flow-run/r-1"
    )
    assert (
        link("https://prefect.test/api/", "r-1")
        == "https://prefect.test/runs/flow-run/r-1"
    )
    monkeypatch.setenv("PREFECT_UI_URL", "https://ui.test/")
    assert link(API, "r-1") == "https://ui.test/runs/flow-run/r-1"


def test_the_flow_run_is_created_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[tuple[str, object, str | None]] = []

    def answer(request: urllib.request.Request, timeout: float) -> _Reply:
        body = json.loads(request.data)  # type: ignore[arg-type]
        sent.append(
            (request.full_url, body, request.get_header("Authorization"))
        )
        if request.full_url.endswith("/flows/"):
            return _Reply({"id": "flow-1"})
        return _Reply({"id": "run-1"})

    monkeypatch.setattr(urllib.request, "urlopen", answer)
    run_id = flow_module._create_flow_run(
        API, "me:pw", "My Flow", "20261126-x-sweep", {"epochs": 3}
    )
    assert run_id == "run-1"
    auth = "Basic bWU6cHc="
    assert sent == [
        (f"{API}/flows/", {"name": "My Flow"}, auth),
        (
            f"{API}/flow_runs/",
            {
                "flow_id": "flow-1",
                "name": "20261126-x-sweep",
                "parameters": {"epochs": 3},
                "state": {"type": "PENDING", "name": "Pending"},
            },
            auth,
        ),
    ]

    def unreachable(request: object, timeout: float) -> _Reply:
        raise urllib.error.URLError("no route")

    monkeypatch.setattr(urllib.request, "urlopen", unreachable)
    assert flow_module._create_flow_run(API, None, "f", "r", {}) is None


def test_submit_sends_the_flow_run_and_returns_its_link(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport.run = False
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    created: list[tuple[object, ...]] = []

    def create(*args: object) -> str:
        created.append(args)
        return "run-1"

    monkeypatch.setattr(flow_module, "_create_flow_run", create)
    flow = ns.PrefectFlow(pipeline, flow_name="My Flow", api_url=API)
    run = flow.submit(3)
    assert created == [
        (API, "me:pw", "My Flow", f"{STAMP}-My Flow", {"epochs": 3})
    ]
    assert run.flow_run_id == "run-1"
    assert run.flow_run_url == "https://prefect.test/runs/flow-run/run-1"
    folder = f"flows/my-flow/{STAMP}-my-flow"
    sent = _payload(backend, f"{folder}/flow")["kwargs"]["flow"]  # type: ignore[index]
    assert sent["flow_run_id"] == "run-1"
    assert sent["parameters"] == {"epochs": 3}
    assert sent["args"] == [3]
    assert sent["folder"] == folder


def test_a_flow_job_that_does_not_start_crashes_its_flow_run(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flow_module, "_create_flow_run", lambda *a: "run-1")
    crashed: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        flow_module, "_mark_crashed", lambda *args: crashed.append(args)
    )

    def refuse(*args: object, **kwargs: object) -> object:
        raise ns.CapacityError("no room")

    monkeypatch.setattr(transport, "create", refuse)
    with pytest.raises(ns.CapacityError):
        ns.PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
    assert len(crashed) == 1
    assert crashed[0][:3] == (API, "me:pw", "run-1")
    assert "did not start" in str(crashed[0][3])


def test_a_flow_here_needs_prefect_here(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "prefect", None)
    flow = ns.PrefectFlow(
        pipeline, flow_name="f", api_url=API, remote_job=False
    )
    assert flow.remote_job is False
    for start in (flow.submit, flow):
        with pytest.raises(ns.ValidationError, match="needs the prefect") as e:
            start()
        assert e.value.recovery == "Install it: pip install prefect"
    assert transport.bodies == []


def doubled(epochs=2):
    """Double two numbers, one of them after the other."""
    first = double_task.submit(epochs)
    return {"first": first.result(), "second": double_task(epochs + 1)}


def test_a_flow_runs_here_as_the_flow_run_made_at_submit(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("prefect")
    from prefect.client.orchestration import get_client
    from prefect.settings import PREFECT_API_URL
    from prefect.testing.utilities import prefect_test_harness

    monkeypatch.setattr(flow_module, "_prefect_login", lambda: None)
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    with prefect_test_harness():
        api = PREFECT_API_URL.value()
        flow = ns.PrefectFlow(
            doubled, flow_name="Here", api_url=api, remote_job=False
        )
        run = flow.submit(epochs=3)
        assert isinstance(run, flow_module.FlowRun)
        assert run.job is None
        assert run.name == STAMP
        assert run.title == f"{STAMP}-Here"
        assert run.flow_run_id is not None
        assert run.flow_run_url == (
            api[: -len("/api")] + f"/runs/flow-run/{run.flow_run_id}"
        )
        assert run.result() == {"first": 6, "second": 8}
        # Called, it runs in this thread. Without a server to create the
        # run at submit, Prefect creates it when the flow starts.
        monkeypatch.setattr(
            flow_module, "_create_flow_run", lambda *args: None
        )
        assert flow(epochs=1) == {"first": 2, "second": 4}

        with get_client(sync_client=True) as client:
            runs = {str(item.id): item for item in client.read_flow_runs()}
        # One flow run each: the one made at submit was used, not a new
        # one.
        assert len(runs) == 2
        made = runs[run.flow_run_id]
        assert made.name == f"{STAMP}-Here"
        assert made.state is not None and made.state.is_completed()
        assert made.parameters == {"epochs": 3}

    # Each task's job keeps its files in its flow run's folder. The
    # second run was launched in the same second, so its name has -2.
    folder = f"flows/here/{STAMP}-here"
    assert run.output_uri == f"s3://{BUCKET.name}/{folder}/"
    payloads = [key for key in backend.puts if key.endswith("payload.json")]
    assert len(payloads) == 4
    for name in (f"{STAMP}-here", f"{STAMP}-2-here"):
        prefix = f"flows/here/{name}/double-"
        assert sum(key.startswith(prefix) for key in payloads) == 2
