"""The Prefect integration: PrefectFlow, Prefect tasks, artifacts."""

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
from nebius_serverless import task as task_module
from nebius_serverless import task_driver as driver_module
from nebius_serverless.image import Image
from nebius_serverless.integrations.prefect import FlowRun, PrefectFlow
from nebius_serverless.integrations.prefect import (
    artifacts as artifacts_module,
)
from nebius_serverless.integrations.prefect import flow as flow_module
from nebius_serverless.integrations.prefect import login as login_module
from nebius_serverless.integrations.prefect.adapter import prefect_task
from nebius_serverless.task import task_from_record, task_record

API = "https://prefect.test/api"
# The real check. The credentials fixture replaces it on the module.
CHECK_PREFECT = login_module.check_server
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

    monkeypatch.setattr(driver_module, "store_credentials", store)
    monkeypatch.setattr(driver_module, "access_token", lambda profile: "tok")
    monkeypatch.setattr(login_module, "find_login", lambda: "me:pw")
    monkeypatch.setattr(login_module, "check_server", lambda url, login: None)
    monkeypatch.setattr(login_module, "create_flow_run", lambda *args: None)
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
    flow = PrefectFlow(pipeline, flow_name="Pipe Line", api_url=API)
    assert sorted(flow._driver.tasks) == ["double_task", "hello"]
    assert flow._driver.constants == {"NAME": NAME, "RATES": RATES}
    assert flow.flow_name == "Pipe Line"
    assert flow.run_name == "Pipe Line"
    assert flow.name == "pipe-line"
    named = PrefectFlow(
        pipeline, flow_name="pipeline", run_name="lr sweep", api_url=API
    )
    assert named.run_name == "lr sweep"
    assert str(inspect.signature(flow)) == "(epochs=5)"
    assert flow.__doc__ == "Run the tasks."
    assert "def pipeline(epochs=5):" in flow._driver.source


def test_other_names_from_the_file_are_refused() -> None:
    with pytest.raises(ns.ValidationError) as caught:
        PrefectFlow(uses_a_helper, flow_name="f", api_url=API)
    assert "double, helper, inspect" in str(caught.value)
    with pytest.raises(ns.ValidationError, match="must be a Python function"):
        PrefectFlow(len, flow_name="f", api_url=API)


def test_it_needs_a_prefect_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    with pytest.raises(ns.ValidationError, match="api_url"):
        PrefectFlow(pipeline, flow_name="f")
    monkeypatch.setenv("PREFECT_API_URL", API)
    assert PrefectFlow(pipeline, flow_name="f").api_url == API


def test_the_flow_name_is_required() -> None:
    with pytest.raises(TypeError, match="flow_name"):
        PrefectFlow(pipeline, api_url=API)  # type: ignore[call-arg]
    with pytest.raises(ns.ValidationError, match="flow_name"):
        PrefectFlow(pipeline, flow_name=" ", api_url=API)
    with pytest.raises(ns.ValidationError, match="run_name"):
        PrefectFlow(pipeline, flow_name="f", run_name="", api_url=API)


def test_the_image_has_a_default() -> None:
    flow = PrefectFlow(pipeline, flow_name="f", api_url=API)
    assert flow.image is PrefectFlow.default_image
    assert flow.image.name == "prefect-flow"
    assert "prefect==" in flow_module.DOCKERFILE
    custom = PrefectFlow(pipeline, flow_name="f", api_url=API, image=IMAGE)
    assert custom.image == IMAGE


def test_submit_sends_the_function_the_tasks_and_the_credentials(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
) -> None:
    transport.run = False
    flow = PrefectFlow(
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
    image = PrefectFlow.default_image
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
    assert run.output_uri == f"s3://{BUCKET.name}/{folder}/"
    assert pointer["run"] == key
    raw = backend.get(BUCKET, f"{key}/payload.json")
    assert raw is not None
    assert pointer["sha256"] == hashlib.sha256(raw).hexdigest()

    # The payload runs this package's flow function with the user's
    # function, its arguments, its constants and the task records.
    payload = _payload(backend, key)
    assert (
        payload["module"] == "nebius_serverless.integrations.prefect.adapter"
    )
    assert payload["qualname"] == "run_in_job"
    sent = payload["kwargs"]["program"]  # type: ignore[index]
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
    assert "nebius_serverless/integrations/prefect/flow.py" in names
    assert "nebius_serverless/task.py" in names
    assert not any(name.endswith(".pyc") for name in names)


def test_a_server_without_a_login_gets_only_the_token(
    backend: FakeBackend,
    transport: RunningTransport,
    credentials: list[dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport.run = False
    monkeypatch.setattr(login_module, "find_login", lambda: None)
    flow = PrefectFlow(pipeline, flow_name="pipeline", api_url=API)
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
    flow = PrefectFlow(pipeline, flow_name="f", api_url=API)
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
        json.dumps(task_record(original, progress, backend, BUCKET))
    )
    assert record["libs"].startswith("libs/")
    copy = task_from_record(record)
    assert copy.name == "unpacks"
    assert str(inspect.signature(copy)) == "(x)"
    assert copy(1) == original(1) == 2
    # The copy sent no library of its own.
    assert len([key for key in backend.puts if key.startswith("libs/")]) == 1

    triple = importlib.import_module("demo_pkg.mathx").triple
    by_name = ns.Task(triple, image=IMAGE, include_libs=["demo_pkg"])
    again = task_from_record(task_record(by_name, progress, backend, BUCKET))
    assert again(4) == 12


def test_a_record_keeps_which_arguments_are_needed() -> None:
    progress = task_module.Progress(quiet=True, logger_name="test")
    fake = FakeBackend(Path("."))
    record = task_record(double_task, progress, fake, BUCKET)
    copy = task_from_record(record)
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
    monkeypatch.setattr(login_module, "PREFECT_AUTH_FILE", tmp_path / "auth")
    assert login_module.find_login() is None
    (tmp_path / "auth").write_text("file-user:file-pw\n")
    assert login_module.find_login() == "file-user:file-pw"
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "env-user:env-pw")
    assert login_module.find_login() == "env-user:env-pw"


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
    monkeypatch.setattr(login_module, "check_server", check)
    with pytest.raises(ns.AccessError, match="refused the login"):
        PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
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

    flow = PrefectFlow(decorated, flow_name="Other Name", api_url=API)
    assert flow.flow_name == "Other Name"
    assert flow._driver.source.startswith("def decorated(epochs=1):")


def test_the_default_image_pins_the_prefect_it_checks() -> None:
    assert flow_module._pinned_version() == "3.9.5"
    assert "prefect==3.9.5" in flow_module.DOCKERFILE


def test_an_older_prefect_server_is_refused() -> None:
    check = login_module.check_versions
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
    monkeypatch.setattr(login_module, "check_server", lambda *args: "3.8.8")
    with pytest.raises(ns.ValidationError, match="older than"):
        PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
    assert credentials == []
    assert transport.bodies == []
    # Another image's Prefect is not known, so it is not checked.
    PrefectFlow(pipeline, flow_name="f", api_url=API, image=IMAGE).submit()
    assert len(transport.bodies) == 1


def test_the_link_goes_to_the_prefect_ui(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    link = login_module.ui_url
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
    run_id = login_module.create_flow_run(
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
    assert login_module.create_flow_run(API, None, "f", "r", {}) is None


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

    monkeypatch.setattr(login_module, "create_flow_run", create)
    flow = PrefectFlow(pipeline, flow_name="My Flow", api_url=API)
    run = flow.submit(3)
    assert created == [
        (API, "me:pw", "My Flow", f"{STAMP}-My Flow", {"epochs": 3})
    ]
    assert run.flow_run_id == "run-1"
    assert run.flow_run_url == "https://prefect.test/runs/flow-run/run-1"
    folder = f"flows/my-flow/{STAMP}-my-flow"
    sent = _payload(backend, f"{folder}/flow")["kwargs"]["program"]  # type: ignore[index]
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
    monkeypatch.setattr(login_module, "create_flow_run", lambda *a: "run-1")
    crashed: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        login_module, "mark_crashed", lambda *args: crashed.append(args)
    )

    def refuse(*args: object, **kwargs: object) -> object:
        raise ns.CapacityError("no room")

    monkeypatch.setattr(transport, "create", refuse)
    with pytest.raises(ns.CapacityError):
        PrefectFlow(pipeline, flow_name="f", api_url=API).submit()
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
    flow = PrefectFlow(pipeline, flow_name="f", api_url=API, remote_job=False)
    assert flow.remote_job is False
    for start in (flow.submit, flow):
        with pytest.raises(ns.ValidationError, match="needs the prefect") as e:
            start()
        assert (
            e.value.recovery
            == 'Install it: pip install "nebius-serverless[prefect]"'
        )
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

    monkeypatch.setattr(login_module, "find_login", lambda: None)
    monkeypatch.delenv("PREFECT_UI_URL", raising=False)
    with prefect_test_harness():
        api = PREFECT_API_URL.value()
        flow = PrefectFlow(
            doubled, flow_name="Here", api_url=api, remote_job=False
        )
        run = flow.submit(epochs=3)
        assert isinstance(run, FlowRun)
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
            login_module, "create_flow_run", lambda *args: None
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


def test_a_prefect_task_keeps_the_name_signature_and_result(
    backend: FakeBackend, transport: RunningTransport
) -> None:
    pytest.importorskip("prefect")
    wrapped = prefect_task(
        ns.Task(double, name="Double It", image=IMAGE), retries=2
    )
    assert wrapped.name == "double-it"
    assert wrapped.retries == 2
    assert str(inspect.signature(wrapped.fn)) == "(x, times=2)"
    assert wrapped.fn(21) == 42
    assert transport.bodies[0]["metadata"] == {
        "parent_id": "project-1",
        "name": f"double-it-{STAMP}",
    }


def test_a_prefect_task_run_shows_its_job_and_inputs(
    backend: FakeBackend,
    transport: RunningTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = ns.Task(double, image=IMAGE).submit(4, times=5)
    monkeypatch.delenv(task_module.CONSOLE_ENV, raising=False)
    shown = artifacts_module.job_markdown(run)
    assert "- Job: `job-1`\n" in shown
    assert f"- Name: `double-{STAMP}`" in shown
    assert "- Logs: `nebius ai job logs job-1`" in shown
    assert f"- Files: `{run.output_uri}`" in shown
    inputs = artifacts_module.values_markdown("Inputs of `double`", run.inputs)
    assert inputs == "### Inputs of `double`\n\n- x: `4`\n- times: `5`"
    assert task_module.console_url("project-1", "job-1") is None

    monkeypatch.setenv(
        task_module.CONSOLE_ENV, "https://console.test/{project}/jobs/{job}"
    )
    link = "https://console.test/project-1/jobs/job-1"
    assert task_module.console_url("project-1", "job-1") == link
    assert f"([open in the console]({link}))" in artifacts_module.job_markdown(
        run
    )

    assert artifacts_module.JOB_ARTIFACT == "nebius-job"
    assert artifacts_module.INPUTS_ARTIFACT == "inputs"
    assert artifacts_module.OUTPUTS_ARTIFACT == "outputs"

    shown = artifacts_module.values_markdown
    outputs = shown(
        "Outputs",
        {
            "path": "/ns/tasks/a/files/model.pt",
            "loss": 0.25,
            "ok": True,
            "tags": ["a", "b"],
            "note": "",
            "code": "a `b` c",
        },
    )
    assert outputs.splitlines()[2:] == [
        "- path: `/ns/tasks/a/files/model.pt`",
        "- loss: `0.25`",
        "- ok: `true`",
        '- tags: `["a", "b"]`',
        "- note: (empty)",
        "- code: `` a `b` c ``",
    ]
    assert shown("Outputs", 9).endswith("- Value: `9`")
    assert shown("Inputs", {}).endswith("- (none)")
    assert shown("Outputs", list(range(2000))).endswith("... (cut)`")
    # Outside a Prefect run these do nothing and do not fail.
    artifacts_module.show_started(run)
    artifacts_module.show_finished(run, {"a": 1})


def test_prefect_tasks_need_prefect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "prefect", None)
    with pytest.raises(ns.ValidationError, match="needs the prefect"):
        prefect_task(ns.Task(double, image=IMAGE))
