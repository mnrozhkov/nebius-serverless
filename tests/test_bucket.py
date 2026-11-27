"""The task bucket: the default name, a chosen bucket, and its root."""

from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from grpc import StatusCode
from nebius.aio.service_error import RequestError, RequestStatusExtended

import nebius_serverless as ns
from nebius_serverless.tasks import storage
from nebius_serverless.tasks.driver import Driver

PROJECT = "project-e00abc"


def _error(code: StatusCode) -> RequestError:
    return RequestError(
        RequestStatusExtended(
            code=code,
            message="",
            details=[],
            request_id="req-1",
            trace_id="trace-1",
            service_errors=[],
        )
    )


class _Done:
    def __init__(self, value: object = None) -> None:
        self.value = value

    def wait(self) -> object:
        return self.value

    def sync_wait(self) -> None:
        return None


class FakeBuckets:
    """``BucketServiceClient``: buckets by name, and the creates sent."""

    def __init__(self, existing: dict[str, str]) -> None:
        self.existing = dict(existing)
        self.creates: list[object] = []
        self.denied = False

    def get_by_name(self, request: object) -> _Done:
        if self.denied:
            raise _error(StatusCode.PERMISSION_DENIED)
        name = request.name  # type: ignore[attr-defined]
        if name not in self.existing:
            raise _error(StatusCode.NOT_FOUND)
        return _Done(
            SimpleNamespace(
                metadata=SimpleNamespace(id=self.existing[name]),
                status=SimpleNamespace(
                    domain_name="storage.test", region="eu-north1"
                ),
            )
        )

    def create(self, request: object) -> _Done:
        self.creates.append(request)
        metadata = request.metadata  # type: ignore[attr-defined]
        self.existing[metadata.name] = f"storagebucket-{metadata.name}"
        return _Done(_Done())


class FakeS3:
    """``_s3``: objects by full key. ``refuse`` makes a method fail."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.refuse: set[str] = set()

    def __call__(
        self, bucket: storage.Bucket, method: str, key: str, body: bytes = b""
    ) -> bytes | None:
        if method in self.refuse:
            raise ns.AccessError("refused", operation="task")
        address = f"{bucket.name}/{key}"
        if method == "PUT":
            self.objects[address] = body
            return b""
        return self.objects.get(address)


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv(storage.BUCKET_ENV, raising=False)
    monkeypatch.delenv(storage.ROOT_ENV, raising=False)
    storage.forget_buckets()
    yield
    storage.forget_buckets()
    ns.configure()


@pytest.fixture
def buckets(monkeypatch: pytest.MonkeyPatch) -> FakeBuckets:
    fake = FakeBuckets({"santa-cards-course": "storagebucket-santa"})
    monkeypatch.setattr(
        "nebius.api.nebius.storage.v1.BucketServiceClient", lambda sdk: fake
    )
    monkeypatch.setattr(
        "nebius_serverless.core.session.get_sdk", lambda profile: object()
    )
    return fake


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> FakeS3:
    fake = FakeS3()
    monkeypatch.setattr(storage, "_s3", fake)
    return fake


def test_the_default_bucket_is_created_with_the_label(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    backend = storage.SdkTaskBackend(None)
    bucket, created = backend.ensure_bucket(PROJECT)
    assert created is True
    assert bucket.name == "ns-workspace-e00abc"
    assert (bucket.root, bucket.chosen) == ("", False)
    (request,) = buckets.creates
    metadata = request.metadata  # type: ignore[attr-defined]
    assert metadata.name == "ns-workspace-e00abc"
    assert dict(metadata.labels) == {"managed-by": "nebius-serverless"}
    assert bucket.key("tasks/train/1") == "tasks/train/1"
    # The default bucket is not written to check access.
    assert s3.objects == {}


def test_a_chosen_bucket_is_checked_and_never_created(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    ns.configure(bucket="santa-cards-course")
    backend = storage.SdkTaskBackend(None)
    chosen = storage.chosen_bucket()
    bucket, created = backend.ensure_bucket(PROJECT, chosen)
    assert created is False
    assert buckets.creates == []
    assert (bucket.name, bucket.id) == (
        "santa-cards-course",
        "storagebucket-santa",
    )
    assert (bucket.root, bucket.chosen) == ("ns", True)
    assert list(s3.objects) == ["santa-cards-course/ns/tasks/.access-check"]
    backend.put(bucket, "tasks/train/1/payload.json", b"{}")
    assert "santa-cards-course/ns/tasks/train/1/payload.json" in s3.objects
    assert storage.bucket_env(bucket) == {
        "NS_BUCKET": "santa-cards-course",
        "NS_BUCKET_PREFIX": "ns",
    }


def test_the_access_check_runs_once_per_process(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    backend = storage.SdkTaskBackend(None)
    backend.ensure_bucket(PROJECT, "santa-cards-course")
    s3.objects.clear()
    backend.ensure_bucket(PROJECT, "santa-cards-course")
    assert s3.objects == {}


def test_a_missing_chosen_bucket_raises_and_creates_nothing(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    backend = storage.SdkTaskBackend(None)
    with pytest.raises(ns.AccessError, match="no-such-bucket") as caught:
        backend.ensure_bucket(PROJECT, "no-such-bucket")
    assert "never creates" in str(caught.value)
    assert buckets.creates == []
    assert s3.objects == {}


def test_a_chosen_bucket_that_cannot_be_written_names_the_right(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    s3.refuse = {"PUT"}
    with pytest.raises(ns.AccessError, match="Cannot write to the bucket"):
        storage.SdkTaskBackend(None).ensure_bucket(
            PROJECT, "santa-cards-course"
        )


def test_a_chosen_bucket_that_cannot_be_read_names_the_right(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    s3.refuse = {"GET"}
    with pytest.raises(ns.AccessError, match="Cannot read from the bucket"):
        storage.SdkTaskBackend(None).ensure_bucket(
            PROJECT, "santa-cards-course"
        )


def test_no_right_to_create_the_default_suggests_choosing_one(
    buckets: FakeBuckets, s3: FakeS3
) -> None:
    buckets.denied = True
    with pytest.raises(ns.AccessError) as caught:
        storage.SdkTaskBackend(None).ensure_bucket(PROJECT)
    assert "ns.configure(bucket=...)" in caught.value.recovery


def test_the_task_bucket_wins_then_configure_then_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert storage.chosen_bucket() is None
    monkeypatch.setenv(storage.BUCKET_ENV, "from-env")
    assert storage.chosen_bucket() == "from-env"
    ns.configure(bucket="from-configure")
    assert storage.chosen_bucket() == "from-configure"
    assert storage.chosen_bucket("from-task") == "from-task"


def test_the_prefix_can_be_changed_or_turned_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert storage.chosen_root() == "ns"
    ns.configure(prefix="")
    assert storage.chosen_root() == ""
    ns.configure(prefix="/lab/runs/")
    assert storage.chosen_root() == "lab/runs"
    ns.configure()
    # In a job, the variable carries the root the client used.
    monkeypatch.setenv(storage.ROOT_ENV, "")
    assert storage.chosen_root() == ""


def test_an_empty_bucket_name_is_refused() -> None:
    with pytest.raises(ns.ValidationError, match="bucket name"):
        ns.configure(bucket="")


def _same(x):
    return x


FIRST = ns.Task(_same, image="python:3.12-slim", bucket="one", name="first")
SECOND = ns.Task(_same, image="python:3.12-slim", bucket="two", name="second")
THIRD = ns.Task(_same, image="python:3.12-slim", bucket="one", name="third")


def _mixed(x):
    return SECOND(FIRST(x))


def _matched(x):
    return THIRD(FIRST(x))


def test_a_pipeline_with_two_buckets_is_refused() -> None:
    def driver(function: object) -> Driver:
        return Driver(
            function,  # type: ignore[arg-type]
            name="pipeline",
            run_name="run",
            image="python:3.12-slim",
            job_options={},
            operation="Flow",
        )

    with pytest.raises(ns.ValidationError, match="different buckets"):
        driver(_mixed).chosen_bucket()
    assert driver(_matched).chosen_bucket() == "one"
