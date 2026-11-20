"""One SDK per process and profile; regions looked up once; ns.close()."""

import threading
from types import SimpleNamespace

import pytest

import nebius_serverless as ns
from nebius_serverless import session
from nebius_serverless.sdk_transport import (
    SdkEndpointTransport,
    SdkTransport,
    project_region,
)


class FakeSdk:
    def __init__(self, profile: str | None) -> None:
        self.profile = profile
        self.closed = 0

    def sync_close(self) -> None:
        self.closed += 1


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[FakeSdk]:
    made: list[FakeSdk] = []

    def fake_open(
        profile: str | None, credentials_file: object = None
    ) -> FakeSdk:
        sdk = FakeSdk(profile)
        made.append(sdk)
        return sdk

    monkeypatch.setattr(session, "_sdks", {})
    monkeypatch.setattr(session, "_regions", {})
    monkeypatch.setattr(session, "_hooked", False)
    monkeypatch.setattr(session, "_open", fake_open)
    hooks: list[object] = []
    monkeypatch.setattr(session.atexit, "register", hooks.append)
    monkeypatch.setattr(session, "_hooks_seen", hooks, raising=False)
    return made


def test_many_transports_share_one_sdk_per_profile(
    opened: list[FakeSdk],
) -> None:
    for _ in range(50):
        SdkTransport(profile="a", project="p1")._session()
        SdkEndpointTransport(profile="a", project="p2")._session()
    assert len(opened) == 1
    SdkTransport(profile="b", project="p1")._session()
    assert [sdk.profile for sdk in opened] == ["a", "b"]
    assert len(session._hooks_seen) == 1  # type: ignore[attr-defined]


def test_a_forked_child_opens_its_own(
    opened: list[FakeSdk], monkeypatch: pytest.MonkeyPatch
) -> None:
    session.get_sdk(None)
    monkeypatch.setattr(session.os, "getpid", lambda: 999_999)
    session.get_sdk(None)
    assert len(opened) == 2


def test_close_closes_each_once_and_the_next_call_reopens(
    opened: list[FakeSdk],
) -> None:
    session.get_sdk("a")
    session.get_sdk("b")
    ns.close()
    assert [sdk.closed for sdk in opened] == [1, 1]
    ns.close()
    assert [sdk.closed for sdk in opened] == [1, 1]
    session.get_sdk("a")
    assert len(opened) == 3


def test_threads_share_one_sdk(opened: list[FakeSdk]) -> None:
    barrier = threading.Barrier(8)

    def work() -> None:
        barrier.wait()
        session.get_sdk("a")

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(opened) == 1


def test_a_project_region_is_looked_up_once(
    opened: list[FakeSdk], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    class FakeProjects:
        def __init__(self, sdk: object) -> None:
            assert isinstance(sdk, FakeSdk)

        def get(self, request: object) -> object:
            calls.append(getattr(request, "id", ""))
            project = SimpleNamespace(spec=SimpleNamespace(region="eu-north1"))
            return SimpleNamespace(wait=lambda: project)

    import nebius.api.nebius.iam.v2 as iam

    monkeypatch.setattr(iam, "ProjectServiceClient", FakeProjects)
    for _ in range(5):
        assert project_region(None, "project-test-0001") == "eu-north1"
    assert project_region(None, "project-test-0002") == "eu-north1"
    assert calls == ["project-test-0001", "project-test-0002"]
    assert len(opened) == 1
