"""In-memory transport and clock for tests."""

import copy
from collections.abc import Mapping

from nebius_serverless.core.transport import CreateResult


class FakeTransport:
    """Records create, get, and cancel. Does not contact Nebius."""

    def __init__(self) -> None:
        self.bodies: list[dict[str, object]] = []
        self.dry_runs: list[dict[str, object]] = []
        self.keys: list[str] = []
        # Results returned, in order, before the normal behaviour.
        self.outcomes: list[CreateResult] = []
        self.gets: list[str] = []
        self.cancels: list[str] = []
        self.drop_id = False
        self.failure: str | None = None
        self.conflict_id: str | None = None
        self.state: str | None = "PROVISIONING"
        self.states = ["RUNNING"]
        self._n = 0
        self._get_i = 0

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str = "",
    ) -> CreateResult:
        self.bodies.append(copy.deepcopy(dict(body)))
        self.keys.append(idempotency_key)
        if self.outcomes:
            return self.outcomes.pop(0)
        self._n += 1
        if self.failure is not None:
            return CreateResult(
                accepted=False,
                failure=self.failure,
                conflict_id=self.conflict_id,
            )
        if self.drop_id:
            return CreateResult(accepted=True, resource_id=None)
        return CreateResult(
            resource_id=f"job-{self._n}",
            accepted=True,
            state=self.state,
        )

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Records the body. Creates nothing; ``failure`` rejects it."""
        self.dry_runs.append(copy.deepcopy(dict(body)))
        if self.failure is not None:
            return CreateResult(accepted=False, failure=self.failure)
        return CreateResult(accepted=True)

    def get(self, job_id: str) -> dict[str, object]:
        self.gets.append(job_id)
        index = min(self._get_i, len(self.states) - 1)
        self._get_i += 1
        state, _, code = self.states[index].partition(":")
        status: dict[str, object] = {"state": state}
        if code:
            status["state_details"] = {"code": code}
        return {
            "metadata": {"id": job_id, "name": "saved"},
            "status": status,
            "spec": {"image": "python:3.12-slim"},
        }

    def cancel(self, job_id: str) -> dict[str, object]:
        self.cancels.append(job_id)
        return {"id": job_id, "accepted": True}


class FakeClock:
    """Monotonic clock that advances only when sleep is called."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float, stop: object = None) -> None:
        self.now += seconds


class FakeEndpointTransport:
    """Records endpoint create, get, stop, start, and delete.

    Does not contact Nebius.

    ``states`` and ``urls`` are consumed one per get, and the last entry
    repeats. A ``urls`` entry may hold several space-separated URLs.
    """

    def __init__(self) -> None:
        self.bodies: list[dict[str, object]] = []
        self.dry_runs: list[dict[str, object]] = []
        self.keys: list[str] = []
        # Results returned, in order, before the normal behaviour.
        self.outcomes: list[CreateResult] = []
        self.gets: list[str] = []
        self.stops: list[str] = []
        self.starts: list[str] = []
        self.reveals: list[str] = []
        self.deletes: list[str] = []
        self.drop_id = False
        self.failure: str | None = None
        self.state: str | None = "PROVISIONING"
        self.states = ["RUNNING"]
        self.urls: list[str | None] = ["https://ep.example.nebius.cloud"]
        self.code: str | None = None
        self.message: str | None = None
        # spec.ports in fetched records; None leaves them out.
        self.spec_ports: list[dict[str, object]] | None = None
        self._n = 0
        self._get_i = 0

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str = "",
    ) -> CreateResult:
        self.bodies.append(copy.deepcopy(dict(body)))
        self.keys.append(idempotency_key)
        if self.outcomes:
            return self.outcomes.pop(0)
        self._n += 1
        if self.failure is not None:
            return CreateResult(
                accepted=False,
                failure=self.failure,
                code=self.code,
                message=self.message,
            )
        if self.drop_id:
            return CreateResult(accepted=True, resource_id=None)
        return CreateResult(
            resource_id=f"endpoint-{self._n}",
            accepted=True,
            state=self.state,
        )

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Records the body. Creates nothing; ``failure`` rejects it."""
        self.dry_runs.append(copy.deepcopy(dict(body)))
        if self.failure is not None:
            return CreateResult(
                accepted=False,
                failure=self.failure,
                code=self.code,
                message=self.message,
            )
        return CreateResult(accepted=True)

    def get(self, endpoint_id: str) -> dict[str, object]:
        self.gets.append(endpoint_id)
        i = self._get_i
        self._get_i += 1
        state = self.states[min(i, len(self.states) - 1)]
        url = self.urls[min(i, len(self.urls) - 1)]
        status: dict[str, object] = {"state": state}
        if url is not None:
            status["public_endpoints"] = url.split()
        spec: dict[str, object] = {"image": "vllm/vllm-openai:v0.19.1"}
        if self.spec_ports is not None:
            spec["ports"] = self.spec_ports
        return {
            "metadata": {"id": endpoint_id, "name": "saved-endpoint"},
            "spec": spec,
            "status": status,
        }

    def stop(self, endpoint_id: str) -> dict[str, object]:
        self.stops.append(endpoint_id)
        return {"id": endpoint_id, "resource_id": endpoint_id}

    def reveal_token(self, endpoint_id: str) -> str | None:
        self.reveals.append(endpoint_id)
        index = int(endpoint_id.rsplit("-", 1)[1]) - 1
        spec = self.bodies[index]["spec"]
        assert isinstance(spec, dict)
        token = spec.get("auth_token")
        return token if isinstance(token, str) else None

    def start(self, endpoint_id: str) -> dict[str, object]:
        self.starts.append(endpoint_id)
        return {"id": endpoint_id, "resource_id": endpoint_id}

    def delete(self, endpoint_id: str) -> dict[str, object]:
        self.deletes.append(endpoint_id)
        return {"id": endpoint_id, "resource_id": endpoint_id}
