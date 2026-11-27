"""Nebius Python SDK adapters for jobs and endpoints.

Imported on the call that needs it. ``JobSpec()`` and ``EndpointSpec()`` do not
import this module.
"""

import json
import os
import time
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Literal

from .._version import USER_AGENT as _USER_AGENT
from ..errors import (
    AccessError,
    CapacityError,
    TransportError,
    ValidationError,
    exposed_request_id,
    exposed_trace_id,
    recovery_for,
)
from .recipe import disk_bytes
from .transport import CreateResult, capacity_text

IDEMPOTENCY_HEADER = "x-idempotency-key"
# Definite codes that mean the request itself was wrong: nothing was
# created, and the server's reason is worth showing.
_REJECTED = frozenset(
    {"INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE", "NOT_FOUND"}
)
# stop/start/delete retry ABORTED "operation conflict" this many times.
CONFLICT_ATTEMPTS = 12
CONFLICT_PAUSE = 10.0
_DEFINITE = frozenset(
    {
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "FAILED_PRECONDITION",
        "OUT_OF_RANGE",
        "UNIMPLEMENTED",
        "CANCELLED",
    }
)


class _SdkSession:
    """Calls through the process-wide SDK for this profile (session.py).

    ``project`` is accepted for compatibility. Creates carry the project
    in ``metadata.parent_id`` and gets go by id, so the SDK needs none.
    """

    def __init__(
        self,
        profile: str | None,
        project: str | None = None,
    ) -> None:
        self._profile = profile

    def _session(self) -> object:
        from .session import get_sdk

        return get_sdk(self._profile)


class SdkTransport(_SdkSession):
    """Create, get, and cancel through ``JobServiceClient``.

    The client does not shell out to the ``nebius`` binary and does not
    hand-roll REST. One SDK instance is kept for later get and cancel.
    """

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str,
    ) -> CreateResult:
        """Create once with this key; wait until the id is known."""
        try:
            from nebius.aio.service_error import RequestError
        except ImportError:
            raise TransportError(
                "The Nebius SDK is required to create a job.",
                operation="create",
            ) from None
        try:
            operation = self._client().create(
                build_create_request(body),
                metadata=[(IDEMPOTENCY_HEADER, idempotency_key)],
            ).wait()
        except RequestError as exc:
            return _from_request(exc, sent=False)
        except Exception as exc:
            return CreateResult(
                accepted=False,
                failure="transport",
                request_id=exposed_request_id(exc),
            )
        return _result_when_id_known(operation)

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Send the create with ``dry_run``: Nebius validates it and
        creates nothing."""
        return _dry_run(
            self._client(), build_create_request(body, dry_run=True)
        )

    def get(self, job_id: str) -> dict[str, object]:
        """Fetch one job and return its JSON object."""
        try:
            from nebius.aio.service_error import RequestError
            from nebius.api.nebius.ai.v1 import GetJobRequest
        except ImportError:
            raise TransportError(
                "The Nebius SDK is required to get a job.",
                operation="get",
                resource_id=job_id,
            ) from None
        try:
            job = self._client().get(GetJobRequest(id=job_id)).wait()
        except RequestError as exc:
            _raise_call(exc, operation="get", job_id=job_id)
        except Exception as exc:
            raise TransportError(
                "The get call failed.",
                operation="get",
                resource_id=job_id,
                request_id=exposed_request_id(exc),
            ) from None
        payload = json.loads(
            job.to_json(preserving_proto_field_name=True)
        )
        if not isinstance(payload, dict):
            raise TransportError(
                "The get call failed.",
                operation="get",
                resource_id=job_id,
            )
        return payload

    def cancel(self, job_id: str) -> dict[str, object]:
        """Ask Nebius to cancel. Return when the request is accepted.

        This does not wait for the cancel operation. The job is
        ``CANCELLING`` right after, and stopping the container can take
        minutes. ``get`` reports ``CANCELLED`` once it has stopped.
        """
        try:
            from nebius.aio.service_error import RequestError
            from nebius.api.nebius.ai.v1 import CancelJobRequest
        except ImportError:
            raise TransportError(
                "The Nebius SDK is required to cancel a job.",
                operation="cancel",
                resource_id=job_id,
            ) from None
        try:
            operation = self._client().cancel(
                CancelJobRequest(id=job_id)
            ).wait()
        except RequestError as exc:
            _raise_call(exc, operation="cancel", job_id=job_id)
        except Exception as exc:
            raise TransportError(
                "The cancel call failed.",
                operation="cancel",
                resource_id=job_id,
                request_id=exposed_request_id(exc),
            ) from None
        return {
            "id": job_id,
            "resource_id": _resource_id(operation) or job_id,
        }

    def _client(self) -> object:
        from nebius.api.nebius.ai.v1 import JobServiceClient

        return JobServiceClient(self._session())


class SdkEndpointTransport(_SdkSession):
    """Create, get, stop, and delete through ``EndpointServiceClient``.

    One SDK instance is kept for later get, stop, and delete.
    """

    def create(
        self,
        body: Mapping[str, object],
        idempotency_key: str,
    ) -> CreateResult:
        """Create once with this key; wait until the id is known."""
        try:
            from nebius.aio.service_error import RequestError
        except ImportError:
            raise TransportError(
                "The Nebius SDK is required to create an endpoint.",
                operation="create",
                recovery=recovery_for(TransportError.code, "endpoint"),
            ) from None
        try:
            operation = self._client().create(
                build_endpoint_request(body),
                metadata=[(IDEMPOTENCY_HEADER, idempotency_key)],
            ).wait()
        except RequestError as exc:
            return _from_request(exc, sent=False)
        except Exception as exc:
            return CreateResult(
                accepted=False,
                failure="transport",
                request_id=exposed_request_id(exc),
            )
        return _result_when_id_known(operation)

    def dry_run(self, body: Mapping[str, object]) -> CreateResult:
        """Send the create with ``dry_run``: Nebius validates it and
        creates nothing."""
        return _dry_run(
            self._client(), build_endpoint_request(body, dry_run=True)
        )

    def get(self, endpoint_id: str) -> dict[str, object]:
        """Fetch one endpoint and return its JSON, without the token."""
        payload = self._fetch(endpoint_id)
        spec = payload.get("spec")
        if isinstance(spec, dict):
            # Only reveal_token() hands the bearer token out.
            spec.pop("auth_token", None)
        return payload

    def reveal_token(self, endpoint_id: str) -> str | None:
        """Fetch once and return ``spec.auth_token``, or ``None``."""
        spec = self._fetch(endpoint_id, operation="reveal_token").get("spec")
        token = spec.get("auth_token") if isinstance(spec, dict) else None
        return token if isinstance(token, str) and token else None

    def _fetch(
        self,
        endpoint_id: str,
        operation: str = "get",
    ) -> dict[str, object]:
        try:
            from nebius.aio.service_error import RequestError
            from nebius.api.nebius.ai.v1 import GetEndpointRequest
        except ImportError:
            raise TransportError(
                "The Nebius SDK is required to get an endpoint.",
                operation=operation,
                resource_id=endpoint_id,
            ) from None
        try:
            endpoint = self._client().get(
                GetEndpointRequest(id=endpoint_id)
            ).wait()
        except RequestError as exc:
            _raise_call(
                exc,
                operation=operation,
                job_id=endpoint_id,
                noun="endpoint",
            )
        except Exception as exc:
            raise TransportError(
                f"The {operation} call failed for endpoint {endpoint_id}.",
                operation=operation,
                resource_id=endpoint_id,
                request_id=exposed_request_id(exc),
                recovery=recovery_for(TransportError.code, "endpoint"),
            ) from None
        payload = json.loads(
            endpoint.to_json(preserving_proto_field_name=True)
        )
        if not isinstance(payload, dict):
            raise TransportError(
                f"The {operation} call failed for endpoint {endpoint_id}.",
                operation=operation,
                resource_id=endpoint_id,
                recovery=recovery_for(TransportError.code, "endpoint"),
            )
        return payload

    def stop(self, endpoint_id: str) -> dict[str, object]:
        """Ask Nebius to stop. Return when the request is accepted.

        This does not wait for the state to reach ``STOPPED``. The
        endpoint stays until it is deleted.
        """
        return self._request_once(endpoint_id, "stop")

    def start(self, endpoint_id: str) -> dict[str, object]:
        """Ask Nebius to start a stopped endpoint; return on acceptance.

        This does not wait for the state to reach ``RUNNING``.
        """
        return self._request_once(endpoint_id, "start")

    def delete(self, endpoint_id: str) -> dict[str, object]:
        """Ask Nebius to delete. Return when the request is accepted.

        This does not wait for the machine to stop.
        """
        return self._request_once(endpoint_id, "delete")

    def _request_once(
        self,
        endpoint_id: str,
        verb: Literal["stop", "start", "delete"],
    ) -> dict[str, object]:
        try:
            from nebius.aio.service_error import RequestError
            from nebius.api.nebius.ai.v1 import (
                DeleteEndpointRequest,
                StartEndpointRequest,
                StopEndpointRequest,
            )
        except ImportError:
            raise TransportError(
                f"The Nebius SDK is required to {verb} an endpoint.",
                operation=verb,
                resource_id=endpoint_id,
            ) from None
        requests = {
            "stop": StopEndpointRequest,
            "start": StartEndpointRequest,
            "delete": DeleteEndpointRequest,
        }
        attempt = 0
        try:
            client = self._client()
            while True:
                attempt += 1
                try:
                    call = getattr(client, verb)
                    request = requests[verb](id=endpoint_id)
                    operation = call(request).wait()
                    break
                except RequestError as exc:
                    # Right after a stop settles, Nebius can still be
                    # finishing that operation and rejects the next one
                    # with ABORTED "operation conflict" (seen live on a
                    # start). The call was not applied; try again.
                    if (
                        _code_of(exc) != "ABORTED"
                        or attempt >= CONFLICT_ATTEMPTS
                    ):
                        raise
                    time.sleep(CONFLICT_PAUSE)
        except RequestError as exc:
            _raise_call(
                exc,
                operation=verb,
                job_id=endpoint_id,
                noun="endpoint",
            )
        except Exception as exc:
            raise TransportError(
                f"The {verb} call failed for endpoint {endpoint_id}.",
                operation=verb,
                resource_id=endpoint_id,
                request_id=exposed_request_id(exc),
                recovery=recovery_for(TransportError.code, "endpoint"),
            ) from None
        return {
            "id": endpoint_id,
            "resource_id": _resource_id(operation) or endpoint_id,
        }

    def _client(self) -> object:
        from nebius.api.nebius.ai.v1 import EndpointServiceClient

        return EndpointServiceClient(self._session())


def open_sdk(
    profile: str | None,
    *,
    credentials_file: object = None,
) -> object:
    """Open an SDK for this auth source. Callers share it via session.py.

    A credentials file wins. Then ``NEBIUS_IAM_TOKEN``: with no CLI config
    file it is the only credential, and with one, ``Config`` already puts
    it first. Then the CLI profile.

    A CLI profile that authenticates with federation and omits
    ``federation-id`` uses the cached CLI token. The API host is the
    profile endpoint without a port, so service routes stay on
    ``cpl.<service>.<endpoint>``.
    """
    try:
        from nebius.aio.cli_config import Config
        from nebius.aio.token.static import EnvBearer
        from nebius.sdk import SDK
    except ImportError:
        raise ValidationError(
            "The Nebius SDK is required to resolve a profile.",
            operation="create",
        ) from None
    from .session import TOKEN_ENV, has_cli_config

    kwargs: dict[str, object] = {"user_agent_prefix": _USER_AGENT}
    if credentials_file is not None:
        return SDK(credentials_file_name=str(credentials_file), **kwargs)
    env_token = bool(os.environ.get(TOKEN_ENV))
    if env_token and not has_cli_config():
        return SDK(credentials=EnvBearer(TOKEN_ENV), **kwargs)
    config = Config(profile=profile)
    profile_body = getattr(config, "_profile", {})
    # The cached-federation shortcut would shadow an env token.
    federation = (
        not env_token
        and isinstance(profile_body, dict)
        and profile_body.get("auth-type") == "federation"
        and "federation-id" not in profile_body
    )
    if federation:
        return SDK(
            credentials=_cached_federation(config),
            domain=_domain(config),
            **kwargs,
        )
    return SDK(config_reader=config, **kwargs)


def project_region(profile: str | None, project_id: str) -> str:
    """Return ``spec.region`` for an IAM v2 project, once per process."""
    from .session import project_region as cached

    return cached(project_id, lambda: _lookup_region(profile, project_id))


def _lookup_region(profile: str | None, project_id: str) -> str:
    try:
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.iam.v2 import (
            GetProjectRequest,
            ProjectServiceClient,
        )
    except ImportError:
        raise ValidationError(
            "The Nebius SDK is required to resolve a profile.",
            operation="create",
        ) from None
    from .session import get_sdk

    try:
        project = (
            ProjectServiceClient(get_sdk(profile))
            .get(GetProjectRequest(id=project_id))
            .wait()
        )
    except RequestError as exc:
        _raise_project(exc, project_id)
    region = getattr(getattr(project, "spec", None), "region", None)
    if not isinstance(region, str) or region.strip() == "":
        raise ValidationError(
            "The project has no region.",
            operation="create",
            project=project_id,
        )
    return region.strip()


def _domain(config: object) -> str:
    endpoint_of = getattr(config, "endpoint", None)
    endpoint = endpoint_of() if callable(endpoint_of) else ""
    host = (endpoint or "").split("://")[-1].split("/")[0]
    if host.endswith(":443"):
        host = host[: -len(":443")]
    return host or "api.nebius.cloud"


def _cached_federation(config: object) -> object:
    from nebius.aio.token.file_cache.file_bearer import (
        PureFileCacheBearer,
    )

    profile = getattr(config, "_profile", {})
    endpoint = None
    if isinstance(profile, dict):
        endpoint = profile.get("federation-endpoint")
    name_of = getattr(config, "profile_name", None)
    name = name_of() if callable(name_of) else None
    if (
        not isinstance(profile, dict)
        or profile.get("auth-type") != "federation"
        or not isinstance(endpoint, str)
        or endpoint.strip() == ""
        or not isinstance(name, str)
        or name.strip() == ""
    ):
        raise ValidationError(
            "Project or region configuration is missing or ambiguous.",
            operation="create",
        )
    federation_id = profile.get("federation-id") or ""
    if not isinstance(federation_id, str):
        raise ValidationError(
            "Project or region configuration is missing or ambiguous.",
            operation="create",
        )
    cache_name = f"federation/{endpoint.strip()}/{federation_id}/{name}"
    return PureFileCacheBearer(cache_name)


def _raise_project(exc: object, project_id: str) -> None:
    status = getattr(exc, "status", None)
    code = getattr(getattr(status, "code", None), "name", "")
    request_id = exposed_request_id(exc)
    if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        raise AccessError(
            "Authentication or permission failed.",
            operation="create",
            project=project_id,
            request_id=request_id,
        ) from None
    raise TransportError(
        "The project lookup failed.",
        operation="create",
        project=project_id,
        request_id=request_id,
    ) from None


def build_create_request(
    body: Mapping[str, object], *, dry_run: bool = False
) -> object:
    """Turn the mapped body into ``CreateJobRequest``.

    ``dry_run=True`` asks Nebius to validate it and create nothing.
    """
    from nebius.api.nebius.ai.v1 import CreateJobRequest, JobSpec
    from nebius.api.nebius.common.v1 import ResourceMetadata
    from nebius.api.nebius.compute.v1 import DiskSpec as ComputeDisk

    metadata = body["metadata"]
    spec = body["spec"]
    if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
        )
    disk = spec["disk"]
    if not isinstance(disk, str):
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
        )
    spec_kwargs: dict[str, object] = {
        "image": spec["image"],
        "container_command": spec["container_command"],
        "timeout": timedelta(seconds=float(spec["timeout_seconds"])),
        "disk": JobSpec.DiskSpec(
            type=ComputeDisk.DiskType.NETWORK_SSD,
            size_bytes=disk_bytes(disk),
        ),
    }
    if "args" in spec:
        spec_kwargs["args"] = spec["args"]
    if "subnet_id" in spec:
        spec_kwargs["subnet_id"] = spec["subnet_id"]
    if "restart_attempts" in spec:
        spec_kwargs["restart_attempts"] = int(spec["restart_attempts"])
    _shared_spec_kwargs(spec, spec_kwargs, JobSpec)
    for key in ("platform", "preset", "gpu", "cpu"):
        if key in spec:
            spec_kwargs[key] = spec[key]
    if "shm_size_bytes" in spec:
        spec_kwargs["shm_size_bytes"] = spec["shm_size_bytes"]
    mounts = spec.get("volumes")
    if isinstance(mounts, list) and mounts:
        spec_kwargs["volumes"] = [
            _mount(item) for item in mounts if isinstance(item, Mapping)
        ]
    injections = spec.get("injected_files")
    if isinstance(injections, list) and injections:
        spec_kwargs["injected_files"] = [
            JobSpec.FileInjection(
                container_path=str(item["container_path"]),
                content=item["content"],
            )
            for item in injections
            if isinstance(item, Mapping)
        ]
    return CreateJobRequest(
        metadata=ResourceMetadata(
            parent_id=str(metadata["parent_id"]),
            name=str(metadata["name"]),
        ),
        spec=JobSpec(**spec_kwargs),
        dry_run=dry_run,
    )


def build_endpoint_request(
    body: Mapping[str, object], *, dry_run: bool = False
) -> object:
    """Turn the mapped endpoint body into ``CreateEndpointRequest``.

    ``dry_run=True`` asks Nebius to validate it and create nothing.
    """
    from nebius.api.nebius.ai.v1 import CreateEndpointRequest, EndpointSpec
    from nebius.api.nebius.common.v1 import ResourceMetadata
    from nebius.api.nebius.compute.v1 import DiskSpec as ComputeDisk

    metadata = body["metadata"]
    spec = body["spec"]
    if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
        )
    disk = spec["disk"]
    if not isinstance(disk, str):
        raise TransportError(
            "The call failed before create was accepted.",
            operation="create",
        )
    spec_kwargs: dict[str, object] = {
        "image": spec["image"],
        "disk": EndpointSpec.DiskSpec(
            type=ComputeDisk.DiskType.NETWORK_SSD,
            size_bytes=disk_bytes(disk),
        ),
        "ports": [
            EndpointSpec.Port(
                container_port=int(item["container_port"]),
                host_port=int(item.get("host_port", 0)),
                protocol=getattr(
                    EndpointSpec.Port.Protocol,
                    str(item.get("protocol", "HTTP")),
                ),
            )
            for item in spec.get("ports", [])
            if isinstance(item, Mapping)
        ],
    }
    if "container_command" in spec:
        spec_kwargs["container_command"] = spec["container_command"]
    if "args" in spec:
        spec_kwargs["args"] = spec["args"]
    if "subnet_id" in spec:
        spec_kwargs["subnet_id"] = spec["subnet_id"]
    if "auth_token" in spec:
        spec_kwargs["auth_token"] = spec["auth_token"]
    token_ref = spec.get("auth_token_mysterybox_secret")
    if isinstance(token_ref, Mapping):
        spec_kwargs["auth_token_mysterybox_secret"] = (
            EndpointSpec.MysteryBoxSecretRef(**token_ref)
        )
    _shared_spec_kwargs(spec, spec_kwargs, EndpointSpec)
    for key in ("platform", "preset", "gpu", "cpu"):
        if key in spec:
            spec_kwargs[key] = spec[key]
    if "shm_size_bytes" in spec:
        spec_kwargs["shm_size_bytes"] = spec["shm_size_bytes"]
    mounts = spec.get("volumes")
    if isinstance(mounts, list) and mounts:
        spec_kwargs["volumes"] = [
            _endpoint_mount(item)
            for item in mounts
            if isinstance(item, Mapping)
        ]
    injections = spec.get("injected_files")
    if isinstance(injections, list) and injections:
        spec_kwargs["injected_files"] = [
            EndpointSpec.FileInjection(
                container_path=str(item["container_path"]),
                content=item["content"],
            )
            for item in injections
            if isinstance(item, Mapping)
        ]
    return CreateEndpointRequest(
        metadata=ResourceMetadata(
            parent_id=str(metadata["parent_id"]),
            name=str(metadata["name"]),
        ),
        spec=EndpointSpec(**spec_kwargs),
        dry_run=dry_run,
    )


def _shared_spec_kwargs(
    spec: Mapping[str, object],
    spec_kwargs: dict[str, object],
    spec_type: Any,
) -> None:
    """Map the fields ``JobSpec`` and ``EndpointSpec`` share by name."""
    from nebius.api.nebius.ai.v1 import (
        FollowsSpotPriceSpec,
        PricingModelSpec,
        SpotPricingPolicySpec,
    )

    if spec.get("preemptible") is True:
        spec_kwargs["preemptible"] = True
        policy = spec.get("spot_pricing_policy")
        if isinstance(policy, str):
            spec_kwargs["pricing_model"] = PricingModelSpec(
                spot_pricing_policy=SpotPricingPolicySpec(id=policy),
            )
        else:
            spec_kwargs["pricing_model"] = PricingModelSpec(
                follows_spot_price=FollowsSpotPriceSpec(),
            )
    if "working_dir" in spec:
        spec_kwargs["working_dir"] = spec["working_dir"]
    registry = spec.get("registry_credentials")
    if isinstance(registry, Mapping):
        spec_kwargs["registry_credentials"] = spec_type.RegistryCredentials(
            mysterybox_secret_version=str(
                registry["mysterybox_secret_version"]
            ),
        )
    variables = spec.get("environment_variables")
    if isinstance(variables, list) and variables:
        spec_kwargs["environment_variables"] = [
            _variable(spec_type, item)
            for item in variables
            if isinstance(item, Mapping)
        ]


def _variable(spec_type: Any, item: Mapping[str, object]) -> object:
    ref = item.get("mysterybox_secret")
    if isinstance(ref, Mapping):
        return spec_type.EnvironmentVariable(
            name=item["name"],
            mysterybox_secret=spec_type.MysteryBoxSecretRef(**ref),
        )
    return spec_type.EnvironmentVariable(
        name=item["name"],
        value=item["value"],
    )


def _endpoint_mount(item: Mapping[str, object]) -> object:
    from nebius.api.nebius.ai.v1 import EndpointSpec

    mode = item.get("mode", "rw")
    selected = (
        EndpointSpec.VolumeMount.Mode.READ_ONLY
        if mode == "ro"
        else EndpointSpec.VolumeMount.Mode.READ_WRITE
    )
    return EndpointSpec.VolumeMount(
        source=str(item["source"]),
        container_path=str(item["container_path"]),
        mode=selected,
    )


def _mount(item: Mapping[str, object]) -> object:
    from nebius.api.nebius.ai.v1 import JobSpec

    mode = item.get("mode", "rw")
    selected = (
        JobSpec.VolumeMount.Mode.READ_ONLY
        if mode == "ro"
        else JobSpec.VolumeMount.Mode.READ_WRITE
    )
    return JobSpec.VolumeMount(
        source=str(item["source"]),
        container_path=str(item["container_path"]),
        mode=selected,
    )


def classify_code(
    code: str,
    *,
    sent: bool,
    conflict_id: str | None,
    capacity: bool,
    access: bool,
) -> CreateResult:
    """Map one SDK status onto a create result. The text is dropped."""
    if access or code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        return CreateResult(accepted=False, failure="access")
    if capacity or code == "RESOURCE_EXHAUSTED":
        return CreateResult(accepted=False, failure="capacity")
    if code == "ALREADY_EXISTS":
        return CreateResult(
            accepted=True,
            failure="conflict",
            conflict_id=conflict_id,
        )
    if sent and code not in _DEFINITE:
        return CreateResult(accepted=True, failure="uncertain")
    if code in _REJECTED:
        return CreateResult(accepted=False, failure="rejected")
    return CreateResult(accepted=False, failure="transport")


def _code_of(exc: object) -> str:
    """The gRPC status code name of a ``RequestError``, or ``""``."""
    status = getattr(exc, "status", None)
    return str(getattr(getattr(status, "code", None), "name", "") or "")


def _from_request(exc: object, *, sent: bool) -> CreateResult:
    status = getattr(exc, "status", None)
    code = _code_of(exc)
    errors = getattr(status, "service_errors", ()) or ()
    result = classify_code(
        str(code),
        sent=sent,
        conflict_id=_conflict_id(errors),
        capacity=_has_detail(
            errors,
            ("not_enough_resources", "quota_failure"),
        ),
        access=_has_detail(errors, ("permission_denied",)),
    )
    message = getattr(status, "message", None)
    return CreateResult(
        resource_id=result.resource_id,
        accepted=result.accepted,
        state=result.state,
        failure=result.failure,
        conflict_id=result.conflict_id,
        request_id=exposed_request_id(exc),
        code=str(code) or None,
        message=message.strip() if isinstance(message, str) else None,
        trace_id=exposed_trace_id(exc),
        violations=_violations(errors),
    )


def _violations(errors: object) -> tuple[dict[str, object], ...]:
    """Quota violations from ``quota_failure`` service errors."""
    found: list[dict[str, object]] = []
    for error in errors:  # type: ignore[union-attr]
        failure = getattr(error, "quota_failure", None)
        for item in getattr(failure, "violations", None) or ():
            found.append(
                {
                    key: getattr(item, key, None)
                    for key in (
                        "quota",
                        "requested",
                        "limit",
                        "region",
                        "message",
                    )
                }
            )
    return tuple(found)


def _has_detail(errors: object, names: tuple[str, ...]) -> bool:
    for error in errors:  # type: ignore[union-attr]
        for name in names:
            if getattr(error, name, None) is not None:
                return True
    return False


def _conflict_id(errors: object) -> str | None:
    for error in errors:  # type: ignore[union-attr]
        for name in ("resource_already_exists", "resource_conflict"):
            detail = getattr(error, name, None)
            resource_id = getattr(detail, "resource_id", None)
            if isinstance(resource_id, str) and resource_id:
                return resource_id
    return None


def _raise_call(
    exc: object,
    *,
    operation: str,
    job_id: str,
    noun: str = "job",
) -> None:
    result = _from_request(exc, sent=True)
    request_id = result.request_id
    ids = {"trace_id": result.trace_id, "status_code": result.code}
    if result.failure == "access":
        raise AccessError(
            "Authentication or permission failed.",
            operation=operation,
            resource_id=job_id,
            request_id=request_id,
            **ids,
        ) from None
    if result.failure == "capacity":
        raise CapacityError(
            capacity_text(noun, result.violations),
            operation=operation,
            resource_id=job_id,
            request_id=request_id,
            violations=result.violations,
            **ids,
        ) from None
    code = _code_of(exc) or "unknown"
    if result.failure == "uncertain" and operation != "get":
        # Not a create: the id is known, but whether the request took
        # effect is not. Say so instead of the create-only message.
        raise TransportError(
            f"The {operation} call for {noun} {job_id} did not complete "
            f"({code}); it may or may not have taken effect.",
            operation=operation,
            resource_id=job_id,
            request_id=request_id,
            recovery=f"Check the state with get(), then retry {operation}.",
            **ids,
        ) from None
    message = f"The {operation} call failed for {noun} {job_id} ({code})."
    raise TransportError(
        message,
        operation=operation,
        resource_id=job_id,
        request_id=request_id,
        **ids,
        recovery=recovery_for(TransportError.code, noun),
    ) from None


def _dry_run(client: object, request: object) -> CreateResult:
    """One create call with ``dry_run`` set. Not retried."""
    try:
        from nebius.aio.service_error import RequestError
    except ImportError:
        raise TransportError(
            "The Nebius SDK is required for a server dry run.",
            operation="dry_run",
        ) from None
    try:
        operation = client.create(request).wait()  # type: ignore[attr-defined]
    except RequestError as exc:
        return _from_request(exc, sent=False)
    except Exception as exc:
        return CreateResult(
            accepted=False,
            failure="transport",
            request_id=exposed_request_id(exc),
        )
    return CreateResult(accepted=True, resource_id=_resource_id(operation))


def _result_when_id_known(operation: object) -> CreateResult:
    """Return once the job id is known.

    ``Operation.sync_wait`` polls until the operation is terminal. For a
    job, that is when the container finishes. This stops at the id.
    """
    found = _resource_id(operation)
    if found:
        return CreateResult(resource_id=found, accepted=True)
    while True:
        if _operation_done(operation):
            return _result_after_terminal(operation)
        try:
            _sync_update(operation)
        except Exception as exc:
            found = _resource_id(operation)
            request_id = exposed_request_id(exc)
            if found:
                return CreateResult(
                    resource_id=found,
                    accepted=True,
                    request_id=request_id,
                )
            return CreateResult(
                accepted=True,
                failure="uncertain",
                request_id=request_id,
            )
        found = _resource_id(operation)
        if found:
            return CreateResult(resource_id=found, accepted=True)
        if _operation_done(operation):
            return _result_after_terminal(operation)
        time.sleep(1)


def _sync_update(operation: object) -> None:
    update = getattr(operation, "sync_update", None)
    if not callable(update):
        raise TransportError(
            "The create call failed before the job id was known.",
            operation="create",
        )
    update()


def _operation_done(operation: object) -> bool:
    done = getattr(operation, "done", None)
    if not callable(done):
        return False
    return bool(done())


def _result_after_terminal(operation: object) -> CreateResult:
    found = _resource_id(operation)
    if found:
        return CreateResult(resource_id=found, accepted=True)
    return CreateResult(accepted=True, failure="uncertain")


def _resource_id(operation: object) -> str | None:
    resource_id = getattr(operation, "resource_id", None)
    if isinstance(resource_id, str) and resource_id:
        return resource_id
    return None
