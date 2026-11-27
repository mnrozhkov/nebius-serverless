"""Container Registry and MysteryBox calls for ``Image.build``.

Imported on the call that needs it. ``Image()`` does not import this
module. Tokens pass through here and are never logged or put in errors.
"""

import os
import threading
from dataclasses import dataclass
from typing import Any

from ..errors import (
    AccessError,
    TransportError,
    ValidationError,
    exposed_request_id,
    exposed_trace_id,
)

REGISTRY_NAME = "generated-images"
PUSH_SECRET_NAME = "generated-images-push"
PUSH_KEY = "REGISTRY_PASSWORD"
_TOKEN_TIMEOUT = 30.0
_OPERATION = "build"

_lock = threading.Lock()
_registries: dict[tuple[int, str, str], "Registry"] = {}


@dataclass(frozen=True)
class Registry:
    """A Container Registry: its id and the host images are pushed to."""

    id: str
    host: str

    @property
    def path(self) -> str:
        """``<host>/<id without registry->``, the prefix of every image."""
        return f"{self.host}/{self.id.removeprefix('registry-')}"


class SdkImageBackend:
    """Registry lookup and create, and the push credential, via the SDK."""

    def __init__(self, profile: str | None) -> None:
        self._profile = profile

    def find_registry(self, project: str) -> Registry | None:
        """The project's ``generated-images`` registry, or ``None``."""
        key = (os.getpid(), self._profile or "", project)
        with _lock:
            found = _registries.get(key)
        if found is not None:
            return found
        found = self._get_by_name(project)
        if found is not None:
            with _lock:
                _registries[key] = found
        return found

    def ensure_registry(self, project: str) -> tuple[Registry, bool]:
        """Find the registry or create it. ``True`` when it was created."""
        found = self.find_registry(project)
        if found is not None:
            return found, False
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.registry.v1 import (
            CreateRegistryRequest,
            RegistrySpec,
        )

        request = CreateRegistryRequest(
            metadata=ResourceMetadata(parent_id=project, name=REGISTRY_NAME),
            spec=RegistrySpec(
                description="Images built by nebius_serverless Image.build"
            ),
        )
        try:
            operation = self._registries().create(request).wait()
            _finish(operation, f"create registry {REGISTRY_NAME}", project)
        except RequestError as exc:
            # Another build created it first: use that one.
            if _code_of(exc) != "ALREADY_EXISTS":
                _raise(exc, f"create registry {REGISTRY_NAME}", project)
        found = self.find_registry(project)
        if found is None:
            raise TransportError(
                f"Registry {REGISTRY_NAME} was created but cannot be read.",
                operation=_OPERATION,
                project=project,
            )
        return found, True

    def has_image(self, registry: Registry, name: str, tag: str) -> bool:
        """Whether ``name:tag`` is an active image in ``registry``."""
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.registry.v1 import (
            ArtifactServiceClient,
            ListArtifactsRequest,
        )

        repository = f"{registry.id.removeprefix('registry-')}/{name}"
        client = ArtifactServiceClient(self._sdk())
        page = ""
        while True:
            try:
                response = client.list(
                    ListArtifactsRequest(
                        parent_id=registry.id, page_size=100, page_token=page
                    )
                ).wait()
            except RequestError as exc:
                _raise(exc, f"list images in {REGISTRY_NAME}", registry.id)
            for item in response.items:
                if (
                    item.name == repository
                    and tag in item.tags
                    and item.status.name == "ACTIVE"
                ):
                    return True
            page = str(response.next_page_token or "")
            if not page:
                return False

    def push_secret_version(self, project: str) -> str:
        """Store a fresh access token; return its ``mbsecver-`` id.

        The token is the caller's own, from the credentials this package
        uses for every call. It goes into the ``generated-images-push``
        secret as a new primary version, so concurrent builds each keep
        the version they were given.
        """
        token = self._token()
        secret_id = self._secret_id(project)
        if secret_id is None:
            return self._create_secret(project, token)
        return self._add_version(project, secret_id, token)

    def delete_secret_version(self, version_id: str) -> None:
        """Delete one version. Best effort: the token expires anyway."""
        try:
            from nebius.api.nebius.mysterybox.v1 import (
                DeleteSecretVersionRequest,
                SecretVersionServiceClient,
            )

            client = SecretVersionServiceClient(self._sdk())
            client.delete(DeleteSecretVersionRequest(id=version_id)).wait()
        except Exception:  # cleanup must not fail a finished build
            pass

    def _get_by_name(self, project: str) -> Registry | None:
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import GetByNameRequest

        try:
            registry = (
                self._registries()
                .get_by_name(
                    GetByNameRequest(parent_id=project, name=REGISTRY_NAME)
                )
                .wait()
            )
        except RequestError as exc:
            if _code_of(exc) == "NOT_FOUND":
                return None
            _raise(exc, f"look up registry {REGISTRY_NAME}", project)
        registry_id = str(registry.metadata.id)
        host = str(getattr(registry.status, "registry_fqdn", "") or "")
        if not registry_id or not host:
            raise TransportError(
                f"Registry {REGISTRY_NAME} has no id or host yet.",
                operation=_OPERATION,
                project=project,
                recovery="Retry in a minute.",
            )
        return Registry(id=registry_id, host=host)

    def _token(self) -> str:
        sdk = self._sdk()
        try:
            token = sdk.get_token_sync(  # type: ignore[attr-defined]
                timeout=_TOKEN_TIMEOUT
            )
            value = token.token
        except Exception:
            raise AccessError(
                "Could not get an access token to push the image.",
                operation=_OPERATION,
            ) from None
        if not isinstance(value, str) or value == "":
            raise AccessError(
                "The credentials in use give no access token, so the "
                "build cannot push.",
                operation=_OPERATION,
            )
        return value

    def _secret_id(self, project: str) -> str | None:
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.mysterybox.v1 import GetSecretByNameRequest

        try:
            secret = (
                self._secrets()
                .get_by_name(
                    GetSecretByNameRequest(
                        parent_id=project, name=PUSH_SECRET_NAME
                    )
                )
                .wait()
            )
        except RequestError as exc:
            if _code_of(exc) == "NOT_FOUND":
                return None
            _raise(exc, f"look up secret {PUSH_SECRET_NAME}", project)
        return str(secret.metadata.id)

    def _create_secret(self, project: str, token: str) -> str:
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.mysterybox.v1 import (
            CreateSecretRequest,
            GetSecretRequest,
            SecretSpec,
            SecretVersionSpec,
        )

        what = f"create secret {PUSH_SECRET_NAME}"
        request = CreateSecretRequest(
            metadata=ResourceMetadata(
                parent_id=project, name=PUSH_SECRET_NAME
            ),
            spec=SecretSpec(
                description=(
                    "Short-lived access token for Image.build pushes to "
                    f"{REGISTRY_NAME}. Rewritten on every build."
                ),
                secret_version=SecretVersionSpec(payload=[_payload(token)]),
            ),
        )
        try:
            operation = self._secrets().create(request).wait()
            _finish(operation, what, project)
            secret_id = str(getattr(operation, "resource_id", "") or "")
            secret = self._secrets().get(GetSecretRequest(id=secret_id)).wait()
        except RequestError as exc:
            if _code_of(exc) == "ALREADY_EXISTS":
                # Another build created it first: add a version instead.
                found = self._secret_id(project)
                if found is not None:
                    return self._add_version(project, found, token)
            _raise(exc, what, project)
        version_id = str(secret.spec.primary_version_id or "")
        if not version_id:
            raise TransportError(
                f"Secret {PUSH_SECRET_NAME} has no version.",
                operation=_OPERATION,
                project=project,
            )
        return version_id

    def _add_version(self, project: str, secret_id: str, token: str) -> str:
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.mysterybox.v1 import (
            CreateSecretVersionRequest,
            SecretVersionServiceClient,
            SecretVersionSpec,
        )

        what = f"write secret {PUSH_SECRET_NAME}"
        request = CreateSecretVersionRequest(
            metadata=ResourceMetadata(parent_id=secret_id),
            spec=SecretVersionSpec(
                payload=[_payload(token)], set_primary=True
            ),
        )
        try:
            client = SecretVersionServiceClient(self._sdk())
            operation = client.create(request).wait()
            _finish(operation, what, project)
        except RequestError as exc:
            _raise(exc, what, project)
        version_id = str(getattr(operation, "resource_id", "") or "")
        if not version_id:
            raise TransportError(
                f"The new version of {PUSH_SECRET_NAME} has no id.",
                operation=_OPERATION,
                project=project,
            )
        return version_id

    def _sdk(self) -> Any:
        from ..core.session import get_sdk

        return get_sdk(self._profile)

    def _registries(self) -> Any:
        from nebius.api.nebius.registry.v1 import RegistryServiceClient

        return RegistryServiceClient(self._sdk())

    def _secrets(self) -> Any:
        from nebius.api.nebius.mysterybox.v1 import SecretServiceClient

        return SecretServiceClient(self._sdk())


def open_backend(profile: str | None) -> SdkImageBackend:
    """The SDK backend, or a clear error when the SDK is missing."""
    try:
        import nebius.api.nebius.registry.v1  # noqa: F401
    except ImportError:
        raise ValidationError(
            "The Nebius SDK is required to build an image.",
            operation=_OPERATION,
        ) from None
    return SdkImageBackend(profile)


def forget_registries() -> None:
    """Drop cached registry lookups (tests, or after a manual delete)."""
    with _lock:
        _registries.clear()


def _payload(token: str) -> Any:
    from nebius.api.nebius.mysterybox.v1 import Payload

    return Payload(key=PUSH_KEY, string_value=token)


def _finish(operation: object, what: str, project: str) -> None:
    """Wait for a create operation; raise when it did not succeed."""
    operation.sync_wait()  # type: ignore[attr-defined]
    successful = getattr(operation, "successful", None)
    if callable(successful) and not successful():
        raise TransportError(
            f"Could not {what}.",
            operation=_OPERATION,
            project=project,
        )


def _code_of(exc: object) -> str:
    status = getattr(exc, "status", None)
    return str(getattr(getattr(status, "code", None), "name", "") or "")


def _raise(exc: object, what: str, project: str) -> None:
    code = _code_of(exc) or None
    request_id = exposed_request_id(exc)
    trace_id = exposed_trace_id(exc)
    if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        raise AccessError(
            f"Not allowed to {what} in project {project}.",
            operation=_OPERATION,
            project=project,
            recovery=(
                "Image.build needs rights to manage Container Registry and "
                "MysteryBox in this project. Run ns.whoami() to see the "
                "account in use."
            ),
            request_id=request_id,
            trace_id=trace_id,
            status_code=code,
        ) from None
    raise TransportError(
        f"Could not {what} ({code or 'unknown'}).",
        operation=_OPERATION,
        project=project,
        recovery="Retry the build.",
        request_id=request_id,
        trace_id=trace_id,
        status_code=code,
    ) from None
