"""The task bucket: found or created with the SDK, and read and written.

From this machine the bucket is read and written over S3 with your
Object Storage credentials. Inside a job it is mounted, and read and
written as files.
"""

import configparser
import hashlib
import hmac
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import AccessError, TransportError
from .task_runner import MOUNT_ENV

BUCKET_PREFIX = "generated-tasks-"


BUCKET_LABELS = {"managed-by": "nebius-serverless"}


_S3_TIMEOUT = 60.0

_lock = threading.Lock()
_buckets: dict[tuple[int, str, str], "Bucket"] = {}


@dataclass(frozen=True)
class Bucket:
    """The project's task bucket: what jobs mount and the client reads."""

    id: str
    name: str
    endpoint: str
    region: str


class SdkTaskBackend:
    """The task bucket: found or created with the SDK, read with S3."""

    def __init__(self, profile: str | None) -> None:
        self._profile = profile

    def ensure_bucket(self, project: str) -> tuple[Bucket, bool]:
        """Find the task bucket or create it. ``True`` when created."""
        cache_key = (os.getpid(), self._profile or "", project)
        with _lock:
            found = _buckets.get(cache_key)
        if found is not None:
            return found, False
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.storage.v1 import (
            BucketServiceClient,
            BucketSpec,
            CreateBucketRequest,
            GetBucketByNameRequest,
        )

        from .session import get_sdk

        name = bucket_name(project)
        sdk: Any = get_sdk(self._profile)
        client = BucketServiceClient(sdk)
        lookup = GetBucketByNameRequest(parent_id=project, name=name)
        created = False
        try:
            try:
                record = client.get_by_name(lookup).wait()
            except RequestError as exc:
                if code_of(exc) != "NOT_FOUND":
                    raise
                request = CreateBucketRequest(
                    metadata=ResourceMetadata(
                        parent_id=project, name=name, labels=BUCKET_LABELS
                    ),
                    spec=BucketSpec(),
                )
                try:
                    client.create(request).wait().sync_wait()
                    created = True
                except RequestError as again:
                    # Another run created it first: use that one.
                    if code_of(again) != "ALREADY_EXISTS":
                        raise
                record = client.get_by_name(lookup).wait()
        except RequestError as exc:
            _raise_bucket(exc, name, project)
        bucket = Bucket(
            id=str(record.metadata.id),
            name=name,
            endpoint=f"https://{record.status.domain_name}",
            region=str(record.status.region),
        )
        with _lock:
            _buckets[cache_key] = bucket
        return bucket, created

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        """Write one object."""
        _s3(bucket, "PUT", key, data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        return _s3(bucket, "GET", key)

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return _s3(bucket, "HEAD", key) is not None


class MountedTaskBackend(SdkTaskBackend):
    """The task bucket where it is already mounted, read as files.

    Used inside a task's job, when that task starts other tasks. The
    job needs no S3 credentials. The bucket is still found with the
    SDK, because a new job mounts it by id.
    """

    def __init__(self, profile: str | None, root: Path) -> None:
        super().__init__(profile)
        self._root = root

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        """Write one object as a file."""
        path = self._root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        try:
            return (self._root / key).read_bytes()
        except (FileNotFoundError, NotADirectoryError):
            return None

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return (self._root / key).is_file()


def bucket_name(project: str) -> str:
    """The task bucket's name: a fixed prefix plus the project id."""
    return BUCKET_PREFIX + project.removeprefix("project-")


def forget_buckets() -> None:
    """Drop cached bucket lookups (tests, or after a manual delete)."""
    with _lock:
        _buckets.clear()


def backend(profile: str | None) -> SdkTaskBackend:
    """S3 from outside a job; the mounted folder from inside one."""
    root = os.environ.get(MOUNT_ENV)
    if root and Path(root).is_dir():
        return MountedTaskBackend(profile, Path(root))
    return SdkTaskBackend(profile)


def code_of(exc: object) -> str:
    status = getattr(exc, "status", None)
    return str(getattr(getattr(status, "code", None), "name", "") or "")


def _raise_bucket(exc: object, name: str, project: str) -> None:
    code = code_of(exc) or None
    if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        raise AccessError(
            f"Not allowed to find or create the bucket {name} in "
            f"project {project}.",
            operation="task",
            project=project,
            recovery=(
                "A task needs rights to manage Object Storage buckets "
                "in this project. Run ns.whoami() to see the account "
                "in use."
            ),
            status_code=code,
        ) from None
    raise TransportError(
        f"Could not find or create the bucket {name} ({code or 'unknown'}).",
        operation="task",
        project=project,
        recovery="Run the task again.",
        status_code=code,
    ) from None


def _s3_credentials() -> tuple[str, str, str | None]:
    """Access key, secret and optional session token. Never logged."""
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access and secret:
        return access, secret, os.environ.get("AWS_SESSION_TOKEN") or None
    path = Path(
        os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")
    ).expanduser()
    profile = os.environ.get("AWS_PROFILE", "default")
    parser = configparser.RawConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        pass
    if parser.has_section(profile):
        access = parser.get(profile, "aws_access_key_id", fallback=None)
        secret = parser.get(profile, "aws_secret_access_key", fallback=None)
        token = parser.get(profile, "aws_session_token", fallback=None)
        if access and secret:
            return access, secret, token or None
    raise AccessError(
        "No S3 credentials were found, so the task bucket cannot be "
        "read or written from this machine.",
        operation="task",
        recovery=(
            "Create an Object Storage access key and put it in "
            f"{path} under [{profile}], or set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY."
        ),
    )


def _s3(
    bucket: Bucket, method: str, key: str, body: bytes = b""
) -> bytes | None:
    """One signed S3 request. ``None`` when the object does not exist."""
    access, secret, token = _s3_credentials()
    host = bucket.endpoint.removeprefix("https://")
    path = "/" + urllib.parse.quote(f"{bucket.name}/{key}", safe="/-_.~")
    now = _utcnow()
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    digest = hashlib.sha256(body).hexdigest()
    headers = {
        "host": host,
        "x-amz-content-sha256": digest,
        "x-amz-date": stamp,
    }
    if token:
        headers["x-amz-security-token"] = token
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [
            method,
            path,
            "",
            *(f"{name}:{headers[name]}" for name in sorted(headers)),
            "",
            signed,
            digest,
        ]
    )
    scope = f"{day}/{bucket.region}/s3/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            stamp,
            scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ]
    )
    signing = f"AWS4{secret}".encode()
    for part in (day, bucket.region, "s3", "aws4_request"):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256)
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature.hexdigest()}"
    )
    del headers["host"]
    request = urllib.request.Request(
        bucket.endpoint + path,
        data=body if method == "PUT" else None,
        method=method,
        headers=headers,
    )
    what = f"{method} s3://{bucket.name}/{key}"
    try:
        with urllib.request.urlopen(request, timeout=_S3_TIMEOUT) as reply:
            return bytes(reply.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        if exc.code in (401, 403):
            raise AccessError(
                f"The S3 credentials on this machine were refused "
                f"({what}, HTTP {exc.code}).",
                operation="task",
                recovery=(
                    "Use an Object Storage access key that can read and "
                    f"write the bucket {bucket.name}."
                ),
            ) from None
        raise TransportError(
            f"The task bucket request failed ({what}, HTTP {exc.code}).",
            operation="task",
            recovery="Run the task again.",
        ) from None
    except (urllib.error.URLError, OSError) as exc:
        raise TransportError(
            f"The task bucket could not be reached ({what}): {exc}.",
            operation="task",
            recovery="Check the network and run the task again.",
        ) from None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
