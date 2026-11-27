"""The task bucket: found or created with the SDK, and read and written.

The bucket is the project's ``ns-workspace-<project id>`` by default,
created on first use. A bucket the user chooses, with ``Task(bucket=)``,
``ns.configure(bucket=)`` or ``NS_BUCKET``, must exist and is never
created. In a chosen bucket the library keeps its folders under one
prefix, ``ns/`` by default, so the user's own files stay apart.

Callers pass keys relative to the library's root in the bucket, such
as ``tasks/train/20261127-152011/payload.json``. The backends add the
root. ``Bucket.key`` gives the full key, for paths that jobs see.

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

from ..errors import AccessError, Error, TransportError
from .runtime import MOUNT_ENV

# The default bucket's name: this, then the project id without
# "project-". "ns" matches the package alias, the /ns mount and the NS_*
# variables; "workspace" says it is a working area a tool manages.
BUCKET_PREFIX = "ns-workspace-"

# The library's folder in a bucket the user chose.
CHOSEN_ROOT = "ns"

# Set in task jobs to the chosen bucket and its root, so tasks started
# inside a job use the same bucket.
BUCKET_ENV = "NS_BUCKET"
ROOT_ENV = "NS_BUCKET_PREFIX"

# Written once per process to check that a chosen bucket can be written
# and read, under the library's own folder.
ACCESS_CHECK = "tasks/.access-check"


BUCKET_LABELS = {"managed-by": "nebius-serverless"}


_S3_TIMEOUT = 60.0

_lock = threading.Lock()
_buckets: dict[tuple[int, str, str, str, str], "Bucket"] = {}


@dataclass(frozen=True)
class Bucket:
    """The task bucket: what jobs mount and the client reads.

    ``root`` is the library's folder in it: ``""`` in the default
    bucket, ``"ns"`` in a bucket the user chose.
    """

    id: str
    name: str
    endpoint: str
    region: str
    root: str = ""
    chosen: bool = False

    def key(self, path: str) -> str:
        """The full key of ``path``, a key relative to the root."""
        path = path.lstrip("/")
        return f"{self.root}/{path}" if self.root else path


def chosen_bucket(task_bucket: str | None = None) -> str | None:
    """The bucket the user chose, or ``None`` for the project default.

    The task's ``bucket=`` wins, then ``ns.configure(bucket=)``, then
    ``NS_BUCKET``.
    """
    from ..core.session import settings

    for value in (task_bucket, settings().bucket, os.environ.get(BUCKET_ENV)):
        if value:
            return value
    return None


def chosen_root() -> str:
    """The library's folder in a chosen bucket: ``ns`` unless set.

    ``ns.configure(prefix=)`` sets it, or ``NS_BUCKET_PREFIX`` in a
    job. An empty value puts the folders at the bucket's root.
    """
    from ..core.session import settings

    configured = settings().prefix
    if configured is None:
        configured = os.environ.get(ROOT_ENV)
    if configured is None:
        return CHOSEN_ROOT
    return configured.strip("/")


class SdkTaskBackend:
    """The task bucket: found or created with the SDK, read with S3."""

    def __init__(self, profile: str | None) -> None:
        self._profile = profile

    def ensure_bucket(
        self,
        project: str,
        chosen: str | None = None,
        *,
        root: str | None = None,
        find_only: bool = False,
    ) -> tuple[Bucket, bool]:
        """The task bucket. ``True`` when it was created now.

        Without ``chosen``, the project's default bucket, created when
        missing. A ``chosen`` bucket must exist: it is never created,
        and on first use it is checked for reading and writing.
        ``root`` overrides the library's folder in a chosen bucket.
        ``find_only=True`` only finds the bucket: nothing is created or
        written, and a missing bucket raises ``Error``.
        """
        if chosen is None:
            root = ""
        elif root is None:
            root = chosen_root()
        cache_key = (
            os.getpid(),
            self._profile or "",
            project,
            chosen or "",
            root,
        )
        with _lock:
            found = _buckets.get(cache_key)
        if found is not None:
            return found, False
        if chosen is not None:
            bucket = self._existing(project, chosen, root)
            if find_only:
                return bucket, False
            self._check_access(bucket)
            with _lock:
                _buckets[cache_key] = bucket
            return bucket, False
        if find_only:
            name = bucket_name(project)
            return self._existing(project, name, "", chosen=False), False
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.common.v1 import ResourceMetadata
        from nebius.api.nebius.storage.v1 import (
            BucketServiceClient,
            BucketSpec,
            CreateBucketRequest,
            GetBucketByNameRequest,
        )

        from ..core.session import get_sdk

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

    def _existing(
        self, project: str, name: str, root: str, *, chosen: bool = True
    ) -> Bucket:
        """Look up a bucket. Never creates one."""
        from nebius.aio.service_error import RequestError
        from nebius.api.nebius.storage.v1 import (
            BucketServiceClient,
            GetBucketByNameRequest,
        )

        from ..core.session import get_sdk

        sdk: Any = get_sdk(self._profile)
        client = BucketServiceClient(sdk)
        lookup = GetBucketByNameRequest(parent_id=project, name=name)
        try:
            record = client.get_by_name(lookup).wait()
        except RequestError as exc:
            if code_of(exc) == "NOT_FOUND" and not chosen:
                raise Error(
                    f"The project's task bucket {name} does not exist in "
                    f"project {project}, so it holds no task runs.",
                    operation="task",
                    project=project,
                    recovery=(
                        "Pass bucket= if the run is in a bucket you chose, "
                        "or project= if it is in another project."
                    ),
                    status_code="NOT_FOUND",
                ) from None
            if code_of(exc) == "NOT_FOUND":
                raise AccessError(
                    f"The bucket {name} was not found in project "
                    f"{project}. A bucket you choose must exist; this "
                    "package never creates it.",
                    operation="task",
                    project=project,
                    recovery=(
                        "Create the bucket, or check its name in "
                        "Task(bucket=), ns.configure(bucket=) or NS_BUCKET. "
                        "Leave all three unset to use the project's "
                        "default bucket."
                    ),
                    status_code="NOT_FOUND",
                ) from None
            _raise_bucket(exc, name, project, chosen=True)
        return Bucket(
            id=str(record.metadata.id),
            name=name,
            endpoint=f"https://{record.status.domain_name}",
            region=str(record.status.region),
            root=root,
            chosen=chosen,
        )

    def _check_access(self, bucket: Bucket) -> None:
        """Write and read one small object, or raise ``AccessError``."""
        what = f"s3://{bucket.name}/{bucket.key(ACCESS_CHECK)}"
        try:
            self.put(bucket, ACCESS_CHECK, b"ok\n")
        except AccessError:
            raise AccessError(
                f"Cannot write to the bucket {bucket.name} ({what}).",
                operation="task",
                recovery=(
                    "Tasks need to write the bucket. Use an Object Storage "
                    f"access key with write access to {bucket.name}, or "
                    "choose another bucket."
                ),
            ) from None
        try:
            data = self.get(bucket, ACCESS_CHECK)
        except AccessError:
            data = None
        if data is None:
            raise AccessError(
                f"Cannot read from the bucket {bucket.name} ({what}).",
                operation="task",
                recovery=(
                    "Tasks need to read the bucket. Use an Object Storage "
                    f"access key with read access to {bucket.name}, or "
                    "choose another bucket."
                ),
            )

    def put(self, bucket: Bucket, key: str, data: bytes) -> None:
        """Write one object. ``key`` is relative to the bucket's root."""
        _s3(bucket, "PUT", bucket.key(key), data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        return _s3(bucket, "GET", bucket.key(key))

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return _s3(bucket, "HEAD", bucket.key(key)) is not None


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
        path = self._root / bucket.key(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, bucket: Bucket, key: str) -> bytes | None:
        """Read one object, or ``None`` when it does not exist."""
        try:
            return (self._root / bucket.key(key)).read_bytes()
        except (FileNotFoundError, NotADirectoryError):
            return None

    def exists(self, bucket: Bucket, key: str) -> bool:
        """Whether the object exists."""
        return (self._root / bucket.key(key)).is_file()


def bucket_name(project: str) -> str:
    """The default bucket's name: ``ns-workspace-`` plus the project id
    without ``project-``."""
    return BUCKET_PREFIX + project.removeprefix("project-")


def bucket_env(bucket: Bucket) -> dict[str, str]:
    """What a task job needs to use the same bucket: nothing for the
    default bucket, the name and root for a chosen one."""
    if not bucket.chosen:
        return {}
    return {BUCKET_ENV: bucket.name, ROOT_ENV: bucket.root}


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


def _raise_bucket(
    exc: object, name: str, project: str, *, chosen: bool = False
) -> None:
    code = code_of(exc) or None
    if code in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
        if chosen:
            raise AccessError(
                f"Not allowed to read the bucket {name} in project "
                f"{project}.",
                operation="task",
                project=project,
                recovery=(
                    "Ask for read access to the bucket in this project. "
                    "Run ns.whoami() to see the account in use."
                ),
                status_code=code,
            ) from None
        raise AccessError(
            f"Not allowed to find or create the bucket {name} in "
            f"project {project}.",
            operation="task",
            project=project,
            recovery=(
                "Creating the project's default bucket needs rights to "
                "manage Object Storage buckets. Or choose a bucket that "
                "exists: Task(bucket=...) or ns.configure(bucket=...). "
                "Run ns.whoami() to see the account in use."
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
