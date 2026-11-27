"""Create a run's jobs: the first attempt, then one per preemption.

Every attempt is built from the run's record (``record.py``): the task's
definition, the run's folder, and its ``payload.json``. So a process that
reattached to a run with ``ns.tasks.get`` creates the next attempt the
same way as the process that started it, without the caller's code.
"""

import tempfile
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core.profile import resolve_location
from ..core.transport import Transport
from ..jobs.job import Job, resubmit_budget
from ..jobs.spec import JobSpec
from ..ui.output import Progress
from . import record, runtime, submit
from .storage import Bucket, SdkTaskBackend, bucket_env

if TYPE_CHECKING:
    from .task import Task

# Set in each task job to its run id, so the run can be found from the
# job id.
RUN_ENV = "NS_TASK_RUN"


class Attempts:
    """The jobs of one run, and its record in the bucket."""

    def __init__(
        self,
        *,
        task: "Task",
        run: dict[str, Any],
        backend: SdkTaskBackend,
        bucket: Bucket,
        payload: bytes | None,
        region: str | None = None,
    ) -> None:
        self.task = task
        self.run = run
        self.backend = backend
        self.bucket = bucket
        self.payload = payload
        self._region = region
        self._lock = threading.Lock()
        options = run["definition"]["options"]
        self.budget = resubmit_budget(
            str(options.get("pricing", "on-demand")),
            int(options.get("restart_attempts", 0)),
            options.get("preemption_retries"),
        )

    @property
    def run_id(self) -> str:
        return str(self.run["id"])

    def first(self, name: str, progress: Progress) -> Job:
        """Create the first attempt. ``wait()`` on it resubmits."""
        return self._create(name, progress, reason="first", again=True)

    def resubmit(
        self,
        name: str,
        _key: str,
        progress: Progress,
        ended: dict[str, object] | None = None,
    ) -> tuple[str, str | None]:
        """Create the next attempt after a preemption. ``Job.wait`` calls
        it, up to ``budget`` times, with the record the preempted job
        ended with."""
        if ended is not None:
            self.ended(ended, write=False)
        job = self._create(name, progress, reason="preempted", again=False)
        assert job.id is not None
        return job.id, job.status

    @property
    def latest(self) -> int:
        """The latest attempt's number, from 1."""
        return len(self.run["attempts"])

    def ended(self, job: dict[str, object], *, write: bool = True) -> None:
        """Keep the state and times of the attempt whose job this is."""
        metadata = job.get("metadata")
        job_id = metadata.get("id") if isinstance(metadata, dict) else None
        status = job.get("status")
        status = status if isinstance(status, dict) else {}
        for item in self.run["attempts"]:
            if job_id is not None and item["job_id"] != job_id:
                continue
            item["state"] = status.get("state")
            details = status.get("state_details")
            if isinstance(details, dict) and details.get("code"):
                item["code"] = details["code"]
            for field in ("started_at", "finished_at"):
                if status.get(field):
                    item[field] = status[field]
            break
        if write:
            record.write(self.backend, self.bucket, self.run)

    def attach(self, transport: Transport) -> Job:
        """A handle to the latest attempt's job, which can resubmit."""
        attempts = self.run["attempts"]
        latest = attempts[-1]
        can_resubmit = self.payload is not None
        return Job._attach(
            str(latest["job_id"]),
            name=str(latest["name"]),
            transport=transport,
            resubmit=self.resubmit if can_resubmit else None,
            budget=self.budget,
            attempts=[str(item["job_id"]) for item in attempts],
            base_name=str(attempts[0]["name"]),
        )

    def _create(
        self, name: str, progress: Progress, *, reason: str, again: bool
    ) -> Job:
        assert self.payload is not None
        definition = self.run["definition"]
        options = dict(definition["options"])
        volumes = [
            f"{self.bucket.id}:{runtime.MOUNT}:rw",
            *(options.pop("volumes", None) or ()),
        ]
        attempt = self.latest + 1
        # The job can find its run and attempt, and tasks it starts use
        # this bucket.
        env = {
            **bucket_env(self.bucket),
            RUN_ENV: self.run_id,
            runtime.ATTEMPT_ENV: str(attempt),
            **dict(options.pop("env", None) or {}),
        }
        libs = definition.get("libs")
        command, files = self.task._attempt_files(
            self.payload,
            self.bucket.key(self.run_id),
            libs and self.bucket.key(libs),
            attempt,
        )
        with self._lock, staged(files) as folder:
            spec = JobSpec(
                command=command,
                image=str(definition["image"]),
                name=name,
                volumes=volumes,
                env=env,
                inject_files=[
                    f"{folder / item}:{runtime.JOB_DIR}/{item}"
                    for item in files
                ],
                **options,
            )
            job = Job(spec)._create(
                region=self._region_id(),
                project=str(self.run["project"]),
                transport=submit.job_transport(),
                progress=progress,
                idempotency_key=None,
                resubmit=self.resubmit if again else None,
            )
            assert job.id is not None
            record.add_attempt(
                self.run, job_id=job.id, name=name, reason=reason
            )
            record.write(self.backend, self.bucket, self.run)
        return job

    def _region_id(self) -> str:
        if self._region is None:
            options = self.run["definition"]["options"]
            _project, self._region = resolve_location(
                profile=options.get("profile"),
                project=str(self.run["project"]),
                region=None,
            )
        return self._region


@contextmanager
def staged(files: Mapping[str, bytes]) -> Iterator[Path]:
    """The generated files on disk until the create has read them."""
    with tempfile.TemporaryDirectory(prefix="ns-task-") as folder:
        root = Path(folder)
        for name, content in files.items():
            (root / name).write_bytes(content)
        yield root
