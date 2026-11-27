"""``TaskRun``: one run of a task, and the result it returns."""

import json
import os
import threading
import time
from typing import TYPE_CHECKING, Any

from ..core.poll import Timeout
from ..errors import Error, TaskFailed, WaitTimeout
from ..jobs.job import Job
from ..ui.output import Progress, call_progress
from . import runtime
from .storage import Bucket, SdkTaskBackend

if TYPE_CHECKING:
    from .attempts import Attempts

_LOG = "nebius_serverless.task"


# The console address of a job, with {project} and {job} in it. When
# set, ``console_url`` links to a run's job.
CONSOLE_ENV = "NS_CONSOLE_JOB_URL"


# A result can appear in the bucket a moment after the job ends.
RESULT_TRIES = 5


RESULT_PAUSE = 2.0


class TaskRun:
    """One run of a task: its job, its inputs, and where its files are.

    ``result()`` waits for the job and returns the task's result.
    ``id`` is the run id: the run's folder in the bucket, such as
    ``tasks/train/20261127-152011``. Store it to reconnect later with
    ``ns.tasks.get(id)``. ``name`` is the UTC time the task was called,
    such as ``20261124-153922``. ``output_path`` is the run's file
    folder as jobs see it, and ``output_uri`` is the same folder as an
    ``s3://`` address.

    A run whose task was given other runs as arguments has no job until
    those runs have finished. ``job``, ``name``, ``inputs``,
    ``output_path`` and ``output_uri`` then wait for the job to exist,
    and ``started`` tells whether it does.
    """

    _FIELDS = (
        "_name",
        "_job",
        "_inputs",
        "_bucket",
        "_key",
        "_scripted",
        "_backend",
        "_project",
        "_output_path",
        "_output_uri",
        "_attempts",
    )
    _name: str
    _job: Job
    _inputs: dict[str, Any]
    _bucket: Bucket
    _key: str
    _scripted: bool
    _backend: SdkTaskBackend
    _project: str
    _output_path: str
    _output_uri: str
    _attempts: "Attempts | None"

    def __init__(
        self,
        *,
        task: str,
        name: str,
        job: Job,
        inputs: dict[str, Any],
        bucket: Bucket,
        key: str,
        scripted: bool,
        backend: SdkTaskBackend,
        project: str,
        bucket_choice: str | None = None,
        attempts: "Attempts | None" = None,
    ) -> None:
        self._begin(task, bucket_choice)
        self._name = name
        self._job = job
        self._inputs = inputs
        folder = bucket.key(key)
        self._output_path = f"{runtime.MOUNT}/{folder}/files"
        self._output_uri = f"s3://{bucket.name}/{folder}/files/"
        self._bucket = bucket
        self._key = key
        self._scripted = scripted
        self._backend = backend
        self._project = project
        self._attempts = attempts
        self._ready.set()

    def _begin(self, task: str, bucket_choice: str | None) -> None:
        self.task = task
        # The bucket the user chose for the task, or None for the default.
        self._bucket_choice = bucket_choice
        # Set once the job exists, or once it is known that it never will.
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._lock = threading.Lock()
        self._outcome: tuple[bool, Any] | None = None

    @classmethod
    def _waiting(
        cls, task: str, bucket_choice: str | None = None
    ) -> "TaskRun":
        """A run that has no job yet, because its inputs are not done."""
        run = cls.__new__(cls)
        run._begin(task, bucket_choice)
        return run

    def _started(self, real: "TaskRun") -> None:
        """Become the run of the job that was started for it."""
        if self._ready.is_set():
            return
        for field in self._FIELDS:
            setattr(self, field, getattr(real, field))
        self._ready.set()

    def _not_started(self, error: BaseException) -> None:
        """Record why no job was started."""
        if self._ready.is_set():
            return
        self._error = error
        self._ready.set()

    def _settled(self) -> "TaskRun":
        """Wait until the job exists. Raise when it was never started."""
        self._ready.wait()
        error = self._error
        if isinstance(error, TaskFailed):
            reason = str(error).splitlines()[0]
            raise TaskFailed(
                f"Task {self.task} was not started, because one of its "
                f"inputs failed: {reason}",
                operation="task",
                state=error.state,
            ) from error
        if error is not None:
            raise error
        return self

    def cancel(self) -> None:
        """Request cancellation of the run's job. Never raises.

        A run still waiting for its inputs is dropped and never starts.
        """
        if not self._ready.is_set():
            self._not_started(
                TaskFailed(
                    f"Task {self.task} was dropped before it started.",
                    operation="task",
                )
            )
            return
        if self._error is not None:
            return
        try:
            self._job.cancel(quiet=True)
        except Error:
            pass

    @property
    def id(self) -> str:
        """The run id: the run's folder in the bucket, relative to the
        library's root. The same across preemptions. Pass it to
        ``ns.tasks.get`` to reconnect."""
        return self._settled()._key

    @property
    def uri(self) -> str:
        """The run's folder as an ``s3://`` address, for other tools."""
        settled = self._settled()
        folder = settled._bucket.key(settled._key)
        return f"s3://{settled._bucket.name}/{folder}/"

    @property
    def project(self) -> str:
        """The project the run's job is in."""
        return self._settled()._project

    @property
    def started(self) -> bool:
        """Whether the run has a job."""
        return self._ready.is_set() and self._error is None

    @property
    def name(self) -> str:
        """The UTC time the job was started, such as ``20261124-153922``."""
        return self._settled()._name

    @property
    def job(self) -> Job:
        """The run's job. Waits for it when the run has not started."""
        return self._settled()._job

    @property
    def inputs(self) -> dict[str, Any]:
        """The inputs by name, with other runs replaced by their results."""
        return self._settled()._inputs

    @inputs.setter
    def inputs(self, value: dict[str, Any]) -> None:
        self._inputs = value

    @property
    def output_path(self) -> str:
        """The run's file folder as jobs see it."""
        return self._settled()._output_path

    @property
    def output_uri(self) -> str:
        """The run's file folder as an ``s3://`` address."""
        return self._settled()._output_uri

    def __repr__(self) -> str:
        if not self._ready.is_set():
            return f"TaskRun({self.task!r}, waiting for its inputs)"
        if self._error is not None:
            return f"TaskRun({self.task!r}, not started)"
        return f"TaskRun({self.task!r}, {self._name!r}, job={self._job.id!r})"

    def result(self, timeout: Timeout = None) -> Any:
        """Wait for the job, then return the task's result.

        Returns the function's return value, or the script's ``outputs``
        as a dict of strings. Raises ``TaskFailed`` when the function
        raised, the script exited with an error, the job ended without
        a result, or the run was not started because one of its inputs
        failed. Later calls return or raise the same outcome without
        waiting again.

        ``timeout`` is a local deadline, such as ``"30m"`` or seconds.
        When it passes, ``WaitTimeout`` is raised with the latest job id
        and the job keeps running; call ``result()`` again later.
        """
        self._settled()
        with self._lock:
            if self._outcome is None:

                def work(progress: Progress) -> tuple[bool, Any]:
                    return self._collect(progress, timeout)

                self._outcome = call_progress(
                    quiet=False, logger_name=_LOG, work=work
                )
            ok, value = self._outcome
        if ok:
            return value
        raise value

    def _collect(
        self, progress: Progress, timeout: Timeout = None
    ) -> tuple[bool, Any]:
        try:
            record = self.job._wait(timeout, progress)
        except WaitTimeout as exc:
            raise WaitTimeout(
                f"Stopped waiting for task {self.task} run {self._key} "
                f"(job {self.job.id}). The job may still be running.",
                operation="result",
                resource_id=self.job.id,
                recovery=(
                    "Call result() again to keep waiting, or later "
                    f"reconnect with ns.tasks.get({self._key!r})."
                ),
            ) from exc
        state = _state(record)
        item = "outputs.bin" if self._scripted else "result.json"
        data = self._read(item, tries=RESULT_TRIES if _done(state) else 1)
        if data is None:
            return False, self._failed(
                f"ended {state} without a result", state
            )
        if self._scripted:
            return self._script_outcome(data, state)
        return self._function_outcome(data, state)

    def _read(self, item: str, *, tries: int) -> bytes | None:
        for attempt in range(tries):
            data = self._backend.get(self._bucket, f"{self._key}/{item}")
            if data is not None:
                return data
            if attempt + 1 < tries:
                time.sleep(RESULT_PAUSE)
        return None

    def _function_outcome(self, data: bytes, state: str) -> tuple[bool, Any]:
        try:
            body = json.loads(data)
            ok = body["ok"]
        except (ValueError, KeyError, TypeError):
            return False, self._failed("wrote an unreadable result", state)
        if ok and _done(state):
            return True, body.get("value")
        if ok:
            # A result counts only from a completed job: one that ended
            # otherwise may have been cut short after writing it.
            return False, self._failed(
                f"returned a value, but its job ended {state}", state
            )
        error = body.get("error") or {}
        remote = str(error.get("traceback") or "")
        message = (
            f"raised {error.get('type')}: {error.get('message')}"
            f"\n\nTraceback from the job:\n{remote}"
        )
        return False, self._failed(message, state, remote_traceback=remote)

    def _script_outcome(self, data: bytes, state: str) -> tuple[bool, Any]:
        parts = data.split(b"\0")
        try:
            code = int(parts[0])
        except ValueError:
            return False, self._failed("wrote unreadable outputs", state)
        values = [part.decode(errors="replace") for part in parts[1:-1]]
        outputs = dict(zip(values[0::2], values[1::2]))
        if code == 0 and _done(state):
            return True, outputs
        return False, self._failed(
            f"exited with code {code}",
            state,
            exit_code=code,
            outputs=outputs,
        )

    def _failed(self, what: str, state: str, **extra: Any) -> TaskFailed:
        return TaskFailed(
            f"Task {self.task} run {self.name} (job {self.job.id}) {what}.",
            operation="task",
            project=self._project,
            resource_id=self.job.id,
            state=state,
            recovery=f"See the log: nebius ai job logs {self.job.id}",
            **extra,
        )


def _state(record: dict[str, object]) -> str:
    status = record.get("status")
    state = status.get("state") if isinstance(status, dict) else None
    return state if isinstance(state, str) else "UNKNOWN"


def _done(state: str) -> bool:
    return state == "COMPLETED"


def console_url(project: str, job_id: str | None) -> str | None:
    """The web console address of a job, when a template is set.

    The template is read from ``NS_CONSOLE_JOB_URL``. It may contain
    ``{project}`` and ``{job}``. ``None`` without a template or a job.
    """
    template = os.environ.get(CONSOLE_ENV)
    if not template or not job_id:
        return None
    return template.replace("{project}", project).replace("{job}", job_id)
