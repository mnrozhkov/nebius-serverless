"""Unofficial client for Nebius Serverless AI Jobs and Endpoints.

Not an official Nebius product, and Nebius does not support it. The
official Python SDK is https://github.com/nebius/pysdk.

``JobSpec`` and ``EndpointSpec`` are local recipes. ``create()`` on one
returns a ``Job`` or an ``Endpoint``: the cloud resource with that id.
Constructing a recipe does not import the Nebius SDK. The SDK is
imported on the call that resolves a profile or contacts Nebius.

Supported imports are the names in ``__all__``, plus
``nebius_serverless.errors``, ``nebius_serverless.tasks.driver`` for
integrations, and ``nebius_serverless.integrations.<name>``. The other
modules are implementation: importable, but not a compatibility
promise.
"""

from . import endpoints, jobs
from ._version import __version__ as __version__
from .core.account import whoami, whoami_async
from .core.session import close, configure
from .endpoints.endpoint import Endpoint
from .endpoints.spec import EndpointSpec
from .errors import (
    AccessError,
    BuildFailed,
    CapacityError,
    Error,
    KeyConflict,
    SubmissionUncertain,
    TaskFailed,
    TransportError,
    UnsupportedFeature,
    ValidationError,
    WaitTimeout,
)
from .images.image import Image
from .jobs.job import Job
from .jobs.spec import JobSpec
from .tasks.task import Task
from .ui.log import configure_logging, diagnostics

__all__ = [
    "AccessError",
    "BuildFailed",
    "CapacityError",
    "Endpoint",
    "EndpointSpec",
    "Error",
    "Image",
    "Job",
    "JobSpec",
    "KeyConflict",
    "SubmissionUncertain",
    "Task",
    "TaskFailed",
    "TransportError",
    "UnsupportedFeature",
    "ValidationError",
    "WaitTimeout",
    "close",
    "configure",
    "configure_logging",
    "diagnostics",
    "endpoints",
    "jobs",
    "whoami",
    "whoami_async",
]
