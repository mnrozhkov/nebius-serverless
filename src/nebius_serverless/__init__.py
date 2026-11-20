"""Unofficial client for Nebius Serverless AI Jobs and Endpoints.

Not an official Nebius product, and Nebius does not support it. The
official Python SDK is https://github.com/nebius/pysdk.

``JobSpec`` and ``EndpointSpec`` are local recipes. ``create()`` on one
returns a ``Job`` or an ``Endpoint``: the cloud resource with that id.
Constructing a recipe does not import the Nebius SDK. The SDK is
imported on the call that resolves a profile or contacts Nebius.
"""

from ._version import __version__ as __version__
from .account import whoami, whoami_async
from .endpoint import Endpoint, endpoints
from .endpoint_spec import EndpointSpec
from .errors import (
    AccessError,
    CapacityError,
    Error,
    KeyConflict,
    SubmissionUncertain,
    TransportError,
    UnsupportedFeature,
    ValidationError,
    WaitTimeout,
)
from .job import Job, jobs
from .job_spec import JobSpec
from .log import configure_logging, diagnostics
from .session import close, configure

__all__ = [
    "AccessError",
    "CapacityError",
    "Endpoint",
    "EndpointSpec",
    "Error",
    "Job",
    "JobSpec",
    "KeyConflict",
    "SubmissionUncertain",
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
