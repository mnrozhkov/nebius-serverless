"""Public exports for the cloud Jobs client."""

import nebius_serverless as ns


def test_version_and_section_10_errors() -> None:
    assert ns.__version__ == "0.1.0"
    from nebius_serverless.sdk_transport import _USER_AGENT

    assert _USER_AGENT == f"nebius-serverless/{ns.__version__}"
    assert ns.diagnostics()["version"] == ns.__version__
    assert issubclass(ns.UnsupportedFeature, ns.ValidationError)
    assert issubclass(ns.ValidationError, ns.Error)
    for name in (
        "AccessError",
        "CapacityError",
        "KeyConflict",
        "SubmissionUncertain",
        "TransportError",
        "WaitTimeout",
    ):
        assert issubclass(getattr(ns, name), ns.Error)


def test_public_surface() -> None:
    assert ns.__all__ == [
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
    for gone in ("Run", "Service"):
        assert not hasattr(ns, gone)
    for name in ("submit", "plan", "result", "refresh"):
        assert name not in ns.__all__
        assert not hasattr(ns, name)
    assert "output" not in ns.__all__
    assert callable(ns.configure_logging)
    assert callable(ns.diagnostics)
    assert not hasattr(ns, "quiet")
