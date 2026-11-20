"""The package version, read once from the installed distribution."""

from importlib.metadata import PackageNotFoundError, version

DISTRIBUTION = "nebius-serverless"

try:
    __version__ = version(DISTRIBUTION)
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0+unknown"

USER_AGENT = f"{DISTRIBUTION}/{__version__}"
