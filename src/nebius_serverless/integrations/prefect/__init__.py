"""Run a pipeline of ``ns.Task`` objects as a Prefect flow.

``PrefectFlow`` runs the flow on a Nebius job of its own, or in this
process with ``remote_job=False``. Running it here needs the extra::

    pip install "nebius-serverless[prefect]"
"""

from .flow import FlowRun, PrefectFlow

__all__ = ["FlowRun", "PrefectFlow"]
