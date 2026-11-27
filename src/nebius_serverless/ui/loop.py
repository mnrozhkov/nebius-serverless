"""Synchronous calls inside a running event loop run on a worker thread.

Jupyter runs every cell inside a running event loop, and the Nebius SDK
refuses synchronous calls there. A synchronous call made from such a
thread runs its work on a worker thread and blocks until it ends, as
``time.sleep`` would. The ``_async`` twins keep the loop responsive.
"""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from typing import TypeVar

T = TypeVar("T")

# How often the waiting thread wakes up, so Ctrl-C is handled promptly
# even when the signal reaches another thread.
WAKE_SECONDS = 0.2


def loop_running() -> bool:
    """Whether this thread is running an event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def off_loop(work: Callable[[], T]) -> T:
    """Call ``work`` here, or on a worker thread when this thread runs an
    event loop. Blocks until it returns, and returns or raises the same.

    An interrupt (Ctrl-C) in the waiting thread propagates at once; the
    worker is not waited for. A caller that shares a progress handle
    with ``work`` closes it, which stops the worker's polling.
    """
    if not loop_running():
        return work()
    pool = ThreadPoolExecutor(1, thread_name_prefix="ns-sync")
    try:
        future = pool.submit(work)
        while True:
            try:
                return future.result(timeout=WAKE_SECONDS)
            except FutureTimeout:
                continue
    finally:
        # Ctrl-C must not block on a worker that is still polling.
        pool.shutdown(wait=False)
