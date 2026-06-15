"""This module provides a single, shared ThreadPoolExecutor for the entire application."""

from concurrent.futures import ThreadPoolExecutor

_executor: ThreadPoolExecutor | None = None


def get_executor() -> ThreadPoolExecutor:
    """
    Returns the global singleton instance of the ThreadPoolExecutor.
    The pool is created on the first call.
    """
    global _executor
    if _executor is None:
        # Create the executor with Python's default worker count,
        # which is a sensible balance for I/O-bound tasks.
        # Default is min(32, os.cpu_count() + 4).
        _executor = ThreadPoolExecutor(thread_name_prefix="rpc_worker")
    return _executor


def shutdown_executor() -> None:
    """
    Shuts down the global executor. Should be called during application cleanup.
    """
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None
