from concurrent.futures import ThreadPoolExecutor

_executor: ThreadPoolExecutor | None = None


def get_executor() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(thread_name_prefix="rpc_worker")
    return _executor


def shutdown_executor() -> None:
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None
