"""Utility functions for the RPC library."""

import time
from collections.abc import AsyncGenerator
from random import randint
from typing import TYPE_CHECKING, Any, TypeVar, cast

from nats.micro.service import ServiceInfo

if TYPE_CHECKING:
    from .client import RPCClient

T = TypeVar("T")


def generate_id() -> str:
    """Generate a unique ID for requests."""
    return f"{int(time.time() * 1000)}-{randint(100000, 999999)}"


class RPCCallbacks:
    """Explicit marker wrapping a dict of callback functions.

    Pass an instance as the last argument to a pull-callback RPC method.
    The proxy detects the marker and routes the call through
    ``call_pull_iterator_with_callback``.

    Example::

        cbs = rpc_callbacks(
            on_item=lambda data: queue.append(data),
            on_end_of_batch=lambda: batch_end.set(),
            oneway=["on_item", "on_end_of_batch"],
        )

        async for _ in service.pull_batches(count, chunks, cbs):
            # batch boundary — apply backpressure here
            pass
    """

    def __init__(self, methods: dict[str, Any], oneway: list[str]) -> None:
        self.methods = methods
        self.oneway = oneway


def rpc_callbacks(**kwargs: Any) -> RPCCallbacks:
    """Build an RPCCallbacks bundle from keyword arguments.

    Special keyword ``oneway`` is a list of method names that should be
    dispatched as oneway (fire-and-forget) by the server. If omitted, all
    methods are treated as oneway.
    """
    oneway = kwargs.pop("oneway", None)
    methods: dict[str, Any] = {}
    for name, val in kwargs.items():
        if callable(val):
            methods[name] = val
    if oneway is None:
        oneway = list(methods.keys())
    return RPCCallbacks(methods, list(oneway))


def is_rpc_callbacks(v: Any) -> bool:
    """True if the value is an RPCCallbacks bundle (as produced by rpc_callbacks())."""
    return isinstance(v, RPCCallbacks)


def is_generator(func: Any) -> bool:
    """Check if a function returns a generator."""
    if not callable(func):
        return False

    # Check if it's a generator function
    return is_sync_generator(func) or is_async_generator(func)


def is_sync_generator(func: Any) -> bool:
    """Check if a function returns a synchronous generator."""
    if not callable(func):
        return False

    # Check if it's a generator function
    import inspect

    return inspect.isgeneratorfunction(func) or inspect.isgenerator(func)


def is_async_generator(func: Any) -> bool:
    """Check if a function returns an async generator."""
    if not callable(func):
        return False

    # Check if it's a coroutine function that might return an async generator
    import inspect

    if inspect.isasyncgenfunction(func):
        return True

    # Check return annotation
    if hasattr(func, "__annotations__"):
        return_annotation = func.__annotations__.get("return")
        if return_annotation:
            # Check if it's AsyncGenerator type
            origin = getattr(return_annotation, "__origin__", None)
            if origin is AsyncGenerator:
                return True

    return False


def is_async_function(func: Any) -> bool:
    """Check if a function is an async function."""
    if not callable(func):
        return False

    # Check if it's a coroutine function
    import inspect

    return inspect.iscoroutinefunction(func) or inspect.iscoroutine(func) or inspect.isawaitable(func)


def create_proxy(
    client: "RPCClient",
    namespace: str,
    path: list[str] | None = None,
    method_cache: set[str] | None = None,
) -> Any:
    """
    Create a proxy for RPC calls with support for nested objects.

    Args:
        client: RPC client with call and call_stream methods
        namespace: RPC namespace
        path: Current path in proxy hierarchy
        method_cache: Shared method cache for method discovery

    Returns:
        Proxy that intercepts attribute access and method calls
    """
    if path is None:
        path = []

    _path: list[str] = path.copy()
    _cache: list[set[str] | None] = [method_cache]  # Use list to allow mutation in nested scope

    def update_cache(methods: list[str]) -> None:
        if _cache[0] is None:
            _cache[0] = set(methods)

    def strip_methods(result: Any) -> Any:
        if result is not None and isinstance(result, dict) and "__methods" in result:
            r = cast(dict[str, Any], result)
            update_cache(r["__methods"])
            return {k: v for k, v in r.items() if k != "__methods"}
        return cast(Any, result)

    class RPCProxy:
        def __getattr__(self, name: str) -> Any:
            # If we have cached methods and this method doesn't exist, return None
            # This enables: result = await proxy.non_existent() if hasattr(proxy, 'non_existent') else None
            if _cache[0] is not None and len(_path) == 0 and name not in _cache[0]:
                return None

            # Build the full path
            full_path = _path + [name]

            # Return a callable proxy that can also be awaited directly
            class CallableProxy:
                def __call__(self, *args: Any, **kwargs: Any) -> Any:
                    method = ".".join(full_path)
                    subject = f"rpc.{namespace}.{method}"
                    is_pull_iterator = "pull" in name.lower()

                    # RPCCallbacks bundle: pull-callback mode. The bundle
                    # marker is unambiguous; no name heuristic needed.
                    cbs_idx = next(
                        (i for i, a in enumerate(args) if is_rpc_callbacks(a)),
                        -1,
                    )
                    if cbs_idx != -1:
                        cbs = cast("RPCCallbacks", args[cbs_idx])
                        other_args = [a for i, a in enumerate(args) if i != cbs_idx]
                        return client.call_pull_iterator_with_callback(
                            subject, cbs.methods, cbs.oneway, *other_args
                        )

                    # Detect plain callable argument: classic callback mode
                    callback_idx = next(
                        (
                            i
                            for i, a in enumerate(args)
                            if callable(a) and not isinstance(a, (str, bytes, type))
                        ),
                        -1,
                    )

                    if callback_idx != -1:
                        callback = args[callback_idx]
                        other_args = [a for i, a in enumerate(args) if i != callback_idx]
                        return client.call_with_callback(subject, other_args, callback)

                    # Check if this is a streaming method
                    is_generator_method = "generate" in name.lower()

                    if is_generator_method:
                        return client.call_stream(subject, *args)
                    elif is_pull_iterator:
                        return client.call_pull_iterator(subject, *args)
                    else:
                        # Wrap call to strip __methods from result
                        async def wrapped_call() -> Any:
                            result = await client.call(subject, *args)
                            return strip_methods(result)

                        return wrapped_call()

                def __getattr__(self, nested_name: str) -> Any:
                    # Handle nested property access (share cache)
                    return create_proxy(client, namespace, full_path, _cache[0]).__getattribute__(nested_name)

                def __await__(self) -> Any:
                    # Make this proxy awaitable - call with no arguments
                    return self().__await__()

            return CallableProxy()

        def __repr__(self) -> str:
            path_str = ".".join(_path) if _path else ""
            return f"<RPCProxy {namespace}{('.' + path_str) if path_str else ''}>"

    return RPCProxy()


def create_service_proxy(
    client: "RPCClient", service_info: ServiceInfo, timeout: int | None = None, path: list[str] | None = None
) -> Any:
    """
    Create a service proxy with proper streaming support.

    Args:
        client: RPC client instance
        service_info: Service information from discovery
        timeout: Optional timeout for requests
        path: Current path in proxy hierarchy

    Returns:
        Proxy that intercepts attribute access and method calls
    """
    if path is None:
        path = []

    _path: list[str] = path.copy()

    class ServiceProxy:
        def __getattr__(self, name: str) -> Any:
            full_path = _path + [name]
            full_path_str = ".".join(full_path)

            # Check if this is an endpoint
            endpoint = None
            for ep in service_info.endpoints:
                # Match exact path or last segment
                if ep.subject == full_path_str:
                    endpoint = ep
                    break
                parts = ep.subject.split(".")
                if parts[-1] == name and ".".join(parts[:-1]) == ".".join(_path):
                    endpoint = ep
                    break

            if endpoint:
                _endpoint = endpoint

                # Return a callable proxy similar to create_proxy
                class CallableProxy:
                    def __call__(self, *args: Any, **kwargs: Any) -> Any:
                        is_pull_iterator = "pull" in name.lower()

                        # RPCCallbacks bundle: pull-callback mode. The
                        # bundle marker is unambiguous; no name heuristic.
                        cbs_idx = next(
                            (i for i, a in enumerate(args) if is_rpc_callbacks(a)),
                            -1,
                        )
                        if cbs_idx != -1:
                            cbs = cast("RPCCallbacks", args[cbs_idx])
                            other_args = [a for i, a in enumerate(args) if i != cbs_idx]
                            return client.call_pull_iterator_with_callback(
                                _endpoint.subject, cbs.methods, cbs.oneway, *other_args
                            )

                        # Detect plain callable argument: callback mode
                        callback_idx = next(
                            (
                                i
                                for i, a in enumerate(args)
                                if callable(a) and not isinstance(a, (str, bytes, type))
                            ),
                            -1,
                        )

                        if callback_idx != -1:
                            callback = args[callback_idx]
                            other_args = [a for i, a in enumerate(args) if i != callback_idx]
                            return client.call_with_callback(_endpoint.subject, other_args, callback)

                        # Check if this is a streaming endpoint
                        is_generator = "generate" in name.lower()

                        if is_generator:
                            return client.call_stream(_endpoint.subject, *args)
                        elif is_pull_iterator:
                            return client.call_pull_iterator(_endpoint.subject, *args)
                        else:
                            return client.call(_endpoint.subject, *args)

                return CallableProxy()

            # Check if this is a nested namespace
            prefix = full_path_str + "."
            has_nested = any(ep.subject.startswith(prefix) for ep in service_info.endpoints)

            if has_nested:
                return create_service_proxy(client, service_info, timeout, full_path)

            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        def __repr__(self) -> str:
            path_str = ".".join(_path) if _path else ""
            return f"<ServiceProxy {service_info.name}{('.' + path_str) if path_str else ''}>"

    return ServiceProxy()
