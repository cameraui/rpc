"""Common handler utilities for RPC client and service."""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, cast

from .errors import RPCException, create_error
from .types import (
    CallbackInvocation,
    CallbackMessage,
    ErrorCode,
    PullIteratorRequest,
    PullIteratorResponse,
    RPCClient,
    RPCError,
    StreamMessage,
)
from .utils import is_async_function, is_async_generator, is_sync_generator


async def handle_stream_request(
    handler: Callable[..., Any],
    args: list[Any],
    stream_subject: str,
    request_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
) -> None:
    """Handle streaming request - common logic for client and service."""
    generator: AsyncGenerator[Any, None]

    # Get generator
    if is_async_generator(handler):
        # Handler is an async generator function
        generator = handler(*args)
    elif is_sync_generator(handler):
        # Handler is a sync generator function - convert to async
        sync_gen = handler(*args)

        async def async_wrapper() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper()
    elif is_async_function(handler):
        # Handler is async function that might return a generator
        result = await handler(*args)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):
            # Sync generator returned from async function
            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for stream")
    else:
        # Run sync handler in thread pool
        loop = asyncio.get_event_loop()
        func = partial(handler, *args)
        result = await loop.run_in_executor(io_pool, func)

        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):
            # Sync generator returned from sync function
            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for stream")

    # Verify we have an async iterator
    if not hasattr(generator, "__aiter__"):
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    # Listen for cancellation
    cancelled = False

    async def cancel_handler(_: Any) -> None:
        nonlocal cancelled
        cancelled = True

    cancel_unsub = await client.subscribe(f"{stream_subject}.cancel", cancel_handler)

    # Give client time to set up subscription
    await asyncio.sleep(0)

    # Stream values
    try:
        async for value in generator:
            if cancelled or not client.is_connected:
                break

            stream_msg: StreamMessage = {
                "id": request_id,
                "type": "data",
                "data": value,
            }
            await client.publish(stream_subject, stream_msg)

        if not cancelled and client.is_connected:
            end_msg: StreamMessage = {"id": request_id, "type": "end"}
            await client.publish(stream_subject, end_msg)

    except Exception as e:
        if not cancelled and client.is_connected:
            try:
                error_msg: StreamMessage = {
                    "id": request_id,
                    "type": "error",
                    "error": e.to_dict()
                    if isinstance(e, RPCException)
                    else {
                        "code": ErrorCode.STREAM_ERROR.value,
                        "message": str(e),
                    },
                }
                await client.publish(stream_subject, error_msg)
            except Exception:
                pass  # Ignore publish errors during disconnect
    finally:
        await cancel_unsub()
        if hasattr(generator, "aclose"):
            # Ensure generator is closed
            with contextlib.suppress(Exception):
                await generator.aclose()


async def handle_normal_rpc(
    handler: Callable[..., Any],
    params: Any,
    io_pool: ThreadPoolExecutor,
) -> Any:
    """Handle normal RPC call - common logic for client and service."""
    # Ensure params is a list
    if not isinstance(params, list):
        params = [params] if params is not None else []

    # Call the handler
    if is_async_function(handler):
        return await handler(*params)
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *params)
        return await loop.run_in_executor(io_pool, func)


def format_error_dict(e: Exception) -> RPCError:
    """Format exception as error dictionary."""
    if isinstance(e, RPCException):
        return e.to_dict()
    else:
        return {
            "code": ErrorCode.INTERNAL_ERROR.value,
            "message": str(e),
        }


async def handle_pull_iterator_request(
    handler: Callable[..., Any],
    args: list[Any],
    iterator_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Handle pull-based iterator request."""
    generator: AsyncGenerator[Any, None]

    # Get generator
    if is_async_generator(handler):
        # Handler is an async generator function
        generator = handler(*args)
    elif is_sync_generator(handler):
        # Handler is a sync generator function - convert to async
        sync_gen = handler(*args)

        async def async_wrapper() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper()
    elif is_async_function(handler):
        # Handler is async function that might return a generator
        result = await handler(*args)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):
            # Sync generator returned from async function
            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull iterator")
    else:
        # Run sync handler in thread pool
        loop = asyncio.get_event_loop()
        func = partial(handler, *args)
        result = await loop.run_in_executor(io_pool, func)

        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):
            # Sync generator returned from sync function
            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull iterator")

    # Verify we have an async iterator
    if not hasattr(generator, "__aiter__"):
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    # Set up request/response subjects
    request_subject = f"_rpc.iterator.{iterator_id}.request"
    response_subject = f"_rpc.iterator.{iterator_id}.response"

    # Track if iterator is active
    active = True
    unsub_func = None

    # Subscribe to iterator requests
    async def handle_pull_request(msg: PullIteratorRequest) -> None:
        nonlocal active

        if not active:
            return

        try:
            if msg.get("type") == "cancel":
                active = False
                # Close the generator explicitly
                if hasattr(generator, "aclose"):
                    await generator.aclose()
                response: PullIteratorResponse = {
                    "id": iterator_id,
                    "type": "done",
                }
                await client.publish(response_subject, response)
            elif msg.get("type") == "next":
                try:
                    value = await generator.__anext__()
                    next_response: PullIteratorResponse = {
                        "id": iterator_id,
                        "type": "value",
                        "value": value,
                    }
                    await client.publish(response_subject, next_response)
                except StopAsyncIteration:
                    active = False
                    done_response: PullIteratorResponse = {
                        "id": iterator_id,
                        "type": "done",
                    }
                    await client.publish(response_subject, done_response)

        except Exception as e:
            active = False
            error_response: PullIteratorResponse = {
                "id": iterator_id,
                "type": "error",
                "error": format_error_dict(e),
            }
            await client.publish(response_subject, error_response)

    unsub_func = await client.subscribe(request_subject, handle_pull_request)

    # Return cleanup function
    async def cleanup() -> None:
        nonlocal active
        active = False
        if unsub_func is not None:
            await unsub_func()
        # Ensure generator is closed
        if hasattr(generator, "aclose"):
            with contextlib.suppress(Exception):
                await generator.aclose()

    return cleanup


class CallbackInvoker:
    """Passed to a pull-callback handler as its last argument.

    Invoke() fires a oneway callback to the client. Only methods registered
    as oneway by the client are dispatched; unknown methods are silently
    dropped per protocol spec (at-most-once semantics).

    Safe for concurrent use. Becomes inert after cancellation — handlers
    can poll `active` to exit long-running loops early.
    """

    def __init__(self, subject: str, client: "RPCClient", oneway: set[str]) -> None:
        self._subject = subject
        self._client = client
        self._oneway = oneway
        self.active = True

    async def invoke(self, method: str, *args: Any) -> None:
        """Fire a oneway callback. No-op if inactive, disconnected, or the
        method is not registered as oneway by the client.
        """
        if not self.active:
            return
        if method not in self._oneway:
            return
        if not self._client.is_connected:
            return
        msg: CallbackInvocation = {"method": method, "args": list(args)}
        await self._client.publish(self._subject, msg)


async def handle_pull_callback_request(
    handler: Callable[..., Any],
    args: list[Any],
    iterator_id: str,
    callback_subject: str,
    oneway_methods: list[str],
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Handle a pull-iterator-with-callbacks request.

    Builds a CallbackInvoker whose invoke() publishes oneway to the
    callback subject, calls the handler with (*args, invoker), then drives
    the returned async generator from iterator next/cancel requests.

    The iterator response carries no value — it is purely a batch-boundary
    signal. See packages/rpc/PULL_CALLBACK_PROTOCOL.md.
    """
    invoker = CallbackInvoker(callback_subject, client, set(oneway_methods))

    # Invoke handler with invoker appended as last positional argument.
    generator: AsyncGenerator[Any, None]

    if is_async_generator(handler):
        generator = handler(*args, invoker)
    elif is_sync_generator(handler):
        sync_gen = handler(*args, invoker)

        async def async_wrapper_sync() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper_sync()
    elif is_async_function(handler):
        result = await handler(*args, invoker)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper_iter() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper_iter()
        else:
            invoker.active = False
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull-callback iterator")
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args, invoker)
        result = await loop.run_in_executor(io_pool, func)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper_sync_fn() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper_sync_fn()
        else:
            invoker.active = False
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull-callback iterator")

    if not hasattr(generator, "__aiter__"):
        invoker.active = False
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    request_subject = f"_rpc.iterator.{iterator_id}.request"
    response_subject = f"_rpc.iterator.{iterator_id}.response"

    active = True
    unsub_func: Callable[[], Coroutine[Any, Any, None]] | None = None

    async def handle_pull_request(msg: PullIteratorRequest) -> None:
        nonlocal active

        if not active:
            return

        try:
            if msg.get("type") == "cancel":
                active = False
                invoker.active = False
                if hasattr(generator, "aclose"):
                    await generator.aclose()
                response: PullIteratorResponse = {
                    "id": iterator_id,
                    "type": "done",
                }
                await client.publish(response_subject, response)
            elif msg.get("type") == "next":
                try:
                    # Drive the generator to its next yield. The yielded
                    # value is ignored — only batch-boundary matters.
                    await generator.__anext__()
                    next_response: PullIteratorResponse = {
                        "id": iterator_id,
                        "type": "value",
                    }
                    await client.publish(response_subject, next_response)
                except StopAsyncIteration:
                    active = False
                    invoker.active = False
                    done_response: PullIteratorResponse = {
                        "id": iterator_id,
                        "type": "done",
                    }
                    await client.publish(response_subject, done_response)

        except Exception as e:
            active = False
            invoker.active = False
            error_response: PullIteratorResponse = {
                "id": iterator_id,
                "type": "error",
                "error": format_error_dict(e),
            }
            await client.publish(response_subject, error_response)

    unsub_func = await client.subscribe(request_subject, handle_pull_request)

    async def cleanup() -> None:
        nonlocal active
        active = False
        invoker.active = False
        if unsub_func is not None:
            await unsub_func()
        if hasattr(generator, "aclose"):
            with contextlib.suppress(Exception):
                await generator.aclose()

    return cleanup


async def handle_callback_request(
    handler: Callable[..., Any],
    args: list[Any],
    callback_subject: str,
    request_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Handle callback subscription request."""

    # Create wrapper callback that publishes to callback_subject
    async def wrapper_callback(value: Any) -> None:
        if not client.is_connected:
            return
        msg: CallbackMessage = {
            "id": request_id,
            "type": "data",
            "data": value,
        }
        await client.publish(callback_subject, msg)

    # For sync handlers, create a sync wrapper
    def sync_wrapper_callback(value: Any) -> None:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(wrapper_callback(value))
            else:
                loop.run_until_complete(wrapper_callback(value))
        except RuntimeError:
            pass

    # Call handler with args + wrapper callback
    handler_cleanup = None
    if is_async_function(handler):
        handler_cleanup = await handler(*args, wrapper_callback)
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args, sync_wrapper_callback)
        handler_cleanup = await loop.run_in_executor(io_pool, func)

    # Helper to invoke cleanup (sync or async)
    async def invoke_cleanup() -> None:
        if handler_cleanup and callable(handler_cleanup):
            if is_async_function(handler_cleanup):
                await cast(Callable[[], Coroutine[Any, Any, None]], handler_cleanup)()
            else:
                handler_cleanup()

    # Subscribe to cancel subject
    async def on_cancel(_msg: Any) -> None:
        await invoke_cleanup()

    cancel_unsub = await client.subscribe(
        f"{callback_subject}.cancel",
        on_cancel,
    )

    # Return combined cleanup
    async def cleanup() -> None:
        await cancel_unsub()
        await invoke_cleanup()

    return cleanup
