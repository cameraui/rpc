"""RPC Client implementation."""

import asyncio
import contextlib
import ssl
import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Generic, Literal, TypeVar, cast, overload

from nats import (
    connect,  # pyright: ignore[reportUnknownVariableType]
    errors,
)
from nats.aio.client import NO_RESPONDERS_STATUS, ErrorCallback
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.api import Header

from .channel import Channel, PrivateChannel
from .chunking import ChunkingManager, create_chunks
from .codec import decode, encode
from .decorators import extract_nested_methods_with_decorators, extract_nested_methods_without_decorators
from .errors import RPCException, create_error
from .executor import get_executor
from .handler import (
    format_error_dict,
    handle_callback_request,
    handle_normal_rpc,
    handle_pull_callback_request,
    handle_pull_iterator_request,
    handle_stream_request,
)
from .service import RPCService
from .types import (
    CallbackParams,
    ChunkedTransferHeader,
    ConnectionOptions,
    ErrorCode,
    NoResponderRetryOptions,
    PullCallbackParams,
    PullIteratorRequest,
    PullIteratorResponse,
    RPCAuthOptions,
    RPCClientOptions,
    RPCError,
    RPCMessage,
    RPCResponse,
    StreamMessage,
)
from .types import ProxyWithClose as ProxyWithCloseProtocol
from .types import RPCClient as RPCClientProtocol
from .utils import create_proxy, create_service_proxy, generate_id, is_async_function

T = TypeVar("T")


def create_rpc_client(options: RPCClientOptions) -> RPCClientProtocol:
    """Create a new RPC client instance."""
    return RPCClient(options)


class ProxyWithClose(Generic[T], ProxyWithCloseProtocol[T]):
    def __init__(self, proxy: T) -> None:
        self._proxy: T = proxy

    @property
    def proxy(self) -> T:
        return self._proxy

    async def close(self) -> None:
        isolated_client = cast(RPCClient | None, getattr(self.proxy, "_isolated_client", None))
        if isolated_client:
            await isolated_client.disconnect()
            # Remove from parent's tracked list if still present
            parent = cast(RPCClient | None, getattr(self.proxy, "_parent_client", None))
            if parent and isolated_client in parent.isolated_clients:
                parent.isolated_clients.remove(isolated_client)


class RPCClient(RPCClientProtocol):
    """RPC client for NATS-based communication."""

    def __init__(self, options: RPCClientOptions):
        """Initialize RPC client with options."""
        self.options: RPCClientOptions = options
        self.service: RPCService = RPCService(cast(Any, self))

        self.nc: NATSClient | None = None
        self.subscriptions: dict[str, list[Subscription]] = {}
        self.chunking_manager: ChunkingManager = ChunkingManager()
        self._max_payload_size: int = 1024 * 1024  # Default 1MB
        self._connection_task: asyncio.Task[NATSClient] | None = None

        self.pending_requests: dict[str, dict[str, Any]] = {}
        self.stream_handlers: dict[str, dict[str, Any]] = {}
        self.isolated_clients: list[RPCClient] = []
        self.pull_iterator_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self.callback_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}

        self._closed = False
        self._suspending = False
        self._subscription_meta: list[dict[str, Any]] = []

        self.io_pool: ThreadPoolExecutor = get_executor()

    @property
    def is_connected(self) -> bool:
        """Check connection status."""
        return self.nc is not None and self.nc.is_connected

    @property
    def is_closed(self) -> bool:
        """Check if the client is closed."""
        return self._closed

    @property
    def max_payload_size(self) -> int:
        """Get the maximum payload size."""
        return self._max_payload_size

    def create_isolated_client(self, options: RPCClientOptions) -> "RPCClient":
        """Create a new isolated RPC client."""
        return RPCClient(options)

    async def connect(self) -> NATSClient:
        """Connect to NATS server."""
        if self.nc and self.nc.is_connected:
            return self.nc

        if not self._connection_task:
            self._connection_task = asyncio.create_task(self._connect())

        try:
            self.nc = await self._connection_task
        finally:
            self._connection_task = None

        return self.nc

    def _make_error_cb(self) -> ErrorCallback:
        """Build the NATS error callback.

        nats-py's default callback dumps full tracebacks to stderr for every transport
        hiccup. During shutdown the broker goes away under us, so connection-reset and
        reconnect errors are expected and pure noise — swallow them. A consumer-provided
        error_cb still wins outside that case.
        """
        user_cb: ErrorCallback | None = self.options.get("error_cb")

        async def _error_cb(error: Exception) -> None:
            if self._closed:
                return
            if user_cb is not None:
                await user_cb(error)

        return _error_cb

    async def _connect(self) -> NATSClient:
        """Internal connection method."""
        # Build connection options
        connect_opts: ConnectionOptions = {
            "servers": self.options["servers"],
            "name": self.options["name"],
            "allow_reconnect": self.options.get("reconnect", True),
            "max_reconnect_attempts": self.options.get("max_reconnect_attempts", -1),
            "reconnect_time_wait": int(
                self.options.get("reconnect_time_wait", 2000) / 1000
            ),  # Convert to seconds
            # "no_echo": True,  # Don't echo messages back to the client
            "pending_size": 6 * 1024 * 1024,  # 6MB pending buffer
            "error_cb": self._make_error_cb(),
        }

        # Add auth if provided
        if auth := self.options.get("auth"):
            connect_opts["user"] = auth.get("user")
            connect_opts["password"] = auth.get("password")

        # Add TLS if provided
        if tls := self.options.get("tls"):
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            if tls.get("ca"):
                context.load_verify_locations(tls["ca"])
            if tls.get("cert") and tls.get("key"):
                context.load_cert_chain(tls["cert"], tls["key"])
            connect_opts["tls"] = context

        self.nc = await connect(**connect_opts)

        # Initialize service
        self.service.init(self.nc)

        # Get max_payload from server info
        self._max_payload_size = self.nc.max_payload

        # Reserve 8KB for NATS protocol overhead and MsgPack envelope per message
        self._max_payload_size = self._max_payload_size - 8192

        # Restore subscriptions after reconnect (used by suspend/connect cycle)
        if self._subscription_meta:
            metas = list(self._subscription_meta)
            self._subscription_meta.clear()
            for meta in metas:
                await self.subscribe(meta["pattern"], meta["handler"], **(meta.get("options") or {}))

        return self.nc

    async def disconnect(self) -> None:
        """Disconnect from NATS server."""
        self._closed = True
        self._subscription_meta.clear()

        # Cleanup pending requests
        for pending in self.pending_requests.values():
            if timeout_handle := pending.get("timeout"):
                timeout_handle.cancel()
            if reject := pending.get("reject"):
                reject(create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed"))
        self.pending_requests.clear()

        # Cleanup stream handlers
        for handler in self.stream_handlers.values():
            try:
                if end := handler.get("end"):
                    end()
            except Exception:
                pass
        self.stream_handlers.clear()

        # Cleanup pull iterators
        await asyncio.gather(
            *[cleanup() for cleanup in self.pull_iterator_cleanups.values()], return_exceptions=True
        )
        self.pull_iterator_cleanups.clear()

        # Cleanup callbacks
        await asyncio.gather(
            *[cleanup() for cleanup in self.callback_cleanups.values()], return_exceptions=True
        )
        self.callback_cleanups.clear()

        # Unsubscribe all subscriptions
        for subs in self.subscriptions.values():
            for sub in subs:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
        self.subscriptions.clear()

        # Clear chunking manager
        self.chunking_manager = ChunkingManager()

        # Shutdown thread pool
        # self.io_pool.shutdown(wait=False)

        # Disconnect all isolated clients, continue even if some fail
        if self.isolated_clients:
            await asyncio.gather(
                *[client.disconnect() for client in self.isolated_clients],
                return_exceptions=True,
            )

        # Close connection with timeout
        if self.nc:
            timeout = self.options.get("disconnect_timeout", 2000) / 1000
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(self.nc.close(), timeout=timeout)
            self.nc = None

    async def suspend(self) -> None:
        """Suspend the connection without marking as closed, preserving subscription metadata for reconnect."""
        # Block on_finish-driven unsubscribe from clearing _subscription_meta
        # while we tear the transport down — the task ending here is not a user-
        # initiated unsubscribe.
        self._suspending = True
        try:
            await self._do_suspend()
        finally:
            self._suspending = False

    async def _do_suspend(self) -> None:
        # Cleanup pending requests
        for pending in self.pending_requests.values():
            if timeout_handle := pending.get("timeout"):
                timeout_handle.cancel()
            if reject := pending.get("reject"):
                reject(create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed"))
        self.pending_requests.clear()

        # Cleanup stream handlers
        for handler in self.stream_handlers.values():
            try:
                if end := handler.get("end"):
                    end()
            except Exception:
                pass
        self.stream_handlers.clear()

        # Cleanup pull iterators
        await asyncio.gather(
            *[cleanup() for cleanup in self.pull_iterator_cleanups.values()], return_exceptions=True
        )
        self.pull_iterator_cleanups.clear()

        # Cleanup callbacks
        await asyncio.gather(
            *[cleanup() for cleanup in self.callback_cleanups.values()], return_exceptions=True
        )
        self.callback_cleanups.clear()

        # Unsubscribe all subscriptions
        for subs in self.subscriptions.values():
            for sub in subs:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
        self.subscriptions.clear()

        # Clear chunking manager
        self.chunking_manager = ChunkingManager()

        # Disconnect all isolated clients, continue even if some fail
        if self.isolated_clients:
            await asyncio.gather(
                *[client.disconnect() for client in self.isolated_clients],
                return_exceptions=True,
            )

        # Close connection with timeout
        if self.nc:
            timeout = self.options.get("disconnect_timeout", 2000) / 1000
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(self.nc.close(), timeout=timeout)
            self.nc = None

    def reconfigure(
        self,
        servers: list[str] | None = None,
        auth: RPCAuthOptions | None = None,
    ) -> None:
        """Update connection options between suspend() and connect().

        Used for token rotation / endpoint switching: subscription metadata is
        preserved by suspend(), reconfigure() points the next connect() at the
        new server, and connect() re-subscribes everything on the fresh
        transport.
        """
        if self.nc and self.nc.is_connected:
            raise RuntimeError("Cannot reconfigure while connected. Call suspend() first.")
        if servers is not None:
            self.options["servers"] = servers
        if auth is not None:
            self.options["auth"] = auth
        for child in self.isolated_clients:
            child.reconfigure(servers=servers, auth=auth)

    async def _with_no_responder_retry(
        self,
        fn: Callable[[], Awaitable[T]],
        override: NoResponderRetryOptions | None = None,
    ) -> T:
        """Retry a call on NoRespondersError with configurable delays.

        ``override`` lets a single call extend (or shrink) the retry window
        beyond the client-wide default — useful when a specific responder is
        known to be flaky (e.g. a child process that may be restarting).
        """
        src = override if override is not None else self.options.get("no_responder_retry", {}) or {}
        max_retries = src.get("max_retries", 3)
        delays = src.get("delays", [0.5, 1.0, 2.0])

        for attempt in range(max_retries + 1):
            try:
                return await fn()
            except Exception as e:
                is_no_responder = (
                    isinstance(e, errors.NoRespondersError)
                    or ("no responders" in str(e).lower())
                    or (isinstance(e, RPCException) and e.code == "503")
                )

                if not is_no_responder or attempt >= max_retries or self._closed:
                    raise

                delay = delays[min(attempt, len(delays) - 1)]
                await asyncio.sleep(delay)

        # Should never reach here, but satisfy type checker
        raise RuntimeError("Retry loop exhausted")

    async def publish(
        self, subject: str, data: Any, headers: dict[str, str] | None = None, reply: str | None = None
    ) -> None:
        """Public publish method with automatic chunking."""
        if not self.nc:
            raise RuntimeError("Not connected")

        encoded = encode(data)

        replyTo = reply if reply else ""

        # Small enough to send directly
        if len(encoded) <= self._max_payload_size:
            await self.nc.publish(subject, encoded, headers=headers, reply=replyTo)
            return

        # Message is too large, chunk it
        transfer_id = generate_id()
        total_chunks = (len(encoded) + self._max_payload_size - 1) // self._max_payload_size

        # Send header message first
        header_msg: ChunkedTransferHeader = {
            "type": "chunked",
            "transferId": transfer_id,
            "totalChunks": total_chunks,
            "totalSize": len(encoded),
            "chunkSize": self._max_payload_size,
        }

        # Header message includes original headers if any
        hdrs = headers or {}
        hdrs["x-chunked-transfer"] = "header"
        hdrs["x-chunk-id"] = transfer_id

        await self.nc.publish(subject, encode(header_msg), headers=hdrs, reply=replyTo)

        # Send chunks lazily from generator (one at a time, not materialized)
        for i, chunk in enumerate(create_chunks(encoded, transfer_id, self._max_payload_size)):
            chunk_hdrs = {"x-chunked-transfer": "chunk", "x-chunk-id": transfer_id, "x-chunk-index": str(i)}

            # Send raw chunk data (not encoded)
            await self.nc.publish(subject, chunk["data"], headers=chunk_hdrs, reply=replyTo)

            # Yield every 50 chunks to prevent blocking
            if i > 0 and i % 50 == 0:
                await asyncio.sleep(0)

    async def subscribe(
        self,
        pattern: str,
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
        queue: str = "",
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        """Public subscribe method with automatic chunk handling."""
        if not self.nc:
            raise RuntimeError("Not connected")

        async def message_handler(msg: Msg) -> None:
            try:
                chunk_type = msg.headers.get("x-chunked-transfer") if msg.headers else None

                if chunk_type == "header":
                    # Chunked transfer header
                    data = decode(msg.data)
                    chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None

                    if not chunk_id or data.get("transferId") != chunk_id:
                        return

                    def on_complete(assembled_data: Any) -> None:
                        asyncio.create_task(self._handle_assembled_data(handler, assembled_data))

                    def on_error(error: Exception) -> None:
                        print(f"Error assembling chunks for {pattern}: {error}")

                    # Setup chunk assembly with pre-allocated buffer
                    self.chunking_manager.start_receiving(
                        data["transferId"],
                        data["totalChunks"],
                        on_complete,
                        on_error,
                        data.get("totalSize"),  # Pass totalSize for pre-allocation
                        data.get("chunkSize"),  # Pass chunkSize for correct offset calculation
                    )

                elif chunk_type == "chunk":
                    # Chunk data
                    if not msg.headers:
                        return

                    chunk_id = msg.headers.get("x-chunk-id")
                    chunk_index = int(msg.headers.get("x-chunk-index", "0"))

                    if not chunk_id:
                        return

                    # Process raw chunk data
                    self.chunking_manager.process_chunk(
                        {"id": chunk_id, "chunkIndex": chunk_index, "data": msg.data, "isLast": False}
                    )

                else:
                    # Regular message - decode MessagePack data
                    data = decode(msg.data)
                    if is_async_function(handler):
                        await handler(data)  # type: ignore[misc]
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(self.io_pool, handler, data)

            except Exception as e:
                print(f"Error processing message for {pattern}:", e)
                print(traceback.format_exc())

        sub = await self.nc.subscribe(pattern, queue=queue, cb=message_handler)

        # Ensure the subscription is ready
        await asyncio.sleep(0)

        if pattern not in self.subscriptions:
            self.subscriptions[pattern] = []
        self.subscriptions[pattern].append(sub)

        # Store subscription metadata for suspend/reconnect
        options: dict[str, Any] = {}
        if queue:
            options["queue"] = queue
        meta = {"pattern": pattern, "handler": handler, "options": options}
        self._subscription_meta.append(meta)

        async def unsubscribe() -> None:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()

            if pattern in self.subscriptions:
                self.subscriptions[pattern] = [s for s in self.subscriptions[pattern] if s != sub]
                if not self.subscriptions[pattern]:
                    del self.subscriptions[pattern]

            # During suspend the NATS msg-task ends as a side-effect of the
            # transport teardown — we want to keep _subscription_meta so that
            # the next connect() can restore subscriptions.
            if not self._suspending:
                self._subscription_meta = [m for m in self._subscription_meta if m["pattern"] != pattern]

        async def on_finish(fut: asyncio.Task[None]) -> None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(fut, None)
                await unsubscribe()

        msg_task: asyncio.Task[None] | None = getattr(sub, "_wait_for_msgs_task", None)
        if msg_task:
            asyncio.create_task(on_finish(msg_task))

        return unsubscribe

    async def _handle_assembled_data(self, handler: Callable[[Any], Any], data: Any) -> None:
        """Handle assembled data from chunks."""
        try:
            if is_async_function(handler):
                await handler(data)  # pyright: ignore[reportGeneralTypeIssues]
            else:
                # Run sync handlers in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.io_pool, handler, data)
        except Exception as e:
            print(f"Error in handler: {e}")

    async def request(
        self,
        subject: str,
        data: Any,
        timeout: int | None = None,
        headers: dict[str, str] | None = None,
        no_responder_retry: NoResponderRetryOptions | None = None,
    ) -> Any:
        """Native NATS request/reply, with automatic retry on no-responder errors.

        ``no_responder_retry`` overrides the client-wide retry config for this
        single call.
        """
        return await self._with_no_responder_retry(
            lambda: self._request_once(subject, data, timeout=timeout, headers=headers),
            override=no_responder_retry,
        )

    async def _request_once(
        self, subject: str, data: Any, timeout: int | None = None, headers: dict[str, str] | None = None
    ) -> Any:
        """Single attempt of a native NATS request/reply."""
        if not self.nc:
            raise RuntimeError("Not connected")

        timeout_sec = (timeout or 5000) / 1000  # Convert to seconds
        encoded = encode(data)

        try:
            msg = await self.nc.request(subject, encoded, timeout=timeout_sec, headers=headers)

            # Check for NATS micro service error response
            if msg.headers and msg.headers.get("Nats-Service-Error-Code"):
                error_code = msg.headers.get("Nats-Service-Error-Code", "500")
                error_msg = msg.headers.get("Nats-Service-Error", "Service error")
                # Try to decode error data
                error_data = None
                if msg.data:
                    with contextlib.suppress(Exception):
                        error_data = decode(msg.data)
                raise create_error(error_code, error_msg, error_data)

            decoded = decode(msg.data)

            # Check if response contains an error field (for request handlers)
            if isinstance(decoded, dict) and "error" in decoded:
                code = cast(str, decoded.get("code", ErrorCode.INTERNAL_ERROR.value))
                message = cast(str, decoded.get("error", "Unknown error"))
                raise create_error(code, message, cast(Any, decoded))

            return cast(Any, decoded)
        except TimeoutError as e:
            raise create_error(
                ErrorCode.TIMEOUT, f"Request to {subject!r} timed out after {timeout or 5000}ms"
            ) from e
        except Exception as e:
            if "no responders" in str(e).lower():
                raise create_error(ErrorCode.NOT_FOUND, "No responders available") from e
            raise

    async def call(
        self,
        subject: str,
        *args: Any,
        no_responder_retry: NoResponderRetryOptions | None = None,
    ) -> Any:
        """Make an RPC call with no-responder retry.

        ``no_responder_retry`` overrides the client-wide retry config for this
        single call.
        """
        return await self._with_no_responder_retry(
            lambda: self._call_once(subject, *args),
            override=no_responder_retry,
        )

    async def _call_once(self, subject: str, *args: Any) -> Any:
        """Make a single RPC call without retry."""
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        request_id = generate_id()
        timeout = self.options.get("timeout", 30000)
        # Use different reply patterns for RPC vs service calls
        reply_subject = (
            f"rpc.reply.{request_id}" if subject.startswith("rpc.") else f"{subject}.reply.{request_id}"
        )

        # Create future for the response
        future: asyncio.Future[Any] = asyncio.Future()

        # Setup timeout
        def handle_timeout() -> None:
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]
                if not future.done():
                    future.set_exception(
                        errors.TimeoutError(f"RPC call to {subject!r} timed out after {timeout}ms")
                    )

        timeout_handle = asyncio.get_event_loop().call_later(timeout / 1000, handle_timeout)

        # Store pending request
        self.pending_requests[request_id] = {"future": future, "timeout": timeout_handle}

        # Handle no responders
        sub: Subscription | None = None
        unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

        # Unsubscribe function to clean up
        async def unsubscribe_all() -> None:
            if request_id in self.pending_requests:
                self.pending_requests[request_id]["timeout"].cancel()
                del self.pending_requests[request_id]
            if sub and not sub._closed:  # pyright: ignore[reportPrivateUsage]
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            if unsubscribe:
                with contextlib.suppress(Exception):
                    await unsubscribe()

        # Subscribe to reply
        async def handle_rpc_response(data: Any) -> None:
            response = data  # assuming data is already parsed as RPCResponse
            if response.get("id") == request_id:
                pending = self.pending_requests.get(response["id"])

                if pending:
                    del self.pending_requests[response["id"]]
                    pending["timeout"].cancel()
                    await unsubscribe_all()

                    if "error" in response:
                        pending["future"].set_exception(RPCException.from_dict(response["error"]))
                    else:
                        result = response.get("result")
                        # Attach __methods to result for proxy method discovery
                        if "__methods" in response and result is not None and isinstance(result, dict):
                            result["__methods"] = response["__methods"]
                        pending["future"].set_result(result)

        async def request_callback(msg: Msg) -> None:
            # Check for no responders status (empty message with 503 status)
            if (
                (msg.data is None or len(msg.data) == 0)  # pyright: ignore[reportUnnecessaryComparison]
                and msg.headers
                and msg.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
            ):
                if not future.done():
                    future.set_exception(errors.NoRespondersError(subject))

                # Cleanup
                await unsubscribe_all()

        unsubscribe = await self.subscribe(reply_subject, handle_rpc_response)

        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox, cb=request_callback, max_msgs=1)

        try:
            # Send request
            message: RPCMessage = {"id": request_id, "method": "call", "params": args}
            await self.publish(subject, message, reply=inbox)

            # Wait for response
            return await future
        except Exception:
            # Cleanup on error
            await unsubscribe_all()
            raise

    def _handle_timeout(
        self, request_id: str, subject: str, future: asyncio.Future[Any], timeout: int
    ) -> None:
        """Handle request timeout."""
        if request_id in self.pending_requests:
            del self.pending_requests[request_id]
            if not future.done():
                future.set_exception(
                    create_error(ErrorCode.TIMEOUT, f"RPC call to {subject!r} timed out after {timeout}ms")
                )

    def _handle_rpc_response(self, data: Any) -> None:
        """Handle RPC response."""
        response = data
        pending = self.pending_requests.get(response["id"])

        if pending:
            del self.pending_requests[response["id"]]
            if timeout_handle := pending.get("timeout"):
                timeout_handle.cancel()

            future = pending["future"]
            if error := response.get("error"):
                future.set_exception(RPCException.from_dict(error))
            else:
                future.set_result(response.get("result"))

    async def call_stream(
        self,
        subject: str,
        *args: Any,
        no_responder_retry: NoResponderRetryOptions | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Make a streaming RPC call, with automatic retry on no-responder errors.

        ``no_responder_retry`` overrides the client-wide retry config for this
        single call.
        """
        src = (
            no_responder_retry
            if no_responder_retry is not None
            else self.options.get("no_responder_retry", {}) or {}
        )
        max_retries = src.get("max_retries", 3)
        delays = src.get("delays", [0.5, 1.0, 2.0])

        for attempt in range(max_retries + 1):
            try:
                async for value in self._call_stream_once(subject, *args):
                    yield value
                return
            except Exception as e:
                is_no_responder = (
                    isinstance(e, errors.NoRespondersError)
                    or ("no responders" in str(e).lower())
                    or (isinstance(e, RPCException) and e.code == "503")
                )

                if not is_no_responder or attempt >= max_retries or self._closed:
                    raise

                delay = delays[min(attempt, len(delays) - 1)]
                await asyncio.sleep(delay)

    async def _call_stream_once(self, subject: str, *args: Any) -> AsyncGenerator[Any, None]:
        """Single attempt of a streaming RPC call."""
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        request_id = generate_id()
        stream_subject = f"stream.{subject}.{request_id}"

        # Stream state
        queue: asyncio.Queue[Any] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        # Initialize variables
        sub: Subscription | None = None
        unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

        def on_push(value: Any) -> None:
            nonlocal ended
            if not ended:
                queue.put_nowait(value)

        def on_end() -> None:
            nonlocal ended
            ended = True
            queue.put_nowait(None)  # End marker

        def on_error(err: Exception) -> None:
            nonlocal error, ended
            error = err
            ended = True
            queue.put_nowait(None)  # End marker

        # Setup handlers
        handler: dict[str, Callable[..., None]] = {
            "push": on_push,
            "end": on_end,
            "error": on_error,
        }

        self.stream_handlers[request_id] = handler

        # Unsubscribe function to clean up
        async def unsubscribe_all() -> None:
            nonlocal ended

            if request_id in self.stream_handlers:
                del self.stream_handlers[request_id]
            if sub and not sub._closed:  # pyright: ignore[reportPrivateUsage]
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            if unsubscribe:
                with contextlib.suppress(Exception):
                    await unsubscribe()

            # Notify server to stop
            if not ended:
                ended = True
                with contextlib.suppress(Exception):
                    await self.publish(f"{stream_subject}.cancel", {"id": request_id})

        # Subscribe to stream messages
        async def handle_stream_message(msg: StreamMessage) -> None:
            if msg.get("id") != request_id:
                return

            stream_handler = self.stream_handlers.get(msg["id"])
            if not stream_handler:
                return

            msg_type = msg.get("type")
            if msg_type == "data":
                stream_handler["push"](msg.get("data"))
            elif msg_type == "end":
                stream_handler["end"]()
                await unsubscribe_all()
            elif msg_type == "error":
                error_data = cast(RPCError, msg.get("error"))
                stream_handler["error"](RPCException.from_dict(error_data))
                await unsubscribe_all()

        async def request_callback(msg: Msg) -> None:
            # Check for no responders status (empty message with 503 status)
            if (
                (msg.data is None or len(msg.data) == 0)  # pyright: ignore[reportUnnecessaryComparison]
                and msg.headers
                and msg.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
            ):
                stream_handler = self.stream_handlers.get(request_id)
                if stream_handler:
                    stream_handler["error"](errors.NoRespondersError(subject))
                await unsubscribe_all()

        unsubscribe = await self.subscribe(stream_subject, handle_stream_message)

        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox, cb=request_callback, max_msgs=1)

        try:
            # Send request
            stream_params: dict[str, Any] = {
                "__stream": True,
                "__streamSubject": stream_subject,
                "args": args,
            }
            message: RPCMessage = {"id": request_id, "method": "stream", "params": stream_params}
            await self.publish(subject, message, reply=inbox)

            # Generator implementation
            while True:
                if error:
                    await unsubscribe_all()
                    raise error

                try:
                    # Wait for next value
                    value = await queue.get()
                    if value is None:  # End marker
                        if error:
                            await unsubscribe_all()
                            raise error
                        await unsubscribe_all()
                        return
                    yield value
                except Exception as e:
                    await unsubscribe_all()
                    raise e

        except Exception:
            await unsubscribe_all()
            raise

    async def call_pull_iterator(self, subject: str, *args: Any) -> AsyncGenerator[Any, None]:
        """Make a pull-based iterator RPC call."""
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        iterator_id = generate_id()
        request_subject = f"_rpc.iterator.{iterator_id}.request"
        response_subject = f"_rpc.iterator.{iterator_id}.response"

        # Initialize the pull iterator using regular call method
        # We pass a special __pullIterator marker as the first argument
        init_response = await self.call(
            subject, {"__pullIterator": True, "__iteratorId": iterator_id, "args": args}
        )

        # The response should contain the iterator ID (same as we sent)
        if not init_response or init_response.get("iteratorId") != iterator_id:
            raise RuntimeError("Failed to initialize pull iterator")

        # Subscribe to responses
        response_queue: asyncio.Queue[PullIteratorResponse] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        async def handle_response(msg: PullIteratorResponse) -> None:
            nonlocal ended, error

            if msg.get("type") == "error":
                if error_data := msg.get("error"):
                    error = RPCException.from_dict(error_data)
                ended = True
            elif msg.get("type") == "done":
                ended = True

            await response_queue.put(msg)

        unsubscribe = await self.subscribe(response_subject, handle_response)

        async def cleanup() -> None:
            if unsubscribe is not None:
                await unsubscribe()

            if sub and not sub._closed:  # pyright: ignore[reportPrivateUsage]
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

            # Send cancel request
            if not ended:
                with contextlib.suppress(Exception):
                    cancel_request: PullIteratorRequest = {
                        "id": iterator_id,
                        "type": "cancel",
                    }
                    await self.publish(request_subject, cancel_request)

        async def request_callback(msg2: Msg) -> None:
            nonlocal ended, error

            # Check for no responders status (empty message with 503 status)
            if (
                (msg2.data is None or len(msg2.data) == 0)  # pyright: ignore[reportUnnecessaryComparison]
                and msg2.headers
                and msg2.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
            ):
                ended = True
                error = create_error("503", str(errors.NoRespondersError(subject)))

                msg: PullIteratorResponse = {
                    "type": "error",
                    "id": iterator_id,
                    "error": error.to_dict(),
                }

                await response_queue.put(msg)

        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox, cb=request_callback, max_msgs=1)

        try:
            while True:
                # Send next request
                next_request: PullIteratorRequest = {
                    "id": iterator_id,
                    "type": "next",
                }
                await self.publish(request_subject, next_request, reply=inbox)

                # Wait for response
                response = await response_queue.get()

                if response.get("type") == "error":
                    if error_data := response.get("error"):
                        raise RPCException.from_dict(error_data)
                elif response.get("type") == "done":
                    break
                elif response.get("type") == "value":
                    yield response.get("value")

        finally:
            await cleanup()

    async def call_pull_iterator_with_callback(
        self,
        subject: str,
        callbacks: dict[str, Callable[..., Any]],
        oneway_methods: list[str],
        *args: Any,
    ) -> AsyncGenerator[None, None]:
        """Make a pull-iterator-with-callbacks RPC call.

        Combines client-driven pull iteration (1 RTT per batch) with a oneway
        callback channel (fire-and-forget server→client) for low-latency data
        delivery with coarse-grained backpressure.

        The returned async generator yields ``None`` for each batch boundary
        the server produces. Meaningful data travels through ``callbacks``.
        See packages/rpc/PULL_CALLBACK_PROTOCOL.md.
        """
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        iterator_id = generate_id()
        request_subject = f"_rpc.iterator.{iterator_id}.request"
        response_subject = f"_rpc.iterator.{iterator_id}.response"
        callback_subject = f"_rpc.cb.{iterator_id}"

        callback_methods = list(callbacks.keys())

        # Track a task-chain of pending callback handlers. The iterator loop
        # awaits this chain before yielding, so a slow handler blocks the
        # next `next()` request — which stalls the server at its own yield.
        # This is what gives true end-to-end backpressure.
        callback_task: asyncio.Task[None] | None = None

        # Subscribe to callback channel BEFORE init call.
        async def handle_cb_msg(msg: Any) -> None:
            nonlocal callback_task
            if not isinstance(msg, dict):
                return
            method = msg.get("method")
            fn = callbacks.get(method) if method else None
            if fn is None:
                return
            fn_args = msg.get("args") or []

            prev = callback_task

            async def run_chained() -> None:
                if prev is not None:
                    with contextlib.suppress(Exception):
                        await prev
                try:
                    if is_async_function(fn):
                        await fn(*fn_args)
                    else:
                        fn(*fn_args)
                except Exception as e:
                    print(f"[rpc] Pull-callback handler '{method}' threw: {e}")

            callback_task = asyncio.create_task(run_chained())

        cb_unsub = await self.subscribe(callback_subject, handle_cb_msg)

        # Init the pull-callback session via regular RPC call.
        init_params: PullCallbackParams = {
            "__pullCallback": True,
            "__iteratorId": iterator_id,
            "__callbackSubject": callback_subject,
            "__callbackMethods": callback_methods,
            "__onewayMethods": list(oneway_methods),
            "args": list(args),
        }

        try:
            init_response = await self.call(subject, init_params)
        except Exception:
            await cb_unsub()
            raise

        if not init_response or init_response.get("iteratorId") != iterator_id:
            await cb_unsub()
            raise RuntimeError("Failed to initialize pull-callback iterator")

        response_queue: asyncio.Queue[PullIteratorResponse] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        async def handle_response(msg: PullIteratorResponse) -> None:
            nonlocal ended, error
            if msg.get("type") == "error":
                if error_data := msg.get("error"):
                    error = RPCException.from_dict(error_data)
                ended = True
            elif msg.get("type") == "done":
                ended = True
            await response_queue.put(msg)

        resp_unsub = await self.subscribe(response_subject, handle_response)

        async def request_callback(msg2: Msg) -> None:
            nonlocal ended, error
            if (
                (msg2.data is None or len(msg2.data) == 0)  # pyright: ignore[reportUnnecessaryComparison]
                and msg2.headers
                and msg2.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
            ):
                ended = True
                error = create_error("503", str(errors.NoRespondersError(subject)))
                msg: PullIteratorResponse = {
                    "type": "error",
                    "id": iterator_id,
                    "error": error.to_dict(),
                }
                await response_queue.put(msg)

        inbox = self.nc.new_inbox()
        sub = await self.nc.subscribe(inbox, cb=request_callback, max_msgs=1)

        async def cleanup() -> None:
            await cb_unsub()
            if resp_unsub is not None:
                await resp_unsub()
            if sub and not sub._closed:  # pyright: ignore[reportPrivateUsage]
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            if not ended:
                with contextlib.suppress(Exception):
                    cancel_request: PullIteratorRequest = {
                        "id": iterator_id,
                        "type": "cancel",
                    }
                    await self.publish(request_subject, cancel_request)

        try:
            while True:
                next_request: PullIteratorRequest = {
                    "id": iterator_id,
                    "type": "next",
                }
                await self.publish(request_subject, next_request, reply=inbox)

                response = await response_queue.get()

                if response.get("type") == "error":
                    if error_data := response.get("error"):
                        raise RPCException.from_dict(error_data)
                elif response.get("type") == "done":
                    break
                elif response.get("type") == "value":
                    # Wait for all callback handlers dispatched so far. If a
                    # handler awaits a bounded queue (backpressure), this
                    # stalls the iterator here until the handler releases,
                    # which suspends the server at its own yield.
                    if callback_task is not None:
                        with contextlib.suppress(Exception):
                            await callback_task
                    yield None
        finally:
            await cleanup()

    def _handle_stream_message(self, msg: StreamMessage) -> None:
        """Handle streaming message."""
        stream_handler = self.stream_handlers.get(msg["id"])
        if not stream_handler:
            return

        if msg["type"] == "data":
            stream_handler["push"](msg.get("data"))
        elif msg["type"] == "end":
            stream_handler["push"](None)  # End marker
            stream_handler["end"]()
            if msg["id"] in self.stream_handlers:
                del self.stream_handlers[msg["id"]]
        elif msg["type"] == "error":
            if error_data := msg.get("error"):
                stream_handler["error"](RPCException.from_dict(error_data))
            stream_handler["push"](None)  # End marker
            if msg["id"] in self.stream_handlers:
                del self.stream_handlers[msg["id"]]

    async def call_with_callback(
        self, subject: str, args: list[Any], callback: Callable[[Any], Any]
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        """Make an RPC call with a callback subscription. Returns async unsubscribe."""
        if not self.is_connected and not self.is_closed:
            await self.connect()
        if not self.nc:
            raise RuntimeError("Not connected")

        request_id = generate_id()
        callback_subject = f"rpc.cb.{request_id}"

        # Subscribe to callback messages
        async def handle_callback_msg(msg: Any) -> None:
            if isinstance(msg, dict):
                if msg.get("type") == "data":
                    try:
                        if is_async_function(callback):
                            await callback(msg.get("data"))
                        else:
                            callback(msg.get("data"))
                    except Exception as e:
                        print(f"[rpc] Callback error: {e}")
                elif msg.get("type") == "error":
                    print(f"[rpc] Callback error: {msg.get('error')}")

        unsub = await self.subscribe(callback_subject, handle_callback_msg)

        # Send RPC request with callback marker
        callback_params: CallbackParams = {
            "__callback": True,
            "__callbackSubject": callback_subject,
            "args": list(args),
        }

        try:
            await self.call(subject, callback_params)
        except Exception:
            await unsub()
            raise

        # Return async unsubscribe function
        async def unsubscribe() -> None:
            with contextlib.suppress(Exception):
                await self.publish(f"{callback_subject}.cancel", {"id": request_id})
            await unsub()

        return unsubscribe

    async def register_handler(
        self,
        namespace: str,
        handlers: object,
        isolated_connection: bool = False,
        without_decorators: bool = False,
        queue: str = "",
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        """Register RPC handlers."""
        if not self.nc:
            raise RuntimeError("Not connected")

        # Use isolated connection if requested
        client: RPCClient = self
        if isolated_connection:
            # Create isolated connection for this handler namespace
            opts: RPCClientOptions = {
                **self.options,
                "name": f"{self.options['name']}-handler-{namespace}",
            }
            client = self.create_isolated_client(opts)
            # Connect the isolated client
            await client.connect()
            self.isolated_clients.append(client)

        unsubscribers: list[Callable[[], Coroutine[Any, Any, None]]] = []
        pull_iterator_ids: list[str] = []

        # Extract methods based on option
        handlers_map = (
            extract_nested_methods_without_decorators(handlers)
            if without_decorators
            else extract_nested_methods_with_decorators(handlers)
        )
        method_names = list(handlers_map.keys())

        for method, handler in handlers_map.items():
            subject = f"rpc.{namespace}.{method}"

            async def handle_message(
                msg: Any, handler: Any = handler, method_names: list[str] = method_names
            ) -> None:
                message = msg
                response: RPCResponse = {"id": message["id"], "__methods": method_names}

                try:
                    # Handle stream request
                    params = message.get("params")
                    is_stream_request = bool(
                        isinstance(params, dict)
                        and params.get("__stream") is not None
                        and params.get("__streamSubject") is not None
                    )

                    # Check if it's a pull iterator request
                    # Could be direct object or wrapped in array from call()
                    is_pull_iterator = False
                    pull_params: dict[str, Any] = {}

                    if params and isinstance(params, dict) and params.get("__pullIterator"):
                        is_pull_iterator = True
                        pull_params = cast(dict[str, Any], params)
                    elif (
                        isinstance(params, list)
                        and len(params) > 0  # pyright: ignore[reportUnknownArgumentType]
                        and isinstance(params[0], dict)
                        and params[0].get("__pullIterator")
                    ):
                        is_pull_iterator = True
                        pull_params = cast(dict[str, Any], params[0])

                    # Check if it's a pull-callback request
                    is_pull_callback = False
                    pc_params: dict[str, Any] = {}

                    if params and isinstance(params, dict) and params.get("__pullCallback"):
                        is_pull_callback = True
                        pc_params = cast(dict[str, Any], params)
                    elif (
                        isinstance(params, list)
                        and len(params) > 0  # pyright: ignore[reportUnknownArgumentType]
                        and isinstance(params[0], dict)
                        and params[0].get("__pullCallback")
                    ):
                        is_pull_callback = True
                        pc_params = cast(dict[str, Any], params[0])

                    if is_stream_request:
                        _params = cast(dict[str, Any], params)
                        stream_subject = cast(str, _params["__streamSubject"])
                        args = cast(list[Any], _params.get("args", []))

                        # Don't await stream requests - they run in background and send data via streamSubject
                        # Awaiting would block the subscription handler and prevent processing of new messages
                        async def run_stream() -> None:
                            try:
                                await handle_stream_request(
                                    handler,
                                    args,
                                    stream_subject,
                                    message["id"],
                                    client,
                                    client.io_pool,
                                )
                            except Exception as e:
                                print(f"Stream request error for {method}: {e}")

                        asyncio.create_task(run_stream())
                        return  # Don't send RPC response for stream requests
                    elif is_pull_iterator:
                        # Handle pull iterator request
                        args = cast(list[Any], pull_params.get("args", []))
                        iterator_id = cast(str, pull_params.get("__iteratorId", message["id"]))
                        cleanup = await handle_pull_iterator_request(
                            handler,
                            args,
                            iterator_id,
                            client,
                            client.io_pool,
                        )

                        # Store cleanup function for later
                        client.pull_iterator_cleanups[iterator_id] = cleanup
                        pull_iterator_ids.append(iterator_id)
                        response["result"] = {"iteratorId": iterator_id}

                        # Send response with iterator ID
                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    elif is_pull_callback:
                        # Handle pull-iterator-with-callbacks request
                        pc_args = cast(list[Any], pc_params.get("args", []))
                        pc_iterator_id = cast(str, pc_params.get("__iteratorId", message["id"]))
                        pc_callback_subject = cast(str, pc_params.get("__callbackSubject", ""))
                        pc_oneway_methods = cast(list[str], pc_params.get("__onewayMethods", []))

                        pc_cleanup = await handle_pull_callback_request(
                            handler,
                            pc_args,
                            pc_iterator_id,
                            pc_callback_subject,
                            pc_oneway_methods,
                            client,
                            client.io_pool,
                        )

                        client.pull_iterator_cleanups[pc_iterator_id] = pc_cleanup
                        pull_iterator_ids.append(pc_iterator_id)
                        response["result"] = {"iteratorId": pc_iterator_id}

                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    elif (
                        isinstance(params, dict)
                        and params.get("__callback") is not None
                        and params.get("__callbackSubject") is not None
                    ) or (
                        isinstance(params, list)
                        and len(params) > 0  # pyright: ignore[reportUnknownArgumentType]
                        and isinstance(params[0], dict)
                        and params[0].get("__callback")
                    ):
                        # Handle callback subscription request
                        _cb_params = cast(
                            dict[str, Any],
                            params[0] if isinstance(params, list) and params[0].get("__callback") else params,
                        )
                        callback_subject = cast(str, _cb_params["__callbackSubject"])
                        cb_args = cast(list[Any], _cb_params.get("args", []))

                        cb_cleanup = await handle_callback_request(
                            handler,
                            cb_args,
                            callback_subject,
                            message["id"],
                            client,
                            client.io_pool,
                        )
                        client.callback_cleanups[message["id"]] = cb_cleanup

                        response["result"] = {"ok": True}
                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    else:
                        # Normal RPC call
                        result = await handle_normal_rpc(
                            handler,
                            message.get("params", []),
                            client.io_pool,
                        )
                        response["result"] = result

                        # Send response
                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)

                except Exception as e:
                    response["error"] = format_error_dict(e)

                    try:
                        # If handler raised an exception, send error response
                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    except Exception as publish_error:
                        if client.is_closed:
                            return  # Ignore publish errors if client is closed

                        print(f"Failed to send error response: {publish_error}")

            unsubscribe = await client.subscribe(subject, handle_message, queue=queue)
            unsubscribers.append(unsubscribe)

        # Return combined unsubscribe function
        async def cleanup() -> None:
            # Unsubscribe all handlers
            await asyncio.gather(*(unsub() for unsub in unsubscribers), return_exceptions=True)

            async def cleanup_iterator(iterator_id: str) -> None:
                if cleanup := client.pull_iterator_cleanups.get(iterator_id):
                    await cleanup()
                    del client.pull_iterator_cleanups[iterator_id]

            # Cleanup pull iterators
            await asyncio.gather(
                *(cleanup_iterator(iterator_id) for iterator_id in pull_iterator_ids),
                return_exceptions=True,
            )

            # Cleanup callbacks
            callback_cleanups = list(client.callback_cleanups.items())
            for cb_id, cb_cleanup in callback_cleanups:
                with contextlib.suppress(Exception):
                    await cb_cleanup()
                client.callback_cleanups.pop(cb_id, None)

            if isolated_connection:
                # Disconnect isolated client if it was created
                await client.disconnect()
                if client in self.isolated_clients:
                    self.isolated_clients.remove(client)

        return cleanup

    async def on_request(
        self, pattern: str, handler: Callable[[Any], Any]
    ) -> Callable[[], Coroutine[Any, None, None]]:
        """Setup a request handler (responder)."""
        if not self.nc:
            raise RuntimeError("Not connected")

        async def handle_request(msg: Msg) -> None:
            try:
                # Decode request
                data = decode(msg.data)

                # Call handler with subject
                if is_async_function(handler):
                    result = await handler(data)  # pyright: ignore[reportGeneralTypeIssues]
                else:
                    loop = asyncio.get_event_loop()
                    func = partial(handler, data)
                    result = await loop.run_in_executor(self.io_pool, func)

                # Send response
                if msg.reply:
                    response = encode(result)
                    await msg.respond(response)

            except Exception as e:
                # Send error response
                if msg.reply:
                    error_response = encode(
                        {
                            "error": str(e),
                            "code": e.code if isinstance(e, RPCException) else ErrorCode.INTERNAL_ERROR.value,
                        }
                    )
                    await msg.respond(error_response)

        sub = await self.nc.subscribe(pattern, cb=handle_request)

        # Ensure the subscription is ready
        await asyncio.sleep(0)

        async def unsubscribe() -> None:
            """Unsubscribe from the request handler."""
            if sub:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

        # Return unsubscribe function
        return unsubscribe

    async def channel(self, channel_id: str, isolated_connection: bool = False) -> Channel:
        """Create or join a bidirectional channel."""
        if isolated_connection:
            # Create a new isolated client for this channel
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-channel-{channel_id}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        channel = Channel(cast(Any, client), channel_id)
        await channel.init()

        # Store reference for cleanup if isolated
        if isolated_connection:
            setattr(channel, "_isolated_client", client)  # noqa: B010

        return channel

    async def private_channel(
        self, channel_id: str, target_client_id: str, isolated_connection: bool = False
    ) -> PrivateChannel:
        """Create a private 1:1 channel."""
        if isolated_connection:
            # Create a new isolated client for this channel
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-private-{channel_id}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        channel = PrivateChannel(cast(Any, client), channel_id, target_client_id)
        await channel.init()

        # Store reference for cleanup if isolated
        if isolated_connection:
            setattr(channel, "_isolated_client", client)  # noqa: B010

        return channel

    @overload
    def create_proxy(
        self, namespace: str, type_class: type[T], isolated_connection: Literal[False] | None = None
    ) -> T: ...
    @overload
    def create_proxy(
        self, namespace: str, type_class: type[T], isolated_connection: Literal[True]
    ) -> ProxyWithClose[T]: ...
    @overload
    def create_proxy(
        self, namespace: str, type_class: None = None, isolated_connection: Literal[False] | None = None
    ) -> Any: ...
    @overload
    def create_proxy(
        self, namespace: str, type_class: None = None, isolated_connection: Literal[True] = True
    ) -> ProxyWithClose[Any]: ...
    def create_proxy(
        self, namespace: str, type_class: type[T] | None = None, isolated_connection: bool | None = False
    ) -> T | Any | ProxyWithClose[T] | ProxyWithClose[Any]:
        """Create proxy for type-safe RPC calls."""
        if isolated_connection:
            # Create an isolated proxy with its own connection
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-proxy-{namespace}"}
            )
            self.isolated_clients.append(client)

            proxy = create_proxy(cast(Any, client), namespace)

            # Store references for potential cleanup
            setattr(proxy, "_isolated_client", client)  # noqa: B010
            setattr(proxy, "_parent_client", self)  # noqa: B010
            return ProxyWithClose(proxy)
        else:
            return create_proxy(cast(Any, self), namespace)

    @overload
    async def create_service_proxy(
        self,
        service_name: str,
        type_class: type[T],
        isolated_connection: Literal[False] | None = None,
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> T: ...
    @overload
    async def create_service_proxy(
        self,
        service_name: str,
        type_class: type[T],
        isolated_connection: Literal[True],
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> ProxyWithClose[T]: ...
    @overload
    async def create_service_proxy(
        self,
        service_name: str,
        type_class: None = None,
        isolated_connection: Literal[False] | None = None,
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> Any: ...
    @overload
    async def create_service_proxy(
        self,
        service_name: str,
        type_class: None = None,
        isolated_connection: Literal[True] = True,
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> ProxyWithClose[Any]: ...
    async def create_service_proxy(
        self,
        service_name: str,
        type_class: type[T] | None = None,
        isolated_connection: bool | None = False,
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> T | Any | ProxyWithClose[T] | ProxyWithClose[Any]:
        """Create a service client proxy with automatic service discovery."""
        if isolated_connection:
            # Create a new isolated client for this service proxy
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-service-{service_name}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        # Discover available services
        monitor = client.service.monitor()
        services = await monitor.info(service_name)

        if not services:
            if isolated_connection:
                await client.disconnect()
                if client in self.isolated_clients:
                    self.isolated_clients.remove(client)
            raise RuntimeError(f"No services found with name: {service_name}")

        # Select service (prefer specific ID if provided)
        selected = (
            next((s for s in services if s.id == preferred_id), services[0]) if preferred_id else services[0]
        )

        # Create the proxy
        proxy = create_service_proxy(cast(Any, client), selected, timeout)

        # If isolated, store references for potential cleanup
        if isolated_connection:
            setattr(proxy, "_isolated_client", client)  # noqa: B010
            setattr(proxy, "_parent_client", self)  # noqa: B010
            return ProxyWithClose(proxy)
        else:
            return proxy
