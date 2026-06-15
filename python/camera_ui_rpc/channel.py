"""Bidirectional communication channel between RPC clients."""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal, TypedDict, overload

from .utils import is_async_function

if TYPE_CHECKING:
    from .client import RPCClient


class ChannelMessage(TypedDict):
    """Channel message format."""

    type: Literal["message", "close", "error"]
    data: Any | None
    error: str | None
    sender: str | None  # For private channels


class Channel:
    """Bidirectional communication channel between RPC clients."""

    def __init__(self, client: "RPCClient", channel_id: str):
        """Initialize channel."""
        self._client: RPCClient = client
        self._channel_id: str = channel_id
        self._closed: bool = False
        self._initialized: bool = False
        self._unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None
        self.__handlers: dict[str, set[Callable[[Any], None | Awaitable[None]]]] = {}
        self.__close_handlers: set[Callable[[], None | Awaitable[None]]] = set()
        self.__error_handlers: set[Callable[[Exception], None | Awaitable[None]]] = set()
        self.__subscriptions: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self.__isolated_client: RPCClient | None = None

    @property
    def is_closed(self) -> bool:
        """Check if channel is closed."""
        return self._closed

    @property
    def id(self) -> str:
        """Get the channel ID."""
        return self._channel_id

    async def init(self) -> None:
        """Initialize the channel (called by RPCClient)."""
        if self._initialized:
            return

        self._initialized = True
        subject = f"channel.{self._channel_id}"

        async def message_handler(msg: ChannelMessage) -> None:
            if msg["type"] == "message":
                await self._emit("message", msg.get("data"))
            elif msg["type"] == "close":
                await self._handle_close()
            elif msg["type"] == "error":
                await self._handle_error(Exception(msg.get("error", "Channel error")))

        self._unsubscribe = await self._client.subscribe(subject, message_handler)

    async def send(self, data: Any) -> None:
        """Send data through the channel."""
        if self._closed:
            raise RuntimeError("Channel is closed")

        msg: ChannelMessage = {"type": "message", "data": data, "error": None, "sender": None}

        await self._client.publish(f"channel.{self._channel_id}", msg)

    async def request(self, data: Any, timeout: int = 5000) -> Any:
        """Send a request and wait for reply."""
        if self._closed:
            raise RuntimeError("Channel is closed")

        subject = f"channel.{self._channel_id}.request"
        return await self._client.request(subject, data, timeout=timeout)

    async def on_request(self, handler: Callable[[Any], Any]) -> Callable[[], Coroutine[Any, None, None]]:
        """Setup a request handler for this channel."""
        subject = f"channel.{self._channel_id}.request"
        unsubscribe = await self._client.on_request(subject, handler)
        self.__subscriptions.append(unsubscribe)
        return unsubscribe

    @overload
    def on(self, event: Literal["message"], handler: Callable[[Any], None | Awaitable[None]]) -> None: ...
    @overload
    def on(self, event: Literal["close"], handler: Callable[[], None | Awaitable[None]]) -> None: ...
    @overload
    def on(self, event: Literal["error"], handler: Callable[[Exception], None | Awaitable[None]]) -> None: ...
    def on(self, event: str, handler: Callable[..., None | Awaitable[None]]) -> None:
        """Listen for events."""
        if event == "close":
            self.__close_handlers.add(handler)
        elif event == "error":
            self.__error_handlers.add(handler)
        else:
            if event not in self.__handlers:
                self.__handlers[event] = set()
            self.__handlers[event].add(handler)

    @overload
    def off(self, event: Literal["message"], handler: Callable[[Any], None | Awaitable[None]]) -> None: ...
    @overload
    def off(self, event: Literal["close"], handler: Callable[[], None | Awaitable[None]]) -> None: ...
    @overload
    def off(
        self, event: Literal["error"], handler: Callable[[Exception], None | Awaitable[None]]
    ) -> None: ...
    def off(self, event: str, handler: Callable[..., None | Awaitable[None]]) -> None:
        """Remove event listener."""
        if event == "close":
            self.__close_handlers.discard(handler)
        elif event == "error":
            self.__error_handlers.discard(handler)
        else:
            if event in self.__handlers:
                self.__handlers[event].discard(handler)

    async def close(self) -> None:
        """Close the channel gracefully."""
        if self._closed:
            return

        self._closed = True

        # Try to notify other side
        try:
            msg: ChannelMessage = {"type": "close", "data": None, "error": None, "sender": None}
            await self._client.publish(f"channel.{self._channel_id}", msg)
        except Exception:
            # Ignore publish errors during close
            pass

        # Cleanup
        await self._cleanup()

    async def _emit(self, event: str, data: Any = None) -> None:
        """Emit an event to handlers."""
        if event in self.__handlers:
            for handler in self.__handlers[event]:
                try:
                    if is_async_function(handler):
                        await handler(data)  # type: ignore[misc]
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(self._client.io_pool, handler, data)
                except Exception as e:
                    print(f"Error in channel handler: {e}")

    async def _handle_close(self) -> None:
        """Handle channel close."""
        if self._closed:
            return

        self._closed = True

        for handler in self.__close_handlers:
            with contextlib.suppress(Exception):
                if is_async_function(handler):
                    await handler()  # type: ignore[misc]
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(self._client.io_pool, handler)

        asyncio.create_task(self._cleanup())

    async def _handle_error(self, error: Exception) -> None:
        """Handle channel error."""
        for handler in self.__error_handlers:
            with contextlib.suppress(Exception):
                if is_async_function(handler):
                    await handler(error)  # type: ignore[misc]
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(self._client.io_pool, handler, error)

    async def _cleanup(self) -> None:
        """Clean up resources."""
        # Clear all handlers
        self.__handlers.clear()
        self.__close_handlers.clear()
        self.__error_handlers.clear()

        # Clear subscriptions
        await asyncio.gather(*[unsub() for unsub in self.__subscriptions], return_exceptions=True)

        # Unsubscribe from NATS
        if self._unsubscribe:
            with contextlib.suppress(Exception):
                await self._unsubscribe()
            self._unsubscribe = None

        # Disconnect isolated client if present
        if self.__isolated_client:
            with contextlib.suppress(Exception):
                await self.__isolated_client.disconnect()


class PrivateChannel(Channel):
    """Private channel for 1:1 communication between two specific clients."""

    def __init__(self, client: "RPCClient", channel_id: str, target_client_id: str):
        """Initialize private channel."""
        super().__init__(client, channel_id)
        self.__target_client_id: str = target_client_id

        # Use the original client name, not the isolated connection name
        full_name = client.options.get("name", f"client-{int(asyncio.get_event_loop().time() * 1000)}")
        # Extract base name by removing any suffixes added for isolated connections
        self.__client_id: str = full_name.split("-channel-")[0].split("-private-")[0]
        self.__remote_client_id: str | None = None
        self._unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

    @property
    def remote_id(self) -> str | None:
        """Get the remote client ID (if connected)."""
        return self.__remote_client_id

    async def init(self) -> None:
        """Initialize private channel with handshake."""
        if self._initialized:
            return

        self._initialized = True

        # Use a unique subject that includes channelId and both client IDs for true privacy
        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}"

        async def message_handler(msg: ChannelMessage) -> None:
            # Filter messages: only process if it's for us
            if msg.get("sender") == self.__client_id:
                # Skip our own messages
                return

            # Only accept messages from the target client
            if msg.get("sender") != self.__target_client_id:
                return

            # Set remote_client_id if not set
            if not self.__remote_client_id:
                self.__remote_client_id = msg["sender"]

            if msg["type"] == "message":
                # Filter out handshake messages
                if isinstance(msg.get("data"), dict):
                    data: Any = msg["data"]
                    if data.get("__handshake"):
                        # Handshake received, connection established
                        return
                await self._emit("message", msg.get("data"))
            elif msg["type"] == "close":
                await self._handle_close()
            elif msg["type"] == "error":
                await self._handle_error(Exception(msg.get("error", "Channel error")))

        self._unsubscribe = await self._client.subscribe(subject, message_handler)

        # Send initial handshake
        with contextlib.suppress(Exception):
            await self._send_raw(
                {
                    "type": "message",
                    "data": {"__handshake": True},
                    "sender": self.__client_id,
                    "error": None,
                }
            )

    async def send(self, data: Any) -> None:
        """Send data through the private channel."""
        if self._closed:
            raise RuntimeError("Channel is closed")

        await self._send_raw({"type": "message", "data": data, "sender": self.__client_id, "error": None})

    async def request(self, data: Any, timeout: int = 5000) -> Any:
        """Send a request and wait for reply using native NATS request/reply."""
        if self._closed:
            raise RuntimeError("Channel is closed")

        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}.request"
        return await self._client.request(subject, data, timeout=timeout)

    async def on_request(self, handler: Callable[[Any], Any]) -> Callable[[], Coroutine[Any, None, None]]:
        """Setup a request handler for this private channel."""
        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}.request"
        return await self._client.on_request(subject, handler)

    async def close(self) -> None:
        """Close the private channel gracefully."""
        if self._closed:
            return

        self._closed = True

        # Try to notify remote client
        with contextlib.suppress(Exception):
            await self._send_raw({"type": "close", "sender": self.__client_id, "data": None, "error": None})

        await self._cleanup()

    def is_connected_to(self, client_id: str) -> bool:
        """Check if channel is connected to a specific client."""
        return self.__remote_client_id == client_id

    async def _send_raw(self, msg: ChannelMessage) -> None:
        """Send raw message."""
        # Use the same subject format as in init()
        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}"
        await self._client.publish(subject, msg)
