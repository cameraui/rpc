import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal, TypedDict, overload

from .utils import is_async_function

if TYPE_CHECKING:
    from .client import RPCClient


class ChannelMessage(TypedDict):
    type: Literal["message", "close", "error"]
    data: Any | None
    error: str | None
    sender: str | None


class Channel:
    def __init__(self, client: "RPCClient", channel_id: str):
        self._client: RPCClient = client
        self._channel_id: str = channel_id
        self._closed: bool = False
        self._initialized: bool = False
        self._unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None
        # handler -> is_async, checked once at registration instead of per message
        self.__handlers: dict[str, dict[Callable[[Any], None | Awaitable[None]], bool]] = {}
        self.__close_handlers: dict[Callable[[], None | Awaitable[None]], bool] = {}
        self.__error_handlers: dict[Callable[[Exception], None | Awaitable[None]], bool] = {}
        # single underscore: RPCClient sets the two clients, PrivateChannel adds subscriptions
        self._subscriptions: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._isolated_client: RPCClient | None = None
        self._parent_client: RPCClient | None = None

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def id(self) -> str:
        return self._channel_id

    async def init(self) -> None:
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
        if self._closed:
            raise RuntimeError("Channel is closed")

        msg: ChannelMessage = {"type": "message", "data": data, "error": None, "sender": None}

        await self._client.publish(f"channel.{self._channel_id}", msg)

    async def request(self, data: Any, timeout: int = 5000) -> Any:
        if self._closed:
            raise RuntimeError("Channel is closed")

        subject = f"channel.{self._channel_id}.request"
        return await self._client.request(subject, data, timeout=timeout)

    async def on_request(self, handler: Callable[[Any], Any]) -> Callable[[], Coroutine[Any, None, None]]:
        subject = f"channel.{self._channel_id}.request"
        unsubscribe = await self._client.on_request(subject, handler)
        self._subscriptions.append(unsubscribe)
        return unsubscribe

    @overload
    def on(self, event: Literal["message"], handler: Callable[[Any], None | Awaitable[None]]) -> None: ...
    @overload
    def on(self, event: Literal["close"], handler: Callable[[], None | Awaitable[None]]) -> None: ...
    @overload
    def on(self, event: Literal["error"], handler: Callable[[Exception], None | Awaitable[None]]) -> None: ...
    def on(self, event: str, handler: Callable[..., None | Awaitable[None]]) -> None:
        handler_is_async = is_async_function(handler)
        if event == "close":
            self.__close_handlers[handler] = handler_is_async
        elif event == "error":
            self.__error_handlers[handler] = handler_is_async
        else:
            if event not in self.__handlers:
                self.__handlers[event] = {}
            self.__handlers[event][handler] = handler_is_async

    @overload
    def off(self, event: Literal["message"], handler: Callable[[Any], None | Awaitable[None]]) -> None: ...
    @overload
    def off(self, event: Literal["close"], handler: Callable[[], None | Awaitable[None]]) -> None: ...
    @overload
    def off(
        self, event: Literal["error"], handler: Callable[[Exception], None | Awaitable[None]]
    ) -> None: ...
    def off(self, event: str, handler: Callable[..., None | Awaitable[None]]) -> None:
        if event == "close":
            self.__close_handlers.pop(handler, None)
        elif event == "error":
            self.__error_handlers.pop(handler, None)
        else:
            if event in self.__handlers:
                self.__handlers[event].pop(handler, None)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        try:
            msg: ChannelMessage = {"type": "close", "data": None, "error": None, "sender": None}
            await self._client.publish(f"channel.{self._channel_id}", msg)
        except Exception:
            pass

        await self._cleanup()

    async def _emit(self, event: str, data: Any = None) -> None:
        if event in self.__handlers:
            # snapshot, an inline handler may add or remove listeners re-entrantly
            for handler, handler_is_async in list(self.__handlers[event].items()):
                try:
                    if handler_is_async:
                        await handler(data)  # type: ignore[misc]
                    else:
                        handler(data)
                except Exception as e:
                    print(f"Error in channel handler: {e}")

    async def _handle_close(self) -> None:
        if self._closed:
            return

        self._closed = True

        for handler, handler_is_async in list(self.__close_handlers.items()):
            with contextlib.suppress(Exception):
                if handler_is_async:
                    await handler()  # type: ignore[misc]
                else:
                    handler()

        asyncio.create_task(self._cleanup())

    async def _handle_error(self, error: Exception) -> None:
        for handler, handler_is_async in list(self.__error_handlers.items()):
            with contextlib.suppress(Exception):
                if handler_is_async:
                    await handler(error)  # type: ignore[misc]
                else:
                    handler(error)

    async def _cleanup(self) -> None:
        self.__handlers.clear()
        self.__close_handlers.clear()
        self.__error_handlers.clear()

        subscriptions = list(self._subscriptions)
        self._subscriptions.clear()
        await asyncio.gather(*[unsub() for unsub in subscriptions], return_exceptions=True)

        if self._unsubscribe:
            with contextlib.suppress(Exception):
                await self._unsubscribe()
            self._unsubscribe = None

        isolated = self._isolated_client
        if isolated is not None:
            self._isolated_client = None
            parent = self._parent_client
            if parent is not None and isolated in parent.isolated_clients:
                parent.isolated_clients.remove(isolated)
            # shielded: a close from the channel's own message handler cancels the task it runs in
            with contextlib.suppress(Exception):
                await asyncio.shield(isolated.disconnect())


class PrivateChannel(Channel):
    def __init__(self, client: "RPCClient", channel_id: str, target_client_id: str):
        super().__init__(client, channel_id)
        self.__target_client_id: str = target_client_id

        # the base client name, without the suffixes isolated connections add
        full_name = client.options.get("name", f"client-{int(asyncio.get_event_loop().time() * 1000)}")
        self.__client_id: str = full_name.split("-channel-")[0].split("-private-")[0]
        self.__remote_client_id: str | None = None
        self._unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

    @property
    def remote_id(self) -> str | None:
        return self.__remote_client_id

    async def init(self) -> None:
        if self._initialized:
            return

        self._initialized = True

        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}"

        async def message_handler(msg: ChannelMessage) -> None:
            if msg.get("sender") == self.__client_id:
                return

            if msg.get("sender") != self.__target_client_id:
                return

            if not self.__remote_client_id:
                self.__remote_client_id = msg["sender"]

            if msg["type"] == "message":
                if isinstance(msg.get("data"), dict):
                    data: Any = msg["data"]
                    if data.get("__handshake"):
                        return
                await self._emit("message", msg.get("data"))
            elif msg["type"] == "close":
                await self._handle_close()
            elif msg["type"] == "error":
                await self._handle_error(Exception(msg.get("error", "Channel error")))

        self._unsubscribe = await self._client.subscribe(subject, message_handler)

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
        if self._closed:
            raise RuntimeError("Channel is closed")

        await self._send_raw({"type": "message", "data": data, "sender": self.__client_id, "error": None})

    async def request(self, data: Any, timeout: int = 5000) -> Any:
        if self._closed:
            raise RuntimeError("Channel is closed")

        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}.request"
        return await self._client.request(subject, data, timeout=timeout)

    async def on_request(self, handler: Callable[[Any], Any]) -> Callable[[], Coroutine[Any, None, None]]:
        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}.request"
        unsubscribe = await self._client.on_request(subject, handler)
        self._subscriptions.append(unsubscribe)
        return unsubscribe

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        with contextlib.suppress(Exception):
            await self._send_raw({"type": "close", "sender": self.__client_id, "data": None, "error": None})

        await self._cleanup()

    def is_connected_to(self, client_id: str) -> bool:
        return self.__remote_client_id == client_id

    async def _send_raw(self, msg: ChannelMessage) -> None:
        # must match the subject init() subscribes
        sorted_ids = sorted([self.__client_id, self.__target_client_id])
        subject = f"channel.private.{self._channel_id}.{'.'.join(sorted_ids)}"
        await self._client.publish(subject, msg)
