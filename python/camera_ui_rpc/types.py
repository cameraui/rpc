from __future__ import annotations

import ssl
from collections.abc import AsyncIterator, Callable, Collection, Coroutine, Mapping
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Protocol,
    TypedDict,
    TypeVar,
    overload,
    runtime_checkable,
)

from nats.aio.client import Callback, Credentials, ErrorCallback, JWTCallback, SignatureCallback
from nats.aio.client import Client as NATSClient
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from .channel import Channel, PrivateChannel
    from .service import RPCService

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


T_co_proxy = TypeVar("T_co_proxy", covariant=True)

CloseHandler = Callable[[], Coroutine[Any, Any, None]]


@runtime_checkable
class ProxyWithClose(Protocol[T_co_proxy]):
    @property
    def proxy(self) -> T_co_proxy: ...
    async def close(self) -> None: ...


@runtime_checkable
class RPCClient(Protocol):
    service: RPCService

    @property
    def is_connected(self) -> bool: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def max_payload_size(self) -> int: ...
    async def connect(self) -> NATSClient: ...
    async def disconnect(self) -> None: ...
    def reconfigure(
        self,
        servers: list[str] | None = None,
        auth: RPCAuthOptions | None = None,
    ) -> None: ...
    async def publish(
        self, subject: str, data: Any, headers: dict[str, str] | None = None, reply: str | None = None
    ) -> None: ...
    async def subscribe(
        self,
        pattern: str,
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
        queue: str = "",
    ) -> Callable[[], Coroutine[Any, Any, None]]: ...
    async def request(
        self,
        subject: str,
        data: Any,
        timeout: int | None = None,
        headers: dict[str, str] | None = None,
        no_responder_retry: NoResponderRetryOptions | None = None,
    ) -> Any: ...
    async def on_request(
        self, pattern: str, handler: Callable[[Any], Any | Coroutine[Any, Any, Any]]
    ) -> Callable[[], Coroutine[Any, None, None]]: ...
    async def register_handler(
        self,
        namespace: str,
        handlers: object,
        isolated_connection: bool = False,
        without_decorators: bool = False,
        queue: str = "",
        *,
        concurrency: Mapping[str, int] | None = None,
        orphan_probe: OrphanProbeOptions | Literal[False] | None = None,
        namespace_subscription: bool = False,
        drop_expired: bool | Collection[str] = False,
    ) -> Callable[[], Coroutine[Any, Any, None]]: ...
    async def call_with_callback(
        self,
        subject: str,
        args: list[Any],
        callback: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]: ...
    def call_pull_iterator_with_callback(
        self,
        subject: str,
        callbacks: dict[str, Callable[..., Any]],
        oneway_methods: list[str],
        *args: Any,
    ) -> AsyncIterator[None]: ...
    async def channel(self, channel_id: str, isolated_connection: bool = False) -> Channel: ...
    async def private_channel(
        self, channel_id: str, target_client_id: str, isolated_connection: bool = False
    ) -> PrivateChannel: ...
    @overload
    def create_proxy(
        self,
        namespace: str,
        type_class: type[T],
        isolated_connection: Literal[False] | None = None,
        *,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
    ) -> T: ...
    @overload
    def create_proxy(
        self,
        namespace: str,
        type_class: type[T],
        isolated_connection: Literal[True],
        *,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
    ) -> ProxyWithClose[T]: ...
    @overload
    def create_proxy(
        self,
        namespace: str,
        type_class: None = None,
        isolated_connection: Literal[False] | None = None,
        *,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
    ) -> Any: ...
    @overload
    def create_proxy(
        self,
        namespace: str,
        type_class: None = None,
        isolated_connection: Literal[True] = True,
        *,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
    ) -> ProxyWithClose[Any]: ...
    def create_proxy(
        self,
        namespace: str,
        type_class: type[T] | None = None,
        isolated_connection: bool | None = False,
        *,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
    ) -> T | Any | ProxyWithClose[T] | ProxyWithClose[Any]: ...
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
        isolated_connection: bool | None = None,
        preferred_id: str | None = None,
        timeout: int | None = None,
    ) -> T | Any | ProxyWithClose[T] | ProxyWithClose[Any]: ...


class NoResponderRetryOptions(TypedDict):
    max_retries: NotRequired[int]
    delays: NotRequired[list[float]]  # seconds, the last one repeats


class OrphanProbeOptions(TypedDict):
    interval_ms: NotRequired[int]  # default and 0: 30000
    grace_ms: NotRequired[int]  # default and 0: 300000


class RPCAuthOptions(TypedDict):
    user: str
    password: str


class RPCClientOptions(TypedDict):
    servers: list[str]
    name: str
    conn_id: NotRequired[str]
    auth: NotRequired[RPCAuthOptions]
    timeout: NotRequired[int]  # ms
    reconnect: NotRequired[bool]
    max_reconnect_attempts: NotRequired[int]  # -1 is infinite, default -1, or 1 without reconnect
    reconnect_time_wait: NotRequired[int]  # ms, default 2000, at least 100
    connect_timeout: NotRequired[int]  # ms
    tls: NotRequired[dict[str, str] | ssl.SSLContext]  # dict keys cert, key and ca
    # bytes, only lowers the server limit, 8192 of it are kept for the envelope
    max_payload_size: NotRequired[int]
    # bytes, also caps all transfers in reassembly together, default and 0: 1 GiB
    max_transfer_size: NotRequired[int]
    wait_on_first_connect: NotRequired[bool]  # default True, False lets disconnect() abort a dial in flight
    disconnect_timeout: NotRequired[int]  # ms, default 2000
    drain_timeout_ms: NotRequired[int]  # default 250, 0 tears down at once and cancels running handlers
    no_responder_retry: NotRequired[NoResponderRetryOptions]
    orphan_probe: NotRequired[OrphanProbeOptions | Literal[False]]
    error_cb: NotRequired[ErrorCallback]  # without it transport errors are swallowed
    reconnected_cb: NotRequired[Callback]


class RPCMessage(TypedDict):
    id: str
    method: str
    params: Any
    error: NotRequired[RPCError | None]
    __discover: NotRequired[bool]
    __timing: NotRequired[bool]
    __ttl: NotRequired[int]  # ms, the caller's timeout, old responders ignore it


class RPCResponse(TypedDict):
    id: str
    result: NotRequired[Any | None]
    error: NotRequired[RPCError | None]
    __methods: NotRequired[list[str] | None]
    __timing: NotRequired[RPCTiming | None]


class RPCTiming(TypedDict):
    start: int
    end: int


class RPCError(TypedDict):
    code: str
    message: str
    data: NotRequired[Any | None]


class StreamMessage(TypedDict):
    id: str
    # ping is a producer's probe whether the consumer still listens
    type: Literal["data", "end", "error", "ping"]
    data: NotRequired[Any | None]
    error: NotRequired[RPCError | None]


class PullIteratorRequest(TypedDict):
    id: str
    type: Literal["next", "cancel"]


class PullIteratorResponse(TypedDict):
    id: str
    type: Literal["value", "done", "error"]
    value: NotRequired[Any | None]
    error: NotRequired[RPCError | None]


class ErrorCode(StrEnum):
    METHOD_NOT_FOUND = "METHOD_NOT_FOUND"
    INVALID_PARAMS = "INVALID_PARAMS"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    TIMEOUT = "TIMEOUT"
    CONNECTION_CLOSED = "CONNECTION_CLOSED"
    STREAM_ERROR = "STREAM_ERROR"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    NOT_FOUND = "NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"


class ChunkedTransferHeader(TypedDict):
    type: Literal["chunked"]
    transferId: str
    totalChunks: int
    totalSize: int
    chunkSize: int


class ChunkData(TypedDict):
    transferId: str
    index: int
    data: bytes


class ConnectionOptions(TypedDict):
    servers: str | list[str]
    error_cb: NotRequired[ErrorCallback]
    disconnected_cb: NotRequired[Callback]
    closed_cb: NotRequired[Callback]
    discovered_server_cb: NotRequired[Callback]
    reconnected_cb: NotRequired[Callback]
    name: NotRequired[str]
    pedantic: NotRequired[bool]
    verbose: NotRequired[bool]
    allow_reconnect: NotRequired[bool]
    connect_timeout: NotRequired[float]
    reconnect_time_wait: NotRequired[float]
    max_reconnect_attempts: NotRequired[int]
    ping_interval: NotRequired[int]
    max_outstanding_pings: NotRequired[int]
    dont_randomize: NotRequired[bool]
    flusher_queue_size: NotRequired[int]
    no_echo: NotRequired[bool]
    tls: NotRequired[ssl.SSLContext]
    tls_hostname: NotRequired[str]
    tls_handshake_first: NotRequired[bool]
    user: NotRequired[str | None]
    password: NotRequired[str | None]
    token: NotRequired[str]
    drain_timeout: NotRequired[int]
    signature_cb: NotRequired[SignatureCallback]
    user_jwt_cb: NotRequired[JWTCallback]
    user_credentials: NotRequired[Credentials]
    nkeys_seed: NotRequired[str]
    nkeys_seed_str: NotRequired[str]
    inbox_prefix: NotRequired[str | bytes]
    pending_size: NotRequired[int]
    flush_timeout: NotRequired[float]


class CallbackParams(TypedDict):
    __callback: bool
    __callbackSubject: str
    args: list[Any]


class CallbackMessage(TypedDict):
    id: str
    type: Literal["data", "error", "ping"]
    data: NotRequired[Any | None]
    error: NotRequired[RPCError | None]


class PullCallbackParams(TypedDict):
    __pullCallback: bool
    __iteratorId: str
    __callbackSubject: str
    __callbackMethods: list[str]
    __onewayMethods: list[str]
    args: list[Any]


class CallbackInvocation(TypedDict):
    method: str
    args: list[Any]
