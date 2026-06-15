"""Type definitions for camera.ui RPC library."""

from __future__ import annotations

import ssl
from collections.abc import AsyncIterator, Callable, Coroutine
from enum import Enum
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

# Type variables
T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


# Create a covariant type variable for ProxyWithClose
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
    async def publish(self, subject: str, data: Any) -> None: ...
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
    """Retry configuration for 503/no-responder errors."""

    max_retries: NotRequired[int]
    """Maximum number of retry attempts (default: 3)."""

    delays: NotRequired[list[float]]
    """Delay in seconds before each retry attempt (default: [0.5, 1.0, 2.0])."""


class RPCAuthOptions(TypedDict):
    """Authentication options for RPC client."""

    user: str
    """Username for authentication"""

    password: str
    """Password for authentication"""


class RPCClientOptions(TypedDict):
    """Configuration options for RPC client."""

    servers: list[str]
    """NATS server URLs"""

    name: str
    """Client name for identification"""

    auth: NotRequired[RPCAuthOptions]
    """Authentication credentials with 'user' and 'pass' keys"""

    timeout: NotRequired[int]
    """Default RPC call timeout in milliseconds"""

    reconnect: NotRequired[bool]
    """Enable automatic reconnection"""

    max_reconnect_attempts: NotRequired[int]
    """Maximum reconnection attempts (-1 for infinite)"""

    reconnect_time_wait: NotRequired[int]
    """Delay between reconnection attempts in milliseconds"""

    tls: NotRequired[dict[str, str]]
    """TLS configuration with 'cert', 'key', and 'ca' keys"""

    max_payload_size: NotRequired[int]
    """Maximum payload size in bytes (default: auto-detect from NATS server)"""

    wait_on_first_connect: NotRequired[bool]
    """Block connect() until the first connection succeeds (default: True).
    Set to False for clients where disconnect() must be able to abort an
    in-flight connection attempt immediately (e.g. browser URL switchover)."""

    disconnect_timeout: NotRequired[int]
    """Maximum time in ms to wait for the NATS connection to fully close
    during disconnect(). Default: 2000"""

    no_responder_retry: NotRequired[NoResponderRetryOptions]
    """Retry configuration for 503/no-responder errors."""


class RPCMessage(TypedDict):
    """RPC request message format."""

    id: str
    """Unique request ID"""

    method: str
    """Method name to call"""

    params: Any
    """Method parameters"""

    error: NotRequired[RPCError | None]
    """Optional error (unused in requests)"""


class RPCResponse(TypedDict):
    """RPC response message format."""

    id: str
    """Request ID this response belongs to"""

    result: NotRequired[Any | None]
    """Result data (if successful)"""

    error: NotRequired[RPCError | None]
    """Error information (if failed)"""

    __methods: NotRequired[list[str] | None]
    """Available methods on namespace (internal metadata for proxy)"""


class RPCError(TypedDict):
    """RPC error format."""

    code: str
    """Error code (see ErrorCode)"""

    message: str
    """Human-readable error message"""

    data: NotRequired[Any | None]
    """Additional error data"""


class StreamMessage(TypedDict):
    """Message format for streaming data."""

    id: str
    """Stream ID (same as request ID)"""

    type: Literal["data", "end", "error"]
    """Message type"""

    data: NotRequired[Any | None]
    """Data payload (for 'data' type)"""

    error: NotRequired[RPCError | None]
    """Error information (for 'error' type)"""


class PullIteratorRequest(TypedDict):
    """Request message for pull-based iterators."""

    id: str
    """Iterator ID"""

    type: Literal["next", "cancel"]
    """Request type"""


class PullIteratorResponse(TypedDict):
    """Response message for pull-based iterators."""

    id: str
    """Iterator ID"""

    type: Literal["value", "done", "error"]
    """Response type"""

    value: NotRequired[Any | None]
    """Value (for 'value' type)"""

    error: NotRequired[RPCError | None]
    """Error information (for 'error' type)"""


class ErrorCode(str, Enum):
    """Standard error codes."""

    METHOD_NOT_FOUND = "METHOD_NOT_FOUND"
    INVALID_PARAMS = "INVALID_PARAMS"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    TIMEOUT = "TIMEOUT"
    CONNECTION_CLOSED = "CONNECTION_CLOSED"
    STREAM_ERROR = "STREAM_ERROR"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    NOT_FOUND = "NOT_FOUND"


class ChunkedTransferHeader(TypedDict):
    """Chunked transfer header."""

    type: Literal["chunked"]
    transferId: str
    totalChunks: int
    totalSize: int
    chunkSize: int


class ChunkData(TypedDict):
    """Individual chunk message."""

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
    connect_timeout: NotRequired[int]
    reconnect_time_wait: NotRequired[int]
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
    """Parameters for callback subscription request."""

    __callback: bool
    __callbackSubject: str
    args: list[Any]


class CallbackMessage(TypedDict):
    """Message format for callback data pushed to subscribers."""

    id: str
    type: Literal["data", "error"]
    data: NotRequired[Any | None]
    error: NotRequired[RPCError | None]


class PullCallbackParams(TypedDict):
    """Parameters for a pull-iterator-with-callbacks request.

    Combines pull-iterator flow control with oneway callbacks for low-latency
    item-level data delivery. See packages/rpc/PULL_CALLBACK_PROTOCOL.md.
    """

    __pullCallback: bool
    __iteratorId: str
    __callbackSubject: str
    __callbackMethods: list[str]
    __onewayMethods: list[str]
    args: list[Any]


class CallbackInvocation(TypedDict):
    """A single oneway callback invocation published from server to client on
    the callback subject.
    """

    method: str
    args: list[Any]
