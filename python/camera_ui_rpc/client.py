import asyncio
import contextlib
import inspect
import math
import ssl
import sys
import time
import traceback
from collections import deque
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Iterable,
    Mapping,
    Sequence,
)
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Generic, Literal, TypeVar, cast, overload

import nats.aio.client as nats_aio_client
from nats import errors
from nats.aio.client import NO_RESPONDERS_STATUS, ErrorCallback
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    Subscription,
)
from nats.js.api import Header
from nats.protocol import command as nats_command

from .channel import Channel, PrivateChannel
from .chunking import (
    CHUNK_RESERVE,
    MAX_CONCURRENT_TRANSFERS,
    MIN_PAYLOAD_LIMIT,
    ChunkingManager,
    TransferBudget,
    create_chunks,
    parse_chunk_index,
)
from .codec import decode, decode_message, encode, encode_message
from .decorators import extract_nested_methods_with_decorators, extract_nested_methods_without_decorators
from .errors import (
    LocalNoRespondersError,
    RPCException,
    RPCTimeoutError,
    create_error,
    create_error_from_dict,
    is_local_no_responders,
    reported_error,
)
from .executor import get_executor
from .handler import (
    format_error_dict,
    handle_callback_request,
    handle_normal_rpc,
    handle_pull_callback_request,
    handle_pull_iterator_request,
    refuse_stream,
    run_in_background,
    set_up_session,
    start_stream_request,
)
from .patches import ARRIVAL_STAMPS, ArrivalMsg, NatsConnection, connect
from .service import RPCService
from .types import (
    CallbackParams,
    ChunkedTransferHeader,
    ConnectionOptions,
    ErrorCode,
    NoResponderRetryOptions,
    OrphanProbeOptions,
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
from .utils import (
    create_proxy,
    create_service_proxy,
    generate_id,
    generate_reply_prefix,
    is_async_function,
)

T = TypeVar("T")

_Teardown = Callable[[], Coroutine[Any, Any, None]]

NATS_READ_BUFFER_SIZE = 256 * 1024  # nats-py's 32 KB default costs a frame-sized message dozens of reads

# nats-py reads the constant on every connect and read, a bigger value set elsewhere is kept
if (
    isinstance(getattr(nats_aio_client, "DEFAULT_BUFFER_SIZE", None), int)
    and nats_aio_client.DEFAULT_BUFFER_SIZE < NATS_READ_BUFFER_SIZE
):
    nats_aio_client.DEFAULT_BUFFER_SIZE = NATS_READ_BUFFER_SIZE


def _inline_delivery_supported() -> bool:
    try:
        has_param = "future" in inspect.signature(NATSClient.subscribe).parameters
        names = set(NATSClient._process_msg.__code__.co_names)  # pyright: ignore[reportPrivateUsage]
    except (AttributeError, TypeError, ValueError):
        return False
    return has_param and {"_future", "cancelled", "set_result"} <= names


INLINE_REPLY_DELIVERY = _inline_delivery_supported()


class _InlineSink:
    __slots__ = ("_handler", "_pattern")

    def __init__(self, handler: Callable[[Msg], None], pattern: str) -> None:
        self._handler = handler
        self._pattern = pattern

    def cancelled(self) -> bool:
        return False

    def set_result(self, msg: Msg) -> None:
        # an exception escaping here ends nats-py's read loop and leaves the connection silently dead
        try:
            self._handler(msg)
        except Exception as e:
            print(f"Error processing message for {self._pattern}:", e)
            print(traceback.format_exc())


async def _idle_callback(_msg: Msg) -> None:
    pass


METHODS_METHOD = "__methods"
DEFAULT_DRAIN_TIMEOUT_MS = 250
EXPIRY_SLACK_MS = 5
# when the caller sets neither delays nor max_retries, about 3.5 s in all
DEFAULT_RETRY_DELAYS = (0.025, 0.05, 0.1, 0.325, 1.0, 2.0)
# for a caller that sets only one of delays and max_retries
LEGACY_RETRY_DELAYS = (0.5, 1.0, 2.0)
LEGACY_MAX_RETRIES = 3
METHOD_PROBE_WINDOW = 0.5


def _queued_ping_supported() -> bool:
    try:
        nc = NATSClient()
        return (
            inspect.iscoroutinefunction(NATSClient._send_command)  # pyright: ignore[reportPrivateUsage]
            and inspect.iscoroutinefunction(NATSClient._flush_pending)  # pyright: ignore[reportPrivateUsage]
            and isinstance(nc._pongs, list)  # pyright: ignore[reportPrivateUsage]
            and {"_pongs", "pop", "set_result"} <= set(NATSClient._process_pong.__code__.co_names)  # pyright: ignore[reportPrivateUsage]
            and getattr(nats_aio_client, "PING_PROTO", None) == b"PING\r\n"
        )
    except Exception:
        return False


QUEUED_PING = _queued_ping_supported()


def _handler_drain_supported() -> bool:
    try:
        nc = NATSClient()
        sub = Subscription(nc, 7, "rpc.x", cb=_idle_callback)
        return (
            QUEUED_PING
            and nats_command.unsub_cmd(7, 0) == b"UNSUB 7 \r\n"
            and sub._id == 7  # pyright: ignore[reportPrivateUsage]
            and sub._wait_for_msgs_task is None  # pyright: ignore[reportPrivateUsage]
            and isinstance(sub._pending_queue, asyncio.Queue)  # pyright: ignore[reportPrivateUsage]
            and {"_pending_queue", "_cb", "task_done"} <= set(Subscription._wait_for_msgs.__code__.co_names)  # pyright: ignore[reportPrivateUsage]
        )
    except Exception:
        return False


HANDLER_DRAIN = _handler_drain_supported()
SUBSCRIBE_CONFIRM_TIMEOUT = 1  # seconds, a half-dead link must not stall a registration


async def _queue_ping(nc: NATSClient) -> asyncio.Future[bool]:
    pong: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    nc._pongs.append(pong)  # pyright: ignore[reportPrivateUsage]
    # through the pending buffer: nats-py's flush() writes its PING ahead of queued commands
    await nc._send_command(nats_aio_client.PING_PROTO)  # pyright: ignore[reportPrivateUsage]
    await nc._flush_pending()  # pyright: ignore[reportPrivateUsage]
    return pong


class _ChunkScope:
    __slots__ = ("key", "manager", "methods", "refs")

    def __init__(self, key: str) -> None:
        self.key = key
        self.manager: ChunkingManager | None = None
        self.methods: dict[str, ChunkingManager] | None = None
        self.refs = 0

    def close(self) -> None:
        if self.manager is not None:
            self.manager.close()
            self.manager = None
        methods, self.methods = self.methods, None
        for manager in (methods or {}).values():
            manager.close()


class _Lane:
    __slots__ = ("_dispatch", "_items", "pending_bytes", "task")

    def __init__(self, dispatch: Callable[[Any], Coroutine[Any, Any, None]]) -> None:
        self._dispatch = dispatch
        self._items: deque[tuple[Any, int]] = deque()
        self.pending_bytes = 0
        self.task: asyncio.Task[None] | None = None

    @property
    def pending_msgs(self) -> int:
        return len(self._items)

    def push(self, item: Any, size: int) -> None:
        self._items.append((item, size))
        self.pending_bytes += size
        if self.task is None:
            self.task = asyncio.get_running_loop().create_task(self._run())

    def take(self) -> list[Any]:
        items = [item for item, _ in self._items]
        self._items.clear()
        self.pending_bytes = 0
        return items

    def stop(self) -> asyncio.Task[None] | None:
        self.take()
        task = self.task
        if task is None or task is asyncio.current_task():
            return None
        task.cancel()
        return task

    async def dispatch(self, item: Any) -> None:
        await self._dispatch(item)

    async def _run(self) -> None:
        try:
            while self._items:
                item, size = self._items.popleft()
                self.pending_bytes -= size
                await self._dispatch(item)
        finally:
            self.task = None


class _Registration:
    __slots__ = ("calls", "entries", "expired", "lanes")

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self.calls: set[asyncio.Task[None]] = set()
        self.lanes: list[_Lane] = []
        self.expired = False


def _stop_lanes(registrations: Iterable[_Registration]) -> list[asyncio.Task[None]]:
    return [task for r in registrations for lane in r.lanes if (task := lane.stop()) is not None]


DEFAULT_ORPHAN_PROBE_INTERVAL_MS = 30_000
DEFAULT_ORPHAN_PROBE_GRACE_MS = 300_000


class _OrphanProbe:
    __slots__ = (
        "_client",
        "_grace",
        "_interval",
        "_last",
        "_missing_since",
        "_on_gone",
        "_ping",
        "_stopped",
        "_timer",
        "_token",
        "due",
        "sent",
    )

    def __init__(self, client: "RPCClient", interval: float, grace: float) -> None:
        self._client = client
        self._interval = interval
        self._grace = grace
        self._last = time.monotonic()
        self.sent = False
        self.due = False
        self._token: str | None = None
        self._missing_since: float | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._ping: Callable[[str], Coroutine[Any, Any, None]] | None = None
        self._on_gone: Callable[[], None] | None = None
        self._stopped = False

    def start(self, ping: Callable[[str], Coroutine[Any, Any, None]], on_gone: Callable[[], None]) -> None:
        if self._stopped:
            return
        self._ping = ping
        self._on_gone = on_gone
        self._arm(self._interval)

    def reply(self) -> str:
        if not self.due or self._stopped or not self._client.is_connected:
            return ""
        return self._next_probe(time.monotonic())

    def touch(self) -> None:
        self._last = time.monotonic()
        self._missing_since = None

    def stop(self) -> None:
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._token is not None:
            self._client.status_handlers.pop(self._token, None)
            self._token = None

    def _next_probe(self, now: float) -> str:
        self.due = False
        token = self._token
        # still registered: no status came back, the consumer received the last probe
        if token is not None and self._client.status_handlers.pop(token, None) is not None:
            self._missing_since = None
        self._last = now
        token = self._token = generate_id(self._client._reply_prefix)
        self._client.status_handlers[token] = self._on_status
        return f"rpc.reply.{token}"

    def _on_status(self, _error: Exception) -> None:
        if self._stopped:
            return
        now = time.monotonic()
        if self._missing_since is None:
            self._missing_since = now
        if now - self._missing_since >= self._grace:
            on_gone = self._on_gone
            self.stop()
            if on_gone is not None:
                on_gone()

    def _arm(self, delay: float) -> None:
        self._timer = asyncio.get_running_loop().call_later(delay, self._tick)

    def _tick(self) -> None:
        self._timer = None
        if self._stopped:
            return
        now = time.monotonic()
        since = now - self._last
        if since < self._interval:
            self._arm(self._interval - since)
            return
        sent, self.sent = self.sent, False
        if sent:
            # the session publishes, its next publish carries the probe
            self.due = True
        elif self._client.is_connected and self._ping is not None:
            run_in_background(self._ping(self._next_probe(now)))
        self._arm(self._interval)


def _expired(message: Any, arrived: float) -> bool:
    ttl = message.get("__ttl") if isinstance(message, dict) else None
    if not isinstance(ttl, int | float) or isinstance(ttl, bool) or ttl <= 0:
        return False
    return (time.monotonic() - arrived) * 1000 > ttl + EXPIRY_SLACK_MS


def _namespace_subscribable(namespace: str) -> bool:
    first = namespace.split(".", 1)[0]
    return first not in ("reply", "cb")


def _reply_error(error: Any) -> RPCException:
    # runs after the call's timer is cancelled, so it must not raise
    if isinstance(error, dict):
        return create_error_from_dict(cast(dict[str, Any], error))
    return create_error(ErrorCode.INTERNAL_ERROR, f"Malformed error in reply: {error!r}")


def _forbidden(subject: str) -> RPCException:
    return create_error(ErrorCode.FORBIDDEN, f'Permission denied for "{subject}"', {"subject": subject})


def _refused_publish_subject(error: Exception) -> str | None:
    marker = 'permissions violation for publish to "'
    text = str(error)
    start = text.lower().find(marker)
    if start < 0:
        return None
    subject, quote, _ = text[start + len(marker) :].partition('"')
    return subject if quote and subject else None


def _forget_session(cleanups: dict[str, Any], ids: set[str], session_id: str) -> None:
    cleanups.pop(session_id, None)
    ids.discard(session_id)


async def _cancel_tasks(tasks: Iterable[asyncio.Task[None]]) -> None:
    # a handler that tears its own client down must not wait for itself
    current = asyncio.current_task()
    pending = [task for task in tasks if task is not current]
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def create_rpc_client(options: RPCClientOptions) -> RPCClientProtocol:
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
            parent = cast(RPCClient | None, getattr(self.proxy, "_parent_client", None))
            if parent and isolated_client in parent.isolated_clients:
                parent.isolated_clients.remove(isolated_client)


class RPCClient(RPCClientProtocol):
    def __init__(self, options: RPCClientOptions):
        payload_cap = options.get("max_payload_size")
        # bool is an int too, a float would make chunk sizes floats
        if payload_cap is not None and type(payload_cap) is not int:
            raise ValueError(f"max_payload_size {payload_cap!r} is not an integer")
        if payload_cap and payload_cap < MIN_PAYLOAD_LIMIT:
            raise ValueError(
                f"max_payload_size {payload_cap} is too small, chunking needs at least {MIN_PAYLOAD_LIMIT} bytes"
            )
        self.options: RPCClientOptions = options
        self.service: RPCService = RPCService(cast(Any, self))

        self.nc: NATSClient | None = None
        self._subscription_seq: int = 0
        self._subscription_entries: dict[int, dict[str, Any]] = {}
        self._chunk_budget: TransferBudget = TransferBudget(options.get("max_transfer_size"))
        self._chunk_scopes: dict[str, _ChunkScope] = {}
        self._endpoint_chunking: set[ChunkingManager] = set()
        # public attribute kept for compatibility, subscriptions reassemble in their own scope
        self.chunking_manager: ChunkingManager = self._new_chunking_manager()
        # one transfer per pending call, so the calls bound it instead of a transfer cap
        self._mux_chunking: ChunkingManager = self._new_chunking_manager(math.inf)
        self._max_payload_size: int = 1024 * 1024
        self._unchunkable_limit: int | None = None
        self._connection_task: asyncio.Task[NATSClient] | None = None

        # conn_id when set: a firewall may allowlist exactly rpc.reply.<conn_id>.>
        self._reply_prefix: str = options.get("conn_id") or generate_reply_prefix()

        self._mux_entry: dict[str, Any] | None = None

        self.status_handlers: dict[str, Callable[[Exception], None]] = {}

        self.pending_requests: dict[str, dict[str, Any]] = {}
        self.stream_handlers: dict[str, dict[str, Any]] = {}
        self.isolated_clients: list[RPCClient] = []
        self.pull_iterator_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self.callback_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self.stream_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self._pull_iterator_settles: set[Callable[[], None]] = set()
        self._concurrent_calls: set[asyncio.Task[None]] = set()
        self._registrations: set[_Registration] = set()
        self._drained_subs: set[Subscription] = set()

        self._closed = False
        self._suspending = False
        self._connection_lost = False
        self._slow_consumer_log: dict[str, tuple[float, int]] = {}
        self._arrival_stamps = False
        self._error_cb: ErrorCallback = self._make_error_cb()

        self.io_pool: ThreadPoolExecutor = get_executor()

    @property
    def is_connected(self) -> bool:
        return self.nc is not None and self.nc.is_connected

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def max_payload_size(self) -> int:
        return self._max_payload_size

    def create_isolated_client(self, options: RPCClientOptions) -> "RPCClient":
        return RPCClient(options)

    async def connect(self) -> NATSClient:
        if self.nc is not None and not self.nc.is_closed:
            return self.nc

        if not self._connection_task:
            self._connection_task = asyncio.create_task(self._connect())

        try:
            self.nc = await self._connection_task
        finally:
            self._connection_task = None

        return self.nc

    def _make_error_cb(self) -> ErrorCallback:
        user_cb: ErrorCallback | None = self.options.get("error_cb")

        async def _error_cb(error: Exception) -> None:
            if self._closed:
                return
            if (subject := _refused_publish_subject(error)) is not None:
                self._fail_refused_publish(subject)
            if user_cb is not None:
                await user_cb(error)
            elif isinstance(error, errors.SlowConsumerError):
                self._log_slow_consumer(error.subject)

        return _error_cb

    def _fail_refused_publish(self, subject: str) -> None:
        refused = subject.lower()
        for request_id, pending in list(self.pending_requests.items()):
            target = pending.get("subject")
            if not isinstance(target, str) or target.lower() != refused:
                continue
            del self.pending_requests[request_id]
            if timeout_handle := pending.get("timeout"):
                timeout_handle.cancel()
            if transfer_key := pending.get("transfer_key"):
                self._mux_chunking.discard(transfer_key)
            future: asyncio.Future[Any] | None = pending.get("future")
            if future is not None and not future.done():
                future.set_exception(_forbidden(target))
        for handler in list(self.stream_handlers.values()):
            target = handler.get("subject")
            if isinstance(target, str) and target.lower() == refused:
                handler["error"](_forbidden(target))

    def _log_slow_consumer(self, subject: str) -> None:
        # nats-py reports every dropped message, one line per subject and second is enough
        now = time.monotonic()
        logged, dropped = self._slow_consumer_log.get(subject, (-math.inf, 0))
        dropped += 1
        if now - logged < 1:
            self._slow_consumer_log[subject] = (logged, dropped)
            return
        self._slow_consumer_log[subject] = (now, 0)
        print(f"rpc: slow consumer on {subject}, dropped {dropped} message(s)", file=sys.stderr)

    def _report_slow_consumer(self, subject: str) -> None:
        error = errors.SlowConsumerError(subject=subject, reply="", sid=0, sub=cast(Subscription, None))
        run_in_background(self._error_cb(error))

    async def _connect(self) -> NATSClient:
        reconnect = self.options.get("reconnect", True)
        connect_opts: ConnectionOptions = {
            "servers": self.options["servers"],
            "name": self.options["name"],
            "allow_reconnect": reconnect,
            # nats-py dials the first connect forever with -1 and with 0, 1 gives up after the second dial
            "max_reconnect_attempts": self.options.get("max_reconnect_attempts", -1 if reconnect else 1),
            # float seconds, the 100 ms floor keeps 0 from busy-looping since nats-py has no jitter
            "reconnect_time_wait": max(self.options.get("reconnect_time_wait", 2000), 100) / 1000,
            # nats-py's 2 s default can burn entirely on a dead ::1 before the ipv4 fallback
            "connect_timeout": max(self.options.get("connect_timeout", 10000), 1) / 1000,
            "pending_size": 6 * 1024 * 1024,
            "error_cb": self._error_cb,
        }

        if reconnected_cb := self.options.get("reconnected_cb"):
            connect_opts["reconnected_cb"] = reconnected_cb

        # bound to this connection, so the late close of a replaced one changes nothing
        opened: list[NATSClient] = []

        async def closed_cb() -> None:
            if opened:
                await self._on_connection_lost(opened[0])

        connect_opts["closed_cb"] = closed_cb

        if auth := self.options.get("auth"):
            connect_opts["user"] = auth.get("user")
            connect_opts["password"] = auth.get("password")

        if tls := self.options.get("tls"):
            if isinstance(tls, ssl.SSLContext):
                connect_opts["tls"] = tls
            else:
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                if tls.get("ca"):
                    context.load_verify_locations(tls["ca"])
                if tls.get("cert") and tls.get("key"):
                    context.load_cert_chain(tls["cert"], tls["key"])
                connect_opts["tls"] = context

        self.nc = await connect(**connect_opts)
        opened.append(self.nc)
        if self._arrival_stamps and ARRIVAL_STAMPS:
            self.nc.msg_class = ArrivalMsg

        # a disconnect()ed client revives here, otherwise auto-connect and the no-responder retry stay off
        self._closed = False

        self.service.init(self.nc)

        # the option only lowers the server's limit, for a leaf link with a lower one
        limits = [
            limit
            for limit in (self.nc.max_payload, self.options.get("max_payload_size"))
            if limit is not None and limit > 0
        ]
        limit = min(limits) if limits else 1024 * 1024
        # below the floor no chunk fits next to the reserve, a message goes out whole or fails
        self._unchunkable_limit = limit if limit < MIN_PAYLOAD_LIMIT else None
        self._max_payload_size = limit if limit < MIN_PAYLOAD_LIMIT else limit - CHUNK_RESERVE

        # a plain entry, so the restore loop below subscribes it on first connect and after every suspend
        self._register_mux_entry()

        # entries keep their identity, so unsubscribe closures held by callers stay valid
        restored = False
        for entry in list(self._subscription_entries.values()):
            sub = entry.get("sub")
            if sub is None or getattr(sub, "_closed", False):
                await self._nats_subscribe(entry, yield_after=False)
                restored = restored or entry is not self._mux_entry
        if restored:
            await self._confirm_subscriptions()

        if self._connection_lost:
            self._connection_lost = False
            # nats-py reports only reconnects of one connection, not its replacement
            if reconnected_cb:
                run_in_background(reconnected_cb())

        # isolated clients come back with their subscriptions after a suspend(), a closed one stays closed
        if children := [child for child in self.isolated_clients if not child.is_closed]:
            await asyncio.gather(*(child.connect() for child in children), return_exceptions=True)

        return self.nc

    async def disconnect(self) -> None:
        registrations = list(self._registrations)
        # before the client counts as closed and its pending calls fail: running handlers may still call out
        await self._drain_handlers(keep=False)
        self._closed = True
        self._connection_lost = False

        # service-path calls still hold a per-call reply entry, the cleanup hook drops it
        cleanups = self._end_session()
        sessions = self._take_handler_sessions()
        await self._run_cleanups(cleanups)
        await self._end_handler_sessions(sessions)

        for entry in list(self._subscription_entries.values()):
            if sub := entry.get("sub"):
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            entry["sub"] = None
        self._subscription_entries.clear()

        # after the unsubscribe: a slot freed by a cancelled call would start the next queued call
        await asyncio.gather(*_stop_lanes(registrations), return_exceptions=True)
        await _cancel_tasks(self._concurrent_calls)
        # entries were dropped, a later connect() must register the mux again
        self._mux_entry = None

        self._reset_chunking()
        self._chunk_scopes.clear()

        if self.isolated_clients:
            await asyncio.gather(
                *[client.disconnect() for client in self.isolated_clients],
                return_exceptions=True,
            )
            self.isolated_clients.clear()

        if self.nc:
            timeout = self.options.get("disconnect_timeout", 2000) / 1000
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(self.nc.close(), timeout=timeout)
            self.nc = None

    async def suspend(self) -> None:
        await self._suspend(drain=True)

    async def _suspend(self, drain: bool) -> None:
        # keeps on_finish from dropping the entries while the transport goes down, connect() restores them
        self._suspending = True
        self._connection_lost = False
        try:
            if drain:
                await self._drain_handlers(keep=True)
            await self._do_suspend()
        finally:
            self._suspending = False
            # connect() restores the subscriptions of these registrations
            for registration in self._registrations:
                registration.expired = False

    async def _drain_handlers(self, keep: bool) -> None:
        registrations = list(self._registrations)
        if not keep:
            self._registrations.clear()
        if not self.isolated_clients:
            await self._drain_registrations(registrations)
            return
        await asyncio.gather(
            self._drain_registrations(registrations),
            *(client._drain_handlers(keep=keep) for client in self.isolated_clients),
            return_exceptions=True,
        )

    async def _drain_registrations(self, registrations: Iterable[_Registration]) -> None:
        timeout_ms = self.options.get("drain_timeout_ms", DEFAULT_DRAIN_TIMEOUT_MS)
        nc = self.nc
        if timeout_ms <= 0 or not HANDLER_DRAIN or not isinstance(nc, NATSClient) or not nc.is_connected:
            return
        registrations = list(registrations)
        drained = [
            (entry, sub)
            for registration in registrations
            for entry in registration.entries
            if isinstance(sub := entry.get("sub"), Subscription) and not sub._closed  # pyright: ignore[reportPrivateUsage]
        ]
        if not drained and not any(
            registration.calls or registration.lanes for registration in registrations
        ):
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_ms / 1000
        # a handler that tears down its own registration must not wait for itself
        current = asyncio.current_task()
        queues: list[asyncio.Queue[Msg]] = []
        try:
            for _, sub in drained:
                self._drained_subs.add(sub)
                await nc._send_command(nats_command.unsub_cmd(sub._id, 0))  # pyright: ignore[reportPrivateUsage]
                if sub._wait_for_msgs_task is not current:  # pyright: ignore[reportPrivateUsage]
                    queues.append(sub._pending_queue)  # pyright: ignore[reportPrivateUsage]

            # the PONG trails every call the server routed here before the UNSUBs
            pong = await _queue_ping(nc)
            # no wait_for: nats-py sets a PONG future's result without checking for a cancel
            await asyncio.wait((pong,), timeout=max(0.0, deadline - loop.time()))

            async def settle() -> None:
                for queue in queues:
                    await queue.join()
                for registration in registrations:
                    while running := [
                        t
                        for t in (*registration.calls, *(lane.task for lane in registration.lanes))
                        if t is not None and t is not current and not t.done()
                    ]:
                        await asyncio.wait(running)

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(settle(), max(0.0, deadline - loop.time()))

            for registration in registrations:
                registration.expired = True
            # a call task created just now starts, so its cancel reaches the handler
            await asyncio.sleep(0)
            stopped: list[asyncio.Task[Any]] = []
            for _, sub in drained:
                task = cast("asyncio.Task[None] | None", sub._wait_for_msgs_task)  # pyright: ignore[reportPrivateUsage]
                if task is not None and task is not current and not task.done():
                    task.cancel()
                    stopped.append(task)
            for registration in registrations:
                for task in (*registration.calls, *(lane.task for lane in registration.lanes)):
                    if task is not None and task is not current and not task.done():
                        task.cancel()
                        stopped.append(task)
            await asyncio.gather(*stopped, return_exceptions=True)

            # requests that never started get CONNECTION_CLOSED through their subscription's own dispatch
            for entry, sub in drained:
                dispatch = entry.get("dispatch")
                queue = sub._pending_queue  # pyright: ignore[reportPrivateUsage]
                while dispatch is not None and not queue.empty():
                    msg = queue.get_nowait()
                    try:
                        await dispatch(msg)
                    finally:
                        queue.task_done()
            for registration in registrations:
                for lane in registration.lanes:
                    for item in lane.take():
                        await lane.dispatch(item)
        finally:
            self._drained_subs.difference_update(sub for _, sub in drained)

    async def _do_suspend(self) -> None:
        # a service-path call's reply entry would come back dead on the next connect()
        cleanups = self._end_session()
        sessions = self._take_handler_sessions()
        await self._run_cleanups(cleanups)
        await self._end_handler_sessions(sessions)

        # keep the entries, connect() restores them on the fresh transport
        for entry in list(self._subscription_entries.values()):
            if sub := entry.get("sub"):
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            entry["sub"] = None

        # after the unsubscribe: a slot freed by a cancelled call would start the next queued call
        await asyncio.gather(*_stop_lanes(self._registrations), return_exceptions=True)
        await _cancel_tasks(self._concurrent_calls)

        self._reset_chunking()

        # drained above, connect() brings them back with their subscriptions
        if self.isolated_clients:
            await asyncio.gather(
                *[client._suspend(drain=False) for client in self.isolated_clients],
                return_exceptions=True,
            )

        if self.nc:
            timeout = self.options.get("disconnect_timeout", 2000) / 1000
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(self.nc.close(), timeout=timeout)
            self.nc = None

    async def _on_connection_lost(self, nc: NATSClient) -> None:
        if nc is not self.nc or self._suspending or self._closed:
            return
        self.nc = None
        self._connection_lost = True
        for entry in self._subscription_entries.values():
            entry["sub"] = None
        self._reset_chunking()
        cleanups = self._end_session()
        sessions = self._take_handler_sessions()
        # the next call may dial a new connection meanwhile, its calls and sessions stay
        calls = set(self._concurrent_calls)
        lanes = _stop_lanes(list(self._registrations))

        await self._run_cleanups(cleanups)
        await self._end_handler_sessions(sessions)
        await asyncio.gather(*lanes, return_exceptions=True)
        await _cancel_tasks(calls)

    def _reset_chunking(self) -> None:
        # the managers are reusable after close(), so the budget lives as long as the client
        for scope in self._chunk_scopes.values():
            scope.close()
        for manager in self._endpoint_chunking:
            manager.close()
        self._endpoint_chunking.clear()
        self.chunking_manager.close()
        self._mux_chunking.close()

    def _orphan_probe_settings(
        self, override: OrphanProbeOptions | Literal[False] | None = None
    ) -> tuple[float, float] | None:
        src = override if override is not None else self.options.get("orphan_probe", OrphanProbeOptions())
        if src is False:
            return None
        interval = src.get("interval_ms")
        grace = src.get("grace_ms")
        if interval is None or interval <= 0:
            interval = DEFAULT_ORPHAN_PROBE_INTERVAL_MS
        if grace is None or grace <= 0:
            grace = DEFAULT_ORPHAN_PROBE_GRACE_MS
        return interval / 1000, grace / 1000

    def _stamp_arrivals(self) -> None:
        self._arrival_stamps = True
        if ARRIVAL_STAMPS and isinstance(self.nc, NATSClient):
            self.nc.msg_class = ArrivalMsg

    def _new_orphan_probe(self, settings: tuple[float, float] | None) -> _OrphanProbe | None:
        return None if settings is None else _OrphanProbe(self, *settings)

    def _new_chunking_manager(self, max_transfers: float = MAX_CONCURRENT_TRANSFERS) -> ChunkingManager:
        return ChunkingManager(self.options.get("max_transfer_size"), max_transfers, self._chunk_budget)

    def _open_endpoint_chunking(self) -> ChunkingManager:
        manager = self._new_chunking_manager()
        self._endpoint_chunking.add(manager)
        return manager

    def _close_endpoint_chunking(self, manager: ChunkingManager) -> None:
        self._endpoint_chunking.discard(manager)
        manager.close()

    def _join_chunk_scope(self, entry: dict[str, Any]) -> _ChunkScope:
        # overlapping subscriptions each see every transfer, the server spreads one across queue members
        queue = entry.get("queue") or ""
        key = f"q:{entry['pattern']}\x00{queue}" if queue else f"s{entry['key']}"
        scope = self._chunk_scopes.get(key)
        if scope is None:
            scope = self._chunk_scopes[key] = _ChunkScope(key)
        scope.refs += 1
        entry["chunks"] = scope
        return scope

    def _leave_chunk_scope(self, entry: dict[str, Any]) -> None:
        scope: _ChunkScope | None = entry.pop("chunks", None)
        if scope is None:
            return
        scope.refs -= 1
        if scope.refs > 0:
            return
        if self._chunk_scopes.get(scope.key) is scope:
            del self._chunk_scopes[scope.key]
        scope.close()

    def _end_session(self) -> list[Callable[[], Coroutine[Any, Any, None]]]:
        cleanups: list[Callable[[], Coroutine[Any, Any, None]]] = []
        for pending in self.pending_requests.values():
            if timeout_handle := pending.get("timeout"):
                timeout_handle.cancel()
            if cleanup := pending.get("cleanup"):
                cleanups.append(cleanup)
            future = pending.get("future")
            if future is not None and not future.done():
                future.set_exception(create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed"))
        self.pending_requests.clear()
        self.status_handlers.clear()

        for handler in self.stream_handlers.values():
            with contextlib.suppress(Exception):
                if end := handler.get("end"):
                    end()
        self.stream_handlers.clear()

        self._settle_pull_iterators()
        return cleanups

    @staticmethod
    async def _run_cleanups(cleanups: list[Callable[[], Coroutine[Any, Any, None]]]) -> None:
        for cleanup in cleanups:
            with contextlib.suppress(Exception):
                await cleanup()

    def _track_session(self, sessions: dict[str, _Teardown], session_id: str, teardown: _Teardown) -> None:
        if self._closed or self._suspending:
            # the running teardown already took the sessions it ends
            run_in_background(teardown())
            return
        sessions[session_id] = teardown

    def _take_handler_sessions(self) -> tuple[list[_Teardown], list[_Teardown]]:
        sessions = [*self.stream_cleanups.values(), *self.pull_iterator_cleanups.values()]
        callbacks = list(self.callback_cleanups.values())
        self.stream_cleanups.clear()
        self.pull_iterator_cleanups.clear()
        self.callback_cleanups.clear()
        return sessions, callbacks

    @staticmethod
    async def _end_handler_sessions(taken: tuple[list[_Teardown], list[_Teardown]]) -> None:
        sessions, callbacks = taken
        await asyncio.gather(*[cleanup() for cleanup in sessions], return_exceptions=True)
        await asyncio.gather(*[cleanup() for cleanup in callbacks], return_exceptions=True)

    def _settle_pull_iterators(self) -> None:
        for settle in list(self._pull_iterator_settles):
            with contextlib.suppress(Exception):
                settle()
        self._pull_iterator_settles.clear()

    def reconfigure(
        self,
        servers: list[str] | None = None,
        auth: RPCAuthOptions | None = None,
    ) -> None:
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
        namespace: str | None = None,
        subject: str = "",
        method_cache: list[Collection[str] | None] | None = None,
    ) -> T:
        max_retries, delays = self._retry_plan(override)
        missing: asyncio.Future[bool] | None = None
        try:
            for attempt in range(max_retries + 1):
                try:
                    return await fn()
                except Exception as e:
                    if not is_local_no_responders(e) or attempt >= max_retries or self._closed:
                        raise

                    delay = delays[min(attempt, len(delays) - 1)]
                    if attempt == 0 and namespace is not None:
                        missing = asyncio.ensure_future(
                            self._method_missing(namespace, subject, method_cache, delay)
                        )
                    if await self._retry_wait(delay, missing):
                        raise
        finally:
            if missing is not None:
                missing.cancel()

        # unreachable, for the type checker
        raise RuntimeError("Retry loop exhausted")

    def _retry_plan(self, override: NoResponderRetryOptions | None) -> tuple[int, Sequence[float]]:
        src = override if override is not None else self.options.get("no_responder_retry") or {}
        max_retries = src.get("max_retries")
        delays = src.get("delays")
        if max_retries is None and delays is None:
            return len(DEFAULT_RETRY_DELAYS), DEFAULT_RETRY_DELAYS
        return (
            max_retries if max_retries is not None else LEGACY_MAX_RETRIES,
            delays if delays is not None else LEGACY_RETRY_DELAYS,
        )

    @staticmethod
    async def _retry_wait(delay: float, missing: asyncio.Future[bool] | None) -> bool:
        if missing is None:
            await asyncio.sleep(delay)
            return False
        started = time.monotonic()
        if not missing.done():
            await asyncio.wait((missing,), timeout=delay)
        if missing.done() and not missing.cancelled() and missing.result():
            return True
        await asyncio.sleep(max(0.0, delay - (time.monotonic() - started)))
        return False

    async def publish(
        self, subject: str, data: Any, headers: dict[str, str] | None = None, reply: str | None = None
    ) -> None:
        if not self.nc:
            raise RuntimeError("Not connected")
        encoded = encode_message(data)
        # inline: a second coroutine per message costs every stream item and reply
        if len(encoded) <= self._max_payload_size:
            await self.nc.publish(subject, encoded, headers=headers, reply=reply if reply else "")
            return
        await self._publish_encoded(subject, encoded, headers, reply)

    async def _publish_encoded(
        self, subject: str, encoded: bytes, headers: dict[str, str] | None = None, reply: str | None = None
    ) -> None:
        if not self.nc:
            raise RuntimeError("Not connected")

        replyTo = reply if reply else ""

        if len(encoded) <= self._max_payload_size:
            await self.nc.publish(subject, encoded, headers=headers, reply=replyTo)
            return
        if self._unchunkable_limit is not None:
            raise create_error(
                ErrorCode.PAYLOAD_TOO_LARGE,
                f"Payload limit {self._unchunkable_limit} is too small for chunking",
            )

        transfer_id = generate_id()
        total_chunks = (len(encoded) + self._max_payload_size - 1) // self._max_payload_size

        header_msg: ChunkedTransferHeader = {
            "type": "chunked",
            "transferId": transfer_id,
            "totalChunks": total_chunks,
            "totalSize": len(encoded),
            "chunkSize": self._max_payload_size,
        }

        hdrs = dict(headers) if headers else {}
        hdrs["x-chunked-transfer"] = "header"
        hdrs["x-chunk-id"] = transfer_id

        await self.nc.publish(subject, encode(header_msg), headers=hdrs, reply=replyTo)

        for i, chunk in enumerate(create_chunks(encoded, transfer_id, self._max_payload_size)):
            chunk_hdrs = {"x-chunked-transfer": "chunk", "x-chunk-id": transfer_id, "x-chunk-index": str(i)}

            await self.nc.publish(subject, chunk["data"], headers=chunk_hdrs, reply=replyTo)

            if i > 0 and i % 50 == 0:
                await asyncio.sleep(0)

    async def _publish_reply(self, subject: str, response: RPCResponse, entry: dict[str, Any]) -> None:
        nc = self.nc
        sub: Subscription | None = entry.get("sub")
        # with further calls queued their replies leave together through nats-py's flusher
        if not isinstance(nc, NatsConnection) or sub is None or sub.pending_msgs:
            await self.publish(subject, response)
            return
        nc.write_through = True
        try:
            await self.publish(subject, response)
        finally:
            nc.write_through = False

    async def subscribe(
        self,
        pattern: str,
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
        queue: str = "",
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        return await self._subscribe(pattern, handler, queue, {})

    async def _subscribe_session(
        self,
        pattern: str,
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        return await self._subscribe(pattern, handler, "", {}, yield_after=False)

    async def _confirm_subscriptions(self) -> None:
        nc = self.nc
        if nc is None or not nc.is_connected:
            return
        if QUEUED_PING and isinstance(nc, NATSClient):
            with contextlib.suppress(Exception):
                pong = await _queue_ping(nc)
                # no wait_for: nats-py sets a PONG future's result without checking for a cancel
                await asyncio.wait((pong,), timeout=SUBSCRIBE_CONFIRM_TIMEOUT)
            return
        # nats-py's flush() writes its PING at once, the yield lets its flusher send the SUBs first
        await asyncio.sleep(0)
        with contextlib.suppress(Exception):
            await nc.flush(timeout=SUBSCRIBE_CONFIRM_TIMEOUT)

    async def _subscribe(
        self,
        pattern: str,
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]],
        queue: str,
        entry: dict[str, Any],
        yield_after: bool = True,
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        if not self.nc:
            raise RuntimeError("Not connected")

        self._subscription_seq += 1
        key = self._subscription_seq
        entry.update(
            {
                "key": key,
                "pattern": pattern,
                "handler": handler,
                "handler_is_async": is_async_function(handler),
                "queue": queue,
                "sub": None,
            }
        )
        self._subscription_entries[key] = entry

        await self._nats_subscribe(entry, yield_after)

        async def unsubscribe() -> None:
            # bookkeeping first, the NATS unsubscribe below may be cut off when it ends the task calling us
            self._subscription_entries.pop(key, None)
            self._leave_chunk_scope(entry)
            sub: Subscription | None = entry.get("sub")
            entry["sub"] = None
            if sub is not None:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

        return unsubscribe

    async def _nats_subscribe(self, entry: dict[str, Any], yield_after: bool = True) -> None:
        if not self.nc:
            raise RuntimeError("Not connected")

        pattern: str = entry["pattern"]
        handler: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]] = entry["handler"]
        handler_is_async: bool = entry["handler_is_async"]
        # the handler takes the time nats-py read the message as its second argument
        with_arrival: bool = entry.get("arrival", False)

        # raw mode for the mux: it needs headers and the subject and reassembles chunks itself
        if entry.get("raw"):
            raw_handler = cast(Callable[[Msg], None], handler)

            async def raw_message_handler(msg: Msg) -> None:
                try:
                    raw_handler(msg)
                except Exception as e:
                    print(f"Error processing message for {pattern}:", e)
                    print(traceback.format_exc())

            if INLINE_REPLY_DELIVERY and isinstance(self.nc, NATSClient):
                # the idle cb keeps nats-py's subscription task, which _watch_subscription_end follows
                raw_sub = await self.nc.subscribe(
                    pattern,
                    queue=entry.get("queue") or "",
                    cb=_idle_callback,
                    future=cast(Any, _InlineSink(raw_handler, pattern)),
                )
            else:
                raw_sub = await self.nc.subscribe(
                    pattern, queue=entry.get("queue") or "", cb=raw_message_handler
                )
            entry["sub"] = raw_sub
            self._watch_subscription_end(entry, raw_sub)
            return

        # assembled payloads drain inline below, create_task would run handlers out of order and break backpressure
        assembled_queue: list[Any] = []
        chunks: _ChunkScope | None = entry.get("chunks")
        if chunks is None:
            chunks = self._join_chunk_scope(entry)

        async def message_handler(msg: Msg) -> None:
            try:
                chunk_type = msg.headers.get("x-chunked-transfer") if msg.headers else None

                if chunk_type == "header":
                    data = decode(msg.data)
                    chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None

                    if not chunk_id or not isinstance(data, dict) or data.get("transferId") != chunk_id:
                        return

                    def on_complete(assembled_data: Any) -> None:
                        assembled_queue.append(assembled_data)

                    def on_error(error: Exception) -> None:
                        print(f"Error assembling chunks for {pattern}: {error}")

                    manager = chunks.manager
                    if manager is None:
                        manager = chunks.manager = self._new_chunking_manager()
                    manager.start_receiving(
                        chunk_id,
                        data.get("totalChunks"),
                        on_complete,
                        on_error,
                        data.get("totalSize"),
                        data.get("chunkSize"),
                    )

                elif chunk_type == "chunk":
                    if not msg.headers:
                        return

                    chunk_id = msg.headers.get("x-chunk-id")
                    chunk_index = parse_chunk_index(msg.headers.get("x-chunk-index"))

                    if not chunk_id:
                        return

                    manager = chunks.manager
                    if manager is not None:
                        manager.process_chunk(
                            {"id": chunk_id, "chunkIndex": chunk_index, "data": msg.data, "isLast": False}
                        )

                    while assembled_queue:
                        await self._handle_assembled_data(handler, assembled_queue.pop(0), handler_is_async)

                else:
                    # views into msg.data are safe, nats-py hands over one bytes object per message
                    data = decode_message(msg.data)
                    if with_arrival:
                        await cast(Any, handler)(data, getattr(msg, "arrived", None))
                    elif handler_is_async:
                        await handler(data)  # type: ignore[misc]
                    else:
                        handler(data)

            except Exception as e:
                print(f"Error processing message for {pattern}:", e)
                print(traceback.format_exc())

        sub = await self.nc.subscribe(pattern, queue=entry.get("queue") or "", cb=message_handler)
        # a drain hands it the messages left in the subscription's queue
        entry["dispatch"] = message_handler
        # yield so nats-py's flusher sends the SUB first, publishes from other connections are not ordered against it
        if yield_after:
            await asyncio.sleep(0)

        entry["sub"] = sub
        self._watch_subscription_end(entry, sub)

    async def _subscribe_namespace(
        self,
        namespace: str,
        dispatchers: Mapping[str, Callable[..., Coroutine[Any, Any, None]]],
        list_methods: Callable[[Any], Coroutine[Any, Any, None]],
        queue: str,
        entry: dict[str, Any],
        registration: _Registration,
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        prefix = f"rpc.{namespace}."
        method_names = list(dispatchers)
        lanes: dict[str, _Lane] = {}

        def method_of(subject: str) -> str | None:
            # only registered methods get a transfer scope, the others share the subscription's
            method = subject[len(prefix) :]
            return method if subject.startswith(prefix) and method in dispatchers else None

        def open_lane(method: str) -> _Lane | None:
            handle = dispatchers.get(method)
            if handle is None:
                return None
            # a reply reads the lane's backlog from it like from a subscription
            lane_entry: dict[str, Any] = {}

            async def dispatch(item: tuple[Any, bool, float]) -> None:
                payload, decoded, arrived = item
                try:
                    await handle(payload if decoded else decode_message(payload), arrived, entry=lane_entry)
                except Exception as e:
                    print(f"Error processing message for {prefix}{method}:", e)
                    print(traceback.format_exc())

            lane = lanes[method] = lane_entry["sub"] = _Lane(dispatch)
            registration.lanes.append(lane)
            return lane

        def route(subject: str, payload: Any, decoded: bool, size: int) -> None:
            if not subject.startswith(prefix):
                return
            method = subject[len(prefix) :]
            if method == METHODS_METHOD:
                run_in_background(list_methods(payload if decoded else decode_message(payload)))
                return
            lane = lanes.get(method) or open_lane(method)
            if lane is None:
                run_in_background(self._reply_missing_method(method, payload, decoded, method_names))
                return
            if (
                lane.pending_msgs >= DEFAULT_SUB_PENDING_MSGS_LIMIT
                or lane.pending_bytes + size > DEFAULT_SUB_PENDING_BYTES_LIMIT
            ):
                self._report_slow_consumer(prefix + method)
                return
            lane.push((payload, decoded, time.monotonic()), size)

        entry["raw"] = True
        return await self._subscribe(
            f"rpc.{namespace}.>", self._raw_router(entry, route, method_of), queue, entry, yield_after=False
        )

    def _raw_router(
        self,
        entry: dict[str, Any],
        deliver: Callable[[str, Any, bool, int], None],
        method_of: Callable[[str], str | None] | None = None,
    ) -> Callable[[Msg], None]:

        def manager_for(scope: _ChunkScope, subject: str, create: bool) -> ChunkingManager | None:
            method = method_of(subject) if method_of is not None else None
            if method is None:
                if scope.manager is None and create:
                    scope.manager = self._new_chunking_manager()
                return scope.manager
            methods = scope.methods
            manager = methods.get(method) if methods is not None else None
            if manager is None and create:
                if methods is None:
                    methods = scope.methods = {}
                manager = methods[method] = self._new_chunking_manager()
            return manager

        def on_message(msg: Msg) -> None:
            subject = msg.subject
            headers = msg.headers
            chunk_type = headers.get("x-chunked-transfer") if headers else None
            if not headers or chunk_type not in ("header", "chunk"):
                deliver(subject, msg.data, False, len(msg.data))
                return
            chunk_id = headers.get("x-chunk-id")
            if not chunk_id:
                return
            scope: _ChunkScope = entry.get("chunks") or self._join_chunk_scope(entry)
            if chunk_type == "header":
                header = decode(msg.data)
                if not isinstance(header, dict) or header.get("transferId") != chunk_id:
                    return
                size = header.get("totalSize")
                manager = cast(ChunkingManager, manager_for(scope, subject, True))
                manager.start_receiving(
                    chunk_id,
                    header.get("totalChunks"),
                    lambda assembled: deliver(subject, assembled, True, size if isinstance(size, int) else 0),
                    lambda error: print(f"Error assembling chunks for {subject}: {error}"),
                    size,
                    header.get("chunkSize"),
                )
            elif (receiver := manager_for(scope, subject, False)) is not None:
                receiver.process_chunk(
                    {
                        "id": chunk_id,
                        "chunkIndex": parse_chunk_index(headers.get("x-chunk-index")),
                        "data": msg.data,
                        "isLast": False,
                    }
                )

        return on_message

    async def _subscribe_raw(
        self, pattern: str, deliver: Callable[[str, Any, bool, int], None]
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        entry: dict[str, Any] = {"raw": True}
        return await self._subscribe(pattern, self._raw_router(entry, deliver), "", entry, yield_after=False)

    async def _reply_missing_method(
        self, method: str, payload: Any, decoded: bool, methods: list[str]
    ) -> None:
        message = payload if decoded else decode_message(payload)
        if not isinstance(message, dict) or not isinstance(request_id := message.get("id"), str):
            return
        error = create_error(ErrorCode.METHOD_NOT_FOUND, f"Method not found: {method}").to_dict()
        params = message.get("params")
        with contextlib.suppress(Exception):
            if isinstance(params, dict) and params.get("__stream") is not None:
                subject = params.get("__streamSubject")
                if isinstance(subject, str) and subject.startswith("stream."):
                    frame: StreamMessage = {"id": request_id, "type": "error", "error": error}
                    await self.publish(subject, frame)
                return
            response: RPCResponse = {"id": request_id, "error": error, "__methods": methods}
            await self.publish(f"rpc.reply.{request_id}", response)

    def _watch_subscription_end(self, entry: dict[str, Any], sub: Subscription) -> None:

        def on_finish(fut: asyncio.Task[None]) -> None:
            # only a clean end of the msg task drops the entry
            if fut.cancelled() or fut.exception() is not None:
                return
            # during a suspend or after a restore onto a new subscription the entry must survive
            if self._suspending or entry.get("sub") is not sub or sub in self._drained_subs:
                return
            # nats-py ends the task the same way when it closes the connection for good
            if getattr(getattr(sub, "_conn", None), "is_closed", False):
                return
            entry["sub"] = None
            self._subscription_entries.pop(entry["key"], None)
            self._leave_chunk_scope(entry)
            if entry is self._mux_entry:
                self._mux_entry = None

            async def unsub() -> None:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

            asyncio.create_task(unsub())

        msg_task: asyncio.Task[None] | None = getattr(sub, "_wait_for_msgs_task", None)
        if msg_task:
            msg_task.add_done_callback(on_finish)

    def _register_mux_entry(self) -> dict[str, Any]:
        if self._mux_entry is None:
            self._subscription_seq += 1
            entry: dict[str, Any] = {
                "key": self._subscription_seq,
                "pattern": f"rpc.reply.{self._reply_prefix}.>",
                "handler": self._handle_mux_message,
                "handler_is_async": False,
                "queue": "",
                "sub": None,
                "raw": True,
            }
            self._mux_entry = entry
            self._subscription_entries[self._subscription_seq] = entry
        return self._mux_entry

    async def _ensure_mux_subscription(self) -> None:
        entry = self._register_mux_entry()
        sub: Subscription | None = entry.get("sub")
        if self.nc and (sub is None or getattr(sub, "_closed", False)):
            await self._nats_subscribe(entry)

    def _handle_mux_message(self, msg: Msg) -> None:
        data = msg.data
        headers = msg.headers

        # the reply subject of a call is rpc.reply.<call id>, of a stream or iterator the registered token
        if (
            (data is None or len(data) == 0)
            and headers
            and headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
        ):  # pyright: ignore[reportUnnecessaryComparison]
            suffix = msg.subject[len("rpc.reply.") :]

            status_handler = self.status_handlers.pop(suffix, None)
            if status_handler is not None:
                try:
                    status_handler(errors.NoRespondersError(suffix))
                except Exception as e:
                    print(f"Error in no-responder status handler: {e}")
                return

            pending = self.pending_requests.pop(suffix, None)
            if pending is not None:
                if timeout_handle := pending.get("timeout"):
                    timeout_handle.cancel()
                future: asyncio.Future[Any] | None = pending.get("future")
                if future is not None and not future.done():
                    future.set_exception(errors.NoRespondersError(pending.get("subject") or suffix))
            return

        chunk_type = headers.get("x-chunked-transfer") if headers else None

        if headers and chunk_type in ("header", "chunk"):
            # the subject suffix is the call id; without a pending call nobody reads the reply
            call_id = msg.subject[len("rpc.reply.") :]
            pending = self.pending_requests.get(call_id)
            if pending is None:
                return
            chunk_id = headers.get("x-chunk-id")
            if not chunk_id:
                print(
                    "Invalid chunk header on reply mux"
                    if chunk_type == "header"
                    else "Chunk missing chunk ID on reply mux"
                )
                return
            transfer_key = f"{call_id}|{chunk_id}"

            if chunk_type == "header":
                decoded = decode(data)
                if not isinstance(decoded, dict) or decoded.get("transferId") != chunk_id:
                    print("Invalid chunk header on reply mux")
                    return
                # one transfer per call keeps the uncapped manager bounded
                previous = pending.get("transfer_key")
                if previous is not None and previous != transfer_key:
                    self._mux_chunking.discard(previous)
                pending["transfer_key"] = transfer_key
                self._mux_chunking.start_receiving(
                    transfer_key,
                    decoded.get("totalChunks"),
                    self._route_mux_response,
                    lambda error: self._fail_chunked_reply(call_id, error),
                    decoded.get("totalSize"),
                    decoded.get("chunkSize"),
                )
            else:
                chunk_index = parse_chunk_index(headers.get("x-chunk-index"))
                self._mux_chunking.process_chunk(
                    {"id": transfer_key, "chunkIndex": chunk_index, "data": data, "isLast": False}
                )
        else:
            self._route_mux_response(decode_message(data))

    def _fail_chunked_reply(self, call_id: str, error: Exception) -> None:
        pending = self.pending_requests.pop(call_id, None)
        if pending is None:
            return
        if timeout_handle := pending.get("timeout"):
            timeout_handle.cancel()
        if transfer_key := pending.get("transfer_key"):
            self._mux_chunking.discard(transfer_key)
        future: asyncio.Future[Any] | None = pending.get("future")
        if future is not None and not future.done():
            subject = pending.get("subject") or call_id
            future.set_exception(
                create_error(ErrorCode.INTERNAL_ERROR, f'Chunked reply to "{subject}" failed: {error}')
            )

    def _route_mux_response(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        response = cast(dict[str, Any], data)
        request_id = response.get("id")
        if not request_id:
            return

        pending = self.pending_requests.pop(request_id, None)
        if pending is None:
            return
        if timeout_handle := pending.get("timeout"):
            timeout_handle.cancel()

        future: asyncio.Future[Any] | None = pending.get("future")
        if future is None or future.done():
            return

        if (error := response.get("error")) is not None:
            future.set_exception(_reply_error(error))
        elif pending.get("meta"):
            # proxies take __methods next to the result, which stays as the responder sent it
            future.set_result((response.get("result"), response.get("__methods")))
        else:
            result = response.get("result")
            # for proxy method discovery
            if "__methods" in response and result is not None and isinstance(result, dict):
                result["__methods"] = response["__methods"]
            future.set_result(result)

    async def _handle_assembled_data(
        self, handler: Callable[[Any], Any], data: Any, handler_is_async: bool | None = None
    ) -> None:
        if handler_is_async is None:
            handler_is_async = is_async_function(handler)
        try:
            if handler_is_async:
                await handler(data)  # pyright: ignore[reportGeneralTypeIssues]
            else:
                handler(data)
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
        return await self._with_no_responder_retry(
            lambda: self._request_once(subject, data, timeout=timeout, headers=headers),
            override=no_responder_retry,
        )

    async def _request_once(
        self, subject: str, data: Any, timeout: int | None = None, headers: dict[str, str] | None = None
    ) -> Any:
        if not self.nc:
            raise RuntimeError("Not connected")

        timeout_sec = (timeout or 5000) / 1000
        encoded = encode_message(data)

        try:
            msg = await self.nc.request(subject, encoded, timeout=timeout_sec, headers=headers)

            if msg.headers and msg.headers.get("Nats-Service-Error-Code"):
                error_code = msg.headers.get("Nats-Service-Error-Code", "500")
                error_msg = msg.headers.get("Nats-Service-Error", "Service error")
                error_data = None
                if msg.data:
                    with contextlib.suppress(Exception):
                        error_data = decode_message(msg.data)
                raise create_error(error_code, error_msg, error_data)

            decoded = decode_message(msg.data)

            if isinstance(decoded, dict) and "error" in decoded:
                code = cast(str, decoded.get("code", ErrorCode.INTERNAL_ERROR.value))
                message = cast(str, decoded.get("error", "Unknown error"))
                raise create_error(code, message, cast(Any, decoded))

            return cast(Any, decoded)
        except TimeoutError as e:
            raise RPCTimeoutError(
                ErrorCode.TIMEOUT.value, f'Request to "{subject}" timed out after {timeout or 5000}ms'
            ) from e
        except errors.NoRespondersError as e:
            raise LocalNoRespondersError(
                ErrorCode.NOT_FOUND.value, "No responders available", subject=subject
            ) from e

    async def call(
        self,
        subject: str,
        *args: Any,
        no_responder_retry: NoResponderRetryOptions | None = None,
        discover: bool = False,
        timeout: int | None = None,
    ) -> Any:
        return await self._with_no_responder_retry(
            lambda: self._call_once(subject, *args, discover=discover, timeout=timeout),
            override=no_responder_retry,
        )

    async def _call_once(
        self,
        subject: str,
        *args: Any,
        discover: bool = False,
        timeout: int | None = None,
        meta: bool = False,
        on_cancel: Callable[[asyncio.Future[Any]], None] | None = None,
    ) -> Any:
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        # service calls (<subject>.reply.<id>) keep the per-call subscription flow
        if not subject.startswith("rpc."):
            result = await self._call_once_service(subject, *args, timeout=timeout, on_cancel=on_cancel)
            return (result, None) if meta else result

        # connect() did this already unless the connection was wired up out-of-band, as in tests
        await self._ensure_mux_subscription()

        request_id = generate_id(self._reply_prefix)
        if timeout is None:
            timeout = self.options.get("timeout", 30000)
        # responders publish to rpc.reply.<msg id>, the id starts with the reply prefix so the mux catches it
        reply_subject = f"rpc.reply.{request_id}"

        future: asyncio.Future[Any] = asyncio.Future()

        def handle_timeout() -> None:
            pending = self.pending_requests.pop(request_id, None)
            if pending is None:
                return
            if transfer_key := pending.get("transfer_key"):
                self._mux_chunking.discard(transfer_key)
            waiter: asyncio.Future[Any] = pending["future"]
            if not waiter.done():
                waiter.set_exception(
                    RPCTimeoutError(
                        ErrorCode.TIMEOUT.value, f'RPC call to "{subject}" timed out after {timeout}ms'
                    )
                )

        timeout_handle = asyncio.get_event_loop().call_later(timeout / 1000, handle_timeout)

        self.pending_requests[request_id] = {
            "future": future,
            "timeout": timeout_handle,
            "subject": subject,
            "meta": meta,
        }

        # reply is the call's own subject, so a no-responder 503 lands where the response would
        message: RPCMessage = {"id": request_id, "method": "call", "params": args}
        if math.isfinite(timeout):
            message["__ttl"] = max(1, math.ceil(timeout))
        if discover:
            # an envelope marker, in params it would leak into the handler args
            message["__discover"] = True
        try:
            try:
                await self.publish(subject, message, reply=reply_subject)
            except Exception:
                if self.pending_requests.pop(request_id, None) is not None:
                    timeout_handle.cancel()
                raise
            return await future
        except asyncio.CancelledError:
            # a cancel during the publish comes after nats-py queued the request
            if on_cancel is not None:
                if (pending := self.pending_requests.get(request_id)) is not None:
                    pending["future"] = asyncio.get_running_loop().create_future()
                    on_cancel(pending["future"])
                else:
                    # the call ended in the same loop step as the cancel
                    on_cancel(future)
            raise

    async def _call_with_meta(
        self,
        subject: str,
        args: tuple[Any, ...],
        *,
        discover: bool,
        no_responder_retry: NoResponderRetryOptions | None = None,
        timeout: int | None = None,
        namespace: str | None = None,
        method_cache: list[Collection[str] | None] | None = None,
    ) -> tuple[Any, list[str] | None]:
        return cast(
            tuple[Any, "list[str] | None"],
            await self._with_no_responder_retry(
                lambda: self._call_once(subject, *args, discover=discover, timeout=timeout, meta=True),
                override=no_responder_retry,
                namespace=namespace,
                subject=subject,
                method_cache=method_cache,
            ),
        )

    async def _list_methods(self, namespace: str, wait: float) -> list[str] | None:
        try:
            methods = await self._call_once(
                f"rpc.{namespace}.{METHODS_METHOD}", timeout=max(1, int(wait * 1000))
            )
        except Exception:
            return None
        return cast(list[str], methods) if isinstance(methods, list) else None

    async def _method_missing(
        self,
        namespace: str,
        subject: str,
        method_cache: list[Collection[str] | None] | None,
        delay: float,
    ) -> bool:
        methods = await self._list_methods(namespace, max(delay, METHOD_PROBE_WINDOW))
        if methods is None or subject.removeprefix(f"rpc.{namespace}.") in methods:
            return False
        # a filled cache keeps its list: a method gone during a responder restart stays callable
        if method_cache is not None and method_cache[0] is None:
            method_cache[0] = methods
        return True

    async def _call_service(self, subject: str, args: tuple[Any, ...], direct: str) -> Any:
        return await self._with_no_responder_retry(
            lambda: self._call_once_service(subject, *args, direct=direct)
        )

    async def _call_once_service(
        self,
        subject: str,
        *args: Any,
        timeout: int | None = None,
        direct: str | None = None,
        on_cancel: Callable[[asyncio.Future[Any]], None] | None = None,
    ) -> Any:
        if not self.nc:
            raise RuntimeError("Not connected")

        request_id = generate_id(self._reply_prefix)
        if timeout is None:
            timeout = self.options.get("timeout", 30000)
        message: RPCMessage = {"id": request_id, "method": "call", "params": args}
        encoded = encode_message(message)
        if direct is not None and len(encoded) > self._max_payload_size:
            subject = direct
        reply_subject = f"{subject}.reply.{request_id}"

        future: asyncio.Future[Any] = asyncio.Future()

        sub: Subscription | None = None
        unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

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

        def handle_timeout() -> None:
            if (pending := self.pending_requests.pop(request_id, None)) is None:
                return
            waiter: asyncio.Future[Any] = pending["future"]
            if not waiter.done():
                waiter.set_exception(
                    RPCTimeoutError(
                        ErrorCode.TIMEOUT.value, f'RPC call to "{subject}" timed out after {timeout}ms'
                    )
                )

        timeout_handle = asyncio.get_event_loop().call_later(timeout / 1000, handle_timeout)

        # the cleanup hook keeps a suspended call from leaving a dead reply subscription behind
        self.pending_requests[request_id] = {
            "future": future,
            "timeout": timeout_handle,
            "subject": subject,
            "cleanup": unsubscribe_all,
        }

        async def handle_rpc_response(data: Any) -> None:
            response = data
            if response.get("id") == request_id:
                pending = self.pending_requests.get(response["id"])

                if pending:
                    del self.pending_requests[response["id"]]
                    pending["timeout"].cancel()

                    # settle first: unsubscribing cancels the message task this handler runs in
                    if (error := response.get("error")) is not None:
                        if not pending["future"].done():
                            pending["future"].set_exception(_reply_error(error))
                    else:
                        result = response.get("result")
                        # for proxy method discovery
                        if "__methods" in response and result is not None and isinstance(result, dict):
                            result["__methods"] = response["__methods"]
                        if not pending["future"].done():
                            pending["future"].set_result(result)

                    await unsubscribe_all()

        async def request_callback(msg: Msg) -> None:
            if (
                (msg.data is None or len(msg.data) == 0)  # pyright: ignore[reportUnnecessaryComparison]
                and msg.headers
                and msg.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
            ):
                pending = self.pending_requests.get(request_id)
                waiter: asyncio.Future[Any] = pending["future"] if pending is not None else future
                if not waiter.done():
                    waiter.set_exception(errors.NoRespondersError(subject))

                await unsubscribe_all()

        try:
            unsubscribe = await self._subscribe_session(reply_subject, handle_rpc_response)

            inbox = self.nc.new_inbox()
            sub = await self.nc.subscribe(inbox, cb=request_callback, max_msgs=1)

            await self._publish_encoded(subject, encoded, reply=inbox)

            return await future
        except asyncio.CancelledError:
            if on_cancel is not None and (pending := self.pending_requests.get(request_id)) is not None:
                rest: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                pending["future"] = rest
                # the except-branch below no longer runs for this call
                rest.add_done_callback(lambda _: run_in_background(unsubscribe_all()))
                on_cancel(rest)
            else:
                # the timer only drops the pending entry, the subscriptions go here
                run_in_background(unsubscribe_all())
                if on_cancel is not None:
                    on_cancel(future)
            raise
        except Exception:
            await unsubscribe_all()
            raise

    def _handle_timeout(
        self, request_id: str, subject: str, future: asyncio.Future[Any], timeout: int
    ) -> None:
        if request_id in self.pending_requests:
            del self.pending_requests[request_id]
            if not future.done():
                future.set_exception(
                    RPCTimeoutError(
                        ErrorCode.TIMEOUT.value, f'RPC call to "{subject}" timed out after {timeout}ms'
                    )
                )

    def _handle_rpc_response(self, data: Any) -> None:
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

    def call_stream(
        self,
        subject: str,
        *args: Any,
        no_responder_retry: NoResponderRetryOptions | None = None,
    ) -> AsyncGenerator[Any, None]:
        return self._stream_with_retry(subject, args, no_responder_retry)

    def _call_stream_with_probe(
        self,
        subject: str,
        args: tuple[Any, ...],
        *,
        no_responder_retry: NoResponderRetryOptions | None,
        namespace: str,
        method_cache: list[Collection[str] | None],
    ) -> AsyncGenerator[Any, None]:
        return self._stream_with_retry(subject, args, no_responder_retry, namespace, method_cache)

    async def _stream_with_retry(
        self,
        subject: str,
        args: tuple[Any, ...],
        no_responder_retry: NoResponderRetryOptions | None,
        namespace: str | None = None,
        method_cache: list[Collection[str] | None] | None = None,
    ) -> AsyncGenerator[Any, None]:
        max_retries, delays = self._retry_plan(no_responder_retry)
        missing: asyncio.Future[bool] | None = None
        try:
            for attempt in range(max_retries + 1):
                yielded = False
                try:
                    # closes the attempt inside our own aclose(), not later through the asyncgen finalizer
                    async with contextlib.aclosing(self._call_stream_once(subject, *args)) as stream:
                        async for value in stream:
                            yielded = True
                            yield value
                    return
                except Exception as e:
                    if yielded or not is_local_no_responders(e) or attempt >= max_retries or self._closed:
                        raise

                    delay = delays[min(attempt, len(delays) - 1)]
                    if attempt == 0 and namespace is not None:
                        missing = asyncio.ensure_future(
                            self._method_missing(namespace, subject, method_cache, delay)
                        )
                    if await self._retry_wait(delay, missing):
                        raise
        finally:
            if missing is not None:
                missing.cancel()

    async def _call_stream_once(self, subject: str, *args: Any) -> AsyncGenerator[Any, None]:
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        await self._ensure_mux_subscription()

        request_id = generate_id(self._reply_prefix)
        stream_subject = f"stream.{subject}.{request_id}"

        queue: asyncio.Queue[Any] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

        def on_push(value: Any) -> None:
            nonlocal ended
            if not ended:
                queue.put_nowait(value)

        def on_end() -> None:
            nonlocal ended
            ended = True
            queue.put_nowait(None)

        def on_error(err: Exception) -> None:
            nonlocal error, ended
            error = err
            ended = True
            queue.put_nowait(None)

        handler: dict[str, Any] = {
            "push": on_push,
            "end": on_end,
            "error": on_error,
            "subject": subject,
        }

        self.stream_handlers[request_id] = handler

        async def unsubscribe_all() -> None:
            nonlocal ended

            if request_id in self.stream_handlers:
                del self.stream_handlers[request_id]
            self.status_handlers.pop(request_id, None)
            if unsubscribe:
                with contextlib.suppress(Exception):
                    await unsubscribe()

            if not ended:
                ended = True
                with contextlib.suppress(Exception):
                    await self.publish(f"{stream_subject}.cancel", {"id": request_id})

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

        try:
            unsubscribe = await self._subscribe_session(stream_subject, handle_stream_message)

            def on_no_responders(_err: Exception) -> None:
                stream_handler = self.stream_handlers.get(request_id)
                if stream_handler:
                    stream_handler["error"](errors.NoRespondersError(subject))

            # the reply subject only carries a 503, stream responders never answer it directly
            self.status_handlers[request_id] = on_no_responders

            stream_params: dict[str, Any] = {
                "__stream": True,
                "__streamSubject": stream_subject,
                "args": args,
            }
            message: RPCMessage = {"id": request_id, "method": "stream", "params": stream_params}
            await self.publish(subject, message, reply=f"rpc.reply.{request_id}")

            # items queued before an error frame are delivered first, the error ends the stream
            while True:
                value = await queue.get()
                if value is None:  # end marker
                    if error:
                        raise error
                    return
                yield value
        finally:
            await unsubscribe_all()

    async def call_pull_iterator(self, subject: str, *args: Any) -> AsyncGenerator[Any, None]:
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        await self._ensure_mux_subscription()

        iterator_id = generate_id(self._reply_prefix)
        request_subject = f"_rpc.iterator.{iterator_id}.request"
        response_subject = f"_rpc.iterator.{iterator_id}.response"
        # 503 inbox for next requests, under the mux wildcard since iterator_id starts with the reply prefix
        status_inbox = f"rpc.reply.{iterator_id}"

        # a no-responder this client saw is queued as the exception itself, so it keeps its marker
        response_queue: asyncio.Queue[PullIteratorResponse | Exception] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        def settle_on_disconnect() -> None:
            nonlocal ended, error
            if not ended:
                ended = True
                error = create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed")
            err = (
                error
                if isinstance(error, RPCException)
                else create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed")
            )
            settle_msg: PullIteratorResponse = {
                "type": "error",
                "id": iterator_id,
                "error": err.to_dict(),
            }
            response_queue.put_nowait(settle_msg)

        self._pull_iterator_settles.add(settle_on_disconnect)

        try:
            await self._init_pull_session(
                subject,
                {"__pullIterator": True, "__iteratorId": iterator_id, "args": args},
                request_subject,
                iterator_id,
                "pull iterator",
            )
        except BaseException:
            self._pull_iterator_settles.discard(settle_on_disconnect)
            raise

        async def handle_response(msg: PullIteratorResponse) -> None:
            nonlocal ended, error

            if msg.get("type") == "error":
                if error_data := msg.get("error"):
                    error = RPCException.from_dict(error_data)
                ended = True
            elif msg.get("type") == "done":
                ended = True

            await response_queue.put(msg)

        unsubscribe = await self._subscribe_session(response_subject, handle_response)

        async def cleanup() -> None:
            self._pull_iterator_settles.discard(settle_on_disconnect)
            self.status_handlers.pop(iterator_id, None)

            if unsubscribe is not None:
                await unsubscribe()

            if not ended:
                with contextlib.suppress(Exception):
                    cancel_request: PullIteratorRequest = {
                        "id": iterator_id,
                        "type": "cancel",
                    }
                    await self.publish(request_subject, cancel_request)

        def on_no_responders(_err: Exception) -> None:
            nonlocal ended, error

            ended = True
            error = LocalNoRespondersError("503", str(errors.NoRespondersError(subject)), subject=subject)
            response_queue.put_nowait(error)

        self.status_handlers[iterator_id] = on_no_responders

        try:
            # every next carries the status inbox, a 503 there means the responder died mid-iteration
            while True:
                next_request: PullIteratorRequest = {
                    "id": iterator_id,
                    "type": "next",
                }
                await self.publish(request_subject, next_request, reply=status_inbox)

                response = await response_queue.get()

                if isinstance(response, Exception):
                    raise response
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
        if not self.is_connected and not self.is_closed:
            await self.connect()

        if not self.nc:
            raise RuntimeError("Not connected")

        await self._ensure_mux_subscription()

        iterator_id = generate_id(self._reply_prefix)
        request_subject = f"_rpc.iterator.{iterator_id}.request"
        response_subject = f"_rpc.iterator.{iterator_id}.response"
        callback_subject = f"_rpc.cb.{iterator_id}"
        status_inbox = f"rpc.reply.{iterator_id}"

        callback_methods = list(callbacks.keys())

        # one consumer task runs the callbacks in order, a slow one holds back the next request and so the producer
        callback_is_async = {name: is_async_function(fn) for name, fn in callbacks.items()}
        # callback messages, and for each batch boundary a future behind the callbacks that came before it
        cb_queue: asyncio.Queue[tuple[Any, bool] | asyncio.Future[None]] = asyncio.Queue()
        boundaries: deque[asyncio.Future[None]] = deque()

        async def consume_callbacks() -> None:
            while True:
                item = await cb_queue.get()
                method: Any = None
                try:
                    if isinstance(item, asyncio.Future):
                        if not item.done():
                            item.set_result(None)
                        continue
                    payload, decoded = item
                    msg = payload if decoded else decode_message(payload)
                    if not isinstance(msg, dict):
                        continue
                    method = msg.get("method")
                    # a producer's liveness probe carries no method of ours
                    if not isinstance(method, str) or (fn := callbacks.get(method)) is None:
                        continue
                    fn_args = msg.get("args") or []
                    if callback_is_async[method]:
                        await fn(*fn_args)
                    else:
                        fn(*fn_args)
                except Exception as e:
                    print(f"[rpc] Pull-callback handler '{method}' threw: {e}")
                finally:
                    cb_queue.task_done()

        consumer_task = asyncio.create_task(consume_callbacks())

        async def stop_consumer() -> None:
            consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer_task

        def on_callback(_subject: str, payload: Any, decoded: bool, _size: int) -> None:
            cb_queue.put_nowait((payload, decoded))

        # subscribed before the init call, both session subjects deliver in wire order
        cb_unsub = await self._subscribe_raw(callback_subject, on_callback)

        # a no-responder this client saw is queued as the exception itself, so it keeps its marker
        response_queue: asyncio.Queue[PullIteratorResponse | Exception] = asyncio.Queue()
        ended = False
        error: Exception | None = None

        def settle_on_disconnect() -> None:
            nonlocal ended, error
            if not ended:
                ended = True
                error = create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed")
            err = (
                error
                if isinstance(error, RPCException)
                else create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed")
            )
            settle_msg: PullIteratorResponse = {
                "type": "error",
                "id": iterator_id,
                "error": err.to_dict(),
            }
            response_queue.put_nowait(settle_msg)

        self._pull_iterator_settles.add(settle_on_disconnect)

        init_params: PullCallbackParams = {
            "__pullCallback": True,
            "__iteratorId": iterator_id,
            "__callbackSubject": callback_subject,
            "__callbackMethods": callback_methods,
            "__onewayMethods": list(oneway_methods),
            "args": list(args),
        }

        try:
            await self._init_pull_session(
                subject, init_params, request_subject, iterator_id, "pull-callback iterator"
            )
        except BaseException:
            self._pull_iterator_settles.discard(settle_on_disconnect)
            await cb_unsub()
            await stop_consumer()
            raise

        def on_response(_subject: str, payload: Any, decoded: bool, _size: int) -> None:
            nonlocal ended, error
            data = payload if decoded else decode_message(payload)
            if not isinstance(data, dict):
                return
            msg = cast(PullIteratorResponse, data)
            if msg.get("type") == "error":
                if error_data := msg.get("error"):
                    error = RPCException.from_dict(error_data)
                ended = True
            elif msg.get("type") == "done":
                ended = True
            elif msg.get("type") == "value":
                boundary = asyncio.get_running_loop().create_future()
                cb_queue.put_nowait(boundary)
                boundaries.append(boundary)
            response_queue.put_nowait(msg)

        resp_unsub = await self._subscribe_raw(response_subject, on_response)

        def on_no_responders(_err: Exception) -> None:
            nonlocal ended, error

            ended = True
            error = LocalNoRespondersError("503", str(errors.NoRespondersError(subject)), subject=subject)
            response_queue.put_nowait(error)

        self.status_handlers[iterator_id] = on_no_responders

        async def cleanup() -> None:
            self._pull_iterator_settles.discard(settle_on_disconnect)
            self.status_handlers.pop(iterator_id, None)
            await cb_unsub()
            await stop_consumer()
            if resp_unsub is not None:
                await resp_unsub()
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
                await self.publish(request_subject, next_request, reply=status_inbox)

                response = await response_queue.get()

                if isinstance(response, Exception):
                    raise response
                if response.get("type") == "error":
                    if error_data := response.get("error"):
                        raise RPCException.from_dict(error_data)
                elif response.get("type") == "done":
                    # run the frames delivered before the done signal
                    await cb_queue.join()
                    break
                elif response.get("type") == "value":
                    # only this batch's callbacks; a slow handler stalls the producer at its yield
                    await boundaries.popleft()
                    yield None
        finally:
            await cleanup()

    async def _init_pull_session(
        self, subject: str, params: Any, request_subject: str, iterator_id: str, kind: str
    ) -> None:

        def follow(rest: asyncio.Future[Any]) -> None:
            run_in_background(self._cancel_after_init(rest, request_subject, iterator_id))

        try:
            response = await self._with_no_responder_retry(
                lambda: self._call_once(subject, params, on_cancel=follow)
            )
            if not response or response.get("iteratorId") != iterator_id:
                raise RuntimeError(f"Failed to initialize {kind}")
        except Exception:
            await self._cancel_pull_session(request_subject, iterator_id)
            raise

    async def _cancel_after_init(
        self, init: asyncio.Future[Any], request_subject: str, iterator_id: str
    ) -> None:
        # a cancel ahead of the init reply can arrive before the responder listens on the request subject
        await asyncio.wait((init,))
        if not init.cancelled():
            init.exception()
        await self._cancel_pull_session(request_subject, iterator_id)

    async def _cancel_pull_session(self, request_subject: str, iterator_id: str) -> None:
        cancel: PullIteratorRequest = {"id": iterator_id, "type": "cancel"}
        with contextlib.suppress(Exception):
            await self.publish(request_subject, cancel)

    def _handle_stream_message(self, msg: StreamMessage) -> None:
        stream_handler = self.stream_handlers.get(msg["id"])
        if not stream_handler:
            return

        if msg["type"] == "data":
            stream_handler["push"](msg.get("data"))
        elif msg["type"] == "end":
            stream_handler["push"](None)  # end marker
            stream_handler["end"]()
            if msg["id"] in self.stream_handlers:
                del self.stream_handlers[msg["id"]]
        elif msg["type"] == "error":
            if error_data := msg.get("error"):
                stream_handler["error"](RPCException.from_dict(error_data))
            stream_handler["push"](None)
            if msg["id"] in self.stream_handlers:
                del self.stream_handlers[msg["id"]]

    async def call_with_callback(
        self, subject: str, args: list[Any], callback: Callable[[Any], Any]
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        if not self.is_connected and not self.is_closed:
            await self.connect()
        if not self.nc:
            raise RuntimeError("Not connected")

        request_id = generate_id(self._reply_prefix)
        callback_subject = f"rpc.cb.{request_id}"
        callback_is_async = is_async_function(callback)

        async def handle_callback_msg(msg: Any) -> None:
            if isinstance(msg, dict):
                if msg.get("type") == "data":
                    try:
                        if callback_is_async:
                            await callback(msg.get("data"))
                        else:
                            callback(msg.get("data"))
                    except Exception as e:
                        print(f"[rpc] Callback error: {e}")
                elif msg.get("type") == "error":
                    print(f"[rpc] Callback error: {msg.get('error')}")

        unsub = await self._subscribe_session(callback_subject, handle_callback_msg)

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
        *,
        concurrency: Mapping[str, int] | None = None,
        orphan_probe: OrphanProbeOptions | Literal[False] | None = None,
        namespace_subscription: bool = False,
        drop_expired: bool | Collection[str] = False,
    ) -> Callable[[], Coroutine[Any, Any, None]]:
        if namespace_subscription and not _namespace_subscribable(namespace):
            raise ValueError(
                f'Namespace "{namespace}" overlaps internal subjects and cannot take a namespace subscription'
            )
        if not self.nc:
            raise RuntimeError("Not connected")

        client: RPCClient = self
        if isolated_connection:
            opts: RPCClientOptions = {
                **self.options,
                "name": f"{self.options['name']}-handler-{namespace}",
            }
            client = self.create_isolated_client(opts)
            await client.connect()
            self.isolated_clients.append(client)

        unsubscribers: list[Callable[[], Coroutine[Any, Any, None]]] = []
        stream_ids: set[str] = set()
        pull_iterator_ids: set[str] = set()
        callback_ids: set[str] = set()
        registration = _Registration()
        limits = {name: limit for name, limit in (concurrency or {}).items() if limit >= 2}
        probe_settings = client._orphan_probe_settings(orphan_probe)
        dispatchers: dict[str, Callable[..., Coroutine[Any, Any, None]]] = {}

        handlers_map = (
            extract_nested_methods_without_decorators(handlers)
            if without_decorators
            else extract_nested_methods_with_decorators(handlers)
        )
        method_names = list(handlers_map.keys())
        dropping: Collection[str] = (
            method_names
            if drop_expired is True
            else ({drop_expired} if isinstance(drop_expired, str) else set(drop_expired or ()))
        )
        # a namespace subscription notes the arrival itself
        if dropping and not namespace_subscription:
            client._stamp_arrivals()

        async def send_error(
            message: Any, response: RPCResponse, e: Exception, entry: dict[str, Any]
        ) -> None:
            error_dict = format_error_dict(e)
            response["error"] = error_dict
            # a result that failed to go out would fail the error reply too
            response.pop("result", None)

            # a METHOD_NOT_FOUND error always carries the method list
            if error_dict["code"] == ErrorCode.METHOD_NOT_FOUND.value:
                response["__methods"] = method_names

            try:
                reply_subject = f"rpc.reply.{message['id']}"
                await client._publish_reply(reply_subject, response, entry)
            except Exception as publish_error:
                if client.is_closed:
                    return

                print(f"Failed to send error response: {publish_error}")

        async def call_normal(
            handler: Any, handler_is_async: bool, message: Any, response: RPCResponse, entry: dict[str, Any]
        ) -> None:
            wants_timing = message.get("__timing") is True
            start = time.time() * 1000 if wants_timing else 0.0
            result = await handle_normal_rpc(
                handler,
                message.get("params", []),
                client.io_pool,
                handler_is_async,
            )
            if wants_timing:
                response["__timing"] = {
                    "start": int(start),
                    "end": int(time.time() * 1000),
                }
            response["result"] = result

            reply_subject = f"rpc.reply.{message['id']}"
            await client._publish_reply(reply_subject, response, entry)

        async def answer_closed(message: Any, response: RPCResponse) -> None:
            # only past the drain deadline; a call answered already keeps its reply, nothing goes out twice
            if (
                not registration.expired
                or "result" in response
                or "error" in response
                or not client.is_connected
            ):
                return
            params = message.get("params")
            with contextlib.suppress(Exception):
                if isinstance(params, dict) and params.get("__stream") is not None:
                    await refuse_stream(client, message["id"], params.get("__streamSubject"))
                    return
                response["error"] = create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed").to_dict()
                await client.publish(f"rpc.reply.{message['id']}", response)

        async def call_limited(
            handler: Any, message: Any, response: RPCResponse, entry: dict[str, Any]
        ) -> None:
            try:
                await call_normal(handler, True, message, response, entry)
            except asyncio.CancelledError:
                await answer_closed(message, response)
                raise
            except Exception as e:
                await send_error(message, response, e, entry)

        async def start_limited(
            slots: asyncio.Semaphore,
            handler: Any,
            message: Any,
            response: RPCResponse,
            entry: dict[str, Any],
            arrived: float | None,
        ) -> None:
            # the slot is taken in the subscription task: at the limit messages wait in nats-py's queue, in arrival order
            await slots.acquire()
            if arrived is not None and _expired(message, arrived):
                slots.release()
                return
            if registration.expired:
                # a task created now is cancelled before its first step and would never answer
                slots.release()
                await answer_closed(message, response)
                return
            task = asyncio.create_task(call_limited(handler, message, response, entry))
            # a done callback also fires for a task cancelled before its first step
            task.add_done_callback(lambda _: slots.release())
            for tasks in (registration.calls, client._concurrent_calls):
                tasks.add(task)
                task.add_done_callback(tasks.discard)

        for method, handler in handlers_map.items():
            subject = f"rpc.{namespace}.{method}"
            handler_is_async = is_async_function(handler)
            # outlives suspend()/connect(): the restored subscription keeps this closure
            slots = asyncio.Semaphore(limits[method]) if handler_is_async and method in limits else None
            # filled by _subscribe, a reply reads the method's queue from it
            entry: dict[str, Any] = {"arrival": True} if method in dropping else {}

            async def handle_message(
                msg: Any,
                arrived: float | None = None,
                *,
                handler: Any = handler,
                method_names: list[str] = method_names,
                handler_is_async: bool = handler_is_async,
                slots: asyncio.Semaphore | None = slots,
                entry: dict[str, Any] = entry,
                drops: bool = method in dropping,
            ) -> None:
                message = msg
                response: RPCResponse = {"id": message["id"]}
                if registration.expired:
                    await answer_closed(message, response)
                    return
                if not drops:
                    arrived = None
                elif arrived is not None and _expired(message, arrived):
                    # its caller gave up already, skipped without a reply
                    return

                # only a request with __discover pays for the method list, old clients never send it
                if isinstance(message, dict) and message.get("__discover") is True:
                    response["__methods"] = method_names

                try:
                    params = message.get("params")
                    is_stream_request = bool(
                        isinstance(params, dict)
                        and params.get("__stream") is not None
                        and params.get("__streamSubject") is not None
                    )

                    # params are the object itself or wrapped in a list by call()
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

                        # streams run in their own task, awaiting them would block the next messages of the subscription
                        stream_id = cast(str, message["id"])
                        try:
                            stream_cleanup, _ = start_stream_request(
                                handler,
                                args,
                                stream_subject,
                                stream_id,
                                client,
                                client.io_pool,
                                on_finished=partial(
                                    _forget_session, client.stream_cleanups, stream_ids, stream_id
                                ),
                                probe=client._new_orphan_probe(probe_settings),
                            )
                        except Exception as e:
                            print(f"Stream request error for {method}: {e}")
                            return
                        stream_ids.add(stream_id)
                        client._track_session(client.stream_cleanups, stream_id, stream_cleanup)
                        return  # no reply for a stream request
                    elif is_pull_iterator:
                        args = cast(list[Any], pull_params.get("args", []))
                        iterator_id = cast(str, pull_params.get("__iteratorId", message["id"]))
                        cleanup = await set_up_session(
                            handle_pull_iterator_request(
                                handler,
                                args,
                                iterator_id,
                                client,
                                client.io_pool,
                                on_finished=partial(
                                    _forget_session,
                                    client.pull_iterator_cleanups,
                                    pull_iterator_ids,
                                    iterator_id,
                                ),
                                probe=client._new_orphan_probe(probe_settings),
                            ),
                            client,
                            f"rpc.reply.{message['id']}",
                            response,
                        )

                        pull_iterator_ids.add(iterator_id)
                        client._track_session(client.pull_iterator_cleanups, iterator_id, cleanup)
                        response["result"] = {"iteratorId": iterator_id}

                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    elif is_pull_callback:
                        pc_args = cast(list[Any], pc_params.get("args", []))
                        pc_iterator_id = cast(str, pc_params.get("__iteratorId", message["id"]))
                        pc_callback_subject = cast(str, pc_params.get("__callbackSubject", ""))
                        pc_oneway_methods = cast(list[str], pc_params.get("__onewayMethods", []))

                        pc_cleanup = await set_up_session(
                            handle_pull_callback_request(
                                handler,
                                pc_args,
                                pc_iterator_id,
                                pc_callback_subject,
                                pc_oneway_methods,
                                client,
                                client.io_pool,
                                on_finished=partial(
                                    _forget_session,
                                    client.pull_iterator_cleanups,
                                    pull_iterator_ids,
                                    pc_iterator_id,
                                ),
                                probe=client._new_orphan_probe(probe_settings),
                            ),
                            client,
                            f"rpc.reply.{message['id']}",
                            response,
                        )

                        pull_iterator_ids.add(pc_iterator_id)
                        client._track_session(client.pull_iterator_cleanups, pc_iterator_id, pc_cleanup)
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
                        _cb_params = cast(
                            dict[str, Any],
                            params[0] if isinstance(params, list) and params[0].get("__callback") else params,
                        )
                        callback_subject = cast(str, _cb_params["__callbackSubject"])
                        cb_args = cast(list[Any], _cb_params.get("args", []))

                        cb_cleanup = await set_up_session(
                            handle_callback_request(
                                handler,
                                cb_args,
                                callback_subject,
                                message["id"],
                                client,
                                client.io_pool,
                                on_finished=partial(
                                    _forget_session, client.callback_cleanups, callback_ids, message["id"]
                                ),
                                probe=client._new_orphan_probe(probe_settings),
                            ),
                            client,
                            f"rpc.reply.{message['id']}",
                            response,
                        )
                        callback_ids.add(message["id"])
                        client._track_session(client.callback_cleanups, message["id"], cb_cleanup)

                        response["result"] = {"ok": True}
                        reply_subject = f"rpc.reply.{message['id']}"
                        await client.publish(reply_subject, response)
                    elif slots is not None:
                        await start_limited(slots, handler, message, response, entry, arrived)
                    else:
                        await call_normal(handler, handler_is_async, message, response, entry)

                except asyncio.CancelledError:
                    await answer_closed(message, response)
                    raise
                except Exception as e:
                    await send_error(message, response, e, entry)

            if namespace_subscription:
                dispatchers[method] = handle_message
                continue
            unsubscribe = await client._subscribe(subject, handle_message, queue, entry, yield_after=False)
            unsubscribers.append(unsubscribe)
            registration.entries.append(entry)

        async def list_methods(msg: Any) -> None:
            if isinstance(msg, dict) and isinstance(request_id := msg.get("id"), str):
                response: RPCResponse = {"id": request_id, "result": method_names}
                await client.publish(f"rpc.reply.{request_id}", response)

        if namespace_subscription:
            namespace_entry: dict[str, Any] = {}
            unsubscribers.append(
                await client._subscribe_namespace(
                    namespace, dispatchers, list_methods, queue, namespace_entry, registration
                )
            )
            registration.entries.append(namespace_entry)
        else:
            # after the methods, so an answer means all of them are subscribed; it goes first at teardown
            methods_entry: dict[str, Any] = {}
            unsubscribers.insert(
                0,
                await client._subscribe(
                    f"rpc.{namespace}.{METHODS_METHOD}", list_methods, queue, methods_entry, yield_after=False
                ),
            )
            registration.entries.insert(0, methods_entry)
        client._registrations.add(registration)
        await client._confirm_subscriptions()

        async def cleanup() -> None:
            # taken out first, so a disconnect() of the client meanwhile does not drain it a second time
            client._registrations.discard(registration)
            await client._drain_registrations((registration,))
            await asyncio.gather(*(unsub() for unsub in unsubscribers), return_exceptions=True)
            await asyncio.gather(*_stop_lanes((registration,)), return_exceptions=True)
            await _cancel_tasks(registration.calls)

            async def end_session(
                cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]], session_id: str
            ) -> None:
                if fn := cleanups.pop(session_id, None):
                    await fn()

            # only the sessions this registration created, the client maps are shared by every namespace
            await asyncio.gather(
                *(end_session(client.stream_cleanups, stream_id) for stream_id in list(stream_ids)),
                *(
                    end_session(client.pull_iterator_cleanups, iterator_id)
                    for iterator_id in list(pull_iterator_ids)
                ),
                return_exceptions=True,
            )
            stream_ids.clear()
            pull_iterator_ids.clear()

            for cb_id in list(callback_ids):
                if cb_cleanup := client.callback_cleanups.pop(cb_id, None):
                    with contextlib.suppress(Exception):
                        await cb_cleanup()

            if isolated_connection:
                await client.disconnect()
                if client in self.isolated_clients:
                    self.isolated_clients.remove(client)

        return cleanup

    async def on_request(
        self, pattern: str, handler: Callable[[Any], Any]
    ) -> Callable[[], Coroutine[Any, None, None]]:
        if not self.nc:
            raise RuntimeError("Not connected")

        handler_is_async = is_async_function(handler)

        async def handle_request(msg: Msg) -> None:
            try:
                data = decode_message(msg.data)

                # sync handlers run inline, cheaper than a thread-pool hop and it keeps the order
                if handler_is_async:
                    result = await handler(data)  # pyright: ignore[reportGeneralTypeIssues]
                else:
                    result = handler(data)

                if msg.reply:
                    response = encode_message(result)
                    await msg.respond(response)

            except Exception as e:
                if msg.reply:
                    reported = reported_error(e)
                    text = (
                        reported.message
                        if isinstance(reported, RPCException) and reported is not e
                        else str(e)
                    )
                    code = (
                        reported.code
                        if isinstance(reported, RPCException)
                        else ErrorCode.INTERNAL_ERROR.value
                    )
                    await msg.respond(encode_message({"error": text, "code": code}))

        sub = await self.nc.subscribe(pattern, cb=handle_request)
        # yield so the flusher sends the SUB first, as in _nats_subscribe
        await asyncio.sleep(0)

        async def unsubscribe() -> None:
            if sub:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

        return unsubscribe

    async def channel(self, channel_id: str, isolated_connection: bool = False) -> Channel:
        if isolated_connection:
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-channel-{channel_id}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        channel = Channel(cast(Any, client), channel_id)
        await channel.init()

        if isolated_connection:
            channel._isolated_client = client
            channel._parent_client = self

        return channel

    async def private_channel(
        self, channel_id: str, target_client_id: str, isolated_connection: bool = False
    ) -> PrivateChannel:
        if isolated_connection:
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-private-{channel_id}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        channel = PrivateChannel(cast(Any, client), channel_id, target_client_id)
        await channel.init()

        if isolated_connection:
            channel._isolated_client = client
            channel._parent_client = self

        return channel

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
    ) -> T | Any | ProxyWithClose[T] | ProxyWithClose[Any]:
        if isolated_connection:
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-proxy-{namespace}"}
            )
            self.isolated_clients.append(client)

            proxy = create_proxy(
                cast(Any, client), namespace, no_responder_retry=no_responder_retry, timeout=timeout
            )

            setattr(proxy, "_isolated_client", client)  # noqa: B010
            setattr(proxy, "_parent_client", self)  # noqa: B010
            return ProxyWithClose(proxy)
        else:
            return create_proxy(
                cast(Any, self), namespace, no_responder_retry=no_responder_retry, timeout=timeout
            )

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
        if isolated_connection:
            client = self.create_isolated_client(
                {**self.options, "name": f"{self.options['name']}-service-{service_name}"}
            )
            self.isolated_clients.append(client)
            await client.connect()
        else:
            client = self

        monitor = client.service.monitor()
        services = await monitor.info(service_name)

        if not services:
            if isolated_connection:
                await client.disconnect()
                if client in self.isolated_clients:
                    self.isolated_clients.remove(client)
            raise RuntimeError(f"No services found with name: {service_name}")

        selected = (
            next((s for s in services if s.id == preferred_id), services[0]) if preferred_id else services[0]
        )

        proxy = create_service_proxy(cast(Any, client), selected, timeout)

        if isolated_connection:
            setattr(proxy, "_isolated_client", client)  # noqa: B010
            setattr(proxy, "_parent_client", self)  # noqa: B010
            return ProxyWithClose(proxy)
        else:
            return proxy
