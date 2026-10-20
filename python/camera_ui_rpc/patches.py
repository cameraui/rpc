from __future__ import annotations

import dataclasses
import inspect
import re
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any

import nats.aio.client as nats_aio_client
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.transport import TcpTransport
from nats.protocol import parser as nats_parser
from nats.protocol.parser import Parser

# nats-py releases the parser and flush patches were checked against
TESTED_NATS_VERSIONS = frozenset({(2, 15), (2, 16)})
LARGE_PAYLOAD = 32 * 1024  # copied once on receive from this size on instead of three times
_AWAITING_CONTROL_LINE: int = getattr(nats_parser, "AWAITING_CONTROL_LINE", 1)
_AWAITING_MSG_PAYLOAD: int = getattr(nats_parser, "AWAITING_MSG_PAYLOAD", 2)
_CRLF_SIZE: int = getattr(nats_parser, "CRLF_SIZE", 2)
_MAX_CONTROL_LINE_SIZE: int = getattr(nats_parser, "MAX_CONTROL_LINE_SIZE", 4096)


def _nats_version() -> tuple[int, int] | None:
    match = re.match(r"(\d+)\.(\d+)\.", str(getattr(nats_aio_client, "__version__", "")))
    return (int(match[1]), int(match[2])) if match else None


def _parser_supported() -> bool:
    try:
        parser = Parser(None)
        message_params = list(inspect.signature(NATSClient._process_msg).parameters)  # pyright: ignore[reportPrivateUsage]
        return (
            inspect.iscoroutinefunction(Parser.parse)
            and type(parser.buf) is bytearray
            and parser.state == _AWAITING_CONTROL_LINE != _AWAITING_MSG_PAYLOAD
            and parser.needed == 0
            and parser.header_needed == 0
            and isinstance(parser.msg_arg, dict)
            and _CRLF_SIZE == 2
            and message_params == ["self", "sid", "subject", "reply", "data", "headers"]
            and type(NATSClient()._ps) is Parser  # pyright: ignore[reportPrivateUsage]
        )
    except Exception:
        return False


def _direct_write_supported() -> bool:
    try:
        nc = NATSClient()
        transport = TcpTransport()  # type: ignore[no-untyped-call]
        return (
            inspect.iscoroutinefunction(NATSClient._flush_pending)  # pyright: ignore[reportPrivateUsage]
            and "force_flush" in inspect.signature(NATSClient._flush_pending).parameters  # pyright: ignore[reportPrivateUsage]
            and isinstance(nc._pending, list)  # pyright: ignore[reportPrivateUsage]
            and nc._pending_data_size == 0  # pyright: ignore[reportPrivateUsage]
            and nc._flusher_task is None  # pyright: ignore[reportPrivateUsage]
            and isinstance(NATSClient.CONNECTED, int)
            and hasattr(nc, "_status")
            and hasattr(transport, "_io_writer")
            and callable(getattr(transport, "writelines", None))
        )
    except Exception:
        return False


_TESTED = _nats_version() in TESTED_NATS_VERSIONS

PAYLOAD_PARSER = _TESTED and _parser_supported()
DIRECT_WRITES = _TESTED and _direct_write_supported()


def _take(parts: list[Any], start: int, stop: int) -> bytes:
    views: list[memoryview] = []
    offset = 0
    for part in parts:
        size = len(part)
        if offset + size > start and offset < stop:
            views.append(memoryview(part)[max(start - offset, 0) : min(stop - offset, size)])
        offset += size
        if offset >= stop:
            break
    if len(views) == 1:
        return bytes(views[0])
    return b"".join(views)


class PayloadParser(Parser):
    def reset(self) -> None:
        super().reset()
        self._parts: list[Any] | None = None
        self._have = 0

    def parse(self, data: bytes = b"") -> Coroutine[Any, Any, None]:  # type: ignore[override]
        if self._parts is None:
            if self.state == _AWAITING_MSG_PAYLOAD:
                if self.needed < LARGE_PAYLOAD:
                    return Parser.parse(self, data)
            elif self.buf or len(data) < LARGE_PAYLOAD:
                return Parser.parse(self, data)
        return self._parse_large(data)

    async def _parse_large(self, data: bytes) -> None:
        pos = 0
        if self._parts is None and self.state == _AWAITING_MSG_PAYLOAD:
            # the stock parser started this payload, its buffer becomes the first part
            self._parts = [self.buf]
            self._have = len(self.buf)
            self.buf = bytearray()
        if self._parts is not None:
            self._parts.append(data)
            self._have += len(data)
            want = self.needed + _CRLF_SIZE
            if self._have < want:
                return
            pos = len(data) - (self._have - want)
            await self._deliver(self._parts)

        while (
            type(data) is bytes
            and len(data) - pos >= LARGE_PAYLOAD
            and not self.buf
            and self.state == _AWAITING_CONTROL_LINE
        ):
            end = data.find(b"\r\n", pos, pos + _MAX_CONTROL_LINE_SIZE)
            if end < 0:
                break
            # the stock parser takes the control line alone and stops in front of the payload
            await Parser.parse(self, data[pos : end + 2])
            pos = end + 2
            if self.state != _AWAITING_MSG_PAYLOAD:
                continue
            if self.needed < LARGE_PAYLOAD:
                break
            available = len(data) - pos
            if available < self.needed + _CRLF_SIZE:
                self._parts = [memoryview(data)[pos:]]
                self._have = available
                return
            stop = pos + self.needed
            await self._deliver([memoryview(data)[pos:stop]])
            pos = stop + _CRLF_SIZE

        if pos < len(data):
            # the stock parser only extends its bytearray from it, a view saves a copy
            await Parser.parse(self, memoryview(data)[pos:] if pos else data)  # type: ignore[arg-type]

    async def _deliver(self, parts: list[Any]) -> None:
        headers = None
        if self.header_needed > 0:
            headers = _take(parts, 0, self.header_needed)
            payload = _take(parts, self.header_needed, self.needed)
            self.header_needed = 0
        else:
            payload = _take(parts, 0, self.needed)
        self._parts = None
        self._have = 0
        self.state = _AWAITING_CONTROL_LINE
        arg = self.msg_arg
        await self.nc._process_msg(arg["sid"], arg["subject"], arg["reply"], payload, headers)


class NatsConnection(NATSClient):
    def __init__(self) -> None:
        super().__init__()
        if PAYLOAD_PARSER and type(self._ps) is Parser:  # pyright: ignore[reportPrivateUsage]
            self._ps = PayloadParser(self)
        self.write_through = False

    async def _flush_pending(self, force_flush: bool = False) -> Any:
        if self.write_through and not force_flush and self._write_pending():
            return None
        return await super()._flush_pending(force_flush)

    def _write_pending(self) -> bool:
        if not DIRECT_WRITES or self._pending_data_size == 0 or self._status != NATSClient.CONNECTED:
            return False
        transport = self._transport
        if type(transport) is not TcpTransport:
            return False
        flusher = self._flusher_task
        writer = transport._io_writer  # pyright: ignore[reportPrivateUsage]
        if flusher is None or flusher.done() or writer is None:
            return False
        sock = writer.transport
        if sock.is_closing() or sock.get_write_buffer_size():
            return False
        transport.writelines(self._pending)  # type: ignore[no-untyped-call]
        self._pending = []
        self._pending_data_size = 0
        return True


async def connect(**options: Any) -> NATSClient:
    nc = NatsConnection() if PAYLOAD_PARSER or DIRECT_WRITES else NATSClient()
    await nc.connect(**options)
    return nc


@dataclass
class ArrivalMsg(Msg):
    arrived: float = field(default_factory=time.monotonic)


def _arrival_stamps_supported() -> bool:
    try:
        return (
            getattr(NATSClient, "msg_class", None) is Msg
            and dataclasses.is_dataclass(Msg)
            and "msg_class" in NATSClient._build_message.__code__.co_names  # pyright: ignore[reportPrivateUsage]
            and ArrivalMsg(None, subject="s", data=b"d", _sid=1).arrived > 0  # type: ignore[arg-type]
        )
    except Exception:
        return False


ARRIVAL_STAMPS = _arrival_stamps_supported()
