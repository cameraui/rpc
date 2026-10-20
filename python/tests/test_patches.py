from __future__ import annotations

import asyncio
import json
import random
import tracemalloc
from typing import Any

import pytest
from nats.aio.client import Client as NATSClient
from nats.aio.transport import TcpTransport
from nats.protocol.parser import Parser
from test_mux import make_client
from test_nats_shape import offline_nats_client

import camera_ui_rpc.patches as patches
from camera_ui_rpc.codec import encode

READ_SIZE = 256 * 1024
HEADERS = b"NATS/1.0\r\nx-chunked-transfer: chunk\r\nx-chunk-id: q\r\n\r\n"
SIZES = [0, 1, 1000, 32767, 32768, 32769, 58000, 65536, 300000, 1228800]


class Events:
    def __init__(self) -> None:
        self.seen: list[tuple[Any, ...]] = []

    async def _process_msg(self, sid: int, subject: bytes, reply: bytes, data: Any, headers: Any) -> None:
        assert type(data) is bytes
        assert headers is None or type(headers) is bytes
        self.seen.append(("msg", sid, bytes(subject), bytes(reply), data, headers))

    async def _process_ping(self) -> None:
        self.seen.append(("ping",))

    async def _process_pong(self) -> None:
        self.seen.append(("pong",))

    async def _process_info(self, info: dict[str, Any]) -> None:
        self.seen.append(("info", info))

    async def _process_err(self, err: str) -> None:
        self.seen.append(("err", err))


def msg(sid: int, payload: bytes, headers: bytes | None = None, reply: bytes = b"rpc.reply.p.1") -> bytes:
    subject = b"rpc.plugin.detectObjects"
    reply_part = b" " + reply if reply else b""
    if headers is None:
        return b"MSG %s %d%s %d\r\n" % (subject, sid, reply_part, len(payload)) + payload + b"\r\n"
    total = len(headers) + len(payload)
    line = b"HMSG %s %d%s %d %d\r\n" % (subject, sid, reply_part, len(headers), total)
    return line + headers + payload + b"\r\n"


def framing(rnd: random.Random) -> tuple[bytes, list[int]]:
    ops: list[bytes] = []
    for sid in range(1, rnd.randint(2, 7)):
        size = rnd.choice(SIZES)
        payload = rnd.randbytes(size) if size < 4096 else bytes([rnd.randrange(256)]) * size
        ops.append(
            msg(sid, payload, HEADERS if rnd.random() < 0.3 else None, b"" if rnd.random() < 0.2 else b"r.1")
        )
        extra = rnd.random()
        if extra < 0.15:
            ops.append(b"PING\r\n")
        elif extra < 0.25:
            ops.append(b"PONG\r\n+OK\r\n")
        elif extra < 0.3:
            # longer than a control line, the payload path must hand it to the stock parser
            ops.append(b"INFO " + json.dumps({"connect_urls": ["x" * 5000]}).encode() + b"\r\n")
    wire = b"".join(ops)
    mode = rnd.random()
    if mode < 0.4:
        cuts = sorted({rnd.randint(1, len(wire) - 1) for _ in range(rnd.randint(0, 40))})
    elif mode < 0.7:
        cuts = list(range(READ_SIZE, len(wire), READ_SIZE))
    else:
        # control line ends, the header boundary and a split CRLF
        marks: set[int] = set()
        offset = 0
        for op in ops:
            line_end = op.find(b"\r\n") + 2
            for cut in (
                line_end - 1,
                line_end,
                line_end + 1,
                line_end + len(HEADERS),
                len(op) - 2,
                len(op) - 1,
            ):
                if 0 < offset + cut < len(wire):
                    marks.add(offset + cut)
            offset += len(op)
        cuts = sorted(rnd.sample(sorted(marks), min(len(marks), rnd.randint(1, 12))))
    return wire, cuts


async def parse_all(parser_cls: type[Parser], wire: bytes, cuts: list[int]) -> tuple[Events, Parser]:
    events = Events()
    parser = parser_cls(events)
    start = 0
    for cut in [*cuts, len(wire)]:
        await parser.parse(wire[start:cut])
        start = cut
    return events, parser


def test_payload_parser_delivers_exactly_what_the_stock_parser_delivers() -> None:
    async def run() -> None:
        rnd = random.Random(20261003)
        for _ in range(250):
            wire, cuts = framing(rnd)
            expected, _ = await parse_all(Parser, wire, cuts)
            got, parser = await parse_all(patches.PayloadParser, wire, cuts)
            assert got.seen == expected.seen
            assert parser.state == 1 and not parser.buf and parser._parts is None  # pyright: ignore[reportPrivateUsage]
            # no memoryview of the buffer may outlive a message, nats-py's read loop dies silently on BufferError
            parser.buf.extend(b"x" * 64)
            del parser.buf[:]

    asyncio.run(run())


@pytest.mark.parametrize("size", [32768, 300000, 1228800])
def test_a_large_payload_arrives_whole_from_any_read_split(size: int) -> None:
    async def run() -> None:
        payload = bytes(range(256)) * (size // 256) + b"\x07" * (size % 256)
        wire = msg(1, payload, HEADERS) + msg(2, b"tail")
        for cut in (1, 30, len(wire) - len(msg(2, b"tail")) - 1, len(wire) // 2, len(wire) - 3):
            events, _ = await parse_all(patches.PayloadParser, wire, [cut])
            assert [(e[1], e[4], e[5]) for e in events.seen] == [(1, payload, HEADERS), (2, b"tail", None)]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("lead", "size", "bound"),
    [
        (b"", 40000, 1.3),
        (b"", 300000, 1.3),
        (b"", 1228800, 1.3),
        # a small message first hands the read to the stock parser, the large payload is joined on the next read
        (msg(9, b"small"), 300000, 2.0),
        (msg(9, b"small"), 1228800, 1.3),
    ],
)
def test_a_large_payload_is_copied_once(lead: bytes, size: int, bound: float) -> None:
    async def run() -> None:
        payload = b"\x03" * size
        wire = lead + msg(1, payload)
        reads = [wire[start : start + READ_SIZE] for start in range(0, len(wire), READ_SIZE)]
        events = Events()
        parser = patches.PayloadParser(events)
        tracemalloc.start()
        try:
            before = tracemalloc.get_traced_memory()[0]
            for data in reads:
                await parser.parse(data)
            peak = tracemalloc.get_traced_memory()[1] - before
        finally:
            tracemalloc.stop()
        assert events.seen[-1][4] == payload
        # the stock parser peaks at three payload sizes: its buffer, the slice and the bytes made from it
        assert peak < bound * size

    asyncio.run(run())


def test_reset_drops_a_half_received_payload() -> None:
    async def run() -> None:
        events = Events()
        parser = patches.PayloadParser(events)
        first = msg(1, b"\x01" * 300000)
        await parser.parse(first[:100000])
        await parser.parse(first[100000:200000])
        assert parser._parts is not None  # pyright: ignore[reportPrivateUsage]

        parser.reset()
        await parser.parse(msg(2, b"\x02" * 40000) + msg(3, b"small"))

        assert [(e[1], len(e[4])) for e in events.seen] == [(2, 40000), (3, 5)]

    asyncio.run(run())


def test_a_connection_hands_large_payloads_to_its_subscriptions() -> None:
    async def run() -> None:
        nc = patches.NatsConnection()
        offline = offline_nats_client()
        nc._send_subscribe = offline._send_subscribe  # type: ignore[method-assign]
        got: list[Any] = []
        done = asyncio.Event()

        async def on_msg(m: Any) -> None:
            got.append((m.subject, m.reply, m.data, m.headers))
            if len(got) == 2:
                done.set()

        sub = await nc.subscribe("rpc.plugin.detectObjects", cb=on_msg)
        payload = b"\x05" * 1228800
        wire = msg(sub._id, payload, HEADERS) + msg(sub._id, b"x")  # pyright: ignore[reportPrivateUsage]
        for start in range(0, len(wire), READ_SIZE):
            await nc._ps.parse(wire[start : start + READ_SIZE])  # pyright: ignore[reportPrivateUsage]
        await asyncio.wait_for(done.wait(), 2)

        assert got[0][:3] == ("rpc.plugin.detectObjects", "rpc.reply.p.1", payload)
        assert got[0][3] == {"x-chunked-transfer": "chunk", "x-chunk-id": "q"}
        assert got[1][2] == b"x"

    asyncio.run(run())


class FakeSocket:
    def __init__(self) -> None:
        self.buffered = 0
        self.closing = False

    def is_closing(self) -> bool:
        return self.closing

    def get_write_buffer_size(self) -> int:
        return self.buffered


class FakeWriter:
    def __init__(self) -> None:
        self.transport = FakeSocket()
        self.sent: list[list[bytes]] = []
        self.drains = 0

    def write(self, data: bytes) -> None:
        self.sent.append([data])

    def writelines(self, data: list[bytes]) -> None:
        self.sent.append(list(data))

    async def drain(self) -> None:
        self.drains += 1


class QueuedTransport(TcpTransport):
    def __init__(self) -> None:
        super().__init__()
        self.queued: list[bytes] = []
        self.sent: list[list[bytes]] = []

    def writelines(self, payload: list[bytes]) -> None:
        self.queued.extend(payload)

    async def drain(self) -> None:
        if self.queued:
            self.sent.append(self.queued)
            self.queued = []


async def connected(
    transport: TcpTransport | None = None, nc: patches.NatsConnection | None = None
) -> tuple[patches.NatsConnection, FakeWriter]:
    if nc is None:
        nc = patches.NatsConnection()
    writer = FakeWriter()
    tcp = TcpTransport() if transport is None else transport
    tcp._io_writer = writer  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage]
    tcp._io_reader = asyncio.StreamReader()  # pyright: ignore[reportPrivateUsage]
    nc._transport = tcp  # pyright: ignore[reportPrivateUsage]
    nc._status = NATSClient.CONNECTED  # pyright: ignore[reportPrivateUsage]
    nc._max_pending_size = 6 * 1024 * 1024  # pyright: ignore[reportPrivateUsage]
    nc._flush_queue = asyncio.Queue()  # pyright: ignore[reportPrivateUsage]
    nc._flusher_task = asyncio.create_task(nc._flusher())  # pyright: ignore[reportPrivateUsage]
    await asyncio.sleep(0)
    assert not nc._flusher_task.done()  # pyright: ignore[reportPrivateUsage]
    return nc, writer


async def stop(nc: patches.NatsConnection) -> None:
    task = nc._flusher_task  # pyright: ignore[reportPrivateUsage]
    assert task is not None
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def pub(subject: str, payload: bytes) -> bytes:
    return b"PUB %s  %d\r\n" % (subject.encode(), len(payload)) + payload + b"\r\n"


def test_a_write_through_publish_is_written_at_once() -> None:
    async def run() -> None:
        nc, writer = await connected()
        nc.write_through = True
        await nc.publish("a", b"1")
        assert writer.sent == [[pub("a", b"1")]] and writer.drains == 0
        assert nc._pending == [] and nc._pending_data_size == 0  # pyright: ignore[reportPrivateUsage]
        await stop(nc)

    asyncio.run(run())


def test_other_publishes_wait_for_the_flusher() -> None:
    async def run() -> None:
        nc, writer = await connected()
        for i in range(3):
            await nc.publish("a", b"%d" % i)
        assert writer.sent == []

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert writer.sent == [[pub("a", b"0"), pub("a", b"1"), pub("a", b"2")]] and writer.drains == 1
        await stop(nc)

    asyncio.run(run())


def test_a_write_through_publish_joins_a_flush_already_scheduled() -> None:
    async def run() -> None:
        nc, writer = await connected()
        await nc.publish("a", b"1")
        nc.write_through = True
        await nc.publish("a", b"2")
        assert writer.sent == []

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert writer.sent == [[pub("a", b"1"), pub("a", b"2")]]
        await stop(nc)

    asyncio.run(run())


def test_queued_commands_go_out_first_and_in_order() -> None:
    async def run() -> None:
        nc, writer = await connected()
        await nc._send_command(b"SUB x  1\r\n")  # pyright: ignore[reportPrivateUsage]
        nc.write_through = True
        await nc.publish("a", b"1")
        assert writer.sent == [[b"SUB x  1\r\n", pub("a", b"1")]]
        await stop(nc)

    asyncio.run(run())


@pytest.mark.parametrize("state", ["socket buffer filled", "socket closing", "reconnecting", "flusher dead"])
def test_a_write_through_publish_takes_the_stock_path_when_writing_is_unsafe(state: str) -> None:
    async def run() -> None:
        nc, writer = await connected()
        if state == "socket buffer filled":
            writer.transport.buffered = 1
        elif state == "socket closing":
            writer.transport.closing = True
        elif state == "reconnecting":
            nc._status = NATSClient.RECONNECTING  # pyright: ignore[reportPrivateUsage]
        else:
            await stop(nc)

        nc.write_through = True
        await nc.publish("a", b"1")

        if state == "flusher dead":
            # nats-py writes and drains inline itself when its flusher is gone
            assert writer.sent == [[pub("a", b"1")]] and writer.drains == 1
            return
        assert writer.sent == []
        if state == "reconnecting":
            assert nc._pending == [pub("a", b"1")]  # pyright: ignore[reportPrivateUsage]
        else:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert writer.sent == [[pub("a", b"1")]] and writer.drains == 1
        await stop(nc)

    asyncio.run(run())


def test_a_transport_that_sends_on_drain_stays_on_the_flusher() -> None:
    async def run() -> None:
        transport = QueuedTransport()
        nc, writer = await connected(transport)
        nc.write_through = True
        await nc.publish("a", b"1")
        assert transport.queued == [] and transport.sent == []

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert transport.sent == [[pub("a", b"1")]]
        assert writer.sent == []
        await stop(nc)

    asyncio.run(run())


def reply_subjects(batch: list[bytes]) -> list[str]:
    return [line.split(b" ", 2)[1].decode() for line in batch]


def test_a_reply_skips_the_flusher_only_while_no_further_call_waits() -> None:
    async def run() -> None:
        nc, writer = await connected()
        client, _ = make_client()
        client.nc = nc

        async def echo(value: Any) -> Any:
            return value

        await client.register_handler("ns", {"echo": echo})
        await asyncio.sleep(0)
        sid = next(sub._id for sub in nc._subs.values() if sub.subject == "rpc.ns.echo")  # pyright: ignore[reportPrivateUsage]
        drains = writer.drains

        async def call(*seqs: int) -> None:
            for seq in seqs:
                body = encode({"id": f"req.{seq}", "params": [seq]})
                await nc._process_msg(sid, b"rpc.ns.echo", b"", body, None)  # pyright: ignore[reportPrivateUsage]
            for _ in range(20):
                await asyncio.sleep(0)

        await call(1)
        assert reply_subjects(writer.sent[-1]) == ["rpc.reply.req.1"]
        assert writer.drains == drains

        # replies to queued calls leave in one write through the flusher
        await call(2, 3, 4)
        assert reply_subjects(writer.sent[-1]) == ["rpc.reply.req.2", "rpc.reply.req.3", "rpc.reply.req.4"]
        assert writer.drains == drains + 1
        assert nc.write_through is False
        await stop(nc)

    asyncio.run(run())


def test_connect_builds_a_patched_connection() -> None:
    async def run() -> None:
        opened: list[Any] = []

        async def fake_connect(self: NATSClient, **options: Any) -> None:
            opened.append((self, options))

        original = NATSClient.connect
        NATSClient.connect = fake_connect  # type: ignore[method-assign]
        try:
            nc = await patches.connect(servers=["nats://127.0.0.1:1"], name="t")
        finally:
            NATSClient.connect = original  # type: ignore[method-assign]

        assert type(nc) is patches.NatsConnection
        assert opened == [(nc, {"servers": ["nats://127.0.0.1:1"], "name": "t"})]

    asyncio.run(run())


def test_the_rpc_client_dials_a_patched_connection() -> None:
    # fails when client.py dials with nats.connect again, PayloadParser and direct replies then sit unused
    async def run() -> None:
        async def fake_connect(self: Any, *_args: Any, **_options: Any) -> None:
            await connected(nc=self)

        original = NATSClient.connect
        NATSClient.connect = fake_connect  # type: ignore[method-assign]
        try:
            client, _ = make_client()
            client.nc = None
            nc = await client.connect()
        finally:
            NATSClient.connect = original  # type: ignore[method-assign]

        assert type(nc) is patches.NatsConnection
        assert type(nc._ps) is patches.PayloadParser  # pyright: ignore[reportPrivateUsage]
        await stop(nc)

    asyncio.run(run())
