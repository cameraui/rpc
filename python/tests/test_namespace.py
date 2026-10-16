from __future__ import annotations

import asyncio
import io
import sys
from collections.abc import Callable
from typing import Any, cast

import pytest
from nats import errors
from test_drain import DrainNC, Gate, answers, until
from test_mux import make_client as make_mock_client
from test_register import PING, GatedNC, sub_line

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.codec import decode, encode

NOT_FOUND = "METHOD_NOT_FOUND"


def make_client(**options: Any) -> tuple[Any, DrainNC]:
    client, _ = make_mock_client(**options)
    nc = DrainNC()
    client.nc = nc
    return client, nc


async def serve(handlers: Any, **kwargs: Any) -> tuple[Any, DrainNC, Callable[[], Any]]:
    client, nc = make_client()
    cleanup = await client.register_handler("ns", handlers, namespace_subscription=True, **kwargs)
    return client, nc, cleanup


def wildcard(nc: DrainNC) -> Any:
    subs = [s for s in nc._subs.values() if s._subject == "rpc.ns.>"]
    assert len(subs) == 1
    return subs[0]


async def send(nc: DrainNC, subject: str, body: Any, headers: dict[str, str] | None = None) -> None:
    data = body if isinstance(body, bytes) else encode(body)
    hdr = b""
    if headers:
        hdr = ("NATS/1.0\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n").encode()
    await nc._process_msg(wildcard(nc)._id, subject.encode(), b"", data, hdr)


async def call(nc: DrainNC, method: str, seq: int, *params: Any) -> None:
    await send(nc, f"rpc.ns.{method}", {"id": f"req.{seq}", "method": "call", "params": list(params)})


def published(nc: DrainNC, subject: str) -> list[Any]:
    return [decode(p["data"]) for p in nc.published if p["subject"] == subject]


def test_one_subscription_serves_every_method_dotted_ones_too() -> None:
    async def run() -> None:
        async def find(q: str) -> str:
            return f"found {q}"

        client, nc, _ = await serve({"a": lambda x: x + 1, "db": {"find": find}})
        assert [s._subject for s in nc._subs.values()] == ["rpc.ns.>"]

        await call(nc, "a", 1, 41)
        await call(nc, "db.find", 2, "x")
        await until(lambda: len(answers(nc)) == 2)

        assert answers(nc) == {"req.1": 42, "req.2": "found x"}

    asyncio.run(run())


@pytest.mark.parametrize("namespace", ["reply", "cb", "cb.deep"])
def test_a_namespace_over_internal_subjects_is_refused(namespace: str) -> None:
    async def run() -> None:
        client, nc = make_client()
        with pytest.raises(ValueError, match="overlaps internal subjects"):
            await client.register_handler(namespace, {"a": lambda: 1}, namespace_subscription=True)
        assert nc._subs == {}

        await client.register_handler("replying", {"a": lambda: 1}, namespace_subscription=True)
        await client.register_handler(namespace, {"a": lambda: 1})

    asyncio.run(run())


def test_a_missing_method_gets_method_not_found_at_once_with_the_method_list() -> None:
    async def run() -> None:
        client, nc, _ = await serve({"a": lambda: 1, "b": lambda: 2})
        await call(nc, "nope", 1)
        await until(lambda: published(nc, "rpc.reply.req.1"))

        [reply] = published(nc, "rpc.reply.req.1")
        assert reply["error"]["code"] == NOT_FOUND
        assert reply["error"]["message"] == "Method not found: nope"
        assert reply["__methods"] == ["a", "b"]

    asyncio.run(run())


def test_a_stream_request_for_a_missing_method_fails_on_its_stream_subject() -> None:
    async def run() -> None:
        client, nc, _ = await serve({"a": lambda: 1})
        params = {"__stream": True, "__streamSubject": "stream.rpc.ns.gen.s1", "args": []}
        await send(nc, "rpc.ns.gen", {"id": "s1", "method": "stream", "params": params})
        await until(lambda: published(nc, "stream.rpc.ns.gen.s1"))

        [frame] = published(nc, "stream.rpc.ns.gen.s1")
        assert frame["type"] == "error"
        assert frame["error"]["code"] == NOT_FOUND
        assert published(nc, "rpc.reply.s1") == []

    asyncio.run(run())


def test_the_method_list_is_answered() -> None:
    async def run() -> None:
        client, nc, _ = await serve({"a": lambda: 1, "b": lambda: 2})
        await send(nc, "rpc.ns.__methods", {"id": "m1", "method": "call", "params": []})
        await until(lambda: published(nc, "rpc.reply.m1"))
        assert published(nc, "rpc.reply.m1") == [{"id": "m1", "result": ["a", "b"]}]

    asyncio.run(run())


def test_a_busy_method_neither_reorders_its_calls_nor_blocks_its_neighbours() -> None:
    async def run() -> None:
        gate = Gate()
        client, nc, _ = await serve({"slow": gate.work, "fast": lambda x: x})
        for seq in range(3):
            await call(nc, "slow", seq, seq)
        await call(nc, "fast", 9, "quick")
        await until(lambda: "req.9" in answers(nc))

        assert gate.started == [0]
        gate.open.set()
        await until(lambda: len(answers(nc)) == 4)
        assert gate.started == [0, 1, 2]
        assert answers(nc) == {"req.0": 0, "req.1": 1, "req.2": 2, "req.9": "quick"}

    asyncio.run(run())


def test_a_concurrency_limit_applies_inside_the_lane() -> None:
    async def run() -> None:
        running = 0
        peak = 0
        release = asyncio.Event()

        async def work(seq: int) -> int:
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await release.wait()
            running -= 1
            return seq

        client, nc, _ = await serve({"work": work}, concurrency={"work": 2})
        for seq in range(5):
            await call(nc, "work", seq, seq)
        await until(lambda: running == 2)
        await asyncio.sleep(0.01)
        assert peak == 2
        release.set()
        await until(lambda: len(answers(nc)) == 5)

    asyncio.run(run())


def test_a_chunked_request_reaches_the_method_its_header_names() -> None:
    async def run() -> None:
        async def size(data: bytes) -> int:
            return len(data)

        client, nc, _ = await serve({"size": size})
        body = encode({"id": "req.1", "method": "call", "params": [b"x" * 3000]})
        transfer = "t1"
        chunk = 1000
        total = -(-len(body) // chunk)
        header = {
            "type": "chunked",
            "transferId": transfer,
            "totalChunks": total,
            "totalSize": len(body),
            "chunkSize": chunk,
        }
        marks = {"x-chunked-transfer": "header", "x-chunk-id": transfer}
        await send(nc, "rpc.ns.size", encode(header), marks)
        for i in range(total):
            marks = {"x-chunked-transfer": "chunk", "x-chunk-id": transfer, "x-chunk-index": str(i)}
            await send(nc, "rpc.ns.size", body[i * chunk : (i + 1) * chunk], marks)
        await until(lambda: answers(nc))

        assert answers(nc) == {"req.1": 3000}

    asyncio.run(run())


def sized_request(request_id: str) -> bytes:
    return encode({"id": request_id, "method": "call", "params": [b"x" * 3000]})


async def open_transfer(nc: DrainNC, method: str, transfer: str, body: bytes, chunk: int = 1000) -> None:
    header = {
        "type": "chunked",
        "transferId": transfer,
        "totalChunks": -(-len(body) // chunk),
        "totalSize": len(body),
        "chunkSize": chunk,
    }
    await send(
        nc, f"rpc.ns.{method}", encode(header), {"x-chunked-transfer": "header", "x-chunk-id": transfer}
    )


async def finish_transfer(nc: DrainNC, method: str, transfer: str, body: bytes, chunk: int = 1000) -> None:
    for i in range(-(-len(body) // chunk)):
        marks = {"x-chunked-transfer": "chunk", "x-chunk-id": transfer, "x-chunk-index": str(i)}
        await send(nc, f"rpc.ns.{method}", body[i * chunk : (i + 1) * chunk], marks)


def namespace_scope(client: Any) -> Any:
    [entry] = [e for e in client._subscription_entries.values() if e["pattern"] == "rpc.ns.>"]
    return entry["chunks"]


def test_every_method_gets_its_own_transfer_limit() -> None:
    async def run() -> None:
        async def size(data: bytes) -> int:
            return len(data)

        client, nc, _ = await serve({"a": size, "b": size})
        for i in range(rpc_client.MAX_CONCURRENT_TRANSFERS):
            await open_transfer(nc, "a", f"a{i}", sized_request(f"req.a{i}"))
        over = sized_request("req.over")
        await open_transfer(nc, "a", "over", over)
        await finish_transfer(nc, "a", "over", over)
        neighbour = sized_request("req.b")
        await open_transfer(nc, "b", "b", neighbour)
        await finish_transfer(nc, "b", "b", neighbour)
        await until(lambda: "req.b" in answers(nc))
        await asyncio.sleep(0.01)

        assert answers(nc) == {"req.b": 3000}
        assert {method: len(m.assemblers) for method, m in namespace_scope(client).methods.items()} == {
            "a": rpc_client.MAX_CONCURRENT_TRANSFERS,
            "b": 0,
        }

    asyncio.run(run())


def test_transfers_to_missing_methods_share_one_scope_and_get_method_not_found() -> None:
    async def run() -> None:
        client, nc, _ = await serve({"a": lambda: 1})
        for i in range(rpc_client.MAX_CONCURRENT_TRANSFERS + 8):
            await open_transfer(nc, f"nope{i}", f"t{i}", sized_request(f"req.{i}"))
        scope = namespace_scope(client)
        assert scope.methods is None
        assert len(scope.manager.assemblers) == rpc_client.MAX_CONCURRENT_TRANSFERS

        await finish_transfer(nc, "nope0", "t0", sized_request("req.0"))
        await until(lambda: published(nc, "rpc.reply.req.0"))
        [reply] = published(nc, "rpc.reply.req.0")
        assert reply["error"]["code"] == NOT_FOUND
        assert reply["error"]["message"] == "Method not found: nope0"

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["cleanup", "disconnect", "suspend"])
def test_teardown_drains_the_lanes(teardown: str) -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=100)
        gate = Gate()
        cleanup = await client.register_handler(
            "ns", {"work": gate.work, "other": gate.work}, namespace_subscription=True
        )
        nc.commands.clear()
        await call(nc, "work", 0, 0)
        await call(nc, "work", 1, 1)
        await until(lambda: gate.started == [0])

        stop = asyncio.create_task(cleanup() if teardown == "cleanup" else getattr(client, teardown)())
        await until(lambda: PING in nc.commands)
        assert nc.commands[0] == f"UNSUB {wildcard(nc)._id} \r\n".encode()
        gate.open.set()
        await asyncio.wait_for(stop, 1)

        # the running call and the one behind it in the lane both finished
        assert gate.started == [0, 1]
        assert answers(nc) == {"req.0": 0, "req.1": 1}

    asyncio.run(run())


def test_past_the_deadline_lane_calls_get_connection_closed() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=30)
        gate = Gate()
        cleanup = await client.register_handler("ns", {"work": gate.work}, namespace_subscription=True)
        for seq in range(3):
            await call(nc, "work", seq, seq)
        await until(lambda: gate.started == [0])

        await asyncio.wait_for(cleanup(), 1)

        closed = {"code": "CONNECTION_CLOSED", "message": "Connection closed", "data": None}
        assert gate.cancelled == [0]
        assert answers(nc) == {"req.0": closed, "req.1": closed, "req.2": closed}

    asyncio.run(run())


def test_connect_restores_the_namespace_subscription(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        conns: list[GatedNC] = []

        async def fake_connect(**_options: Any) -> GatedNC:
            conns.append(GatedNC(open_gate=True))
            return conns[-1]

        monkeypatch.setattr(rpc_client, "connect", fake_connect)
        client, _ = make_mock_client()
        client.nc = None
        await client.connect()
        await client.register_handler("ns", {"a": lambda: 7}, namespace_subscription=True)
        await client.suspend()
        await client.connect()

        nc = conns[1]
        mux = f"rpc.reply.{client._reply_prefix}.>"
        assert nc.commands == [sub_line(nc, mux), sub_line(nc, "rpc.ns.>"), PING]
        sid = next(s._id for s in nc._subs.values() if s._subject == "rpc.ns.>")
        body = encode({"id": "req.1", "method": "call", "params": []})
        await nc._process_msg(sid, b"rpc.ns.a", b"", body, b"")
        await until(lambda: answers(nc))
        assert answers(nc) == {"req.1": 7}

    asyncio.run(run())


def test_a_full_lane_drops_and_reports_a_slow_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rpc_client, "DEFAULT_SUB_PENDING_MSGS_LIMIT", 2)

    async def run() -> None:
        reported: list[Exception] = []

        async def on_error(error: Exception) -> None:
            reported.append(error)

        client, nc = make_client(error_cb=on_error)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work}, namespace_subscription=True)
        await call(nc, "work", 0, 0)
        await until(lambda: gate.started == [0])
        for seq in range(1, 4):
            await call(nc, "work", seq, seq)
        await until(lambda: reported)

        assert isinstance(reported[0], errors.SlowConsumerError)
        assert reported[0].subject == "rpc.ns.work"
        gate.open.set()
        # the first call was running, two waited, the fourth was dropped
        await until(lambda: len(answers(nc)) == 3)
        await asyncio.sleep(0.01)
        assert sorted(answers(nc)) == ["req.0", "req.1", "req.2"]

    asyncio.run(run())


def test_a_lane_also_drops_once_its_bytes_are_full(monkeypatch: pytest.MonkeyPatch) -> None:
    size = len(encode({"id": "req.1", "method": "call", "params": [1]}))
    monkeypatch.setattr(rpc_client, "DEFAULT_SUB_PENDING_BYTES_LIMIT", 2 * size)

    async def run() -> None:
        reported: list[Exception] = []

        async def on_error(error: Exception) -> None:
            reported.append(error)

        client, nc = make_client(error_cb=on_error)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work}, namespace_subscription=True)
        await call(nc, "work", 0, 0)
        await until(lambda: gate.started == [0])
        for seq in range(1, 4):
            await call(nc, "work", seq, seq)
        await until(lambda: reported)

        assert isinstance(reported[0], errors.SlowConsumerError)
        gate.open.set()
        # two waiting calls fill the bytes, the fourth was dropped
        await until(lambda: len(answers(nc)) == 3)
        await asyncio.sleep(0.01)
        assert sorted(answers(nc)) == ["req.0", "req.1", "req.2"]

    asyncio.run(run())


def test_slow_consumer_drops_are_logged_with_their_subject(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, _ = make_client()
        err = io.StringIO()
        monkeypatch.setattr(sys, "stderr", err)
        for _ in range(5):
            await client._error_cb(errors.SlowConsumerError("rpc.ns.work", "", 3, cast(Any, None)))
        await client._error_cb(errors.SlowConsumerError("rpc.ns.other", "", 4, cast(Any, None)))

        # once per subject and second, the drops in between are counted
        assert err.getvalue().splitlines() == [
            "rpc: slow consumer on rpc.ns.work, dropped 1 message(s)",
            "rpc: slow consumer on rpc.ns.other, dropped 1 message(s)",
        ]
        assert client._slow_consumer_log["rpc.ns.work"][1] == 4

    asyncio.run(run())


def test_without_the_option_every_method_keeps_its_subscription() -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.register_handler("ns", {"a": lambda: 1, "b": lambda: 2})
        subjects = sorted(s._subject for s in nc._subs.values())
        assert subjects == ["rpc.ns.__methods", "rpc.ns.a", "rpc.ns.b"]

    asyncio.run(run())


def test_the_fallback_without_inline_delivery_serves_calls_too(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rpc_client, "INLINE_REPLY_DELIVERY", False)

    async def run() -> None:
        client, nc, _ = await serve({"a": lambda x: x * 2})
        await call(nc, "a", 1, 21)
        await call(nc, "missing", 2)
        await until(lambda: len(answers(nc)) == 2)
        assert answers(nc)["req.1"] == 42
        assert answers(nc)["req.2"]["code"] == NOT_FOUND

    asyncio.run(run())
