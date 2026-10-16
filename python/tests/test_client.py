from __future__ import annotations

import asyncio
import contextlib
import math
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
from nats import errors
from test_drain import until as until_timeout
from test_mux import MockNC, MockSub, make_client, make_msg, mux_sub, no_responder_headers, until

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.chunking import MAX_CHUNK_SIZE, MAX_CONCURRENT_TRANSFERS, ChunkingManager
from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.errors import RPCException


def subs_for(nc: MockNC, pattern: str) -> list[MockSub]:
    return [s for s in nc.subs if s.pattern == pattern]


def sub_chunks(client: Any, pattern: str) -> ChunkingManager | None:
    entry = next(e for e in client._subscription_entries.values() if e["pattern"] == pattern)
    return entry["chunks"].manager


def chunk_header(transfer_id: str, total_size: int, chunk_size: int) -> tuple[bytes, dict[str, str]]:
    header = {
        "type": "chunked",
        "transferId": transfer_id,
        "totalChunks": -(-total_size // chunk_size),
        "totalSize": total_size,
        "chunkSize": chunk_size,
    }
    return encode(header), {"x-chunked-transfer": "header", "x-chunk-id": transfer_id}


def chunk_headers(transfer_id: str, index: int) -> dict[str, str]:
    return {"x-chunked-transfer": "chunk", "x-chunk-id": transfer_id, "x-chunk-index": str(index)}


async def flood(sub: MockSub, count: int) -> None:
    for i in range(count):
        await sub.cb(make_msg(sub.pattern, *chunk_header(f"flood{i}", MAX_CHUNK_SIZE, MAX_CHUNK_SIZE)))


async def deliver(sub: MockSub, published: list[dict[str, Any]]) -> None:
    for pub in published:
        await sub.cb(make_msg(pub["subject"], pub["data"], headers=pub["headers"]))


def test_chunked_publish_leaves_caller_headers_untouched() -> None:
    async def run() -> None:
        client, nc = make_client()
        client._max_payload_size = 1024  # pyright: ignore[reportPrivateUsage]
        headers = {"x-app": "1"}

        await client.publish("test.big", b"\x01" * 5000, headers=headers)
        await client.publish("test.small", {"small": 1}, headers=headers)

        assert headers == {"x-app": "1"}
        assert nc.published[-1]["headers"] == {"x-app": "1"}
        assert nc.published[0]["headers"]["x-app"] == "1"
        assert nc.published[0]["headers"]["x-chunked-transfer"] == "header"

    asyncio.run(run())


def test_chunked_publish_reassembles_on_a_subscriber() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 1000  # pyright: ignore[reportPrivateUsage]
        value = {"blob": bytes(range(256)) * 40, "n": 7}
        await sender.publish("test.big", value)
        assert len(sender_nc.published) > 3

        receiver, receiver_nc = make_client()
        got: list[Any] = []
        await receiver.subscribe("test.big", got.append)
        await deliver(subs_for(receiver_nc, "test.big")[0], sender_nc.published)

        assert len(got) == 1
        assert bytes(got[0]["blob"]) == value["blob"] and got[0]["n"] == 7
        manager = sub_chunks(receiver, "test.big")
        assert manager is not None and manager.assemblers == {}
        assert manager.budget.used == 0

    asyncio.run(run())


def test_overlapping_subscriptions_each_reassemble_their_copy() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.overlap", {"payload": "y" * 3000})

        receiver, receiver_nc = make_client()
        got_a: list[Any] = []
        got_b: list[Any] = []
        await receiver.subscribe("test.overlap", got_a.append)
        await receiver.subscribe("test.overlap", got_b.append)
        sub_a, sub_b = subs_for(receiver_nc, "test.overlap")

        # nats delivers each message to both subscriptions in turn
        for pub in sender_nc.published:
            msg_args = (pub["subject"], pub["data"])
            await sub_a.cb(make_msg(*msg_args, headers=pub["headers"]))
            await sub_b.cb(make_msg(*msg_args, headers=pub["headers"]))

        assert got_a == [{"payload": "y" * 3000}]
        assert got_b == [{"payload": "y" * 3000}]

    asyncio.run(run())


def test_subscription_drops_chunk_with_garbage_index() -> None:
    async def run() -> None:
        client, nc = make_client()
        errors: list[str] = []
        got: list[Any] = []
        await client.subscribe("test.bad", got.append)
        sub = subs_for(nc, "test.bad")[0]
        header = {"type": "chunked", "transferId": "t1", "totalChunks": 2, "totalSize": 20, "chunkSize": 10}
        await sub.cb(
            make_msg("test.bad", encode(header), {"x-chunked-transfer": "header", "x-chunk-id": "t1"})
        )
        manager = sub_chunks(client, "test.bad")
        assert manager is not None and len(manager.assemblers) == 1

        original = manager.error_callbacks
        for key, cb in list(original.items()):
            original[key] = lambda e, cb=cb: (errors.append(str(e)), cb(e))
        bad_index = {"x-chunked-transfer": "chunk", "x-chunk-id": "t1", "x-chunk-index": "abc"}
        await sub.cb(make_msg("test.bad", b"\x00" * 10, bad_index))

        assert got == []
        assert manager.assemblers == {}
        assert len(errors) == 1

    asyncio.run(run())


def test_reply_mux_rejects_oversized_chunk_header() -> None:
    async def run() -> None:
        client, nc = make_client(max_transfer_size=1000)
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        header = {
            "type": "chunked",
            "transferId": "t1",
            "totalChunks": 2,
            "totalSize": 2000,
            "chunkSize": 1000,
        }

        await mux_sub(client, nc).cb(
            make_msg(
                f"rpc.reply.{req['id']}", encode(header), {"x-chunked-transfer": "header", "x-chunk-id": "t1"}
            )
        )

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]
        # well inside the 30 s call timeout
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"
        assert 'Chunked reply to "rpc.test.big" failed' in caught.value.message
        assert "invalid totalSize" in caught.value.message
        assert client.pending_requests == {}

    asyncio.run(run())


def test_queue_members_on_one_connection_share_a_transfer() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.queue", {"payload": "q" * 3000})
        header, *chunks = sender_nc.published
        assert len(chunks) > 2

        receiver, receiver_nc = make_client()
        got: list[Any] = []
        await receiver.subscribe("test.queue", got.append, queue="workers")
        await receiver.subscribe("test.queue", got.append, queue="workers")
        sub_a, sub_b = subs_for(receiver_nc, "test.queue")

        # the server spreads one transfer over both members
        await deliver(sub_a, [header])
        await deliver(sub_b, chunks[:-1])
        await deliver(sub_a, chunks[-1:])

        assert got == [{"payload": "q" * 3000}]
        manager = sub_chunks(receiver, "test.queue")
        assert manager is not None and manager.assemblers == {}

    asyncio.run(run())


def test_subscription_ignores_a_header_that_is_not_a_map(capsys: pytest.CaptureFixture[str]) -> None:
    async def run() -> None:
        client, nc = make_client()
        got: list[Any] = []
        await client.subscribe("test.bad", got.append)
        sub = subs_for(nc, "test.bad")[0]

        await sub.cb(
            make_msg("test.bad", encode(["t1", 2]), {"x-chunked-transfer": "header", "x-chunk-id": "t1"})
        )

        assert got == []
        assert sub_chunks(client, "test.bad") is None
        assert capsys.readouterr().out == ""

    asyncio.run(run())


def test_reply_mux_drops_a_transfer_on_a_garbage_chunk_index() -> None:
    async def run() -> None:
        client, nc = make_client()
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        subject = f"rpc.reply.{decode(nc.published[0]['data'])['id']}"
        mux = mux_sub(client, nc)
        header = {"type": "chunked", "transferId": "t1", "totalChunks": 2, "totalSize": 20, "chunkSize": 10}
        await mux.cb(make_msg(subject, encode(header), {"x-chunked-transfer": "header", "x-chunk-id": "t1"}))
        call_id = subject.removeprefix("rpc.reply.")
        assert list(client._mux_chunking.assemblers) == [f"{call_id}|t1"]  # pyright: ignore[reportPrivateUsage]

        bad_index = {"x-chunked-transfer": "chunk", "x-chunk-id": "t1", "x-chunk-index": "abc"}
        await mux.cb(make_msg(subject, b"\x00" * 10, bad_index))

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]
        # well inside the 30 s call timeout
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"

    asyncio.run(run())


def test_subscription_drops_a_transfer_on_a_repeated_chunk_with_the_wrong_length(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.dup", {"payload": "d" * 3000})
        header, first, *rest = sender_nc.published

        client, nc = make_client()
        got: list[Any] = []
        await client.subscribe("test.dup", got.append)
        sub = subs_for(nc, "test.dup")[0]
        await deliver(sub, [header, first])
        await sub.cb(make_msg("test.dup", bytes(first["data"])[:-1], first["headers"]))

        manager = sub_chunks(client, "test.dup")
        assert manager is not None and manager.assemblers == {}
        assert manager.budget.used == 0
        await deliver(sub, [first, *rest])

        assert got == []
        assert len(capsys.readouterr().out.strip().splitlines()) == 1

    asyncio.run(run())


def test_reply_mux_rejects_a_call_on_a_repeated_chunk_with_the_wrong_length() -> None:
    async def run() -> None:
        client, nc = make_client()
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        subject = f"rpc.reply.{req['id']}"
        msgs = chunked_reply(subject, "t1", encode({"id": req["id"], "result": "x" * 300}))
        mux = mux_sub(client, nc)
        timer = client.pending_requests[req["id"]]["timeout"]

        await mux.cb(msgs[0])
        await mux.cb(msgs[1])
        await mux.cb(make_msg(subject, bytes(msgs[1].data)[:-1], msgs[1].headers))

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]
        assert client._chunk_budget.used == 0  # pyright: ignore[reportPrivateUsage]
        assert client.pending_requests == {}
        assert timer.cancelled()
        # well inside the 30 s call timeout
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"
        assert 'Chunked reply to "rpc.test.big" failed' in caught.value.message

    asyncio.run(run())


BAD_CHUNK_CASES = [
    "index x",
    "index +1",
    "index -1",
    "index past the end",
    "short",
    "long",
    "short last",
    "long last",
    "repeated with the wrong length",
]


def bad_chunks(case: str, payload: bytes, chunk_size: int) -> list[tuple[str, bytes]]:
    total = -(-len(payload) // chunk_size)
    last_index = str(total - 1)
    first = payload[:chunk_size]
    last = payload[(total - 1) * chunk_size :]
    return {
        "index x": [("x", first)],
        "index +1": [("+1", payload[chunk_size : 2 * chunk_size])],
        "index -1": [("-1", first)],
        "index past the end": [(str(total), first)],
        "short": [("0", first[:-1])],
        "long": [("0", first + b"\x00")],
        "short last": [(last_index, last[:-1])],
        "long last": [(last_index, last + b"\x00")],
        "repeated with the wrong length": [("0", first), ("0", first[:-1])],
    }[case]


def raw_chunk(transfer_id: str, index: str) -> dict[str, str]:
    return {"x-chunked-transfer": "chunk", "x-chunk-id": transfer_id, "x-chunk-index": index}


@pytest.mark.parametrize("case", BAD_CHUNK_CASES)
def test_subscription_drops_the_transfer_on_a_bad_chunk(
    case: str, capsys: pytest.CaptureFixture[str]
) -> None:
    async def run() -> None:
        client, nc = make_client()
        got: list[Any] = []
        await client.subscribe("test.bad", got.append)
        sub = subs_for(nc, "test.bad")[0]
        payload = encode({"v": "y" * 30})
        await sub.cb(make_msg("test.bad", *chunk_header("t1", len(payload), 10)))

        for index, data in bad_chunks(case, payload, 10):
            await sub.cb(make_msg("test.bad", data, raw_chunk("t1", index)))

        manager = sub_chunks(client, "test.bad")
        assert manager is not None and manager.assemblers == {}
        assert manager.budget.used == 0
        for i in range(0, len(payload), 10):
            await sub.cb(make_msg("test.bad", payload[i : i + 10], chunk_headers("t1", i // 10)))
        assert got == []
        assert len(capsys.readouterr().out.strip().splitlines()) == 1

    asyncio.run(run())


@pytest.mark.parametrize("case", BAD_CHUNK_CASES)
def test_reply_mux_rejects_the_call_on_a_bad_chunk(case: str) -> None:
    async def run() -> None:
        client, nc = make_client()
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        subject = f"rpc.reply.{req['id']}"
        payload = encode({"id": req["id"], "result": "x" * 30})
        mux = mux_sub(client, nc)
        await mux.cb(make_msg(subject, *chunk_header("t1", len(payload), 10)))

        for index, data in bad_chunks(case, payload, 10):
            await mux.cb(make_msg(subject, data, raw_chunk("t1", index)))

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]
        assert client._chunk_budget.used == 0  # pyright: ignore[reportPrivateUsage]
        # well inside the 30 s call timeout
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"
        assert 'Chunked reply to "rpc.test.big" failed' in caught.value.message

    asyncio.run(run())


def test_reply_mux_rejects_the_call_on_a_reply_that_does_not_decode() -> None:
    async def run() -> None:
        client, nc = make_client()
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        subject = f"rpc.reply.{decode(nc.published[0]['data'])['id']}"
        # 0xc1 is never used in msgpack
        for msg in chunked_reply(subject, "t1", b"\xc1" * 25, chunk_size=10):
            await mux_sub(client, nc).cb(msg)

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"

    asyncio.run(run())


def chunked_reply(subject: str, transfer_id: str, encoded: bytes, chunk_size: int = 100) -> list[Any]:
    msgs = [make_msg(subject, *chunk_header(transfer_id, len(encoded), chunk_size))]
    for index, start in enumerate(range(0, len(encoded), chunk_size)):
        msgs.append(make_msg(subject, encoded[start : start + chunk_size], chunk_headers(transfer_id, index)))
    return msgs


def test_reply_mux_assembles_more_chunked_replies_than_the_subscription_cap() -> None:
    async def run() -> None:
        client, nc = make_client()
        calls = [asyncio.create_task(client.call("rpc.test.big", i)) for i in range(40)]
        await until(lambda: len(nc.published) == 40)
        mux = mux_sub(client, nc)
        replies = []
        for i, pub in enumerate(nc.published):
            req = decode(pub["data"])
            encoded = encode({"id": req["id"], "result": f"r{i}" * 100})
            replies.append(chunked_reply(f"rpc.reply.{req['id']}", f"t{i}", encoded))

        # every header first, so all 40 transfers are open at once
        for header, *_ in replies:
            await mux.cb(header)
        for _, *chunks in replies:
            for chunk in chunks:
                await mux.cb(chunk)

        assert await asyncio.gather(*calls) == [f"r{i}" * 100 for i in range(40)]
        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_reply_mux_ignores_a_transfer_without_a_pending_call() -> None:
    async def run() -> None:
        client, nc = make_client()
        await client._ensure_mux_subscription()  # pyright: ignore[reportPrivateUsage]
        encoded = encode({"id": "nobody.1", "result": "x" * 300})

        for msg in chunked_reply("rpc.reply.nobody.1", "t1", encoded)[:2]:
            await mux_sub(client, nc).cb(msg)

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_reply_mux_keeps_one_transfer_per_call() -> None:
    async def run() -> None:
        client, nc = make_client()
        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        subject = f"rpc.reply.{req['id']}"
        mux = mux_sub(client, nc)
        first = chunked_reply(subject, "t1", encode({"id": req["id"], "result": "a" * 300}))
        second = chunked_reply(subject, "t2", encode({"id": req["id"], "result": "b" * 300}))

        await mux.cb(first[0])
        await mux.cb(first[1])
        await mux.cb(second[0])
        assert list(client._mux_chunking.assemblers) == [f"{req['id']}|t2"]  # pyright: ignore[reportPrivateUsage]

        for chunk in first[2:] + second[1:]:
            await mux.cb(chunk)
        assert await task == "b" * 300

    asyncio.run(run())


def test_reply_mux_drops_a_half_assembled_reply_on_timeout() -> None:
    async def run() -> None:
        client, nc = make_client(timeout=20)
        task = asyncio.create_task(client.call("rpc.test.slow"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        msgs = chunked_reply(f"rpc.reply.{req['id']}", "t1", encode({"id": req["id"], "result": "x" * 300}))
        await mux_sub(client, nc).cb(msgs[0])
        await mux_sub(client, nc).cb(msgs[1])
        assert len(client._mux_chunking.assemblers) == 1  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(errors.TimeoutError):
            await task

        assert client._mux_chunking.assemblers == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["disconnect", "suspend"])
def test_teardown_stops_the_chunk_timer_and_keeps_the_size_limit(teardown: str) -> None:
    async def run() -> None:
        client, nc = make_client(max_transfer_size=1000)
        await client.subscribe("test.big", lambda d: None)
        await subs_for(nc, "test.big")[0].cb(make_msg("test.big", *chunk_header("t1", 20, 10)))
        await subs_for(nc, "test.big")[0].cb(make_msg("test.big", b"\x00" * 10, chunk_headers("t1", 0)))
        manager = sub_chunks(client, "test.big")
        assert manager is not None
        timer = manager._timer  # pyright: ignore[reportPrivateUsage]
        assert timer is not None
        mux = client._mux_chunking  # pyright: ignore[reportPrivateUsage]
        mux.start_receiving("c|t1", 2, lambda d: None, lambda e: None, 20, 10)
        mux_timer = mux._timer  # pyright: ignore[reportPrivateUsage]
        assert mux_timer is not None
        budget = manager.budget
        assert budget.used == 20

        await getattr(client, teardown)()

        assert timer.cancelled()
        assert manager.assemblers == {}
        assert budget.used == 0
        assert client.chunking_manager.max_transfer_size == 1000
        assert mux_timer.cancelled()
        assert mux.assemblers == {}
        assert client._mux_chunking is mux  # pyright: ignore[reportPrivateUsage]
        assert mux.max_transfer_size == 1000
        assert mux.max_transfers == math.inf
        assert mux.budget is budget and budget.limit == 1000

    asyncio.run(run())


def test_suspend_keeps_the_subscription_scope_and_starts_a_fresh_manager() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.big", {"payload": "s" * 3000})

        client, nc = make_client()
        got: list[Any] = []
        await client.subscribe("test.big", got.append)
        await deliver(subs_for(nc, "test.big")[0], sender_nc.published[:2])
        before = sub_chunks(client, "test.big")
        assert before is not None and len(before.assemblers) == 1

        await client.suspend()
        assert sub_chunks(client, "test.big") is None
        client.nc = cast(Any, nc)
        await client._nats_subscribe(  # pyright: ignore[reportPrivateUsage]
            next(e for e in client._subscription_entries.values() if e["pattern"] == "test.big")  # pyright: ignore[reportPrivateUsage]
        )
        await deliver(subs_for(nc, "test.big")[-1], sender_nc.published)

        assert got == [{"payload": "s" * 3000}]
        after = sub_chunks(client, "test.big")
        assert after is not None and after is not before

    asyncio.run(run())


def test_each_subscription_has_its_own_transfer_cap() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.big", {"payload": "c" * 3000})

        client, nc = make_client()
        got: list[Any] = []
        await client.subscribe("test.flood", lambda d: None)
        await client.subscribe("test.big", got.append)
        await flood(subs_for(nc, "test.flood")[0], 40)
        flooded = sub_chunks(client, "test.flood")
        assert flooded is not None and len(flooded.assemblers) == MAX_CONCURRENT_TRANSFERS

        await deliver(subs_for(nc, "test.big")[0], sender_nc.published)

        assert got == [{"payload": "c" * 3000}]
        assert flooded.budget.used == 0

    asyncio.run(run())


def test_service_call_reply_is_not_capped_by_handler_traffic() -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.subscribe("test.flood", lambda d: None)
        await flood(subs_for(nc, "test.flood")[0], MAX_CONCURRENT_TRANSFERS)

        task = asyncio.create_task(client.call("svc.echo"))
        await until(lambda: any(p["subject"] == "svc.echo" for p in nc.published))
        request = next(p for p in nc.published if p["subject"] == "svc.echo")
        reply_subject = f"svc.echo.reply.{decode(request['data'])['id']}"

        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish(reply_subject, {"id": decode(request["data"])["id"], "result": "r" * 3000})
        await deliver(subs_for(nc, reply_subject)[0], sender_nc.published)

        assert await asyncio.wait_for(task, 1) == "r" * 3000

    asyncio.run(run())


def test_one_budget_spans_all_subscriptions_of_a_client() -> None:
    async def run() -> None:
        client, nc = make_client(max_transfer_size=100)
        errors: list[str] = []
        await client.subscribe("test.a", lambda d: None)
        await client.subscribe("test.b", lambda d: None)
        sub_a = subs_for(nc, "test.a")[0]
        sub_b = subs_for(nc, "test.b")[0]
        await sub_a.cb(make_msg("test.a", *chunk_header("t1", 60, 10)))
        await sub_a.cb(make_msg("test.a", b"\x00" * 10, chunk_headers("t1", 0)))
        await sub_b.cb(make_msg("test.b", *chunk_header("t1", 60, 10)))
        manager_b = sub_chunks(client, "test.b")
        assert manager_b is not None
        manager_b.error_callbacks["t1"] = lambda e: errors.append(str(e))

        await sub_b.cb(make_msg("test.b", b"\x00" * 10, chunk_headers("t1", 0)))

        assert len(errors) == 1 and "more than 100 bytes" in errors[0]
        assert manager_b.assemblers == {}
        assert manager_b.budget.used == 60

    asyncio.run(run())


def test_unsubscribe_drops_open_transfers_and_returns_their_bytes() -> None:
    async def run() -> None:
        client, nc = make_client()
        unsubscribe = await client.subscribe("test.a", lambda d: None)
        sub = subs_for(nc, "test.a")[0]
        await sub.cb(make_msg("test.a", *chunk_header("t1", 60, 10)))
        await sub.cb(make_msg("test.a", b"\x00" * 10, chunk_headers("t1", 0)))
        manager = sub_chunks(client, "test.a")
        assert manager is not None and manager.budget.used == 60
        timer = manager._timer  # pyright: ignore[reportPrivateUsage]
        assert timer is not None

        await unsubscribe()

        assert manager.assemblers == {}
        assert manager.budget.used == 0
        assert timer.cancelled()
        assert client._chunk_scopes == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_queue_scope_lives_until_its_last_member_leaves() -> None:
    async def run() -> None:
        client, nc = make_client()
        leave_a = await client.subscribe("test.q", lambda d: None, queue="workers")
        leave_b = await client.subscribe("test.q", lambda d: None, queue="workers")
        sub_a = subs_for(nc, "test.q")[0]
        await sub_a.cb(make_msg("test.q", *chunk_header("t1", 60, 10)))
        await sub_a.cb(make_msg("test.q", b"\x00" * 10, chunk_headers("t1", 0)))
        manager = sub_chunks(client, "test.q")
        assert manager is not None and manager.budget.used == 60

        await leave_a()
        assert list(manager.assemblers) == ["t1"]
        await leave_b()
        assert manager.assemblers == {}
        assert manager.budget.used == 0
        assert client._chunk_scopes == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_a_subscription_nats_ends_leaves_its_chunk_scope() -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = SerialNC()
        client.nc = cast(Any, nc)
        await client.subscribe("test.a", lambda d: None)
        sub = subs_for(nc, "test.a")[0]
        assert isinstance(sub, SerialSub)
        await sub.cb(make_msg("test.a", *chunk_header("t1", 60, 10)))
        await sub.cb(make_msg("test.a", b"\x00" * 10, chunk_headers("t1", 0)))
        manager = sub_chunks(client, "test.a")
        assert manager is not None and manager.budget.used == 60

        # a subscription task that ends while its connection lives, like an auto-unsubscribe
        sub._wait_for_msgs_task.cancel()

        await until(lambda: client._chunk_scopes == {})  # pyright: ignore[reportPrivateUsage]
        assert manager.assemblers == {}
        assert manager.budget.used == 0

    asyncio.run(run())


def test_disconnect_forgets_every_chunk_scope() -> None:
    async def run() -> None:
        client, _ = make_client()
        await client.subscribe("test.a", lambda d: None)
        await client.subscribe("test.q", lambda d: None, queue="w")
        assert len(client._chunk_scopes) == 2  # pyright: ignore[reportPrivateUsage]

        await client.disconnect()

        assert client._chunk_scopes == {}  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_a_stale_unsubscribe_keeps_the_scope_of_a_new_queue_member() -> None:
    async def run() -> None:
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("test.q", {"payload": "n" * 3000})
        client, _ = make_client()
        leave_old = await client.subscribe("test.q", lambda d: None, queue="w")
        await client.disconnect()
        nc = MockNC()
        client.nc = cast(Any, nc)
        got: list[Any] = []
        await client.subscribe("test.q", got.append, queue="w")
        [scope] = client._chunk_scopes.values()  # pyright: ignore[reportPrivateUsage]

        await leave_old()

        assert list(client._chunk_scopes.values()) == [scope]  # pyright: ignore[reportPrivateUsage]
        await deliver(subs_for(nc, "test.q")[0], sender_nc.published)
        assert got == [{"payload": "n" * 3000}]

    asyncio.run(run())


def test_queue_members_of_different_subjects_have_their_own_cap() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def idle(value: Any) -> None:
            pass

        async def echo(value: Any) -> int:
            return len(value)

        await client.register_handler("ns", {"a": idle, "b": echo}, queue="workers")
        await flood(subs_for(nc, "rpc.ns.a")[0], 40)
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("rpc.ns.b", {"id": "req.1", "method": "call", "params": ["w" * 3000]})

        await deliver(subs_for(nc, "rpc.ns.b")[0], sender_nc.published)

        await until(lambda: "rpc.reply.req.1" in replies(nc))
        assert replies(nc)["rpc.reply.req.1"]["result"] == 3000
        assert sub_chunks(client, "rpc.ns.a") is not sub_chunks(client, "rpc.ns.b")

    asyncio.run(run())


def test_a_method_subscription_keeps_no_method_table() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def echo(value: Any) -> int:
            return len(value)

        await client.register_handler("ns", {"b": echo})
        sender, sender_nc = make_client()
        sender._max_payload_size = 500  # pyright: ignore[reportPrivateUsage]
        await sender.publish("rpc.ns.b", {"id": "req.1", "method": "call", "params": ["w" * 3000]})

        await deliver(subs_for(nc, "rpc.ns.b")[0], sender_nc.published)

        await until(lambda: "rpc.reply.req.1" in replies(nc))
        entry = next(e for e in client._subscription_entries.values() if e["pattern"] == "rpc.ns.b")
        assert entry["chunks"].manager is not None
        assert entry["chunks"].methods is None

    asyncio.run(run())


def test_reply_mux_shares_the_client_budget() -> None:
    async def run() -> None:
        client, nc = make_client(max_transfer_size=100)
        await client.subscribe("test.a", lambda d: None)
        sub = subs_for(nc, "test.a")[0]
        await sub.cb(make_msg("test.a", *chunk_header("t1", 60, 10)))
        await sub.cb(make_msg("test.a", b"\x00" * 10, chunk_headers("t1", 0)))

        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: any(p["subject"] == "rpc.test.big" for p in nc.published))
        request = next(p for p in nc.published if p["subject"] == "rpc.test.big")
        subject = f"rpc.reply.{decode(request['data'])['id']}"
        mux = mux_sub(client, nc)
        await mux.cb(make_msg(subject, *chunk_header("r1", 60, 10)))
        await mux.cb(make_msg(subject, b"\x00" * 10, chunk_headers("r1", 0)))

        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "INTERNAL_ERROR"
        assert "more than 100 bytes" in caught.value.message

    asyncio.run(run())


def retrying_client() -> tuple[Any, MockNC]:
    return make_client(no_responder_retry={"max_retries": 3, "delays": [0.01]}, timeout=500)


def requests_to(nc: MockNC, subject: str) -> list[dict[str, Any]]:
    return [p for p in nc.published if p["subject"] == subject]


async def answer(client: Any, nc: MockNC, pub: dict[str, Any], body: dict[str, Any]) -> None:
    req = decode(pub["data"])
    await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req['id']}", encode({"id": req["id"], **body})))


@pytest.mark.parametrize(
    "error",
    [
        {"code": "NOT_FOUND", "message": "downstream call failed: no responders"},
        {"code": "503", "message": "service unavailable"},
        {"code": "INTERNAL_ERROR", "message": "No responders available for subject"},
    ],
)
def test_call_does_not_retry_a_remote_error(error: dict[str, str]) -> None:
    async def run() -> None:
        client, nc = retrying_client()
        task = asyncio.create_task(client.call("rpc.remote.fail"))
        await until(lambda: len(nc.published) == 1)

        await answer(client, nc, nc.published[0], {"error": error})

        with pytest.raises(RPCException) as exc_info:
            await asyncio.wait_for(task, 2)
        assert exc_info.value.code == error["code"]
        assert exc_info.value.message == error["message"]
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.remote.fail")) == 1

    asyncio.run(run())


async def reply_to(client: Any, nc: MockNC, subject: str, body: dict[str, Any]) -> None:
    req_id = decode(requests_to(nc, subject)[0]["data"])["id"]
    encoded = encode({"id": req_id, **body})
    if subject.startswith("rpc."):
        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req_id}", encoded))
    else:
        reply_subject = f"{subject}.reply.{req_id}"
        await subs_for(nc, reply_subject)[0].cb(make_msg(reply_subject, encoded))


@pytest.mark.parametrize("subject", ["rpc.test.bad", "svc.test.bad"])
@pytest.mark.parametrize(
    ("error", "code"),
    [("boom", "INTERNAL_ERROR"), ({"code": "X"}, "X"), ({}, "INTERNAL_ERROR")],
)
def test_a_reply_with_a_malformed_error_rejects_the_call(subject: str, error: Any, code: str) -> None:
    async def run() -> None:
        client, nc = make_client(timeout=30000)
        task = asyncio.create_task(client.call(subject))
        await until(lambda: len(requests_to(nc, subject)) == 1)

        await reply_to(client, nc, subject, {"error": error})

        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == code
        assert client.pending_requests == {}

    asyncio.run(run())


@pytest.mark.parametrize("subject", ["rpc.test.bad", "svc.test.bad"])
def test_a_reply_with_a_null_error_resolves_the_call(subject: str) -> None:
    async def run() -> None:
        client, nc = make_client(timeout=30000)
        task = asyncio.create_task(client.call(subject))
        await until(lambda: len(requests_to(nc, subject)) == 1)

        await reply_to(client, nc, subject, {"error": None, "result": 7})

        assert await asyncio.wait_for(task, 1) == 7

    asyncio.run(run())


def test_call_retries_a_local_no_responder_status() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        task = asyncio.create_task(client.call("rpc.ghost.method"))

        for attempt in range(4):
            await until_timeout(lambda attempt=attempt: len(nc.published) == attempt + 1)
            req = decode(nc.published[-1]["data"])
            await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req['id']}", b"", no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        assert len(requests_to(nc, "rpc.ghost.method")) == 4

    asyncio.run(run())


class RequestNC(MockNC):
    def __init__(self, replies: list[Any]) -> None:
        super().__init__()
        self.replies = replies
        self.requests = 0

    async def request(self, subject: str, payload: bytes, timeout: float = 0, headers: Any = None) -> Any:
        self.requests += 1
        reply = self.replies[min(self.requests, len(self.replies)) - 1]
        if isinstance(reply, BaseException):
            raise reply
        return make_msg(subject, encode(reply))


@pytest.mark.parametrize(
    ("code", "message"),
    [("NOT_FOUND", "downstream call failed: no responders"), ("INTERNAL_ERROR", "no responders upstream")],
)
def test_request_keeps_remote_error_code_and_text(code: str, message: str) -> None:
    async def run() -> None:
        client, _ = retrying_client()
        nc = RequestNC([{"error": message, "code": code}])
        client.nc = nc

        with pytest.raises(RPCException) as exc_info:
            await client.request("svc.remote", {})

        assert exc_info.value.code == code
        assert exc_info.value.message == message
        assert nc.requests == 1

    asyncio.run(run())


def test_request_retries_local_no_responders_and_keeps_the_error_shape() -> None:
    async def run() -> None:
        client, _ = retrying_client()
        nc = RequestNC([errors.NoRespondersError()])
        client.nc = nc

        with pytest.raises(RPCException) as exc_info:
            await client.request("svc.ghost", {})

        assert exc_info.value.code == "NOT_FOUND"
        assert exc_info.value.message == "No responders available"
        assert nc.requests == 4

    asyncio.run(run())


def stream_subject_of(nc: MockNC, subject: str) -> tuple[str, str]:
    req = decode(requests_to(nc, subject)[-1]["data"])
    return req["id"], req["params"]["__streamSubject"]


async def push(nc: MockNC, stream_subject: str, body: dict[str, Any]) -> None:
    await subs_for(nc, stream_subject)[0].cb(make_msg(stream_subject, encode(body)))


def test_call_stream_does_not_retry_a_remote_error_after_items() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        items: list[Any] = []

        async def consume() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")

        for seq in range(2):
            await push(nc, stream_subject, {"id": request_id, "type": "data", "data": seq})
            await until(lambda seq=seq: len(items) == seq + 1)
        error = {"code": "NOT_FOUND", "message": "downstream call failed: no responders"}
        await push(nc, stream_subject, {"id": request_id, "type": "error", "error": error})

        with pytest.raises(RPCException) as exc_info:
            await asyncio.wait_for(task, 2)
        assert exc_info.value.code == "NOT_FOUND"
        assert items == [0, 1]
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.gen.items")) == 1

    asyncio.run(run())


def test_call_stream_does_not_retry_a_remote_error_before_items() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        stream = client.call_stream("rpc.gen.items")
        task = asyncio.create_task(stream.__anext__())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")

        error = {"code": "INTERNAL_ERROR", "message": "no responders downstream"}
        await push(nc, stream_subject, {"id": request_id, "type": "error", "error": error})

        with pytest.raises(RPCException):
            await asyncio.wait_for(task, 2)
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.gen.items")) == 1

    asyncio.run(run())


def test_call_stream_retries_local_no_responders_before_the_first_item() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        items: list[Any] = []

        async def consume() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, _ = stream_subject_of(nc, "rpc.gen.items")
        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{request_id}", b"", no_responder_headers()))

        await until_timeout(lambda: len(requests_to(nc, "rpc.gen.items")) == 2)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        await push(nc, stream_subject, {"id": request_id, "type": "data", "data": "ok"})
        await until(lambda: items == ["ok"])
        await push(nc, stream_subject, {"id": request_id, "type": "end"})
        await asyncio.wait_for(task, 2)

    asyncio.run(run())


def test_call_stream_does_not_retry_once_an_item_was_delivered() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        items: list[Any] = []

        async def consume() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        await push(nc, stream_subject, {"id": request_id, "type": "data", "data": 0})
        await until(lambda: items == [0])

        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{request_id}", b"", no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.gen.items")) == 1

    asyncio.run(run())


def test_proxy_no_responder_retry_override_turns_retry_off() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        proxy = client.create_proxy("ghost", no_responder_retry={"max_retries": 0})
        task = asyncio.create_task(proxy.echo(1))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req['id']}", b"", no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.ghost.echo")) == 1

    asyncio.run(run())


def test_proxy_timeout_applies_to_its_calls() -> None:
    async def run() -> None:
        client, nc = make_client(timeout=30000)
        proxy = client.create_proxy("slow", timeout=30)
        started = asyncio.get_running_loop().time()

        with pytest.raises(errors.TimeoutError):
            await asyncio.wait_for(proxy.wait(), 2)

        assert asyncio.get_running_loop().time() - started < 1
        assert len(client.pending_requests) == 0

    asyncio.run(run())


def test_call_takes_a_timeout_per_call() -> None:
    async def run() -> None:
        client, _ = make_client(timeout=30000)
        started = asyncio.get_running_loop().time()
        with pytest.raises(errors.TimeoutError):
            await asyncio.wait_for(client.call("rpc.slow.wait", timeout=30), 2)
        assert asyncio.get_running_loop().time() - started < 1

    asyncio.run(run())


def test_call_timeout_applies_to_service_subjects() -> None:
    async def run() -> None:
        client, _ = make_client(timeout=30000)
        started = asyncio.get_running_loop().time()
        with pytest.raises(errors.TimeoutError):
            await asyncio.wait_for(client.call("svc.slow.wait", timeout=30), 2)
        assert asyncio.get_running_loop().time() - started < 1
        assert client.pending_requests == {}

    asyncio.run(run())


def test_isolated_proxy_takes_the_retry_override() -> None:
    async def run() -> None:
        client, _ = retrying_client()
        isolated, isolated_nc = retrying_client()
        client.create_isolated_client = lambda _options: isolated
        handle = client.create_proxy("ghost", None, True, no_responder_retry={"max_retries": 0})
        task = asyncio.create_task(handle.proxy.echo(1))
        await until(lambda: len(isolated_nc.published) == 1)
        req = decode(isolated_nc.published[0]["data"])

        await mux_sub(isolated, isolated_nc).cb(
            make_msg(f"rpc.reply.{req['id']}", b"", no_responder_headers())
        )

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        await asyncio.sleep(0.05)
        assert len(requests_to(isolated_nc, "rpc.ghost.echo")) == 1

    asyncio.run(run())


def test_proxy_stream_uses_the_retry_override() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        proxy = client.create_proxy("gen", no_responder_retry={"max_retries": 0})
        stream = proxy.generateItems(3)
        task = asyncio.create_task(stream.__anext__())
        await until(lambda: len(requests_to(nc, "rpc.gen.generateItems")) == 1)
        request_id, _ = stream_subject_of(nc, "rpc.gen.generateItems")

        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{request_id}", b"", no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        await asyncio.sleep(0.05)
        assert len(requests_to(nc, "rpc.gen.generateItems")) == 1

    asyncio.run(run())


def test_proxy_without_options_keeps_the_client_retry() -> None:
    async def run() -> None:
        client, nc = retrying_client()
        proxy = client.create_proxy("ghost")
        task = asyncio.create_task(proxy.echo(1))
        # four attempts and, after the first, one ask for the namespace's methods
        for sent in range(5):
            await until_timeout(lambda sent=sent: len(nc.published) == sent + 1)
            req = decode(nc.published[-1]["data"])
            await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req['id']}", b"", no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(task, 2)
        assert len(requests_to(nc, "rpc.ghost.echo")) == 4
        assert len(requests_to(nc, "rpc.ghost.__methods")) == 1

    asyncio.run(run())


def assert_stream_released(client: Any, nc: MockNC, stream_subject: str) -> None:
    assert client.stream_handlers == {}
    assert client.status_handlers == {}
    assert all(s._closed for s in subs_for(nc, stream_subject))
    assert not any(e["pattern"] == stream_subject for e in client._subscription_entries.values())  # pyright: ignore[reportPrivateUsage]


def test_breaking_out_of_call_stream_cancels_the_server_and_cleans_up() -> None:
    async def run() -> None:
        client, nc = make_client()
        stream = client.call_stream("rpc.gen.items")
        first = asyncio.create_task(stream.__anext__())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        await push(nc, stream_subject, {"id": request_id, "type": "data", "data": 0})
        assert await asyncio.wait_for(first, 2) == 0

        await stream.aclose()

        assert len(requests_to(nc, f"{stream_subject}.cancel")) == 1
        assert_stream_released(client, nc, stream_subject)

    asyncio.run(run())


def test_async_for_break_releases_the_stream() -> None:
    async def run() -> None:
        client, nc = make_client()
        items: list[Any] = []

        async def consume() -> None:
            async with contextlib.aclosing(client.call_stream("rpc.gen.items")) as stream:
                async for item in stream:
                    items.append(item)
                    if len(items) == 2:
                        break

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        for seq in range(3):
            await push(nc, stream_subject, {"id": request_id, "type": "data", "data": seq})
        await asyncio.wait_for(task, 2)

        assert items == [0, 1]
        assert len(requests_to(nc, f"{stream_subject}.cancel")) == 1
        assert_stream_released(client, nc, stream_subject)

    asyncio.run(run())


def test_cancelling_the_consumer_task_releases_the_stream() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def consume() -> None:
            async with contextlib.aclosing(client.call_stream("rpc.gen.items")) as stream:
                async for _ in stream:
                    pass

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        _, stream_subject = stream_subject_of(nc, "rpc.gen.items")

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(requests_to(nc, f"{stream_subject}.cancel")) == 1
        assert_stream_released(client, nc, stream_subject)

    asyncio.run(run())


def test_call_stream_delivers_items_queued_before_the_error() -> None:
    async def run() -> None:
        client, nc = make_client()
        items: list[Any] = []

        async def consume() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        sub = subs_for(nc, stream_subject)[0]
        frames = [{"id": request_id, "type": "data", "data": seq} for seq in range(3)]
        frames.append(
            {"id": request_id, "type": "error", "error": {"code": "409", "message": "failed after 3"}}
        )
        for frame in frames:
            await sub.cb(make_msg(stream_subject, encode(frame)))

        with pytest.raises(RPCException) as exc_info:
            await asyncio.wait_for(task, 2)
        assert exc_info.value.code == "409"
        assert items == [0, 1, 2]
        assert len(requests_to(nc, f"{stream_subject}.cancel")) == 0
        assert_stream_released(client, nc, stream_subject)

    asyncio.run(run())


def test_call_stream_end_releases_without_cancel() -> None:
    async def run() -> None:
        client, nc = make_client()
        items: list[Any] = []

        async def consume() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.gen.items")) == 1)
        request_id, stream_subject = stream_subject_of(nc, "rpc.gen.items")
        await push(nc, stream_subject, {"id": request_id, "type": "data", "data": "a"})
        await push(nc, stream_subject, {"id": request_id, "type": "end"})
        await asyncio.wait_for(task, 2)

        assert items == ["a"]
        assert len(requests_to(nc, f"{stream_subject}.cancel")) == 0
        assert_stream_released(client, nc, stream_subject)

    asyncio.run(run())


class ReconnectingNC(MockNC):
    is_connected = False
    is_closed = False


def counting_connect(client: Any) -> list[int]:
    calls: list[int] = []

    async def fake_connect() -> Any:
        calls.append(1)
        return MockNC()

    client._connect = fake_connect  # pyright: ignore[reportPrivateUsage]
    return calls


def test_connect_reuses_a_reconnecting_connection() -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = ReconnectingNC()
        client.nc = nc
        calls = counting_connect(client)

        assert await client.connect() is nc
        assert calls == []

    asyncio.run(run())


def test_call_during_reconnect_publishes_on_the_same_connection() -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = ReconnectingNC()
        client.nc = nc
        calls = counting_connect(client)

        task = asyncio.create_task(client.call("rpc.test.echo", 1))
        await until(lambda: len(nc.published) == 1)
        await answer(client, nc, nc.published[0], {"result": 1})

        assert await asyncio.wait_for(task, 2) == 1
        assert calls == []
        assert client.nc is nc

    asyncio.run(run())


def test_connect_replaces_a_closed_connection() -> None:
    async def run() -> None:
        client, _ = make_client()
        closed = ReconnectingNC()
        closed.is_closed = True
        client.nc = closed
        calls = counting_connect(client)

        await client.connect()

        assert calls == [1]

    asyncio.run(run())


def call_msg(subject: str, seq: int, *params: Any) -> Any:
    return make_msg(subject, encode({"id": f"req.{seq}", "method": "call", "params": list(params)}))


async def call_message(client: Any, nc: MockNC, subject: str, seq: int, *params: Any) -> None:
    await subs_for(nc, subject)[0].cb(call_msg(subject, seq, *params))


def replies(nc: MockNC) -> dict[str, Any]:
    return {
        p["subject"]: decode(p["data"]) for p in nc.published if p["subject"].startswith("rpc.reply.req.")
    }


class Recorder:
    def __init__(self) -> None:
        self.started: list[int] = []
        self.running = 0
        self.peak = 0
        self.release = asyncio.Event()

    async def work(self, seq: int) -> int:
        self.started.append(seq)
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            await self.release.wait()
            return seq
        finally:
            self.running -= 1


def test_register_handler_runs_opted_in_calls_concurrently_in_arrival_order() -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()
        await client.register_handler("ns", {"work": rec.work}, concurrency={"work": 3})

        delivered = [asyncio.create_task(call_message(client, nc, "rpc.ns.work", i, i)) for i in range(5)]
        await until(lambda: len(rec.started) == 3)
        await asyncio.sleep(0.01)

        assert rec.started == [0, 1, 2]
        assert sum(task.done() for task in delivered) == 3
        rec.release.set()
        await asyncio.wait_for(asyncio.gather(*delivered), 2)
        await until(lambda: len(replies(nc)) == 5)

        assert rec.started == [0, 1, 2, 3, 4]
        assert rec.peak == 3
        assert {r["id"]: r["result"] for r in replies(nc).values()} == {f"req.{i}": i for i in range(5)}

    asyncio.run(run())


@pytest.mark.parametrize("concurrency", [None, {}, {"work": 1}, {"other": 4}])
def test_register_handler_stays_serial_without_a_limit(concurrency: Any) -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()
        await client.register_handler("ns", {"work": rec.work}, concurrency=concurrency)

        first = asyncio.create_task(call_message(client, nc, "rpc.ns.work", 0, 0))
        await until(lambda: rec.started == [0])
        await asyncio.sleep(0.01)

        assert not first.done()
        rec.release.set()
        await asyncio.wait_for(first, 2)
        assert rec.peak == 1

    asyncio.run(run())


def test_concurrent_call_error_keeps_the_reply_shape() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def fail(code: str) -> None:
            raise RPCException(code, "nope", {"x": 1})

        await client.register_handler("ns", {"fail": fail}, concurrency={"fail": 2})
        await call_message(client, nc, "rpc.ns.fail", 0, "NOT_FOUND")
        await until(lambda: len(replies(nc)) == 1)

        reply = replies(nc)["rpc.reply.req.0"]
        assert reply["error"] == {"code": "NOT_FOUND", "message": "nope", "data": {"x": 1}}

    asyncio.run(run())


def test_unregister_cancels_running_concurrent_calls() -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()
        cleanup = await client.register_handler("ns", {"work": rec.work}, concurrency={"work": 2})
        await call_message(client, nc, "rpc.ns.work", 0, 0)
        await until(lambda: rec.running == 1)

        await cleanup()

        assert rec.running == 0
        assert client._concurrent_calls == set()  # pyright: ignore[reportPrivateUsage]
        rec.release.set()
        await asyncio.sleep(0.01)
        assert replies(nc) == {}

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["suspend", "disconnect"])
def test_teardown_cancels_running_concurrent_calls(teardown: str) -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()
        await client.register_handler("ns", {"work": rec.work}, concurrency={"work": 2})
        await call_message(client, nc, "rpc.ns.work", 0, 0)
        await until(lambda: rec.running == 1)

        await getattr(client, teardown)()

        assert rec.running == 0
        assert client._concurrent_calls == set()  # pyright: ignore[reportPrivateUsage]
        assert replies(nc) == {}

    asyncio.run(run())


class SerialSub(MockSub):
    # like nats-py: one task delivers queued messages in turn, a cancel ends it cleanly
    def __init__(self, pattern: str, cb: Any, queue: str = "") -> None:
        super().__init__(pattern, cb, queue)
        self.pending: asyncio.Queue[Any] = asyncio.Queue()
        self._wait_for_msgs_task = asyncio.create_task(self._wait_for_msgs())

    async def _wait_for_msgs(self) -> None:
        while True:
            try:
                await self.cb(await self.pending.get())
            except asyncio.CancelledError:
                break

    async def unsubscribe(self) -> None:
        await super().unsubscribe()
        self._wait_for_msgs_task.cancel()


class SerialNC(MockNC):
    async def subscribe(self, pattern: str, queue: str = "", cb: Any = None, max_msgs: int = 0) -> MockSub:
        sub = SerialSub(pattern, cb, queue)
        self.subs.append(sub)
        return sub


@pytest.mark.parametrize("teardown", ["suspend", "disconnect"])
def test_queued_calls_do_not_start_once_teardown_begins(teardown: str) -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = SerialNC()
        client.nc = nc
        rec = Recorder()
        await client.register_handler("ns", {"work": rec.work}, concurrency={"work": 2})
        sub = subs_for(nc, "rpc.ns.work")[0]
        assert isinstance(sub, SerialSub)
        for seq in range(6):
            sub.pending.put_nowait(call_msg("rpc.ns.work", seq, seq))
        await until(lambda: rec.running == 2)

        await getattr(client, teardown)()
        await asyncio.sleep(0.01)

        assert rec.started == [0, 1]
        assert rec.running == 0
        assert client._concurrent_calls == set()  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


async def restore(client: Any, nc: MockNC) -> None:
    # the part of connect() that matters here, without a real connection
    client.nc = nc
    for entry in list(client._subscription_entries.values()):
        if entry["sub"] is None:
            await client._nats_subscribe(entry)


def test_a_call_cancelled_before_it_started_frees_its_slot() -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()
        await client.register_handler("ns", {"work": rec.work}, concurrency={"work": 2})
        await call_message(client, nc, "rpc.ns.work", 0, 0)

        await client.suspend()
        # suspend cancelled the call task before its first step
        assert rec.started == []
        await restore(client, nc)

        sub = subs_for(nc, "rpc.ns.work")[-1]
        delivered = [asyncio.create_task(sub.cb(call_msg("rpc.ns.work", i, i))) for i in (1, 2)]
        await until(lambda: rec.running == 2)
        rec.release.set()
        await asyncio.wait_for(asyncio.gather(*delivered), 2)
        await until(lambda: len(replies(nc)) == 2)

    asyncio.run(run())


def test_a_concurrent_call_can_unregister_its_own_handler() -> None:
    async def run() -> None:
        client, nc = make_client()
        registration: list[Any] = []

        async def leave() -> str:
            await registration[0]()
            return "bye"

        registration.append(await client.register_handler("ns", {"leave": leave}, concurrency={"leave": 2}))
        await call_message(client, nc, "rpc.ns.leave", 0)
        await until(lambda: len(replies(nc)) == 1)

        assert replies(nc)["rpc.reply.req.0"]["result"] == "bye"

    asyncio.run(run())


def test_sync_handler_with_a_limit_stays_serial() -> None:
    async def run() -> None:
        client, nc = make_client()

        def add(a: int, b: int) -> int:
            return a + b

        await client.register_handler("ns", {"add": add}, concurrency={"add": 4})
        for seq in range(2):
            await call_message(client, nc, "rpc.ns.add", seq, seq, 2)
            # the serial path has answered before the delivery returns
            assert replies(nc)[f"rpc.reply.req.{seq}"] == {"id": f"req.{seq}", "result": seq + 2}

    asyncio.run(run())


def test_streams_of_an_opted_in_method_skip_the_call_slots() -> None:
    async def run() -> None:
        client, nc = make_client()
        rec = Recorder()

        async def fetch(seq: int) -> Any:
            if seq < 0:
                return [seq, seq]
            return await rec.work(seq)

        await client.register_handler("ns", {"fetch": fetch}, concurrency={"fetch": 2})
        for seq in range(2):
            await call_message(client, nc, "rpc.ns.fetch", seq, seq)
        await until(lambda: rec.running == 2)

        sub = subs_for(nc, "rpc.ns.fetch")[0]
        stream_subjects = [f"stream.rpc.ns.fetch.req.s{n}" for n in (1, 2)]
        for n, stream_subject in enumerate(stream_subjects, 1):
            params = {"__stream": True, "__streamSubject": stream_subject, "args": [-n]}
            request = {"id": f"req.s{n}", "method": "stream", "params": params}
            await asyncio.wait_for(sub.cb(make_msg("rpc.ns.fetch", encode(request))), 1)

        for stream_subject in stream_subjects:
            await until(lambda s=stream_subject: len(requests_to(nc, s)) == 3)
            frames = [decode(p["data"]) for p in requests_to(nc, stream_subject)]
            assert [f["type"] for f in frames] == ["data", "data", "end"]
        assert rec.running == 2
        rec.release.set()
        await until(lambda: len(replies(nc)) == 2)

    asyncio.run(run())


class ClosableNC(MockNC):
    is_closed = False

    def __init__(self, closed_cb: Any = None) -> None:
        super().__init__()
        self.closed_cb = closed_cb

    async def close(self) -> None:
        # like nats-py: the closed callback runs inside close()
        self.is_closed = True
        self.is_connected = False
        if self.closed_cb is not None:
            await self.closed_cb()


def dialing(monkeypatch: pytest.MonkeyPatch, **options: Any) -> tuple[Any, list[ClosableNC]]:
    conns: list[ClosableNC] = []

    async def fake_connect(**connect_options: Any) -> ClosableNC:
        nc = ClosableNC(connect_options.get("closed_cb"))
        conns.append(nc)
        return nc

    monkeypatch.setattr(rpc_client, "connect", fake_connect)
    client, _ = make_client(**options)
    client.nc = None
    return client, conns


def test_a_connection_closed_for_good_ends_the_session_at_the_close(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        reconnects: list[int] = []

        async def on_reconnect() -> None:
            reconnects.append(1)

        client, conns = dialing(monkeypatch, reconnected_cb=on_reconnect)
        await client.connect()
        nc = conns[0]
        await client.register_handler("ns", {"echo": lambda value: value})
        await client.subscribe("test.a", lambda d: None)
        await subs_for(nc, "test.a")[0].cb(make_msg("test.a", *chunk_header("t1", 60, 10)))
        await subs_for(nc, "test.a")[0].cb(make_msg("test.a", b"\x00" * 10, chunk_headers("t1", 0)))
        endpoint = client._open_endpoint_chunking()  # pyright: ignore[reportPrivateUsage]
        endpoint.start_receiving("e1", 6, lambda d: None, lambda e: None, 60, 10)
        endpoint.process_chunk({"id": "e1", "chunkIndex": 0, "data": b"\x00" * 10})
        call = asyncio.create_task(client.call("rpc.test.slow"))
        await until(lambda: len(requests_to(nc, "rpc.test.slow")) == 1)
        req = decode(requests_to(nc, "rpc.test.slow")[0]["data"])
        reply = chunked_reply(f"rpc.reply.{req['id']}", "m1", encode({"id": req["id"], "result": "x" * 300}))
        await mux_sub(client, nc).cb(reply[0])
        await mux_sub(client, nc).cb(reply[1])
        managers = [sub_chunks(client, "test.a"), endpoint, client._mux_chunking]  # pyright: ignore[reportPrivateUsage]
        budget = client._chunk_budget  # pyright: ignore[reportPrivateUsage]
        assert budget.used > 120
        entries = dict(client._subscription_entries)  # pyright: ignore[reportPrivateUsage]

        await nc.close()

        assert client.nc is None
        assert not client.is_closed
        assert budget.used == 0
        for manager in managers:
            assert manager is not None and manager.assemblers == {}
            assert manager._timer is None  # pyright: ignore[reportPrivateUsage]
        assert client._subscription_entries == entries  # pyright: ignore[reportPrivateUsage]
        assert all(entry["sub"] is None for entry in entries.values())
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(call, 1)
        assert caught.value.code == "CONNECTION_CLOSED"
        assert reconnects == []

        again = asyncio.create_task(client.call("rpc.test.echo", 1))
        await until(lambda: len(conns) == 2 and len(requests_to(conns[1], "rpc.test.echo")) == 1)
        fresh = conns[1]
        assert sorted(sub.pattern for sub in fresh.subs) == sorted(e["pattern"] for e in entries.values())
        await answer(client, fresh, requests_to(fresh, "rpc.test.echo")[0], {"result": 1})
        assert await asyncio.wait_for(again, 1) == 1
        await until(lambda: reconnects == [1])

    asyncio.run(run())


def test_a_connection_closed_for_good_ends_stream_and_pull_consumers(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        nc = conns[0]
        items: list[Any] = []

        async def consume_stream() -> None:
            async for item in client.call_stream("rpc.gen.items"):
                items.append(item)

        async def consume_pull() -> None:
            async for item in client.call_pull_iterator("rpc.gen.pull"):
                items.append(item)

        stream = asyncio.create_task(consume_stream())
        pull = asyncio.create_task(consume_pull())
        await until(lambda: len(requests_to(nc, "rpc.gen.pull")) == 1)
        init = decode(requests_to(nc, "rpc.gen.pull")[0]["data"])
        iterator_id = init["params"][0]["__iteratorId"]
        await answer(client, nc, requests_to(nc, "rpc.gen.pull")[0], {"result": {"iteratorId": iterator_id}})
        await until(lambda: any(p["subject"] == f"_rpc.iterator.{iterator_id}.request" for p in nc.published))

        await nc.close()

        await asyncio.wait_for(stream, 1)
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(pull, 1)
        assert caught.value.code == "CONNECTION_CLOSED"
        assert items == []

    asyncio.run(run())


def test_a_subscription_task_ending_with_its_closed_connection_keeps_the_entry() -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = SerialNC()
        client.nc = cast(Any, nc)
        await client.subscribe("test.a", lambda d: None)
        sub = subs_for(nc, "test.a")[0]
        assert isinstance(sub, SerialSub)
        sub._conn = SimpleNamespace(is_closed=True)  # type: ignore[attr-defined]

        # nats-py ends every subscription task like this when it closes the connection for good
        sub._wait_for_msgs_task.cancel()
        await asyncio.sleep(0.01)

        assert [e["pattern"] for e in client._subscription_entries.values()] == ["test.a"]  # pyright: ignore[reportPrivateUsage]
        assert set(client._chunk_scopes) != set()  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["suspend", "disconnect"])
def test_our_own_close_is_not_a_lost_connection(monkeypatch: pytest.MonkeyPatch, teardown: str) -> None:
    async def run() -> None:
        reconnects: list[int] = []

        async def on_reconnect() -> None:
            reconnects.append(1)

        client, conns = dialing(monkeypatch, reconnected_cb=on_reconnect)
        await client.connect()
        lost: list[Any] = []
        original = client._on_connection_lost  # pyright: ignore[reportPrivateUsage]

        async def spy(nc: Any) -> None:
            lost.append(nc)
            await original(nc)

        client._on_connection_lost = spy  # pyright: ignore[reportPrivateUsage]
        await client.subscribe("test.a", lambda d: None)

        await getattr(client, teardown)()
        await client.connect()
        await asyncio.sleep(0.01)

        assert conns[0].is_closed
        assert client.nc is conns[1]
        assert reconnects == []
        assert lost in ([], [conns[0]])
        patterns = [e["pattern"] for e in client._subscription_entries.values()]  # pyright: ignore[reportPrivateUsage]
        assert ("test.a" in patterns) == (teardown == "suspend")

    asyncio.run(run())


def test_a_late_close_of_a_replaced_connection_changes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        await conns[0].close()
        await client.connect()
        call = asyncio.create_task(client.call("rpc.test.slow"))
        await until(lambda: len(requests_to(conns[1], "rpc.test.slow")) == 1)

        closed_cb = conns[0].closed_cb
        assert closed_cb is not None
        await closed_cb()

        assert client.nc is conns[1]
        assert not call.done()
        call.cancel()

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["suspend", "disconnect"])
def test_teardown_keeps_one_budget_for_the_client_lifetime(teardown: str) -> None:
    async def run() -> None:
        client, _ = make_client()
        budget = client._chunk_budget  # pyright: ignore[reportPrivateUsage]
        manager = client.chunking_manager
        mux = client._mux_chunking  # pyright: ignore[reportPrivateUsage]

        await getattr(client, teardown)()

        assert client._chunk_budget is budget  # pyright: ignore[reportPrivateUsage]
        assert client.chunking_manager is manager
        assert client._mux_chunking is mux  # pyright: ignore[reportPrivateUsage]
        assert manager.budget is budget and mux.budget is budget

    asyncio.run(run())


@pytest.mark.parametrize("subject", ["rpc.slow.wait", "svc.slow.wait"])
def test_call_timeout_is_an_rpc_timeout_naming_the_subject(subject: str) -> None:
    async def run() -> None:
        client, _ = make_client()

        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(client.call(subject, timeout=20), 2)

        assert caught.value.code == "TIMEOUT"
        assert caught.value.message == f'RPC call to "{subject}" timed out after 20ms'
        assert isinstance(caught.value, errors.TimeoutError)
        assert isinstance(caught.value, TimeoutError)
        assert client.pending_requests == {}

    asyncio.run(run())


def test_request_timeout_is_an_rpc_timeout_naming_the_subject() -> None:
    async def run() -> None:
        client, _ = make_client()
        client.nc = RequestNC([errors.TimeoutError()])

        with pytest.raises(errors.TimeoutError) as caught:
            await client.request("svc.slow", {}, timeout=20)

        assert isinstance(caught.value, RPCException)
        assert caught.value.code == "TIMEOUT"
        assert caught.value.message == 'Request to "svc.slow" timed out after 20ms'

    asyncio.run(run())


@pytest.mark.parametrize(
    ("options", "wait", "connect_timeout"),
    [
        ({}, 2.0, 10.0),
        ({"reconnect_time_wait": 500, "connect_timeout": 750}, 0.5, 0.75),
        ({"reconnect_time_wait": 1500, "connect_timeout": 2500}, 1.5, 2.5),
        ({"reconnect_time_wait": 0, "connect_timeout": 0}, 0.1, 0.001),
    ],
)
def test_millisecond_options_reach_nats_py_as_seconds_without_truncation(
    monkeypatch: pytest.MonkeyPatch, options: dict[str, int], wait: float, connect_timeout: float
) -> None:
    seen: list[dict[str, Any]] = []

    async def fake_connect(**connect_options: Any) -> MockNC:
        seen.append(connect_options)
        return MockNC()

    monkeypatch.setattr(rpc_client, "connect", fake_connect)
    client, _ = make_client(**options)
    client.nc = None
    asyncio.run(client.connect())

    assert seen[0]["reconnect_time_wait"] == pytest.approx(wait)
    assert seen[0]["connect_timeout"] == pytest.approx(connect_timeout)


def test_a_handler_reports_its_own_no_responder_as_upstream_unavailable() -> None:
    async def run() -> None:
        client, nc = make_client()
        seen: list[BaseException] = []

        async def find() -> Any:
            try:
                return await client.call("rpc.up.find")
            except Exception as e:
                seen.append(e)
                raise

        await client.register_handler("mid", {"find": find})
        delivered = asyncio.create_task(call_message(client, nc, "rpc.mid.find", 0))
        await until(lambda: len(requests_to(nc, "rpc.up.find")) == 1)
        upstream = decode(requests_to(nc, "rpc.up.find")[0]["data"])
        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{upstream['id']}", b"", no_responder_headers()))
        await asyncio.wait_for(delivered, 1)

        # the handler saw the no-responder unchanged, its caller a failed call
        assert isinstance(seen[0], errors.NoRespondersError)
        assert replies(nc)["rpc.reply.req.0"]["error"] == {
            "code": "INTERNAL_ERROR",
            "message": "upstream unavailable",
            "data": {"subject": "rpc.up.find"},
        }

    asyncio.run(run())


def test_a_request_responder_reports_its_own_no_responder_as_upstream_unavailable() -> None:
    async def run() -> None:
        client, nc = make_client()
        client.nc = RequestNC([errors.NoRespondersError()])
        responded: list[Any] = []

        async def ask(_data: Any) -> Any:
            return await client.request("svc.up", {}, no_responder_retry={"max_retries": 0})

        await client.on_request("svc.mid", ask)
        sub = client.nc.subs[0]

        async def respond(data: bytes) -> None:
            responded.append(decode(data))

        await sub.cb(SimpleNamespace(data=encode({}), reply="_INBOX.r", respond=respond))

        assert responded == [{"error": "upstream unavailable", "code": "INTERNAL_ERROR"}]

    asyncio.run(run())


def test_a_pull_iterator_raises_its_no_responder_with_the_marker() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def consume() -> list[Any]:
            return [item async for item in client.call_pull_iterator("rpc.up.pullItems")]

        task = asyncio.create_task(consume())
        await until(lambda: len(requests_to(nc, "rpc.up.pullItems")) == 1)
        init = decode(requests_to(nc, "rpc.up.pullItems")[0]["data"])
        iterator_id = init["params"][0]["__iteratorId"]
        await mux_sub(client, nc).cb(
            make_msg(
                f"rpc.reply.{init['id']}", encode({"id": init["id"], "result": {"iteratorId": iterator_id}})
            )
        )
        await until(lambda: any(p["reply"] == f"rpc.reply.{iterator_id}" for p in nc.published))
        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{iterator_id}", b"", no_responder_headers()))

        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(task, 1)
        assert caught.value.code == "503"
        assert caught.value.message == "nats: no responders available for request"
        assert rpc_client.is_local_no_responders(caught.value)

    asyncio.run(run())


def test_register_handler_answers_its_method_list_after_subscribing_the_methods() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def echo(value: int) -> int:
            return value

        cleanup = await client.register_handler("ns", {"echo": echo, "find": echo})
        assert [s.pattern for s in nc.subs] == ["rpc.ns.echo", "rpc.ns.find", "rpc.ns.__methods"]

        await subs_for(nc, "rpc.ns.__methods")[0].cb(make_msg("rpc.ns.__methods", encode({"id": "req.m"})))
        assert decode(nc.published[-1]["data"]) == {"id": "req.m", "result": ["echo", "find"]}

        await cleanup()
        assert all(s._closed for s in nc.subs)

    asyncio.run(run())


async def answer_attempts(client: Any, nc: MockNC, subject: str, methods: Any, probe: bool = True) -> None:
    """503 every call to subject, answer the method list with methods (503 when None, nothing without probe)."""
    answered = 0
    while True:
        await asyncio.sleep(0.001)
        for pub in nc.published[answered:]:
            answered += 1
            req = decode(pub["data"])
            reply = f"rpc.reply.{req['id']}"
            if pub["subject"].endswith(".__methods") and not probe:
                continue
            if pub["subject"].endswith(".__methods") and methods is not None:
                await mux_sub(client, nc).cb(make_msg(reply, encode({"id": req["id"], "result": methods})))
            elif pub["subject"] in (subject, subject.rsplit(".", 1)[0] + ".__methods"):
                await mux_sub(client, nc).cb(make_msg(reply, b"", no_responder_headers()))


@pytest.mark.parametrize(("method", "listed"), [("missing", ["echo", "db.find"]), ("db.gone", ["db.find"])])
def test_a_proxy_call_to_a_method_a_live_namespace_lacks_fails_at_once(
    method: str, listed: list[str]
) -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 3, "delays": [0.5, 1.0, 2.0]})
        proxy = client.create_proxy("ns")
        subject = f"rpc.ns.{method}"
        server = asyncio.create_task(answer_attempts(client, nc, subject, listed))
        target = proxy
        for part in method.split("."):
            target = getattr(target, part)

        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(target(), 2)
        server.cancel()

        assert time.monotonic() - started < 0.2
        assert len(requests_to(nc, subject)) == 1
        probe = decode(requests_to(nc, "rpc.ns.__methods")[0]["data"])
        assert probe["params"] == []
        # the list it got back filled the proxy's method cache
        assert proxy.missing is None
        assert proxy.db is not None

    asyncio.run(run())


@pytest.mark.parametrize("listed", [["echo"], None])
def test_a_proxy_call_keeps_the_retry_while_the_method_may_come(listed: Any) -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 1, "delays": [0.15]})
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.echo", listed))

        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.echo(), 2)
        server.cancel()

        # the ask ran inside the first delay instead of adding to it
        assert 0.14 < time.monotonic() - started < 0.3
        assert len(requests_to(nc, "rpc.ns.echo")) == 2
        assert len(requests_to(nc, "rpc.ns.__methods")) == 1

    asyncio.run(run())


def test_a_method_list_that_never_comes_adds_nothing_to_the_first_delay() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 1, "delays": [0.15]})
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.echo", None, probe=False))

        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.echo(), 3)
        server.cancel()

        # the unanswered ask gives up with the first delay, and its time counts toward that delay
        assert 0.14 < time.monotonic() - started < 0.25
        assert len(requests_to(nc, "rpc.ns.echo")) == 2
        assert len(requests_to(nc, "rpc.ns.__methods")) == 1

    asyncio.run(run())


def test_a_filled_method_cache_keeps_a_method_that_is_gone_for_a_moment() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 1, "delays": [0.5]})
        proxy = client.create_proxy("ns")
        served = ["a", "m"]

        async def responder() -> None:
            answered = 0
            while True:
                await asyncio.sleep(0.001)
                for pub in nc.published[answered:]:
                    answered += 1
                    if pub["subject"] == "rpc.ns.__methods":
                        await answer(client, nc, pub, {"result": list(served)})
                    elif "m" in served:
                        await answer(client, nc, pub, {"result": "m", "__methods": list(served)})
                    else:
                        reply = f"rpc.reply.{decode(pub['data'])['id']}"
                        await mux_sub(client, nc).cb(make_msg(reply, b"", no_responder_headers()))

        server = asyncio.create_task(responder())
        # held across the calls, so no lookup reads the cache before the method is gone
        method = proxy.m
        assert await asyncio.wait_for(method(), 2) == "m"

        served.remove("m")
        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(method(), 2)
        assert time.monotonic() - started < 0.2

        served.append("m")
        assert proxy.m is method
        assert await asyncio.wait_for(proxy.m(), 2) == "m"
        server.cancel()

    asyncio.run(run())


def test_a_proxy_without_retry_does_not_ask_for_the_methods() -> None:
    async def run() -> None:
        client, nc = make_client()
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.echo", ["other"]))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.echo(), 2)
        server.cancel()

        assert requests_to(nc, "rpc.ns.__methods") == []

    asyncio.run(run())


def test_a_proxy_stream_a_live_namespace_lacks_fails_at_once() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 3, "delays": [0.5, 1.0, 2.0]})
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.generateFrames", ["echo"]))

        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.generateFrames().__anext__(), 2)
        server.cancel()

        assert time.monotonic() - started < 0.2
        assert len(requests_to(nc, "rpc.ns.generateFrames")) == 1
        assert len(requests_to(nc, "rpc.ns.__methods")) == 1
        assert proxy.generateFrames is None
        assert proxy.echo is not None

    asyncio.run(run())


def test_a_proxy_stream_keeps_the_retry_while_the_method_is_listed() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 1, "delays": [0.15]})
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.generateFrames", ["generateFrames"]))

        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.generateFrames().__anext__(), 2)
        server.cancel()

        assert 0.14 < time.monotonic() - started < 0.3
        assert len(requests_to(nc, "rpc.ns.generateFrames")) == 2
        assert len(requests_to(nc, "rpc.ns.__methods")) == 1

    asyncio.run(run())


def test_call_stream_outside_a_proxy_does_not_ask_for_the_methods() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 1, "delays": [0.05]})
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.generateFrames", ["echo"]))

        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(client.call_stream("rpc.ns.generateFrames").__anext__(), 2)
        server.cancel()

        assert len(requests_to(nc, "rpc.ns.generateFrames")) == 2
        assert requests_to(nc, "rpc.ns.__methods") == []

    asyncio.run(run())
