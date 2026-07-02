"""Tests for the muxed reply inbox (behavioral spec: node/test/mux.test.ts).

A minimal mock NATS connection captures subscriptions and published
messages; replies are delivered manually by invoking the captured
subscription callbacks — no server involved.
"""

import asyncio
import re
from types import SimpleNamespace
from typing import Any, cast

import pytest
from nats import errors
from nats.aio.client import NO_RESPONDERS_STATUS
from nats.js.api import Header

from camera_ui_rpc.chunking import create_chunks
from camera_ui_rpc.client import RPCClient
from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.errors import RPCException
from camera_ui_rpc.types import RPCClientOptions
from camera_ui_rpc.utils import generate_id, generate_reply_prefix


class MockSub:
    def __init__(self, pattern: str, cb: Any, queue: str = "") -> None:
        self.pattern = pattern
        self.cb = cb
        self.queue = queue
        self._closed = False

    async def unsubscribe(self) -> None:
        self._closed = True


class MockNC:
    is_connected = True
    max_payload = 1024 * 1024

    def __init__(self) -> None:
        self.subs: list[MockSub] = []
        self.published: list[dict[str, Any]] = []

    async def subscribe(self, pattern: str, queue: str = "", cb: Any = None, max_msgs: int = 0) -> MockSub:
        sub = MockSub(pattern, cb, queue)
        self.subs.append(sub)
        return sub

    async def publish(
        self, subject: str, data: bytes, headers: dict[str, str] | None = None, reply: str = ""
    ) -> None:
        self.published.append({"subject": subject, "data": data, "headers": headers, "reply": reply})

    def new_inbox(self) -> str:
        return "_INBOX.mock"


def make_client(**overrides: Any) -> tuple[RPCClient, MockNC]:
    options: dict[str, Any] = {
        "servers": ["nats://127.0.0.1:4222"],
        "name": "mux-test",
        # Retries would re-publish and slow the 503 tests down — disable.
        "no_responder_retry": {"max_retries": 0, "delays": [0.001]},
    }
    options.update(overrides)
    client = RPCClient(cast(RPCClientOptions, options))
    client.nc = cast(Any, MockNC())
    return client, cast(MockNC, cast(Any, client.nc))


def mux_sub(client: RPCClient, nc: MockNC) -> MockSub:
    """The single wildcard mux subscription of a client."""
    prefix = client._reply_prefix  # pyright: ignore[reportPrivateUsage]
    matches = [s for s in nc.subs if s.pattern == f"rpc.reply.{prefix}.>"]
    assert len(matches) == 1
    return matches[0]


def make_msg(subject: str, data: bytes = b"", headers: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(subject=subject, data=data, headers=headers, reply="")


def no_responder_headers() -> dict[str, str]:
    return {Header.STATUS: NO_RESPONDERS_STATUS}


async def until(cond: Any, tries: int = 200) -> None:
    for _ in range(tries):
        if cond():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition not met")


def test_generate_reply_prefix_format() -> None:
    prefix = generate_reply_prefix()
    assert re.fullmatch(r"[0-9a-z]{10}", prefix)
    assert generate_reply_prefix() != prefix


def test_generate_id_carries_reply_prefix() -> None:
    prefix = generate_reply_prefix()
    request_id = generate_id(prefix)
    assert request_id.split(".")[0] == prefix
    assert re.fullmatch(r"\d+-[0-9a-z]{9}", request_id.removeprefix(f"{prefix}."))


def test_routes_response_to_pending_call_by_envelope_id() -> None:
    async def run() -> None:
        client, nc = make_client()
        prefix = client._reply_prefix  # pyright: ignore[reportPrivateUsage]

        task = asyncio.create_task(client.call("rpc.test.echo", "hello"))
        await until(lambda: len(nc.published) == 1)

        # Request went out with reply = the call's own muxed reply subject.
        request = decode(nc.published[0]["data"])
        assert request["id"].startswith(f"{prefix}.")
        assert nc.published[0]["reply"] == f"rpc.reply.{request['id']}"
        assert nc.published[0]["subject"] == "rpc.test.echo"
        assert request["params"] == ["hello"]

        sub = mux_sub(client, nc)
        await sub.cb(make_msg(f"rpc.reply.{request['id']}", encode({"id": request["id"], "result": "world"})))

        assert await task == "world"
        # Settled call leaves no pending entry behind.
        assert len(client.pending_requests) == 0

    asyncio.run(run())


def test_single_mux_subscription_across_many_calls() -> None:
    async def run() -> None:
        client, nc = make_client()

        tasks = [
            asyncio.create_task(client.call("rpc.a.one")),
            asyncio.create_task(client.call("rpc.a.two")),
            asyncio.create_task(client.call("rpc.a.three")),
        ]
        await until(lambda: len(nc.published) == 3)

        sub = mux_sub(client, nc)  # asserts count == 1
        for pub in nc.published:
            req = decode(pub["data"])
            await sub.cb(
                make_msg(f"rpc.reply.{req['id']}", encode({"id": req["id"], "result": req["params"]}))
            )
        await asyncio.gather(*tasks)
        # Only the wildcard mux — no per-call subscriptions.
        assert len(nc.subs) == 1

    asyncio.run(run())


def test_routes_out_of_order_responses_correctly() -> None:
    async def run() -> None:
        client, nc = make_client()

        t1 = asyncio.create_task(client.call("rpc.test.first"))
        t2 = asyncio.create_task(client.call("rpc.test.second"))
        await until(lambda: len(nc.published) == 2)

        req1, req2 = (decode(p["data"]) for p in nc.published)
        sub = mux_sub(client, nc)

        # Answer the second call first.
        await sub.cb(make_msg(f"rpc.reply.{req2['id']}", encode({"id": req2["id"], "result": 2})))
        await sub.cb(make_msg(f"rpc.reply.{req1['id']}", encode({"id": req1["id"], "result": 1})))

        assert await t2 == 2
        assert await t1 == 1

    asyncio.run(run())


def test_rejects_with_rpc_exception_on_error_response() -> None:
    async def run() -> None:
        client, nc = make_client()

        task = asyncio.create_task(client.call("rpc.test.fail"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        await mux_sub(client, nc).cb(
            make_msg(
                f"rpc.reply.{req['id']}",
                encode({"id": req["id"], "error": {"code": "METHOD_NOT_FOUND", "message": "nope"}}),
            )
        )

        with pytest.raises(RPCException) as exc_info:
            await task
        assert exc_info.value.code == "METHOD_NOT_FOUND"
        assert exc_info.value.message == "nope"

    asyncio.run(run())


def test_attaches_methods_metadata_to_dict_results() -> None:
    async def run() -> None:
        client, nc = make_client()

        task = asyncio.create_task(client.call("rpc.test.meta"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        await mux_sub(client, nc).cb(
            make_msg(
                f"rpc.reply.{req['id']}",
                encode({"id": req["id"], "result": {"value": 7}, "__methods": ["meta", "other"]}),
            )
        )

        result = await task
        # Python semantics: __methods rides inside dict results (the proxy
        # strips it) — scalar results stay untouched.
        assert result == {"value": 7, "__methods": ["meta", "other"]}

    asyncio.run(run())


def test_rejects_pending_call_with_no_responders_on_503_status() -> None:
    async def run() -> None:
        client, nc = make_client()

        task = asyncio.create_task(client.call("rpc.ghost.method"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        # NATS no-responder status: empty payload + status header 503 on the
        # reply subject.
        await mux_sub(client, nc).cb(make_msg(f"rpc.reply.{req['id']}", b"", headers=no_responder_headers()))

        with pytest.raises(errors.NoRespondersError):
            await task
        assert len(client.pending_requests) == 0

    asyncio.run(run())


def test_routes_503_to_registered_status_handler_one_shot() -> None:
    async def run() -> None:
        client, nc = make_client()

        # Force mux creation without an RPC call.
        await client._ensure_mux_subscription()  # pyright: ignore[reportPrivateUsage]
        prefix = client._reply_prefix  # pyright: ignore[reportPrivateUsage]
        token = f"{prefix}.iter-token"

        received: list[Exception] = []
        client.status_handlers[token] = received.append

        sub = mux_sub(client, nc)
        await sub.cb(make_msg(f"rpc.reply.{token}", b"", headers=no_responder_headers()))

        assert len(received) == 1
        assert isinstance(received[0], errors.NoRespondersError)
        # One-shot: the registration is consumed (max_msgs=1 semantics) …
        assert len(client.status_handlers) == 0
        # … so a second status is dropped silently.
        await sub.cb(make_msg(f"rpc.reply.{token}", b"", headers=no_responder_headers()))
        assert len(received) == 1

    asyncio.run(run())


def test_reassembles_chunked_responses_and_routes_by_envelope_id() -> None:
    async def run() -> None:
        client, nc = make_client()

        task = asyncio.create_task(client.call("rpc.test.big"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        reply_subject = f"rpc.reply.{req['id']}"
        sub = mux_sub(client, nc)

        # Build a chunked response exactly like publish() would.
        big_result = "x" * 1000
        encoded = encode({"id": req["id"], "result": big_result})
        chunk_size = 100
        transfer_id = "transfer-1"
        total_chunks = -(-len(encoded) // chunk_size)

        await sub.cb(
            make_msg(
                reply_subject,
                encode(
                    {
                        "type": "chunked",
                        "transferId": transfer_id,
                        "totalChunks": total_chunks,
                        "totalSize": len(encoded),
                        "chunkSize": chunk_size,
                    }
                ),
                headers={"x-chunked-transfer": "header", "x-chunk-id": transfer_id},
            )
        )

        for chunk in create_chunks(encoded, transfer_id, chunk_size):
            await sub.cb(
                make_msg(
                    reply_subject,
                    chunk["data"],
                    headers={
                        "x-chunked-transfer": "chunk",
                        "x-chunk-id": transfer_id,
                        "x-chunk-index": str(chunk["index"]),
                    },
                )
            )

        assert await task == big_result

    asyncio.run(run())


def test_drops_responses_for_unknown_ids_silently() -> None:
    async def run() -> None:
        client, nc = make_client()

        task = asyncio.create_task(client.call("rpc.test.keep"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])
        sub = mux_sub(client, nc)

        # Foreign/late response — must not settle or throw.
        await sub.cb(make_msg("rpc.reply.someone.else", encode({"id": "someone.else", "result": "nope"})))

        await sub.cb(make_msg(f"rpc.reply.{req['id']}", encode({"id": req["id"], "result": "mine"})))
        assert await task == "mine"

    asyncio.run(run())


def test_times_out_and_ignores_late_response() -> None:
    async def run() -> None:
        client, nc = make_client(timeout=20)

        task = asyncio.create_task(client.call("rpc.test.slow"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        with pytest.raises(errors.TimeoutError):
            await task
        assert len(client.pending_requests) == 0

        # Late response after the timeout must be a no-op.
        await mux_sub(client, nc).cb(
            make_msg(f"rpc.reply.{req['id']}", encode({"id": req["id"], "result": "late"}))
        )

    asyncio.run(run())


def test_uses_conn_id_as_reply_prefix_when_configured() -> None:
    async def run() -> None:
        client, nc = make_client(conn_id="conn42")

        task = asyncio.create_task(client.call("rpc.test.scoped"))
        await until(lambda: len(nc.published) == 1)
        req = decode(nc.published[0]["data"])

        assert client._reply_prefix == "conn42"  # pyright: ignore[reportPrivateUsage]
        assert req["id"].startswith("conn42.")
        assert nc.subs[0].pattern == "rpc.reply.conn42.>"

        await mux_sub(client, nc).cb(
            make_msg(f"rpc.reply.{req['id']}", encode({"id": req["id"], "result": True}))
        )
        assert await task is True

    asyncio.run(run())
