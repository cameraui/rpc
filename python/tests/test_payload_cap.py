from __future__ import annotations

import asyncio
import contextlib
import math
import re
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from nats.micro.service import ServiceConfig
from test_client import subs_for
from test_drain import until
from test_handler import frames, open_pull, pull, serve, wait_for
from test_register import GatedNC
from test_service import FakeSvcm, request

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.codec import decode, encode, encode_message
from camera_ui_rpc.errors import RPCException
from camera_ui_rpc.types import RPCClientOptions


def connecting(monkeypatch: pytest.MonkeyPatch, server_max: int) -> list[GatedNC]:
    conns: list[GatedNC] = []

    async def fake_connect(**_options: Any) -> GatedNC:
        nc = GatedNC(open_gate=True)
        nc._max_payload = server_max
        conns.append(nc)
        return nc

    monkeypatch.setattr(rpc_client, "connect", fake_connect)
    return conns


@pytest.mark.parametrize(
    ("server_max", "cap", "limit"),
    [
        (4 * 1024 * 1024, None, 4 * 1024 * 1024),
        (4 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
        (1024 * 1024, 4 * 1024 * 1024, 1024 * 1024),
        (1024 * 1024, 0, 1024 * 1024),
    ],
)
def test_the_chunk_limit_is_the_lower_of_server_and_option(
    server_max: int, cap: int | None, limit: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def run() -> None:
        connecting(monkeypatch, server_max)
        options: dict[str, Any] = {"servers": ["nats://127.0.0.1:1"], "name": "cap"}
        if cap is not None:
            options["max_payload_size"] = cap
        client = rpc_client.RPCClient(RPCClientOptions(**options))  # type: ignore[typeddict-item]
        await client.connect()
        assert client.max_payload_size == limit - 8192

    asyncio.run(run())


def test_a_message_over_the_cap_goes_out_chunked(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        conns = connecting(monkeypatch, 4 * 1024 * 1024)
        client = rpc_client.RPCClient(
            {"servers": ["nats://127.0.0.1:1"], "name": "cap", "max_payload_size": 64 * 1024}
        )
        await client.connect()
        await client.publish("events.big", b"x" * 200_000)

        sent = [p for p in conns[0].published if p["subject"] == "events.big"]
        header = decode(sent[0]["data"])
        assert header["type"] == "chunked"
        assert header["chunkSize"] == 64 * 1024 - 8192
        assert len(sent) == 1 + header["totalChunks"]

    asyncio.run(run())


@pytest.mark.parametrize("cap", [1, 8192, 16383, -1])
def test_a_payload_option_below_the_floor_is_refused(cap: int) -> None:
    with pytest.raises(ValueError, match=f"max_payload_size {cap} is too small"):
        rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "cap", "max_payload_size": cap})


@pytest.mark.parametrize("cap", [20000.5, 20000.0, math.nan, math.inf, -math.inf, True, False, "20000"])
def test_a_payload_option_that_is_not_an_int_is_refused(cap: Any) -> None:
    with pytest.raises(ValueError, match=f"max_payload_size {re.escape(repr(cap))} is not an integer"):
        rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "cap", "max_payload_size": cap})


def test_the_floor_itself_still_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        conns = connecting(monkeypatch, 4 * 1024 * 1024)
        client = rpc_client.RPCClient(
            {"servers": ["nats://127.0.0.1:1"], "name": "cap", "max_payload_size": 16384}
        )
        await client.connect()
        await client.publish("events.big", b"x" * 20_000)

        sent = [p for p in conns[0].published if p["subject"] == "events.big"]
        header = decode(sent[0]["data"])
        assert header["chunkSize"] == 8192
        assert len(sent) == 1 + header["totalChunks"]

    asyncio.run(run())


@pytest.mark.parametrize("server_max", [4096, 8192, 16383])
def test_a_server_limit_below_the_floor_sends_what_fits_and_fails_the_rest(
    server_max: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def run() -> None:
        conns = connecting(monkeypatch, server_max)
        client = rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "cap"})
        await client.connect()
        nc = conns[0]

        await client.publish("events.small", b"x" * 100)
        [small] = [p for p in nc.published if p["subject"] == "events.small"]
        assert small["data"] == encode_message(b"x" * 100)

        with pytest.raises(RPCException) as published:
            await client.publish("events.big", b"x" * server_max)
        with pytest.raises(RPCException) as called:
            await asyncio.wait_for(client.call("rpc.ns.big", b"x" * server_max), 1)

        for caught in (published, called):
            assert caught.value.code == "PAYLOAD_TOO_LARGE"
            assert caught.value.message == f"Payload limit {server_max} is too small for chunking"
        assert [p for p in nc.published if p["subject"] in ("events.big", "rpc.ns.big")] == []
        assert client.pending_requests == {}

    asyncio.run(run())


def test_a_reply_over_a_limit_below_the_floor_reaches_the_caller_as_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        conns = connecting(monkeypatch, 8192)
        client = rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "cap"})
        await client.connect()
        nc = conns[0]
        await client.register_handler("ns", {"blob": lambda: b"x" * 20_000})
        [sub] = [s for s in nc._subs.values() if s._subject == "rpc.ns.blob"]
        request = encode({"id": "req.1", "method": "call", "params": []})
        await nc._process_msg(sub._id, b"rpc.ns.blob", b"", request, b"")
        await until(lambda: [p for p in nc.published if p["subject"] == "rpc.reply.req.1"])

        [reply] = [decode(p["data"]) for p in nc.published if p["subject"] == "rpc.reply.req.1"]
        assert "result" not in reply
        assert reply["error"]["code"] == "PAYLOAD_TOO_LARGE"
        assert reply["error"]["message"] == "Payload limit 8192 is too small for chunking"

    asyncio.run(run())


def test_a_service_reply_over_a_limit_below_the_floor_reaches_the_caller_as_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        conns = connecting(monkeypatch, 8192)
        client = rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "cap"})
        await client.connect()
        nc = conns[0]
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        await client.service.register_handler(
            ServiceConfig(name="svc", version="1.0.0"), {"blob": lambda: b"x" * 20_000}
        )
        failures: list[str] = []
        body = encode({"id": "req.1", "method": "call", "params": []})
        await svcm.service.endpoints["blob"](request(body, {}, failures))
        await until(lambda: [p for p in nc.published if p["subject"] == "svc.echo.reply.req.1"])

        [reply] = [decode(p["data"]) for p in nc.published if p["subject"] == "svc.echo.reply.req.1"]
        assert "result" not in reply
        assert reply["error"]["code"] == "PAYLOAD_TOO_LARGE"
        assert reply["error"]["message"] == "Payload limit 8192 is too small for chunking"
        assert failures == []

    asyncio.run(run())


@pytest.mark.parametrize("producer", ["catches the error", "lets the error out"])
@pytest.mark.parametrize("frame", ["over the limit", "not encodable"])
def test_a_pull_callback_frame_that_cannot_go_out_ends_the_session(frame: str, producer: str) -> None:
    async def run() -> None:
        caught: list[str] = []
        closed: list[bool] = []
        bad: Any = "f" * 5000 if frame == "over the limit" else object()

        async def play(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                for batch in range(3):
                    await invoker.invoke("frame", f"small {batch}")
                    try:
                        await invoker.invoke("frame", bad)
                    except Exception as error:
                        caught.append(type(error).__name__)
                        if producer == "lets the error out":
                            raise
                    yield None
            finally:
                closed.append(True)

        client, nc, _ = await serve({"play": play})
        client._max_payload_size = 4096
        client._unchunkable_limit = 4096
        response = await open_pull(nc, "play", ["frame"])
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response), 1)
        await wait_for(lambda: closed, 1)
        pull(nc, "next")
        await asyncio.sleep(0.05)

        [answer] = frames(nc, response)
        assert answer["type"] == "error"
        if frame == "over the limit":
            assert caught == ["RPCException"]
            assert answer["error"]["code"] == "PAYLOAD_TOO_LARGE"
            assert answer["error"]["message"] == "Payload limit 4096 is too small for chunking"
        else:
            assert caught == ["TypeError"]
            assert answer["error"]["code"] == "INTERNAL_ERROR"
        assert frames(nc, "_rpc.cb.it1") == [{"method": "frame", "args": ["small 0"]}]
        assert all(sub._closed for sub in subs_for(nc, "_rpc.iterator.it1.request"))

    asyncio.run(run())


def test_a_pull_callback_frame_that_fails_between_steps_answers_the_next_step() -> None:
    async def run() -> None:
        batches: list[int] = []
        closed: list[bool] = []
        late: list[asyncio.Task[None]] = []

        async def send_late(invoker: Any) -> None:
            with contextlib.suppress(RPCException):
                await invoker.invoke("frame", "f" * 5000)

        async def play(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                for batch in range(3):
                    batches.append(batch)
                    if batch == 0:
                        late.append(asyncio.create_task(send_late(invoker)))
                    yield None
            finally:
                closed.append(True)

        client, nc, _ = await serve({"play": play})
        client._max_payload_size = 4096
        client._unchunkable_limit = 4096
        response = await open_pull(nc, "play", ["frame"])
        await wait_for(lambda: late and late[0].done(), 1)
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response), 1)
        pull(nc, "next")
        await wait_for(lambda: len(frames(nc, response)) == 2, 1)
        await wait_for(lambda: closed, 1)

        first, second = frames(nc, response)
        assert first == {"id": "it1", "type": "value"}
        assert second["type"] == "error"
        assert second["error"]["code"] == "PAYLOAD_TOO_LARGE"
        assert batches == [0]
        assert frames(nc, "_rpc.cb.it1") == []

    asyncio.run(run())
