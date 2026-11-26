from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from test_drain import DrainNC, answers, make_client, until

from camera_ui_rpc import CALLER_ROLE_HEADER, caller_is_viewer
from camera_ui_rpc.codec import decode, encode

VIEWER = (CALLER_ROLE_HEADER, "viewer")


def header_block(*lines: tuple[str, str]) -> bytes:
    return b"NATS/1.0\r\n" + b"".join(f"{key}: {value}\r\n".encode() for key, value in lines) + b"\r\n"


async def viewer(*_args: Any) -> bool:
    await asyncio.sleep(0)
    return caller_is_viewer()


def sync_viewer() -> bool:
    return caller_is_viewer()


def stream_viewer() -> Iterator[bool]:
    yield caller_is_viewer()


async def send(nc: DrainNC, subject: str, body: bytes, headers: bytes = b"") -> None:
    namespace = subject.rsplit(".", 1)[0] + ".>"
    sid = next(s._id for s in nc._subs.values() if s._subject in (subject, namespace))
    await nc._process_msg(sid, subject.encode(), b"", body, headers)


async def ask(nc: DrainNC, subject: str, seq: int, headers: bytes = b"") -> None:
    await send(nc, subject, encode({"id": f"req.{seq}", "method": "call", "params": []}), headers)


@pytest.mark.parametrize(
    "options",
    [{}, {"namespace_subscription": True}, {"drop_expired": True}],
    ids=["per method", "namespace", "drop expired"],
)
def test_a_handler_sees_a_viewer_caller(options: dict[str, Any]) -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.register_handler("who", {"viewer": viewer, "sync": sync_viewer}, **options)
        await ask(nc, "rpc.who.viewer", 0)
        await ask(nc, "rpc.who.viewer", 1, header_block(VIEWER))
        await ask(nc, "rpc.who.viewer", 2, header_block((CALLER_ROLE_HEADER, "admin"), VIEWER))
        await ask(nc, "rpc.who.sync", 3, header_block(VIEWER))
        await until(lambda: len(answers(nc)) == 4)
        assert answers(nc) == {"req.0": False, "req.1": True, "req.2": True, "req.3": True}

    asyncio.run(run())


@pytest.mark.parametrize("namespace_subscription", [False, True])
def test_a_chunked_request_keeps_its_viewer_caller(namespace_subscription: bool) -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.register_handler(
            "who", {"viewer": viewer}, namespace_subscription=namespace_subscription
        )
        body = encode({"id": "req.0", "method": "call", "params": ["x" * 64]})
        step = 16
        header = {
            "type": "chunked",
            "transferId": "t1",
            "totalChunks": -(-len(body) // step),
            "totalSize": len(body),
            "chunkSize": step,
        }
        chunked = (("x-chunked-transfer", "header"), ("x-chunk-id", "t1"), VIEWER)
        await send(nc, "rpc.who.viewer", encode(header), header_block(*chunked))
        for index, start in enumerate(range(0, len(body), step)):
            lines = (("x-chunked-transfer", "chunk"), ("x-chunk-id", "t1"), ("x-chunk-index", str(index)))
            await send(nc, "rpc.who.viewer", body[start : start + step], header_block(*lines))
        await until(lambda: "req.0" in answers(nc))
        assert answers(nc) == {"req.0": True}

    asyncio.run(run())


def test_a_sync_stream_handler_sees_its_viewer_caller() -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.register_handler("who", {"stream": stream_viewer})
        body = encode(
            {"id": "s.1", "method": "call", "params": {"__stream": True, "__streamSubject": "stream.s.1"}}
        )
        await send(nc, "rpc.who.stream", body, header_block(VIEWER))
        await until(lambda: any(p["subject"] == "stream.s.1" for p in nc.published))
        first = decode(next(p["data"] for p in nc.published if p["subject"] == "stream.s.1"))
        assert first == {"id": "s.1", "type": "data", "data": True}

    asyncio.run(run())


def test_a_pull_handler_sees_its_viewer_caller_on_every_step() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def pull_viewer() -> AsyncIterator[bool]:
            yield caller_is_viewer()
            yield caller_is_viewer()

        await client.register_handler("who", {"pull": pull_viewer})
        body = encode(
            {
                "id": "p1",
                "method": "call",
                "params": [{"__pullIterator": True, "__iteratorId": "p1", "args": []}],
            }
        )
        await send(nc, "rpc.who.pull", body, header_block(VIEWER))
        await until(lambda: any(p["subject"] == "rpc.reply.p1" for p in nc.published))
        responses = "_rpc.iterator.p1.response"
        for step in (1, 2):
            await send(nc, "_rpc.iterator.p1.request", encode({"id": "p1", "type": "next"}))
            await until(lambda: sum(p["subject"] == responses for p in nc.published) == step)
        values = [decode(p["data"])["value"] for p in nc.published if p["subject"] == responses]
        assert values == [True, True]

    asyncio.run(run())


def test_a_handler_registered_during_a_viewer_call_tells_the_next_caller_apart() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def register() -> bool:
            await client.register_handler("late", {"viewer": viewer})
            return caller_is_viewer()

        await client.register_handler("who", {"register": register})
        await ask(nc, "rpc.who.register", 0, header_block(VIEWER))
        await until(lambda: len(answers(nc)) == 1)
        await ask(nc, "rpc.late.viewer", 1)
        await until(lambda: len(answers(nc)) == 2)
        assert answers(nc) == {"req.0": True, "req.1": False}

    asyncio.run(run())


def test_outside_a_handler_there_is_no_viewer() -> None:
    assert caller_is_viewer() is False
