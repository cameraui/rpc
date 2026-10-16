from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

from test_drain import DrainNC, make_client, until
from test_handler import frames, open_pull, pull, serve

from camera_ui_rpc.codec import decode, encode


def sid_of(nc: DrainNC, subject: str) -> int:
    for sub in nc._subs.values():
        pattern = sub._subject
        if pattern == subject or (pattern.endswith(".>") and subject.startswith(pattern[:-1])):
            return int(sub._id)
    raise AssertionError(f"no subscription for {subject}")


async def deliver(nc: DrainNC, subject: str, body: Any) -> None:
    await nc._process_msg(sid_of(nc, subject), subject.encode(), b"", encode(body), b"")


def test_a_boundary_waits_only_for_the_callbacks_of_its_batch() -> None:
    async def run() -> None:
        client, nc = make_client()
        seen: list[str] = []
        held = asyncio.Event()

        async def frame(name: str) -> None:
            seen.append(name)
            if name == "next-batch":
                await held.wait()

        playback = client.call_pull_iterator_with_callback("rpc.ns.play", {"frame": frame}, ["frame"])
        first = asyncio.ensure_future(playback.__anext__())
        await until(lambda: any(p["subject"] == "rpc.ns.play" for p in nc.published))
        init = decode(next(p["data"] for p in nc.published if p["subject"] == "rpc.ns.play"))
        iterator = init["params"][0]["__iteratorId"]
        await deliver(nc, f"rpc.reply.{init['id']}", {"id": init["id"], "result": {"iteratorId": iterator}})
        await until(lambda: any(p["subject"] == f"_rpc.iterator.{iterator}.request" for p in nc.published))

        # a producer that runs ahead sends the next batch's first frame before the consumer asks
        await deliver(nc, f"_rpc.cb.{iterator}", {"method": "frame", "args": ["this-batch"]})
        await deliver(nc, f"_rpc.iterator.{iterator}.response", {"id": iterator, "type": "value"})
        await deliver(nc, f"_rpc.cb.{iterator}", {"method": "frame", "args": ["next-batch"]})

        await asyncio.wait_for(first, 1)
        assert seen[0] == "this-batch"
        held.set()
        await playback.aclose()

    asyncio.run(run())


def test_a_boundary_still_waits_for_the_callbacks_before_it() -> None:
    async def run() -> None:
        client, nc = make_client()
        release = asyncio.Event()
        done: list[str] = []

        async def frame(name: str) -> None:
            await release.wait()
            done.append(name)

        playback = client.call_pull_iterator_with_callback("rpc.ns.play", {"frame": frame}, ["frame"])
        first = asyncio.ensure_future(playback.__anext__())
        await until(lambda: any(p["subject"] == "rpc.ns.play" for p in nc.published))
        init = decode(next(p["data"] for p in nc.published if p["subject"] == "rpc.ns.play"))
        iterator = init["params"][0]["__iteratorId"]
        await deliver(nc, f"rpc.reply.{init['id']}", {"id": init["id"], "result": {"iteratorId": iterator}})
        await until(lambda: any(p["subject"] == f"_rpc.iterator.{iterator}.request" for p in nc.published))

        await deliver(nc, f"_rpc.cb.{iterator}", {"method": "frame", "args": ["a"]})
        # a liveness probe of the producer is skipped on the way
        await deliver(nc, f"_rpc.cb.{iterator}", {"type": "ping"})
        await deliver(nc, f"_rpc.cb.{iterator}", {"method": "frame", "args": ["b"]})
        await deliver(nc, f"_rpc.iterator.{iterator}.response", {"id": iterator, "type": "value"})
        await asyncio.sleep(0.02)
        assert not first.done()

        release.set()
        await asyncio.wait_for(first, 1)
        assert done == ["a", "b"]
        await playback.aclose()

    asyncio.run(run())


def test_a_pull_callback_producer_sends_its_first_batch_before_the_first_next() -> None:
    async def run() -> None:
        async def playback(invoker: Any) -> AsyncGenerator[None, None]:
            for i in range(3):
                await invoker.invoke("frame", i)
            yield None

        client, nc, _ = await serve({"playback": playback})
        response = await open_pull(nc, "playback", ["frame"])
        await until(lambda: len(frames(nc, "_rpc.cb.it1")) == 3)

        # the boundary waits for the consumer's next, it may not listen yet
        await asyncio.sleep(0.02)
        assert frames(nc, response) == []
        pull(nc, "next")
        await until(lambda: frames(nc, response))
        assert frames(nc, response) == [{"id": "it1", "type": "value"}]

    asyncio.run(run())


def test_an_eager_producer_that_ends_at_once_says_done_only_when_asked() -> None:
    async def run() -> None:
        async def empty(invoker: Any) -> AsyncGenerator[None, None]:
            return
            yield None

        client, nc, _ = await serve({"empty": empty})
        response = await open_pull(nc, "empty", ["frame"])
        await asyncio.sleep(0.02)
        assert frames(nc, response) == []

        pull(nc, "next")
        await until(lambda: frames(nc, response))
        assert frames(nc, response) == [{"id": "it1", "type": "done"}]

    asyncio.run(run())


def test_a_cancel_before_the_first_next_ends_with_one_done() -> None:
    async def run() -> None:
        closed: list[bool] = []

        async def playback(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                await invoker.invoke("frame", 0)
                yield None
                yield None
            finally:
                closed.append(True)

        client, nc, _ = await serve({"playback": playback})
        response = await open_pull(nc, "playback", ["frame"])
        await until(lambda: frames(nc, "_rpc.cb.it1"))

        pull(nc, "cancel")
        await until(lambda: closed)
        await asyncio.sleep(0.02)
        assert frames(nc, response) == [{"id": "it1", "type": "done"}]

    asyncio.run(run())
