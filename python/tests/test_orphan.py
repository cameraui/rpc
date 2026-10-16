from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

import pytest
from test_client import SerialNC
from test_handler import deliver, frames, open_pull, open_stream, pull, wait_for
from test_mux import make_client, make_msg, no_responder_headers

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.codec import decode

FAST = {"interval_ms": 20, "grace_ms": 80}


async def serve(handlers: dict[str, Any], probe: Any = None, **options: Any) -> tuple[Any, SerialNC]:
    client, _ = make_client(**options)
    nc = SerialNC()
    client.nc = cast(Any, nc)
    await client.register_handler("ns", handlers, orphan_probe=probe)
    return client, nc


def probes(nc: SerialNC, subject: str) -> list[dict[str, Any]]:
    return [p for p in nc.published if p["subject"] == subject and p["reply"]]


def missing(client: Any, publish: dict[str, Any]) -> None:
    # what nats-server sends to the reply subject of a message no subscriber received
    client._handle_mux_message(make_msg(publish["reply"], b"", no_responder_headers()))


async def answer_missing_until(
    client: Any, nc: SerialNC, subject: str, done: Callable[[], bool], timeout: float = 2.0
) -> int:
    answered = 0
    deadline = time.monotonic() + timeout
    while not done():
        if time.monotonic() > deadline:
            raise AssertionError("the session did not end")
        sent = probes(nc, subject)
        for publish in sent[answered:]:
            missing(client, publish)
        answered = len(sent)
        await asyncio.sleep(0.002)
    return answered


def open_callback(nc: SerialNC, method: str) -> str:
    params = {"__callback": True, "__callbackSubject": "rpc.cb.c1", "args": []}
    deliver(nc, f"rpc.ns.{method}", {"id": "c1", "method": "call", "params": [params]})
    return "rpc.cb.c1"


class Listener:
    def __init__(self) -> None:
        self.cleaned = False
        self.emit: Any = None

    async def listen(self, emit: Any) -> Any:
        self.emit = emit

        def cleanup() -> None:
            self.cleaned = True

        return cleanup


def test_a_callback_session_ends_once_its_consumer_stays_missing() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, FAST)
        subject = open_callback(nc, "listen")
        await wait_for(lambda: client.callback_cleanups)
        started = time.monotonic()

        answered = await answer_missing_until(client, nc, subject, lambda: listener.cleaned)

        # idle: the probes were pings, and the consumer stayed missing for the grace period
        assert {decode(p["data"])["type"] for p in probes(nc, subject)} == {"ping"}
        assert decode(probes(nc, subject)[0]["data"]) == {"id": "c1", "type": "ping"}
        assert answered >= 2
        assert time.monotonic() - started >= 0.08
        assert client.callback_cleanups == {}
        # values after the end go nowhere
        await listener.emit("late")
        assert all(decode(p["data"])["type"] == "ping" for p in nc.published if p["subject"] == subject)

    asyncio.run(run())


def test_a_probe_without_a_status_keeps_the_session() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, FAST)
        subject = open_callback(nc, "listen")
        await wait_for(lambda: len(probes(nc, subject)) >= 1)
        # the first probe found nobody, the later ones found the consumer back
        missing(client, probes(nc, subject)[0])
        await asyncio.sleep(0.3)

        assert len(probes(nc, subject)) >= 5
        assert not listener.cleaned
        assert list(client.callback_cleanups) == ["c1"]

    asyncio.run(run())


def test_data_carries_the_probe_at_most_once_per_interval() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, {"interval_ms": 30, "grace_ms": 1000})
        subject = open_callback(nc, "listen")
        await wait_for(lambda: listener.emit is not None)
        started = time.monotonic()
        while time.monotonic() - started < 0.2:
            await listener.emit("v")
            await asyncio.sleep(0.002)

        sent = [decode(p["data"])["type"] for p in probes(nc, subject)]
        data = [p for p in nc.published if p["subject"] == subject]
        assert 3 <= sent.count("data") <= 8
        assert len(sent) <= 8
        assert len(data) > 40

    asyncio.run(run())


def test_a_stream_ends_once_its_consumer_stays_missing() -> None:
    async def run() -> None:
        closed: list[bool] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(10)
            finally:
                closed.append(True)

        client, nc = await serve({"ticks": ticks}, FAST)
        subject = open_stream(nc, "ticks")
        await wait_for(lambda: frames(nc, subject))

        await answer_missing_until(client, nc, subject, lambda: bool(closed))

        assert decode(probes(nc, subject)[0]["data"]) == {"id": "s1", "type": "ping"}
        await wait_for(lambda: client.stream_cleanups == {})
        # ended like a cancel: no end or error frame
        assert {f["type"] for f in frames(nc, subject)} == {"data", "ping"}

    asyncio.run(run())


def test_a_parked_pull_session_pings_the_consumer_inbox_and_ends() -> None:
    async def run() -> None:
        closed: list[bool] = []

        async def playback(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                while True:
                    await invoker.invoke("frame", 1)
                    yield None
            finally:
                closed.append(True)

        client, nc = await serve({"playback": playback}, FAST)
        response = await open_pull(nc, "playback", ["frame"])
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))

        # parked: nothing to publish, the ping goes to the consumer's reply inbox for this iterator
        await answer_missing_until(client, nc, "rpc.reply.it1", lambda: bool(closed))

        assert decode(probes(nc, "rpc.reply.it1")[0]["data"]) == {"type": "ping"}
        await wait_for(lambda: client.pull_iterator_cleanups == {})

    asyncio.run(run())


def test_a_pulling_consumer_is_not_probed() -> None:
    async def run() -> None:
        async def items() -> AsyncGenerator[int, None]:
            i = 0
            while True:
                i += 1
                yield i

        client, nc = await serve({"items": items}, FAST)
        response = await open_pull(nc, "items")
        started = time.monotonic()
        while time.monotonic() - started < 0.15:
            pull(nc, "next")
            await asyncio.sleep(0.005)

        assert frames(nc, response)
        assert probes(nc, "rpc.reply.it1") == []

    asyncio.run(run())


def test_the_probe_can_be_turned_off_and_back_on_per_registration() -> None:
    async def run() -> None:
        off = Listener()
        client, nc = await serve({"listen": off.listen}, None, orphan_probe=False)
        open_callback(nc, "listen")
        await asyncio.sleep(0.1)
        assert probes(nc, "rpc.cb.c1") == []

        on = Listener()
        client, nc = await serve({"listen": on.listen}, FAST, orphan_probe=False)
        open_callback(nc, "listen")
        await wait_for(lambda: probes(nc, "rpc.cb.c1"))

    asyncio.run(run())


def test_zero_or_negative_values_take_the_defaults() -> None:
    client, _ = make_client()
    assert client._orphan_probe_settings({"interval_ms": 0}) == (30.0, 300.0)
    assert client._orphan_probe_settings({"interval_ms": -5, "grace_ms": -1}) == (30.0, 300.0)
    assert client._orphan_probe_settings({"interval_ms": 20, "grace_ms": 0}) == (0.02, 300.0)


def test_a_zero_interval_does_not_ping_a_parked_session() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, {"interval_ms": 0})
        subject = open_callback(nc, "listen")
        await wait_for(lambda: client.callback_cleanups)
        await asyncio.sleep(0.1)
        assert probes(nc, subject) == []

    asyncio.run(run())


def test_a_zero_grace_keeps_the_session_for_the_default_grace() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, {"interval_ms": 20, "grace_ms": 0})
        subject = open_callback(nc, "listen")
        await wait_for(lambda: len(probes(nc, subject)) >= 3)
        for publish in probes(nc, subject):
            missing(client, publish)
        await asyncio.sleep(0.05)
        assert not listener.cleaned
        assert list(client.callback_cleanups) == ["c1"]

    asyncio.run(run())


def test_a_probe_that_got_through_resets_the_suspicion() -> None:
    async def run() -> None:
        listener = Listener()
        client, nc = await serve({"listen": listener.listen}, FAST)
        subject = open_callback(nc, "listen")
        await wait_for(lambda: probes(nc, subject))
        missing(client, probes(nc, subject)[0])
        # the probes after it reached the consumer, so a later miss past the grace starts over
        await asyncio.sleep(0.15)
        seen = len(probes(nc, subject))
        await wait_for(lambda: len(probes(nc, subject)) > seen)
        latest = probes(nc, subject)[-1]
        assert latest["reply"].removeprefix("rpc.reply.") in client.status_handlers
        missing(client, latest)
        await asyncio.sleep(0.03)

        assert not listener.cleaned
        assert list(client.callback_cleanups) == ["c1"]

    asyncio.run(run())


def test_a_session_that_published_within_the_interval_gets_no_idle_ping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    pings: list[float] = []
    monkeypatch.setattr(rpc_client.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(rpc_client._OrphanProbe, "_arm", lambda self, delay: None)
    monkeypatch.setattr(rpc_client, "run_in_background", lambda work: (pings.append(now[0]), work.close()))

    async def ping(_reply: str) -> None:
        pass

    client, _ = make_client()
    client.nc = cast(Any, SerialNC())
    probe = rpc_client._OrphanProbe(client, 1.0, 10.0)
    probe._ping = ping

    def publish(at: float) -> None:
        now[0] = at
        probe.sent = True
        if probe.due:
            probe.reply()

    def tick(at: float) -> None:
        now[0] = at
        probe._tick()

    publish(0.5)
    tick(1.0)
    publish(1.003)
    publish(1.5)
    # early tick: the probe went out 0.997 s ago, the timer re-arms for the rest
    tick(2.0)
    tick(2.003)
    assert pings == []
    assert probe.due

    tick(3.003)
    assert pings == [3.003]


def test_a_busy_stream_carries_the_probe_on_its_items() -> None:
    async def run() -> None:
        async def ticks() -> AsyncGenerator[int, None]:
            started = time.monotonic()
            while time.monotonic() - started < 0.2:
                yield 1
                await asyncio.sleep(0.002)

        client, nc = await serve({"ticks": ticks}, {"interval_ms": 30, "grace_ms": 1000})
        subject = open_stream(nc, "ticks")
        await wait_for(lambda: any(f["type"] == "end" for f in frames(nc, subject)))

        carried = [decode(p["data"])["type"] for p in probes(nc, subject)]
        assert carried.count("data") >= 3
        assert len(carried) <= 8

    asyncio.run(run())


def test_a_busy_pull_callback_session_carries_the_probe_on_its_callbacks() -> None:
    async def run() -> None:
        busy = asyncio.Event()

        async def playback(invoker: Any) -> AsyncGenerator[None, None]:
            started = time.monotonic()
            while time.monotonic() - started < 0.2:
                await invoker.invoke("frame", 1)
                await asyncio.sleep(0.002)
            busy.set()
            yield None

        client, nc = await serve({"playback": playback}, {"interval_ms": 30, "grace_ms": 1000})
        await open_pull(nc, "playback", ["frame"])
        await asyncio.wait_for(busy.wait(), 2)

        # an idle probe of a pull session goes to the consumer's reply inbox, these all carry a callback
        carried = probes(nc, "_rpc.cb.it1")
        assert 3 <= len(carried) <= 8
        assert all(decode(p["data"]) == {"method": "frame", "args": [1]} for p in carried)

    asyncio.run(run())
