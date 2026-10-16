from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, cast

import nats.aio.client as nats_aio_client
import pytest

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.client import RPCClient
from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.types import RPCClientOptions

CLOSED = {"code": "CONNECTION_CLOSED", "message": "Connection closed", "data": None}


class DrainNC(nats_aio_client.Client):
    """nats-py's client without a socket: commands are recorded and the server answers every PING."""

    def __init__(self) -> None:
        super().__init__()
        self._status = nats_aio_client.Client.CONNECTED
        self.commands: list[bytes] = []
        self.published: list[dict[str, Any]] = []

    async def _send_subscribe(self, sub: Any) -> None:
        pass

    async def _send_command(self, cmd: bytes, priority: bool = False) -> None:
        self.commands.append(cmd)
        if cmd == b"PING\r\n":
            asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(self._process_pong()))

    async def _flush_pending(self, force_flush: bool = False) -> Any:
        return None

    async def publish(
        self, subject: str, payload: bytes = b"", reply: str = "", headers: dict[str, str] | None = None
    ) -> None:
        self.published.append({"subject": subject, "data": payload})

    async def close(self) -> None:
        self._status = nats_aio_client.Client.CLOSED
        for sub in self._subs.values():
            if sub._wait_for_msgs_task is not None:
                sub._wait_for_msgs_task.cancel()


class SilentNC(DrainNC):
    """A half-dead link: nats-py still reports it connected, but no PONG comes back."""

    async def _send_command(self, cmd: bytes, priority: bool = False) -> None:
        self.commands.append(cmd)


def make_client(**options: Any) -> tuple[RPCClient, DrainNC]:
    client = RPCClient(
        cast(RPCClientOptions, {"servers": ["nats://127.0.0.1:1"], "name": "drain", **options})
    )
    nc = DrainNC()
    client.nc = nc
    return client, nc


def sub_of(nc: DrainNC, subject: str) -> Any:
    return next(s for s in nc._subs.values() if s._subject == subject)


async def deliver(nc: DrainNC, subject: str, seq: int, *params: Any) -> None:
    body = encode({"id": f"req.{seq}", "method": "call", "params": list(params)})
    await nc._process_msg(sub_of(nc, subject)._id, subject.encode(), b"", body, b"")


def replies(nc: DrainNC) -> list[dict[str, Any]]:
    return [decode(p["data"]) for p in nc.published if p["subject"].startswith("rpc.reply.req.")]


def answers(nc: DrainNC) -> dict[str, Any]:
    return {r["id"]: r.get("result", r.get("error")) for r in replies(nc)}


async def until(cond: Any, timeout: float = 2.0) -> None:
    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.001)


class Gate:
    def __init__(self) -> None:
        self.started: list[int] = []
        self.cancelled: list[int] = []
        self.open = asyncio.Event()

    async def work(self, seq: int) -> int:
        self.started.append(seq)
        try:
            await self.open.wait()
        except asyncio.CancelledError:
            self.cancelled.append(seq)
            raise
        return seq


@pytest.mark.parametrize("teardown", ["disconnect", "suspend", "cleanup"])
def test_teardown_unsubscribes_first_and_lets_received_calls_finish(teardown: str) -> None:
    async def run() -> None:
        client, nc = make_client()
        gate = Gate()
        cleanup = await client.register_handler("ns", {"work": gate.work, "other": gate.work})
        # the registration's own PING confirmed its subscriptions
        assert nc.commands == [b"PING\r\n"]
        nc.commands.clear()
        await deliver(nc, "rpc.ns.work", 0, 0)
        await deliver(nc, "rpc.ns.work", 1, 1)
        await until(lambda: gate.started == [0])

        stop = asyncio.create_task(cleanup() if teardown == "cleanup" else getattr(client, teardown)())
        await until(lambda: b"PING\r\n" in nc.commands)
        # the method list goes before the methods it lists
        subjects = ("rpc.ns.__methods", "rpc.ns.work", "rpc.ns.other")
        assert nc.commands[:4] == [
            *(f"UNSUB {sub_of(nc, s)._id} \r\n".encode() for s in subjects),
            b"PING\r\n",
        ]
        assert not stop.done()

        gate.open.set()
        await asyncio.wait_for(stop, 1)
        # the queued call ran too, both answered with their result
        assert gate.started == [0, 1]
        assert answers(nc) == {"req.0": 0, "req.1": 1}

    asyncio.run(run())


def test_a_handler_waiting_on_its_own_call_gets_the_reply_during_the_drain() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=200)

        async def mid() -> Any:
            return await client.call("rpc.up.find")

        await client.register_handler("ns", {"mid": mid})
        nc.commands.clear()
        await deliver(nc, "rpc.ns.mid", 0)
        await until(lambda: any(p["subject"] == "rpc.up.find" for p in nc.published))
        upstream = next(decode(p["data"]) for p in nc.published if p["subject"] == "rpc.up.find")

        stop = asyncio.create_task(client.disconnect())
        await until(lambda: b"PING\r\n" in nc.commands)
        mux = next(s for s in nc._subs.values() if s._subject.startswith("rpc.reply."))
        reply = encode({"id": upstream["id"], "result": 42})
        await nc._process_msg(mux._id, f"rpc.reply.{upstream['id']}".encode(), b"", reply, b"")
        await asyncio.wait_for(stop, 1)

        assert answers(nc) == {"req.0": 42}

    asyncio.run(run())


def test_calls_still_running_or_queued_at_the_deadline_get_connection_closed_once() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=50)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work})
        for seq in range(3):
            await deliver(nc, "rpc.ns.work", seq, seq)
        await until(lambda: gate.started == [0])

        started = time.monotonic()
        await client.disconnect()
        elapsed = time.monotonic() - started

        assert 0.04 < elapsed < 0.5
        assert gate.started == [0]
        assert gate.cancelled == [0]
        assert answers(nc) == {"req.0": CLOSED, "req.1": CLOSED, "req.2": CLOSED}
        assert len(replies(nc)) == 3

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["disconnect", "cleanup"])
def test_the_deadline_holds_when_the_pong_never_comes(teardown: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # registering waits for its own PONG too, bounded by this
    monkeypatch.setattr(rpc_client, "SUBSCRIBE_CONFIRM_TIMEOUT", 0.05)

    async def run() -> None:
        client, _ = make_client(drain_timeout_ms=50)
        nc = SilentNC()
        client.nc = nc
        gate = Gate()
        cleanup = await client.register_handler("ns", {"work": gate.work})
        nc.commands.clear()
        await deliver(nc, "rpc.ns.work", 0, 0)
        await until(lambda: gate.started == [0])

        started = time.monotonic()
        await asyncio.wait_for(cleanup() if teardown == "cleanup" else client.disconnect(), 1)

        assert time.monotonic() - started < 0.2
        assert b"PING\r\n" in nc.commands
        assert gate.cancelled == [0]
        assert answers(nc) == {"req.0": CLOSED}
        assert len(replies(nc)) == 1

    asyncio.run(run())


def test_opted_in_calls_finish_during_the_drain_and_the_rest_get_connection_closed() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=50)
        gate = Gate()
        quick: list[int] = []

        async def fast(seq: int) -> int:
            quick.append(seq)
            return seq

        await client.register_handler("ns", {"work": gate.work, "fast": fast}, concurrency={"work": 2})
        for seq in range(3):
            await deliver(nc, "rpc.ns.work", seq, seq)
        await deliver(nc, "rpc.ns.fast", 9, 9)
        await until(lambda: gate.started == [0, 1])

        await client.disconnect()

        # two calls ran and were cancelled at the deadline, the third waited for a slot
        assert sorted(gate.cancelled) == [0, 1]
        assert answers(nc) == {"req.0": CLOSED, "req.1": CLOSED, "req.2": CLOSED, "req.9": 9}
        assert client._concurrent_calls == set()  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_a_call_that_gets_its_slot_as_the_deadline_passes_gets_connection_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at_deadline: list[Callable[[], None]] = []

    class Registration(rpc_client._Registration):  # pyright: ignore[reportPrivateUsage]
        __slots__ = ("_expired",)

        @property
        def expired(self) -> bool:
            return self._expired

        @expired.setter
        def expired(self, value: bool) -> None:
            # a running call frees its slot in the loop iteration the drain marks the deadline in
            while value and at_deadline:
                at_deadline.pop()()
            self._expired = value

    monkeypatch.setattr(rpc_client, "_Registration", Registration)

    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=50)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work}, concurrency={"work": 2})
        for seq in range(3):
            await deliver(nc, "rpc.ns.work", seq, seq)
        await until(lambda: gate.started == [0, 1])
        entries = client._subscription_entries.values()  # pyright: ignore[reportPrivateUsage]
        handler = next(e["handler"] for e in entries if e["pattern"] == "rpc.ns.work")
        slots = handler.__kwdefaults__.values()
        at_deadline.append(next(d for d in slots if isinstance(d, asyncio.Semaphore)).release)

        await client.disconnect()

        assert gate.started == [0, 1]
        assert answers(nc) == {"req.0": CLOSED, "req.1": CLOSED, "req.2": CLOSED}
        assert len(replies(nc)) == 3

    asyncio.run(run())


def test_a_stream_request_that_never_started_gets_the_closed_frame() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=20)
        gate = Gate()

        async def items(_n: int) -> Any:
            yield 1

        await client.register_handler("ns", {"work": gate.work, "items": items})
        await deliver(nc, "rpc.ns.work", 0, 0)
        await until(lambda: gate.started == [0])
        stream_subject = "stream.rpc.ns.work.req.s"
        request = {
            "id": "req.s",
            "method": "stream",
            "params": {"__stream": True, "__streamSubject": stream_subject, "args": [1]},
        }
        await nc._process_msg(sub_of(nc, "rpc.ns.work")._id, b"rpc.ns.work", b"", encode(request), b"")

        await client.disconnect()

        frames = [decode(p["data"]) for p in nc.published if p["subject"] == stream_subject]
        assert frames == [{"id": "req.s", "type": "error", "error": CLOSED}]

    asyncio.run(run())


def test_drain_timeout_zero_keeps_the_immediate_teardown() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=0)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work})
        nc.commands.clear()
        await deliver(nc, "rpc.ns.work", 0, 0)
        await until(lambda: gate.started == [0])

        await client.disconnect()
        await asyncio.sleep(0.01)

        assert not any(c.startswith(b"PING") for c in nc.commands)
        assert gate.cancelled == [0]
        assert replies(nc) == []

    asyncio.run(run())


def test_cleanup_drains_only_its_own_registration() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=30)
        gate = Gate()

        async def echo(value: int) -> int:
            return value

        cleanup = await client.register_handler("a", {"work": gate.work})
        await client.register_handler("b", {"echo": echo})
        await deliver(nc, "rpc.a.work", 0, 0)
        await until(lambda: gate.started == [0])

        await cleanup()
        await deliver(nc, "rpc.b.echo", 1, 7)
        await until(lambda: len(replies(nc)) == 2)

        assert answers(nc) == {"req.0": CLOSED, "req.1": 7}
        assert [s._subject for s in nc._subs.values()] == ["rpc.b.echo", "rpc.b.__methods"]

    asyncio.run(run())


def test_handlers_answer_normally_again_after_suspend_and_connect() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=20)
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work})
        await deliver(nc, "rpc.ns.work", 0, 0)
        await until(lambda: gate.started == [0])

        await client.suspend()
        assert answers(nc) == {"req.0": CLOSED}

        nc2 = DrainNC()
        client.nc = nc2
        for entry in list(client._subscription_entries.values()):  # pyright: ignore[reportPrivateUsage]
            if entry["sub"] is None:
                await client._nats_subscribe(entry)  # pyright: ignore[reportPrivateUsage]
        gate.open.set()
        await deliver(nc2, "rpc.ns.work", 1, 1)
        await until(lambda: len(replies(nc2)) == 1)
        assert answers(nc2) == {"req.1": 1}

    asyncio.run(run())


@pytest.mark.parametrize("concurrency", [None, {"leave": 2}])
def test_a_handler_can_tear_down_its_own_registration(concurrency: Any) -> None:
    async def run() -> None:
        client, nc = make_client()
        registration: list[Any] = []

        async def leave() -> str:
            await registration[0]()
            return "bye"

        registration.append(await client.register_handler("ns", {"leave": leave}, concurrency=concurrency))
        await deliver(nc, "rpc.ns.leave", 0)
        # no wait for itself: well before the 250 ms deadline
        await until(lambda: len(replies(nc)) == 1, timeout=0.1)

        # a serial handler runs in its subscription's task, which its own unsubscribe cancels
        assert answers(nc) == {"req.0": "bye" if concurrency else CLOSED}

    asyncio.run(run())


def test_disconnect_drains_handlers_of_isolated_registrations_alongside() -> None:
    async def run() -> None:
        client, nc = make_client(drain_timeout_ms=60)
        child, child_nc = make_client(drain_timeout_ms=60)
        client.isolated_clients.append(child)
        gate = Gate()
        await client.register_handler("a", {"work": gate.work})
        await child.register_handler("b", {"work": gate.work})
        await deliver(nc, "rpc.a.work", 0, 0)
        await deliver(child_nc, "rpc.b.work", 1, 1)
        await until(lambda: sorted(gate.started) == [0, 1])

        started = time.monotonic()
        await client.disconnect()

        # side by side: one deadline, not one after the other
        assert time.monotonic() - started < 0.11
        assert answers(nc) == {"req.0": CLOSED}
        assert answers(child_nc) == {"req.1": CLOSED}

    asyncio.run(run())
