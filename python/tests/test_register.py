from __future__ import annotations

import asyncio
from typing import Any

import nats.aio.client as nats_aio_client
import pytest
from test_drain import DrainNC, make_client, until
from test_mux import make_client as make_mock_client

import camera_ui_rpc.client as rpc_client
import camera_ui_rpc.handler as handler

PING = b"PING\r\n"


class GatedNC(DrainNC):
    """Writes SUBs like nats-py does; a PING is answered once the test opens the gate."""

    def __init__(self, open_gate: bool = False) -> None:
        super().__init__()
        self.pong_gate = asyncio.Event()
        if open_gate:
            self.pong_gate.set()

    async def _send_subscribe(self, sub: Any) -> None:
        await nats_aio_client.Client._send_subscribe(self, sub)

    async def _send_command(self, cmd: bytes, priority: bool = False) -> None:
        self.commands.append(cmd)
        if cmd == PING:
            asyncio.ensure_future(self._answer())

    async def _answer(self) -> None:
        await self.pong_gate.wait()
        await self._process_pong()


def sub_line(nc: DrainNC, subject: str) -> bytes:
    sid = next(s._id for s in nc._subs.values() if s._subject == subject)
    return f"SUB {subject}  {sid}\r\n".encode()


def test_register_handler_returns_once_the_server_confirmed_its_subscriptions() -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = GatedNC()
        client.nc = nc
        task = asyncio.create_task(client.register_handler("ns", {"a": lambda: 1, "b": lambda: 2}))
        await until(lambda: PING in nc.commands)

        # one PING behind all SUBs, the method list last among them
        subjects = ("rpc.ns.a", "rpc.ns.b", "rpc.ns.__methods")
        assert nc.commands == [*(sub_line(nc, s) for s in subjects), PING]
        await asyncio.sleep(0.01)
        assert not task.done()

        nc.pong_gate.set()
        await asyncio.wait_for(task, 1)

    asyncio.run(run())


def test_register_handler_does_not_yield_per_subscription(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, _ = make_client()
        nc = GatedNC(open_gate=True)
        client.nc = nc
        yields = 0
        sleep = asyncio.sleep

        async def counting_sleep(delay: float, *args: Any) -> Any:
            nonlocal yields
            if delay == 0:
                yields += 1
            return await sleep(delay, *args)

        monkeypatch.setattr(asyncio, "sleep", counting_sleep)
        await client.register_handler("ns", {f"m{i}": (lambda: i) for i in range(40)})
        monkeypatch.setattr(asyncio, "sleep", sleep)

        assert yields == 0
        assert nc.commands.count(PING) == 1

    asyncio.run(run())


def test_a_registration_on_a_silent_link_returns_after_the_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rpc_client, "SUBSCRIBE_CONFIRM_TIMEOUT", 0.05)

    async def run() -> None:
        client, _ = make_client()
        nc = GatedNC()
        client.nc = nc
        await asyncio.wait_for(client.register_handler("ns", {"a": lambda: 1}), 1)
        assert nc.commands[-1] == PING

    asyncio.run(run())


def test_connect_restores_the_subscriptions_and_confirms_them_once(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        conns: list[GatedNC] = []

        async def fake_connect(**_options: Any) -> GatedNC:
            conns.append(GatedNC(open_gate=True))
            return conns[-1]

        monkeypatch.setattr(rpc_client, "connect", fake_connect)
        client, _ = make_client()
        client.nc = None
        await client.connect()
        await client.register_handler("ns", {"a": lambda: 1})
        await client.subscribe("events.x", lambda _data: None)

        await client.suspend()
        await client.connect()

        nc = conns[1]
        mux = f"rpc.reply.{client._reply_prefix}.>"
        subjects = (mux, "rpc.ns.a", "rpc.ns.__methods", "events.x")
        assert nc.commands == [*(sub_line(nc, s) for s in subjects), PING]

    asyncio.run(run())


def test_session_subjects_subscribe_without_a_yield_of_their_own() -> None:
    class Recording:
        is_connected = True

        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        async def subscribe(self, subject: str, _handler: Any, queue: str = "") -> Any:
            self.calls.append(("subscribe", subject))
            return self.unsubscribe

        async def _subscribe_session(self, subject: str, _handler: Any) -> Any:
            self.calls.append(("session", subject))
            return self.unsubscribe

        async def unsubscribe(self) -> None:
            pass

        async def publish(self, subject: str, data: Any) -> None:
            pass

    async def produce() -> Any:
        yield 1

    async def run() -> None:
        client: Any = Recording()
        pool = rpc_client.get_executor()
        await handler.handle_pull_iterator_request(produce, [], "it1", client, pool)
        await handler.handle_callback_request(lambda _cb: None, [], "rpc.cb.c1", "c1", client, pool)
        await asyncio.wait_for(handler.handle_stream_request(produce, [], "stream.s1", "s1", client, pool), 1)

        assert client.calls == [
            ("session", "_rpc.iterator.it1.request"),
            ("session", "rpc.cb.c1.cancel"),
            # a cancel can come at any time, its SUB still goes out on its own
            ("subscribe", "stream.s1.cancel"),
        ]

    asyncio.run(run())


class FlushNC(GatedNC):
    """nats-py without the queued PING: the confirm falls back to flush()."""

    def __init__(self) -> None:
        super().__init__(open_gate=True)
        self.flushed: list[list[bytes]] = []

    async def flush(self, timeout: int = 10) -> None:
        self.flushed.append(list(self.commands))


def test_without_a_queued_ping_registration_flushes_once_after_its_subs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rpc_client, "QUEUED_PING", False)

    async def run() -> None:
        client, _ = make_client()
        nc = FlushNC()
        client.nc = nc
        await client.register_handler("ns", {"a": lambda: 1, "b": lambda: 2})

        subjects = ("rpc.ns.a", "rpc.ns.b", "rpc.ns.__methods")
        assert nc.flushed == [[sub_line(nc, s) for s in subjects]]
        assert PING not in nc.commands

    asyncio.run(run())


def test_client_session_subjects_subscribe_without_a_yield_of_their_own() -> None:
    async def run() -> None:
        client, _ = make_mock_client()
        yields: dict[str, bool] = {}
        subscribe = client._subscribe

        async def recording(pattern: str, *args: Any, yield_after: bool = True) -> Any:
            yields[pattern] = yield_after
            return await subscribe(pattern, *args, yield_after=yield_after)

        async def call(_subject: str, params: Any = None, **_kwargs: Any) -> Any:
            return {"iteratorId": params["__iteratorId"]} if "__iteratorId" in params else True

        client._subscribe = recording  # type: ignore[method-assign]
        client.call = call  # type: ignore[method-assign]
        client._call_once = call  # type: ignore[method-assign]
        await client.call_with_callback("rpc.ns.listen", [], lambda _value: None)
        waiting = [
            asyncio.ensure_future(anext(client._call_stream_once("rpc.ns.items"))),
            asyncio.ensure_future(anext(client.call_pull_iterator("rpc.ns.pull"))),
            asyncio.ensure_future(client._call_once_service("svc.alpha.echo")),
        ]

        def sessions() -> dict[str, bool]:
            # the reply mux is no session subject
            return {p: y for p, y in yields.items() if not p.startswith("rpc.reply.")}

        await until(lambda: len(sessions()) == 4)
        for task in waiting:
            task.cancel()
        await asyncio.gather(*waiting, return_exceptions=True)

        assert sorted(p.split(".")[0] for p in sessions()) == ["_rpc", "rpc", "stream", "svc"]
        assert not any(sessions().values())

    asyncio.run(run())
