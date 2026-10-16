from __future__ import annotations

import asyncio
from typing import Any

import pytest
from test_drain import Gate, answers, make_client, sub_of, until
from test_mux import make_client as make_mock_client
from test_register import GatedNC

import camera_ui_rpc.client as rpc_client
import camera_ui_rpc.patches as patches
from camera_ui_rpc.codec import decode, encode


async def request(nc: Any, subject: str, seq: int, ttl: int | None, *params: Any) -> None:
    body: dict[str, Any] = {"id": f"req.{seq}", "method": "call", "params": list(params)}
    if ttl is not None:
        body["__ttl"] = ttl
    sid = next(s._id for s in nc._subs.values() if s._subject in (subject, subject.rsplit(".", 1)[0] + ".>"))
    await nc._process_msg(sid, subject.encode(), b"", encode(body), b"")


def test_a_call_tells_the_responder_its_timeout() -> None:
    async def run() -> None:
        client, nc = make_mock_client(timeout=1234)
        task = asyncio.create_task(client.call("rpc.ns.echo", timeout=700))
        other = asyncio.create_task(client.call("rpc.ns.echo"))
        await until(lambda: len(nc.published) == 2)
        assert [decode(p["data"])["__ttl"] for p in nc.published] == [700, 1234]
        task.cancel()
        other.cancel()

    asyncio.run(run())


def test_the_timeout_goes_out_as_whole_milliseconds() -> None:
    async def run() -> None:
        client, nc = make_mock_client()
        calls = [asyncio.create_task(client.call("rpc.ns.echo", timeout=t)) for t in (1500.5, 0.2)]
        await until(lambda: len(nc.published) == 2)
        ttls = [decode(p["data"])["__ttl"] for p in nc.published]
        assert ttls == [1501, 1]
        assert all(type(ttl) is int for ttl in ttls)
        for task in calls:
            task.cancel()

    asyncio.run(run())


@pytest.mark.parametrize("namespace_subscription", [False, True])
def test_an_opted_in_method_skips_a_request_whose_caller_gave_up(namespace_subscription: bool) -> None:
    async def run() -> None:
        client, nc = make_client()
        gate = Gate()
        await client.register_handler(
            "ns", {"work": gate.work}, drop_expired=True, namespace_subscription=namespace_subscription
        )
        await request(nc, "rpc.ns.work", 0, 10000, 0)
        await until(lambda: gate.started == [0])
        await request(nc, "rpc.ns.work", 1, 20, 1)
        await request(nc, "rpc.ns.work", 2, 10000, 2)
        await asyncio.sleep(0.06)

        gate.open.set()
        await until(lambda: len(answers(nc)) == 2)
        await asyncio.sleep(0.01)
        # the second request waited 60 ms for a 20 ms caller: not run, not answered
        assert gate.started == [0, 2]
        assert answers(nc) == {"req.0": 0, "req.2": 2}

    asyncio.run(run())


def test_without_the_option_every_request_runs() -> None:
    async def run() -> None:
        client, nc = make_client()
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work})
        await request(nc, "rpc.ns.work", 0, 10000, 0)
        await until(lambda: gate.started == [0])
        await request(nc, "rpc.ns.work", 1, 20, 1)
        await asyncio.sleep(0.06)

        gate.open.set()
        await until(lambda: len(answers(nc)) == 2)
        assert gate.started == [0, 1]

    asyncio.run(run())


def test_only_the_listed_methods_skip() -> None:
    async def run() -> None:
        client, nc = make_client()
        gate = Gate()
        keep = Gate()
        await client.register_handler("ns", {"work": gate.work, "keep": keep.work}, drop_expired=["work"])
        for method, g in (("work", gate), ("keep", keep)):
            await request(nc, f"rpc.ns.{method}", 0 if method == "work" else 10, 10000, 0)
            await until(lambda g=g: g.started == [0])
        await request(nc, "rpc.ns.work", 1, 20, 1)
        await request(nc, "rpc.ns.keep", 11, 20, 1)
        await asyncio.sleep(0.06)

        gate.open.set()
        keep.open.set()
        await until(lambda: len(answers(nc)) == 3)
        await asyncio.sleep(0.01)
        assert gate.started == [0]
        assert keep.started == [0, 1]

    asyncio.run(run())


def test_the_wait_for_a_concurrency_slot_counts() -> None:
    async def run() -> None:
        client, nc = make_client()
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work}, drop_expired=True, concurrency={"work": 2})
        await request(nc, "rpc.ns.work", 0, 10000, 0)
        await request(nc, "rpc.ns.work", 1, 10000, 1)
        await until(lambda: gate.started == [0, 1])
        await request(nc, "rpc.ns.work", 2, 20, 2)
        await asyncio.sleep(0.06)

        gate.open.set()
        await until(lambda: len(answers(nc)) == 2)
        await asyncio.sleep(0.01)
        assert gate.started == [0, 1]

    asyncio.run(run())


def test_the_connection_stamps_arrivals_once_a_method_opts_in() -> None:
    async def run() -> None:
        client, nc = make_client()
        await client.register_handler("ns", {"a": lambda: 1})
        assert nc.msg_class is not patches.ArrivalMsg
        await client.register_handler("other", {"b": lambda: 1}, drop_expired=["b"])
        assert nc.msg_class is patches.ArrivalMsg
        await nc._process_msg(sub_of(nc, "rpc.other.b")._id, b"rpc.other.b", b"", encode({}), b"")

    asyncio.run(run())


def test_a_new_connection_stamps_arrivals_too(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        conns: list[GatedNC] = []

        async def fake_connect(**_options: Any) -> GatedNC:
            conns.append(GatedNC(open_gate=True))
            return conns[-1]

        monkeypatch.setattr(rpc_client, "connect", fake_connect)
        client, _ = make_mock_client()
        client.nc = None
        await client.connect()
        gate = Gate()
        await client.register_handler("ns", {"work": gate.work}, drop_expired=True)
        await client.suspend()
        await client.connect()

        nc = conns[1]
        assert nc.msg_class is patches.ArrivalMsg
        await request(nc, "rpc.ns.work", 0, 10000, 0)
        await until(lambda: gate.started == [0])
        await request(nc, "rpc.ns.work", 1, 20, 1)
        await asyncio.sleep(0.06)

        gate.open.set()
        await until(lambda: answers(nc))
        await asyncio.sleep(0.01)
        assert gate.started == [0]
        assert answers(nc) == {"req.0": 0}

    asyncio.run(run())
