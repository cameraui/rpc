from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

import pytest
from nats.micro.service import ServiceConfig
from test_client import SerialNC, SerialSub, dialing, requests_to, restore, subs_for
from test_mux import MockNC, make_client, make_msg
from test_service import FakeSvcm, request

import camera_ui_rpc.handler as handler
from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.errors import RPCException

CONNECTION_CLOSED = {"code": "CONNECTION_CLOSED", "message": "Connection closed"}


async def serve(handlers: dict[str, Any]) -> tuple[Any, SerialNC, Any]:
    client, _ = make_client()
    nc = SerialNC()
    client.nc = cast(Any, nc)
    unregister = await client.register_handler("ns", handlers)
    return client, nc, unregister


async def wait_for(cond: Callable[[], Any], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.001)


def deliver(nc: SerialNC, subject: str, body: dict[str, Any]) -> None:
    sub = subs_for(nc, subject)[0]
    assert isinstance(sub, SerialSub)
    sub.pending.put_nowait(make_msg(subject, encode(body)))


def frames(nc: MockNC, subject: str) -> list[dict[str, Any]]:
    return [decode(p["data"]) for p in nc.published if p["subject"] == subject]


def count(nc: MockNC, subject: str) -> int:
    return sum(1 for p in nc.published if p["subject"] == subject)


def open_stream(nc: SerialNC, method: str) -> str:
    subject = f"stream.rpc.ns.{method}.s1"
    params = {"__stream": True, "__streamSubject": subject, "args": []}
    deliver(nc, f"rpc.ns.{method}", {"id": "s1", "method": "stream", "params": params})
    return subject


async def open_pull(nc: SerialNC, method: str, callbacks: list[str] | None = None) -> str:
    params: dict[str, Any] = {"__pullIterator": True, "__iteratorId": "it1", "args": []}
    if callbacks is not None:
        params = {
            "__pullCallback": True,
            "__iteratorId": "it1",
            "__callbackSubject": "_rpc.cb.it1",
            "__callbackMethods": callbacks,
            "__onewayMethods": callbacks,
            "args": [],
        }
    deliver(nc, f"rpc.ns.{method}", {"id": "init.1", "method": "call", "params": [params]})
    await wait_for(lambda: subs_for(nc, "_rpc.iterator.it1.request"))
    return "_rpc.iterator.it1.response"


def pull(nc: SerialNC, kind: str) -> None:
    deliver(nc, "_rpc.iterator.it1.request", {"id": "it1", "type": kind})


def test_stream_cancel_reaches_a_producer_parked_in_a_wait() -> None:
    async def run() -> None:
        closed: list[float] = []
        cancelled: list[bool] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
                yield 1
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
            finally:
                closed.append(time.monotonic())

        client, nc, _ = await serve({"ticks": ticks})
        subject = open_stream(nc, "ticks")
        await wait_for(lambda: frames(nc, subject))
        deliver(nc, f"{subject}.cancel", {"id": "s1"})
        sent = time.monotonic()

        await wait_for(lambda: closed, 2)
        assert closed[0] - sent < 1
        assert cancelled == [True]
        await asyncio.sleep(0.02)
        assert frames(nc, subject) == [{"id": "s1", "type": "data", "data": 0}]
        assert client.stream_cleanups == {}

    asyncio.run(run())


def test_stream_cancel_during_handler_setup_ends_the_session() -> None:
    async def run() -> None:
        setup: list[str] = []

        async def slow_start() -> Any:
            try:
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                setup.append("cancelled")
                raise
            setup.append("done")

            async def items() -> AsyncGenerator[int, None]:
                yield 1

            return items()

        _, nc, _ = await serve({"slowStart": slow_start})
        subject = open_stream(nc, "slowStart")
        await wait_for(lambda: subs_for(nc, f"{subject}.cancel"))
        deliver(nc, f"{subject}.cancel", {"id": "s1"})

        await wait_for(lambda: setup, 3)
        assert setup == ["cancelled"]
        await asyncio.sleep(0.05)
        assert frames(nc, subject) == []

    asyncio.run(run())


def test_stream_cancel_is_read_while_a_tight_producer_runs() -> None:
    async def run() -> None:
        closed: list[int] = []

        async def flood() -> AsyncGenerator[int, None]:
            i = 0
            try:
                while i < 30000:
                    yield i
                    i += 1
            finally:
                closed.append(i)

        _, nc, _ = await serve({"flood": flood})
        subject = open_stream(nc, "flood")
        await wait_for(lambda: frames(nc, subject))
        deliver(nc, f"{subject}.cancel", {"id": "s1"})

        await wait_for(lambda: closed, 10)
        sent = frames(nc, subject)
        assert closed[0] < 30000
        assert len(sent) < 30000
        assert all(frame["type"] == "data" for frame in sent)

    asyncio.run(run())


def test_stream_that_ends_on_its_own_leaves_no_session_behind() -> None:
    async def run() -> None:
        async def three() -> AsyncGenerator[int, None]:
            for i in range(3):
                yield i

        client, nc, _ = await serve({"three": three})
        subject = open_stream(nc, "three")

        await wait_for(lambda: len(frames(nc, subject)) == 4)
        assert frames(nc, subject)[-1] == {"id": "s1", "type": "end"}
        await wait_for(lambda: client.stream_cleanups == {})
        await wait_for(lambda: subs_for(nc, f"{subject}.cancel")[0]._closed)

    asyncio.run(run())


def test_pull_cancel_answers_done_at_once_while_a_next_waits_on_the_producer() -> None:
    async def run() -> None:
        closed: list[float] = []

        async def slow() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
                yield 1
            finally:
                closed.append(time.monotonic())

        client, nc, unregister = await serve({"slow": slow})
        response = await open_pull(nc, "slow")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))
        pull(nc, "next")
        await asyncio.sleep(0.05)
        pull(nc, "cancel")
        sent = time.monotonic()

        await wait_for(lambda: len(frames(nc, response)) == 2, 2)
        assert time.monotonic() - sent < 1
        await wait_for(lambda: closed, 2)
        await unregister()
        await asyncio.sleep(0.05)
        assert frames(nc, response) == [
            {"id": "it1", "type": "value", "value": 0},
            {"id": "it1", "type": "done"},
        ]
        assert len(closed) == 1
        assert client.pull_iterator_cleanups == {}

    asyncio.run(run())


def test_pull_nexts_run_one_after_another() -> None:
    async def run() -> None:
        async def counter() -> AsyncGenerator[int, None]:
            for i in range(3):
                await asyncio.sleep(0.01)
                yield i

        _, nc, _ = await serve({"counter": counter})
        response = await open_pull(nc, "counter")
        for _ in range(4):
            pull(nc, "next")

        await wait_for(lambda: len(frames(nc, response)) == 4)
        assert [frame.get("value") for frame in frames(nc, response)[:3]] == [0, 1, 2]
        assert frames(nc, response)[3] == {"id": "it1", "type": "done"}

    asyncio.run(run())


def test_pull_callback_cancel_stops_the_batch_at_once() -> None:
    async def run() -> None:
        closed: list[float] = []

        async def batch(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                for i in range(50):
                    await invoker.invoke("frame", i)
                    await asyncio.sleep(0.05)
                yield None
            finally:
                closed.append(time.monotonic())

        _, nc, _ = await serve({"batch": batch})
        response = await open_pull(nc, "batch", ["frame"])
        pull(nc, "next")
        await asyncio.sleep(0.1)
        pull(nc, "cancel")
        before = len(frames(nc, "_rpc.cb.it1"))

        await wait_for(lambda: frames(nc, response), 1)
        await wait_for(lambda: closed, 1)
        await asyncio.sleep(0.1)
        assert len(frames(nc, "_rpc.cb.it1")) - before <= 1
        assert frames(nc, response) == [{"id": "it1", "type": "done"}]

    asyncio.run(run())


def test_pull_callback_cancel_is_read_while_a_tight_batch_invokes() -> None:
    async def run() -> None:
        async def tight(invoker: Any) -> AsyncGenerator[None, None]:
            for i in range(100000):
                await invoker.invoke("frame", i)
            yield None

        _, nc, _ = await serve({"tight": tight})
        response = await open_pull(nc, "tight", ["frame"])
        pull(nc, "next")
        await wait_for(lambda: count(nc, "_rpc.cb.it1"))
        before = count(nc, "_rpc.cb.it1")
        pull(nc, "cancel")

        await wait_for(lambda: count(nc, response), 10)
        assert frames(nc, response) == [{"id": "it1", "type": "done"}]
        # a 2 ms guard lets about 2000 through on the fake, a 10 ms one about 10000
        assert count(nc, "_rpc.cb.it1") - before < 5000

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["unregister", "disconnect", "suspend"])
def test_teardown_ends_every_open_session_with_connection_closed(teardown: str) -> None:
    async def run() -> None:
        closed: list[str] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                i = 0
                while True:
                    yield i
                    i += 1
                    await asyncio.sleep(0.005)
            finally:
                closed.append("push")

        async def slow() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
                yield 1
            finally:
                closed.append("pull")

        client, nc, unregister = await serve({"ticks": ticks, "slow": slow})
        subject = open_stream(nc, "ticks")
        response = await open_pull(nc, "slow")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))
        pull(nc, "next")
        await wait_for(lambda: len(frames(nc, subject)) >= 3)

        started = time.monotonic()
        if teardown == "unregister":
            await unregister()
        else:
            await getattr(client, teardown)()
        assert time.monotonic() - started < 1
        counts = (len(frames(nc, subject)), len(frames(nc, response)))

        await wait_for(lambda: sorted(closed) == ["pull", "push"], 2)
        await asyncio.sleep(0.05)
        assert (len(frames(nc, subject)), len(frames(nc, response))) == counts
        for last in (frames(nc, subject)[-1], frames(nc, response)[-1]):
            assert last["type"] == "error"
            assert {key: last["error"][key] for key in ("code", "message")} == CONNECTION_CLOSED
        assert [frame["type"] for frame in frames(nc, response)] == ["value", "error"]
        assert client.stream_cleanups == {} and client.pull_iterator_cleanups == {}

    asyncio.run(run())


def test_unregister_does_not_wait_for_a_producer_mid_step() -> None:
    async def run() -> None:
        closed: list[float] = []

        async def stubborn() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
            finally:
                await asyncio.sleep(2)
                closed.append(time.monotonic())

        _, nc, unregister = await serve({"stubborn": stubborn})
        response = await open_pull(nc, "stubborn")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))
        pull(nc, "next")
        await asyncio.sleep(0.02)

        started = time.monotonic()
        await unregister()
        assert time.monotonic() - started < 1
        await wait_for(lambda: closed, 5)

    asyncio.run(run())


def test_service_stop_ends_its_push_streams() -> None:
    async def run() -> None:
        client, nc = make_client()
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        closed: list[float] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
            finally:
                closed.append(time.monotonic())

        service = await client.service.register_handler(
            ServiceConfig(name="svc", version="1.0.0"), {"ticks": ticks}
        )
        subject = "stream.svc.ticks.s1"
        params = {"__stream": True, "__streamSubject": subject, "args": []}
        body = encode({"id": "s1", "method": "stream", "params": params})
        endpoint = asyncio.create_task(svcm.service.endpoints["ticks"](request(body, {}, [])))
        await wait_for(lambda: [p for p in nc.published if p["subject"] == subject])

        await service.stop()

        await asyncio.wait_for(endpoint, 2)
        await wait_for(lambda: closed, 2)
        sent = [decode(p["data"]) for p in nc.published if p["subject"] == subject]
        assert [frame["type"] for frame in sent] == ["data", "error"]
        assert sent[-1]["error"]["code"] == "CONNECTION_CLOSED"

    asyncio.run(run())


SETUP_PARAMS: dict[str, dict[str, Any]] = {
    "pull": {"__pullIterator": True, "__iteratorId": "it1", "args": []},
    "pullCallback": {
        "__pullCallback": True,
        "__iteratorId": "it1",
        "__callbackSubject": "_rpc.cb.it1",
        "__callbackMethods": ["frame"],
        "__onewayMethods": ["frame"],
        "args": [],
    },
    "callback": {"__callback": True, "__callbackSubject": "rpc.cb.c1", "args": []},
}


@pytest.mark.parametrize("kind", list(SETUP_PARAMS))
def test_unregister_answers_a_session_still_setting_up_with_connection_closed(kind: str) -> None:
    async def run() -> None:
        cancelled: list[str] = []

        async def setup(*_: Any) -> Any:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled.append(kind)
                raise

        client, nc, unregister = await serve({"setup": setup})
        deliver(nc, "rpc.ns.setup", {"id": "init.1", "method": "call", "params": [SETUP_PARAMS[kind]]})
        await asyncio.sleep(0.02)

        await unregister()

        await wait_for(lambda: frames(nc, "rpc.reply.init.1"), 1)
        await asyncio.sleep(0.02)
        [reply] = frames(nc, "rpc.reply.init.1")
        assert "result" not in reply
        assert {key: reply["error"][key] for key in ("code", "message")} == CONNECTION_CLOSED
        assert cancelled == [kind]
        assert client.pull_iterator_cleanups == {} and client.callback_cleanups == {}

    asyncio.run(run())


def test_service_stop_answers_a_pull_still_setting_up_with_connection_closed() -> None:
    async def run() -> None:
        client, nc = make_client()
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010

        async def setup() -> Any:
            await asyncio.sleep(5)

        service = await client.service.register_handler(
            ServiceConfig(name="svc", version="1.0.0"), {"setup": setup}
        )
        body = encode({"id": "p1", "method": "call", "params": [SETUP_PARAMS["pull"]]})
        endpoint = asyncio.create_task(svcm.service.endpoints["setup"](request(body, {}, [])))
        await asyncio.sleep(0.02)

        await service.stop()
        # nats-py stops an endpoint by unsubscribing, which cancels its message task
        endpoint.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await endpoint

        [reply] = frames(nc, "svc.echo.reply.p1")
        assert {key: reply["error"][key] for key in ("code", "message")} == CONNECTION_CLOSED
        assert client.pull_iterator_cleanups == {}

    asyncio.run(run())


def test_a_lost_connection_ends_every_open_session() -> None:
    async def run() -> None:
        closed: list[str] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                i = 0
                while True:
                    yield i
                    i += 1
                    await asyncio.sleep(0.005)
            finally:
                closed.append("push")

        async def slow() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
                yield 1
            finally:
                closed.append("pull")

        client, nc, _ = await serve({"ticks": ticks, "slow": slow})
        subject = open_stream(nc, "ticks")
        response = await open_pull(nc, "slow")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))
        pull(nc, "next")
        await wait_for(lambda: len(frames(nc, subject)) >= 3)
        call = asyncio.create_task(client.call("svc.slow.wait", timeout=5000))
        await wait_for(lambda: client.pending_requests)

        nc.is_connected = False
        await client._on_connection_lost(nc)  # pyright: ignore[reportPrivateUsage]
        counts = (count(nc, subject), count(nc, response))

        await wait_for(lambda: sorted(closed) == ["pull", "push"], 2)
        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(call, 1)
        assert caught.value.code == "CONNECTION_CLOSED"
        await asyncio.sleep(0.05)
        assert (count(nc, subject), count(nc, response)) == counts
        assert client.stream_cleanups == {} and client.pull_iterator_cleanups == {}

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["suspend", "disconnect", "lost"])
def test_client_teardown_ends_the_sessions_of_its_services(teardown: str) -> None:
    async def run() -> None:
        client, nc = make_client()
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        closed: list[str] = []

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
            finally:
                closed.append("push")

        async def slow() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(5)
            finally:
                closed.append("pull")

        await client.service.register_handler(
            ServiceConfig(name="svc", version="1.0.0"), {"ticks": ticks, "slow": slow}
        )
        subject = "stream.svc.ticks.s1"
        params = {"__stream": True, "__streamSubject": subject, "args": []}
        body = encode({"id": "s1", "method": "stream", "params": params})
        endpoint = asyncio.create_task(svcm.service.endpoints["ticks"](request(body, {}, [])))
        init = {"__pullIterator": True, "__iteratorId": "it1", "args": []}
        body = encode({"id": "p1", "method": "call", "params": [init]})
        await svcm.service.endpoints["slow"](request(body, {}, []))
        response = "_rpc.iterator.it1.response"
        next_msg = make_msg("_rpc.iterator.it1.request", encode({"id": "it1", "type": "next"}))
        await subs_for(nc, "_rpc.iterator.it1.request")[0].cb(next_msg)
        await wait_for(lambda: frames(nc, subject) and frames(nc, response))

        if teardown == "lost":
            nc.is_connected = False
            await client._on_connection_lost(nc)  # pyright: ignore[reportPrivateUsage]
        else:
            await getattr(client, teardown)()

        await asyncio.wait_for(endpoint, 1)
        await wait_for(lambda: sorted(closed) == ["pull", "push"], 1)
        assert client.stream_cleanups == {} and client.pull_iterator_cleanups == {}
        assert getattr(svcm.service, "_pull_iterator_cleanups") == {}  # noqa: B009
        if teardown != "lost":
            for last in (frames(nc, subject)[-1], frames(nc, response)[-1]):
                assert last["type"] == "error"
                assert {key: last["error"][key] for key in ("code", "message")} == CONNECTION_CLOSED

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["suspend", "disconnect"])
def test_a_stream_opened_while_the_teardown_runs_ends_with_it(teardown: str) -> None:
    async def run() -> None:
        async def parked() -> AsyncGenerator[int, None]:
            try:
                yield 1
                await asyncio.sleep(10)
            finally:
                # a producer that closes a resource on its way out keeps the teardown busy
                await asyncio.sleep(0.3)

        async def ticks() -> AsyncGenerator[int, None]:
            i = 0
            while True:
                yield i
                i += 1
                await asyncio.sleep(0.05)

        client, nc, _ = await serve({"parked": parked, "ticks": ticks})
        response = await open_pull(nc, "parked")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))

        tearing = asyncio.create_task(getattr(client, teardown)())
        await asyncio.sleep(0.05)
        assert not tearing.done()
        subject = "stream.rpc.ns.ticks.s9"
        params = {"__stream": True, "__streamSubject": subject, "args": []}
        deliver(nc, "rpc.ns.ticks", {"id": "s9", "method": "stream", "params": params})
        await wait_for(lambda: frames(nc, subject))
        await tearing

        assert "s9" not in client.stream_cleanups
        assert [frame["type"] for frame in frames(nc, subject)] == ["error"]
        assert frames(nc, subject)[0]["error"]["code"] == "CONNECTION_CLOSED"
        if teardown == "suspend":
            fresh = SerialNC()
            await restore(client, fresh)
            await asyncio.sleep(0.15)
            assert frames(fresh, subject) == []
            await client.disconnect()

    asyncio.run(run())


def test_sessions_on_the_next_connection_survive_the_teardown_of_the_lost_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        work_done: list[int] = []
        ticks_closed: list[int] = []

        async def parked() -> AsyncGenerator[int, None]:
            try:
                yield 1
                await asyncio.sleep(10)
            finally:
                await asyncio.sleep(0.3)

        async def work() -> str:
            await asyncio.sleep(0.2)
            work_done.append(1)
            return "ok"

        async def ticks() -> AsyncGenerator[int, None]:
            try:
                yield 0
                await asyncio.sleep(10)
            finally:
                ticks_closed.append(1)

        await client.register_handler(
            "ns", {"parked": parked, "work": work, "ticks": ticks}, concurrency={"work": 2}
        )
        lost = conns[0]
        init = {"__pullIterator": True, "__iteratorId": "it1", "args": []}
        await subs_for(lost, "rpc.ns.parked")[0].cb(
            make_msg("rpc.ns.parked", encode({"id": "i1", "method": "call", "params": [init]}))
        )
        await subs_for(lost, "_rpc.iterator.it1.request")[0].cb(
            make_msg("_rpc.iterator.it1.request", encode({"id": "it1", "type": "next"}))
        )

        closing = asyncio.create_task(lost.close())
        await asyncio.sleep(0.05)
        assert not closing.done()
        # an outgoing call dials the next connection while the teardown still runs
        outgoing = asyncio.create_task(client.call("rpc.other.x", timeout=3000))
        await wait_for(lambda: len(conns) == 2 and requests_to(conns[1], "rpc.other.x"))
        fresh = conns[1]
        await subs_for(fresh, "rpc.ns.work")[0].cb(
            make_msg("rpc.ns.work", encode({"id": "w1", "method": "work", "params": []}))
        )
        subject = "stream.rpc.ns.ticks.s9"
        params = {"__stream": True, "__streamSubject": subject, "args": []}
        await subs_for(fresh, "rpc.ns.ticks")[0].cb(
            make_msg("rpc.ns.ticks", encode({"id": "s9", "method": "stream", "params": params}))
        )
        await closing

        await wait_for(lambda: frames(fresh, "rpc.reply.w1"), 1)
        assert work_done == [1]
        assert "s9" in client.stream_cleanups
        await client.disconnect()
        await wait_for(lambda: ticks_closed, 1)
        assert frames(fresh, subject)[-1]["error"]["code"] == "CONNECTION_CLOSED"
        outgoing.cancel()

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["unregister", "disconnect", "suspend"])
def test_a_teardown_right_after_a_cancel_lets_the_producer_finish_its_cleanup(teardown: str) -> None:
    async def run() -> None:
        steps: list[str] = []

        async def parked() -> AsyncGenerator[int, None]:
            try:
                yield 1
                await asyncio.sleep(10)
            finally:
                steps.append("started")
                await asyncio.sleep(0.2)
                steps.append("completed")

        client, nc, unregister = await serve({"parked": parked})
        response = await open_pull(nc, "parked")
        pull(nc, "next")
        await wait_for(lambda: frames(nc, response))
        pull(nc, "cancel")
        await wait_for(lambda: steps)
        await asyncio.sleep(0.05)

        if teardown == "unregister":
            await unregister()
        else:
            await getattr(client, teardown)()

        await wait_for(lambda: len(steps) == 2, 1)
        assert steps == ["started", "completed"]
        assert [frame["type"] for frame in frames(nc, response)] == ["value", "done"]

    asyncio.run(run())


@pytest.mark.parametrize("end", ["cancel", "unregister"])
def test_two_nexts_in_flight_end_with_the_producer_closed_once(end: str) -> None:
    async def run() -> None:
        closed: list[float] = []

        async def slow() -> AsyncGenerator[int, None]:
            try:
                await asyncio.sleep(5)
                yield 0
                await asyncio.sleep(5)
                yield 1
            finally:
                closed.append(time.monotonic())

        client, nc, unregister = await serve({"slow": slow})
        response = await open_pull(nc, "slow")
        pull(nc, "next")
        pull(nc, "next")
        await asyncio.sleep(0.05)

        started = time.monotonic()
        if end == "cancel":
            pull(nc, "cancel")
        else:
            await unregister()

        await wait_for(lambda: closed, 1)
        assert closed[0] - started < 0.5
        await asyncio.sleep(0.05)
        assert len(closed) == 1
        assert [frame["type"] for frame in frames(nc, response)] == ["done" if end == "cancel" else "error"]
        assert client.pull_iterator_cleanups == {}

    asyncio.run(run())


@pytest.mark.parametrize("method", ["failsLater", "failsInSetup"])
def test_stream_error_frames_carry_internal_error_for_plain_exceptions(method: str) -> None:
    async def run() -> None:
        async def fails_later() -> AsyncGenerator[int, None]:
            yield 0
            raise ValueError("camera gone")

        async def fails_in_setup() -> Any:
            raise ValueError("camera gone")

        _, nc, _ = await serve({"failsLater": fails_later, "failsInSetup": fails_in_setup})
        subject = open_stream(nc, method)

        await wait_for(lambda: frames(nc, subject) and frames(nc, subject)[-1]["type"] == "error")
        assert frames(nc, subject)[-1]["error"] == {"code": "INTERNAL_ERROR", "message": "camera gone"}

    asyncio.run(run())


def test_steps_start_by_hand_where_tasks_cannot_start_eagerly() -> None:
    # fails when asyncio drops these: on 3.11 every next would then cost an extra loop turn
    assert handler._enter_task is not None and handler._leave_task is not None  # pyright: ignore[reportPrivateUsage]


def test_a_step_started_by_hand_answers_within_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler, "_EAGER_STEPS", False)

    async def run() -> None:
        tasks: list[asyncio.Task[Any] | None] = []

        async def counter() -> AsyncGenerator[int, None]:
            for i in range(3):
                tasks.append(asyncio.current_task())
                yield i

        _, nc, _ = await serve({"counter": counter})
        response = await open_pull(nc, "counter")
        request_sub = subs_for(nc, "_rpc.iterator.it1.request")[0]

        await request_sub.cb(make_msg("_rpc.iterator.it1.request", encode({"id": "it1", "type": "next"})))

        assert frames(nc, response) == [{"id": "it1", "type": "value", "value": 0}]
        assert tasks[0] is not None and tasks[0] is not asyncio.current_task()

    asyncio.run(run())


def test_a_step_started_by_hand_runs_as_its_own_task(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler, "_EAGER_STEPS", False)

    async def run() -> None:
        async def bounded() -> AsyncGenerator[str, None]:
            try:
                # asyncio.timeout() cancels the current task, the caller's would be hit if the step ran as it
                async with asyncio.timeout(0.05):
                    await asyncio.sleep(5)
            except TimeoutError:
                yield "timed out"

        _, nc, _ = await serve({"bounded": bounded})
        response = await open_pull(nc, "bounded")
        request_sub = subs_for(nc, "_rpc.iterator.it1.request")[0]

        await request_sub.cb(make_msg("_rpc.iterator.it1.request", encode({"id": "it1", "type": "next"})))
        await wait_for(lambda: frames(nc, response), 1)

        assert frames(nc, response) == [{"id": "it1", "type": "value", "value": "timed out"}]

    asyncio.run(run())


def test_a_cancel_before_steps_by_hand_first_run_reaches_the_producer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(handler, "_EAGER_STEPS", False)

    async def run() -> None:
        closed: list[int] = []

        async def slow() -> AsyncGenerator[int, None]:
            try:
                await asyncio.sleep(5)
                yield 0
            finally:
                closed.append(1)

        _, nc, _ = await serve({"slow": slow})
        response = await open_pull(nc, "slow")
        request_sub = subs_for(nc, "_rpc.iterator.it1.request")[0]

        for kind in ("next", "next", "cancel"):
            await request_sub.cb(make_msg("_rpc.iterator.it1.request", encode({"id": "it1", "type": kind})))

        await wait_for(lambda: closed, 1)
        await asyncio.sleep(0.05)
        assert closed == [1]
        assert frames(nc, response) == [{"id": "it1", "type": "done"}]

    asyncio.run(run())


def test_yield_guard_skips_the_yield_while_the_loop_runs_between_items() -> None:
    async def run() -> None:
        guard = handler._YieldGuard()  # pyright: ignore[reportPrivateUsage]
        due: list[bool] = []
        for _ in range(5):
            await asyncio.sleep(0.003)
            due.append(guard.due())
        assert due == [False] * 5

        started = time.monotonic()
        while time.monotonic() - started < 0.003:
            pass
        assert guard.due() is True
        await asyncio.sleep(0)
        assert guard.due() is False

    asyncio.run(run())
