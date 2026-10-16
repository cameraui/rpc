from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from test_client import subs_for
from test_drain import make_client as make_drain_client
from test_drain import until
from test_handler import deliver, frames, pull, serve, wait_for
from test_pull_boundary import deliver as deliver_raw

from camera_ui_rpc.errors import RPCException, RPCTimeoutError

REQUEST = "_rpc.iterator.it1.request"
RESPONSE = "_rpc.iterator.it1.response"


def init_params(callbacks: list[str] | None) -> dict[str, Any]:
    if callbacks is None:
        return {"__pullIterator": True, "__iteratorId": "it1", "args": []}
    return {
        "__pullCallback": True,
        "__iteratorId": "it1",
        "__callbackSubject": "_rpc.cb.it1",
        "__callbackMethods": callbacks,
        "__onewayMethods": callbacks,
        "args": [],
    }


@pytest.mark.parametrize("callbacks", [None, ["frame"]], ids=["pull", "pull-callback"])
def test_pull_cancel_during_handler_setup_ends_the_session_after_it(callbacks: list[str] | None) -> None:
    async def run() -> None:
        release = asyncio.Event()
        produced: list[int] = []

        async def slow_start(*_: Any) -> Any:
            await release.wait()

            async def items() -> AsyncGenerator[int, None]:
                while True:
                    produced.append(1)
                    yield 1

            return items()

        client, nc, _ = await serve({"slowStart": slow_start})
        deliver(
            nc, "rpc.ns.slowStart", {"id": "init.1", "method": "call", "params": [init_params(callbacks)]}
        )
        await wait_for(lambda: subs_for(nc, REQUEST), 1)
        pull(nc, "cancel")
        await asyncio.sleep(0.02)
        release.set()

        await wait_for(lambda: frames(nc, "rpc.reply.init.1"), 1)
        [reply] = frames(nc, "rpc.reply.init.1")
        assert reply["result"] == {"iteratorId": "it1"}
        await wait_for(lambda: client.pull_iterator_cleanups == {}, 1)
        assert frames(nc, RESPONSE) == [{"id": "it1", "type": "done"}]
        assert all(sub._closed for sub in subs_for(nc, REQUEST))

        pull(nc, "next")
        await asyncio.sleep(0.02)
        assert produced == []
        assert frames(nc, RESPONSE) == [{"id": "it1", "type": "done"}]

    asyncio.run(run())


@pytest.mark.parametrize("callbacks", [None, ["frame"]], ids=["pull", "pull-callback"])
def test_pull_handler_that_fails_leaves_no_request_subscription(callbacks: list[str] | None) -> None:
    async def run() -> None:
        async def broken(*_: Any) -> Any:
            await asyncio.sleep(0)
            raise RuntimeError("no recording")

        client, nc, _ = await serve({"broken": broken})
        deliver(nc, "rpc.ns.broken", {"id": "init.1", "method": "call", "params": [init_params(callbacks)]})

        await wait_for(lambda: frames(nc, "rpc.reply.init.1"), 1)
        [reply] = frames(nc, "rpc.reply.init.1")
        assert reply["error"]["message"] == "no recording"
        assert all(sub._closed for sub in subs_for(nc, REQUEST))
        assert client.pull_iterator_cleanups == {}

    asyncio.run(run())


async def start_pull(
    client: Any, nc: Any, callbacks: bool, subject: str = "rpc.ns.play"
) -> tuple[asyncio.Future[Any], dict[str, Any], str]:
    if callbacks:
        iterator = client.call_pull_iterator_with_callback(subject, {"frame": lambda *_: None}, ["frame"])
    else:
        iterator = client.call_pull_iterator(subject)
    first = asyncio.ensure_future(iterator.__anext__())
    await until(lambda: frames(nc, subject))
    [init] = frames(nc, subject)
    return first, init, init["params"][0]["__iteratorId"]


def session_subs(nc: Any, iterator_id: str) -> list[Any]:
    return [sub for sub in nc._subs.values() if iterator_id in sub._subject]


@pytest.mark.parametrize("outcome", ["set-up", "failed", "timed-out"])
@pytest.mark.parametrize("callbacks", [False, True], ids=["pull", "pull-callback"])
def test_a_consumer_cancelled_during_the_init_call_cancels_the_session_after_it(
    callbacks: bool, outcome: str
) -> None:
    async def run() -> None:
        problems: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _, context: problems.append(context))
        client, nc = make_drain_client(timeout=300)
        first, init, iterator_id = await start_pull(client, nc, callbacks)
        request = f"_rpc.iterator.{iterator_id}.request"

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert client._pull_iterator_settles == set()
        assert session_subs(nc, iterator_id) == []
        assert not [t for t in asyncio.all_tasks() if "consume_callbacks" in t.get_coro().__qualname__]
        await asyncio.sleep(0.01)
        assert frames(nc, request) == []

        if outcome == "set-up":
            await deliver_raw(
                nc, f"rpc.reply.{init['id']}", {"id": init["id"], "result": {"iteratorId": iterator_id}}
            )
        elif outcome == "failed":
            failure = {"code": "INTERNAL_ERROR", "message": "no recording"}
            await deliver_raw(nc, f"rpc.reply.{init['id']}", {"id": init["id"], "error": failure})
        await until(lambda: frames(nc, request))
        await asyncio.sleep(0.01)
        gc.collect()

        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]
        assert client.pending_requests == {}
        assert iterator_id not in client.status_handlers
        assert session_subs(nc, iterator_id) == []
        assert problems == []

    asyncio.run(run())


@pytest.mark.parametrize("order", ["reply-first", "cancel-first"])
@pytest.mark.parametrize("callbacks", [False, True], ids=["pull", "pull-callback"])
def test_a_cancel_in_the_step_the_init_reply_arrives_still_cancels_the_session(
    callbacks: bool, order: str
) -> None:
    async def run() -> None:
        client, nc = make_drain_client()
        first, init, iterator_id = await start_pull(client, nc, callbacks)
        request = f"_rpc.iterator.{iterator_id}.request"

        reply = {"id": init["id"], "result": {"iteratorId": iterator_id}}
        if order == "cancel-first":
            first.cancel()
        await deliver_raw(nc, f"rpc.reply.{init['id']}", reply)
        assert client.pending_requests == {}
        if order == "reply-first":
            first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await until(lambda: frames(nc, request))
        await asyncio.sleep(0.01)

        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]
        assert client._pull_iterator_settles == set()
        assert session_subs(nc, iterator_id) == []

    asyncio.run(run())


@pytest.mark.parametrize("callbacks", [False, True], ids=["pull", "pull-callback"])
def test_a_consumer_cancelled_while_the_init_request_goes_out_cancels_the_session_after_it(
    callbacks: bool,
) -> None:
    async def run() -> None:
        client, nc = make_drain_client()
        flushing: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        queued = nc.publish

        async def publish(subject: str, payload: bytes = b"", reply: str = "", headers: Any = None) -> None:
            await queued(subject, payload, reply, headers)
            if subject == "rpc.ns.play":
                # nats-py has queued the request and waits for a forced flush
                await flushing

        nc.publish = publish  # type: ignore[method-assign]
        first, init, iterator_id = await start_pull(client, nc, callbacks)
        request = f"_rpc.iterator.{iterator_id}.request"

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0.01)
        assert frames(nc, request) == []

        reply = {"id": init["id"], "result": {"iteratorId": iterator_id}}
        await deliver_raw(nc, f"rpc.reply.{init['id']}", reply)
        await until(lambda: frames(nc, request))
        await asyncio.sleep(0.01)

        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]
        assert client.pending_requests == {}
        assert session_subs(nc, iterator_id) == []

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["set-up", "no-responders", "timed-out"])
@pytest.mark.parametrize("callbacks", [False, True], ids=["pull", "pull-callback"])
def test_a_consumer_cancelled_during_a_service_init_call_cancels_the_session_after_it(
    callbacks: bool, outcome: str
) -> None:
    async def run() -> None:
        problems: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _, context: problems.append(context))
        client, nc = make_drain_client(timeout=300)
        first, init, iterator_id = await start_pull(client, nc, callbacks, subject="svc.player.play")
        request = f"_rpc.iterator.{iterator_id}.request"
        reply_subject = f"svc.player.play.reply.{init['id']}"
        [inbox] = [sub._subject for sub in nc._subs.values() if sub._subject.startswith("_INBOX.")]

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0.01)
        assert frames(nc, request) == []

        if outcome == "set-up":
            await deliver_raw(nc, reply_subject, {"id": init["id"], "result": {"iteratorId": iterator_id}})
        elif outcome == "no-responders":
            sid = next(sub._id for sub in nc._subs.values() if sub._subject == inbox)
            await nc._process_msg(sid, inbox.encode(), b"", b"", b"NATS/1.0 503\r\n\r\n")
        await until(lambda: frames(nc, request))
        await asyncio.sleep(0.01)
        gc.collect()

        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]
        assert client.pending_requests == {}
        assert [sub for sub in nc._subs.values() if sub._subject in (reply_subject, inbox)] == []
        assert session_subs(nc, iterator_id) == []
        assert problems == []

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["timed-out", "failed", "wrong-id"])
@pytest.mark.parametrize("callbacks", [False, True], ids=["pull", "pull-callback"])
def test_an_init_call_that_does_not_open_the_session_is_followed_by_one_cancel(
    callbacks: bool, outcome: str
) -> None:
    async def run() -> None:
        client, nc = make_drain_client(timeout=50)
        first, init, iterator_id = await start_pull(client, nc, callbacks)
        request = f"_rpc.iterator.{iterator_id}.request"
        reply_subject = f"rpc.reply.{init['id']}"

        if outcome == "failed":
            failure = {"code": "INTERNAL_ERROR", "message": "no recording"}
            await deliver_raw(nc, reply_subject, {"id": init["id"], "error": failure})
        elif outcome == "wrong-id":
            await deliver_raw(nc, reply_subject, {"id": init["id"], "result": {"iteratorId": "other"}})
        with pytest.raises(Exception) as raised:
            await asyncio.wait_for(first, 2)

        if outcome == "timed-out":
            assert isinstance(raised.value, RPCTimeoutError)
        elif outcome == "failed":
            assert isinstance(raised.value, RPCException)
            assert raised.value.message == "no recording"
        else:
            kind = "pull-callback iterator" if callbacks else "pull iterator"
            assert str(raised.value) == f"Failed to initialize {kind}"
        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]
        assert client.pending_requests == {}
        assert client._pull_iterator_settles == set()
        assert session_subs(nc, iterator_id) == []
        assert not [t for t in asyncio.all_tasks() if "consume_callbacks" in t.get_coro().__qualname__]
        await asyncio.sleep(0.02)
        assert frames(nc, request) == [{"id": iterator_id, "type": "cancel"}]

    asyncio.run(run())
