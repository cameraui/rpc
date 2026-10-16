from __future__ import annotations

import asyncio
import inspect
import re
import time
from types import SimpleNamespace
from typing import Any, cast

import nats.aio.client as nats_aio_client
from nats.protocol import command as nats_command

import camera_ui_rpc.client as rpc_client
import camera_ui_rpc.patches as patches
from camera_ui_rpc.client import RPCClient
from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.types import RPCClientOptions


def test_read_buffer_size_is_raised_on_import() -> None:
    assert nats_aio_client.DEFAULT_BUFFER_SIZE >= rpc_client.NATS_READ_BUFFER_SIZE == 256 * 1024


def test_nats_reads_the_buffer_size_at_runtime() -> None:
    # fails when a nats-py update stops reading the constant at runtime, the bigger read buffer is then lost
    client = nats_aio_client.Client
    assert "DEFAULT_BUFFER_SIZE" in client._connect_to_server.__code__.co_names
    assert "DEFAULT_BUFFER_SIZE" in client._read_loop.__code__.co_names


def offline_nats_client() -> nats_aio_client.Client:
    nc = nats_aio_client.Client()

    async def no_wire(*_args: Any, **_kwargs: Any) -> None:
        pass

    nc._send_subscribe = no_wire  # type: ignore[method-assign]
    nc.publish = no_wire  # type: ignore[method-assign]
    return nc


def test_nats_resolves_a_subscription_future_from_the_read_loop() -> None:
    # fails when a nats-py update changes how a subscription future is resolved, the reply mux then
    # falls back to queued delivery
    async def run() -> None:
        nc = offline_nats_client()
        seen: list[Any] = []

        class Sink:
            def cancelled(self) -> bool:
                return False

            def set_result(self, msg: Any) -> None:
                seen.append(msg.data)

        async def idle(_msg: Any) -> None:
            pass

        sub = await nc.subscribe("rpc.reply.x.>", cb=idle, future=Sink())  # type: ignore[arg-type]
        await nc._process_msg(sub._id, b"rpc.reply.x.1", b"", b"payload", b"")

        assert seen == [b"payload"]
        assert sub._pending_queue.qsize() == 0
        assert sub._wait_for_msgs_task is not None
        assert rpc_client.INLINE_REPLY_DELIVERY is True

    asyncio.run(run())


def test_reply_mux_settles_calls_inside_the_read_loop() -> None:
    async def run() -> None:
        nc = offline_nats_client()
        published: list[tuple[str, bytes, str]] = []

        async def publish(subject: str, data: bytes, headers: Any = None, reply: str = "") -> None:
            published.append((subject, data, reply))

        nc.publish = publish  # type: ignore[method-assign]
        client = RPCClient(
            cast(RPCClientOptions, {"servers": ["nats://127.0.0.1:1"], "name": "inline", "timeout": 2000})
        )
        client.nc = nc
        await client._ensure_mux_subscription()  # pyright: ignore[reportPrivateUsage]
        sub = client._mux_entry["sub"]  # type: ignore[index]  # pyright: ignore[reportPrivateUsage, reportOptionalSubscript]

        task = asyncio.create_task(client.call("rpc.t.echo", 1))
        for _ in range(100):
            if published:
                break
            await asyncio.sleep(0)
        _, data, reply = published[0]
        request_id = decode(data)["id"]

        await nc._process_msg(sub._id, reply.encode(), b"", encode({"id": request_id, "result": 42}), b"")

        assert client.pending_requests == {}
        assert sub._pending_queue.qsize() == 0
        assert await asyncio.wait_for(task, 2) == 42

    asyncio.run(run())


def test_inline_sink_keeps_handler_errors_out_of_the_read_loop() -> None:
    def broken(_msg: Any) -> None:
        raise ValueError("boom")

    sink = rpc_client._InlineSink(broken, "rpc.reply.x.>")  # pyright: ignore[reportPrivateUsage]
    sink.set_result(cast(Any, SimpleNamespace(data=b"")))
    assert sink.cancelled() is False


def test_a_subscription_knows_its_connection_and_ends_cleanly_on_close() -> None:
    # fails when a nats-py update drops Subscription._conn or the closed callback: a connection closed for
    # good would then drop every subscription entry again instead of keeping it for the next connect()
    async def run() -> None:
        nc = offline_nats_client()

        async def idle(_msg: Any) -> None:
            pass

        sub = await nc.subscribe("rpc.x", cb=idle)
        assert sub._conn is nc
        assert nc.is_closed is False
        assert "closed_cb" in inspect.signature(nats_aio_client.Client.connect).parameters

        # nats-py cancels the task on close and the task swallows it, an end that looks like an unsubscribe
        task = sub._wait_for_msgs_task
        assert task is not None
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.done() and not task.cancelled() and task.exception() is None

    asyncio.run(run())


def test_nats_marks_the_connection_closed_before_it_ends_subscriptions_and_calls_closed_cb() -> None:
    # fails when a nats-py update reorders its close: entries would be dropped for good again, or a
    # connection closed for good would no longer end the sessions at the close
    async def run() -> None:
        nc = offline_nats_client()

        async def no_flush() -> None:
            pass

        nc._flush_pending = no_flush  # type: ignore[method-assign]
        closed_cb_saw: list[bool] = []

        async def closed_cb() -> None:
            closed_cb_saw.append(nc.is_closed)

        nc._closed_cb = closed_cb

        async def idle(_msg: Any) -> None:
            pass

        sub = await nc.subscribe("rpc.x", cb=idle)
        task = sub._wait_for_msgs_task
        assert task is not None
        task_end: list[tuple[bool, BaseException | None, bool]] = []
        task.add_done_callback(
            lambda t: task_end.append(
                (t.cancelled(), None if t.cancelled() else t.exception(), sub._conn.is_closed)
            )
        )
        await asyncio.sleep(0)

        await nc._close(nats_aio_client.Client.CLOSED)
        assert closed_cb_saw == [True]
        await asyncio.gather(task, return_exceptions=True)
        assert task_end == [(False, None, True)]

    asyncio.run(run())


def test_installed_nats_py_is_a_tested_release_for_the_patches() -> None:
    # fails on a nats-py release outside TESTED_NATS_VERSIONS: check PayloadParser and NatsConnection against
    # its parser.py and flush code, then add the release, otherwise connections silently fall back to stock
    assert patches._nats_version() in patches.TESTED_NATS_VERSIONS  # pyright: ignore[reportPrivateUsage]
    assert patches.PAYLOAD_PARSER is True
    assert patches.DIRECT_WRITES is True
    assert type(patches.NatsConnection()._ps) is patches.PayloadParser  # pyright: ignore[reportPrivateUsage]


def test_nats_keeps_one_parser_per_connection_and_reads_through_it() -> None:
    # fails when a nats-py update builds a new parser on reconnect or reads past _ps, PayloadParser would be lost
    source = inspect.getsource(nats_aio_client.Client)
    assert re.findall(r"self\._ps\b\s*(?::[^=\n]*)?=(?!=)", source) == ["self._ps: Parser ="]
    assert set(re.findall(r"self\._ps\.(\w+)", source)) == {"reset", "parse"}
    assert {"_ps", "parse"} <= set(nats_aio_client.Client._read_loop.__code__.co_names)


def test_nats_publishes_through_flush_pending_and_its_flusher() -> None:
    # fails when a nats-py update stops routing publishes through _flush_pending or changes what the flusher
    # writes, NatsConnection's direct write would then skip or duplicate pending commands
    client = nats_aio_client.Client
    assert {"_send_command", "_flush_pending", "_flush_queue"} <= set(client._send_publish.__code__.co_names)
    assert {"_pending", "_pending_data_size", "writelines", "drain"} <= set(client._flusher.__code__.co_names)
    assert {"_pending", "_pending_data_size"} <= set(client._send_command.__code__.co_names)


def test_nats_has_what_the_handler_drain_uses() -> None:
    # fails when a nats-py update moves the pieces the drain uses: teardowns would then end handler
    # subscriptions at once again and running calls would wait for their timeout
    async def run() -> None:
        nc = offline_nats_client()
        handled: list[bytes] = []
        release = asyncio.Event()

        async def slow(msg: Any) -> None:
            handled.append(msg.data)
            await release.wait()

        sub = await nc.subscribe("rpc.x", cb=slow)
        await nc._process_msg(sub._id, b"rpc.x", b"", b"first", b"")
        await asyncio.sleep(0)
        written: list[bytes] = []

        async def record(cmd: bytes, priority: bool = False) -> None:
            written.append(cmd)

        nc._send_command = record  # type: ignore[method-assign]
        await nc._send_command(nats_command.unsub_cmd(sub._id, 0))
        # a raw UNSUB leaves the subscription in place: calls routed before it still arrive and queue
        await nc._process_msg(sub._id, b"rpc.x", b"", b"second", b"")
        assert sub._pending_queue.qsize() == 1

        pong: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        nc._pongs.append(pong)
        await nc._process_pong()
        assert pong.result() is True

        release.set()
        await asyncio.wait_for(sub._pending_queue.join(), 1)
        assert handled == [b"first", b"second"]
        assert written == [f"UNSUB {sub._id} \r\n".encode()]
        assert rpc_client.HANDLER_DRAIN is True

    asyncio.run(run())


def test_nats_builds_every_message_through_its_msg_class() -> None:
    # fails when a nats-py update builds messages another way: drop_expired would then see no arrival
    # time and run every request again
    async def run() -> None:
        nc = offline_nats_client()
        nc.msg_class = patches.ArrivalMsg
        received: list[Any] = []

        async def keep(msg: Any) -> None:
            received.append(msg)

        sub = await nc.subscribe("rpc.x", cb=keep)
        before = time.monotonic()
        await nc._process_msg(sub._id, b"rpc.x", b"r", b"data", b"")
        await until_received(received)
        assert isinstance(received[0], patches.ArrivalMsg)
        assert received[0].data == b"data" and received[0].reply == "r"
        assert received[0].arrived >= before
        assert patches.ARRIVAL_STAMPS is True

    asyncio.run(run())


async def until_received(received: list[Any]) -> None:
    for _ in range(100):
        if received:
            return
        await asyncio.sleep(0)
    raise AssertionError("nothing delivered")
