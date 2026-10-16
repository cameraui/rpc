from __future__ import annotations

import asyncio
from typing import Any

import pytest
from nats import errors
from test_mux import make_client, until

from camera_ui_rpc.errors import RPCException

DENIED = "rpc.sensors.rpc.getSensors"


def refused(subject: str, operation: str = "publish") -> errors.Error:
    # nats-py lowercases the server's -ERR text, the subject included
    return errors.Error(f'nats: permissions violation for {operation} to "{subject}"'.lower())


def sent_to(nc: Any, subject: str) -> int:
    return sum(1 for p in nc.published if p["subject"] == subject)


def test_refused_publish_fails_every_call_waiting_on_that_subject_with_forbidden() -> None:
    async def run() -> None:
        client, nc = make_client(timeout=10_000)
        first = asyncio.ensure_future(client.call(DENIED))
        second = asyncio.ensure_future(client.call(DENIED))
        other = asyncio.ensure_future(client.call("rpc.sensors.rpc.getSensorStates"))
        await until(lambda: sent_to(nc, DENIED) == 2)

        await client._error_cb(refused(DENIED))  # pyright: ignore[reportPrivateUsage]

        for call in (first, second):
            with pytest.raises(RPCException) as caught:
                await asyncio.wait_for(call, 0.5)
            assert caught.value.code == "FORBIDDEN"
            assert caught.value.data == {"subject": DENIED}
        await asyncio.sleep(0.02)
        assert not other.done()
        assert sent_to(nc, DENIED) == 2
        assert len(client.pending_requests) == 1
        other.cancel()

    asyncio.run(run())


def test_refused_publish_ends_a_stream_start() -> None:
    async def run() -> None:
        client, nc = make_client()

        async def first_item() -> Any:
            async for item in client.call_stream(DENIED):
                return item

        stream = asyncio.ensure_future(first_item())
        await until(lambda: sent_to(nc, DENIED) == 1)

        await client._error_cb(refused(DENIED))  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(RPCException) as caught:
            await asyncio.wait_for(stream, 0.5)
        assert caught.value.code == "FORBIDDEN"
        assert client.stream_handlers == {}
        assert sent_to(nc, f"stream.{DENIED}") == 0

    asyncio.run(run())


def test_refused_subscription_leaves_calls_alone() -> None:
    async def run() -> None:
        client, nc = make_client(timeout=10_000)
        call = asyncio.ensure_future(client.call(DENIED))
        await until(lambda: sent_to(nc, DENIED) == 1)

        await client._error_cb(refused(DENIED, "subscription"))  # pyright: ignore[reportPrivateUsage]

        await asyncio.sleep(0.02)
        assert not call.done()
        call.cancel()

    asyncio.run(run())
