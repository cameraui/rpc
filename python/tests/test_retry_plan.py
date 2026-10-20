from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from nats import errors
from test_client import answer_attempts, requests_to
from test_mux import MockNC, make_client, make_msg, mux_sub, no_responder_headers

from camera_ui_rpc.codec import decode, encode


class Sleeps:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.delays: list[float] = []
        self._sleep = asyncio.sleep
        monkeypatch.setattr(asyncio, "sleep", self._record)

    async def _record(self, delay: float, *args: Any) -> Any:
        if delay > 0:
            self.delays.append(round(delay, 3))
        return await self._sleep(0, *args)


async def nobody_answers(client: Any, nc: MockNC) -> None:
    answered = 0
    while True:
        await asyncio.sleep(0)
        for pub in nc.published[answered:]:
            answered += 1
            reply = f"rpc.reply.{decode(pub['data'])['id']}"
            await mux_sub(client, nc).cb(make_msg(reply, b"", no_responder_headers()))


@pytest.mark.parametrize(
    ("retry", "delays"),
    [
        (None, [0.025, 0.05, 0.1, 0.325, 1.0, 2.0]),
        ({"delays": [0.01, 0.02]}, [0.01, 0.02, 0.02]),
        ({"max_retries": 2}, [0.5, 1.0]),
        ({"max_retries": 2, "delays": [0.3]}, [0.3, 0.3]),
    ],
)
def test_the_retry_plan_follows_what_the_caller_set(
    retry: Any, delays: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry=retry)
        sleeps = Sleeps(monkeypatch)
        server = asyncio.create_task(nobody_answers(client, nc))
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(client.call("rpc.gone.echo"), 2)
        server.cancel()

        assert sleeps.delays == delays
        assert len(requests_to(nc, "rpc.gone.echo")) == len(delays) + 1

    asyncio.run(run())


def test_a_stream_uses_the_same_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry=None)
        sleeps = Sleeps(monkeypatch)
        server = asyncio.create_task(nobody_answers(client, nc))
        with pytest.raises(errors.NoRespondersError):
            async for _ in client.call_stream("rpc.gone.generate"):
                pass
        server.cancel()

        assert sleeps.delays == [0.025, 0.05, 0.1, 0.325, 1.0, 2.0]

    asyncio.run(run())


def test_the_method_list_may_answer_after_a_short_first_delay() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry={"max_retries": 2, "delays": [0.025, 1.0]})
        proxy = client.create_proxy("ns")

        async def slow_method_list() -> None:
            answered = 0
            while True:
                await asyncio.sleep(0.001)
                for pub in nc.published[answered:]:
                    answered += 1
                    req = decode(pub["data"])
                    reply = f"rpc.reply.{req['id']}"
                    if pub["subject"] == "rpc.ns.__methods":
                        # longer than the first delay, well within the probe window
                        await asyncio.sleep(0.1)
                        body = encode({"id": req["id"], "result": ["other"]})
                        await mux_sub(client, nc).cb(make_msg(reply, body))
                    else:
                        await mux_sub(client, nc).cb(make_msg(reply, b"", no_responder_headers()))

        server = asyncio.create_task(slow_method_list())
        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.echo(), 3)
        server.cancel()

        assert time.monotonic() - started < 0.5
        assert len(requests_to(nc, "rpc.ns.__methods")) == 1

    asyncio.run(run())


def test_a_proxy_still_fails_at_once_for_a_missing_method_with_the_default_plan() -> None:
    async def run() -> None:
        client, nc = make_client(no_responder_retry=None)
        proxy = client.create_proxy("ns")
        server = asyncio.create_task(answer_attempts(client, nc, "rpc.ns.missing", ["echo"]))
        started = time.monotonic()
        with pytest.raises(errors.NoRespondersError):
            await asyncio.wait_for(proxy.missing(), 3)
        server.cancel()
        assert time.monotonic() - started < 0.2

    asyncio.run(run())
