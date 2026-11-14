from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from test_client import SerialNC, SerialSub
from test_handler import wait_for
from test_mux import make_client, no_responder_headers

from camera_ui_rpc.codec import decode
from camera_ui_rpc.errors import RPCException


def subject_matches(pattern: str, subject: str) -> bool:
    want = pattern.split(".")
    have = subject.split(".")
    for i, token in enumerate(want):
        if token == ">":
            return len(have) > i
        if i >= len(have) or token not in ("*", have[i]):
            return False
    return len(want) == len(have)


def is_request(subject: str) -> bool:
    return subject.startswith("_rpc.iterator.") and subject.endswith(".request")


def is_response(subject: str) -> bool:
    return subject.startswith("_rpc.iterator.") and subject.endswith(".response")


class Wire:
    # clients on one wire, a message can disappear like one written into a dead connection: no delivery, no 503
    def __init__(self) -> None:
        self.ends: list[WireNC] = []
        self.sent: list[tuple[str, bytes]] = []
        self.lose: Callable[[str], bool] | None = None

    def join(self) -> Any:
        client, _ = make_client()
        nc = WireNC(self)
        client.nc = cast(Any, nc)
        self.ends.append(nc)
        return client

    def consumer(self) -> Any:
        client = self.join()
        client._pull_probe_interval = 0.02
        return client

    def requests(self, kind: str) -> int:
        return sum(1 for subject, data in self.sent if is_request(subject) and decode(data)["type"] == kind)

    def route(self, subject: str, data: bytes, headers: dict[str, str] | None, reply: str) -> None:
        self.sent.append((subject, data))
        if self.lose is not None and self.lose(subject):
            self.lose = None
            return
        if not self._deliver(subject, data, headers, reply) and reply:
            self._deliver(reply, b"", no_responder_headers(), "")

    def _deliver(self, subject: str, data: bytes, headers: dict[str, str] | None, reply: str) -> bool:
        delivered = False
        for nc in self.ends:
            for sub in nc.subs:
                if sub._closed or not subject_matches(sub.pattern, subject):
                    continue
                assert isinstance(sub, SerialSub)
                sub.pending.put_nowait(
                    SimpleNamespace(subject=subject, data=data, headers=headers, reply=reply)
                )
                delivered = True
        return delivered


class WireNC(SerialNC):
    def __init__(self, wire: Wire) -> None:
        super().__init__()
        self._wire = wire

    async def publish(
        self, subject: str, data: bytes, headers: dict[str, str] | None = None, reply: str = ""
    ) -> None:
        await super().publish(subject, data, headers, reply)
        self._wire.route(subject, bytes(data), headers, reply)


class Counting:
    def __init__(self, slow_from: int | None = None, delay: float = 0.0) -> None:
        self.ended = False
        self._slow_from = slow_from
        self._delay = delay

    async def items(self) -> AsyncGenerator[int, None]:
        try:
            i = 0
            while True:
                if self._slow_from is not None and i >= self._slow_from:
                    await asyncio.sleep(self._delay)
                yield i
                i += 1
        finally:
            self.ended = True


async def step(iterator: Any) -> Any:
    return await asyncio.wait_for(iterator.__anext__(), 2)


@pytest.mark.parametrize("lost", [is_request, is_response], ids=["request", "answer"])
def test_a_pull_step_that_lost_a_message_ends_with_an_error_and_ends_the_session(
    lost: Callable[[str], bool],
) -> None:
    async def run() -> None:
        wire = Wire()
        source = Counting()
        await wire.join().register_handler("ns", {"items": source.items})

        iterator = wire.consumer().call_pull_iterator("rpc.ns.items")
        assert await step(iterator) == 0

        wire.lose = lost
        with pytest.raises(RPCException, match="lost a message") as failure:
            await step(iterator)
        assert failure.value.code == "STREAM_ERROR"
        await wait_for(lambda: source.ended, 1)
        assert wire.requests("cancel") == 1

        probes = wire.requests("probe")
        await asyncio.sleep(0.06)
        assert wire.requests("probe") == probes

    asyncio.run(run())


def test_a_pull_step_ends_with_no_responders_when_the_last_word_of_the_handler_got_lost() -> None:
    async def run() -> None:
        wire = Wire()

        async def one() -> AsyncGenerator[int, None]:
            yield 0

        await wire.join().register_handler("ns", {"one": one})
        iterator = wire.consumer().call_pull_iterator("rpc.ns.one")
        assert await step(iterator) == 0

        wire.lose = is_response
        with pytest.raises(RPCException) as failure:
            await step(iterator)
        assert failure.value.code == "503"

    asyncio.run(run())


def test_a_pull_callback_step_whose_request_got_lost_ends_with_an_error() -> None:
    async def run() -> None:
        wire = Wire()
        ended: list[bool] = []

        async def frames(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                i = 0
                while True:
                    await invoker.invoke("frame", i)
                    yield None
                    i += 1
            finally:
                ended.append(True)

        await wire.join().register_handler("ns", {"frames": frames})
        seen: list[int] = []
        iterator = wire.consumer().call_pull_iterator_with_callback(
            "rpc.ns.frames", {"frame": seen.append}, ["frame"]
        )
        await step(iterator)
        assert seen == [0]

        wire.lose = is_request
        with pytest.raises(RPCException, match="lost a message"):
            await step(iterator)
        await wait_for(lambda: ended, 1)

    asyncio.run(run())


def test_a_pull_step_outlives_its_probes_while_the_source_is_slow() -> None:
    async def run() -> None:
        wire = Wire()
        source = Counting(slow_from=1, delay=0.15)
        await wire.join().register_handler("ns", {"items": source.items})

        iterator = wire.consumer().call_pull_iterator("rpc.ns.items")
        for want in range(3):
            assert await step(iterator) == want
        assert wire.requests("probe") >= 6

        await iterator.aclose()
        await wait_for(lambda: source.ended, 1)

    asyncio.run(run())


def test_no_probe_goes_out_while_the_consumer_takes_its_time_between_steps() -> None:
    async def run() -> None:
        wire = Wire()
        await wire.join().register_handler("ns", {"items": Counting().items})

        iterator = wire.consumer().call_pull_iterator("rpc.ns.items")
        for want in range(3):
            assert await step(iterator) == want
            await asyncio.sleep(0.06)
        assert wire.requests("probe") == 0
        await iterator.aclose()

    asyncio.run(run())


def test_idle_answers_to_earlier_steps_are_ignored() -> None:
    async def run() -> None:
        wire = Wire()
        await wire.join().register_handler("ns", {"items": Counting().items})
        consumer = wire.consumer()
        consumer._pull_probe_interval = 60

        iterator = consumer.call_pull_iterator("rpc.ns.items")
        assert await step(iterator) == 0
        [iterator_id] = consumer._pull_sessions
        request = f"_rpc.iterator.{iterator_id}.request"
        for want in (1, 2, 3):
            pending = asyncio.ensure_future(step(iterator))
            await wait_for(lambda want=want: wire.requests("next") == want + 1)
            # as after probes that waited in a stalled connection: value 0 was step 1,
            # so the step before the one waiting here has the number of its value
            for earlier in (want, want, 0):
                await consumer.publish(request, {"id": iterator_id, "type": "probe", "step": earlier})
            assert await pending == want
        await iterator.aclose()

    asyncio.run(run())
