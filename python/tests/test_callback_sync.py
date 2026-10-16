from __future__ import annotations

import asyncio
import threading
from typing import Any

from test_handler import deliver, frames, wait_for
from test_orphan import open_callback, serve


def values(nc: Any, subject: str, tag: str) -> list[int]:
    return [f["data"][1] for f in frames(nc, subject) if f["type"] == "data" and f["data"][0] == tag]


def test_a_sync_handler_delivers_every_value_from_the_pool_and_its_own_thread_in_order() -> None:
    async def run() -> None:
        threads: list[threading.Thread] = []

        def events(emit: Any) -> Any:
            # runs in the pool
            for i in range(1000):
                emit(["pool", i])

            def later() -> None:
                for i in range(1000):
                    emit(["thread", i])

            threads.append(threading.Thread(target=later))
            threads[0].start()
            return None

        client, nc = await serve({"events": events})
        subject = open_callback(nc, "events")
        await wait_for(lambda: len(frames(nc, subject)) == 2000, 5)
        threads[0].join()

        assert values(nc, subject, "pool") == list(range(1000))
        assert values(nc, subject, "thread") == list(range(1000))

    asyncio.run(run())


def test_a_cancel_drops_the_values_still_queued() -> None:
    async def run() -> None:
        emits: list[Any] = []

        def events(emit: Any) -> Any:
            emits.append(emit)
            return None

        client, nc = await serve({"events": events})
        subject = open_callback(nc, "events")
        await wait_for(lambda: emits and client.callback_cleanups)

        deliver(nc, f"{subject}.cancel", {"id": "c1"})
        await wait_for(lambda: not client.callback_cleanups)
        emits[0](["late", 1])
        await asyncio.sleep(0.02)
        assert frames(nc, subject) == []

    asyncio.run(run())
