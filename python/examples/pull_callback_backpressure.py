#!/usr/bin/env python3
import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker


class DataService:
    async def pull_paced_batches(
        self, batch_count: int, chunks_per_batch: int, invoker: CallbackInvoker
    ) -> AsyncGenerator[None, None]:
        for b in range(batch_count):
            if not invoker.active:
                return
            batch_start = time.perf_counter()
            for i in range(chunks_per_batch):
                await invoker.invoke("onChunk", {"batch": b, "index": i})
            await invoker.invoke("onChunk", None)
            produced_ms = (time.perf_counter() - batch_start) * 1000
            print(f"Batch {b} produced in {produced_ms:.1f}ms, suspending at yield...")
            yield
            print(f"Batch {b} resumed (client called next)")


async def main() -> None:
    print("Pull-Callback Backpressure Example (Python)\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-bp-server",
        }
    )
    await server.connect()
    unsub_handler = await server.register_handler("data", DataService(), without_decorators=True)

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-bp-client",
        }
    )
    await client.connect()

    proxy = client.create_proxy("data")

    BATCHES = 4
    CHUNKS = 1000
    CLIENT_DELAY_MS = 500

    stats: list[dict[str, Any]] = [
        {"first_wall": -1, "last_wall": -1, "count": 0} for _ in range(BATCHES)
    ]

    def on_chunk(data: Any) -> None:
        if data is None:
            return
        batch = int(data.get("batch", -1))
        if batch < 0 or batch >= BATCHES:
            return
        now_us = int(time.perf_counter() * 1_000_000)
        s = stats[batch]
        if s["first_wall"] == -1:
            s["first_wall"] = now_us
        s["last_wall"] = now_us
        s["count"] += 1

    cbs = rpc_callbacks(onChunk=on_chunk, oneway=["onChunk"])

    print(
        f"Consuming {BATCHES} batches × {CHUNKS} chunks, "
        f"client delays {CLIENT_DELAY_MS}ms between batches...\n"
    )
    start = time.perf_counter()

    idx = 0
    async for _ in proxy.pull_paced_batches(BATCHES, CHUNKS, cbs):
        print(f"Received batch boundary {idx}, sleeping {CLIENT_DELAY_MS}ms...")
        await asyncio.sleep(CLIENT_DELAY_MS / 1000)
        idx += 1

    elapsed_ms = (time.perf_counter() - start) * 1000
    await asyncio.sleep(0.05)

    print("\nPer-Batch Stats (wall-clock timestamps, us)")
    baseline = stats[0]["first_wall"]
    for i in range(BATCHES):
        s = stats[i]
        span_ms = (s["last_wall"] - s["first_wall"]) / 1000.0
        first_rel = (s["first_wall"] - baseline) / 1000.0
        last_rel = (s["last_wall"] - baseline) / 1000.0
        print(
            f"  Batch {i}: {s['count']} chunks, span {span_ms:.1f}ms, "
            f"first@{first_rel:.1f}ms, last@{last_rel:.1f}ms"
        )

    print("\nInter-Batch Gaps (FYI - Python asyncio scheduling)")
    for i in range(1, BATCHES):
        gap_ms = (stats[i]["first_wall"] - stats[i - 1]["last_wall"]) / 1000.0
        print(f"  Gap between batch {i - 1} and {i}: {gap_ms:.1f}ms")

    expected_total_ms = BATCHES * CLIENT_DELAY_MS
    print(f"\nTotal elapsed: {elapsed_ms:.1f}ms (expected ~{expected_total_ms}ms)")

    bp_ok = abs(elapsed_ms - expected_total_ms) < expected_total_ms * 0.2

    print()
    if bp_ok:
        print("PASS - total elapsed time matches client pacing (backpressure works)")
    else:
        print("FAIL - total elapsed time does not match client pacing")

    await unsub_handler()
    await client.disconnect()
    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
