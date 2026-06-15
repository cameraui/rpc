#!/usr/bin/env python3
import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker


class DataService:
    """Server-side handler. Method name must contain "pull" to route through
    pull-callback mode.
    """

    async def pull_batches(
        self, batch_count: int, chunks_per_batch: int, invoker: CallbackInvoker
    ) -> AsyncGenerator[None, None]:
        print(f"pull_batches({batch_count}, {chunks_per_batch}) started")
        for b in range(batch_count):
            if not invoker.active:
                return
            for i in range(chunks_per_batch):
                await invoker.invoke("onChunk", {"batch": b, "index": i})
            await invoker.invoke("onChunk", None)  # end-of-batch sentinel
            print(f"Batch {b} produced, yielding...")
            yield
            print(f"Resumed after batch {b}")
        print("Generator complete")


async def main() -> None:
    total_start = time.perf_counter()
    print("Pull-Callback Basic Example (Python)\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-basic-server",
        }
    )
    await server.connect()
    unsub_handler = await server.register_handler("data", DataService(), without_decorators=True)
    print("Server connected, handler registered\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-basic-client",
        }
    )
    await client.connect()
    print("Client connected\n")

    proxy = client.create_proxy("data")

    received: list[dict[str, Any]] = []
    batch_ends = 0

    def on_chunk(data: Any) -> None:
        nonlocal batch_ends
        if data is None:
            batch_ends += 1
            print(f"Batch {batch_ends - 1} end-of-batch sentinel received")
            return
        received.append(data)

    cbs = rpc_callbacks(onChunk=on_chunk, oneway=["onChunk"])

    BATCHES = 3
    CHUNKS = 5

    print(f"Starting pull iteration for {BATCHES} batches × {CHUNKS} chunks...\n")
    start = time.perf_counter()

    batches_consumed = 0
    async for _ in proxy.pull_batches(BATCHES, CHUNKS, cbs):
        batches_consumed += 1
        print(
            f"Batch {batches_consumed - 1} boundary crossed "
            f"(iteration {batches_consumed}/{BATCHES})"
        )

    elapsed_ms = (time.perf_counter() - start) * 1000

    # Allow in-flight callback messages to land.
    await asyncio.sleep(0.05)

    print("\nResults")
    print(f"  Batches consumed:   {batches_consumed} (expected {BATCHES})")
    print(f"  Chunks received:    {len(received)} (expected {BATCHES * CHUNKS})")
    print(f"  End-of-batch marks: {batch_ends} (expected {BATCHES})")
    print(f"  Total elapsed:      {elapsed_ms:.1f}ms")

    ok = (
        batches_consumed == BATCHES
        and len(received) == BATCHES * CHUNKS
        and batch_ends == BATCHES
        and all(
            r.get("batch") == i // CHUNKS and r.get("index") == i % CHUNKS
            for i, r in enumerate(received)
        )
    )

    print()
    print("PASS - correctness check" if ok else "FAIL - correctness check")

    await unsub_handler()
    await client.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"\nTotal test time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    asyncio.run(main())
