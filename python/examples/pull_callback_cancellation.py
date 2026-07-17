#!/usr/bin/env python3
import asyncio
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker


class DataService:
    def __init__(self) -> None:
        self.teardown_count = 0
        self.produced_batches = 0

    async def pull_forever(self, invoker: CallbackInvoker) -> AsyncGenerator[None, None]:
        print("pull_forever started")
        try:
            b = 0
            while True:
                if not invoker.active:
                    print(f"invoker inactive at batch {b} - exit loop")
                    return
                for i in range(3):
                    await invoker.invoke("onChunk", {"batch": b, "index": i})
                self.produced_batches = b + 1
                print(f"Batch {b} yielded")
                yield
                b += 1
        finally:
            self.teardown_count += 1
            print(f"teardown ran (count={self.teardown_count})")


async def main() -> None:
    print("Pull-Callback Cancellation Example (Python)\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-cancel-server",
        }
    )
    await server.connect()

    svc = DataService()
    unsub_handler = await server.register_handler("data", svc, without_decorators=True)

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-cancel-client",
        }
    )
    await client.connect()

    proxy = client.create_proxy("data")

    chunks_before_cancel = 0
    chunks_after_cancel = 0
    did_cancel = False

    def on_chunk(_data: Any) -> None:
        nonlocal chunks_before_cancel, chunks_after_cancel
        if did_cancel:
            chunks_after_cancel += 1
        else:
            chunks_before_cancel += 1

    cbs = rpc_callbacks(onChunk=on_chunk, oneway=["onChunk"])

    print("Starting infinite generator, breaking after 3 batches...\n")

    batches_consumed = 0
    async for _ in proxy.pull_forever(cbs):
        batches_consumed += 1
        print(f"Batch {batches_consumed - 1} consumed")
        if batches_consumed >= 3:
            print("break!")
            did_cancel = True
            break

    # Allow teardown + any in-flight callback messages to settle.
    await asyncio.sleep(0.5)

    print("\nResults")
    print(f"  Batches consumed on client:   {batches_consumed} (expected 3)")
    print(
        f"  Batches produced on server:   {svc.produced_batches} (expected 3 or 4, tolerance for in-flight)"
    )
    print(f"  Chunks received before cancel: {chunks_before_cancel}")
    print(f"  Chunks received after cancel:  {chunks_after_cancel} (expected small, async drain)")
    print(f"  Server teardown count:         {svc.teardown_count} (expected 1)")

    ok = (
        batches_consumed == 3
        and svc.teardown_count == 1
        and svc.produced_batches <= 5
        and chunks_after_cancel <= 6
    )

    print()
    if ok:
        print("PASS - cancellation cleanly stops server + callbacks")
    else:
        print("FAIL - cancellation did not clean up properly")

    await unsub_handler()
    await client.disconnect()
    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
