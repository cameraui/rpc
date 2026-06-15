#!/usr/bin/env python3
import asyncio
import contextlib
import time
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client


class EventService:
    def __init__(self) -> None:
        self._subscribers: list[Any] = []

    async def on_events(self, prefix: str, callback: Any) -> Any:
        """Subscribe to events. Handler receives a callback to push values."""
        self._subscribers.append(callback)
        print(f"New subscriber for prefix '{prefix}'")

        for i in range(5):
            await callback({"prefix": prefix, "index": i, "type": "event"})
            await asyncio.sleep(0.05)

        def cleanup() -> None:
            if callback in self._subscribers:
                self._subscribers.remove(callback)
            print(f"Cleanup called for prefix '{prefix}'")

        return cleanup


async def main() -> None:
    total_start = time.perf_counter()

    print("Callback Subscription Example\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "callback-test-server",
        }
    )
    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "callback-test-client",
        }
    )

    await server.connect()
    await client.connect()

    try:
        service = EventService()
        cleanup_handler = await server.register_handler("events", service, without_decorators=True)
        print("Server connected\nHandler registered\n")

        proxy = client.create_proxy("events", EventService)
        print("Client connected\n")

        print("Test 1: Basic Callback Subscription\n")

        received: list[Any] = []
        done_event = asyncio.Event()

        def on_event(value: Any) -> None:
            received.append(value)
            print(f"  Received event: prefix={value['prefix']} index={value['index']}")
            if len(received) >= 5:
                done_event.set()

        start = time.perf_counter()
        unsubscribe = await proxy.on_events("test", on_event)

        try:
            await asyncio.wait_for(done_event.wait(), timeout=5.0)
        except TimeoutError:
            print("  Timeout waiting for events")
        elapsed = time.perf_counter() - start

        print(f"\n  Received {len(received)} events in {elapsed * 1000:.1f}ms")
        print("  Unsubscribing...")
        await unsubscribe()
        print("  Unsubscribed")

        print("\nTest 2: Multiple Concurrent Subscriptions\n")

        counts = [0, 0, 0]
        done_events = [asyncio.Event() for _ in range(3)]
        unsubs: list[Any] = []

        for i in range(3):
            idx = i

            def make_cb(index: int, done: asyncio.Event) -> Any:
                def cb(value: Any) -> None:
                    counts[index] += 1
                    if counts[index] >= 5:
                        done.set()

                return cb

            unsub = await proxy.on_events(f"sub-{i}", make_cb(idx, done_events[idx]))
            unsubs.append(unsub)

        for de in done_events:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(de.wait(), timeout=5.0)

        for unsub in unsubs:
            await unsub()

        for i in range(3):
            print(f"  Subscriber {i} received {counts[i]} events")

        print("\nTest 3: Direct call_with_callback\n")

        direct_count = 0
        direct_done = asyncio.Event()

        def on_direct(value: Any) -> None:
            nonlocal direct_count
            direct_count += 1
            if direct_count <= 3:
                print(f"  Direct callback received: prefix={value['prefix']} index={value['index']}")
            if direct_count >= 5:
                direct_done.set()

        direct_unsub = await client.call_with_callback("rpc.events.on_events", ["direct"], on_direct)

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(direct_done.wait(), timeout=5.0)

        await direct_unsub()
        print(f"  Total direct callbacks received: {direct_count}")

        print("")
        if received and len(received) >= 5:
            print("All assertions passed")
        else:
            print(f"Expected at least 5 events, got {len(received)}")

        print("\nCleaning up...")
        await cleanup_handler()

    finally:
        await client.disconnect()
        await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"\nAll tests completed. Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
