#!/usr/bin/env python3
import asyncio
import time
from collections.abc import AsyncGenerator

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker


async def pull_steady(count: int, invoker: CallbackInvoker) -> AsyncGenerator[None, None]:
    for i in range(count):
        if not invoker.active:
            return
        await invoker.invoke("onItem", i)
        yield


async def main() -> None:
    print("Pull-Callback Mechanism Test (Python)\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-mech-server",
        }
    )
    await server.connect()
    handlers = {"pullSteady": pull_steady}
    unsub_handler = await server.register_handler("mech", handlers, without_decorators=True)

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "pull-callback-mech-client",
        }
    )
    await client.connect()

    proxy = client.create_proxy("mech")

    COUNT = 10
    HANDLER_DELAY_S = 0.2

    starts: list[float] = []
    ends: list[float] = []

    async def on_item(idx: int) -> None:
        starts.append(time.perf_counter())
        await asyncio.sleep(HANDLER_DELAY_S)
        ends.append(time.perf_counter())
        print(f"  [Client] onItem({idx}) finished after {int(HANDLER_DELAY_S * 1000)}ms")

    cbs = rpc_callbacks(onItem=on_item, oneway=["onItem"])

    print(
        f"Consuming {COUNT} items, each handler awaits {int(HANDLER_DELAY_S * 1000)}ms.\n"
        "Consumer loop has no delay — only the handler blocks.\n"
    )

    start = time.perf_counter()

    async for _ in proxy.pullSteady(COUNT, cbs):
        pass

    elapsed_ms = (time.perf_counter() - start) * 1000
    await asyncio.sleep(0.05)

    print("\nTiming")
    print(f"  Handler invocations:   {len(starts)}")
    print(f"  Total elapsed:         {elapsed_ms:.1f}ms")
    expected_ms = COUNT * HANDLER_DELAY_S * 1000
    print(f"  Expected (serialized): {expected_ms:.0f}ms")
    print(f"  Expected (no BP):      ~{COUNT * 2}ms (network only)")

    overlaps = 0
    for i in range(1, min(len(starts), len(ends))):
        if starts[i] < ends[i - 1] - 0.005:  # 5ms slack
            overlaps += 1
    print(f"  Overlapping handlers:  {overlaps} (expected 0)")

    within = expected_ms * 0.9 <= elapsed_ms <= expected_ms * 1.3
    ok = len(starts) == COUNT and within and overlaps == 0

    print()
    if ok:
        print("PASS - callback handlers are serialized, backpressure propagates")
    else:
        print("FAIL - handlers did not serialize, or timing does not match")

    await unsub_handler()
    await client.disconnect()
    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
