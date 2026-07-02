#!/usr/bin/env python3
"""Hot-path benchmark: measures the production-critical RPC paths
(NVR frame delivery, parallel/sequential calls, channel throughput).

Setup happens outside the measurements; each section runs one
unmeasured warmup round first.
"""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker

ONEWAY_BATCHES = 5
ONEWAY_FRAMES_PER_BATCH = 1000
PARALLEL_CALLS = 500
SEQUENTIAL_CALLS = 200
CHANNEL_MESSAGES = 2000


class FrameService:
    """Server-side handler. Method name contains "pull" so it routes
    through pull-callback mode."""

    async def pull_frames(
        self, batches: int, frames_per_batch: int, frame_size: int, invoker: CallbackInvoker
    ) -> AsyncGenerator[None, None]:
        frame = bytes(i % 256 for i in range(frame_size))
        for _b in range(batches):
            if not invoker.active:
                return
            for _i in range(frames_per_batch):
                await invoker.invoke("onFrame", frame)
            yield


async def wait_for_condition(condition: Callable[[], bool], timeout: float = 30.0) -> None:
    deadline = time.perf_counter() + timeout
    while not condition():
        if time.perf_counter() > deadline:
            raise TimeoutError("Timeout waiting for condition")
        await asyncio.sleep(0.001)


async def run_oneway(proxy: Any, frame_size: int, batches: int, frames_per_batch: int) -> tuple[float, int]:
    """Runs one pull-callback iteration; returns (elapsed_ms, bytes_received)."""
    expected = batches * frames_per_batch
    frames = 0
    total_bytes = 0

    def on_frame(frame: bytes) -> None:
        nonlocal frames, total_bytes
        frames += 1
        total_bytes += len(frame)

    cbs = rpc_callbacks(onFrame=on_frame, oneway=["onFrame"])

    start = time.perf_counter()
    async for _ in proxy.pull_frames(batches, frames_per_batch, frame_size, cbs):
        pass  # batch boundary
    await wait_for_condition(lambda: frames >= expected)
    elapsed_ms = (time.perf_counter() - start) * 1000

    if frames != expected:
        raise AssertionError(f"Oneway frame count mismatch: got {frames}, expected {expected}")
    if total_bytes != expected * frame_size:
        raise AssertionError(
            f"Oneway byte count mismatch: got {total_bytes}, expected {expected * frame_size}"
        )

    return elapsed_ms, total_bytes


async def main() -> None:
    print("Perf Hotpath Benchmark (Python)\n")

    # --- Setup (not measured) ---
    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "name": "perf-hotpath-server",
            "auth": {"user": "server", "password": "server_password"},
        }
    )
    await server.connect()

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "name": "perf-hotpath-client",
            "auth": {"user": "server", "password": "server_password"},
        }
    )
    await client.connect()

    unsub_frames = await server.register_handler("hotpath-frames", FrameService(), without_decorators=True)

    async def handle_echo(obj: dict[str, Any]) -> dict[str, Any]:
        return obj

    echo_handlers: dict[str, Any] = {"echo": handle_echo}
    unsub_echo = await server.register_handler("hotpath-rpc", echo_handlers)
    await asyncio.sleep(0.05)  # Let subscriptions settle

    frame_proxy = client.create_proxy("hotpath-frames")
    echo_proxy = client.create_proxy("hotpath-rpc")

    # --- 1. Oneway callback throughput (NVR frame path) ---
    print("1. Oneway Callback Throughput (pull-callback iterator)")

    await run_oneway(frame_proxy, 1024, 1, 50)  # warmup (not measured)
    oneway_1k_ms, _ = await run_oneway(frame_proxy, 1024, ONEWAY_BATCHES, ONEWAY_FRAMES_PER_BATCH)
    msgs_per_sec = (ONEWAY_BATCHES * ONEWAY_FRAMES_PER_BATCH) / (oneway_1k_ms / 1000)
    print(f"Oneway 1KB: {oneway_1k_ms:.2f}ms ({msgs_per_sec:.0f} msg/s)")

    await run_oneway(frame_proxy, 100 * 1024, 1, 50)  # warmup (not measured)
    oneway_100k_ms, oneway_100k_bytes = await run_oneway(
        frame_proxy, 100 * 1024, ONEWAY_BATCHES, ONEWAY_FRAMES_PER_BATCH
    )
    mb_per_sec = oneway_100k_bytes / (1024 * 1024) / (oneway_100k_ms / 1000)
    print(f"Oneway 100KB: {oneway_100k_ms:.2f}ms ({mb_per_sec:.1f} MB/s)")

    # --- 2. Parallel calls ---
    print("\n2. Parallel Calls")

    async def echo_call(i: int) -> None:
        result = await echo_proxy.echo({"seq": i, "camera": "cam-01", "kind": "echo"})
        if result["seq"] != i:
            raise AssertionError(f"Echo seq mismatch at {i}")

    await asyncio.gather(*(echo_call(i) for i in range(20)))  # warmup (not measured)

    parallel_start = time.perf_counter()
    await asyncio.gather(*(echo_call(i) for i in range(PARALLEL_CALLS)))
    parallel_ms = (time.perf_counter() - parallel_start) * 1000
    print(f"Parallel {PARALLEL_CALLS} calls: {parallel_ms:.2f}ms")

    # --- 3. Sequential calls ---
    print("\n3. Sequential Calls")

    for i in range(10):
        await echo_call(i)  # warmup (not measured)

    seq_start = time.perf_counter()
    for i in range(SEQUENTIAL_CALLS):
        await echo_call(i)
    seq_ms = (time.perf_counter() - seq_start) * 1000
    us_per_call = seq_ms * 1000 / SEQUENTIAL_CALLS
    print(f"Sequential {SEQUENTIAL_CALLS} calls: {seq_ms:.2f}ms ({us_per_call:.1f} µs/call)")

    # --- 4. Channel throughput ---
    print("\n4. Channel Throughput")

    server_channel = await server.channel("hotpath-channel")
    client_channel = await client.channel("hotpath-channel")

    channel_received = 0

    def on_message(_msg: Any) -> None:
        nonlocal channel_received
        channel_received += 1

    server_channel.on("message", on_message)
    await asyncio.sleep(0.05)  # Let subscription settle

    # Warmup (not measured)
    for i in range(10):
        await client_channel.send({"index": i, "warmup": True})
    await wait_for_condition(lambda: channel_received >= 10)

    channel_target = channel_received + CHANNEL_MESSAGES
    channel_start = time.perf_counter()
    for i in range(CHANNEL_MESSAGES):
        await client_channel.send({"index": i})
    await wait_for_condition(lambda: channel_received >= channel_target)
    channel_ms = (time.perf_counter() - channel_start) * 1000
    us_per_msg = channel_ms * 1000 / CHANNEL_MESSAGES
    print(f"Channel {CHANNEL_MESSAGES} msgs: {channel_ms:.2f}ms ({us_per_msg:.1f} µs/msg)")

    await server_channel.close()
    await client_channel.close()

    # --- Cleanup ---
    await unsub_frames()
    await unsub_echo()
    await client.disconnect()
    await server.disconnect()

    print("\nAll hotpath benchmarks completed successfully!")


if __name__ == "__main__":
    import uvloop

    uvloop.run(main())
