import asyncio
import os
import time
from collections.abc import AsyncGenerator
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None

from camera_ui_rpc import RPCClass, create_rpc_client

BUFFER_1MB = b"x" * (1024 * 1024)


class DataService:
    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        ...

    async def generate_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        ...

    async def pull_numbers(self, count: int) -> AsyncGenerator[int, None]:
        ...

    async def pull_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        ...

    async def generate_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        ...

    async def pull_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        ...


@RPCClass
class DataServiceImpl:
    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        print(f"[Server] Starting push-based number generation ({count} items)")
        for i in range(count):
            yield i
        print("[Server] Push-based generation complete")

    async def generate_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        print(f"[Server] Starting push-based data generation ({chunks} x 1MB)")
        for _ in range(chunks):
            yield BUFFER_1MB
        print("[Server] Push-based data generation complete")

    async def pull_numbers(self, count: int) -> AsyncGenerator[int, None]:
        print(f"[Server] Starting pull-based number iteration ({count} items)")
        for i in range(count):
            print(f"[Server] Client pulled number {i}")
            yield i
        print("[Server] Pull-based iteration complete")

    async def pull_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        print(f"[Server] Starting pull-based data iteration ({chunks} x 1MB)")
        for i in range(chunks):
            print(f"[Server] Client pulled chunk {i}")
            yield BUFFER_1MB
        print("[Server] Pull-based data iteration complete")

    async def generate_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        print(f"[Server] Starting slow push generation ({delay_ms}ms delay)")
        for i in range(10):
            await asyncio.sleep(delay_ms / 1000)
            yield f"Push data {i} at {time.strftime('%H:%M:%S.%f')[:-3]}"

    async def pull_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        print(f"[Server] Starting slow pull iteration ({delay_ms}ms delay)")
        for i in range(10):
            await asyncio.sleep(delay_ms / 1000)
            yield f"Pull data {i} at {time.strftime('%H:%M:%S.%f')[:-3]}"


async def test_push_vs_pull(service: Any):
    print("Test 1: Fast Number Generation\n")

    start = time.perf_counter()
    count = 0
    print("[Client] Starting push-based consumption...")
    async for num in service.generate_numbers(1000):
        count += 1
        if count % 100 == 0:
            print(f"[Client] Processing pushed number {num}...")
            await asyncio.sleep(0.01)
    push_time = (time.perf_counter() - start) * 1000
    print(f"[Client] Push-based: Received {count} numbers in {push_time:.1f}ms\n")

    start = time.perf_counter()
    count = 0
    print("[Client] Starting pull-based consumption...")
    async for num in service.pull_numbers(1000):
        count += 1
        if count % 100 == 0:
            print(f"[Client] Processing pulled number {num}...")
            await asyncio.sleep(0.01)
    pull_time = (time.perf_counter() - start) * 1000
    print(f"[Client] Pull-based: Received {count} numbers in {pull_time:.1f}ms\n")

    print(f"Performance comparison: Push {push_time:.1f}ms vs Pull {pull_time:.1f}ms\n")


async def test_backpressure(service: Any):
    print("Test 2: Backpressure Handling (Large Data)\n")

    print("[Client] Testing push-based with slow consumer...")
    start = time.perf_counter()
    bytes_received = 0
    chunk_count = 0

    async for chunk in service.generate_large_data(10):
        bytes_received += len(chunk)
        chunk_count += 1
        print(f"[Client] Received push chunk {chunk_count}, total: {bytes_received / 1024 / 1024:.1f}MB")
        await asyncio.sleep(0.1)

    push_time = (time.perf_counter() - start) * 1000
    push_throughput = (bytes_received / 1024 / 1024) / (push_time / 1000)
    print(
        f"[Client] Push completed: {bytes_received / 1024 / 1024:.1f}MB in {push_time:.1f}ms ({push_throughput:.1f} MB/s)\n"
    )

    print("[Client] Testing pull-based with slow consumer...")
    start = time.perf_counter()
    bytes_received = 0
    chunk_count = 0

    async for chunk in service.pull_large_data(10):
        bytes_received += len(chunk)
        chunk_count += 1
        print(f"[Client] Received pull chunk {chunk_count}, total: {bytes_received / 1024 / 1024:.1f}MB")
        await asyncio.sleep(0.1)

    pull_time = (time.perf_counter() - start) * 1000
    pull_throughput = (bytes_received / 1024 / 1024) / (pull_time / 1000)
    print(
        f"[Client] Pull completed: {bytes_received / 1024 / 1024:.1f}MB in {pull_time:.1f}ms ({pull_throughput:.1f} MB/s)\n"
    )


async def test_memory_pressure(service: Any):
    print("Test 3: Memory Pressure Simulation\n")

    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024
    print(f"Initial memory: {initial_memory:.1f}MB")

    print("\n[Client] Starting push-based with intentional delay...")
    max_memory_push = initial_memory

    push_gen = service.generate_large_data(20)
    await asyncio.sleep(1)

    async for _ in push_gen:
        current_memory = process.memory_info().rss / 1024 / 1024
        max_memory_push = max(max_memory_push, current_memory)
        await asyncio.sleep(0.01)

    print(f"Push-based peak memory: {max_memory_push:.1f}MB")

    print("\n[Client] Starting pull-based with same delay pattern...")
    max_memory_pull = initial_memory

    pull_gen = service.pull_large_data(20)
    # Delay has no effect - nothing is generated yet
    await asyncio.sleep(1)

    async for _ in pull_gen:
        current_memory = process.memory_info().rss / 1024 / 1024
        max_memory_pull = max(max_memory_pull, current_memory)
        await asyncio.sleep(0.01)

    print(f"Pull-based peak memory: {max_memory_pull:.1f}MB")
    print(f"Memory difference: {(max_memory_push - max_memory_pull):.1f}MB\n")


async def test_cancellation(service: Any):
    print("Test 4: Early Termination\n")

    print("[Client] Testing push-based early termination...")
    start = time.perf_counter()
    count = 0

    async for data in service.generate_slow_data(100):
        print(f"[Client] Received push: {data}")
        count += 1
        if count >= 3:
            print("[Client] Breaking from push loop...")
            break

    push_time = (time.perf_counter() - start) * 1000
    print(f"[Client] Push terminated after {count} items in {push_time:.1f}ms\n")

    print("[Client] Testing pull-based early termination...")
    start = time.perf_counter()
    count = 0

    async for data in service.pull_slow_data(100):
        print(f"[Client] Received pull: {data}")
        count += 1
        if count >= 3:
            print("[Client] Breaking from pull loop...")
            break

    pull_time = (time.perf_counter() - start) * 1000
    print(f"[Client] Pull terminated after {count} items in {pull_time:.1f}ms\n")


async def main():
    print("Pull vs Push Generator Comparison\n")
    print("This example demonstrates the differences between:")
    print('- Push-based generators (method names with "generate")')
    print('- Pull-based iterators (method names with "pull")\n')

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "generator-test-server",
        }
    )

    await server.connect()
    print("Server connected")

    unsub = await server.register_handler("data", DataServiceImpl())
    print("Service registered\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "generator-test-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    try:
        total_start = time.perf_counter()

        service = client.create_proxy("data", DataService)

        await test_push_vs_pull(service)
        await test_backpressure(service)
        if psutil is not None:
            await test_memory_pressure(service)
        else:
            print("Test 3: Memory Pressure Simulation\n")
            print("Skipped (psutil not installed)\n")
        await test_cancellation(service)

        print("Summary\n")
        print("Push-based (generate*):")
        print("  Server sends all data immediately")
        print("  Good for small datasets or fast consumers")
        print("  Can cause memory pressure with slow consumers")
        print("  No natural backpressure")
        print()
        print("Pull-based (pull*):")
        print("  Client controls the flow")
        print("  Natural backpressure handling")
        print("  Memory efficient")
        print("  Better cancellation behavior")
        print("  Slightly more latency per item")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll tests completed! Total time: {total_elapsed:.1f}ms")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await unsub()
        await client.disconnect()
        await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
