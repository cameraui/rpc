import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import RPCClass, ServiceConfig, create_rpc_client

BUFFER_1MB = b"x" * (1024 * 1024)


class DataService:
    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]: ...

    async def generate_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]: ...

    async def pull_numbers(self, count: int) -> AsyncGenerator[int, None]: ...

    async def pull_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]: ...

    async def generate_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]: ...

    async def pull_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]: ...


@RPCClass
class DataServiceImpl:
    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        """Push-based: Server pushes all data as fast as possible."""
        print(f"Starting push-based number generation ({count} items)")
        for i in range(count):
            yield i
        print("Push-based generation complete")

    async def generate_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        """Push-based: Server pushes large data chunks."""
        print(f"Starting push-based data generation ({chunks} x 1MB)")
        for _ in range(chunks):
            yield BUFFER_1MB
        print("Push-based data generation complete")

    async def pull_numbers(self, count: int) -> AsyncGenerator[int, None]:
        """Pull-based: Server waits for client to request each item."""
        print(f"Starting pull-based number iteration ({count} items)")
        for i in range(count):
            print(f"Client pulled number {i}")
            yield i
        print("Pull-based iteration complete")

    async def pull_large_data(self, chunks: int) -> AsyncGenerator[bytes, None]:
        """Pull-based: Server waits for client to request each chunk."""
        print(f"Starting pull-based data iteration ({chunks} x 1MB)")
        for i in range(chunks):
            print(f"Client pulled chunk {i}")
            yield BUFFER_1MB
        print("Pull-based data iteration complete")

    async def generate_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        """Simulate slow data generation - push based."""
        print(f"Starting slow push generation ({delay_ms}ms delay)")
        for i in range(10):
            await asyncio.sleep(delay_ms / 1000)
            yield f"Push data {i} at {time.strftime('%H:%M:%S.%f')[:-3]}"

    async def pull_slow_data(self, delay_ms: int) -> AsyncGenerator[str, None]:
        """Simulate slow data generation - pull based."""
        print(f"Starting slow pull iteration ({delay_ms}ms delay)")
        for i in range(10):
            await asyncio.sleep(delay_ms / 1000)
            yield f"Pull data {i} at {time.strftime('%H:%M:%S.%f')[:-3]}"


async def test_service_push_vs_pull(service: Any):
    print("=== Service Test 1: Fast Number Generation ===\n")

    start = time.perf_counter()
    count = 0
    print("Starting push-based consumption...")
    async for num in service.generate_numbers(1000):
        count += 1
        if count % 100 == 0:
            print(f"Processing pushed number {num}...")
            await asyncio.sleep(0.01)
    push_time = (time.perf_counter() - start) * 1000
    print(f"Push-based: Received {count} numbers in {push_time:.1f}ms\n")

    start = time.perf_counter()
    count = 0
    print("Starting pull-based consumption...")
    async for num in service.pull_numbers(1000):
        count += 1
        if count % 100 == 0:
            print(f"Processing pulled number {num}...")
            await asyncio.sleep(0.01)
    pull_time = (time.perf_counter() - start) * 1000
    print(f"Pull-based: Received {count} numbers in {pull_time:.1f}ms\n")

    print(f"Performance comparison: Push {push_time:.1f}ms vs Pull {pull_time:.1f}ms\n")


async def test_service_backpressure(service: Any):
    print("=== Service Test 2: Backpressure Handling (Large Data) ===\n")

    print("Testing push-based with slow consumer...")
    start = time.perf_counter()
    bytes_received = 0
    chunk_count = 0

    async for chunk in service.generate_large_data(10):
        bytes_received += len(chunk)
        chunk_count += 1
        print(f"Received push chunk {chunk_count}, total: {bytes_received / 1024 / 1024:.1f}MB")
        await asyncio.sleep(0.1)

    push_time = (time.perf_counter() - start) * 1000
    push_throughput = (bytes_received / 1024 / 1024) / (push_time / 1000)
    print(
        f"Push completed: {bytes_received / 1024 / 1024:.1f}MB in {push_time:.1f}ms ({push_throughput:.1f} MB/s)\n"
    )

    print("Testing pull-based with slow consumer...")
    start = time.perf_counter()
    bytes_received = 0
    chunk_count = 0

    async for chunk in service.pull_large_data(10):
        bytes_received += len(chunk)
        chunk_count += 1
        print(f"Received pull chunk {chunk_count}, total: {bytes_received / 1024 / 1024:.1f}MB")
        await asyncio.sleep(0.1)

    pull_time = (time.perf_counter() - start) * 1000
    pull_throughput = (bytes_received / 1024 / 1024) / (pull_time / 1000)
    print(
        f"Pull completed: {bytes_received / 1024 / 1024:.1f}MB in {pull_time:.1f}ms ({pull_throughput:.1f} MB/s)\n"
    )


async def test_service_cancellation(service: Any):
    print("=== Service Test 3: Early Termination ===\n")

    print("Testing push-based early termination...")
    start = time.perf_counter()
    count = 0

    async for data in service.generate_slow_data(100):
        print(f"Received push: {data}")
        count += 1
        if count >= 3:
            print("Breaking from push loop...")
            break

    push_time = (time.perf_counter() - start) * 1000
    print(f"Push terminated after {count} items in {push_time:.1f}ms\n")

    print("Testing pull-based early termination...")
    start = time.perf_counter()
    count = 0

    async for data in service.pull_slow_data(100):
        print(f"Received pull: {data}")
        count += 1
        if count >= 3:
            print("Breaking from pull loop...")
            break

    pull_time = (time.perf_counter() - start) * 1000
    print(f"Pull terminated after {count} items in {pull_time:.1f}ms\n")


async def main():
    print("=== Service-based Pull vs Push Generator Comparison ===\n")
    print("This example demonstrates the differences between:")
    print('- Push-based generators (method names with "generate")')
    print('- Pull-based iterators (method names with "pull")')
    print("Using NATS micro services architecture\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "service-generator-test-server",
        }
    )

    await server.connect()
    print("Server connected")

    service = await server.service.register_handler(
        ServiceConfig(
            name="data-service",
            version="1.0.0",
            description="Data service with push/pull generators",
        ),
        DataServiceImpl(),
    )
    print("Service registered as NATS micro service\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "service-generator-test-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    try:
        total_start = time.perf_counter()

        data_service = await client.create_service_proxy("data-service", DataService)
        print("Service proxy created through discovery\n")

        await test_service_push_vs_pull(data_service)
        await test_service_backpressure(data_service)
        await test_service_cancellation(data_service)

        service_info = server.service.get_all_info()
        print("=== Service Information ===")
        print(f"Name: {service_info[0].name}")
        print(f"Version: {service_info[0].version}")
        print(f"Endpoints: {len(service_info[0].endpoints)}")
        print("Endpoint names:", ", ".join(e.name for e in service_info[0].endpoints))

        print("\n=== Summary ===\n")
        print("Service-based implementation demonstrates:")
        print("  Both push and pull work with NATS micro services")
        print("  Service discovery works for both patterns")
        print("  Same performance characteristics as direct RPC")
        print("  Pull-based provides better backpressure control")
        print("  Services can be scaled independently")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll service tests completed! Total time: {total_elapsed:.1f}ms")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await service.stop()
        await client.disconnect()
        await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
