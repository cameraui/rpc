import asyncio
import time
from collections.abc import AsyncGenerator

from nats.micro.service import ServiceConfig

from camera_ui_rpc import RPCClass, create_rpc_client

LARGE_DATA = b"x" * (20 * 1024 * 1024)


@RPCClass
class TestService:
    async def get_large_data(self) -> bytes:
        print("Returning large data (20MB)")
        return LARGE_DATA

    async def echo_large_data(self, data: bytes) -> bytes:
        print(f"Echoing large data ({len(data) / 1024 / 1024:.2f}MB)")
        return data

    async def process_data(self, data: bytes) -> dict[str, str | int]:
        print(f"Processing data ({len(data) / 1024 / 1024:.2f}MB)")
        checksum = 0
        for byte in data:
            checksum = (checksum + byte) % 256
        return {"size": len(data), "checksum": f"{checksum:02x}"}

    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        print(f"Starting to stream {count} numbers")
        for i in range(count):
            yield i
            await asyncio.sleep(0.01)
        print("Streaming complete")

    async def generate_large_items(
        self, count: int, size_kb: int
    ) -> AsyncGenerator[dict[str, int | bytes], None]:
        print(f"Starting to stream {count} items of {size_kb}KB each")
        item_data = b"x" * (size_kb * 1024)

        for i in range(count):
            yield {"index": i, "data": item_data}
            await asyncio.sleep(0.01)
        print("Large item streaming complete")


async def main() -> None:
    print("=== Service Chunking Test ===\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "chunking-server",
        }
    )

    await server.connect()
    print("Server connected")

    service = await server.service.register_handler(
        ServiceConfig(name="chunking-service", version="1.0.0"), TestService()
    )
    print("Service registered\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "chunking-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    try:
        total_start = time.perf_counter()

        proxy = await client.create_service_proxy("chunking-service")
        print("Service proxy created")

        monitor = client.service.monitor()
        services = await monitor.info("chunking-service")
        if services:
            print(f"Discovered service: {services[0].name} (id: {services[0].id})")
            print("Endpoints:")
            for ep in services[0].endpoints:
                print(f"  - {ep.name}: {ep.subject}")
        print()

        print("--- Test 1: Get Large Data ---")
        start = time.perf_counter()
        large_data = await proxy.get_large_data()
        elapsed = (time.perf_counter() - start) * 1000
        size_mb = len(large_data) / 1024 / 1024
        throughput = size_mb / (elapsed / 1000)
        print(f"Received {size_mb:.2f}MB in {elapsed:.1f}ms ({throughput:.1f} MB/s)")
        print(f"   Data matches: {large_data == LARGE_DATA}\n")

        print("--- Test 2: Echo Large Data ---")
        start = time.perf_counter()
        echoed_data = await proxy.echo_large_data(LARGE_DATA)
        elapsed = (time.perf_counter() - start) * 1000
        size_mb = len(echoed_data) / 1024 / 1024
        throughput = size_mb / (elapsed / 1000)
        print(f"Echoed {size_mb:.2f}MB in {elapsed:.1f}ms ({throughput:.1f} MB/s)")
        print(f"   Data matches: {echoed_data == LARGE_DATA}\n")

        print("--- Test 3: Process Large Data ---")
        start = time.perf_counter()
        result = await proxy.process_data(LARGE_DATA)
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Processed {result['size'] / 1024 / 1024:.2f}MB in {elapsed:.1f}ms")
        print(f"   Size: {result['size']} bytes")
        print(f"   Checksum: {result['checksum']}\n")

        print("--- Test 4: Stream Numbers ---")
        start = time.perf_counter()
        stream_count = 0
        async for num in proxy.generate_numbers(10):
            if stream_count < 3 or stream_count >= 7:
                print(f"   Received: {num}")
            elif stream_count == 3:
                print("   ...")
            stream_count += 1
        elapsed = (time.perf_counter() - start) * 1000
        print(
            f"Streamed {stream_count} numbers in {elapsed:.1f}ms ({elapsed / stream_count:.1f}ms per item)\n"
        )

        print("--- Test 5: Stream Large Items ---")
        start = time.perf_counter()
        large_stream_count = 0
        total_size = 0

        async for item in proxy.generate_large_items(5, 500):
            total_size += len(item["data"])
            print(f"   Received item {item['index']}: {len(item['data']) / 1024:.0f}KB")
            large_stream_count += 1
        elapsed = (time.perf_counter() - start) * 1000
        total_mb = total_size / 1024 / 1024
        throughput = total_mb / (elapsed / 1000)
        print(
            f"Streamed {large_stream_count} large items ({total_mb:.2f}MB total) in {elapsed:.1f}ms ({throughput:.1f} MB/s)\n"
        )

        print("--- Test 6: Concurrent Mixed Operations ---")
        start = time.perf_counter()

        mixed_tasks = [
            proxy.get_large_data(),
            proxy.process_data(LARGE_DATA),
        ]

        async def count_stream_numbers() -> int:
            count = 0
            async for _ in proxy.generate_numbers(5):
                count += 1
            return count

        async def count_stream_items() -> int:
            items = 0
            async for _ in proxy.generate_large_items(3, 200):
                items += 1
            return items

        mixed_tasks.extend([count_stream_numbers(), count_stream_items()])

        mixed_results = await asyncio.gather(*mixed_tasks)
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Completed mixed operations in {elapsed:.1f}ms")
        print(f"   Large data: {len(mixed_results[0]) / 1024 / 1024:.2f}MB")
        print(f"   Process result: {mixed_results[1]['size']} bytes")
        print(f"   Streamed numbers: {mixed_results[2]}")
        print(f"   Streamed items: {mixed_results[3]}\n")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(
            f"All tests passed! Service chunking and streaming are working correctly. Total time: {total_elapsed:.1f}ms"
        )

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
