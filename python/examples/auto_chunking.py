import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, create_rpc_client

small_data = "This is a small response that fits in a single message"
medium_data = bytearray(5 * 1024 * 1024)  # 5MB
large_data = bytearray(50 * 1024 * 1024)  # 50MB

for i in range(len(medium_data)):
    medium_data[i] = i % 256

for i in range(len(large_data)):
    large_data[i] = i % 256

chunk_buffers: list[bytes] = []

for i in range(3):
    buffer = bytearray(5 * 1024 * 1024)
    for j in range(len(buffer)):
        buffer[j] = (i + j) % 256
    chunk_buffers.append(bytes(buffer))

echo_data = bytearray(4 * 1024 * 1024)
for i in range(len(echo_data)):
    echo_data[i] = (i * 2) % 256
echo_data = bytes(echo_data)


@RPCClass
class TestService:
    async def get_small_data(self) -> str:
        print(f"Returning small data: {small_data}")
        return small_data

    async def get_medium_data(self) -> bytes:
        print("Returning 5MB buffer")
        return bytes(medium_data)

    async def get_large_data(self) -> bytes:
        print("Returning 50MB buffer")
        return bytes(large_data)

    async def echo(self, data: bytes) -> bytes:
        print(f"Echoing {len(data) / 1024 / 1024:.2f}MB")
        return data

    async def generate_large_data_stream(self) -> AsyncGenerator[bytes, None]:
        print("Streaming 3 chunks of 5MB each")

        for i in range(3):
            yield chunk_buffers[i]
            await asyncio.sleep(0)

    async def ping(self) -> str:
        return f"pong at {time.strftime('%Y-%m-%d %H:%M:%S')}"


@RPCClass
class SmallPayloadService:
    async def get_small_data(self) -> str:
        return "Small response that fits in 1KB"

    async def get_medium_data(self) -> bytes:
        return bytes(10 * 1024)

    async def get_large_data(self) -> bytes:
        return bytes(5 * 1024 * 1024)


async def main():
    print("Automatic Chunking Test\n")
    print("Testing transparent chunking in RPC library")
    print("Note: NATS server default max_payload is typically 1MB\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-service",
        }
    )

    await server.connect()
    unsub_test = await server.register_handler("test", TestService())

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()
    service = client.create_proxy("test", TestService)

    total_start = time.perf_counter()

    print("Small Data")
    small_start = time.perf_counter()
    small_data = await service.get_small_data()
    print(f'Small data received in {(time.perf_counter() - small_start) * 1000:.0f}ms: "{small_data}"')

    print("\nMedium Data (5MB)")
    medium_start = time.perf_counter()
    medium_data = await service.get_medium_data()
    medium_time = time.perf_counter() - medium_start
    print(f"Medium data received: {len(medium_data) / 1024 / 1024:.2f}MB in {medium_time * 1000:.0f}ms")
    print(f"Throughput: {5 / medium_time:.2f} MB/s")

    corrupted = False
    for i in range(min(1000, len(medium_data))):
        if medium_data[i] != i % 256:
            corrupted = True
            break
    print(f"Data integrity: {'CORRUPTED' if corrupted else 'OK'}")

    print("\nLarge Data (50MB)")
    large_start = time.perf_counter()
    large_data = await service.get_large_data()
    large_time = time.perf_counter() - large_start
    print(f"Large data received: {len(large_data) / 1024 / 1024:.2f}MB in {large_time * 1000:.0f}ms")
    print(f"Throughput: {50 / large_time:.2f} MB/s")

    print("\nEcho Test (4MB round-trip)")
    echo_start = time.perf_counter()
    echoed = await service.echo(echo_data)
    echo_time = time.perf_counter() - echo_start
    print(f"Echo completed in {echo_time * 1000:.0f}ms")
    print(f"Round-trip throughput: {8 / echo_time:.2f} MB/s")
    print(f"Data matches: {'yes' if echo_data == echoed else 'no'}")

    print("\nStreaming Large Chunks")
    stream_start = time.perf_counter()
    streamed_bytes = 0
    chunk_count = 0

    async for chunk in service.generate_large_data_stream():
        chunk_count += 1
        chunk_size = len(chunk) if chunk else 0
        streamed_bytes += chunk_size
        print(f"Received stream chunk {chunk_count}: {chunk_size / 1024 / 1024:.2f}MB")

    stream_time = time.perf_counter() - stream_start
    print(f"Stream completed: {streamed_bytes / 1024 / 1024:.2f}MB in {stream_time * 1000:.0f}ms")
    if streamed_bytes == 0:
        print("Known issue: Buffer streaming through MessagePack may lose data")
    else:
        print(f"Stream throughput: {streamed_bytes / 1024 / 1024 / stream_time:.2f} MB/s")

    print("\nConcurrent Operations")
    concurrent_tasks: list[asyncio.Task[Any]] = []

    async def large_transfer():
        data = await service.get_large_data()
        print(f"Large data received: {len(data) / 1024 / 1024:.2f}MB")

    concurrent_tasks.append(asyncio.create_task(large_transfer()))

    for i in range(5):

        async def ping_test(index: int):
            await asyncio.sleep(index * 0.1)
            ping_start = time.perf_counter()
            await service.ping()
            print(f"Ping {index + 1} latency: {(time.perf_counter() - ping_start) * 1000:.0f}ms")

        concurrent_tasks.append(asyncio.create_task(ping_test(i)))

    await asyncio.gather(*concurrent_tasks)

    print("\nExtreme Chunking (1KB max payload)")

    small_payload_server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "small-payload-server",
            "max_payload_size": 1024,
        }
    )

    await small_payload_server.connect()

    unsub_small = await small_payload_server.register_handler("test-small-payload", SmallPayloadService())

    small_payload_client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "small-payload-client",
            "max_payload_size": 1024,
        }
    )

    await small_payload_client.connect()
    small_payload_service = small_payload_client.create_proxy("test-small-payload", SmallPayloadService)

    small_result = await small_payload_service.get_small_data()
    print(f'Small data with 1KB limit: "{small_result}"')

    start = time.perf_counter()
    medium_buffer = await small_payload_service.get_medium_data()
    elapsed = time.perf_counter() - start
    expected_chunks = (10 * 1024 + 1023) // 1024
    print(
        f"10KB data with 1KB limit: received {len(medium_buffer)} bytes in {elapsed * 1000:.0f}ms (~{expected_chunks} chunks)"
    )

    large_start_new = time.perf_counter()
    large_buffer = await small_payload_service.get_large_data()
    large_elapsed = time.perf_counter() - large_start_new
    expected_chunks = (5 * 1024 * 1024 + 1023) // 1024
    print(
        f"5MB data with 1KB limit: received {len(large_buffer)} bytes in {large_elapsed * 1000:.0f}ms (~{expected_chunks} chunks)"
    )

    print("\nCleaning up...")
    await unsub_small()
    await small_payload_client.disconnect()
    await small_payload_server.disconnect()

    await unsub_test()
    await client.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"\nAll tests completed! Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
