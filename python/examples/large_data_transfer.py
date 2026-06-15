import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, create_rpc_client

# Pre-allocate buffers for consistent performance testing
buffer_10mb = b"x" * (10 * 1024 * 1024)
buffer_20mb = b"x" * (20 * 1024 * 1024)


@RPCClass
class DataService:
    async def generate_large_data(self, size_mb: int) -> AsyncGenerator[bytes, None]:
        chunk_size = 1024 * 1024  # 1MB chunks
        total_size = size_mb * 1024 * 1024
        sent = 0

        print(f"Starting to stream {size_mb}MB of data...")

        while sent < total_size:
            remaining = total_size - sent
            current_chunk_size = min(chunk_size, remaining)
            chunk = b"x" * current_chunk_size
            sent += current_chunk_size

            yield chunk

            progress = int((sent / total_size) * 100)
            if progress % 10 == 0 and progress > 0 and progress != 100:
                print(f"Streamed {progress}%")

        print("Streaming completed")


async def main():
    total_start = time.perf_counter()
    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "server",
        }
    )

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "client",
        }
    )

    await server.connect()
    await client.connect()

    print("Connected to NATS\n")
    print(f"Server max payload: {server.max_payload_size / 1024 / 1024:.1f}MB")
    print(f"Client max payload: {client.max_payload_size / 1024 / 1024:.1f}MB\n")

    print("=== Method 1: Publish/Subscribe with Auto-Chunking ===\n")

    received_data = None

    async def large_data_handler(data: dict[str, Any]) -> None:
        nonlocal received_data
        received_data = data
        print(f"Received {len(data['payload']) / 1024 / 1024:.1f}MB via pub/sub")

    unsub = await server.subscribe("large.data.transfer", large_data_handler)

    large_payload = buffer_10mb
    print(f"Sending {len(large_payload) / 1024 / 1024:.1f}MB via publish...")
    start = time.perf_counter()
    await client.publish("large.data.transfer", {"payload": large_payload, "checksum": len(large_payload)})

    await asyncio.sleep(1.0)
    elapsed = (time.perf_counter() - start) * 1000

    if received_data:
        print(f"Transfer completed in {elapsed:.0f}ms")
        print(f"Checksum verified: {received_data['checksum'] == len(large_payload)}\n")
    else:
        print("Data not received\n")

    await unsub()

    print("=== Method 2: RPC Streaming for Large Data ===\n")

    unregister = await server.register_handler("data", DataService())

    data_service: DataService = client.create_proxy("data")

    print("Requesting 10MB of streamed data...")
    start = time.perf_counter()
    total_received = 0

    async for chunk in data_service.generate_large_data(10):
        total_received += len(chunk)

    elapsed = (time.perf_counter() - start) * 1000
    print(f"Received {total_received / 1024 / 1024:.1f}MB in {elapsed:.0f}ms")
    print(f"Transfer rate: {(total_received / 1024 / 1024) / (elapsed / 1000):.1f}MB/s\n")

    await unregister()

    print("=== Method 3: Channels for Bidirectional Transfer ===\n")

    server_channel = await server.channel("large-data-channel")
    client_channel = await client.channel("large-data-channel")

    server_received = 0

    async def channel_handler(data: Any) -> None:
        nonlocal server_received
        if "chunk" in data:
            server_received += len(data["chunk"])
            if data.get("last"):
                print(f"Received total: {server_received / 1024 / 1024:.1f}MB")
                await server_channel.send({"received": server_received, "status": "complete"})

    server_channel.on("message", channel_handler)

    print("Sending 5MB through channel in chunks...")
    start = time.perf_counter()
    chunk_size = 512 * 1024  # 512KB chunks
    total_size = 5 * 1024 * 1024
    sent = 0

    while sent < total_size:
        remaining = total_size - sent
        current_chunk_size = min(chunk_size, remaining)
        chunk = b"x" * current_chunk_size
        sent += current_chunk_size

        await client_channel.send({"chunk": chunk, "last": sent >= total_size})

    confirmed = False

    def confirmation_handler(data: Any) -> None:
        nonlocal confirmed
        if data.get("status") == "complete":
            confirmed = True
            elapsed = (time.perf_counter() - start) * 1000
            print(f"Channel transfer completed in {elapsed:.0f}ms")
            print(f"Server confirmed {data['received'] / 1024 / 1024:.1f}MB received\n")

    client_channel.on("message", confirmation_handler)

    for _ in range(20):
        if confirmed:
            break
        await asyncio.sleep(0.1)

    print("=== Method 4: Request/Reply for Large Data ===\n")

    try:
        print("Sending 20MB via request/reply (should fail)...")
        large_payload = buffer_20mb
        start = time.perf_counter()
        response = await client.request(
            "large.data.reply", {"payload": large_payload, "checksum": len(large_payload)}
        )
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Received response in {elapsed:.0f}ms")
        print(f"Checksum verified: {response['checksum'] == len(large_payload)}\n")
    except Exception as e:
        print(f"Request/Reply failed: {e}\n")
        print("This method is not suitable for large data transfers due to NATS max_payload limits.\n")

    await server_channel.close()
    await client_channel.close()

    print("\n=== Summary ===")
    print("1. Publish/Subscribe: Best for one-way large data broadcasts")
    print("2. RPC Streaming: Best for controlled data flow with backpressure")
    print("3. Channels: Best for bidirectional communication with large data")
    print("4. Request/Reply: Limited by NATS max_payload, no auto-chunking")

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
