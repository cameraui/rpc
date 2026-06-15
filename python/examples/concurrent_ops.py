import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import uvloop

from camera_ui_rpc import Channel, RPCClass, create_rpc_client

small_buffer = b"This is a small response that fits in a single message"
medium_buffer_2mb = bytearray(2 * 1024 * 1024)
medium_buffer_5mb = bytearray(5 * 1024 * 1024)
large_buffer_10mb = b"x" * (10 * 1024 * 1024)
chunk_buffer_1mb = bytearray(1 * 1024 * 1024)

for i in range(len(medium_buffer_2mb)):
    medium_buffer_2mb[i] = i % 256
for i in range(len(medium_buffer_5mb)):
    medium_buffer_5mb[i] = i % 256
for i in range(len(chunk_buffer_1mb)):
    chunk_buffer_1mb[i] = i % 256

medium_buffer_2mb = bytes(medium_buffer_2mb)
medium_buffer_5mb = bytes(medium_buffer_5mb)
chunk_buffer_1mb = bytes(chunk_buffer_1mb)


@RPCClass
class TestService:
    async def get_small_data(self) -> str:
        return small_buffer.decode()

    async def get_medium_data(self, size_mb: int) -> bytes:
        print(f"[Service] Returning {size_mb}MB buffer")
        if size_mb == 2:
            return medium_buffer_2mb
        if size_mb == 5:
            return medium_buffer_5mb
        buffer = bytearray(size_mb * 1024 * 1024)
        for i in range(len(buffer)):
            buffer[i] = i % 256
        return bytes(buffer)

    async def get_large_data(self, size_mb: int) -> bytes:
        return await self.get_medium_data(size_mb)

    async def echo(self, data: bytes) -> bytes:
        print(f"[Service] Echoing {len(data) / 1024 / 1024:.2f}MB")
        return data

    async def generate_large_data_stream(
        self, chunk_size_mb: int, chunks: int
    ) -> AsyncGenerator[bytes, None]:
        print(f"[Service] Streaming {chunks} chunks of {chunk_size_mb}MB each")

        for i in range(chunks):
            if chunk_size_mb == 1:
                print(f"[Service] Yielding chunk {i + 1}/{chunks} - size: {len(chunk_buffer_1mb)} bytes")
                yield chunk_buffer_1mb
            else:
                buffer = bytearray(chunk_size_mb * 1024 * 1024)
                for j in range(len(buffer)):
                    buffer[j] = (i + j) % 256
                print(f"[Service] Yielding chunk {i + 1}/{chunks} - size: {len(buffer)} bytes")
                yield bytes(buffer)

            await asyncio.sleep(0.01)

    async def ping(self) -> str:
        return f"pong at {datetime.now().isoformat()}"


async def test_infinite_messages(
    is_connected: list[bool], client_a_channel: Channel, client_b_channel: Channel
) -> None:
    count = 0
    current_channel = client_a_channel
    file_data = large_buffer_10mb

    while is_connected[0]:
        try:
            from_client = "Client A" if current_channel == client_a_channel else "Client B"
            await current_channel.send(
                {
                    "from": from_client,
                    "text": f"Message {count}",
                    "date": datetime.now().isoformat(),
                    "file": file_data,
                }
            )
            count += 1
        except Exception as err:
            if not is_connected[0]:
                break
            print(f"Error sending message: {err}")
        finally:
            await asyncio.sleep(0.1)
            current_channel = client_b_channel if current_channel == client_a_channel else client_a_channel


async def main():
    total_start = time.perf_counter()
    is_connected = [False]  # list so it stays mutable in nested function

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "server",
        }
    )

    await server.connect()
    unsub = await server.register_handler("test", TestService())

    client_a = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "client-a",
        }
    )

    client_b = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "client-b",
        }
    )

    await client_a.connect()
    await client_b.connect()

    print("Clients connected")

    is_connected[0] = True

    client_a_channel = await client_a.private_channel("secret-chat", "client-b")
    client_b_channel = await client_b.private_channel("secret-chat", "client-a")

    print("Private channels created")

    def on_message_a(data: dict[str, Any]) -> None:
        display_data = {k: v for k, v in data.items() if k != "file"}
        if "file" in data:
            display_data["file"] = f"<{len(data['file'])} bytes>"
        print(f"[Client A received]: {display_data}")

    def on_message_b(data: dict[str, Any]) -> None:
        display_data = {k: v for k, v in data.items() if k != "file"}
        if "file" in data:
            display_data["file"] = f"<{len(data['file'])} bytes>"
        print(f"[Client B received]: {display_data}")

    client_a_channel.on("message", on_message_a)
    client_b_channel.on("message", on_message_b)

    infinite_task = asyncio.create_task(
        test_infinite_messages(is_connected, client_a_channel, client_b_channel)
    )

    service_a = client_a.create_proxy("test")

    print("\nTesting service methods")

    start = time.perf_counter()
    small_data = await service_a.get_small_data()
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Small Data: {len(small_data)} bytes ({elapsed:.1f}ms)")

    start = time.perf_counter()
    medium_data = await service_a.get_medium_data(2)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Medium Data (2MB): {len(medium_data)} bytes ({elapsed:.1f}ms, {2000 / elapsed:.1f} MB/s)")

    start = time.perf_counter()
    large_data = await service_a.get_large_data(5)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Large Data (5MB): {len(large_data)} bytes ({elapsed:.1f}ms, {5000 / elapsed:.1f} MB/s)")

    start = time.perf_counter()
    echo_data = await service_a.echo(b"Hello, World!")
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Echo Data: {len(echo_data)} bytes ({elapsed:.1f}ms)")

    print("\nTesting streaming")
    start = time.perf_counter()
    chunk_count = 0
    async for chunk in service_a.generate_large_data_stream(1, 100):
        chunk_count += 1
        if chunk_count <= 3 or chunk_count > 97:  # first 3 and last 3
            print(f"Received Chunk {chunk_count}: {len(chunk)} bytes")
        elif chunk_count == 4:
            print("... (skipping intermediate chunks) ...")
    elapsed = (time.perf_counter() - start) * 1000
    print(
        f"Stream completed: {chunk_count} chunks, total {chunk_count}MB in {elapsed:.1f}ms ({chunk_count * 1000 / elapsed:.1f} MB/s)"
    )

    start = time.perf_counter()
    ping_response = await service_a.ping()
    elapsed = (time.perf_counter() - start) * 1000
    print(f"\nPing Response: {ping_response} ({elapsed:.1f}ms)")

    print("\nRunning concurrent message test for 10 seconds")
    await asyncio.sleep(10)

    print("\nDisconnecting")
    is_connected[0] = False

    infinite_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await infinite_task

    print("\nCleaning up...")
    await unsub()
    await client_a_channel.close()
    await client_b_channel.close()
    await client_a.disconnect()
    await client_b.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"Test completed. Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
