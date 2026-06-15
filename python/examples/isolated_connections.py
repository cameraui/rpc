import asyncio
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, create_rpc_client


@RPCClass
class TestService:
    async def ping(self) -> str:
        return f"pong at {datetime.now().isoformat()}"

    async def heavy_computation(self, seconds: int) -> int:
        print(f"[Service] Starting heavy computation for {seconds}s")
        start = time.perf_counter()
        while time.perf_counter() - start < seconds:
            await asyncio.sleep(0.001)
        print("[Service] Heavy computation completed")
        return int((time.perf_counter() - start) * 1000)

    async def generate_data_stream(self) -> AsyncGenerator[str, None]:
        for i in range(100):
            yield f"Data chunk {i} at {datetime.now().isoformat()}"
            await asyncio.sleep(0.01)


async def test_isolated_channels():
    print("Testing Isolated Channels\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()

    regular_channel = await client.channel("regular-channel")

    isolated_channel = await client.channel("isolated-channel", isolated_connection=True)

    def on_regular_message(data: Any):
        display_data = {k: v for k, v in data.items() if k != "data"}
        if "data" in data:
            display_data["data"] = f"<{len(data['data'])} bytes>"

        print(f"[Regular Channel]: {display_data}")

    def on_isolated_message(data: Any):
        display_data = {k: v for k, v in data.items() if k != "data"}
        if "data" in data:
            display_data["data"] = f"<{len(data['data'])} bytes>"

        print(f"[Isolated Channel]: {display_data}")

    regular_channel.on("message", on_regular_message)
    isolated_channel.on("message", on_isolated_message)

    print("Sending heavy traffic on isolated channel...")
    isolated_start = time.perf_counter()
    promises: list[asyncio.Task[Any]] = []
    test_buffer = b"x" * 10000  # Pre-allocate 10KB buffer
    for i in range(1000):
        promises.append(
            asyncio.create_task(
                isolated_channel.send(
                    {
                        "type": "bulk",
                        "index": i,
                        "data": test_buffer,
                    }
                )
            )
        )

    print("Testing regular channel responsiveness...")
    regular_start = time.perf_counter()
    await regular_channel.send({"type": "ping", "timestamp": time.perf_counter()})
    regular_time = (time.perf_counter() - regular_start) * 1000
    print(f"Regular channel responded in {regular_time:.1f}ms")

    await asyncio.gather(*promises)
    isolated_time = (time.perf_counter() - isolated_start) * 1000
    throughput = (1000 * 10) / (isolated_time / 1000)  # 10KB * 1000 messages
    print(f"Heavy traffic completed in {isolated_time:.1f}ms ({throughput / 1024:.1f} MB/s)\n")

    await regular_channel.close()
    await isolated_channel.close()
    await client.disconnect()


async def test_isolated_proxies():
    print("Testing Isolated Proxies\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-server",
        }
    )

    await server.connect()
    unsub = await server.register_handler("test", TestService())

    # Setup client
    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()

    regular_proxy = client.create_proxy("test")

    isolated_proxy = client.create_proxy("test", isolated_connection=True)

    print("Starting heavy computation on isolated proxy...")
    heavy_start = time.perf_counter()
    heavy_promise = asyncio.create_task(isolated_proxy.proxy.heavy_computation(3))

    print("Testing regular proxy responsiveness...")
    ping_times: list[float] = []
    for i in range(5):
        ping_start = time.perf_counter()
        result = await regular_proxy.ping()
        ping_time = (time.perf_counter() - ping_start) * 1000
        ping_times.append(ping_time)
        print(f"Regular proxy ping {i + 1}: {ping_time:.1f}ms - {result}")
        await asyncio.sleep(0.5)
    avg_ping_time = sum(ping_times) / len(ping_times)
    print(f"Average ping time during heavy computation: {avg_ping_time:.1f}ms")

    computation_time = await heavy_promise
    total_heavy_time = (time.perf_counter() - heavy_start) * 1000
    print(f"Heavy computation completed in {computation_time}ms (total: {total_heavy_time:.1f}ms)\n")

    print("Testing streaming on isolated proxy...")
    stream_start = time.perf_counter()
    count = 0
    async for _ in isolated_proxy.proxy.generate_data_stream():
        count += 1
        if count % 20 == 0:
            print(f"Received {count} chunks from isolated stream")
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(
        f"Stream completed: {count} total chunks in {stream_time:.1f}ms ({count * 1000 / stream_time:.1f} chunks/s)\n"
    )

    await unsub()

    await isolated_proxy.close()
    await client.disconnect()
    await server.disconnect()

    print("Isolated proxies test completed\n")


async def test_mixed_workload():
    print("Testing Mixed Workload\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "mixed-client",
        }
    )

    await client.connect()

    chat_channel = await client.private_channel("chat", "partner", isolated_connection=True)

    data_channel = await client.private_channel("data-transfer", "partner", isolated_connection=True)

    print("Starting mixed workload...")

    chat_task_running = True

    async def send_chat_messages():
        while chat_task_running:
            await chat_channel.send(
                {
                    "type": "chat",
                    "message": "Hello from chat",
                    "timestamp": datetime.now().isoformat(),
                }
            )
            await asyncio.sleep(1)

    chat_task = asyncio.create_task(send_chat_messages())

    data_start = time.perf_counter()
    data_promises: list[asyncio.Task[Any]] = []
    data_buffer = b"x" * (100 * 1024)  # Pre-allocate 100KB buffer
    for i in range(100):
        data_promises.append(
            asyncio.create_task(
                data_channel.send(
                    {
                        "type": "data",
                        "chunk": i,
                        "payload": data_buffer,
                    }
                )
            )
        )

    chat_messages = 0
    data_chunks = 0

    def on_chat_message(_: Any):
        nonlocal chat_messages
        chat_messages += 1

    def on_data_message(_: Any):
        nonlocal data_chunks
        data_chunks += 1

    chat_channel.on("message", on_chat_message)
    data_channel.on("message", on_data_message)

    await asyncio.gather(*data_promises)
    chat_task_running = False
    await chat_task
    data_time = (time.perf_counter() - data_start) * 1000
    data_throughput = (100 * 100) / (data_time / 1000)  # 100KB * 100 messages

    print(f"Chat messages: {chat_messages}")
    print(f"Data chunks: {data_chunks}")
    print(f"Data transfer: 10MB in {data_time:.1f}ms ({data_throughput / 1024:.1f} MB/s)")
    print("Mixed workload completed\n")

    await chat_channel.close()
    await data_channel.close()
    await client.disconnect()


async def main():
    total_start = time.perf_counter()
    try:
        await test_isolated_channels()
        await test_isolated_proxies()
        await test_mixed_workload()

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"All isolated connection tests completed! Total time: {total_elapsed:.1f}ms")
    except Exception as error:
        print(f"Test failed: {error}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
