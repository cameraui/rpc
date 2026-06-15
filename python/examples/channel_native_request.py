import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client

large_buffer = b"x" * (5 * 1024 * 1024)  # 5MB


async def main():
    total_start = time.perf_counter()
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

    print("Clients connected\n")

    print("Testing Public Channel with Native Request/Reply\n")

    channel_a = await client_a.channel("test-channel")
    channel_b = await client_b.channel("test-channel")

    unsub_b: Callable[..., Coroutine[Any, Any, None]] | None = None
    unsub_priv_b: Callable[..., Coroutine[Any, Any, None]] | None = None
    unsub_concurrent: Callable[..., Coroutine[Any, Any, None]] | None = None
    unsub_large: Callable[..., Coroutine[Any, Any, None]] | None = None

    async def handle_request_b(data: dict[str, Any]) -> dict[str, Any]:
        print(f"[Client B] Received request: {data}")

        await asyncio.sleep(0.1)

        result: dict[str, Any] = {"answer": data.get("question", "NO QUESTION").upper(), "processed": True}
        print(f"[Client B] Sending reply: {result}")
        return result

    unsub_b = await channel_b.on_request(handle_request_b)

    try:
        print("[Client A] Sending request...")
        req_start = time.perf_counter()
        response = await channel_a.request({"question": "hello world"}, 2000)
        req_time = (time.perf_counter() - req_start) * 1000
        print(f"[Client A] Got response: {response}")
        print(f"[Client A] Request took {req_time:.1f}ms\n")
    except Exception as error:
        print(f"[Client A] Request failed: {error}")

    print("Testing Private Channel with Native Request/Reply\n")

    private_a = await client_a.private_channel("private-chat", "client-b")
    private_b = await client_b.private_channel("private-chat", "client-a")

    async def handle_private_request_b(data: dict[str, Any]) -> dict[str, Any]:
        print(f"[Client B Private] Received request: {data}")

        if data.get("type") == "calculation":
            return {"result": data["a"] + data["b"]}
        elif data.get("type") == "error-test":
            raise Exception("Simulated error")

        return {"echo": data}

    unsub_priv_b = await private_b.on_request(handle_private_request_b)

    try:
        print("[Client A Private] Sending calculation request...")
        calc_start = time.perf_counter()
        result = await private_a.request({"type": "calculation", "a": 5, "b": 3})
        calc_time = (time.perf_counter() - calc_start) * 1000
        print(f"[Client A Private] Got result: {result}")
        print(f"[Client A Private] Calculation request took {calc_time:.1f}ms\n")
    except Exception as error:
        print(f"[Client A Private] Request failed: {error}")

    try:
        print("[Client A Private] Sending error test request...")
        await private_a.request({"type": "error-test"})
    except Exception as error:
        print(f"[Client A Private] Got expected error: {error}\n")

    print("Testing Concurrent Channel Requests\n")

    # Unsubscribe the previous handler to avoid conflicts
    if unsub_b:  # type: ignore
        await unsub_b()
        unsub_b = None

    request_count = 0

    async def handle_concurrent_request(data: dict[str, Any]) -> dict[str, Any]:
        nonlocal request_count
        request_count += 1
        req_num = request_count
        print(f"[Client B] Processing request {req_num}...")

        import random

        delay = random.random() * 0.2  # 0-200ms
        await asyncio.sleep(delay)

        return {
            "original": data,
            "requestNumber": req_num,
            "processingTime": f"{int(delay * 1000)}ms",
        }

    unsub_concurrent = await channel_b.on_request(handle_concurrent_request)

    async def send_request(index: int) -> dict[str, Any]:
        response = await channel_a.request({"index": index, "timestamp": int(time.perf_counter() * 1000)})
        print(f"[Client A] Response for request {index}: {response}")
        return response

    promises = [send_request(i) for i in range(5)]
    await asyncio.gather(*promises)
    print("\nAll concurrent requests completed\n")

    print("Testing Channel Request/Reply with Large Data\n")

    # Unsubscribe the previous private handler to avoid conflicts
    if unsub_priv_b:  # type: ignore
        await unsub_priv_b()
        unsub_priv_b = None

    async def handle_large_data_request(data: dict[str, Any]) -> dict[str, Any]:
        if data.get("type") == "large-data":
            print("[Client B] Processing large data request...")

            print(f"[Client B] Sending large response ({len(large_buffer) / 1024 / 1024:.1f}MB)")

            return {"echo": data["message"], "data": large_buffer, "size": len(large_buffer)}
        return {}

    unsub_large = await private_b.on_request(handle_large_data_request)

    try:
        print("[Client A] Sending large data request...")
        start = time.perf_counter()
        response = await private_a.request(
            {"type": "large-data", "message": "Please send me large data"}, 10000
        )
        elapsed = (time.perf_counter() - start) * 1000  # ms

        print(
            f"[Client A] Got large response: size={len(response['data']) / 1024 / 1024:.1f}MB, time={elapsed:.1f}ms"
        )
        print(f"[Client A] Throughput: {5 / (elapsed / 1000):.2f} MB/s\n")
    except Exception as error:
        print(f"[Client A] Large data request failed: {error}")

    print("Testing Mixed Messages and Requests\n")

    def on_regular_message(data: dict[str, Any]) -> None:
        print(f"[Client B] Regular message: {data}")

    channel_b.on("message", on_regular_message)

    await channel_a.send({"type": "regular", "content": "This is a regular message"})

    await asyncio.sleep(0.1)

    try:
        response = await channel_a.request({"type": "request", "content": "This is a request"})
        print(f"[Client A] Request response: {response}\n")
    except Exception as error:
        print(f"[Client A] Request failed: {error}")

    if unsub_b:
        await unsub_b()
    if unsub_priv_b:
        await unsub_priv_b()
    if unsub_concurrent:  # type: ignore
        await unsub_concurrent()
    if unsub_large:  # type: ignore
        await unsub_large()

    await channel_a.close()
    await channel_b.close()
    await private_a.close()
    await private_b.close()
    await client_a.disconnect()
    await client_b.disconnect()

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
