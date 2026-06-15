import asyncio
import json
import random
import time
from typing import Any, cast

import uvloop

from camera_ui_rpc import create_rpc_client


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

    print("=== Testing Native NATS Request/Reply ===\n")

    async def echo_handler(data: Any) -> dict[str, Any]:
        print(f"[Server] Received echo request: {json.dumps(data)}")
        return {"echo": data, "timestamp": int(time.perf_counter() * 1000)}

    unsub_echo = await server.on_request("echo", echo_handler)

    try:
        print("Sending echo request...")
        start = time.perf_counter()
        response = await client.request("echo", {"message": "Hello NATS!"})
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Got response: {json.dumps(response)} ({elapsed:.1f}ms)\n")
    except Exception as error:
        print(f"Request failed: {error}")

    print("=== Testing Math Service ===\n")

    async def math_handler(data: Any) -> dict[str, Any]:
        operation = cast(str, data.get("operation") if isinstance(data, dict) else None)

        print(f"Math operation: {operation}, data: {json.dumps(data)}")

        if operation == "add":
            return {"result": data["a"] + data["b"]}
        elif operation == "multiply":
            return {"result": data["a"] * data["b"]}
        elif operation == "divide":
            if data["b"] == 0:
                raise ValueError("Division by zero")
            return {"result": data["a"] / data["b"]}
        else:
            raise ValueError(f"Unknown operation: {operation}")

    unsub_math = await server.on_request("math.*", math_handler)

    math_tests: list[dict[str, Any]] = [
        {"subject": "math.add", "data": {"a": 5, "b": 3, "operation": "add"}},
        {"subject": "math.multiply", "data": {"a": 4, "b": 7, "operation": "multiply"}},
        {"subject": "math.divide", "data": {"a": 20, "b": 4, "operation": "divide"}},
        {"subject": "math.divide", "data": {"a": 10, "b": 0, "operation": "divide"}},
    ]

    for test in math_tests:
        try:
            print(f"Requesting {test['subject']} with {json.dumps(test['data'])}")
            start = time.perf_counter()
            result = await client.request(test["subject"], test["data"])
            elapsed = (time.perf_counter() - start) * 1000
            print(f"Result: {json.dumps(result)} ({elapsed:.1f}ms)")
        except Exception as error:
            print(f"Error: {error}")

    print("\n=== Testing Request/Reply Data Limitations ===\n")
    print("NOTE: Native NATS request/reply doesn't support automatic chunking.")
    print("For large data transfers, use streaming or channels instead.\n")

    async def data_handler(data: Any) -> dict[str, Any]:
        size_kb = cast(int, data.get("size_kb", 100) if isinstance(data, dict) else 100)
        print(f"Got request for {size_kb}KB of data")
        buffer = b"x" * (size_kb * 1024)
        return {
            "data": buffer,
            "size": len(buffer),
            "checksum": len(buffer),
        }

    unsub_data = await server.on_request("data.test", data_handler)

    try:
        print("Requesting 500KB of data...")
        start = time.perf_counter()
        response = await client.request("data.test", {"size_kb": 500}, timeout=10000)
        elapsed = (time.perf_counter() - start) * 1000
        throughput = (response["size"] / 1024 / 1024) / (elapsed / 1000)
        print(f"Got {response['size'] / 1024:.1f}KB in {elapsed:.1f}ms ({throughput:.1f} MB/s)")
        print(f"Max payload size: {client.max_payload_size / 1024:.0f}KB\n")
    except Exception as error:
        print(f"Data request failed: {error}\n")

    try:
        print("Requesting 10MB of data (should fail with proper error)...")
        response = await client.request("data.test", {"size_kb": 10240}, timeout=10000)
    except Exception as error:
        print(f"Expected error: {error}")
        print("Correctly rejected oversized request\n")

    print("=== Testing Concurrent Requests ===\n")

    request_count = 0

    async def concurrent_handler(data: Any) -> dict[str, Any]:
        nonlocal request_count
        request_count += 1
        request_id = request_count
        delay = random.random() * 0.2
        print(f"Processing request {request_id}, delay: {delay * 1000:.0f}ms")

        await asyncio.sleep(delay)

        return {
            "requestId": request_id,
            "input": data,
            "processingTime": f"{delay * 1000:.0f}ms",
        }

    unsub_concurrent = await server.on_request("concurrent.test", concurrent_handler)

    async def make_request(index: int) -> dict[str, Any]:
        start = time.perf_counter()
        res = await client.request("concurrent.test", {"index": index})
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Response {index}: {json.dumps(res)} ({elapsed:.1f}ms)")
        return res

    print("Sending 5 concurrent requests...")
    concurrent_start = time.perf_counter()
    tasks = [make_request(i) for i in range(5)]
    await asyncio.gather(*tasks)
    concurrent_elapsed = (time.perf_counter() - concurrent_start) * 1000
    print(f"\nAll concurrent requests completed in {concurrent_elapsed:.1f}ms\n")

    print("=== Testing No Responders ===\n")
    start = time.perf_counter()

    try:
        print("Requesting non-existent service...")
        await client.request("does.not.exist", {"test": True}, timeout=1000)
    except Exception as error:
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Expected error: {error} ({elapsed:.1f}ms)\n")

    print("=== Testing Timeout ===\n")

    async def slow_handler(data: Any) -> dict[str, Any]:
        print("Slow service - waiting 3 seconds...")
        await asyncio.sleep(3)
        return {"done": True}

    unsub_slow = await server.on_request("slow.service", slow_handler)
    start = time.perf_counter()

    try:
        print("Requesting slow service with 1s timeout...")
        await client.request("slow.service", {"test": True}, timeout=1000)
    except Exception as error:
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Expected timeout: {error} ({elapsed:.1f}ms)\n")

    await unsub_echo()
    await unsub_math()
    await unsub_data()
    await unsub_concurrent()
    await unsub_slow()

    await client.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"All tests completed! Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
