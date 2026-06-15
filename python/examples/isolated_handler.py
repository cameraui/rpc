import asyncio
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client


async def main():
    print("Isolated Handler Connection Test\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "main-server",
        }
    )

    await server.connect()
    print("Main server connected")

    async def echo(msg: str) -> str:
        print(f"[Handler] Echo received: {msg}")
        return f"Echo: {msg}"

    async def get_stats() -> dict[str, Any]:
        print("[Handler] Stats requested")
        return {
            "timestamp": datetime.now().isoformat(),
            "connections": 42,
            "uptime": 123.45,
        }

    async def slow_operation(delay: int) -> str:
        print(f"[Handler] Starting slow operation ({delay}ms)")
        await asyncio.sleep(delay / 1000)
        print("[Handler] Slow operation completed")
        return f"Completed after {delay}ms"

    async def generate_data(count: int) -> AsyncGenerator[dict[str, Any], None]:
        print(f"[Handler] Streaming {count} items")
        for i in range(count):
            yield {"index": i, "data": f"Item {i}"}
        print("[Handler] Stream completed")

    handlers: dict[str, Any] = {
        "echo": echo,
        "getStats": get_stats,
        "slowOperation": slow_operation,
        "generateData": generate_data,
    }

    print("\n--- Registering handlers with isolated connection ---")
    unsubscribe = await server.register_handler("isolated-test", handlers, isolated_connection=True)
    print("Handlers registered with isolated connection\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    proxy = client.create_proxy("isolated-test")

    try:
        total_start = time.perf_counter()

        print("--- Test 1: Echo ---")
        start = time.perf_counter()
        echo_result = await proxy.echo("Hello Isolated World!")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Result: {echo_result} - Time: {elapsed:.1f}ms")

        print("\n--- Test 2: Get Stats ---")
        start = time.perf_counter()
        stats = await proxy.getStats()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Stats: {stats} - Time: {elapsed:.1f}ms")

        print("\n--- Test 3: Concurrent Calls ---")
        start = time.perf_counter()
        tasks = [
            proxy.slowOperation(100),
            proxy.slowOperation(200),
            proxy.slowOperation(150),
            proxy.echo("Concurrent test"),
            proxy.getStats(),
        ]

        results = await asyncio.gather(*tasks)
        elapsed = (time.perf_counter() - start) * 1000
        print(f"All concurrent calls completed in {elapsed:.1f}ms:")
        for i, result in enumerate(results):
            if isinstance(result, dict):
                print(f"  {i + 1}: {result}")
            else:
                print(f"  {i + 1}: {result}")

        print("\n--- Test 4: Streaming ---")
        start = time.perf_counter()
        count = 0
        async for item in proxy.generateData(5):
            print(f"  Received: {item}")
            count += 1
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Streamed {count} items in {elapsed:.1f}ms")

        print("\n--- Test 5: Main Server Disconnect ---")
        print("Disconnecting main server...")
        await server.disconnect()
        print("Main server disconnected")

        print("Testing handlers after main disconnect...")
        try:
            await proxy.echo("Should fail")
            print("ERROR: Call should have failed")
        except Exception as e:
            print(f"Call failed as expected: {e}")

        print("\n--- Test 6: Unsubscribe Handlers ---")
        await unsubscribe()
        print("Handlers unsubscribed")

        try:
            await proxy.echo("Should fail")
            print("ERROR: Call should have failed")
        except Exception as e:
            print(f"Call failed as expected: {e}")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll tests completed! Total time: {total_elapsed:.1f}ms")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await client.disconnect()
        if server.is_connected:
            await server.disconnect()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
