import time
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client


async def main():
    print("=== Plain Object Property Access Test (Python) ===\n")

    handlers: dict[str, Any] = {
        "name": "test-service",
        "version": "1.0.0",
        "getName": lambda: handlers["name"],
        "getVersion": lambda: handlers["version"],
        "echo": lambda msg: f"Echo: {msg}",  # pyright: ignore[reportUnknownLambdaType]
    }

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "plain-object-server",
        }
    )

    await server.connect()
    print("Server connected")

    unsub = await server.register_handler("test", handlers)
    print("Handlers registered\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "plain-object-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    proxy = client.create_proxy("test")

    try:
        total_start = time.perf_counter()

        print("--- Testing Direct Property Access ---")

        start = time.perf_counter()
        name = await proxy.name
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Name (direct): {name} - Time: {elapsed:.1f}ms")

        start = time.perf_counter()
        version = await proxy.version
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Version (direct): {version} - Time: {elapsed:.1f}ms")

        print("\n--- Testing Method Calls ---")
        start = time.perf_counter()
        name_via_method = await proxy.getName()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Name (via method): {name_via_method} - Time: {elapsed:.1f}ms")

        start = time.perf_counter()
        echo = await proxy.echo("Hello World")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Echo: {echo} - Time: {elapsed:.1f}ms")

        print("\n--- Performance Test ---")
        iterations = 100

        start = time.perf_counter()
        for _ in range(iterations):
            _ = await proxy.name
        elapsed = (time.perf_counter() - start) * 1000
        print(
            f"Direct property access: {iterations} calls in {elapsed:.1f}ms ({elapsed / iterations:.2f}ms avg)"
        )

        start = time.perf_counter()
        for _ in range(iterations):
            _ = await proxy.echo("test")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Method calls: {iterations} calls in {elapsed:.1f}ms ({elapsed / iterations:.2f}ms avg)")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll tests passed! Total time: {total_elapsed:.1f}ms")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        print("\nCleaning up...")
        await unsub()
        await client.disconnect()
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
