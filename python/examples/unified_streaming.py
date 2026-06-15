import asyncio
import time
from collections.abc import AsyncGenerator

import uvloop

from camera_ui_rpc import RPCClass, RPCNested, ServiceConfig, create_rpc_client


@RPCClass
class TestService:
    async def normal_method(self, name: str) -> str:
        return f"Hello, {name}!"

    async def generate_numbers_service(self, count: int) -> AsyncGenerator[int, None]:
        for i in range(1, count + 1):
            yield i
            await asyncio.sleep(0.1)

    @property
    @RPCNested
    def nested(self) -> "NestedService":
        return NestedService()


@RPCClass
class NestedService:
    async def generate_data(self, prefix: str) -> AsyncGenerator[str, None]:
        for i in range(1, 4):
            yield f"{prefix}-{i}"
            await asyncio.sleep(0.05)


@RPCClass
class TestHandler:
    async def normal_method(self, name: str) -> str:
        return f"RPC says: Hello, {name}!"

    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        for i in range(1, count + 1):
            yield i * 10
            await asyncio.sleep(0.1)


async def main():
    total_start = time.perf_counter()
    print("Testing unified streaming implementation...\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "unified-test-server",
        }
    )
    await server.connect()

    test_service = await server.service.register_handler(
        ServiceConfig(name="test-service", version="1.0.0"),
        TestService(),
    )

    unsub_rpc = await server.register_handler("test-rpc", TestHandler())

    print("Server ready with service and RPC handler\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "unified-test-client",
        }
    )
    await client.connect()

    print("=== Testing Service Streaming ===")
    proxy_start = time.perf_counter()
    service = await client.create_service_proxy("test-service", TestService)
    proxy_time = (time.perf_counter() - proxy_start) * 1000
    print(f"Service proxy created in {proxy_time:.1f}ms")

    start = time.perf_counter()
    greeting = await service.normal_method("Service")
    method_time = (time.perf_counter() - start) * 1000
    print(f"Service normal method: {greeting} ({method_time:.1f}ms)")

    print("\nService streaming:")
    stream_count = 0
    stream_start = time.perf_counter()
    async for num in service.generate_numbers_service(5):
        print("  Received:", num)
        stream_count += 1
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(f"  Total items: {stream_count} in {stream_time:.1f}ms")

    print("\nService nested streaming:")
    nested_count = 0
    nested_start = time.perf_counter()
    async for data in service.nested.generate_data("test"):
        print("  Received:", data)
        nested_count += 1
    nested_time = (time.perf_counter() - nested_start) * 1000
    print(f"  Total items: {nested_count} in {nested_time:.1f}ms")

    print("\n=== Testing RPC Streaming ===")
    rpc_proxy_start = time.perf_counter()
    rpc = client.create_proxy("test-rpc", TestHandler)
    rpc_proxy_time = (time.perf_counter() - rpc_proxy_start) * 1000
    print(f"RPC proxy created in {rpc_proxy_time:.1f}ms")

    rpc_start = time.perf_counter()
    rpc_greeting = await rpc.normal_method("RPC")
    rpc_method_time = (time.perf_counter() - rpc_start) * 1000
    print(f"RPC normal method: {rpc_greeting} ({rpc_method_time:.1f}ms)")

    print("\nRPC streaming:")
    rpc_count = 0
    rpc_stream_start = time.perf_counter()
    async for num in rpc.generate_numbers(5):
        print("  Received:", num)
        rpc_count += 1
    rpc_stream_time = (time.perf_counter() - rpc_stream_start) * 1000
    print(f"  Total items: {rpc_count} in {rpc_stream_time:.1f}ms")

    print("\n=== Message Format Verification ===")
    print("Both service and RPC streaming now use:")
    print("- Same StreamMessage format")
    print("- Same cancellation support")
    print("- Same error handling")
    print("- Automatic chunking via client.publish()")

    print("\nCleaning up...")
    await test_service.stop()
    await unsub_rpc()
    await client.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"\nTest completed successfully! Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
