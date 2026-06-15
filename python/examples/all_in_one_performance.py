import asyncio
import random
import time
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import ErrorCode, RPCClass, RPCException, ServiceConfig, create_rpc_client


@RPCClass
class PerformanceTimer:
    def __init__(self, name: str):
        self.name = name
        self.start_time = 0.0
        self.operations: list[tuple[str, float]] = []

    def start_operation(self, operation: str) -> "OperationTimer":
        return OperationTimer(self, operation)

    def record_operation(self, operation: str, duration: float) -> None:
        self.operations.append((operation, duration))
        print(f"  {operation}: {duration:.2f}ms")

    def print_summary(self) -> None:
        print(f"\n{self.name} Performance Summary:")
        print("=" * 50)
        total_time = sum(duration for _, duration in self.operations)
        for operation, duration in self.operations:
            percentage = (duration / total_time) * 100 if total_time > 0 else 0
            print(f"{operation:.<40} {duration:>8.2f}ms ({percentage:>5.1f}%)")
        print("=" * 50)
        print(f"{'Total Time:':.<40} {total_time:>8.2f}ms")
        print()


@RPCClass
class OperationTimer:
    def __init__(self, timer: PerformanceTimer, operation: str):
        self.timer = timer
        self.operation = operation
        self.start_time = 0.0

    def __enter__(self) -> "OperationTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        duration = (time.perf_counter() - self.start_time) * 1000
        self.timer.record_operation(self.operation, duration)


small_data = "This is a small test message"
medium_data = bytes([i % 256 for i in range(5 * 1024 * 1024)])  # 5MB
large_data = bytes([i % 256 for i in range(10 * 1024 * 1024)])  # 10MB


@RPCClass
class TestService:
    async def echo(self, msg: str) -> str:
        return msg

    async def add(self, a: int, b: int) -> int:
        return a + b

    async def get_large_data(self) -> bytes:
        return large_data

    async def echo_data(self, data: bytes) -> bytes:
        return data

    async def generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def error_method(self, should_fail: bool) -> str:
        if should_fail:
            raise RPCException(ErrorCode.INTERNAL_ERROR, "Test error")
        return "Success"


async def test_all_features() -> None:
    print("Comprehensive RPC Performance Test\n")

    timer = PerformanceTimer("All-in-One Test")

    with timer.start_operation("Server Client Creation"):
        server = create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "name": "test-server",
                "auth": {"user": "server", "password": "server_password"},
            }
        )
        await server.connect()

    with timer.start_operation("Client Creation"):
        client = create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "name": "test-client",
                "auth": {"user": "server", "password": "server_password"},
            }
        )
        await client.connect()

    print("\n 1. Native Request/Reply")
    with timer.start_operation("Request Handler Setup & 100 calls"):

        async def echo_handler(data: Any) -> dict[str, Any]:
            return {"echo": data["msg"], "timestamp": int(time.perf_counter() * 1000)}

        unsub = await server.on_request("echo.*", echo_handler)
        await asyncio.sleep(0.05)  # Let subscription settle

        for i in range(100):
            result = await client.request("echo.test", {"msg": f"Message {i}"})
            assert result["echo"] == f"Message {i}"

        await unsub()

    print("\n 2. Register Handlers (RPC style)")
    with timer.start_operation("RPC Handler Setup & 100 calls"):

        async def handle_echo(msg: str) -> str:
            return f"Echo: {msg}"

        async def handle_add(a: int, b: int) -> int:
            return a + b

        handlers: dict[str, Any] = {
            "echo": handle_echo,
            "add": handle_add,
        }

        unsub_rpc = await server.register_handler("test", handlers)
        await asyncio.sleep(0.05)  # More time for handler setup

        test_proxy = client.create_proxy("test")

        for i in range(50):
            echo_result = await test_proxy.echo(f"Message {i}")
            assert echo_result == f"Echo: Message {i}"

            add_result = await test_proxy.add(i, i + 1)
            assert add_result == 2 * i + 1

        await unsub_rpc()

    print("\n 3. Large Data Transfer (Auto-Chunking)")
    with timer.start_operation("Large Data Transfer (10MB)"):

        async def handle_large() -> bytes:
            return large_data

        async def handle_echo_data(data: bytes) -> bytes:
            return data

        large_handlers: dict[str, Any] = {
            "get_large": handle_large,
            "echo_data": handle_echo_data,
        }

        unsub_large = await server.register_handler("data", large_handlers)
        await asyncio.sleep(0.05)

        data_proxy = client.create_proxy("data")

        result = await data_proxy.get_large()
        assert len(result) == len(large_data)

        echo_result = await data_proxy.echo_data(medium_data)
        assert echo_result == medium_data

        await unsub_large()

    print("\n 4. Channel Communication")
    with timer.start_operation("Channel Setup & 1000 messages"):
        server_channel = await server.channel("perf-channel")
        client_channel = await client.channel("perf-channel")

        messages_received: list[dict[str, Any]] = []

        def on_message(msg: dict[str, Any]) -> None:
            messages_received.append(msg)

        server_channel.on("message", on_message)
        await asyncio.sleep(0.05)

        for i in range(1000):
            await client_channel.send({"index": i, "data": f"Message {i}"})

        await asyncio.sleep(0.2)
        assert len(messages_received) == 1000

        await server_channel.close()
        await client_channel.close()

    print("\n 5. Private Channel Communication")
    with timer.start_operation("Private Channel Setup & 100 calls"):
        server_private = await server.private_channel("perf-private", "test-client")

        async def handle_private(data: dict[str, Any]) -> dict[str, Any]:
            return {"processed": data["value"] * 2}

        unsub_private = await server_private.on_request(handle_private)
        await asyncio.sleep(0.05)

        client_private = await client.private_channel("perf-private", "test-server")

        for i in range(100):
            result = await client_private.request({"value": i}, timeout=5000)
            assert result["processed"] == i * 2

        await unsub_private()
        await server_private.close()
        await client_private.close()

    print("\n 6. Service Creation & Discovery")
    with timer.start_operation("Service Setup"):
        service = await server.service.register_handler(
            ServiceConfig(name="perf-service", version="1.0.0", description="Performance test service"),
            TestService(),
        )

        await asyncio.sleep(0.1)  # Let service register

        monitor = client.service.monitor()
        services = await monitor.info("perf-service")
        assert len(services) > 0
        assert services[0].name == "perf-service"

    print("\n 7. Service Proxy Calls")
    with timer.start_operation("Service Proxy Creation & 50 calls"):
        calc_service = await client.create_service_proxy("perf-service", TestService)

        for i in range(50):
            result = await calc_service.add(i, i + 1)
            assert result == 2 * i + 1

    print("\n 8. Service Streaming")
    with timer.start_operation("Stream Processing (100 items)"):
        stream_sum = 0
        stream = calc_service.generate_numbers(100)

        async for value in stream:
            stream_sum += value

        assert stream_sum == sum(range(100))

    # Services don't support chunking, so large data goes via RPC
    print("\n 8b. Large Data via RPC Handler")
    with timer.start_operation("Get Large Data (10MB) via RPC"):

        async def handle_get_large_data() -> bytes:
            return large_data

        large_data_handlers: dict[str, Any] = {"getLargeData": handle_get_large_data}

        unsub_large_rpc = await server.register_handler("largedata", large_data_handlers)
        await asyncio.sleep(0.05)

        large_proxy = client.create_proxy("largedata")
        large_result = await large_proxy.getLargeData()
        assert len(large_result) == len(large_data)

        await unsub_large_rpc()

    print("\n 9. Concurrent Operations")
    with timer.start_operation("Concurrent Requests (500 parallel)"):

        async def concurrent_handler(data: dict[str, Any]) -> dict[str, Any]:
            return {"echo": data, "handled": True}

        unsub_concurrent = await server.on_request("echo.concurrent", concurrent_handler)
        await asyncio.sleep(0.05)

        async def concurrent_call(i: int) -> dict[str, Any]:
            return await client.request("echo.concurrent", {"index": i})

        tasks = [concurrent_call(i) for i in range(500)]
        results = await asyncio.gather(*tasks)
        assert len(results) == 500

        # Keep the handler for mixed workload test
        # await unsub_concurrent()

    print("\n 10. Error Handling")
    with timer.start_operation("Error Handling Test"):
        result = await calc_service.echo("test")
        assert result == "test"

        try:
            await calc_service.error_method(True)
            raise AssertionError("Should have raised exception")
        except RPCException as e:
            # The error code might come as a string "500" or the enum value
            assert str(e.code) == "500" or e.code == ErrorCode.INTERNAL_ERROR

    print("\n 11. Isolated Connection Proxy")
    with timer.start_operation("Isolated Proxy Test"):
        handlers_isolated: dict[str, Any] = {
            "echo": handle_echo,
            "add": handle_add,
        }
        unsub_isolated = await server.register_handler("test", handlers_isolated)
        await asyncio.sleep(0.05)

        isolated_proxy_with_close = client.create_proxy("test", isolated_connection=True)
        isolated_proxy = isolated_proxy_with_close.proxy

        for i in range(10):
            result = await isolated_proxy.echo(f"Isolated {i}")
            assert result == f"Echo: Isolated {i}"

        await isolated_proxy_with_close.close()
        await unsub_isolated()

    print("\n 12. Mixed Workload (Simulating Real Usage)")
    with timer.start_operation("Mixed Operations"):
        test_channel = await client.channel("mixed-channel")

        async def mixed_operation():
            ops: list[asyncio.Task[Any]] = []

            for _ in range(10):
                ops.append(asyncio.create_task(client.request("echo.concurrent", {"msg": "quick"})))

                ops.append(
                    asyncio.create_task(calc_service.add(random.randint(1, 100), random.randint(1, 100)))
                )

                ops.append(asyncio.create_task(test_channel.send({"event": "mixed", "data": "test"})))

            # Service calls don't support auto-chunking, so use echo to avoid "maximum payload exceeded"
            ops.append(asyncio.create_task(calc_service.echo("test data")))

            await asyncio.gather(*ops)

        rounds: list[asyncio.Task[Any]] = []
        for _ in range(5):
            rounds.append(asyncio.create_task(mixed_operation()))

        await asyncio.gather(*rounds)
        await test_channel.close()

    print("\nCleanup")
    with timer.start_operation("Cleanup"):
        await unsub_concurrent()
        await service.stop()
        await client.disconnect()
        await server.disconnect()

    timer.print_summary()


async def main() -> None:
    try:
        await test_all_features()
        print("All tests completed successfully!")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    import uvloop

    uvloop.run(main())
