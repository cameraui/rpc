import asyncio
import os
import time
from collections.abc import AsyncGenerator
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, RPCNested, ServiceConfig, create_rpc_client

# Pre-allocate buffer for consistent performance testing
buffer_2mb = bytearray(2 * 1024 * 1024)
for i in range(len(buffer_2mb)):
    buffer_2mb[i] = i % 256
buffer_2mb = bytes(buffer_2mb)


@RPCClass
class MathService:
    async def add(self, a: int, b: int) -> int:
        return a + b

    async def multiply(self, a: int, b: int) -> int:
        return a * b

    async def divide(self, a: float, b: float) -> float:
        if b == 0:
            raise ValueError("Division by zero")
        return a / b


@RPCClass
class StringService:
    async def concat(self, a: str, b: str) -> str:
        return a + b

    async def reverse(self, text: str) -> str:
        return text[::-1]

    async def generate_words(self, count: int) -> AsyncGenerator[str, None]:
        words = ["hello", "world", "nats", "rpc", "service"]
        for i in range(count):
            yield words[i % len(words)]
            await asyncio.sleep(0.1)


@RPCClass
class DataService:
    async def createBuffer(self, size_mb: int) -> bytes:
        if size_mb == 2:
            return buffer_2mb
        buffer = bytearray(size_mb * 1024 * 1024)
        for i in range(len(buffer)):
            buffer[i] = i % 256
        return bytes(buffer)

    @property
    @RPCNested
    def info(self) -> "InfoNamespace":
        return InfoNamespace()


@RPCClass
class InfoNamespace:
    async def version(self) -> str:
        return "1.0.0"

    async def status(self) -> dict[str, Any]:
        return {
            "healthy": True,
            "uptime": time.perf_counter() - startup_time,
            "memory": {
                "rss": os.getpid(),  # Simplified, would need psutil for real memory usage
                "heapUsed": 0,  # Python doesn't have direct heap stats like Node.js
            },
        }


startup_time = 0.0


async def main():
    total_start = time.perf_counter()
    global startup_time
    startup_time = time.perf_counter()

    print("Starting multi-service test...\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "multi-service-server",
        }
    )

    await server.connect()
    print("Server connected")

    math_service = await server.service.register_handler(
        ServiceConfig(
            name="math",
            version="1.0.0",
            description="Mathematical operations service",
            queue_group="math-workers",
        ),
        MathService(),
    )

    string_service = await server.service.register_handler(
        ServiceConfig(
            name="string",
            version="1.0.0",
            description="String manipulation service",
            queue_group="string-workers",
        ),
        StringService(),
    )

    data_service = await server.service.register_handler(
        ServiceConfig(
            name="data",
            version="2.0.0",
            description="Data generation and info service",
            metadata={
                "author": "test",
                "capabilities": "buffer-generation,status-info",
            },
        ),
        DataService(),
    )

    print("\nRegistered services:")
    for info in server.service.get_all_info():
        print(f"- {info.name} v{info.version}: {info.description}")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()
    print("\nClient connected")

    print("\n--- Service Discovery ---")
    monitor = server.service.monitor()

    services = await monitor.info()
    for info in services:
        print(f"Found: {info.name} v{info.version} ({info.id})")

    print("\n--- Math Service ---")
    math_start = time.perf_counter()
    math = await client.create_service_proxy("math", MathService, isolated_connection=False)
    math_proxy_time = (time.perf_counter() - math_start) * 1000
    print(f"Math proxy created in {math_proxy_time:.1f}ms")

    start = time.perf_counter()
    result = await math.add(5, 3)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"5 + 3 = {result} ({elapsed:.1f}ms)")

    start = time.perf_counter()
    result = await math.multiply(4, 7)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"4 * 7 = {result} ({elapsed:.1f}ms)")

    start = time.perf_counter()
    result = await math.divide(10, 2)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"10 / 2 = {result} ({elapsed:.1f}ms)")

    print("\n--- String Service ---")
    string_start = time.perf_counter()
    string = await client.create_service_proxy("string", StringService, isolated_connection=False)
    string_proxy_time = (time.perf_counter() - string_start) * 1000
    print(f"String proxy created in {string_proxy_time:.1f}ms")

    start = time.perf_counter()
    result = await string.concat("Hello", "World")
    elapsed = (time.perf_counter() - start) * 1000
    print(f'concat("Hello", "World") = {result} ({elapsed:.1f}ms)')

    start = time.perf_counter()
    result = await string.reverse("NATS")
    elapsed = (time.perf_counter() - start) * 1000
    print(f'reverse("NATS") = {result} ({elapsed:.1f}ms)')

    print("\nStreaming words:")
    stream_start = time.perf_counter()
    words = string.generate_words(5)
    word_count = 0
    async for word in words:
        print(" -", word)
        word_count += 1
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(f"Streamed {word_count} words in {stream_time:.1f}ms")

    print("\n--- Data Service ---")
    data_start = time.perf_counter()
    data: DataService = await client.create_service_proxy("data", isolated_connection=False)
    data_proxy_time = (time.perf_counter() - data_start) * 1000
    print(f"Data proxy created in {data_proxy_time:.1f}ms")

    start = time.perf_counter()
    buffer = await data.createBuffer(2)
    buffer_time = (time.perf_counter() - start) * 1000
    print(
        f"Generated buffer: {len(buffer) / 1024 / 1024:.2f}MB in {buffer_time:.1f}ms ({2000 / buffer_time:.1f} MB/s)"
    )

    start = time.perf_counter()
    version = await data.info.version()
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Service version: {version} ({elapsed:.1f}ms)")

    start = time.perf_counter()
    status = await data.info.status()
    elapsed = (time.perf_counter() - start) * 1000
    print(
        "Service status:",
        {
            "healthy": status["healthy"],
            "uptime": f"{status['uptime']:.2f}s",
            "memory": f"{status['memory']['rss']}",  # Simplified
        },
    )

    print("\n--- All Services Stats ---")
    all_stats = await server.service.get_all_stats()
    for stats in all_stats:
        print(f"\n{stats.name} ({stats.id}):")
        print(f"  Started: {stats.started}")
        print(f"  Total endpoints: {len(stats.endpoints) if stats.endpoints else 0}")
        total_requests = sum(e.num_requests for e in stats.endpoints) if stats.endpoints else 0
        print(f"  Total requests: {total_requests}")

    print("\n--- Stopping string service ---")
    await server.service.stop("string")

    print("Remaining services:")
    for info in server.service.get_all_info():
        print(f"- {info.name} v{info.version}")

    try:
        print("\nTrying to use stopped string service...")
        await string.reverse("test")
    except Exception as error:
        print("Expected error:", str(error))

    await asyncio.sleep(1)
    await math_service.stop()
    await string_service.stop()
    await data_service.stop()
    await server.service.stop_all()
    await client.disconnect()
    await server.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"Test completed! Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
