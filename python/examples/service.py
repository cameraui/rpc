import asyncio
import random
import time
from collections.abc import AsyncGenerator
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, RPCNested, ServiceConfig, create_rpc_client

buffer_5mb = bytearray(5 * 1024 * 1024)
for i in range(len(buffer_5mb)):
    buffer_5mb[i] = i % 256
buffer_5mb = bytes(buffer_5mb)


@RPCClass
class ComputeService:
    def __init__(self):
        self.name = "GPU Compute Node 1"

    async def add(self, a: int, b: int) -> int:
        return a + b

    async def create_data(self, size_mb: int) -> bytes:
        if size_mb == 5:
            return buffer_5mb
        buffer = bytearray(size_mb * 1024 * 1024)
        for i in range(len(buffer)):
            buffer[i] = i % 256
        return bytes(buffer)

    async def generate_numbers(self, count: int) -> AsyncGenerator[dict[str, Any], None]:
        for i in range(count):
            data = bytes([i % 256] * (1024 * 1024))

            yield {
                "index": i,
                "data": data,
                "timestamp": int(time.perf_counter() * 1000),
            }

            await asyncio.sleep(0.1)

    async def failing_method(self) -> None:
        raise Exception("This method always fails")

    @property
    @RPCNested
    def config(self) -> "ConfigNamespace":
        return ConfigNamespace(self)


@RPCClass
class ConfigNamespace:
    def __init__(self, parent: ComputeService):
        self.parent: ComputeService = parent
        self._monitor: MonitorNamespace = MonitorNamespace()

    @property
    @RPCNested
    def monitor(self) -> "MonitorNamespace":
        return self._monitor

    async def get(self) -> dict[str, Any]:
        return {
            "name": self.parent.name,
            "maxConcurrent": 10,
            "gpu": True,
        }

    async def set(self, config: dict[str, Any]) -> dict[str, bool]:
        if "name" in config:
            self.parent.name = config["name"]
        return {"success": True}


@RPCClass
class MonitorNamespace:
    async def cpu(self) -> dict[str, float]:
        return {"usage": random.random() * 100}

    async def memory(self) -> dict[str, float]:
        return {
            "used": random.random() * 16 * 1024,
            "total": 16 * 1024,
        }


async def main():
    total_start = time.perf_counter()
    print("Starting service test...\n")

    server1 = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "compute-server-1",
        }
    )

    await server1.connect()
    print("Server 1 connected")

    service1 = await server1.service.register_handler(
        ServiceConfig(
            name="compute",
            version="1.0.0",
            description="High-performance compute service",
            queue_group="compute-workers",  # Enable load balancing
            metadata={
                "host": "server1",
                "gpu": "true",
                "region": "us-east",
            },
        ),
        ComputeService(),
    )

    print(f"Service 1 registered: {service1.info()}")

    server2 = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "compute-server-2",
        }
    )

    await server2.connect()

    compute_service2 = ComputeService()
    await compute_service2.config.set({"name": "GPU Compute Node 2"})

    service2 = await server2.service.register_handler(
        ServiceConfig(
            name="compute",
            version="1.0.0",
            description="High-performance compute service",
            queue_group="compute-workers",  # Same queue for load balancing
            metadata={
                "host": "server2",
                "gpu": "false",
                "region": "eu-west",
            },
        ),
        compute_service2,
    )

    print(f"Service 2 registered: {service2.info()}")

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
    monitor = server1.service.monitor()

    services = await monitor.info("compute")
    for info in services:
        print(f"Found service: {info.id}")
        print(f"  Version: {info.version}")
        print(f"  Metadata: {info.metadata}")
        print(f"  Endpoints: {[e.subject for e in info.endpoints]}")

    print("\n--- Testing Service Calls ---")
    proxy_start = time.perf_counter()
    compute = await client.create_service_proxy("compute", ComputeService, isolated_connection=False)
    proxy_time = (time.perf_counter() - proxy_start) * 1000
    print(f"Service proxy created in {proxy_time:.1f}ms")

    start = time.perf_counter()
    sum_result = await compute.add(5, 3)
    add_time = (time.perf_counter() - start) * 1000
    print(f"Add result: {sum_result} ({add_time:.1f}ms)")

    start = time.perf_counter()
    config = await compute.config.get()
    config_time = (time.perf_counter() - start) * 1000
    print(f"Config: {config} ({config_time:.1f}ms)")

    start = time.perf_counter()
    cpu = await compute.config.monitor.cpu()
    cpu_time = (time.perf_counter() - start) * 1000
    print(f"CPU usage: {cpu} ({cpu_time:.1f}ms)")

    print("\n--- Testing Large Data Transfer ---")
    start = time.perf_counter()
    large_data = await compute.create_data(5)
    data_time = (time.perf_counter() - start) * 1000
    throughput = (5 * 1024 * 1024) / ((data_time / 1000) * 1024 * 1024)
    print(
        f"Received {len(large_data) / 1024 / 1024:.2f}MB of data in {data_time:.1f}ms ({throughput:.1f} MB/s)"
    )

    print("\n--- Testing Streaming ---")
    stream_start = time.perf_counter()
    stream = compute.generate_numbers(5)
    stream_count = 0
    async for item in stream:
        print(f"Stream item {item['index']}: {len(item['data']) / 1024 / 1024:.2f}MB")
        stream_count += 1
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(f"Received {stream_count} stream items in {stream_time:.1f}ms")

    print("\n--- Testing Error Handling ---")
    start = time.perf_counter()
    try:
        await compute.failing_method()
    except Exception as e:
        error_time = (time.perf_counter() - start) * 1000
        print(f"Caught expected error: {e} ({error_time:.1f}ms)")

    print("\n--- Testing Load Balancing ---")
    print("Making 10 requests to see distribution...")
    lb_start = time.perf_counter()
    for i in range(10):
        req_start = time.perf_counter()
        config = await compute.config.get()
        req_time = (time.perf_counter() - req_start) * 1000
        print(f"Request {i + 1}: Handled by {config['name']} ({req_time:.1f}ms)")
    lb_time = (time.perf_counter() - lb_start) * 1000
    print(f"Total load balancing test time: {lb_time:.1f}ms")

    print("\n--- Service Stats ---")
    stats_list = await monitor.stats("compute")
    for stats in stats_list:
        print(f"Stats for {stats.id}:")
        print(f"  Started: {stats.started}")
        if stats.endpoints:
            endpoints_data: list[dict[str, Any]] = []
            for e in stats.endpoints:
                endpoints_data.append(
                    {
                        "name": e.name,
                        "requests": e.num_requests,
                        "errors": e.num_errors,
                        "avgTime": f"{e.average_processing_time / 1_000_000:.2f}ms"
                        if hasattr(e, "average_processing_time")
                        else "N/A",
                    }
                )
            print(f"  Endpoints: {endpoints_data}")

    await asyncio.sleep(1)
    await service1.stop()
    await service2.stop()
    await client.disconnect()
    await server1.disconnect()
    await server2.disconnect()

    total_elapsed = (time.perf_counter() - total_start) * 1000
    print(f"\nTest completed! Total time: {total_elapsed:.1f}ms")


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
