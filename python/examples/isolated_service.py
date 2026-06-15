import asyncio
import time
from collections.abc import AsyncGenerator

import uvloop

from camera_ui_rpc import RPCClass, ServiceConfig, create_rpc_client


@RPCClass
class ComputeService:
    async def fibonacci(self, n: int) -> int:
        print(f"Computing fibonacci({n})")
        if n <= 1:
            return n

        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b

        await asyncio.sleep(0.1)
        return b

    async def generate_primes(self, limit: int) -> AsyncGenerator[int, None]:
        print(f"Generating primes up to {limit}")

        for num in range(2, limit + 1):
            is_prime = True

            for i in range(2, int(num**0.5) + 1):
                if num % i == 0:
                    is_prime = False
                    break

            if is_prime:
                yield num
                await asyncio.sleep(0.05)


@RPCClass
class EchoService:
    async def echo(self, message: str) -> str:
        print(f"Echo: {message}")
        return f"Echo: {message}"

    async def ping(self) -> str:
        return "pong"


async def main():
    total_start = time.perf_counter()
    print("Starting isolated service connection test...\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "service-server",
        }
    )

    await server.connect()
    print("Server connected")

    compute_service = await server.service.register_handler(
        ServiceConfig(
            name="compute",
            version="1.0.0",
            description="Heavy computation service",
        ),
        ComputeService(),
    )

    echo_service = await server.service.register_handler(
        ServiceConfig(
            name="echo",
            version="1.0.0",
            description="Lightweight echo service",
        ),
        EchoService(),
    )

    print("Services registered\n")

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "test-client",
        }
    )

    await client.connect()
    print("Client connected\n")

    print("--- Creating Service Proxies ---")

    compute_proxy = await client.create_service_proxy(
        "compute",
        isolated_connection=True,
        timeout=60000,  # 60 seconds for heavy computations
    )
    compute_proxy_service: ComputeService = compute_proxy.proxy
    print("Compute service proxy created (isolated connection)")

    echo_proxy_service = await client.create_service_proxy("echo", EchoService, isolated_connection=False)
    print("Echo service proxy created (shared connection)\n")

    print("--- Testing Concurrent Operations ---")

    print("Starting heavy computation (fibonacci)...")
    fib_start = time.perf_counter()

    async def compute_fibonacci() -> int:
        result = await compute_proxy_service.fibonacci(40)
        elapsed = (time.perf_counter() - fib_start) * 1000
        print(f"Fibonacci(40) = {result} ({elapsed:.1f}ms)")
        return result

    fib_task = asyncio.create_task(compute_fibonacci())

    print("\nTesting echo service while computing...")
    echo_times: list[float] = []
    for i in range(1, 6):
        start = time.perf_counter()
        result = await echo_proxy_service.echo(f"Message {i}")
        elapsed = (time.perf_counter() - start) * 1000
        echo_times.append(elapsed)
        print(f"{result} ({elapsed:.1f}ms)")
        await asyncio.sleep(0.2)
    avg_echo_time = sum(echo_times) / len(echo_times)
    print(f"Average echo time: {avg_echo_time:.1f}ms")

    print("\nWaiting for fibonacci computation...")
    await fib_task

    print("\n--- Testing Streaming with Isolation ---")
    stream_start = time.perf_counter()
    primes_gen = compute_proxy_service.generate_primes(30)
    collected_primes: list[int] = []

    print("Collecting primes while doing other work...")

    async def collect_primes() -> None:
        async for prime in primes_gen:
            collected_primes.append(prime)
            print(f"Received prime: {prime}")

    prime_task = asyncio.create_task(collect_primes())

    for i in range(1, 4):
        await asyncio.sleep(0.1)
        pong = await echo_proxy_service.ping()
        print(f"Ping {i}: {pong}")

    await prime_task
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(
        f"\nCollected {len(collected_primes)} primes in {stream_time:.1f}ms: {', '.join(map(str, collected_primes))}"
    )

    print("\n--- Testing Connection Isolation ---")
    print("Disconnecting compute service (isolated connection)...")
    await compute_proxy.close()
    print("Isolated connection closed")

    print("\nTesting echo service after compute disconnect...")
    try:
        start = time.perf_counter()
        result = await echo_proxy_service.echo("Still working?")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Echo service: {result} ({elapsed:.1f}ms)")
    except Exception as e:
        print(f"Echo failed: {e}")

    print("\nTesting compute service after disconnect...")
    try:
        await compute_proxy_service.fibonacci(10)
        print("ERROR: Compute service should have failed!")
    except Exception as e:
        print(f"Expected error: {e}")

    print("\nCleaning up...")
    await compute_service.stop()
    await echo_service.stop()
    await client.disconnect()
    await server.disconnect()

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
