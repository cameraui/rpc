import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

import uvloop

from camera_ui_rpc import ProxyWithClose, RPCClass, RPCClient, ServiceConfig, create_rpc_client


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
        result = f"Echo: {message}"
        return result

    async def ping(self) -> str:
        return "pong"


async def main():
    total_start = time.perf_counter()
    print("Starting server-side isolated service test...\n")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "service-server",
        }
    )

    await server.connect()
    print("Server connected")

    print("Registering compute service with isolated connection...")
    compute_service = await server.service.register_handler(
        ServiceConfig(
            name="compute",
            version="1.0.0",
            description="Heavy computation service (isolated)",
        ),
        ComputeService(),
        isolated_connection=True,
    )

    print("Registering echo service on main connection...")
    echo_service = await server.service.register_handler(
        ServiceConfig(
            name="echo",
            version="1.0.0",
            description="Lightweight echo service",
        ),
        EchoService(),
    )

    print("Services registered\n")

    clients: list[RPCClient] = []
    NUM_CLIENTS = 5

    for i in range(NUM_CLIENTS):
        client = create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "auth": {"user": "server", "password": "server_password"},
                "name": f"test-client-{i}",
            }
        )
        await client.connect()
        clients.append(client)

    print(f"{NUM_CLIENTS} clients connected\n")

    print("--- Testing Concurrent Heavy Computations ---")
    print("All clients will request fibonacci calculations simultaneously...\n")

    compute_proxies: list[ProxyWithClose[Any]] = []
    echo_proxies: list[Any] = []

    for client in clients:
        compute_proxy = await client.create_service_proxy("compute", isolated_connection=True)
        echo_proxy = await client.create_service_proxy("echo", isolated_connection=False)
        compute_proxies.append(compute_proxy)
        echo_proxies.append(echo_proxy)

    compute_start = time.perf_counter()
    compute_promises: list[asyncio.Task[Any]] = []
    for i, proxy_info in enumerate(compute_proxies):
        n = 35 + i
        print(f"Client {i}: Starting fibonacci({n})")

        async def compute_and_log(client_id: int, fib_n: int, proxy_obj: Any) -> int:
            client_start = time.perf_counter()
            result = await proxy_obj.fibonacci(fib_n)
            elapsed = (time.perf_counter() - client_start) * 1000
            print(f"Client {client_id}: fibonacci({fib_n}) = {result} ({elapsed:.1f}ms)")
            return result

        compute_promises.append(asyncio.create_task(compute_and_log(i, n, proxy_info.proxy)))

    print("\nTesting echo service responsiveness during heavy load...")
    echo_times: list[list[float]] = []
    for round_num in range(3):
        await asyncio.sleep(0.1)

        echo_promises: list[asyncio.Task[Any]] = []
        for i, proxy_info in enumerate(echo_proxies):

            async def echo_test(client_id: int, round_id: int, proxy_obj: Any) -> dict[str, Any]:
                start = time.perf_counter()
                await proxy_obj.echo(f"Round {round_id + 1} from client {client_id}")
                elapsed = (time.perf_counter() - start) * 1000
                return {"client": client_id, "elapsed": elapsed}

            echo_promises.append(asyncio.create_task(echo_test(i, round_num, proxy_info)))

        results = await asyncio.gather(*echo_promises)
        round_times = [r["elapsed"] for r in results]
        echo_times.append(round_times)
        times = ", ".join(f"{r['elapsed']:.1f}ms" for r in results)
        print(f"Round {round_num + 1} echo times: {times}")

    all_times = [t for round_times in echo_times for t in round_times]
    avg_echo_time = sum(all_times) / len(all_times)
    print(f"Average echo time during heavy load: {avg_echo_time:.1f}ms")

    print("\nWaiting for all computations to complete...")
    await asyncio.gather(*compute_promises)
    compute_time = (time.perf_counter() - compute_start) * 1000
    print(f"All computations completed in {compute_time:.1f}ms")

    print("\n--- Testing Concurrent Streaming ---")
    stream_start = time.perf_counter()
    stream_promises: list[asyncio.Task[Any]] = []

    for i in range(3):
        proxy_info = compute_proxies[i]

        async def stream_primes(client_id: int, proxy_obj: Any) -> list[int]:
            print(f"Client {client_id}: Starting prime generation")
            client_stream_start = time.perf_counter()
            primes = proxy_obj.generate_primes(20)
            collected: list[int] = []

            async for prime in primes:
                collected.append(prime)

            elapsed = (time.perf_counter() - client_stream_start) * 1000
            print(f"Client {client_id}: Collected {len(collected)} primes in {elapsed:.1f}ms")
            return collected

        stream_promises.append(asyncio.create_task(stream_primes(i, proxy_info.proxy)))

    await asyncio.gather(*stream_promises)
    stream_time = (time.perf_counter() - stream_start) * 1000
    print(f"All streams completed in {stream_time:.1f}ms")

    print("\n--- Connection Status ---")
    print(f"Main server connection active: {server.is_connected}")
    print("Isolated service connections managed by services")

    print("\nCleaning up...")

    await asyncio.gather(*(proxy.close() for proxy in compute_proxies))

    await asyncio.gather(*(client.disconnect() for client in clients))

    await echo_service.stop()
    await compute_service.stop()
    await server.service.stop_all()
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
