import asyncio
import time
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from typing import Any

import uvloop

from camera_ui_rpc import RPCClass, RPCMethod, create_rpc_client


@RPCClass
class GeneratorService:
    @RPCMethod
    async def async_generator_generate_numbers(self, count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            await asyncio.sleep(0.1)
            yield i

    @RPCMethod
    def sync_generator_generate_numbers(self, count: int) -> Generator[int, None, None]:
        for i in range(count):
            yield i * 2

    @RPCMethod
    async def async_generate_func_returning_async_gen(self, count: int) -> AsyncGenerator[str, Any] | str:
        return_async = False

        async def inner_gen():
            for i in range(count):
                await asyncio.sleep(0.05)
                yield f"async-{i}"

        if return_async:
            return ""

        return inner_gen()

    @RPCMethod
    async def async_generate_func_returning_sync_gen(self, count: int) -> Generator[str, None, None] | str:
        await asyncio.sleep(0.1)

        return_async = False

        def inner_gen():
            for i in range(count):
                yield f"sync-from-async-{i}"

        if return_async:
            return ""

        return inner_gen()

    @RPCMethod
    def sync_generate_func_returning_sync_gen(self, count: int) -> Generator[dict[str, Any], None, None]:
        return ({"index": i, "value": i**2} for i in range(count))

    @RPCMethod
    async def mixed_generate_type_generator(
        self, count: int
    ) -> AsyncGenerator[int | str | dict[str, Any], None]:
        for i in range(count):
            if i % 3 == 0:
                yield i
            elif i % 3 == 1:
                yield f"string-{i}"
            else:
                yield {"type": "dict", "value": i}

    @RPCMethod
    def get_iterable_array(self, count: int) -> list[int]:
        return [i * 3 for i in range(count)]

    @RPCMethod
    async def get_async_iterable_array(self, count: int) -> list[str]:
        await asyncio.sleep(0.05)
        return [f"item-{i}" for i in range(count)]


async def test_generators():
    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "name": "generator-test-server",
            "auth": {"user": "server", "password": "server_password"},
        }
    )

    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "name": "generator-test-client",
            "auth": {"user": "server", "password": "server_password"},
        }
    )

    unsubscribe: Callable[[], Coroutine[Any, Any, None]] | None = None

    try:
        await server.connect()
        await client.connect()

        service = GeneratorService()
        unsubscribe = await server.register_handler("generator", service)

        # The proxy automatically converts generators to async generators at runtime
        # This is because Python's type system can't express the transformation
        # from Generator to AsyncGenerator like TypeScript can
        proxy: Any = client.create_proxy("generator", GeneratorService)

        await asyncio.sleep(0.1)

        print("Testing different generator types:\n")
        total_start = time.perf_counter()

        print("1. Async generator function:")
        start = time.perf_counter()
        values: list[Any] = []
        async for value in proxy.async_generator_generate_numbers(5):
            values.append(value)
            print(f"   Received: {value}")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms (5 items, ~{elapsed / 5:.1f}ms per item)")

        print("\n2. Sync generator function:")
        start = time.perf_counter()
        values = []
        async for value in proxy.sync_generator_generate_numbers(5):
            values.append(value)
            print(f"   Received: {value}")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms ({len(values)} items, ~{elapsed / len(values):.1f}ms per item)")

        print("\n3. Async function returning async generator:")
        start = time.perf_counter()
        values = []
        async for value in proxy.async_generate_func_returning_async_gen(5):
            values.append(value)
            print(f"   Received: {value}")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms ({len(values)} items)")

        print("\n4. Async function returning sync generator:")
        start = time.perf_counter()
        values = []
        async for value in proxy.async_generate_func_returning_sync_gen(5):
            values.append(value)
            print(f"   Received: {value}")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms ({len(values)} items)")

        print("\n5. Sync function returning sync generator:")
        start = time.perf_counter()
        values = []
        async for value in proxy.sync_generate_func_returning_sync_gen(5):
            values.append(value)
            print(f"   Received: {value}")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms ({len(values)} items)")

        print("\n6. Mixed type generator:")
        start = time.perf_counter()
        type_counts = {"int": 0, "str": 0, "dict": 0}
        async for value in proxy.mixed_generate_type_generator(9):
            type_name = type(value).__name__
            type_counts[type_name] += 1
            print(f"   Received: {value} (type: {type_name})")
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms (types: {type_counts})")

        print("\n7. Function returning iterable (list):")
        start = time.perf_counter()
        iterable_array = await proxy.get_iterable_array(5)
        elapsed = (time.perf_counter() - start) * 1000
        for value in iterable_array:
            print(f"   Received: {value}")
        print(f"   Time: {elapsed:.1f}ms (returned {len(iterable_array)} items)")

        print("\n8. Async function returning iterable:")
        start = time.perf_counter()
        async_iterable_array = await proxy.get_async_iterable_array(5)
        elapsed = (time.perf_counter() - start) * 1000
        for value in async_iterable_array:
            print(f"   Received: {value}")
        print(f"   Time: {elapsed:.1f}ms (returned {len(async_iterable_array)} items)")

        print("\n9. Early termination test:")
        start = time.perf_counter()
        count = 0
        async for value in proxy.async_generator_generate_numbers(100):
            print(f"   Received: {value}")
            count += 1
            if count >= 3:
                print("   Breaking early...")
                break
        elapsed = (time.perf_counter() - start) * 1000
        print(f"   Time: {elapsed:.1f}ms (received {count} items before breaking)")

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll generator tests passed! Total time: {total_elapsed:.1f}ms")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if unsubscribe:
            await unsubscribe()
        await client.disconnect()
        await server.disconnect()


async def main():
    try:
        await test_generators()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error in Python server: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    uvloop.run(main())
