#!/usr/bin/env python3
import argparse
import asyncio
import signal
import sys
from collections.abc import AsyncGenerator
from typing import Any

from camera_ui_rpc import create_rpc_client, rpc_callbacks
from camera_ui_rpc.handler import CallbackInvoker


async def pull_batches(
    batch_count: int, chunks_per_batch: int, invoker: CallbackInvoker
) -> AsyncGenerator[None, None]:
    for b in range(batch_count):
        if not invoker.active:
            return
        for i in range(chunks_per_batch):
            await invoker.invoke("onChunk", {"batch": b, "index": i})
        await invoker.invoke("onChunk", None)
        yield


async def run_server(name: str) -> None:
    print(f"[py-server {name}] starting...")
    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": f"pullcb-cross-py-server-{name}",
        }
    )
    await client.connect()
    # Register under camelCase subject so Node/Go clients can reach it.
    handlers = {"pullBatches": pull_batches}
    unsub = await client.register_handler(f"pullcb-{name}", handlers, without_decorators=True)
    print(f"[py-server {name}] registered under namespace pullcb-{name}, ready.")

    stop_event = asyncio.Event()

    def _stop() -> None:
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _stop)

    await stop_event.wait()
    print(f"[py-server {name}] shutting down...")
    await unsub()
    await client.disconnect()


async def test_target(client_name: str, target: str) -> bool:
    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": f"pullcb-cross-py-client-{client_name}-to-{target}",
        }
    )
    try:
        await client.connect()
        proxy = client.create_proxy(f"pullcb-{target}")

        received: list[dict[str, Any]] = []
        batch_ends = 0

        def on_chunk(data: Any) -> None:
            nonlocal batch_ends
            if data is None:
                batch_ends += 1
                return
            received.append(data)

        cbs = rpc_callbacks(onChunk=on_chunk, oneway=["onChunk"])

        BATCHES = 3
        CHUNKS = 5

        batches_consumed = 0
        # camelCase method name — matches Node/Go conventions and the
        # camelCase key in the server's handler dict.
        async for _ in proxy.pullBatches(BATCHES, CHUNKS, cbs):
            batches_consumed += 1

        await asyncio.sleep(0.1)

        order_ok = all(
            int(r.get("batch", -1)) == i // CHUNKS and int(r.get("index", -1)) == i % CHUNKS
            for i, r in enumerate(received)
        )

        ok = (
            batches_consumed == BATCHES
            and len(received) == BATCHES * CHUNKS
            and batch_ends == BATCHES
            and order_ok
        )

        if ok:
            print(f"  [py -> {target}] PASS ({len(received)} chunks, {batch_ends} EOB)")
        else:
            print(
                f"  [py -> {target}] FAIL: consumed={batches_consumed}/{BATCHES}, "
                f"chunks={len(received)}/{BATCHES * CHUNKS}, ends={batch_ends}/{BATCHES}, order_ok={order_ok}"
            )
        return ok
    except Exception as e:
        print(f"  [py -> {target}] ERROR: {e}")
        return False
    finally:
        await client.disconnect()


async def run_client(targets: list[str]) -> None:
    print(f"[py-client] testing targets: {', '.join(targets)}")
    results: list[tuple[str, bool]] = []
    for t in targets:
        ok = await test_target("py", t)
        results.append((t, ok))

    failed = [t for t, ok in results if not ok]
    print()
    if not failed:
        print(f"[py-client] all {len(results)} targets passed")
        sys.exit(0)
    else:
        print(f"[py-client] {len(failed)}/{len(results)} targets failed")
        sys.exit(1)


async def _main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["server", "client"], default="client")
    parser.add_argument("--name", default="python")
    parser.add_argument("--targets", default="node,go,python")
    args = parser.parse_args(argv)

    if args.role == "server":
        await run_server(args.name)
    else:
        targets = [t.strip() for t in args.targets.split(",") if t.strip()]
        await run_client(targets)


async def main(argv: list[str] | None = None) -> None:
    await _main(argv if argv is not None else sys.argv[1:])


if __name__ == "__main__":
    asyncio.run(main())
