import argparse
import asyncio
import contextlib
import os
import signal
import sys
from datetime import datetime
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client


def calculate_checksum(data: bytes | bytearray) -> int:
    checksum = 0
    for byte in data:
        checksum = (checksum + byte) % 0xFFFFFFFF
    return checksum


def info_method_for_target(target: str) -> str:
    mapping = {
        "python-service": "getPythonInfo",
        "node-service": "getNodeInfo",
        "go-service": "getGoInfo",
    }
    return mapping.get(target, "getInfo")


async def run_python_server(targets: list[str]):
    print("Python RPC Server Starting...")
    print(f"   Targets: {targets}")

    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "name": "python-server-unique",
            "auth": {"user": "server", "password": "server_password"},
        }
    )

    await server.connect()
    print("Python server connected")

    async def greet(name: str) -> str:
        print(f"[Python] Received greet request for: {name}")
        return f"Hello {name} from Python!"

    async def calculate(a: float, b: float, operation: str) -> float | str:
        print(f"[Python] Calculate: {a} {operation} {b}")
        if operation == "add":
            return a + b
        elif operation == "subtract":
            return a - b
        elif operation == "multiply":
            return a * b
        elif operation == "divide":
            return a / b if b != 0 else "Error: Division by zero"
        else:
            return "Unknown operation"

    async def get_python_info() -> dict[str, Any]:
        print("[Python] Returning Python info")
        return {
            "platform": "Python",
            "version": sys.version.split()[0],
            "timestamp": datetime.now().isoformat(),
            "pid": os.getpid(),
        }

    async def echo_data(data: Any) -> Any:
        print(f"[Python] Echoing data: {data}")
        return data

    async def raise_error(message: str) -> Any:
        print(f"[Python] Raising error: {message}")
        raise RuntimeError(message)

    async def get_large_data() -> dict[str, Any]:
        print("[Python] Creating 20MB test data...")
        size = 20 * 1024 * 1024  # 20MB
        data = bytearray(size)

        for i in range(size):
            data[i] = i % 256

        print("[Python] Sending 20MB data to test auto-chunking...")
        return {
            "type": "large-data",
            "size": size,
            "data": bytes(data),
            "checksum": calculate_checksum(data),
        }

    async def verify_large_data(payload: dict[str, Any]) -> dict[str, Any]:
        data: bytes | bytearray = payload["data"]
        if isinstance(data, list):
            data = bytes(data)

        print(f"[Python] Verifying received data: {len(data) / 1024 / 1024:.2f}MB")

        checksum = calculate_checksum(data)
        valid = checksum == payload["checksum"] and len(data) == payload["size"]

        print(f"[Python] Verification: {'PASSED' if valid else 'FAILED'}")
        return {
            "valid": valid,
            "receivedSize": len(data),
            "checksumMatch": checksum == payload["checksum"],
        }

    async def on_status_updates(prefix: str, callback: Any) -> Any:
        print(f"[Python] New callback subscriber for prefix '{prefix}'")
        for i in range(3):
            await callback(
                {
                    "source": "python",
                    "prefix": prefix,
                    "index": i,
                    "time": datetime.now().isoformat(),
                }
            )
            await asyncio.sleep(0.05)

        def cleanup() -> None:
            print(f"[Python] Callback cleanup for prefix '{prefix}'")

        return cleanup

    handlers: dict[str, Any] = {
        "name": "python-service",
        "greet": greet,
        "calculate": calculate,
        "getPythonInfo": get_python_info,
        "echoData": echo_data,
        "raiseError": raise_error,
        "getLargeData": get_large_data,
        "verifyLargeData": verify_large_data,
        "onStatusUpdates": on_status_updates,
    }

    unsub = await server.register_handler("python-service", handlers)
    print("Python handlers registered")

    channel = await server.channel("cross-language-chat")

    async def on_message(msg: dict[str, Any]) -> None:
        print(f"[Python Channel] Received: {msg}")

        # Respond to initial messages from any other service, not responses
        if msg.get("from") != "python" and msg.get("type") != "response":
            await channel.send(
                {
                    "from": "python",
                    "type": "response",
                    "original": msg,
                    "message": f'Python received: "{msg.get("message")}"',
                }
            )

    channel.on("message", on_message)
    print("Python channel ready")

    # Wait a bit for other services to set up
    await asyncio.sleep(3)

    print("\nPython calling target services...\n")

    failures = 0
    for target in targets:
        print(f"--- Calling {target} ---")
        proxy = server.create_proxy(target)

        try:
            service_name = await proxy.name
            print(f"{target} service name: {service_name}")

            greeting = await proxy.greet("Python")
            print(f"{target} greeting: {greeting}")

            product = await proxy.calculate(6, 7, "multiply")
            print(f"{target} calculation (6 * 7): {product}")

            info_method = info_method_for_target(target)
            info = await getattr(proxy, info_method)()
            print(f"{target} info: {info}")

            complex_data: dict[str, Any] = {
                "numbers": [10, 20, 30, 40, 50],
                "nested": {"hello": "world", "answer": 42},
                "binary": b"Hello from Python",
                "unicode": "你好世界 🌍",
            }
            echoed = await proxy.echoData(complex_data)
            print(
                f"{target} echoed complex data correctly: "
                f"{echoed['nested']['hello'] == 'world' and echoed['unicode'] == '你好世界 🌍'}"
            )

            await channel.send(
                {"from": "python", "type": "greeting", "message": f"Hello {target}, this is Python speaking!"}
            )

            print(f"\nTesting 20MB data transfer with {target}...")

            large_data = await proxy.getLargeData()
            print(f"Received {len(large_data['data']) / 1024 / 1024:.2f}MB from {target}")

            data: bytes | bytearray = large_data["data"]
            if isinstance(data, list):
                data = bytes(data)
            checksum = calculate_checksum(data)
            print(f"Data integrity check: {'PASSED' if checksum == large_data['checksum'] else 'FAILED'}")
            if checksum != large_data["checksum"]:
                failures += 1

            test_data = bytearray(20 * 1024 * 1024)
            for i in range(len(test_data)):
                test_data[i] = i % 256

            verify_result = await proxy.verifyLargeData(
                {
                    "type": "python-large-data",
                    "size": len(test_data),
                    "data": bytes(test_data),
                    "checksum": calculate_checksum(test_data),
                }
            )
            print(
                f"{target} verification of our 20MB data: {'PASSED' if verify_result['valid'] else 'FAILED'}"
            )
            if not verify_result["valid"]:
                failures += 1

            print(f"Testing callback subscription with {target}...")
            cb_count = 0
            cb_done = asyncio.Event()

            def on_status(value: Any) -> None:
                nonlocal cb_count
                cb_count += 1
                print(f"Callback from {target}: source={value.get('source')} index={value.get('index')}")
                if cb_count >= 3:
                    cb_done.set()

            cb_unsub = await proxy.onStatusUpdates(f"python-to-{target}", on_status)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(cb_done.wait(), timeout=5.0)
            await cb_unsub()
            print(f"Callback subscription test with {target}: {cb_count} events received")
            if cb_count < 3:
                failures += 1

        except Exception as e:
            print(f"Error calling {target}: {e}")
            import traceback

            traceback.print_exc()
            failures += 1

        # Error propagation: the peer handler raises; it must reach us as an
        # exception carrying the same message, not a silent success.
        err_token = f"boom python->{target}"
        try:
            await proxy.raiseError(err_token)
            print(f"{target} raiseError did NOT propagate an error")
            failures += 1
        except Exception as e:
            if err_token in str(e):
                print(f"{target} error propagation: PASSED")
            else:
                print(f"{target} error propagation: wrong message: {e}")
                failures += 1

        print()

    print("Python server running... Press Ctrl+C to stop\n")

    stop_event = asyncio.Event()

    def signal_handler():
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await stop_event.wait()
    finally:
        print("\nShutting down Python server...")
        await unsub()
        await channel.close()
        await server.disconnect()

    if failures > 0:
        print(f"\nPython cross-language test FAILED ({failures} error(s))")
        sys.exit(1)
    print("\nPython cross-language test passed")


async def main(args: Any = None):
    parser = argparse.ArgumentParser(description="Cross-language RPC test")
    parser.add_argument(
        "--targets",
        type=str,
        default="node-service",
        help="Comma-separated list of target services (default: node-service)",
    )
    parsed = parser.parse_args(args)
    targets = [t.strip() for t in parsed.targets.split(",")]

    try:
        await run_python_server(targets)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error in Python server: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    uvloop.run(main())
