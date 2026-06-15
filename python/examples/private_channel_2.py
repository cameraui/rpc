import asyncio
import contextlib
import time
from datetime import datetime
from typing import Any

import uvloop

from camera_ui_rpc import create_rpc_client

buffer_10mb = bytearray(10 * 1024 * 1024)
for i in range(len(buffer_10mb)):
    buffer_10mb[i] = ord("x")
buffer_10mb = bytes(buffer_10mb)


async def main():
    total_start = time.perf_counter()
    is_connected = False

    client_a = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "client-a",
        }
    )

    client_b = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "client-b",
        }
    )

    await client_a.connect()
    await client_b.connect()

    print("Clients connected")

    is_connected = True

    start = time.perf_counter()
    client_a_channel = await client_a.private_channel("secret-chat", "client-b", isolated_connection=True)
    channel_a_time = (time.perf_counter() - start) * 1000

    start = time.perf_counter()
    client_b_channel = await client_b.private_channel("secret-chat", "client-a", isolated_connection=True)
    channel_b_time = (time.perf_counter() - start) * 1000

    print(f"Private channels created (A: {channel_a_time:.1f}ms, B: {channel_b_time:.1f}ms)")

    # Give time for handshakes
    await asyncio.sleep(0.1)

    def client_a_handler(data: Any) -> None:
        msg_info = {k: v for k, v in data.items() if k != "file"}
        if "file" in data:
            msg_info["file_size"] = len(data["file"])
        print("[Client A received]:", msg_info)

    def client_b_handler(data: Any) -> None:
        msg_info = {k: v for k, v in data.items() if k != "file"}
        if "file" in data:
            msg_info["file_size"] = len(data["file"])
        print("[Client B received]:", msg_info)

    client_a_channel.on("message", client_a_handler)
    client_b_channel.on("message", client_b_handler)

    async def test_infinite_message():
        nonlocal is_connected
        count = 0
        current_channel = client_a_channel
        file_data = buffer_10mb
        send_times: list[float] = []

        while is_connected:
            try:
                from_client = "Client A" if current_channel == client_a_channel else "Client B"
                send_start = time.perf_counter()
                await current_channel.send(
                    {
                        "from": from_client,
                        "text": f"Message {count}",
                        "date": datetime.now().isoformat(),
                        "file": file_data,
                    }
                )
                send_time = (time.perf_counter() - send_start) * 1000
                send_times.append(send_time)
                throughput = (10 * 1024 * 1024) / ((send_time / 1000) * 1024 * 1024)
                print(
                    f"[{from_client}] Sent 10MB message {count} in {send_time:.1f}ms ({throughput:.1f} MB/s)"
                )
                count += 1
            except Exception as err:
                if not is_connected:
                    break

                print(f"Error sending message: {err}")

            if is_connected:
                await asyncio.sleep(1)
                current_channel = (
                    client_b_channel if current_channel == client_a_channel else client_a_channel
                )

    message_task = asyncio.create_task(test_infinite_message())

    await asyncio.sleep(10)

    print("\nDisconnecting clients...")
    is_connected = False

    message_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await message_task

    await client_a_channel.close()
    await client_b_channel.close()
    await client_a.disconnect()
    await client_b.disconnect()

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
