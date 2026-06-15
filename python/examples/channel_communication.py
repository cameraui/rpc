import asyncio
import time
from typing import Any, TypedDict

import uvloop

from camera_ui_rpc import Channel, RPCClient, create_rpc_client

large_buffer = b"x" * (2 * 1024 * 1024)  # 2MB


class ChannelInfo(TypedDict):
    user: str
    channel: Channel


async def channel_example():
    print("Channel Communication Example\n")

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

    print("Connected both clients\n")

    channel_a = await client_a.channel("chat-room-123")
    channel_b = await client_b.channel("chat-room-123")

    print("Created channels on both sides\n")

    def on_message_a(data: Any) -> None:
        display_data = {k: v for k, v in data.items() if k != "content"}
        if "content" in data:
            display_data["content"] = f"<{len(data['content'])} bytes>"

        print(f"Channel A received message: {display_data}")

    def on_close_a() -> None:
        print("Channel A closed")

    channel_a.on("message", on_message_a)
    channel_a.on("close", on_close_a)

    def on_message_b(data: Any) -> None:
        display_data = {k: v for k, v in data.items() if k != "content"}
        if "content" in data:
            display_data["content"] = f"<{len(data['content'])} bytes>"

        print(f"Channel B received message: {display_data}")

    def on_close_b() -> None:
        print("Channel B closed")

    channel_b.on("message", on_message_b)
    channel_b.on("close", on_close_b)

    print("Sending messages")

    small_start = time.perf_counter()
    await channel_a.send({"from": "A", "message": "Hello from client A!"})
    await channel_b.send({"from": "B", "message": "Hi from client B!"})
    await channel_a.send({"type": "user-info", "user": {"id": 1, "name": "Alice", "status": "online"}})
    small_time = time.perf_counter() - small_start
    print(f"Small messages sent in {small_time * 1000:.0f}ms")

    large_data: dict[str, Any] = {
        "from": "A",
        "type": "file-transfer",
        "filename": "large-dataset.json",
        "content": large_buffer,
    }

    print("\nSending large data (2MB)")
    large_start = time.perf_counter()
    await channel_a.send(large_data)
    large_time = time.perf_counter() - large_start
    print(f"Large data (2MB) sent in {large_time * 1000:.0f}ms ({2 / large_time:.2f} MB/s)")

    def on_error_a(error: Exception) -> None:
        print(f"Channel A error: {error}")

    channel_a.on("error", on_error_a)

    await asyncio.sleep(0.1)

    print("\nClosing channel from Client A")
    await channel_a.close()

    try:
        await channel_a.send({"message": "This should fail"})
    except Exception as e:
        print(f"Client A expected error: {e}")

    await client_a.disconnect()
    await client_b.disconnect()

    print("\nChannel communication example completed")


async def chat_room_example():
    print("\n\nMulti-Party Chat Room Example\n")

    users = ["Alice", "Bob", "Charlie"]
    clients: list[RPCClient] = []
    channels: list[ChannelInfo] = []
    room_id = "team-standup"

    for user in users:
        client = create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "auth": {"user": "server", "password": "server_password"},
                "name": f"user-{user.lower()}",
            }
        )
        await client.connect()
        clients.append(client)

        channel = await client.channel(room_id)
        channels.append({"user": user, "channel": channel})

        def make_handler(u: str):
            def handler(data: dict[str, Any]):
                if data.get("from") != u:
                    print(f"[{u}] {data.get('from')}: {data.get('message')}")

            return handler

        channel.on("message", make_handler(user))

    print("All users joined the chat room\n")

    await channels[0]["channel"].send({"from": "Alice", "message": "Good morning team!"})
    await channels[1]["channel"].send({"from": "Bob", "message": "Hi Alice! Ready for standup?"})
    await channels[2]["channel"].send({"from": "Charlie", "message": "Hey everyone!"})

    await asyncio.sleep(0.1)

    print("\nFile sharing")
    await channels[0]["channel"].send(
        {
            "from": "Alice",
            "type": "file-share",
            "message": "Sharing today's agenda",
            "file": {
                "name": "standup-agenda.md",
                "size": 1024,
                "content": "# Today's Standup\n1. Updates\n2. Blockers\n3. Plans",
            },
        }
    )

    await asyncio.sleep(0.1)

    for channel_info in channels:
        await channel_info["channel"].close()
    for client in clients:
        await client.disconnect()

    print("\nChat room example completed")


async def main():
    total_start = time.perf_counter()
    try:
        await channel_example()
        await chat_room_example()
        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll tests completed. Total time: {total_elapsed:.1f}ms")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error in native channel request test: {e}")
        import traceback

        traceback.print_exc()
