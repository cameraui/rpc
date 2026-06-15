import asyncio
import time
from typing import Any

import uvloop

from camera_ui_rpc import PrivateChannel, RPCClient, create_rpc_client

buffer_3mb = bytearray(3 * 1024 * 1024)
for i in range(len(buffer_3mb)):
    buffer_3mb[i] = ord("x")
buffer_3mb = bytes(buffer_3mb)


async def private_channel_example():
    print("=== Private Channel Example ===\n")

    alice = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "alice",
        }
    )

    bob = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "bob",
        }
    )

    charlie = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "charlie",
        }
    )

    await alice.connect()
    await bob.connect()
    await charlie.connect()

    print("All clients connected\n")

    start = time.perf_counter()
    alice_channel = await alice.private_channel("secret-chat", "bob")
    alice_channel_time = (time.perf_counter() - start) * 1000

    start = time.perf_counter()
    bob_channel = await bob.private_channel("secret-chat", "alice")
    bob_channel_time = (time.perf_counter() - start) * 1000

    # Charlie joins the same channel ID but won't receive anything
    start = time.perf_counter()
    charlie_channel = await charlie.private_channel("secret-chat", "alice")
    charlie_channel_time = (time.perf_counter() - start) * 1000

    print(
        f"Private channels created (Alice: {alice_channel_time:.1f}ms, Bob: {bob_channel_time:.1f}ms, Charlie: {charlie_channel_time:.1f}ms)\n"
    )

    # Give time for handshakes
    await asyncio.sleep(0.05)

    def alice_handler(data: Any) -> None:
        display_data = {k: v for k, v in data.items() if k != "data"}
        if "data" in data:
            display_data["data"] = f"<{len(data['data'])} bytes>"

        print("[Alice received]:", display_data)

    def bob_handler(data: Any) -> None:
        display_data = {k: v for k, v in data.items() if k != "data"}
        if "data" in data:
            display_data["data"] = f"<{len(data['data'])} bytes>"

        print("[Bob received]:", display_data)

    def charlie_handler(data: Any) -> None:
        display_data = {k: v for k, v in data.items() if k != "data"}
        if "data" in data:
            display_data["data"] = f"<{len(data['data'])} bytes>"

        print("[Charlie received]:", display_data)

    alice_channel.on("message", alice_handler)
    bob_channel.on("message", bob_handler)
    charlie_channel.on("message", charlie_handler)

    print("--- Private Messages ---")
    msg_start = time.perf_counter()
    await alice_channel.send({"from": "Alice", "text": "Hi Bob, this is private!"})
    alice_send_time = (time.perf_counter() - msg_start) * 1000

    msg_start = time.perf_counter()
    await bob_channel.send({"from": "Bob", "text": "Hi Alice, got your private message!"})
    bob_send_time = (time.perf_counter() - msg_start) * 1000

    # Charlie's send should not be received by Alice or Bob
    msg_start = time.perf_counter()
    await charlie_channel.send({"from": "Charlie", "text": "Can anyone hear me?"})
    charlie_send_time = (time.perf_counter() - msg_start) * 1000

    await asyncio.sleep(0.1)

    print(
        f"\nMessage send times: Alice: {alice_send_time:.1f}ms, Bob: {bob_send_time:.1f}ms, Charlie: {charlie_send_time:.1f}ms"
    )

    print("\n--- Channel Info ---")
    print(f"Alice connected to: {alice_channel.remote_id}")
    print(f"Bob connected to: {bob_channel.remote_id}")
    print(f"Charlie connected to: {charlie_channel.remote_id or 'nobody'}")

    await alice_channel.close()
    await bob_channel.close()
    await charlie_channel.close()

    await alice.disconnect()
    await bob.disconnect()
    await charlie.disconnect()

    print("\nPrivate channel example completed!")


async def direct_messaging_example():
    print("\n\n=== Direct Messaging System ===\n")

    class User:
        def __init__(self, client: RPCClient, name: str):
            self.client = client
            self.name = name
            self.channels: dict[str, PrivateChannel] = {}

        async def connect(self):
            await self.client.connect()

        async def send_dm(self, recipient: str, message: str):
            channel_id = "-".join(sorted([self.name, recipient]))

            channel = self.channels.get(recipient)
            if not channel:
                channel = await self.client.private_channel(channel_id, recipient)
                self.channels[recipient] = channel

                def handler(data: Any) -> None:
                    print(f"[{self.name}] DM from {data['from']}: {data['message']}")

                channel.on("message", handler)

            await channel.send({"from": self.name, "message": message})

        async def listen_for_dms(self):
            # In a real system, you'd have a discovery mechanism
            # For this example, we'll just listen on expected channels
            pass

        async def disconnect(self):
            for channel in self.channels.values():
                await channel.close()
            await self.client.disconnect()

    alice = User(
        create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "auth": {"user": "server", "password": "server_password"},
                "name": "alice",
            }
        ),
        "alice",
    )

    bob = User(
        create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "auth": {"user": "server", "password": "server_password"},
                "name": "bob",
            }
        ),
        "bob",
    )

    charlie = User(
        create_rpc_client(
            {
                "servers": ["nats://localhost:4222"],
                "auth": {"user": "server", "password": "server_password"},
                "name": "charlie",
            }
        ),
        "charlie",
    )

    await alice.connect()
    await bob.connect()
    await charlie.connect()

    bob_from_alice = await bob.client.private_channel("alice-bob", "alice")

    def bob_from_alice_handler(data: Any) -> None:
        if "message" in data:
            print(f"[{bob.name}] DM from {data['from']}: {data['message']}")
        elif "type" in data and data["type"] == "file":
            print(
                f"[{bob.name}] Received file from {data['from']}: {data['filename']} ({data['size']} bytes)"
            )

    bob_from_alice.on("message", bob_from_alice_handler)

    charlie_from_bob = await charlie.client.private_channel("bob-charlie", "bob")

    def charlie_from_bob_handler(data: Any) -> None:
        print(f"[{charlie.name}] DM from {data['from']}: {data['message']}")

    charlie_from_bob.on("message", charlie_from_bob_handler)

    # Give time for handshakes
    await asyncio.sleep(0.05)

    print("--- Direct Messages ---")

    dm_start = time.perf_counter()
    await alice.send_dm("bob", "Hey Bob, are you free for lunch?")
    alice_dm_time = (time.perf_counter() - dm_start) * 1000

    dm_start = time.perf_counter()
    await bob.send_dm("charlie", "Charlie, did you finish the report?")
    bob_dm_time = (time.perf_counter() - dm_start) * 1000

    # Alice and Charlie have no channel set up, so Alice won't receive this
    dm_start = time.perf_counter()
    await charlie.send_dm("alice", "Alice, I need your help!")
    charlie_dm_time = (time.perf_counter() - dm_start) * 1000

    print(
        f"\nDM send times: Alice: {alice_dm_time:.1f}ms, Bob: {bob_dm_time:.1f}ms, Charlie: {charlie_dm_time:.1f}ms"
    )

    await asyncio.sleep(0.2)

    print("\n--- Private File Transfer ---")
    large_file: dict[str, Any] = {
        "from": "alice",
        "type": "file",
        "filename": "confidential.pdf",
        "size": 1024 * 1024 * 3,  # 3MB
        "data": buffer_3mb,
    }

    await alice.send_dm("bob", "Sending you the confidential file...")
    # Large data is chunked automatically when sent over the channel
    channel = alice.channels.get("bob")
    if channel:
        file_start = time.perf_counter()
        await channel.send(large_file)
        file_time = (time.perf_counter() - file_start) * 1000
        throughput = (3 * 1024 * 1024) / ((file_time / 1000) * 1024 * 1024)
        print(f"\nFile transfer completed: 3MB in {file_time:.1f}ms ({throughput:.1f} MB/s)")

    await asyncio.sleep(0.1)

    await alice.disconnect()
    await bob.disconnect()
    await charlie.disconnect()

    print("\nDirect messaging example completed!")


async def main():
    total_start = time.perf_counter()
    try:
        await private_channel_example()
        await direct_messaging_example()

        total_elapsed = (time.perf_counter() - total_start) * 1000
        print(f"\nAll examples completed! Total time: {total_elapsed:.1f}ms")
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
