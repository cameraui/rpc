import asyncio
import sys

from camera_ui_rpc.client import RPCClient

VALID = ["nats://localhost:4222"]


def assert_eq(got: object, want: object, msg: str) -> None:
    if got != want:
        raise AssertionError(f"{msg}: got {got!r}, want {want!r}")


async def main() -> None:
    server = RPCClient(
        {
            "servers": VALID,
            "auth": {"user": "server", "password": "server_password"},
            "name": "reconfigure-test-server",
        }
    )
    client = RPCClient(
        {
            "servers": VALID,
            "auth": {"user": "server", "password": "server_password"},
            "name": "reconfigure-test-client",
        }
    )

    await server.connect()
    await client.connect()
    print("Initial connect ok")

    received: list[int] = []

    async def handler(data: dict[str, int]) -> None:
        received.append(data["n"])

    await client.subscribe("reconfigure.test", handler)

    await server.publish("reconfigure.test", {"n": 1})
    await asyncio.sleep(0.1)
    assert_eq(received, [1], "pre-suspend publish")
    print("Pre-suspend publish delivered")

    threw = False
    try:
        client.reconfigure(servers=VALID)
    except RuntimeError:
        threw = True
    assert threw, "reconfigure while connected must raise"
    print("reconfigure while connected raises")

    await client.suspend()
    client.reconfigure(servers=VALID)
    await client.connect()

    received.clear()
    await server.publish("reconfigure.test", {"n": 5})
    await asyncio.sleep(0.1)
    assert_eq(received, [5], "post-reconfigure publish")
    print("Subscriptions auto-restored after suspend, reconfigure, connect")

    await client.suspend()
    client.reconfigure(auth={"user": "server", "password": "server_password"})
    await client.connect()
    received.clear()
    await server.publish("reconfigure.test", {"n": 9})
    await asyncio.sleep(0.1)
    assert_eq(received, [9], "post-auth-replacement publish")
    print("reconfigure with auth replacement keeps subscriptions live")

    await client.suspend()
    client.reconfigure(servers=["nats://other:4222"])
    assert_eq(
        client.options["servers"][0],
        "nats://other:4222",
        "servers mutated",
    )
    print("options['servers'] reflects reconfigure")

    await client.disconnect()
    await server.disconnect()
    print("\nALL RECONFIGURE TESTS PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"TEST FAILED: {e}")
        sys.exit(1)
