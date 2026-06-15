import time

import uvloop

from camera_ui_rpc import RPCClass, RPCProperty, create_rpc_client


@RPCClass
class ConfigService:
    name = RPCProperty()
    version = RPCProperty()
    max_connections = RPCProperty()

    def __init__(self):
        self.name = "Config Service"
        self.version = "1.0.0"
        self.max_connections = 100


async def main():
    total_start = time.perf_counter()

    print("RPCProperty Decorator Example\n")

    start = time.perf_counter()
    server = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "property-server",
        }
    )

    await server.connect()
    server_connect_time = (time.perf_counter() - start) * 1000
    print(f"Server connected ({server_connect_time:.1f}ms)")

    start = time.perf_counter()
    unsub = await server.register_handler("config", ConfigService())
    register_time = (time.perf_counter() - start) * 1000
    print(f"Service registered ({register_time:.1f}ms)\n")

    start = time.perf_counter()
    client = create_rpc_client(
        {
            "servers": ["nats://localhost:4222"],
            "auth": {"user": "server", "password": "server_password"},
            "name": "property-client",
        }
    )

    await client.connect()
    client_connect_time = (time.perf_counter() - start) * 1000
    print(f"Client connected ({client_connect_time:.1f}ms)\n")

    start = time.perf_counter()
    config = client.create_proxy("config")
    proxy_time = (time.perf_counter() - start) * 1000
    print(f"Proxy created ({proxy_time:.1f}ms)\n")

    print("Testing Property Getters")
    start = time.perf_counter()
    name = await config.name
    name_time = (time.perf_counter() - start) * 1000
    print(f"Name: {name} ({name_time:.1f}ms)")

    start = time.perf_counter()
    version = await config.version
    version_time = (time.perf_counter() - start) * 1000
    print(f"Version: {version} ({version_time:.1f}ms)")

    start = time.perf_counter()
    max_connections = await config.max_connections
    max_conn_time = (time.perf_counter() - start) * 1000
    print(f"Max Connections: {max_connections} ({max_conn_time:.1f}ms)")

    print("\nUpdating Name")
    start = time.perf_counter()
    await config.setName("Updated Config Service")
    set_time = (time.perf_counter() - start) * 1000
    print(f"Name updated ({set_time:.1f}ms)")

    start = time.perf_counter()
    new_name = await config.name
    get_new_time = (time.perf_counter() - start) * 1000
    print(f"New Name: {new_name} ({get_new_time:.1f}ms)")

    print("\nCleaning up...")
    await unsub()
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
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
