"""RPC Service wrapper with automatic encoding/decoding and streaming support."""

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, cast

from nats.aio.client import Client as NATSClient
from nats.micro.request import Request
from nats.micro.service import (
    Group,
    ServiceConfig,
    ServiceInfo,
    ServicePing,
    ServiceStats,
)
from nats.micro.service import (
    Service as NATSService,
)

from .codec import decode, encode
from .decorators import extract_nested_methods_with_decorators
from .handler import (
    format_error_dict,
    handle_normal_rpc,
    handle_pull_iterator_request,
    handle_stream_request,
)
from .svcm import Svcm
from .types import ErrorCode, RPCClientOptions

if TYPE_CHECKING:
    from .client import RPCClient


class RPCService:
    """RPC Service wrapper with automatic encoding/decoding and streaming support."""

    def __init__(self, client: "RPCClient"):
        self.__client: RPCClient = client
        self.__svc: Svcm | None = None
        self.__services: list[NATSService] = []
        self.__nc: NATSClient | None = None

    def init(self, nc: NATSClient) -> None:
        """Initialize the service manager.

        Re-initializes when the underlying connection changed (suspend() +
        connect() creates a fresh NATS connection) — a Svcm bound to the old,
        closed connection would break service registration after every
        reconnect cycle.
        """
        if self.__nc is nc:
            return

        self.__nc = nc
        self.__svc = Svcm(nc)
        self.__services = []

    async def register_handler(
        self, config: ServiceConfig, handlers: object, isolated_connection: bool = False
    ) -> "Service":
        """Add a service with automatic RPC endpoint wrapping."""
        if not self.__svc:
            raise RuntimeError("RPCService is not initialized. Call init() first.")

        # Use isolated connection if requested
        service_client = self.__client
        svc = self.__svc

        if isolated_connection:
            opts: RPCClientOptions = {
                **self.__client.options,
                "servers": self.__client.options["servers"],
                "name": f"{self.__client.options['name']}-service-{config.name}",
            }

            # Create isolated connection for this service
            service_client = self.__client.create_isolated_client(opts)

            # Initialize isolated client
            nc = await service_client.connect()

            # Create service manager for isolated connection
            svc = Svcm(nc)

        service = await svc.add(config)
        self.__services.append(service)

        # Store reference to isolated connection if used
        if isolated_connection:
            setattr(service, "_isolated_client", service_client)  # noqa: B010

        # Track pull iterator cleanups for this service
        pull_iterator_cleanups: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}

        # Extract all methods including nested ones
        methods = extract_nested_methods_with_decorators(handlers)

        # Create groups for nested paths
        groups: dict[str, Group] = {}

        for path, handler in methods.items():
            parts = path.split(".")
            method_name = parts.pop()

            target: NATSService | Group = service

            # Create nested groups if needed
            if parts:
                group_path = ".".join(parts)
                if group_path not in groups:
                    current: NATSService | Group = service
                    for part in parts:
                        current = current.add_group(name=part)
                    groups[group_path] = cast(Group, current)
                target = groups[group_path]

            # Add endpoint with auto-encoding/decoding
            def make_endpoint_handler(h: Callable[..., Any]) -> Callable[[Request], Awaitable[None]]:
                async def handle_rpc_message(rpc_msg: dict[str, Any], original_msg: Request) -> None:
                    """Handle RPC protocol message."""
                    response = {"id": rpc_msg["id"]}

                    try:
                        # Handle stream request
                        params = rpc_msg.get("params", {})
                        if (
                            isinstance(params, dict)
                            and params.get("__stream")
                            and params.get("__streamSubject")
                        ):
                            stream_subject = cast(str, params["__streamSubject"])
                            args = cast(list[Any], params.get("args", []))

                            await handle_stream_request(
                                h,
                                args,
                                stream_subject,
                                rpc_msg["id"],
                                service_client,
                                service_client.io_pool,
                            )
                        elif (
                            # Check if it's a pull iterator request
                            # Could be direct object or wrapped in array from call()
                            (isinstance(params, dict) and params.get("__pullIterator"))
                            or (
                                isinstance(params, list)
                                and len(params) > 0  # pyright: ignore[reportUnknownArgumentType]
                                and isinstance(params[0], dict)
                                and params[0].get("__pullIterator")
                            )
                        ):
                            # Extract pull iterator params
                            pull_params = cast(
                                dict[str, Any],
                                params
                                if isinstance(params, dict) and params.get("__pullIterator")
                                else params[0],
                            )
                            args = cast(list[Any], pull_params.get("args", []))
                            iterator_id = cast(str, pull_params.get("__iteratorId", rpc_msg["id"]))
                            cleanup = await handle_pull_iterator_request(
                                h,
                                args,
                                iterator_id,
                                service_client,
                                service_client.io_pool,
                                on_finished=lambda iid=iterator_id: pull_iterator_cleanups.pop(iid, None),
                            )

                            # Store cleanup function for later
                            pull_iterator_cleanups[iterator_id] = cleanup
                            response["result"] = {"iteratorId": iterator_id}

                            # Send response with iterator ID
                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)
                        else:
                            # Normal RPC call
                            result = await handle_normal_rpc(
                                h,
                                rpc_msg.get("params", []),
                                service_client.io_pool,
                            )
                            response["result"] = result

                            # Send response
                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)

                    except Exception as e:
                        response["error"] = format_error_dict(e)

                        try:
                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)
                        except Exception as publish_error:
                            if service_client.is_closed:
                                return  # Ignore publish errors if client is closed

                            print(f"Failed to send error response: {publish_error}")

                async def handle_assembled_data(assembled_data: Any, original_msg: Request) -> None:
                    """Handle assembled data from chunks."""
                    await handle_rpc_message(assembled_data, original_msg)

                async def endpoint_handler(msg: Request) -> None:
                    try:
                        # Check for chunked transfer
                        chunk_type = msg.headers.get("x-chunked-transfer") if msg.headers else None

                        if chunk_type == "header":
                            # Chunked transfer header
                            data = decode(msg.data)
                            chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None

                            if not chunk_id or data.get("transferId") != chunk_id:
                                return

                            def on_complete(assembled_data: Any) -> None:
                                asyncio.create_task(handle_assembled_data(assembled_data, msg))

                            def on_error(error: Exception) -> None:
                                print(f"Error assembling chunks for {method_name}: {error}")

                            # Setup chunk assembly with pre-allocated buffer
                            service_client.chunking_manager.start_receiving(
                                data["transferId"],
                                data["totalChunks"],
                                on_complete,
                                on_error,
                                data.get("totalSize"),  # Pass totalSize for pre-allocation
                                data.get("chunkSize"),  # Pass chunkSize for correct offset calculation
                            )
                        elif chunk_type == "chunk":
                            # Chunk data
                            chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None
                            chunk_index = int(msg.headers.get("x-chunk-index", "0") if msg.headers else "0")

                            if not chunk_id:
                                return

                            # Process raw chunk data
                            service_client.chunking_manager.process_chunk(
                                {
                                    "id": chunk_id,
                                    "chunkIndex": chunk_index,
                                    "data": msg.data,
                                    "isLast": False,
                                }
                            )
                        else:
                            # Regular message - decode as RPC message
                            decoded = decode(msg.data)
                            await handle_rpc_message(decoded, msg)
                    except Exception as e:
                        # Send error response
                        import traceback

                        error_data: dict[str, Any] = {
                            "message": str(e),
                            "stack": traceback.format_exc() if hasattr(e, "__traceback__") else None,
                            "code": getattr(e, "code", ErrorCode.INTERNAL_ERROR.value),
                        }

                        await msg.respond_error(
                            code="408" if getattr(e, "code", None) == ErrorCode.TIMEOUT.value else "500",
                            description=str(e),
                            data=encode(error_data),
                        )

                return endpoint_handler

            handler_func = make_endpoint_handler(handler)
            await target.add_endpoint(name=method_name, handler=handler_func)

        # Extend service with cleanup management
        setattr(service, "_pull_iterator_cleanups", pull_iterator_cleanups)  # noqa: B010

        # Override stop method to clean up pull iterators
        original_stop = service.stop

        async def stop_with_cleanup() -> None:
            # Clean up all pull iterators
            await asyncio.gather(
                *[
                    cleanup()
                    for cleanup in pull_iterator_cleanups.values()
                    if asyncio.iscoroutinefunction(cleanup)
                ],
                return_exceptions=True,
            )
            pull_iterator_cleanups.clear()

            # Call original stop
            await original_stop()

        setattr(service, "stop", stop_with_cleanup)  # noqa: B010

        return Service(service)

    def monitor(self) -> "ServiceMonitor":
        """Get service monitor for discovery."""
        return ServiceMonitor(self.__client)

    async def stop_all(self) -> None:
        """Stop all services and cleanup isolated connections."""

        async def stop_service(s: NATSService) -> None:
            try:
                await s.stop()
                # Disconnect isolated connection if present
                isolated_client: RPCClient | None = getattr(s, "_isolated_client", None)
                if isolated_client:
                    await isolated_client.disconnect()
            except Exception:
                pass

        await asyncio.gather(*[stop_service(s) for s in self.__services], return_exceptions=True)
        self.__services = []

    async def stop(self, service_name: str) -> None:
        """Stop a specific service by name."""
        service = next((s for s in self.__services if s.info().name == service_name), None)
        if service:
            with contextlib.suppress(Exception):
                await service.stop()
                # Disconnect isolated connection if present
                isolated_client = getattr(service, "_isolated_client", None)
                if isolated_client:
                    await isolated_client.disconnect()
                self.__services = [s for s in self.__services if s != service]

    def get_all_info(self) -> list[ServiceInfo]:
        """Get all services info."""
        return [s.info() for s in self.__services]

    def get_info(self, service_name: str) -> ServiceInfo | None:
        """Get info for a specific service."""
        service = next((s for s in self.__services if s.info().name == service_name), None)
        return service.info() if service else None

    async def get_all_stats(self) -> list[ServiceStats]:
        """Get all services stats."""
        return [s.stats() for s in self.__services]

    async def get_stats(self, service_name: str) -> ServiceStats | None:
        """Get stats for a specific service."""
        service = next((s for s in self.__services if s.info().name == service_name), None)
        return service.stats() if service else None


class ServiceMonitor:
    """Service discovery monitor using NATS micro services."""

    def __init__(self, client: "RPCClient"):  # type: ignore
        """
        Initialize service monitor.

        Args:
            client: RPC client instance
        """
        self.__client: RPCClient = client

    async def ping(self, service_name: str | None = None) -> list[ServicePing]:
        """
        Ping services to check availability.

        Args:
            service_name: Optional specific service name

        Returns:
            list of ping responses
        """
        if not self.__client.nc:
            raise RuntimeError("NATS client is not connected")

        # Construct the ping subject
        subject = f"$SRV.PING.{service_name}" if service_name else "$SRV.PING"

        # Collect all responses from services
        responses: list[ServicePing] = []

        try:
            # Create inbox for responses
            inbox = self.__client.nc.new_inbox()
            sub = await self.__client.nc.subscribe(inbox)

            # Send request
            await self.__client.nc.publish(subject, b"", reply=inbox)

            while True:
                try:
                    msg = await asyncio.wait_for(sub.next_msg(), timeout=0.01)
                    json_data = json.loads(msg.data.decode("utf-8"))
                    responses.append(ServicePing.from_dict(json_data))
                except TimeoutError:
                    # No more messages in this interval
                    break
                except Exception:
                    # Skip malformed responses
                    break

            with contextlib.suppress(Exception):
                await sub.unsubscribe()

        except Exception:
            # Return empty list on any error
            pass

        return responses

    async def info(self, service_name: str | None = None) -> list[ServiceInfo]:
        """
        Get service information.

        Args:
            service_name: Optional specific service name

        Returns:
            list of service information
        """
        if not self.__client.nc:
            raise RuntimeError("NATS client is not connected")

        # Construct the info subject
        subject = f"$SRV.INFO.{service_name}" if service_name else "$SRV.INFO"

        # Collect all responses from services
        responses: list[ServiceInfo] = []

        try:
            # Create inbox for responses
            inbox = self.__client.nc.new_inbox()
            sub = await self.__client.nc.subscribe(inbox)

            # Send request
            await self.__client.nc.publish(subject, b"", reply=inbox)

            while True:
                try:
                    msg = await asyncio.wait_for(sub.next_msg(), timeout=0.01)
                    json_data = json.loads(msg.data.decode("utf-8"))
                    responses.append(ServiceInfo.from_dict(json_data))
                except TimeoutError:
                    # No more messages in this interval
                    break
                except Exception:
                    # Skip malformed responses
                    break

            with contextlib.suppress(Exception):
                await sub.unsubscribe()

        except Exception:
            # Return empty list on any error
            pass

        return responses

    async def stats(self, service_name: str | None = None) -> list[ServiceStats]:
        """
        Get service statistics.

        Args:
            service_name: Optional specific service name

        Returns:
            list of service statistics
        """
        if not self.__client.nc:
            raise RuntimeError("NATS client is not connected")

        # Construct the stats subject
        subject = f"$SRV.STATS.{service_name}" if service_name else "$SRV.STATS"

        # Collect all responses from services
        responses: list[ServiceStats] = []

        try:
            # Create inbox for responses
            inbox = self.__client.nc.new_inbox()
            sub = await self.__client.nc.subscribe(inbox)

            # Send request
            await self.__client.nc.publish(subject, b"", reply=inbox)

            while True:
                try:
                    msg = await asyncio.wait_for(sub.next_msg(), timeout=0.01)
                    json_data = json.loads(msg.data.decode("utf-8"))
                    responses.append(ServiceStats.from_dict(json_data))
                except TimeoutError:
                    # No more messages in this interval
                    break
                except Exception:
                    # Skip malformed responses
                    break

            with contextlib.suppress(Exception):
                await sub.unsubscribe()

        except Exception:
            # Return empty list on any error
            pass

        return responses


class Service:
    def __init__(self, service: NATSService):
        self.__service: NATSService = service

    @property
    def is_stopped(self) -> bool:
        return bool(self.__service.stopped.is_set())

    def info(self) -> ServiceInfo:
        """Get service info."""
        return self.__service.info()

    def stats(self) -> ServiceStats:
        """Get service stats."""
        return self.__service.stats()

    async def stop(self) -> None:
        with contextlib.suppress(Exception):
            await self.__service.stop()
