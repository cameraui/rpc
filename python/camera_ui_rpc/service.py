import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import replace
from functools import partial
from typing import TYPE_CHECKING, Any, TypeVar, cast

from nats.aio.client import NO_RESPONDERS_STATUS
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js.api import Header
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

from .chunking import ChunkingManager, parse_chunk_index
from .codec import decode, decode_message, encode
from .decorators import extract_nested_methods_with_decorators
from .errors import RPCException, reported_error
from .handler import (
    format_error_dict,
    handle_callback_request,
    handle_normal_rpc,
    handle_pull_callback_request,
    handle_pull_iterator_request,
    session_params,
    set_up_session,
    start_stream_request,
)
from .svcm import Svcm
from .types import ErrorCode, RPCClientOptions
from .utils import DIRECT_SUBJECT_METADATA, direct_subject, is_async_function, service_subject

if TYPE_CHECKING:
    from .client import RPCClient

_Teardown = Callable[[], Coroutine[Any, Any, None]]
_Info = TypeVar("_Info")

DISCOVERY_TIMEOUT = 2.0
DISCOVERY_STALL = 0.01  # seconds of quiet after the last answer before discovery ends


def _forget_session(own: dict[str, _Teardown], tracked: dict[str, _Teardown], session_id: str) -> None:
    own.pop(session_id, None)
    tracked.pop(session_id, None)


async def _serve_direct(handler: Callable[[Request], Awaitable[None]], msg: Msg) -> None:
    # the instance subject is a plain subscription, its requests take the endpoint's way
    with contextlib.suppress(Exception):
        await handler(Request(msg))


def _take_sessions(own: dict[str, _Teardown], tracked: dict[str, _Teardown]) -> list[_Teardown]:
    for session_id in own:
        tracked.pop(session_id, None)
    teardowns = list(own.values())
    own.clear()
    return teardowns


class RPCService:
    def __init__(self, client: "RPCClient"):
        self.__client: RPCClient = client
        self.__svc: Svcm | None = None
        self.__services: list[NATSService] = []
        self.__nc: NATSClient | None = None

    def init(self, nc: NATSClient) -> None:
        if self.__nc is nc:
            return

        self.__nc = nc
        self.__svc = Svcm(nc)
        self.__services = []

    async def register_handler(
        self, config: ServiceConfig, handlers: object, isolated_connection: bool = False
    ) -> "Service":
        if not self.__svc:
            raise RuntimeError("RPCService is not initialized. Call init() first.")

        service_client = self.__client
        svc = self.__svc

        if isolated_connection:
            opts: RPCClientOptions = {
                **self.__client.options,
                "servers": self.__client.options["servers"],
                "name": f"{self.__client.options['name']}-service-{config.name}",
            }

            service_client = self.__client.create_isolated_client(opts)

            nc = await service_client.connect()

            svc = Svcm(nc)

        service = await svc.add(
            replace(config, metadata={**(config.metadata or {}), DIRECT_SUBJECT_METADATA: "1"})
        )
        self.__services.append(service)
        instance_id = service.info().id
        direct_subscriptions: list[Any] = []

        if isolated_connection:
            setattr(service, "_isolated_client", service_client)  # noqa: B010

        # the client tracks them too, so its suspend(), disconnect() and a lost connection end them
        pull_iterator_cleanups: dict[str, _Teardown] = {}
        stream_cleanups: dict[str, _Teardown] = {}
        callback_cleanups: dict[str, _Teardown] = {}
        endpoint_chunks: list[ChunkingManager] = []

        probe_settings = service_client._orphan_probe_settings()  # pyright: ignore[reportPrivateUsage]

        methods = extract_nested_methods_with_decorators(handlers)

        groups: dict[str, Group] = {}

        for path, handler in methods.items():
            parts = path.split(".")
            method_name = parts.pop()

            target: NATSService | Group = service

            if parts:
                group_path = ".".join(parts)
                if group_path not in groups:
                    current: NATSService | Group = service
                    for part in parts:
                        current = current.add_group(name=part)
                    groups[group_path] = cast(Group, current)
                target = groups[group_path]

            def make_endpoint_handler(h: Callable[..., Any]) -> Callable[[Request], Awaitable[None]]:
                # once per endpoint instead of per message
                h_is_async = is_async_function(h)
                chunks = service_client._open_endpoint_chunking()  # pyright: ignore[reportPrivateUsage]
                endpoint_chunks.append(chunks)

                async def handle_rpc_message(rpc_msg: dict[str, Any], original_msg: Request) -> None:
                    response = {"id": rpc_msg["id"]}

                    try:
                        params = rpc_msg.get("params", {})
                        if (
                            isinstance(params, dict)
                            and params.get("__stream")
                            and params.get("__streamSubject")
                        ):
                            stream_subject = cast(str, params["__streamSubject"])
                            args = cast(list[Any], params.get("args", []))
                            stream_id = cast(str, rpc_msg["id"])

                            stream_cleanup, stream_task = start_stream_request(
                                h,
                                args,
                                stream_subject,
                                stream_id,
                                service_client,
                                service_client.io_pool,
                                on_finished=partial(
                                    _forget_session,
                                    stream_cleanups,
                                    service_client.stream_cleanups,
                                    stream_id,
                                ),
                                probe=service_client._new_orphan_probe(probe_settings),  # pyright: ignore[reportPrivateUsage]
                            )
                            stream_cleanups[stream_id] = stream_cleanup
                            service_client._track_session(  # pyright: ignore[reportPrivateUsage]
                                service_client.stream_cleanups, stream_id, stream_cleanup
                            )
                            # one stream at a time per endpoint, the endpoint task waits for it
                            await asyncio.wait((stream_task,))
                        elif (
                            # a direct object, or wrapped in an array by call()
                            (isinstance(params, dict) and params.get("__pullIterator"))
                            or (
                                isinstance(params, list)
                                and len(params) > 0  # pyright: ignore[reportUnknownArgumentType]
                                and isinstance(params[0], dict)
                                and params[0].get("__pullIterator")
                            )
                        ):
                            pull_params = cast(
                                dict[str, Any],
                                params
                                if isinstance(params, dict) and params.get("__pullIterator")
                                else params[0],
                            )
                            args = cast(list[Any], pull_params.get("args", []))
                            iterator_id = cast(str, pull_params.get("__iteratorId", rpc_msg["id"]))
                            cleanup = await set_up_session(
                                handle_pull_iterator_request(
                                    h,
                                    args,
                                    iterator_id,
                                    service_client,
                                    service_client.io_pool,
                                    on_finished=partial(
                                        _forget_session,
                                        pull_iterator_cleanups,
                                        service_client.pull_iterator_cleanups,
                                        iterator_id,
                                    ),
                                    probe=service_client._new_orphan_probe(probe_settings),  # pyright: ignore[reportPrivateUsage]
                                ),
                                service_client,
                                f"{original_msg.subject}.reply.{rpc_msg['id']}",
                                response,
                            )

                            pull_iterator_cleanups[iterator_id] = cleanup
                            service_client._track_session(  # pyright: ignore[reportPrivateUsage]
                                service_client.pull_iterator_cleanups, iterator_id, cleanup
                            )
                            response["result"] = {"iteratorId": iterator_id}

                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)
                        elif (pc_params := session_params(params, "__pullCallback")) is not None:
                            pc_iterator_id = cast(str, pc_params.get("__iteratorId", rpc_msg["id"]))
                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            cleanup = await set_up_session(
                                handle_pull_callback_request(
                                    h,
                                    cast(list[Any], pc_params.get("args", [])),
                                    pc_iterator_id,
                                    cast(str, pc_params.get("__callbackSubject", "")),
                                    cast(list[str], pc_params.get("__onewayMethods", [])),
                                    service_client,
                                    service_client.io_pool,
                                    on_finished=partial(
                                        _forget_session,
                                        pull_iterator_cleanups,
                                        service_client.pull_iterator_cleanups,
                                        pc_iterator_id,
                                    ),
                                    probe=service_client._new_orphan_probe(probe_settings),  # pyright: ignore[reportPrivateUsage]
                                ),
                                service_client,
                                reply_subject,
                                response,
                            )
                            pull_iterator_cleanups[pc_iterator_id] = cleanup
                            service_client._track_session(  # pyright: ignore[reportPrivateUsage]
                                service_client.pull_iterator_cleanups, pc_iterator_id, cleanup
                            )
                            response["result"] = {"iteratorId": pc_iterator_id}
                            await service_client.publish(reply_subject, response)
                        elif (cb_params := session_params(params, "__callback")) is not None:
                            callback_id = cast(str, rpc_msg["id"])
                            reply_subject = f"{original_msg.subject}.reply.{callback_id}"
                            cleanup = await set_up_session(
                                handle_callback_request(
                                    h,
                                    cast(list[Any], cb_params.get("args", [])),
                                    cast(str, cb_params.get("__callbackSubject")),
                                    callback_id,
                                    service_client,
                                    service_client.io_pool,
                                    on_finished=partial(
                                        _forget_session,
                                        callback_cleanups,
                                        service_client.callback_cleanups,
                                        callback_id,
                                    ),
                                    probe=service_client._new_orphan_probe(probe_settings),  # pyright: ignore[reportPrivateUsage]
                                ),
                                service_client,
                                reply_subject,
                                response,
                            )
                            callback_cleanups[callback_id] = cleanup
                            service_client._track_session(  # pyright: ignore[reportPrivateUsage]
                                service_client.callback_cleanups, callback_id, cleanup
                            )
                            response["result"] = {"ok": True}
                            await service_client.publish(reply_subject, response)
                        else:
                            result = await handle_normal_rpc(
                                h,
                                rpc_msg.get("params", []),
                                service_client.io_pool,
                                h_is_async,
                            )
                            response["result"] = result

                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)

                    except Exception as e:
                        response["error"] = format_error_dict(e)
                        # a result that failed to go out would fail the error reply too
                        response.pop("result", None)

                        try:
                            reply_subject = f"{original_msg.subject}.reply.{rpc_msg['id']}"
                            await service_client.publish(reply_subject, response)
                        except Exception as publish_error:
                            if service_client.is_closed:
                                return

                            print(f"Failed to send error response: {publish_error}")

                async def handle_assembled_data(assembled_data: Any, original_msg: Request) -> None:
                    await handle_rpc_message(assembled_data, original_msg)

                async def endpoint_handler(msg: Request) -> None:
                    try:
                        chunk_type = msg.headers.get("x-chunked-transfer") if msg.headers else None

                        if chunk_type == "header":
                            data = decode(msg.data)
                            chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None

                            if (
                                not chunk_id
                                or not isinstance(data, dict)
                                or data.get("transferId") != chunk_id
                            ):
                                return

                            def on_complete(assembled_data: Any) -> None:
                                asyncio.create_task(handle_assembled_data(assembled_data, msg))

                            def on_error(error: Exception) -> None:
                                print(f"Error assembling chunks for {method_name}: {error}")

                            chunks.start_receiving(
                                chunk_id,
                                data.get("totalChunks"),
                                on_complete,
                                on_error,
                                data.get("totalSize"),
                                data.get("chunkSize"),
                            )
                        elif chunk_type == "chunk":
                            chunk_id = msg.headers.get("x-chunk-id") if msg.headers else None
                            chunk_index = parse_chunk_index(
                                msg.headers.get("x-chunk-index") if msg.headers else None
                            )

                            if not chunk_id:
                                return

                            chunks.process_chunk(
                                {
                                    "id": chunk_id,
                                    "chunkIndex": chunk_index,
                                    "data": msg.data,
                                    "isLast": False,
                                }
                            )
                        else:
                            # zero-copy binary views into msg.data
                            decoded = decode_message(msg.data)
                            await handle_rpc_message(decoded, msg)
                    except Exception as e:
                        import traceback

                        reported = reported_error(e)
                        text = (
                            reported.message
                            if isinstance(reported, RPCException) and reported is not e
                            else str(e)
                        )
                        error_data: dict[str, Any] = {
                            "message": text,
                            "stack": traceback.format_exc() if hasattr(e, "__traceback__") else None,
                            "code": getattr(reported, "code", ErrorCode.INTERNAL_ERROR.value),
                        }

                        await msg.respond_error(
                            code="408"
                            if getattr(reported, "code", None) == ErrorCode.TIMEOUT.value
                            else "500",
                            description=text,
                            data=encode(error_data),
                        )

                return endpoint_handler

            handler_func = make_endpoint_handler(handler)
            await target.add_endpoint(name=method_name, handler=handler_func)
            await service.add_endpoint(
                name=method_name, handler=handler_func, subject=service_subject(config.name, path)
            )
            direct_subscriptions.append(
                await service_client.nc.subscribe(  # type: ignore[union-attr]
                    direct_subject(instance_id, path), cb=partial(_serve_direct, handler_func)
                )
            )

        setattr(service, "_pull_iterator_cleanups", pull_iterator_cleanups)  # noqa: B010

        original_stop = service.stop

        async def stop_with_cleanup() -> None:
            for sub in direct_subscriptions:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
            direct_subscriptions.clear()
            teardowns = [
                *_take_sessions(stream_cleanups, service_client.stream_cleanups),
                *_take_sessions(pull_iterator_cleanups, service_client.pull_iterator_cleanups),
            ]
            await asyncio.gather(*(teardown() for teardown in teardowns), return_exceptions=True)
            # callbacks last, like the client's own teardown
            callbacks = _take_sessions(callback_cleanups, service_client.callback_cleanups)
            await asyncio.gather(*(teardown() for teardown in callbacks), return_exceptions=True)
            for chunks in endpoint_chunks:
                service_client._close_endpoint_chunking(chunks)  # pyright: ignore[reportPrivateUsage]

            await original_stop()

        setattr(service, "stop", stop_with_cleanup)  # noqa: B010

        return Service(service)

    def monitor(self) -> "ServiceMonitor":
        return ServiceMonitor(self.__client)

    async def stop_all(self) -> None:

        async def stop_service(s: NATSService) -> None:
            try:
                await s.stop()
                isolated_client: RPCClient | None = getattr(s, "_isolated_client", None)
                if isolated_client:
                    await isolated_client.disconnect()
            except Exception:
                pass

        await asyncio.gather(*[stop_service(s) for s in self.__services], return_exceptions=True)
        self.__services = []

    async def stop(self, service_name: str) -> None:
        service = next((s for s in self.__services if s.info().name == service_name), None)
        if service:
            with contextlib.suppress(Exception):
                await service.stop()
                isolated_client = getattr(service, "_isolated_client", None)
                if isolated_client:
                    await isolated_client.disconnect()
                self.__services = [s for s in self.__services if s != service]

    def get_all_info(self) -> list[ServiceInfo]:
        return [s.info() for s in self.__services]

    def get_info(self, service_name: str) -> ServiceInfo | None:
        service = next((s for s in self.__services if s.info().name == service_name), None)
        return service.info() if service else None

    async def get_all_stats(self) -> list[ServiceStats]:
        return [s.stats() for s in self.__services]

    async def get_stats(self, service_name: str) -> ServiceStats | None:
        service = next((s for s in self.__services if s.info().name == service_name), None)
        return service.stats() if service else None


class ServiceMonitor:
    def __init__(self, client: "RPCClient"):  # type: ignore
        self.__client: RPCClient = client

    async def ping(self, service_name: str | None = None) -> list[ServicePing]:
        subject = f"$SRV.PING.{service_name}" if service_name else "$SRV.PING"
        return await self._collect(subject, ServicePing.from_dict)

    async def info(self, service_name: str | None = None) -> list[ServiceInfo]:
        subject = f"$SRV.INFO.{service_name}" if service_name else "$SRV.INFO"
        return await self._collect(subject, ServiceInfo.from_dict)

    async def stats(self, service_name: str | None = None) -> list[ServiceStats]:
        subject = f"$SRV.STATS.{service_name}" if service_name else "$SRV.STATS"
        return await self._collect(subject, ServiceStats.from_dict)

    async def _collect(self, subject: str, parse: Callable[[dict[str, Any]], _Info]) -> list[_Info]:
        nc = self.__client.nc
        if not nc:
            raise RuntimeError("NATS client is not connected")

        responses: list[_Info] = []
        try:
            inbox = nc.new_inbox()
            sub = await nc.subscribe(inbox)
            try:
                await nc.publish(subject, b"", reply=inbox)
                wait = DISCOVERY_TIMEOUT
                while True:
                    try:
                        msg = await sub.next_msg(timeout=wait)
                    except TimeoutError:
                        break
                    if (
                        not msg.data
                        and msg.headers
                        and msg.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS
                    ):
                        break
                    try:
                        responses.append(parse(json.loads(msg.data.decode("utf-8"))))
                    except Exception:
                        continue
                    wait = DISCOVERY_STALL
            finally:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
        except Exception:
            # return what came on any error
            pass

        return responses


class Service:
    def __init__(self, service: NATSService):
        self.__service: NATSService = service

    @property
    def is_stopped(self) -> bool:
        return bool(self.__service.stopped.is_set())

    def info(self) -> ServiceInfo:
        return self.__service.info()

    def stats(self) -> ServiceStats:
        return self.__service.stats()

    async def stop(self) -> None:
        with contextlib.suppress(Exception):
            await self.__service.stop()
