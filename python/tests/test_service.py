from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from nats.micro.service import ServiceConfig
from test_client import BAD_CHUNK_CASES, bad_chunks, raw_chunk
from test_mux import make_client

from camera_ui_rpc.chunking import ChunkingManager
from camera_ui_rpc.codec import encode


class FakeService:
    def __init__(self) -> None:
        self.endpoints: dict[str, Any] = {}
        # every endpoint by subject: the bare one and svc.<service>.<path>
        self.subjects: dict[str, Any] = {}

    def info(self) -> Any:
        return SimpleNamespace(id="inst1", name="svc")

    async def add_endpoint(self, name: str, handler: Any, subject: str | None = None) -> None:
        self.endpoints.setdefault(name, handler)
        self.subjects[subject or name] = handler

    def add_group(self, name: str) -> Any:
        return FakeGroup(self, name)

    async def stop(self) -> None:
        pass


class FakeGroup:
    def __init__(self, service: FakeService, prefix: str) -> None:
        self.service = service
        self.prefix = prefix

    async def add_endpoint(self, name: str, handler: Any) -> None:
        self.service.subjects[f"{self.prefix}.{name}"] = handler

    def add_group(self, name: str) -> Any:
        return FakeGroup(self.service, f"{self.prefix}.{name}")


class FakeSvcm:
    def __init__(self) -> None:
        self.service = FakeService()

    async def add(self, config: ServiceConfig) -> FakeService:
        return self.service


def request(data: bytes, headers: dict[str, str], errors: list[str]) -> Any:
    async def respond_error(code: str, description: str, data: bytes = b"") -> None:
        errors.append(description)

    return SimpleNamespace(subject="svc.echo", data=data, headers=headers, respond_error=respond_error)


def header_msg(transfer_id: str, total_chunks: int, total_size: int, chunk_size: int) -> bytes:
    return encode(
        {
            "type": "chunked",
            "transferId": transfer_id,
            "totalChunks": total_chunks,
            "totalSize": total_size,
            "chunkSize": chunk_size,
        }
    )


def capture_managers(client: Any) -> list[ChunkingManager]:
    created: list[ChunkingManager] = []
    factory = client._new_chunking_manager

    def capture(*args: Any) -> ChunkingManager:
        created.append(factory(*args))
        return created[-1]

    client._new_chunking_manager = capture
    return created


def test_service_endpoint_rejects_bad_chunk_headers_and_indexes() -> None:
    async def run() -> None:
        client, _ = make_client(max_transfer_size=1000)
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        managers = capture_managers(client)

        async def echo(value: Any) -> Any:
            return value

        await client.service.register_handler(ServiceConfig(name="svc", version="1.0.0"), {"echo": echo})
        endpoint = svcm.service.endpoints["echo"]
        [manager] = managers
        errors: list[str] = []

        await endpoint(
            request(
                header_msg("t1", 2, 2000, 1000), {"x-chunked-transfer": "header", "x-chunk-id": "t1"}, errors
            )
        )
        assert manager.assemblers == {}

        await endpoint(
            request(header_msg("t2", 2, 20, 10), {"x-chunked-transfer": "header", "x-chunk-id": "t2"}, errors)
        )
        assert list(manager.assemblers) == ["t2"]

        chunk_headers = {"x-chunked-transfer": "chunk", "x-chunk-id": "t2", "x-chunk-index": "abc"}
        await endpoint(request(b"\x00" * 10, chunk_headers, errors))

        assert manager.assemblers == {}
        assert errors == []

    asyncio.run(run())


@pytest.mark.parametrize("case", BAD_CHUNK_CASES)
def test_service_endpoint_drops_the_transfer_on_a_bad_chunk(case: str) -> None:
    async def run() -> None:
        client, _ = make_client()
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        managers = capture_managers(client)
        calls: list[Any] = []

        async def echo(value: Any) -> Any:
            calls.append(value)
            return value

        await client.service.register_handler(ServiceConfig(name="svc", version="1.0.0"), {"echo": echo})
        endpoint = svcm.service.endpoints["echo"]
        [manager] = managers
        errors: list[str] = []
        payload = encode({"id": "c1", "method": "echo", "params": ["y" * 30]})
        total = -(-len(payload) // 10)
        header = {"x-chunked-transfer": "header", "x-chunk-id": "t1"}
        await endpoint(request(header_msg("t1", total, len(payload), 10), header, errors))

        for index, data in bad_chunks(case, payload, 10):
            await endpoint(request(data, raw_chunk("t1", index), errors))

        assert manager.assemblers == {}
        assert manager.budget.used == 0
        for i in range(total):
            await endpoint(request(payload[i * 10 : (i + 1) * 10], raw_chunk("t1", str(i)), errors))
        await asyncio.sleep(0.01)
        assert calls == []

    asyncio.run(run())


@pytest.mark.parametrize("teardown", ["stop", "disconnect", "suspend"])
def test_service_endpoints_reassemble_apart_on_the_client_budget(teardown: str) -> None:
    async def run() -> None:
        client, _ = make_client(max_transfer_size=1000)
        svcm = FakeSvcm()
        setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
        managers = capture_managers(client)

        async def echo(value: Any) -> Any:
            return value

        service = await client.service.register_handler(
            ServiceConfig(name="svc", version="1.0.0"), {"echo": echo, "ping": echo}
        )
        echo_manager, ping_manager = managers
        assert echo_manager is not client.chunking_manager
        assert echo_manager.budget is ping_manager.budget is client.chunking_manager.budget
        errors: list[str] = []
        header = {"x-chunked-transfer": "header", "x-chunk-id": "t1"}
        await svcm.service.endpoints["echo"](request(header_msg("t1", 2, 20, 10), header, errors))
        await svcm.service.endpoints["ping"](request(header_msg("t1", 2, 20, 10), header, errors))
        chunk = {"x-chunked-transfer": "chunk", "x-chunk-id": "t1", "x-chunk-index": "0"}
        await svcm.service.endpoints["echo"](request(b"\x00" * 10, chunk, errors))

        assert list(echo_manager.assemblers) == ["t1"]
        assert list(ping_manager.assemblers) == ["t1"]
        assert echo_manager.budget.used == 20

        if teardown == "stop":
            await service.stop()
        else:
            await getattr(client, teardown)()

        assert echo_manager.assemblers == {} and ping_manager.assemblers == {}
        assert echo_manager.budget.used == 0
        assert echo_manager._timer is None  # pyright: ignore[reportPrivateUsage]
        assert client._endpoint_chunking == set()  # pyright: ignore[reportPrivateUsage]
        assert errors == []

    asyncio.run(run())
