from __future__ import annotations

import asyncio
import copy
import gc
from types import SimpleNamespace
from typing import Any

import pytest

import camera_ui_rpc.utils as rpc_utils
from camera_ui_rpc.utils import create_proxy, create_service_proxy, rpc_callbacks


class FakeClient:
    def __init__(self, result: Any = None, methods: list[str] | None = None) -> None:
        self.result = result
        self.methods = methods
        self.calls: list[tuple[str, str, tuple[Any, ...], dict[str, Any]]] = []

    async def call(self, subject: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("call", subject, args, kwargs))
        result = self.result() if callable(self.result) else self.result
        return result

    async def _call_with_meta(self, subject: str, args: tuple[Any, ...], **kwargs: Any) -> Any:
        self.calls.append(("call", subject, args, kwargs))
        result = self.result() if callable(self.result) else self.result
        return result, self.methods

    def call_stream(self, subject: str, *args: Any, **kwargs: Any) -> str:
        self.calls.append(("stream", subject, args, kwargs))
        return "stream"

    def _call_stream_with_probe(self, subject: str, args: tuple[Any, ...], **kwargs: Any) -> str:
        self.calls.append(("stream", subject, args, kwargs))
        return "stream"

    def call_pull_iterator(self, subject: str, *args: Any) -> str:
        self.calls.append(("pull", subject, args, {}))
        return "pull"

    def call_with_callback(self, subject: str, args: list[Any], callback: Any) -> str:
        self.calls.append(("callback", subject, tuple(args), {"callback": callback}))
        return "callback"

    def call_pull_iterator_with_callback(self, subject: str, methods: Any, oneway: Any, *args: Any) -> str:
        self.calls.append(("pull-callback", subject, args, {"methods": methods, "oneway": oneway}))
        return "pull-callback"


def test_plain_call_requests_discovery_until_the_cache_fills() -> None:
    async def run() -> None:
        client = FakeClient(lambda: {"value": 1}, ["echo", "other"])
        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]

        assert await proxy.echo(1, 2) == {"value": 1}
        assert await proxy.echo(3) == {"value": 1}

        assert [c[1] for c in client.calls] == ["rpc.ns.echo", "rpc.ns.echo"]
        assert client.calls[0][2] == (1, 2)
        assert client.calls[0][3]["discover"] is True
        assert client.calls[1][3]["discover"] is False
        assert proxy.missing is None
        assert proxy.other is not None

    asyncio.run(run())


def test_routing_follows_arguments_then_name() -> None:
    client = FakeClient()
    proxy = create_proxy(client, "ns")  # type: ignore[arg-type]

    def on_item(_: Any) -> None:
        pass

    cbs = rpc_callbacks(on_item=on_item)
    assert proxy.pullFrames(1, on_item, cbs) == "pull-callback"
    assert proxy.subscribe(on_item, 5) == "callback"
    assert proxy.generateItems(3) == "stream"
    assert proxy.pullItems(4) == "pull"
    assert proxy.generatePull(1) == "stream"

    assert client.calls[0][:3] == ("pull-callback", "rpc.ns.pullFrames", (1, on_item))
    assert client.calls[1][:3] == ("callback", "rpc.ns.subscribe", (5,))
    assert client.calls[2][:3] == ("stream", "rpc.ns.generateItems", (3,))
    assert client.calls[3][:3] == ("pull", "rpc.ns.pullItems", (4,))


def test_await_without_call_invokes_the_method() -> None:
    async def run() -> None:
        client = FakeClient(7)
        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]
        assert await proxy.value == 7
        assert client.calls[0][1:3] == ("rpc.ns.value", ())

    asyncio.run(run())


def test_attribute_access_returns_the_same_method_object() -> None:
    proxy = create_proxy(FakeClient(), "ns")  # type: ignore[arg-type]
    assert proxy.echo is proxy.echo


def test_nested_path_and_repr() -> None:
    async def run() -> None:
        client = FakeClient(None)
        proxy = create_proxy(client, "ns", ["sub"])  # type: ignore[arg-type]
        await proxy.method()
        assert client.calls[0][1] == "rpc.ns.sub.method"
        assert repr(proxy) == "<RPCProxy ns.sub>"
        assert repr(create_proxy(client, "ns")) == "<RPCProxy ns>"  # type: ignore[arg-type]

    asyncio.run(run())


def test_isolated_client_attributes_stay_settable() -> None:
    proxy = create_proxy(FakeClient(), "ns")  # type: ignore[arg-type]
    marker = object()
    setattr(proxy, "_isolated_client", marker)  # noqa: B010
    assert getattr(proxy, "_isolated_client", None) is marker


def test_copy_of_a_proxy_does_not_recurse() -> None:
    proxy = create_proxy(FakeClient(), "ns")  # type: ignore[arg-type]
    assert repr(copy.copy(proxy)) == "<RPCProxy ns>"


def test_proxy_calls_leave_no_cyclic_garbage() -> None:
    async def run() -> int:
        client = FakeClient(1)
        cached = create_proxy(client, "ns")  # type: ignore[arg-type]
        gc.collect()
        gc.disable()
        try:
            for i in range(200):
                await cached.echo(i)
                await create_proxy(client, "ns").echo(i)  # type: ignore[arg-type]
            return gc.collect()
        finally:
            gc.enable()

    assert asyncio.run(run()) < 50


def service_info(*subjects: str, metadata: dict[str, str] | None = None) -> Any:
    endpoints = [SimpleNamespace(subject=s, name=s) for s in subjects]
    return SimpleNamespace(name="svc", id="inst1", metadata=metadata or {}, endpoints=endpoints)


def test_service_proxy_resolves_endpoints_and_groups() -> None:
    async def run() -> None:
        client = FakeClient("ok")
        proxy = create_service_proxy(client, service_info("ping", "cams.list", "cams.generateFeed"))  # type: ignore[arg-type]

        assert await proxy.ping() == "ok"
        assert await proxy.cams.list(1) == "ok"
        assert proxy.cams.generateFeed() == "stream"
        assert proxy.cams is proxy.cams
        assert repr(proxy.cams) == "<ServiceProxy svc.cams>"
        assert [c[1] for c in client.calls] == ["ping", "cams.list", "cams.generateFeed"]
        with pytest.raises(AttributeError, match="'ServiceProxy' object has no attribute 'nope'"):
            _ = proxy.nope
        assert not hasattr(proxy.cams, "missing")

    asyncio.run(run())


@pytest.mark.parametrize("result", [None, 7, b"raw", [1, 2], "text", {"value": 1}])
def test_discovery_finishes_whatever_the_result_type(result: Any) -> None:
    async def run() -> None:
        client = FakeClient(result, ["echo"])
        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]

        for _ in range(3):
            assert await proxy.echo() == result

        assert [c[3]["discover"] for c in client.calls] == [True, False, False]
        assert proxy.missing is None

    asyncio.run(run())


def test_the_method_roots_are_derived_only_when_a_lookup_needs_them(monkeypatch: pytest.MonkeyPatch) -> None:
    derived: list[Any] = []
    method_roots = rpc_utils.method_roots
    monkeypatch.setattr(
        rpc_utils, "method_roots", lambda methods: derived.append(methods) or method_roots(methods)
    )

    async def run() -> None:
        client = FakeClient(1, ["echo", "db.find"])
        assert await create_proxy(client, "ns").echo() == 1  # type: ignore[arg-type]
        assert derived == []

        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]
        await proxy.echo()
        assert proxy.db is not None
        assert proxy.missing is None
        assert derived == [["echo", "db.find"]]

    asyncio.run(run())


def test_a_result_keeps_its_own_methods_key() -> None:
    async def run() -> None:
        client = FakeClient({"__methods": ["mine"]}, ["echo"])
        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]
        assert await proxy.echo() == {"__methods": ["mine"]}

    asyncio.run(run())


def test_nested_methods_stay_reachable_once_the_cache_is_filled() -> None:
    async def run() -> None:
        client = FakeClient("ok", ["db.find", "db.tables.list", "ping"])
        proxy = create_proxy(client, "ns")  # type: ignore[arg-type]

        assert await proxy.db.find(1) == "ok"
        assert await proxy.ping() == "ok"
        assert await proxy.db.find(2) == "ok"
        assert await proxy.db.tables.list() == "ok"
        assert await proxy.db.count == "ok"

        assert [c[1] for c in client.calls] == [
            "rpc.ns.db.find",
            "rpc.ns.ping",
            "rpc.ns.db.find",
            "rpc.ns.db.tables.list",
            "rpc.ns.db.count",
        ]
        # one cache for the whole tree: only the first call asked for the method list
        assert [c[3]["discover"] for c in client.calls] == [True, False, False, False, False]
        assert proxy.missing is None
        assert proxy.db.find is proxy.db.find

    asyncio.run(run())


def test_nested_access_follows_stream_and_pull_names() -> None:
    client = FakeClient()
    proxy = create_proxy(client, "ns")  # type: ignore[arg-type]
    assert proxy.cams.generateFeed(1) == "stream"
    assert proxy.cams.pullFrames(2) == "pull"
    assert client.calls[0][:3] == ("stream", "rpc.ns.cams.generateFeed", (1,))
    assert client.calls[1][:3] == ("pull", "rpc.ns.cams.pullFrames", (2,))


def test_probes_on_a_method_do_not_create_remote_methods() -> None:
    proxy = create_proxy(FakeClient(), "ns")  # type: ignore[arg-type]
    method = proxy.echo
    assert not hasattr(method, "__wrapped__")
    assert not hasattr(method, "_private")
    with pytest.raises(AttributeError):
        _ = method.__deepcopy__
