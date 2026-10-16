from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest
from nats.aio.client import NO_RESPONDERS_STATUS
from nats.js.api import Header
from nats.micro.service import ServiceConfig
from test_drain import make_client as make_drain_client
from test_handler import wait_for
from test_mux import MockNC, make_client
from test_proxy import FakeClient, service_info
from test_service import FakeSvcm

from camera_ui_rpc.codec import decode, encode
from camera_ui_rpc.service import ServiceMonitor
from camera_ui_rpc.utils import create_service_proxy


class RecordingSvcm(FakeSvcm):
    async def add(self, config: ServiceConfig) -> Any:
        self.config = config
        return self.service


async def serve(handlers: Any) -> tuple[Any, MockNC, RecordingSvcm, Any]:
    client, nc = make_client()
    svcm = RecordingSvcm()
    setattr(client.service, "_RPCService__svc", svcm)  # noqa: B010
    service = await client.service.register_handler(ServiceConfig(name="alpha", version="1.0.0"), handlers)
    return client, nc, svcm, service


def endpoint_request(subject: str, body: dict[str, Any]) -> Any:
    async def respond_error(code: str, description: str, data: bytes = b"") -> None:
        raise AssertionError(description)

    return SimpleNamespace(subject=subject, data=encode(body), headers=None, respond_error=respond_error)


def published(nc: MockNC, subject: str) -> list[Any]:
    return [decode(p["data"]) for p in nc.published if p["subject"] == subject]


def test_every_method_also_has_a_subject_of_its_service_and_of_its_instance() -> None:
    async def run() -> None:
        client, nc, svcm, _ = await serve({"whoami": lambda: "alpha", "db": {"find": lambda: 1}})

        assert svcm.config.metadata == {"cui.chunk.direct": "1"}
        assert set(svcm.service.subjects) >= {"whoami", "svc.alpha.whoami", "svc.alpha.db.find"}
        direct = sorted(s.pattern for s in nc.subs if s.pattern.startswith("$CUI.SVC."))
        assert direct == ["$CUI.SVC.inst1.db.find", "$CUI.SVC.inst1.whoami"]

        # a request on the instance subject is answered there
        sub = next(s for s in nc.subs if s.pattern == "$CUI.SVC.inst1.whoami")
        body = encode({"id": "r1", "method": "call", "params": []})
        await sub.cb(SimpleNamespace(subject=sub.pattern, data=body, headers=None, reply="_INBOX.x"))
        await wait_for(lambda: published(nc, "$CUI.SVC.inst1.whoami.reply.r1"))
        assert published(nc, "$CUI.SVC.inst1.whoami.reply.r1") == [{"id": "r1", "result": "alpha"}]

    asyncio.run(run())


def test_service_endpoints_serve_callbacks_and_pull_callbacks() -> None:
    async def run() -> None:
        stopped: list[str] = []

        async def events(emit: Any) -> Any:
            await emit("first")
            return lambda: stopped.append("events")

        async def playback(invoker: Any) -> AsyncGenerator[None, None]:
            try:
                await invoker.invoke("frame", 1)
                yield None
            finally:
                stopped.append("playback")

        client, nc, svcm, service = await serve({"events": events, "playback": playback})
        handle = svcm.service.subjects["svc.alpha.events"]
        callback = {"__callback": True, "__callbackSubject": "rpc.cb.c1", "args": []}
        await handle(
            endpoint_request("svc.alpha.events", {"id": "c1", "method": "call", "params": [callback]})
        )
        assert published(nc, "svc.alpha.events.reply.c1") == [{"id": "c1", "result": {"ok": True}}]
        assert published(nc, "rpc.cb.c1") == [{"id": "c1", "type": "data", "data": "first"}]

        handle = svcm.service.subjects["svc.alpha.playback"]
        params = {
            "__pullCallback": True,
            "__iteratorId": "it1",
            "__callbackSubject": "_rpc.cb.it1",
            "__callbackMethods": ["frame"],
            "__onewayMethods": ["frame"],
            "args": [],
        }
        await handle(
            endpoint_request("svc.alpha.playback", {"id": "p1", "method": "call", "params": [params]})
        )
        assert published(nc, "svc.alpha.playback.reply.p1") == [{"id": "p1", "result": {"iteratorId": "it1"}}]
        await wait_for(lambda: published(nc, "_rpc.cb.it1"))
        assert published(nc, "_rpc.cb.it1") == [{"method": "frame", "args": [1]}]
        assert list(client.callback_cleanups) == ["c1"]
        assert list(client.pull_iterator_cleanups) == ["it1"]

        direct = [s for s in nc.subs if s.pattern.startswith("$CUI.SVC.inst1.")]
        assert len(direct) == 2 and not any(s._closed for s in direct)

        await service.stop()
        assert sorted(stopped) == ["events", "playback"]
        assert client.callback_cleanups == {} and client.pull_iterator_cleanups == {}
        assert all(s._closed for s in direct)

    asyncio.run(run())


class ServiceCaller(FakeClient):
    def __init__(self) -> None:
        super().__init__("ok")

    async def _call_service(self, subject: str, args: tuple[Any, ...], direct: str) -> Any:
        self.calls.append(("service", subject, args, {"direct": direct}))
        return "ok"


def test_a_service_proxy_prefers_the_subject_of_its_service() -> None:
    async def run() -> None:
        client = ServiceCaller()
        info = service_info("whoami", "svc.svc.whoami", "db.find", "svc.svc.db.find", "legacy")
        proxy = create_service_proxy(client, info)  # type: ignore[arg-type]
        await proxy.whoami()
        await proxy.db.find()
        await proxy.legacy()

        assert [c[1] for c in client.calls] == ["svc.svc.whoami", "svc.svc.db.find", "legacy"]
        # the service's own subjects add no attribute of their own
        assert not hasattr(proxy, "svc")

    asyncio.run(run())


def test_a_service_proxy_hands_the_instance_subject_to_its_calls() -> None:
    async def run() -> None:
        client = ServiceCaller()
        info = service_info("echo", "svc.svc.echo", metadata={"cui.chunk.direct": "1"})
        proxy = create_service_proxy(client, info)  # type: ignore[arg-type]
        await proxy.echo(1)

        assert client.calls == [("service", "svc.svc.echo", (1,), {"direct": "$CUI.SVC.inst1.echo"})]

    asyncio.run(run())


def test_a_call_too_big_for_one_message_goes_to_the_instance() -> None:
    async def run() -> None:
        client, nc = make_client()
        client._max_payload_size = 1000
        small = asyncio.create_task(client._call_service("svc.a.echo", ("x",), "$CUI.SVC.i.echo"))
        big = asyncio.create_task(client._call_service("svc.a.echo", (b"x" * 5000,), "$CUI.SVC.i.echo"))
        await wait_for(lambda: len({p["subject"] for p in nc.published}) == 2)

        subjects = [p["subject"] for p in nc.published]
        assert subjects.count("svc.a.echo") == 1
        # the big one went out chunked, header and chunks to the one instance
        header = next(decode(p["data"]) for p in nc.published if p["subject"] == "$CUI.SVC.i.echo")
        assert subjects.count("$CUI.SVC.i.echo") == 1 + header["totalChunks"]
        assert any(s.pattern.startswith("$CUI.SVC.i.echo.reply.") for s in nc.subs)
        small.cancel()
        big.cancel()

    asyncio.run(run())


@pytest.mark.parametrize("order", ["cancel, then timeout", "timeout, then cancel in the same step"])
def test_a_cancelled_service_call_leaves_no_subscription_behind(order: str) -> None:
    async def run() -> None:
        problems: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _, context: problems.append(context))
        client, nc = make_drain_client(timeout=50)
        call = asyncio.create_task(client.call("svc.a.echo", 1))
        await wait_for(lambda: nc.published)
        [request_id] = list(client.pending_requests)
        session = {sub._subject for sub in nc._subs.values()}
        assert len(session) == 2 and f"svc.a.echo.reply.{request_id}" in session

        if order == "timeout, then cancel in the same step":
            timer = client.pending_requests[request_id]["timeout"]
            fire, args = timer._callback, timer._args
            timer.cancel()
            fire(*args)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        await asyncio.sleep(0.1)

        assert client.pending_requests == {}
        assert [sub for sub in nc._subs.values() if sub._subject in session] == []
        assert problems == []

    asyncio.run(run())


def test_a_service_call_whose_inbox_cannot_subscribe_leaves_nothing_behind() -> None:
    async def run() -> None:
        problems: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _, context: problems.append(context))
        client, nc = make_drain_client(timeout=50)
        subscribe = nc.subscribe

        async def refuse_inboxes(subject: str, *args: Any, **kwargs: Any) -> Any:
            if subject.startswith("_INBOX."):
                raise RuntimeError("no inbox")
            return await subscribe(subject, *args, **kwargs)

        nc.subscribe = refuse_inboxes  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="no inbox"):
            await client.call("svc.a.echo", 1)
        await asyncio.sleep(0.1)

        assert client.pending_requests == {}
        assert [sub for sub in nc._subs.values() if sub._subject.startswith("svc.a.echo.reply.")] == []
        assert problems == []

    asyncio.run(run())


class DiscoveryNC:
    """Answers $SRV requests after a delay, with a no-responder status or garbage first when asked."""

    def __init__(self, replies: list[tuple[float, Any]]) -> None:
        self.replies = replies
        self.queue: asyncio.Queue[Any] = asyncio.Queue()

    def new_inbox(self) -> str:
        return "_INBOX.d"

    async def subscribe(self, inbox: str) -> Any:
        queue = self.queue

        async def next_msg(timeout: float) -> Any:
            return await asyncio.wait_for(queue.get(), timeout)

        async def unsubscribe() -> None:
            pass

        return SimpleNamespace(next_msg=next_msg, unsubscribe=unsubscribe)

    async def publish(self, subject: str, data: bytes, reply: str = "") -> None:
        async def answer(delay: float, body: Any) -> None:
            await asyncio.sleep(delay)
            if body == 503:
                self.queue.put_nowait(
                    SimpleNamespace(data=b"", headers={Header.STATUS: NO_RESPONDERS_STATUS})
                )
            else:
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.queue.put_nowait(SimpleNamespace(data=data, headers=None))

        for delay, body in self.replies:
            asyncio.ensure_future(answer(delay, body))


def ping(service_id: str) -> dict[str, Any]:
    return {"type": "io.nats.micro.v1.ping_response", "name": "alpha", "id": service_id, "version": "1.0.0"}


def test_discovery_waits_for_a_slow_first_answer_and_skips_garbage() -> None:
    async def run() -> None:
        client = SimpleNamespace(
            nc=DiscoveryNC([(0.05, b"{not json"), (0.06, ping("a")), (0.065, ping("b"))])
        )
        started = time.monotonic()
        found = await ServiceMonitor(client).ping("alpha")  # type: ignore[arg-type]
        assert [p.id for p in found] == ["a", "b"]
        assert time.monotonic() - started < 0.5

    asyncio.run(run())


def test_discovery_ends_at_once_when_no_service_answers() -> None:
    async def run() -> None:
        client = SimpleNamespace(nc=DiscoveryNC([(0.0, 503)]))
        started = time.monotonic()
        assert await ServiceMonitor(client).ping("nobody") == []  # type: ignore[arg-type]
        assert time.monotonic() - started < 0.1

    asyncio.run(run())
