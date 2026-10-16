from __future__ import annotations

import asyncio
from typing import Any

import pytest
from test_drain import answers, until
from test_register import GatedNC

import camera_ui_rpc.client as rpc_client
from camera_ui_rpc.codec import encode


def dialing(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[GatedNC]]:
    conns: list[GatedNC] = []

    async def fake_connect(**options: Any) -> GatedNC:
        nc = GatedNC(open_gate=True)
        nc.name = options.get("name")  # type: ignore[attr-defined]
        conns.append(nc)
        return nc

    monkeypatch.setattr(rpc_client, "connect", fake_connect)
    client = rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "parent"})
    return client, conns


def subjects(nc: GatedNC) -> list[str]:
    return sorted(s._subject for s in nc._subs.values() if not s._subject.startswith("rpc.reply."))


def test_suspend_and_connect_bring_isolated_handlers_back(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        await client.register_handler("main", {"a": lambda: "main"})
        await client.register_handler("iso", {"b": lambda: "iso"}, isolated_connection=True)
        child = client.isolated_clients[0]
        assert [c.name for c in conns] == ["parent", "parent-handler-iso"]  # type: ignore[attr-defined]

        await client.suspend()
        # suspended, not closed: its entries stay for the next connect
        assert child.nc is None and not child.is_closed
        assert child._subscription_entries

        await client.connect()

        assert [c.name for c in conns[2:]] == ["parent", "parent-handler-iso"]  # type: ignore[attr-defined]
        assert subjects(conns[2]) == ["rpc.main.__methods", "rpc.main.a"]
        assert subjects(conns[3]) == ["rpc.iso.__methods", "rpc.iso.b"]
        nc = conns[3]
        sid = next(s._id for s in nc._subs.values() if s._subject == "rpc.iso.b")
        await nc._process_msg(
            sid, b"rpc.iso.b", b"", encode({"id": "req.1", "method": "call", "params": []}), b""
        )
        await until(lambda: answers(nc))
        assert answers(nc) == {"req.1": "iso"}

    asyncio.run(run())


def test_a_closed_isolated_proxy_stays_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        closed = client.create_proxy("gone", isolated_connection=True)
        kept = client.create_proxy("kept", isolated_connection=True)
        await closed.close()
        assert len(client.isolated_clients) == 1

        await client.suspend()
        await client.connect()

        # the parent and the proxy that is still open, which now connects at once
        assert [c.name for c in conns] == ["parent", "parent", "parent-proxy-kept"]  # type: ignore[attr-defined]
        assert kept is not None

    asyncio.run(run())
