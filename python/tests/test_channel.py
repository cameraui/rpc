from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from test_drain import until
from test_handler import open_pull, pull, wait_for
from test_handler import serve as serve_handlers
from test_isolated import dialing

from camera_ui_rpc.codec import encode


def subjects(nc: Any) -> list[str]:
    return [s._subject for s in nc._subs.values()]


@pytest.mark.parametrize("private", [False, True])
def test_closing_an_isolated_channel_closes_its_connection(
    private: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        if private:
            channel = await client.private_channel("c1", "peer", isolated_connection=True)
        else:
            channel = await client.channel("c1", isolated_connection=True)
        child = client.isolated_clients[0]
        assert child.nc is conns[1]

        await channel.close()

        assert child.is_closed
        assert conns[1].is_closed
        assert client.isolated_clients == []

    asyncio.run(run())


def test_a_close_from_the_other_side_closes_the_isolated_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        await client.channel("c1", isolated_connection=True)
        child = client.isolated_clients[0]
        nc = conns[1]
        sid = next(s._id for s in nc._subs.values() if s._subject == "channel.c1")
        close = encode({"type": "close", "data": None, "error": None, "sender": None})
        await nc._process_msg(sid, b"channel.c1", b"", close, b"")

        await until(lambda: child.is_closed)
        assert client.isolated_clients == []

    asyncio.run(run())


def test_a_private_channel_drops_its_request_handler_on_close(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        client, conns = dialing(monkeypatch)
        await client.connect()
        channel = await client.private_channel("c1", "peer")
        await channel.on_request(lambda data: data)
        request = "channel.private.c1.parent.peer.request"
        assert request in subjects(conns[0])

        await channel.close()

        assert request not in subjects(conns[0])

    asyncio.run(run())


def test_finished_sessions_leave_no_ids_in_their_registration() -> None:
    async def run() -> None:
        async def items() -> AsyncGenerator[int, None]:
            yield 1

        client, nc, unregister = await serve_handlers({"items": items})

        def holding(session_id: str) -> list[Any]:
            # the per-registration id collections live in the cleanup closure of register_handler
            cells = [cell.cell_contents for cell in unregister.__closure__ or ()]
            return [c for c in cells if isinstance(c, set | list) and session_id in c]

        await open_pull(nc, "items")
        assert holding("it1")
        pull(nc, "next")
        pull(nc, "next")
        await wait_for(lambda: client.pull_iterator_cleanups == {})

        assert holding("it1") == []

    asyncio.run(run())
