from __future__ import annotations

import asyncio
from typing import Any

import pytest

import camera_ui_rpc.client as rpc_client


class _Refused(Exception):
    pass


@pytest.mark.parametrize(
    ("options", "attempts"),
    [
        # nats-py dials forever with -1 and with 0, 1 gives up after the second dial
        ({"reconnect": False}, 1),
        ({"reconnect": False, "max_reconnect_attempts": 5}, 5),
        ({}, -1),
        ({"reconnect": True}, -1),
    ],
)
def test_a_client_that_does_not_reconnect_gives_up_dialing(
    monkeypatch: pytest.MonkeyPatch, options: dict[str, Any], attempts: int
) -> None:
    seen: list[dict[str, Any]] = []

    async def fake_connect(**connect_options: Any) -> Any:
        seen.append(connect_options)
        raise _Refused

    monkeypatch.setattr(rpc_client, "connect", fake_connect)
    client = rpc_client.RPCClient({"servers": ["nats://127.0.0.1:1"], "name": "dial", **options})  # type: ignore[typeddict-item]

    async def run() -> None:
        with pytest.raises(_Refused):
            await client.connect()

    asyncio.run(run())
    assert seen[0]["max_reconnect_attempts"] == attempts
    assert seen[0]["allow_reconnect"] is options.get("reconnect", True)
