"""Tests for pure utility helpers."""

import re
from collections.abc import AsyncGenerator, Generator

from camera_ui_rpc.utils import (
    RPCCallbacks,
    generate_id,
    is_async_function,
    is_async_generator,
    is_generator,
    is_rpc_callbacks,
    is_sync_generator,
    rpc_callbacks,
)

ID_PATTERN = re.compile(r"^\d+-[0-9a-z]{9}$")


def test_generate_id_format() -> None:
    assert ID_PATTERN.match(generate_id())


def test_generate_id_random_suffix_charset() -> None:
    suffix = generate_id().split("-")[1]
    assert len(suffix) == 9
    assert all(c in "0123456789abcdefghijklmnopqrstuvwxyz" for c in suffix)


def test_generate_id_uniqueness() -> None:
    ids = {generate_id() for _ in range(1000)}
    assert len(ids) > 1


def test_rpc_callbacks_collects_callables() -> None:
    bundle = rpc_callbacks(on_item=lambda _: None, on_end=lambda: None)
    assert isinstance(bundle, RPCCallbacks)
    assert set(bundle.methods.keys()) == {"on_item", "on_end"}


def test_rpc_callbacks_ignores_non_callables() -> None:
    bundle = rpc_callbacks(on_item=lambda _: None, not_a_func=123)
    assert "not_a_func" not in bundle.methods
    assert "on_item" in bundle.methods


def test_rpc_callbacks_defaults_oneway_to_all_methods() -> None:
    bundle = rpc_callbacks(a=lambda: None, b=lambda: None)
    assert set(bundle.oneway) == {"a", "b"}


def test_rpc_callbacks_explicit_oneway() -> None:
    bundle = rpc_callbacks(a=lambda: None, b=lambda: None, oneway=["a"])
    assert bundle.oneway == ["a"]


def test_is_rpc_callbacks_true_for_bundle() -> None:
    assert is_rpc_callbacks(rpc_callbacks(a=lambda: None)) is True


def test_is_rpc_callbacks_false_for_other() -> None:
    assert is_rpc_callbacks({"a": 1}) is False
    assert is_rpc_callbacks(None) is False


def test_is_sync_generator_true() -> None:
    def gen() -> Generator[int, None, None]:
        yield 1

    assert is_sync_generator(gen) is True


def test_is_sync_generator_false_for_generator_instance() -> None:
    def gen() -> Generator[int, None, None]:
        yield 1

    assert is_sync_generator(gen()) is False


def test_is_sync_generator_false_for_plain_function() -> None:
    def plain() -> int:
        return 1

    assert is_sync_generator(plain) is False


def test_is_async_generator_true() -> None:
    async def agen() -> AsyncGenerator[int, None]:
        yield 1

    assert is_async_generator(agen) is True


def test_is_async_generator_false_for_sync() -> None:
    def gen() -> Generator[int, None, None]:
        yield 1

    assert is_async_generator(gen) is False


def test_is_async_function_true() -> None:
    async def coro() -> int:
        return 1

    assert is_async_function(coro) is True


def test_is_async_function_false_for_plain() -> None:
    def plain() -> int:
        return 1

    assert is_async_function(plain) is False


def test_is_generator_combines_sync_and_async() -> None:
    def gen() -> Generator[int, None, None]:
        yield 1

    async def agen() -> AsyncGenerator[int, None]:
        yield 1

    assert is_generator(gen) is True
    assert is_generator(agen) is True


def test_predicates_false_for_non_callable() -> None:
    assert is_generator(42) is False
    assert is_sync_generator(42) is False
    assert is_async_generator(42) is False
    assert is_async_function(42) is False
