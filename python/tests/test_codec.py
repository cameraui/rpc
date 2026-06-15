"""Tests for the MessagePack codec round-trips."""

from datetime import date, datetime, time, timezone
from enum import Enum

import pytest

from camera_ui_rpc.codec import decode, encode


def roundtrip(value: object) -> object:
    return decode(encode(value))


@pytest.mark.parametrize(
    "value",
    [
        0,
        1,
        -1,
        42,
        -42,
        2**31,
        -(2**31),
        2**53,
    ],
)
def test_int_roundtrip(value: int) -> None:
    assert roundtrip(value) == value


@pytest.mark.parametrize("value", [0.0, 1.5, -1.5, 3.141592653589793, -0.0001])
def test_float_roundtrip(value: float) -> None:
    assert roundtrip(value) == value


@pytest.mark.parametrize("value", [True, False])
def test_bool_roundtrip(value: bool) -> None:
    result = roundtrip(value)
    assert result is value


def test_none_roundtrip() -> None:
    assert roundtrip(None) is None


def test_empty_string_roundtrip() -> None:
    assert roundtrip("") == ""


@pytest.mark.parametrize("value", ["hello", "with spaces", "tab\tnewline\n"])
def test_string_roundtrip(value: str) -> None:
    assert roundtrip(value) == value


def test_unicode_roundtrip() -> None:
    value = "你好世界 🌍"
    assert roundtrip(value) == value


def test_emoji_roundtrip() -> None:
    value = "🎉🎊"
    assert roundtrip(value) == value


def test_bytes_roundtrip() -> None:
    value = b"\x00\x01\x02\xff binary data"
    assert roundtrip(value) == value


def test_empty_bytes_roundtrip() -> None:
    assert roundtrip(b"") == b""


def test_empty_dict_roundtrip() -> None:
    assert roundtrip({}) == {}


def test_empty_list_roundtrip() -> None:
    assert roundtrip([]) == []


def test_flat_dict_roundtrip() -> None:
    value = {"a": 1, "b": "two", "c": True, "d": None}
    assert roundtrip(value) == value


def test_nested_dict_roundtrip() -> None:
    value = {"outer": {"inner": {"deep": [1, 2, 3]}}, "list": [{"k": "v"}]}
    assert roundtrip(value) == value


def test_list_roundtrip() -> None:
    value = [1, "two", 3.0, True, None, [4, 5], {"k": "v"}]
    assert roundtrip(value) == value


def test_unicode_dict_keys_roundtrip() -> None:
    value = {"你好": "世界", "🎉": "party"}
    assert roundtrip(value) == value


def test_non_str_dict_keys_roundtrip() -> None:
    value = {1: "one", 2: "two"}
    assert roundtrip(value) == value


def test_encode_returns_bytes() -> None:
    assert isinstance(encode({"a": 1}), bytes)


def test_decode_accepts_bytearray() -> None:
    encoded = encode({"a": 1})
    assert decode(bytearray(encoded)) == {"a": 1}


def test_decode_accepts_memoryview() -> None:
    encoded = encode([1, 2, 3])
    assert decode(memoryview(encoded)) == [1, 2, 3]


def test_enum_encoded_as_value() -> None:
    class Color(str, Enum):
        RED = "red"

    assert roundtrip(Color.RED) == "red"


def test_int_enum_encoded_as_value() -> None:
    class Level(int, Enum):
        HIGH = 3

    assert roundtrip(Level.HIGH) == 3


def test_datetime_roundtrip() -> None:
    value = datetime(2026, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
    result = decode(encode(value))
    assert isinstance(result, datetime)
    assert result == value


def test_date_roundtrip() -> None:
    value = date(2026, 6, 15)
    result = decode(encode(value))
    assert result == "2026-06-15"


def test_time_encoded_as_isoformat() -> None:
    value = time(14, 30, 0)
    assert roundtrip(value) == "14:30:00"


def test_unsupported_type_raises_type_error() -> None:
    class Custom:
        pass

    with pytest.raises(TypeError):
        encode(Custom())
