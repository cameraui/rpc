"""Tests for the MessagePack codec round-trips."""

import struct
from datetime import date, datetime, time, timezone
from enum import Enum
from typing import Any

import pytest

from camera_ui_rpc.codec import (
    BINARY_EXTRACT_THRESHOLD,
    decode,
    decode_message,
    encode,
    encode_message,
)


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


def _bytes(length: int, fill: int = 7) -> bytes:
    return bytes([fill]) * length


# Extractable sizes are expressed relative to the threshold so the tests
# survive threshold tuning.
T = BINARY_EXTRACT_THRESHOLD


def message_roundtrip(value: Any) -> Any:
    return decode_message(encode_message(value))


class TestFraming:
    def test_message_without_large_binaries_is_byte_identical_to_encode(self) -> None:
        message = {"id": "abc", "method": "call", "params": [1, "x", {"nested": True}, b"\x01\x02\x03"]}
        assert encode_message(message) == encode(message)

    def test_frames_extracted_binaries_as_magic_envlen_envelope_segments(self) -> None:
        bin_data = _bytes(T + 1024, 9)
        message = {"id": "abc", "params": [bin_data]}
        encoded = encode_message(message)

        # Magic "CUIB"
        assert encoded[:4] == b"CUIB"

        env_len = struct.unpack_from("<I", encoded, 4)[0]
        envelope = decode(encoded[8 : 8 + env_len])
        assert envelope == {"id": "abc", "params": [{"__cui_bin__": 0, "l": T + 1024}]}

        # Segment lies back-to-back after the envelope
        assert len(encoded) == 8 + env_len + T + 1024
        assert encoded[8 + env_len :] == bin_data

    def test_lays_out_multiple_segments_in_traversal_order(self) -> None:
        a = _bytes(T, 1)
        b = _bytes(T + 500, 2)
        c = _bytes(T + 4096, 3)
        message = {"first": a, "nested": {"deep": [b]}, "last": c}
        encoded = encode_message(message)

        env_len = struct.unpack_from("<I", encoded, 4)[0]
        envelope = decode(encoded[8 : 8 + env_len])
        assert envelope["first"] == {"__cui_bin__": 0, "l": T}
        assert envelope["nested"]["deep"][0] == {"__cui_bin__": 1, "l": T + 500}
        assert envelope["last"] == {"__cui_bin__": 2, "l": T + 4096}

        base = 8 + env_len
        assert encoded[base] == 1
        assert encoded[base + T] == 2
        assert encoded[base + T + (T + 500)] == 3
        assert len(encoded) == base + T + (T + 500) + (T + 4096)

    def test_does_not_mutate_the_input_message_during_extraction(self) -> None:
        bin_data = _bytes(T)
        inner = {"inner": bin_data}
        params = [bin_data, inner]
        message = {"params": params}
        encode_message(message)
        assert message["params"] is params
        assert params[0] is bin_data
        assert params[1] is inner
        assert inner["inner"] is bin_data


class TestMessageRoundtrip:
    def test_roundtrips_a_binary_inside_an_args_array(self) -> None:
        bin_data = _bytes(T + 4096, 42)
        message = {"id": "x1", "method": "call", "params": ["snapshot", bin_data, {"quality": 80}]}
        result = message_roundtrip(message)
        assert result["id"] == "x1"
        assert result["params"][0] == "snapshot"
        assert bytes(result["params"][1]) == bin_data
        assert result["params"][2] == {"quality": 80}

    def test_roundtrips_a_binary_inside_a_nested_object(self) -> None:
        bin_data = _bytes(T + 10_000, 5)
        message = {"result": {"frame": {"data": bin_data, "pts": 1234}, "ok": True}}
        result = message_roundtrip(message)
        assert result["result"]["frame"]["pts"] == 1234
        assert result["result"]["ok"] is True
        assert bytes(result["result"]["frame"]["data"]) == bin_data

    def test_roundtrips_multiple_binaries_and_keeps_contents_distinct(self) -> None:
        a = _bytes(T, 1)
        b = _bytes(T + 2048, 2)
        c = _bytes(T + 1, 3)
        message = {"params": [a, {"b": b}, [c]]}
        result = message_roundtrip(message)
        assert bytes(result["params"][0]) == a
        assert bytes(result["params"][1]["b"]) == b
        assert bytes(result["params"][2][0]) == c

    def test_keeps_a_1023_byte_binary_inline_below_threshold(self) -> None:
        bin_data = _bytes(BINARY_EXTRACT_THRESHOLD - 1)
        message = {"params": [bin_data]}
        encoded = encode_message(message)
        assert encoded == encode(message)
        assert bytes(message_roundtrip(message)["params"][0]) == bin_data

    def test_extracts_a_1024_byte_binary_at_threshold(self) -> None:
        bin_data = _bytes(BINARY_EXTRACT_THRESHOLD)
        message = {"params": [bin_data]}
        encoded = encode_message(message)
        assert encoded[:4] == b"CUIB"
        assert bytes(message_roundtrip(message)["params"][0]) == bin_data

    def test_roundtrips_a_bytearray_as_an_extracted_segment(self) -> None:
        buf = bytearray(b"\xab" * (T + 5000))
        result = message_roundtrip({"params": [buf]})
        assert bytes(result["params"][0]) == bytes(buf)

    def test_roundtrips_a_memoryview_as_an_extracted_segment(self) -> None:
        backing = b"\xcd" * (T + 2000)
        result = message_roundtrip({"params": [memoryview(backing)]})
        assert bytes(result["params"][0]) == backing

    def test_roundtrips_binaries_inside_tuples(self) -> None:
        bin_data = _bytes(T + 2048, 0x33)
        message = {"params": ("lead", bin_data)}
        result = message_roundtrip(message)
        assert result["params"][0] == "lead"
        assert bytes(result["params"][1]) == bin_data

    def test_roundtrips_a_root_level_binary(self) -> None:
        bin_data = _bytes(T + 3000, 6)
        result = message_roundtrip(bin_data)
        assert bytes(result) == bin_data


class TestZeroCopyDecode:
    def test_returns_memoryview_slices_into_the_received_buffer(self) -> None:
        bin_data = _bytes(T + 2048, 0x11)
        encoded = bytearray(encode_message({"params": [bin_data]}))
        result = decode_message(encoded)
        view = result["params"][0]
        assert isinstance(view, memoryview)
        assert view.obj is encoded
        # Mutating the receive buffer is visible through the view (no copy).
        encoded[-1] = 0x99
        assert view[-1] == 0x99

    def test_views_reference_a_bytes_payload_without_copy(self) -> None:
        bin_data = _bytes(T + 2048, 0x22)
        encoded = encode_message({"params": [bin_data]})
        result = decode_message(encoded)
        view = result["params"][0]
        assert isinstance(view, memoryview)
        assert view.obj is encoded


class TestPlaceholderCollisionSafety:
    def test_does_not_replace_a_user_map_with_cui_bin_but_without_l(self) -> None:
        message = {"params": [{"__cui_bin__": 0}, _bytes(T)]}
        result = message_roundtrip(message)
        assert result["params"][0] == {"__cui_bin__": 0}

    def test_does_not_replace_a_user_map_with_a_non_integer_l(self) -> None:
        message = {"params": [{"__cui_bin__": 0, "l": "nope"}, _bytes(T)]}
        result = message_roundtrip(message)
        assert result["params"][0] == {"__cui_bin__": 0, "l": "nope"}

    def test_does_not_replace_a_user_map_with_extra_keys(self) -> None:
        message = {"params": [{"__cui_bin__": 0, "l": 1, "extra": True}, _bytes(T)]}
        result = message_roundtrip(message)
        assert result["params"][0] == {"__cui_bin__": 0, "l": 1, "extra": True}

    def test_leaves_placeholder_shaped_user_maps_untouched_in_binary_free_messages(self) -> None:
        # Without extracted binaries there is no CUIB frame, hence no
        # placeholder substitution at all.
        message = {"params": [{"__cui_bin__": 0, "l": 123}]}
        result = message_roundtrip(message)
        assert result["params"][0] == {"__cui_bin__": 0, "l": 123}


class TestOutOfOrderPlaceholders:
    def test_restores_segments_whose_placeholders_appear_out_of_traversal_order(self) -> None:
        # A port whose map serialization order differs from its extraction
        # order (e.g. Go map iteration) may emit placeholder 1 before 0 in
        # the envelope. Segments still lie back-to-back in index order.
        seg0 = _bytes(T, 0xAA)
        seg1 = _bytes(T + 2048, 0xBB)
        envelope = encode({"b": {"__cui_bin__": 1, "l": T + 2048}, "a": {"__cui_bin__": 0, "l": T}})
        frame = b"CUIB" + struct.pack("<I", len(envelope)) + envelope + seg0 + seg1

        result = decode_message(frame)
        assert bytes(result["a"]) == seg0
        assert bytes(result["b"]) == seg1

    def test_rejects_an_index_gap_in_out_of_order_placeholders(self) -> None:
        seg = _bytes(T, 0xCC)
        envelope = encode({"b": {"__cui_bin__": 2, "l": T}})
        frame = b"CUIB" + struct.pack("<I", len(envelope)) + envelope + seg
        with pytest.raises(ValueError, match="missing placeholder"):
            decode_message(frame)


class TestFrameValidation:
    def test_rejects_a_frame_whose_envelope_length_exceeds_the_payload(self) -> None:
        encoded = bytearray(encode_message({"params": [_bytes(T)]}))
        struct.pack_into("<I", encoded, 4, len(encoded))
        with pytest.raises(ValueError, match="envelope length"):
            decode_message(encoded)

    def test_rejects_a_truncated_frame_segment_exceeds_payload(self) -> None:
        encoded = encode_message({"params": [_bytes(T)]})
        with pytest.raises(ValueError, match="Invalid CUIB frame"):
            decode_message(encoded[: len(encoded) - 10])

    def test_rejects_trailing_bytes_after_the_last_segment(self) -> None:
        encoded = encode_message({"params": [_bytes(T)]})
        with pytest.raises(ValueError, match="expected payload size"):
            decode_message(encoded + b"\x00\x00\x00\x00")
