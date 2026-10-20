import struct
from datetime import UTC, date, datetime, time
from enum import Enum, StrEnum
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
    class Color(StrEnum):
        RED = "red"

    assert roundtrip(Color.RED) == "red"


def test_int_enum_encoded_as_value() -> None:
    class Level(int, Enum):
        HIGH = 3

    assert roundtrip(Level.HIGH) == 3


def test_datetime_roundtrip() -> None:
    value = datetime(2026, 6, 15, 12, 30, 45, tzinfo=UTC)
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


# sizes relative to the threshold, so the tests survive tuning it
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

        assert encoded[:4] == b"CUIB"

        env_len = struct.unpack_from("<I", encoded, 4)[0]
        envelope = decode(encoded[8 : 8 + env_len])
        assert envelope == {"id": "abc", "params": [{"__cui_bin__": 0, "l": T + 1024}]}

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
        # mutating the receive buffer shows through the view, no copy
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
        # without extracted binaries there is no CUIB frame, so no placeholder substitution
        message = {"params": [{"__cui_bin__": 0, "l": 123}]}
        result = message_roundtrip(message)
        assert result["params"][0] == {"__cui_bin__": 0, "l": 123}


class TestOutOfOrderPlaceholders:
    def test_restores_segments_whose_placeholders_appear_out_of_traversal_order(self) -> None:
        # Go map iteration may emit placeholder 1 before 0, segments still follow in index order
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


def _reference_extract(value: Any, segments: list[Any]) -> Any:
    # the walk encode_message used before the pack-first path, kept as the byte-identity oracle
    if isinstance(value, (bytes, bytearray)) and len(value) >= BINARY_EXTRACT_THRESHOLD:
        segments.append(value)
        return {"__cui_bin__": len(segments) - 1, "l": len(value)}
    if isinstance(value, memoryview) and value.nbytes >= BINARY_EXTRACT_THRESHOLD:
        segment = value if value.c_contiguous else memoryview(value.tobytes())
        segments.append(segment)
        return {"__cui_bin__": len(segments) - 1, "l": segment.nbytes}
    if isinstance(value, dict):
        out = {key: _reference_extract(item, segments) for key, item in value.items()}
        return out if any(out[k] is not value[k] for k in value) else value
    if isinstance(value, (list, tuple)):
        items = [_reference_extract(item, segments) for item in value]
        return items if any(a is not b for a, b in zip(items, value, strict=True)) else value
    return value


def reference_encode_message(data: Any) -> bytes:
    segments: list[Any] = []
    transformed = _reference_extract(data, segments)
    if not segments:
        return encode(data)
    envelope = encode(transformed)
    return b"".join((b"CUIB", struct.pack("<I", len(envelope)), envelope, *segments))


class Label(StrEnum):
    PERSON = "person"


class BigBytes(bytes):
    pass


def _detections(n: int) -> list[dict[str, Any]]:
    return [{"label": Label.PERSON, "score": 0.5 + i / 100, "box": [i, i + 1, 20, 30]} for i in range(n)]


GOLDEN_MESSAGES: list[Any] = [
    None,
    1,
    "x" * 20000,
    {"id": "a.b", "result": _detections(0)},
    {"id": "a.b", "result": _detections(3)},
    {"id": "a.b", "result": {"detections": _detections(12), "embedding": [0.25] * 512}},
    {"id": "a.b", "method": "call", "params": ({"data": _bytes(307200), "width": 320},)},
    {"id": "a.b", "method": "call", "params": ([{"data": _bytes(58000)}, {"data": _bytes(58000)}],)},
    {"id": "a.b", "method": "call", "params": [[[{"deep": _bytes(BINARY_EXTRACT_THRESHOLD)}]]]},
    {"id": "a.b", "result": _bytes(BINARY_EXTRACT_THRESHOLD - 1)},
    {"id": "a.b", "result": _bytes(BINARY_EXTRACT_THRESHOLD)},
    {"id": "a.b", "result": bytearray(_bytes(BINARY_EXTRACT_THRESHOLD + 5))},
    {"id": "a.b", "result": memoryview(_bytes(20000))},
    {"id": "a.b", "result": memoryview(_bytes(40000))[::2]},
    {"id": "a.b", "result": memoryview(_bytes(2000))[::2]},
    {"id": "a.b", "result": BigBytes(_bytes(20000))},
    {"id": "a.b", "result": [{"k": i, "v": "s" * 30} for i in range(1000)]},
    {"id": "a.b", "result": ("tuple", [1, 2, (3, _bytes(16400))])},
    {"k": [1] * 20 + [_bytes(17000)]},
    {f"key{i}": i for i in range(40)} | {"late": _bytes(18000)},
]


@pytest.mark.parametrize("message", GOLDEN_MESSAGES)
def test_encode_message_bytes_match_the_full_walk(message: Any) -> None:
    try:
        expected = reference_encode_message(message)
    except TypeError:
        with pytest.raises(TypeError):
            encode_message(message)
        return
    assert encode_message(message) == expected


def test_encode_message_bytes_match_the_full_walk_on_random_structures() -> None:
    import random

    rng = random.Random(1234)
    sizes = [0, 10, BINARY_EXTRACT_THRESHOLD - 1, BINARY_EXTRACT_THRESHOLD, 30000]

    def build(depth: int) -> Any:
        pick = rng.random()
        if depth > 4 or pick < 0.35:
            return rng.choice([None, True, 3, 2.5, "s", _bytes(rng.choice(sizes), rng.randrange(256))])
        if pick < 0.7:
            return {f"k{i}": build(depth + 1) for i in range(rng.randrange(0, 20))}
        seq = [build(depth + 1) for _ in range(rng.randrange(0, 12))]
        return tuple(seq) if rng.random() < 0.3 else seq

    for _ in range(150):
        message = {"id": "x", "result": build(0)}
        assert encode_message(message) == reference_encode_message(message)


def test_small_message_is_packed_without_the_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    import camera_ui_rpc.codec as codec

    def no_walk(*_args: Any) -> Any:
        raise AssertionError("walked a message under the extract threshold")

    monkeypatch.setattr(codec, "_extract_binaries", no_walk)
    message = {"id": "a.b", "result": {"detections": _detections(12)}}
    assert codec.encode_message(message) == encode(message)


def test_frame_message_is_packed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import camera_ui_rpc.codec as codec

    packs: list[int] = []
    real_encode = codec.encode

    def counting_encode(data: Any) -> bytes:
        packs.append(1)
        return real_encode(data)

    monkeypatch.setattr(codec, "encode", counting_encode)
    codec.encode_message({"id": "a.b", "method": "call", "params": ({"data": _bytes(307200)},)})
    assert packs == [1]


@pytest.mark.parametrize("kind", [bytes, bytearray, memoryview])
def test_msgpack_packs_binaries_at_full_length(kind: Any) -> None:
    # encode_message returns small packs without the walk because of this
    value = kind(_bytes(BINARY_EXTRACT_THRESHOLD))
    assert len(encode({"v": value})) >= BINARY_EXTRACT_THRESHOLD
