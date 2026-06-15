#!/usr/bin/env python3
"""Cross-language codec test — Python decodes Node.js and Go encoded data."""

import math
import os
import sys
from datetime import datetime
from typing import Any

current_dir = os.path.dirname(os.path.abspath(__file__))
relative_path = os.path.join(current_dir, "..", "python")
absolute_path = os.path.abspath(relative_path)
sys.path.insert(0, absolute_path)

from camera_ui_rpc.codec import decode  # type: ignore

expected_data: dict[str, Any] = {
    "string": "Hello World",
    "empty_string": "",
    "number_int": 42,
    "number_negative": -42,
    "number_zero": 0,
    "number_large": 2**31 - 1,
    "float": 3.14159,
    "float_negative": -3.14159,
    "float_zero": 0.0,
    "float_inf": float("inf"),
    "float_neg_inf": float("-inf"),
    "float_nan": float("nan"),
    "boolean_true": True,
    "boolean_false": False,
    "null": None,
    "empty_array": [],
    "array": [1, 2, 3, 4, 5],
    "mixed_array": ["hello", 42, True, None],
    "nested_array": [[1, 2], [3, 4], [5, 6]],
    "array_with_objects": [{"a": 1}, {"b": 2}],
    "tuple": [1, 2, 3],
    "nested_tuple": [[1, 2], [3, 4]],
    "empty_object": {},
    "simple_object": {"key": "value", "number": 123},
    "nested_object": {"outer": {"inner": {"value": 42}}},
    "object_mixed_keys": {"str": "text", "num": 42, "bool": True, "null": None},
    "datetime": "__type_check__",
    "date": "__type_check__",
    "time": "__type_check__",
    "timestamp": "__type_check__",
    "enum": "INTERNAL_ERROR",
    "enum_in_dict": {"error": "TIMEOUT", "code": 408},
    "complex": "__type_check__",
    "binary": b"Hello binary world",
    "empty_binary": b"",
    "binary_with_nulls": b"\x00\x01\x02\x03\x04",
    "unicode": "你好世界 🌍",
    "emoji": "🎉🎊🎈🎁🎀",
    "special_chars": "äöü ñ é à ß",
    "escape_chars": "line1\nline2\ttab\r\nwindows",
    "quotes": "He said \"Hello\" and she said 'Hi'",
    "very_long_string": "x" * 10000,
    "deeply_nested": {"l1": {"l2": {"l3": {"l4": {"l5": {"value": "deep"}}}}}},
    "large_array": "__type_check__",
    "max_safe_int": 9007199254740991,
    "min_safe_int": -9007199254740991,
    "rpc_message": {
        "id": "1234567890-abcdef",
        "method": "test.method",
        "params": [1, "two", {"three": 3}],
        "error": None,
    },
    "stream_message": {"id": "stream-123", "type": "data", "data": {"chunk": 1, "total": 10}},
    "js_timestamp": 1708786800000,
    "date_ext": "__type_check__",
    "camera_config": {
        "cameraId": "cam-abc-123",
        "fps": 30,
        "eventTimeout": 30,
        "timestamp": 1708786800000,
        "confidence": 0.85,
        "enabled": True,
        "name": "Front Door",
    },
    "detection_event": {
        "type": "start",
        "data": {
            "id": "evt-abc-123",
            "state": "active",
            "types": ["motion", "audio"],
            "startTime": 1708786800000,
            "endTime": 0,
            "triggers": [
                {"type": "motion", "timestamp": 1708786800000, "data": {"score": 0.95}},
                {"type": "audio", "timestamp": 1708786800500, "data": {"decibels": -25.5}},
            ],
            "segments": [],
        },
    },
    "map_with_nil": {"key": "value", "optional": None, "count": 0, "flag": False},
    "nested_ints": {"a": {"b": {"c": 42}}, "d": [1, 2, 3], "e": 100},
    "empty_nested": {"a": {}, "b": [], "c": {"d": {}}},
    "sensor_list": [
        {"id": "sensor-1", "type": "motion", "online": True, "score": 0.95},
        {"id": "sensor-2", "type": "audio", "online": False, "score": 0.0},
        {"id": "sensor-3", "type": "object", "online": True, "score": 0.87},
    ],
    "mixed_numerics": "__type_check__",
}


def check_type_only(name: str, actual: Any) -> bool:
    if name == "datetime":
        return isinstance(actual, (datetime, str))
    if name in ("date", "time"):
        return isinstance(actual, str) and len(actual) > 0
    if name == "timestamp":
        return isinstance(actual, (int, float))
    if name == "date_ext":
        return isinstance(actual, datetime)
    if name == "complex":
        if not isinstance(actual, dict):
            return False
        return actual.get("id") == "test-123" and actual.get("method") == "greet"
    if name == "large_array":
        return isinstance(actual, list) and len(actual) == 1000
    if name == "mixed_numerics":
        if not isinstance(actual, dict):
            return False
        return actual.get("float_val") == 3.14 and actual.get("integer") == 42
    return actual is not None


def compare_values(expected: Any, actual: Any, test_name: str) -> bool:
    if test_name == "float_nan":
        return isinstance(actual, float) and math.isnan(actual)

    if test_name == "float_zero":
        return actual == 0 or actual == 0.0

    if expected == "__type_check__":
        return check_type_only(test_name, actual)

    if isinstance(expected, (bytes, bytearray)):
        if isinstance(actual, (bytes, bytearray)):
            return bytes(expected) == bytes(actual)
        if len(expected) == 0 and actual is None:
            return True
        return False

    return actual == expected


def run_cross_test(source_name: str, path_pattern: str) -> tuple[int, int]:
    passed = 0
    failed = 0

    print(f"Python cross-language decode: {source_name} data")
    print("=" * 60)

    for test_name, expected in expected_data.items():
        filepath = path_pattern.replace("%s", test_name)

        if not os.path.exists(filepath):
            print(f"  SKIP {test_name}: file not found")
            continue

        try:
            with open(filepath, "rb") as f:
                encoded = f.read()

            decoded = decode(encoded)
            success = compare_values(expected, decoded, test_name)

            if success:
                print(f"  OK {test_name}")
                passed += 1
            else:
                print(f"  FAIL {test_name}")
                print(f"    Expected: {str(expected)[:80]}")
                print(f"    Got:      {str(decoded)[:80]}")
                failed += 1

        except Exception as e:
            print(f"  FAIL {test_name}: {e}")
            failed += 1

    return passed, failed


results = [
    run_cross_test("Node.js", "/tmp/node-encoded-%s.msgpack"),
    run_cross_test("Go", "/tmp/go-encoded-%s.msgpack"),
]

print()
total_passed = sum(r[0] for r in results)
total_failed = sum(r[1] for r in results)
print(f"Total: {total_passed} passed, {total_failed} failed")

if total_failed > 0:
    sys.exit(1)
