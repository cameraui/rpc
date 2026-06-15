#!/usr/bin/env python3
"""Test Python codec functionality — encode + roundtrip verification."""

import math
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

current_dir = os.path.dirname(os.path.abspath(__file__))
relative_path = os.path.join(current_dir, "..", "python")
absolute_path = os.path.abspath(relative_path)
sys.path.insert(0, absolute_path)

from camera_ui_rpc.codec import decode, encode  # type: ignore
from camera_ui_rpc.errors import ErrorCode  # type: ignore

test_cases: list[dict[str, Any]] = [
    {"name": "string", "data": "Hello World"},
    {"name": "empty_string", "data": ""},
    {"name": "number_int", "data": 42},
    {"name": "number_negative", "data": -42},
    {"name": "number_zero", "data": 0},
    {"name": "number_large", "data": 2**31 - 1},
    {"name": "float", "data": 3.14159},
    {"name": "float_negative", "data": -3.14159},
    {"name": "float_zero", "data": 0.0},
    {"name": "float_inf", "data": float("inf")},
    {"name": "float_neg_inf", "data": float("-inf")},
    {"name": "float_nan", "data": float("nan")},
    {"name": "boolean_true", "data": True},
    {"name": "boolean_false", "data": False},
    {"name": "null", "data": None},
    {"name": "empty_array", "data": []},
    {"name": "array", "data": [1, 2, 3, 4, 5]},
    {"name": "mixed_array", "data": ["hello", 42, True, None]},
    {"name": "nested_array", "data": [[1, 2], [3, 4], [5, 6]]},
    {"name": "array_with_objects", "data": [{"a": 1}, {"b": 2}]},
    {"name": "tuple", "data": (1, 2, 3)},
    {"name": "nested_tuple", "data": ((1, 2), (3, 4))},
    {"name": "empty_object", "data": {}},
    {"name": "simple_object", "data": {"key": "value", "number": 123}},
    {"name": "nested_object", "data": {"outer": {"inner": {"value": 42}}}},
    {"name": "object_mixed_keys", "data": {"str": "text", "num": 42, "bool": True, "null": None}},
    {"name": "datetime", "data": datetime.now()},
    {"name": "date", "data": datetime.now().date()},
    {"name": "time", "data": datetime.now().time()},
    {"name": "timestamp", "data": time.time()},
    {"name": "enum", "data": ErrorCode.INTERNAL_ERROR},
    {"name": "enum_in_dict", "data": {"error": ErrorCode.TIMEOUT, "code": 408}},
    {
        "name": "complex",
        "data": {
            "id": "test-123",
            "method": "greet",
            "params": ["Python"],
            "nested": {"foo": "bar", "baz": [1, 2, 3]},
            "timestamp": datetime.now(),
            "metadata": {
                "version": 1.0,
                "features": ["streaming", "chunking"],
                "limits": {"max_size": 10485760, "timeout": 30000},
            },
        },
    },
    {"name": "binary", "data": b"Hello binary world"},
    {"name": "empty_binary", "data": b""},
    {"name": "binary_with_nulls", "data": b"\x00\x01\x02\x03\x04"},
    {"name": "unicode", "data": "你好世界 🌍"},
    {"name": "emoji", "data": "🎉🎊🎈🎁🎀"},
    {"name": "special_chars", "data": "äöü ñ é à ß"},
    {"name": "escape_chars", "data": "line1\nline2\ttab\r\nwindows"},
    {"name": "quotes", "data": 'He said "Hello" and she said \'Hi\''},
    {"name": "very_long_string", "data": "x" * 10000},
    {"name": "deeply_nested", "data": {"l1": {"l2": {"l3": {"l4": {"l5": {"value": "deep"}}}}}}},
    {"name": "large_array", "data": list(range(1000))},
    {"name": "max_safe_int", "data": 2**53 - 1},
    {"name": "min_safe_int", "data": -(2**53 - 1)},
    {
        "name": "rpc_message",
        "data": {
            "id": "1234567890-abcdef",
            "method": "test.method",
            "params": [1, "two", {"three": 3}],
            "error": None,
        },
    },
    {
        "name": "stream_message",
        "data": {"id": "stream-123", "type": "data", "data": {"chunk": 1, "total": 10}},
    },
    {"name": "js_timestamp", "data": 1708786800000},
    {"name": "date_ext", "data": datetime(2024, 2, 24, 12, 0, 0, tzinfo=timezone.utc)},
    {
        "name": "camera_config",
        "data": {
            "cameraId": "cam-abc-123",
            "fps": 30,
            "eventTimeout": 30,
            "timestamp": 1708786800000,
            "confidence": 0.85,
            "enabled": True,
            "name": "Front Door",
        },
    },
    {
        "name": "detection_event",
        "data": {
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
    },
    {"name": "map_with_nil", "data": {"key": "value", "optional": None, "count": 0, "flag": False}},
    {"name": "nested_ints", "data": {"a": {"b": {"c": 42}}, "d": [1, 2, 3], "e": 100}},
    {"name": "empty_nested", "data": {"a": {}, "b": [], "c": {"d": {}}}},
    {
        "name": "sensor_list",
        "data": [
            {"id": "sensor-1", "type": "motion", "online": True, "score": 0.95},
            {"id": "sensor-2", "type": "audio", "online": False, "score": 0.0},
            {"id": "sensor-3", "type": "object", "online": True, "score": 0.87},
        ],
    },
    {
        "name": "mixed_numerics",
        "data": {"integer": 42, "float_val": 3.14, "zero_int": 0, "zero_float": 0.0, "negative": -10, "neg_float": -2.5},
    },
]


def compare_values(expected: Any, actual: Any, test_name: str) -> bool:
    """Compare values with special handling for certain types."""
    if test_name == "float_nan":
        return isinstance(expected, float) and math.isnan(expected) and isinstance(actual, float) and math.isnan(actual)

    if isinstance(expected, datetime):
        if isinstance(actual, datetime):
            exp = expected.replace(tzinfo=timezone.utc) if expected.tzinfo is None else expected
            act = actual.replace(tzinfo=timezone.utc) if actual.tzinfo is None else actual
            return abs((exp - act).total_seconds()) < 1
        if isinstance(actual, str):
            return expected.isoformat().split("+")[0].split("Z")[0] == actual.split("+")[0].split("Z")[0]
        return str(expected) == str(actual)

    if hasattr(expected, "strftime") and not hasattr(expected, "hour"):
        return str(expected) == str(actual)

    if test_name == "time" and hasattr(expected, "strftime") and hasattr(expected, "hour"):
        expected_str = expected.strftime("%H:%M:%S")
        if isinstance(actual, str):
            return expected_str == actual.split(".")[0]
        return False

    if isinstance(expected, tuple):

        def tuple_to_list(obj: Any) -> Any:
            if isinstance(obj, tuple):
                return [tuple_to_list(item) for item in obj]
            return obj

        return tuple_to_list(expected) == actual

    if hasattr(expected, "value"):
        return expected.value == actual

    if isinstance(expected, (bytes, bytearray)):
        return isinstance(actual, (bytes, bytearray)) and bytes(expected) == bytes(actual)

    if test_name == "timestamp" and isinstance(expected, float) and isinstance(actual, float):
        return abs(expected - actual) < 2

    if test_name == "float_zero":
        return actual == 0 or actual == 0.0

    if test_name == "complex" and isinstance(expected, dict) and isinstance(actual, dict):
        exp_copy = expected.copy()
        act_copy = actual.copy()
        exp_copy.pop("timestamp", None)
        act_copy.pop("timestamp", None)
        if "metadata" in exp_copy and "metadata" in act_copy:
            exp_meta = exp_copy["metadata"].copy()
            act_meta = act_copy["metadata"].copy()
            exp_meta.pop("version", None)
            act_meta.pop("version", None)
            if exp_meta != act_meta:
                return False
            exp_copy.pop("metadata")
            act_copy.pop("metadata")
        return exp_copy == act_copy

    return expected == actual


print("Python codec test")
print("=" * 60)
print()
print("Phase 1: Encoding + roundtrip...")

passed = 0
failed = 0

for test in test_cases:
    try:
        encoded = encode(test["data"])
        decoded = decode(encoded)
        success = compare_values(test["data"], decoded, test["name"])

        if success:
            print(f"  OK {test['name']} ({len(encoded)} bytes)")  # type: ignore
            passed += 1
        else:
            print(f"  FAIL {test['name']}: roundtrip mismatch")
            print(f"    Expected: {str(test['data'])[:100]}")
            print(f"    Got:      {str(decoded)[:100]}")
            failed += 1

        with open(f"/tmp/py-encoded-{test['name']}.msgpack", "wb") as f:
            f.write(encoded)  # type: ignore

    except Exception as e:
        print(f"  FAIL {test['name']}: {e}")
        failed += 1

print()
print(f"Results: {passed} passed, {failed} failed")

if failed > 0:
    sys.exit(1)
