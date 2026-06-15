"""Tests for RPC exception types and helpers."""

import pytest

from camera_ui_rpc.errors import (
    RPCException,
    create_error,
    create_error_from_dict,
)
from camera_ui_rpc.types import ErrorCode


def test_exception_stores_fields() -> None:
    exc = RPCException("MY_CODE", "something failed", {"detail": 1})
    assert exc.code == "MY_CODE"
    assert exc.message == "something failed"
    assert exc.data == {"detail": 1}


def test_exception_is_exception_subclass() -> None:
    exc = RPCException("C", "m")
    assert isinstance(exc, Exception)
    assert str(exc.args[0]) == "m"


def test_exception_default_data_is_none() -> None:
    exc = RPCException("C", "m")
    assert exc.data is None


def test_to_dict_roundtrip() -> None:
    exc = RPCException("CODE", "msg", {"k": "v"})
    error = exc.to_dict()
    assert error["code"] == "CODE"
    assert error["message"] == "msg"
    assert error["data"] == {"k": "v"}


def test_from_dict_reconstructs_exception() -> None:
    error = {"code": "CODE", "message": "msg", "data": [1, 2]}
    exc = RPCException.from_dict(error)  # type: ignore[arg-type]
    assert exc.code == "CODE"
    assert exc.message == "msg"
    assert exc.data == [1, 2]


def test_from_dict_without_data() -> None:
    error = {"code": "CODE", "message": "msg"}
    exc = RPCException.from_dict(error)  # type: ignore[arg-type]
    assert exc.data is None


def test_str_includes_code_and_message() -> None:
    exc = RPCException("CODE", "msg")
    assert str(exc) == "[CODE] msg"


def test_str_includes_data_when_present() -> None:
    exc = RPCException("CODE", "msg", {"x": 1})
    rendered = str(exc)
    assert "[CODE] msg" in rendered
    assert "data:" in rendered


def test_repr_is_developer_friendly() -> None:
    exc = RPCException("CODE", "msg", None)
    assert repr(exc) == "RPCException(code='CODE', message='msg', data=None)"


def test_create_error_with_string_code() -> None:
    exc = create_error("CUSTOM", "boom")
    assert isinstance(exc, RPCException)
    assert exc.code == "CUSTOM"
    assert exc.message == "boom"


def test_create_error_with_enum_code_uses_value() -> None:
    exc = create_error(ErrorCode.TIMEOUT, "timed out")
    assert exc.code == "TIMEOUT"
    assert exc.code == ErrorCode.TIMEOUT.value


def test_create_error_passes_data() -> None:
    exc = create_error(ErrorCode.INVALID_PARAMS, "bad", {"field": "name"})
    assert exc.data == {"field": "name"}


def test_create_error_from_dict_full() -> None:
    exc = create_error_from_dict({"code": "X", "message": "y", "data": 7})
    assert exc.code == "X"
    assert exc.message == "y"
    assert exc.data == 7


def test_create_error_from_dict_defaults() -> None:
    exc = create_error_from_dict({})
    assert exc.code == ErrorCode.INTERNAL_ERROR.value
    assert exc.message == "Unknown error"
    assert exc.data is None


@pytest.mark.parametrize(
    "member",
    [
        ErrorCode.METHOD_NOT_FOUND,
        ErrorCode.INVALID_PARAMS,
        ErrorCode.INTERNAL_ERROR,
        ErrorCode.TIMEOUT,
        ErrorCode.CONNECTION_CLOSED,
        ErrorCode.STREAM_ERROR,
        ErrorCode.PAYLOAD_TOO_LARGE,
        ErrorCode.NOT_FOUND,
    ],
)
def test_error_code_value_equals_name(member: ErrorCode) -> None:
    assert member.value == member.name


def test_error_code_is_str_enum() -> None:
    assert isinstance(ErrorCode.TIMEOUT, str)
    assert ErrorCode.TIMEOUT == "TIMEOUT"
