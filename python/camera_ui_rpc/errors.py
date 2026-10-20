from typing import Any

from nats import errors as nats_errors

from .types import ErrorCode, RPCError


class RPCException(Exception):
    def __init__(self, code: str, message: str, data: Any | None = None):
        super().__init__(message)
        self.code: str = code
        self.message: str = message
        self.data: Any | None = data

    def to_dict(self) -> RPCError:
        return RPCError(code=self.code, message=self.message, data=self.data)

    @classmethod
    def from_dict(cls, error: RPCError) -> "RPCException":
        return cls(code=error["code"], message=error["message"], data=error.get("data"))

    def __str__(self) -> str:
        if self.data:
            return f"[{self.code}] {self.message} (data: {self.data})"
        return f"[{self.code}] {self.message}"

    def __repr__(self) -> str:
        return f"RPCException(code={self.code!r}, message={self.message!r}, data={self.data!r})"


class RPCTimeoutError(RPCException, nats_errors.TimeoutError): ...


class LocalNoRespondersError(RPCException):
    def __init__(self, code: str, message: str, data: Any | None = None, *, subject: str | None = None):
        super().__init__(code, message, data)
        self.subject: str | None = subject


def is_local_no_responders(e: BaseException) -> bool:
    return isinstance(e, (nats_errors.NoRespondersError, LocalNoRespondersError))


def reported_error(e: Exception) -> Exception:
    if not is_local_no_responders(e):
        return e
    subject = getattr(e, "subject", None)
    if subject is None and e.args and isinstance(e.args[0], str):
        # the client raises nats' NoRespondersError with the subject as its only argument
        subject = e.args[0]
    return RPCException(ErrorCode.INTERNAL_ERROR.value, "upstream unavailable", {"subject": subject})


def create_error(code: ErrorCode | str, message: str, data: Any | None = None) -> RPCException:
    if isinstance(code, ErrorCode):
        code = code.value
    return RPCException(code=code, message=message, data=data)


def create_error_from_dict(error_dict: dict[str, Any]) -> RPCException:
    return RPCException(
        error_dict.get("code", ErrorCode.INTERNAL_ERROR.value),
        error_dict.get("message", "Unknown error"),
        error_dict.get("data"),
    )
