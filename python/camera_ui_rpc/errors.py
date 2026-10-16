"""RPC exception classes."""

from typing import Any

from nats import errors as nats_errors

from .types import ErrorCode, RPCError


class RPCException(Exception):
    """Base RPC exception class."""

    def __init__(self, code: str, message: str, data: Any | None = None):
        """
        Initialize RPC exception.

        Args:
            code: Error code
            message: Error message
            data: Optional additional error data
        """
        super().__init__(message)
        self.code: str = code
        self.message: str = message
        self.data: Any | None = data

    def to_dict(self) -> RPCError:
        """Convert exception to RPC error format."""
        return RPCError(code=self.code, message=self.message, data=self.data)

    @classmethod
    def from_dict(cls, error: RPCError) -> "RPCException":
        """Create exception from RPC error format."""
        return cls(code=error["code"], message=error["message"], data=error.get("data"))

    def __str__(self) -> str:
        """String representation."""
        if self.data:
            return f"[{self.code}] {self.message} (data: {self.data})"
        return f"[{self.code}] {self.message}"

    def __repr__(self) -> str:
        """Developer representation."""
        return f"RPCException(code={self.code!r}, message={self.message!r}, data={self.data!r})"


class RPCTimeoutError(RPCException, nats_errors.TimeoutError):
    """A call or request that got no reply within its timeout.

    Code TIMEOUT, the message names the subject and the timeout. It is also a
    nats.errors.TimeoutError and a builtin TimeoutError, so except clauses
    written for the error the client raised before keep matching.
    """


class LocalNoRespondersError(RPCException):
    """A no-responder status this client saw itself, as opposed to an error a responder sent back.

    Code and message are what callers saw before; ``subject`` names the
    subject nobody served.
    """

    def __init__(self, code: str, message: str, data: Any | None = None, *, subject: str | None = None):
        super().__init__(code, message, data)
        self.subject: str | None = subject


def is_local_no_responders(e: BaseException) -> bool:
    """Whether ``e`` is a no-responder this client saw itself, never one decoded from a reply."""
    return isinstance(e, (nats_errors.NoRespondersError, LocalNoRespondersError))


def reported_error(e: Exception) -> Exception:
    """The error a responder puts on the wire for ``e``.

    A no-responder that a handler's own call ran into becomes INTERNAL_ERROR
    'upstream unavailable' with the subject in ``data``: for the handler's
    caller it is a failed call, not a missing responder its retry could find.
    Everything else is reported as it is.
    """
    if not is_local_no_responders(e):
        return e
    subject = getattr(e, "subject", None)
    if subject is None and e.args and isinstance(e.args[0], str):
        # the client raises nats' NoRespondersError with the subject as its only argument
        subject = e.args[0]
    return RPCException(ErrorCode.INTERNAL_ERROR.value, "upstream unavailable", {"subject": subject})


def create_error(code: ErrorCode | str, message: str, data: Any | None = None) -> RPCException:
    """
    Create an RPC exception with the given code and message.

    Args:
        code: Error code (from ErrorCode enum or custom string)
        message: Error message
        data: Optional additional error data

    Returns:
        RPCException instance
    """
    if isinstance(code, ErrorCode):
        code = code.value
    return RPCException(code=code, message=message, data=data)


def create_error_from_dict(error_dict: dict[str, Any]) -> RPCException:
    """
    Create an RPC exception from a dictionary.

    Args:
        error_dict: Error dictionary with code, message, and optional data

    Returns:
        RPCException instance
    """
    return RPCException(
        error_dict.get("code", ErrorCode.INTERNAL_ERROR.value),
        error_dict.get("message", "Unknown error"),
        error_dict.get("data"),
    )
