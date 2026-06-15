"""RPC exception classes."""

from typing import Any

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
