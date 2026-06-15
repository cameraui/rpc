"""Camera UI RPC library for Python."""

from importlib.metadata import PackageNotFoundError, version

# NATS service types
from nats.micro.service import ServiceConfig, ServiceInfo, ServiceStats

# Channel/PrivateChannel for bidirectional communication
from .channel import Channel, PrivateChannel

# Main client
from .client import create_rpc_client

# Decorators
from .decorators import RPCClass, RPCMethod, RPCNested, RPCProperty

# Error handling
from .errors import RPCException

# Service support
from .service import RPCService, Service

# Core types that users need
from .types import (
    CallbackInvocation,
    CloseHandler,
    ErrorCode,
    NoResponderRetryOptions,
    ProxyWithClose,  # For isolated connections
    PullCallbackParams,
    RPCClient,
    RPCClientOptions,
    RPCError,
)

# Pull-iterator-with-callbacks helper
from .utils import RPCCallbacks, rpc_callbacks

__all__ = [
    # Main client
    "create_rpc_client",
    # Channels
    "Channel",
    "PrivateChannel",
    # Errors
    "RPCException",
    "ErrorCode",
    # Service
    "RPCService",
    "Service",
    # Decorators
    "RPCClass",
    "RPCMethod",
    "RPCNested",
    "RPCProperty",
    # Pull-callback
    "rpc_callbacks",
    "RPCCallbacks",
    # Types
    "RPCClient",
    "RPCClientOptions",
    "NoResponderRetryOptions",
    "RPCError",
    "ProxyWithClose",
    "CloseHandler",
    "PullCallbackParams",
    "CallbackInvocation",
    # NATS service types
    "ServiceConfig",
    "ServiceInfo",
    "ServiceStats",
]

try:
    __version__ = version("camera-ui-rpc")
except PackageNotFoundError:
    __version__ = "0.0.0"
