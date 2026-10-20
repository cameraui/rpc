from importlib.metadata import PackageNotFoundError, version

from nats.micro.service import ServiceConfig, ServiceInfo, ServiceStats

from .channel import Channel, PrivateChannel
from .client import create_rpc_client
from .decorators import RPCClass, RPCMethod, RPCNested, RPCProperty
from .errors import RPCException, RPCTimeoutError
from .service import RPCService, Service
from .types import (
    CallbackInvocation,
    CloseHandler,
    ErrorCode,
    NoResponderRetryOptions,
    OrphanProbeOptions,
    ProxyWithClose,
    PullCallbackParams,
    RPCClient,
    RPCClientOptions,
    RPCError,
)
from .utils import RPCCallbacks, rpc_callbacks

__all__ = [
    "create_rpc_client",
    "Channel",
    "PrivateChannel",
    "RPCException",
    "RPCTimeoutError",
    "ErrorCode",
    "RPCService",
    "Service",
    "RPCClass",
    "RPCMethod",
    "RPCNested",
    "RPCProperty",
    "rpc_callbacks",
    "RPCCallbacks",
    "RPCClient",
    "RPCClientOptions",
    "NoResponderRetryOptions",
    "OrphanProbeOptions",
    "RPCError",
    "ProxyWithClose",
    "CloseHandler",
    "PullCallbackParams",
    "CallbackInvocation",
    "ServiceConfig",
    "ServiceInfo",
    "ServiceStats",
]

try:
    __version__ = version("camera-ui-rpc")
except PackageNotFoundError:
    __version__ = "0.0.0"
