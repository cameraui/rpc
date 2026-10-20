import inspect
import secrets
import time
from collections.abc import AsyncGenerator, Collection
from random import choices
from typing import TYPE_CHECKING, Any, TypeVar, cast

from nats.micro.service import ServiceInfo

if TYPE_CHECKING:
    from .client import RPCClient
    from .types import NoResponderRetryOptions

T = TypeVar("T")

DIRECT_SUBJECT_METADATA = "cui.chunk.direct"


def direct_subject(instance_id: str, path: str) -> str:
    return f"$CUI.SVC.{instance_id}.{path}"


def service_subject(service_name: str, path: str) -> str:
    return f"svc.{service_name}.{path}"


_ID_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"


def generate_id(reply_prefix: str | None = None) -> str:
    request_id = f"{int(time.time() * 1000)}-{''.join(choices(_ID_CHARS, k=9))}"
    return f"{reply_prefix}.{request_id}" if reply_prefix else request_id


def generate_reply_prefix() -> str:
    value = secrets.randbits(48)
    chars: list[str] = []
    while value:
        value, rem = divmod(value, 36)
        chars.append(_ID_CHARS[rem])
    return "".join(reversed(chars)).rjust(10, "0")


class RPCCallbacks:
    def __init__(self, methods: dict[str, Any], oneway: list[str]) -> None:
        self.methods = methods
        self.oneway = oneway


def rpc_callbacks(**kwargs: Any) -> RPCCallbacks:
    oneway = kwargs.pop("oneway", None)
    methods: dict[str, Any] = {}
    for name, val in kwargs.items():
        if callable(val):
            methods[name] = val
    if oneway is None:
        oneway = list(methods.keys())
    return RPCCallbacks(methods, list(oneway))


def is_rpc_callbacks(v: Any) -> bool:
    return isinstance(v, RPCCallbacks)


def is_generator(func: Any) -> bool:
    if not callable(func):
        return False

    return is_sync_generator(func) or is_async_generator(func)


def is_sync_generator(func: Any) -> bool:
    if not callable(func):
        return False

    return inspect.isgeneratorfunction(func) or inspect.isgenerator(func)


def is_async_generator(func: Any) -> bool:
    if not callable(func):
        return False

    if inspect.isasyncgenfunction(func):
        return True

    if hasattr(func, "__annotations__"):
        return_annotation = func.__annotations__.get("return")
        if return_annotation:
            origin = getattr(return_annotation, "__origin__", None)
            if origin is AsyncGenerator:
                return True

    return False


def is_async_function(func: Any) -> bool:
    if not callable(func):
        return False

    return inspect.iscoroutinefunction(func) or inspect.iscoroutine(func) or inspect.isawaitable(func)


def method_roots(methods: Collection[str]) -> set[str]:
    return {name.split(".", 1)[0] for name in methods if isinstance(name, str)}


_PLAIN_CALL = object()
_MODE_CALL, _MODE_STREAM, _MODE_PULL = 0, 1, 2


def _method_mode(name: str) -> int:
    lowered = name.lower()
    if "generate" in lowered:
        return _MODE_STREAM
    if "pull" in lowered:
        return _MODE_PULL
    return _MODE_CALL


def _route_call(
    client: "RPCClient",
    subject: str,
    mode: int,
    args: tuple[Any, ...],
    no_responder_retry: "NoResponderRetryOptions | None" = None,
    namespace: str | None = None,
    method_cache: list[Collection[str] | None] | None = None,
) -> Any:
    if args:
        for i, arg in enumerate(args):
            if is_rpc_callbacks(arg):
                cbs = cast(RPCCallbacks, arg)
                others = [a for j, a in enumerate(args) if j != i]
                return client.call_pull_iterator_with_callback(subject, cbs.methods, cbs.oneway, *others)
        for i, arg in enumerate(args):
            if callable(arg) and not isinstance(arg, (str, bytes, type)):
                others = [a for j, a in enumerate(args) if j != i]
                return client.call_with_callback(subject, others, arg)
    if mode == _MODE_STREAM:
        if namespace is None or method_cache is None:
            return client.call_stream(subject, *args, no_responder_retry=no_responder_retry)
        return client._call_stream_with_probe(  # pyright: ignore[reportPrivateUsage]
            subject,
            args,
            no_responder_retry=no_responder_retry,
            namespace=namespace,
            method_cache=method_cache,
        )
    if mode == _MODE_PULL:
        return client.call_pull_iterator(subject, *args)
    return _PLAIN_CALL


class CallableProxy:
    __slots__ = (
        "_cache",
        "_children",
        "_client",
        "_method",
        "_mode",
        "_namespace",
        "_no_responder_retry",
        "_subject",
        "_timeout",
    )

    def __init__(
        self,
        client: "RPCClient",
        namespace: str,
        method: str,
        cache: list[Collection[str] | None],
        no_responder_retry: "NoResponderRetryOptions | None",
        timeout: int | None,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._method = method
        self._subject = f"rpc.{namespace}.{method}"
        self._mode = _method_mode(method.rsplit(".", 1)[-1])
        # one cell for the whole proxy tree, the first response of any of its methods fills it
        self._cache = cache
        self._no_responder_retry = no_responder_retry
        self._timeout = timeout
        self._children: dict[str, CallableProxy] | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        routed = _route_call(
            self._client,
            self._subject,
            self._mode,
            args,
            self._no_responder_retry,
            self._namespace,
            self._cache,
        )
        if routed is not _PLAIN_CALL:
            return routed
        return self._call(args)

    def __getattr__(self, name: str) -> Any:
        # copy, pickle, inspect and hasattr probes must not turn into remote methods
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        children = self._children
        if children is None:
            children = self._children = {}
        child = children.get(name)
        if child is None:
            child = children[name] = CallableProxy(
                self._client,
                self._namespace,
                f"{self._method}.{name}",
                self._cache,
                self._no_responder_retry,
                self._timeout,
            )
        return child

    def __await__(self) -> Any:
        return self().__await__()

    async def _call(self, args: tuple[Any, ...]) -> Any:
        cache = self._cache
        # discovery is requested only while the cache is empty, older responders send __methods every time
        result, methods = await self._client._call_with_meta(  # pyright: ignore[reportPrivateUsage]
            self._subject,
            args,
            discover=cache[0] is None,
            no_responder_retry=self._no_responder_retry,
            timeout=self._timeout,
            namespace=self._namespace,
            method_cache=cache,
        )
        if cache[0] is None and isinstance(methods, list):
            cache[0] = methods
        return result


class RPCProxy:
    def __init__(
        self,
        client: "RPCClient",
        namespace: str,
        path: list[str],
        method_cache: set[str] | None,
        no_responder_retry: "NoResponderRetryOptions | None",
        timeout: int | None,
    ) -> None:
        self.__client = client
        self.__namespace = namespace
        self.__path = path
        self.__prefix = "".join(f"{part}." for part in path)
        # [method names, their first segments], the second is built on first use
        self.__cache: list[Collection[str] | None] = [method_cache, None]
        self.__callables: dict[str, CallableProxy] = {}
        self.__no_responder_retry = no_responder_retry
        self.__timeout = timeout

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_RPCProxy__"):
            # copy/pickle build instances without __init__
            raise AttributeError(name)
        cache = self.__cache
        methods = cache[0]
        if methods is not None and not self.__path:
            roots = cache[1]
            if roots is None:
                roots = cache[1] = method_roots(methods)
            if name not in roots:
                return None
        method = self.__callables.get(name)
        if method is None:
            method = CallableProxy(
                self.__client,
                self.__namespace,
                self.__prefix + name,
                self.__cache,
                self.__no_responder_retry,
                self.__timeout,
            )
            self.__callables[name] = method
        return method

    def __repr__(self) -> str:
        path_str = ".".join(self.__path) if self.__path else ""
        return f"<RPCProxy {self.__namespace}{('.' + path_str) if path_str else ''}>"


def create_proxy(
    client: "RPCClient",
    namespace: str,
    path: list[str] | None = None,
    method_cache: set[str] | None = None,
    *,
    no_responder_retry: "NoResponderRetryOptions | None" = None,
    timeout: int | None = None,
) -> Any:
    return RPCProxy(client, namespace, list(path) if path else [], method_cache, no_responder_retry, timeout)


class ServiceCallableProxy:
    __slots__ = ("_client", "_direct", "_mode", "_subject")

    def __init__(self, client: "RPCClient", subject: str, name: str, direct: str | None = None) -> None:
        self._client = client
        self._subject = subject
        self._direct = direct
        self._mode = _method_mode(name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        routed = _route_call(self._client, self._subject, self._mode, args)
        if routed is not _PLAIN_CALL:
            return routed
        if self._direct is None:
            return self._client.call(self._subject, *args)
        return self._client._call_service(self._subject, args, self._direct)  # pyright: ignore[reportPrivateUsage]


class ServiceProxy:
    def __init__(
        self,
        client: "RPCClient",
        service_info: ServiceInfo,
        timeout: int | None,
        path: list[str],
        index: tuple[dict[str, Any], frozenset[str]],
    ) -> None:
        self.__client = client
        self.__service_info = service_info
        self.__timeout = timeout
        self.__path = path
        self.__base = ".".join(path)
        self.__index = index
        self.__children: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_ServiceProxy__"):
            # copy/pickle build instances without __init__
            raise AttributeError(name)
        child = self.__children.get(name)
        if child is not None:
            return child

        full = f"{self.__base}.{name}" if self.__path else name
        by_subject, prefixes = self.__index
        info = self.__service_info
        # the subject of this service alone first, the bare one is shared by every service with the method
        endpoint = by_subject.get(service_subject(info.name, full)) or by_subject.get(full)
        if endpoint is not None:
            direct = (
                direct_subject(info.id, full)
                if (info.metadata or {}).get(DIRECT_SUBJECT_METADATA) == "1"
                else None
            )
            child = ServiceCallableProxy(self.__client, endpoint.subject, name, direct)
        elif full in prefixes:
            child = ServiceProxy(
                self.__client, self.__service_info, self.__timeout, [*self.__path, name], self.__index
            )
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        self.__children[name] = child
        return child

    def __repr__(self) -> str:
        path_str = ".".join(self.__path) if self.__path else ""
        return f"<ServiceProxy {self.__service_info.name}{('.' + path_str) if path_str else ''}>"


def _endpoint_index(service_info: ServiceInfo) -> tuple[dict[str, Any], frozenset[str]]:
    by_subject: dict[str, Any] = {}
    prefixes: set[str] = set()
    own = service_subject(service_info.name, "")
    for ep in service_info.endpoints:
        by_subject.setdefault(ep.subject, ep)
        path = ep.subject.removeprefix(own)
        prefixes.update(path[:i] for i, char in enumerate(path) if char == ".")
    return by_subject, frozenset(prefixes)


def create_service_proxy(
    client: "RPCClient", service_info: ServiceInfo, timeout: int | None = None, path: list[str] | None = None
) -> Any:
    return ServiceProxy(
        client, service_info, timeout, list(path) if path else [], _endpoint_index(service_info)
    )
