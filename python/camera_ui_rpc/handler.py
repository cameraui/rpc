import asyncio
import contextlib
import contextvars
import inspect
import threading
import time
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Protocol, cast

from .errors import RPCException, create_error, reported_error
from .types import (
    CallbackInvocation,
    CallbackMessage,
    ErrorCode,
    PullIteratorRequest,
    PullIteratorResponse,
    RPCClient,
    RPCError,
    RPCResponse,
    StreamMessage,
)
from .utils import is_async_function, is_async_generator, is_sync_generator

STREAM_SUBJECT_PREFIX = "stream."
CALLBACK_SUBJECT_PREFIX = "rpc.cb."
PULL_CALLBACK_SUBJECT_PREFIX = "_rpc.cb."

# a producer that never waits would keep cancel messages unread, publish loops yield after this long
_YIELD_INTERVAL = 0.002

_background: set[asyncio.Future[Any]] = set()


def _eager_tasks_supported() -> bool:
    try:
        return "eager_start" in inspect.signature(asyncio.Task).parameters
    except (TypeError, ValueError):
        return False


_EAGER_STEPS = _eager_tasks_supported()
# without eager tasks (3.11) a step runs up to its first await by hand, as the current task
_enter_task: Callable[[asyncio.AbstractEventLoop, asyncio.Task[Any]], None] | None = getattr(
    asyncio.tasks, "_enter_task", None
)
_leave_task: Callable[[asyncio.AbstractEventLoop, asyncio.Task[Any]], None] | None = getattr(
    asyncio.tasks, "_leave_task", None
)


class SessionProbe(Protocol):
    sent: bool
    due: bool

    def start(
        self, ping: Callable[[str], Coroutine[Any, Any, None]], on_gone: Callable[[], None]
    ) -> None: ...

    def reply(self) -> str: ...

    def touch(self) -> None: ...

    def stop(self) -> None: ...


def run_in_background(work: Awaitable[Any]) -> asyncio.Future[Any]:
    future = asyncio.ensure_future(work)
    _background.add(future)
    future.add_done_callback(_background.discard)
    return future


class _EagerStep(Coroutine[Any, Any, None]):
    def __init__(self, coro: Coroutine[Any, Any, None]) -> None:
        self._coro = coro
        self._waiting: Any = None
        self._error: BaseException | None = None
        self._handed = False
        self.finished = False

    def run(self) -> None:
        try:
            self._waiting = self._coro.send(None)
        except StopIteration:
            self.finished = True
        except BaseException as e:
            self._error = e

    def send(self, value: Any) -> Any:
        if self._handed:
            return self._coro.send(value)
        self._handed = True
        return self._first_yield()

    def throw(self, typ: Any, val: Any = None, tb: Any = None, /) -> Any:
        if not self._handed:
            self._handed = True
            if self.finished or self._error is not None:
                return self._first_yield()
            # only a cancel reaches a task before its first step: cancel what the step waits on and keep waiting
            if isinstance(self._waiting, asyncio.Future) and self._waiting.cancel():
                return self._waiting
        return self._coro.throw(typ) if val is None and tb is None else self._coro.throw(typ, val, tb)

    def close(self) -> None:
        self._coro.close()

    def __await__(self) -> Any:
        return self

    def __next__(self) -> Any:
        return self.send(None)

    def _first_yield(self) -> Any:
        if self._error is not None:
            raise self._error
        if self.finished:
            raise StopIteration
        return self._waiting


def _start_step(step: Coroutine[Any, Any, None]) -> asyncio.Task[None] | None:
    loop = asyncio.get_running_loop()
    # eager: a producer with its value ready answers without an extra loop turn
    if _EAGER_STEPS:
        return asyncio.Task(step, loop=loop, eager_start=True)  # type: ignore[call-arg]
    if _enter_task is None or _leave_task is None:
        return loop.create_task(step)
    return _start_step_by_hand(loop, step, _enter_task, _leave_task)


def _start_step_by_hand(
    loop: asyncio.AbstractEventLoop,
    step: Coroutine[Any, Any, None],
    enter: Callable[[asyncio.AbstractEventLoop, asyncio.Task[Any]], None],
    leave: Callable[[asyncio.AbstractEventLoop, asyncio.Task[Any]], None],
) -> asyncio.Task[None] | None:
    eager = _EagerStep(step)
    context = contextvars.copy_context()
    task = loop.create_task(eager, context=context)
    # the producer must see its own task, asyncio.timeout() cancels the current one
    caller = asyncio.current_task(loop)
    if caller is not None:
        leave(loop, caller)
    enter(loop, task)
    try:
        context.run(eager.run)
    finally:
        leave(loop, task)
        if caller is not None:
            enter(loop, caller)
    return None if eager.finished else task


async def _subscribe_session(
    client: RPCClient, subject: str, handler: Callable[[Any], Coroutine[Any, Any, None]]
) -> Callable[[], Coroutine[Any, Any, None]]:
    # the caller's first message here follows our reply on the same connection, the SUB goes out with it
    subscribe = getattr(client, "_subscribe_session", None)
    if subscribe is None:
        return await client.subscribe(subject, handler)
    return cast(Callable[[], Coroutine[Any, Any, None]], await subscribe(subject, handler))


def _connection_closed() -> RPCError:
    return create_error(ErrorCode.CONNECTION_CLOSED, "Connection closed").to_dict()


class _YieldGuard:
    def __init__(self) -> None:
        self._since = time.monotonic()
        self._ran = True

    def due(self) -> bool:
        now = time.monotonic()
        if self._ran:
            # the loop ran since the last check, so it read what had arrived
            self._ran = False
            self._since = now
            asyncio.get_running_loop().call_soon(self._mark)
            return False
        return now - self._since >= _YIELD_INTERVAL

    def _mark(self) -> None:
        self._ran = True


async def _stream_generator(
    handler: Callable[..., Any], args: list[Any], io_pool: ThreadPoolExecutor
) -> AsyncGenerator[Any, None]:
    generator: AsyncGenerator[Any, None]

    if is_async_generator(handler):
        generator = handler(*args)
    elif is_sync_generator(handler):
        sync_gen = handler(*args)

        async def async_wrapper() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper()
    elif is_async_function(handler):
        result = await handler(*args)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for stream")
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args)
        result = await loop.run_in_executor(io_pool, func)

        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for stream")

    if not hasattr(generator, "__aiter__"):
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    return generator


class _StreamSession:
    def __init__(
        self,
        handler: Callable[..., Any],
        args: list[Any],
        stream_subject: str,
        request_id: str,
        client: RPCClient,
        io_pool: ThreadPoolExecutor,
        on_finished: Callable[[], Any] | None,
        probe: SessionProbe | None = None,
    ) -> None:
        self._handler = handler
        self._args = args
        self._subject = stream_subject
        self._id = request_id
        self._client = client
        self._io_pool = io_pool
        self._on_finished = on_finished
        self._probe = probe
        self._stopped = False
        self._ended = False
        self._producing = False
        self._cancel_unsub: Callable[[], Coroutine[Any, Any, None]] | None = None
        self.task: asyncio.Task[None] = asyncio.get_running_loop().create_task(self._run())

    async def close(self) -> None:
        notify = not self._stopped and not self._ended
        self._stop()
        if notify and self._client.is_connected:
            self._ended = True
            message: StreamMessage = {"id": self._id, "type": "error", "error": _connection_closed()}
            with contextlib.suppress(Exception):
                await self._client.publish(self._subject, message)

    def _stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        # never the caller's task: on the service path that is the nats-py subscription task
        if self._producing and not self.task.done():
            self.task.cancel()

    async def _on_cancel(self, _msg: Any) -> None:
        self._stop()

    async def _ping(self, reply: str) -> None:
        message: StreamMessage = {"id": self._id, "type": "ping"}
        with contextlib.suppress(Exception):
            await self._client.publish(self._subject, message, reply=reply)

    async def _run(self) -> None:
        generator: AsyncGenerator[Any, None] | None = None
        probe = self._probe
        try:
            self._cancel_unsub = await self._client.subscribe(f"{self._subject}.cancel", self._on_cancel)
            if self._stopped:
                return
            self._producing = True
            if probe is not None:
                probe.start(self._ping, self._stop)
            generator = await _stream_generator(self._handler, self._args, self._io_pool)
            guard = _YieldGuard()
            async for value in generator:
                if self._stopped or not self._client.is_connected:
                    return
                data: StreamMessage = {"id": self._id, "type": "data", "data": value}
                if probe is None:
                    await self._client.publish(self._subject, data)
                else:
                    probe.sent = True
                    await self._client.publish(
                        self._subject, data, reply=probe.reply() if probe.due else None
                    )
                if guard.due():
                    await asyncio.sleep(0)
            await self._end({"id": self._id, "type": "end"})
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._end({"id": self._id, "type": "error", "error": format_error_dict(e)})
        finally:
            self._producing = False
            if probe is not None:
                probe.stop()
            if generator is not None:
                with contextlib.suppress(Exception):
                    await generator.aclose()
            if self._on_finished is not None:
                with contextlib.suppress(Exception):
                    self._on_finished()
            if self._cancel_unsub is not None:
                with contextlib.suppress(Exception):
                    await self._cancel_unsub()

    async def _end(self, message: StreamMessage) -> None:
        if self._stopped or self._ended or not self._client.is_connected:
            return
        self._ended = True
        with contextlib.suppress(Exception):
            await self._client.publish(self._subject, message)


def start_stream_request(
    handler: Callable[..., Any],
    args: list[Any],
    stream_subject: str,
    request_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
    on_finished: Callable[[], Any] | None = None,
    probe: SessionProbe | None = None,
) -> tuple[Callable[[], Coroutine[Any, Any, None]], asyncio.Task[None]]:
    _check_control_subject(stream_subject, STREAM_SUBJECT_PREFIX)
    session = _StreamSession(handler, args, stream_subject, request_id, client, io_pool, on_finished, probe)
    return session.close, session.task


async def refuse_stream(client: RPCClient, request_id: str, stream_subject: Any) -> None:
    _check_control_subject(stream_subject, STREAM_SUBJECT_PREFIX)
    message: StreamMessage = {"id": request_id, "type": "error", "error": _connection_closed()}
    await client.publish(stream_subject, message)


async def handle_stream_request(
    handler: Callable[..., Any],
    args: list[Any],
    stream_subject: str,
    request_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
) -> None:
    _, task = start_stream_request(handler, args, stream_subject, request_id, client, io_pool)
    await asyncio.wait((task,))


async def handle_normal_rpc(
    handler: Callable[..., Any],
    params: Any,
    io_pool: ThreadPoolExecutor,
    handler_is_async: bool | None = None,
) -> Any:
    if not isinstance(params, list):
        params = [params] if params is not None else []

    if handler_is_async is None:
        handler_is_async = is_async_function(handler)

    if handler_is_async:
        return await handler(*params)
    return handler(*params)


def format_error_dict(e: Exception) -> RPCError:
    e = reported_error(e)
    if isinstance(e, RPCException):
        return e.to_dict()
    else:
        return {
            "code": ErrorCode.INTERNAL_ERROR.value,
            "message": str(e),
        }


class _PullSession:
    def __init__(
        self,
        iterator_id: str,
        client: RPCClient,
        invoker: "CallbackInvoker | None",
        on_finished: Callable[[], Any] | None,
        probe: SessionProbe | None = None,
    ) -> None:
        self._generator: AsyncGenerator[Any, None] | None = None
        self._id = iterator_id
        self._client = client
        self._invoker = invoker
        self._on_finished = on_finished
        self._probe = probe
        self._response_subject = f"_rpc.iterator.{iterator_id}.response"
        self._active = True
        self._finished = False
        self._step: asyncio.Task[None] | None = None
        # set by the consumer's first next, the eager first step answers it
        self._first: asyncio.Future[None] | None = None
        self._closer: asyncio.Future[Any] | None = None
        self._unsub: Callable[[], Coroutine[Any, Any, None]] | None = None
        # read while the handler set up, the session ends once it is set up
        self._cancelled = False
        self._failure: PullIteratorResponse | None = None

    def fail(self, error: Exception) -> None:
        if not self._active or self._failure is not None:
            return
        self._failure = {"id": self._id, "type": "error", "error": format_error_dict(error)}
        if self._invoker is not None:
            self._invoker.active = False

    async def listen(self) -> None:
        self._unsub = await _subscribe_session(
            self._client, f"_rpc.iterator.{self._id}.request", self._on_request
        )

    async def abandon(self) -> None:
        self._deactivate()
        unsub, self._unsub = self._unsub, None
        if unsub is not None:
            with contextlib.suppress(Exception):
                await asyncio.shield(unsub())

    async def start(self, generator: AsyncGenerator[Any, None]) -> Callable[[], Coroutine[Any, Any, None]]:
        self._generator = generator
        if self._cancelled:
            # the caller tracks the session once this returns, an end before that would leave its entry behind
            asyncio.get_running_loop().call_soon(lambda: run_in_background(self._cancel_after_setup()))
            return self.cleanup
        if self._probe is not None:
            self._probe.start(self._ping, self._orphaned)
        if self._invoker is not None:
            self._first = asyncio.get_running_loop().create_future()
            self._step = _start_step(self._next(None, self._first))
        return self.cleanup

    async def cleanup(self) -> None:
        notify = self._active
        self._deactivate()
        self._forget()
        mid_step = self._cancel_step()
        closer = self._close_generator()
        if notify and self._client.is_connected:
            error: PullIteratorResponse = {"id": self._id, "type": "error", "error": _connection_closed()}
            with contextlib.suppress(Exception):
                await self._client.publish(self._response_subject, error)
        if self._unsub is not None:
            await self._unsub()
        if not mid_step:
            await asyncio.shield(closer)

    async def _on_request(self, msg: PullIteratorRequest) -> None:
        if not self._active:
            return
        if self._probe is not None:
            self._probe.touch()
        kind = msg.get("type")
        if self._generator is None:
            if kind == "cancel":
                self._cancelled = True
            return
        if kind == "next":
            first = self._first
            if first is not None:
                self._first = None
                first.set_result(None)
                return
            self._step = _start_step(self._next(self._step))
        elif kind == "cancel":
            await self._cancel()

    async def _next(
        self, previous: asyncio.Task[None] | None, asked: asyncio.Future[None] | None = None
    ) -> None:
        try:
            if previous is not None and not previous.done():
                # awaited directly, so cancelling the newest step reaches the one running the producer
                await previous
            if not self._active:
                return
            response: PullIteratorResponse
            generator = self._generator
            if generator is None:
                return
            # a frame that failed between steps leaves the producer alone
            if self._failure is None:
                try:
                    value = await generator.__anext__()
                except StopAsyncIteration:
                    response = {"id": self._id, "type": "done"}
                except Exception as e:
                    response = {"id": self._id, "type": "error", "error": format_error_dict(e)}
                else:
                    response = {"id": self._id, "type": "value"}
                    # a pull-callback batch boundary carries no value
                    if self._invoker is None:
                        response["value"] = value
            if self._failure is not None:
                response = self._failure
                # a producer that caught the error sits at a yield
                self._close_generator()
            if asked is not None:
                # the consumer subscribes its responses after the session started, so it hears nothing before it asks
                await asked
            if response["type"] != "value":
                await self._end(response)
            elif self._active:
                await self._client.publish(self._response_subject, response)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._end({"id": self._id, "type": "error", "error": format_error_dict(e)})

    async def _ping(self, reply: str) -> None:
        # a parked session publishes nothing, the consumer's own reply inbox ignores this and every consumer has one
        with contextlib.suppress(Exception):
            await self._client.publish(f"rpc.reply.{self._id}", {"type": "ping"}, reply=reply)

    def _orphaned(self) -> None:
        if self._active:
            run_in_background(self._cancel())

    async def _cancel_after_setup(self) -> None:
        if self._active:
            await self._cancel()

    async def _cancel(self) -> None:
        self._deactivate()
        self._cancel_step()
        done: PullIteratorResponse = {"id": self._id, "type": "done"}
        try:
            await self._client.publish(self._response_subject, done)
        finally:
            self._close_generator()
            self._finish()

    async def _end(self, response: PullIteratorResponse) -> None:
        if not self._active:
            return
        self._deactivate()
        try:
            with contextlib.suppress(Exception):
                await self._client.publish(self._response_subject, response)
        finally:
            self._finish()

    def _cancel_step(self) -> bool:
        step = self._step
        if step is None or step.done():
            return False
        step.cancel()
        return True

    def _close_generator(self) -> asyncio.Future[Any]:
        # a task of its own: an unsubscribe cancels the message task, which would cut the producer's cleanup short
        if self._closer is None:
            self._closer = run_in_background(self._aclose())
        return self._closer

    async def _aclose(self) -> None:
        step = self._step
        # aclose() raises on a running generator, the newest step settles after all older ones
        if step is not None and not step.done():
            await asyncio.wait((step,))
        if self._generator is None:
            return
        with contextlib.suppress(Exception):
            await self._generator.aclose()

    def _deactivate(self) -> None:
        self._active = False
        if self._invoker is not None:
            self._invoker.active = False
        if self._probe is not None:
            self._probe.stop()

    def _finish(self) -> None:
        # without this every finished session keeps its request subscription and cleanup until disconnect
        if self._forget() and self._unsub is not None:
            # unsubscribe() cancels the message task it is called from
            run_in_background(self._unsub())

    def _forget(self) -> bool:
        if self._finished:
            return False
        self._finished = True
        if self._on_finished is not None:
            with contextlib.suppress(Exception):
                self._on_finished()
        return True


async def set_up_session(
    setup: Awaitable[Callable[[], Coroutine[Any, Any, None]]],
    client: RPCClient,
    reply_subject: str,
    response: RPCResponse | dict[str, Any],
) -> Callable[[], Coroutine[Any, Any, None]]:
    try:
        return await setup
    except asyncio.CancelledError:
        if client.is_connected:
            response["error"] = _connection_closed()
            with contextlib.suppress(Exception):
                await client.publish(reply_subject, response)
        raise


async def _serve_pull(
    session: _PullSession, open_generator: Callable[[], Awaitable[AsyncGenerator[Any, None]]]
) -> Callable[[], Coroutine[Any, Any, None]]:
    await session.listen()
    try:
        generator = await open_generator()
    except BaseException:
        await session.abandon()
        raise
    return await session.start(generator)


async def handle_pull_iterator_request(
    handler: Callable[..., Any],
    args: list[Any],
    iterator_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
    on_finished: Callable[[], Any] | None = None,
    probe: SessionProbe | None = None,
) -> Callable[[], Coroutine[Any, Any, None]]:
    session = _PullSession(iterator_id, client, None, on_finished, probe)
    return await _serve_pull(session, partial(_open_pull_generator, handler, args, io_pool))


async def _open_pull_generator(
    handler: Callable[..., Any], args: list[Any], io_pool: ThreadPoolExecutor
) -> AsyncGenerator[Any, None]:
    generator: AsyncGenerator[Any, None]

    if is_async_generator(handler):
        generator = handler(*args)
    elif is_sync_generator(handler):
        sync_gen = handler(*args)

        async def async_wrapper() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper()
    elif is_async_function(handler):
        result = await handler(*args)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull iterator")
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args)
        result = await loop.run_in_executor(io_pool, func)

        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper()
        else:
            raise create_error(ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull iterator")

    if not hasattr(generator, "__aiter__"):
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    return generator


class CallbackInvoker:
    def __init__(
        self,
        subject: str,
        client: "RPCClient",
        oneway: set[str],
        probe: SessionProbe | None = None,
        on_fail: Callable[[Exception], None] | None = None,
    ) -> None:
        self._subject = subject
        self._client = client
        self._oneway = oneway
        self._probe = probe
        self._on_fail = on_fail
        self._guard = _YieldGuard()
        self.active = True

    async def invoke(self, method: str, *args: Any) -> None:
        if not self.active:
            return
        if method not in self._oneway:
            return
        if not self._client.is_connected:
            return
        msg: CallbackInvocation = {"method": method, "args": list(args)}
        probe = self._probe
        try:
            if probe is None:
                await self._client.publish(self._subject, msg)
            else:
                probe.sent = True
                await self._client.publish(self._subject, msg, reply=probe.reply() if probe.due else None)
        except Exception as error:
            # an error while disconnected means the connection is gone
            if self._on_fail is not None and self._client.is_connected:
                self._on_fail(error)
            raise
        if self._guard.due():
            await asyncio.sleep(0)


async def handle_pull_callback_request(
    handler: Callable[..., Any],
    args: list[Any],
    iterator_id: str,
    callback_subject: str,
    oneway_methods: list[str],
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
    on_finished: Callable[[], Any] | None = None,
    probe: SessionProbe | None = None,
) -> Callable[[], Coroutine[Any, Any, None]]:
    _check_control_subject(callback_subject, PULL_CALLBACK_SUBJECT_PREFIX)

    def fail(error: Exception) -> None:
        session.fail(error)

    invoker = CallbackInvoker(callback_subject, client, set(oneway_methods), probe, fail)
    session = _PullSession(iterator_id, client, invoker, on_finished, probe)
    return await _serve_pull(session, partial(_open_pull_callback_generator, handler, args, invoker, io_pool))


async def _open_pull_callback_generator(
    handler: Callable[..., Any], args: list[Any], invoker: CallbackInvoker, io_pool: ThreadPoolExecutor
) -> AsyncGenerator[Any, None]:
    generator: AsyncGenerator[Any, None]

    if is_async_generator(handler):
        generator = handler(*args, invoker)
    elif is_sync_generator(handler):
        sync_gen = handler(*args, invoker)

        async def async_wrapper_sync() -> AsyncGenerator[Any, None]:
            for value in sync_gen:
                yield value

        generator = async_wrapper_sync()
    elif is_async_function(handler):
        result = await handler(*args, invoker)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper_iter() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper_iter()
        else:
            invoker.active = False
            raise create_error(
                ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull-callback iterator"
            )
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args, invoker)
        result = await loop.run_in_executor(io_pool, func)
        if hasattr(result, "__aiter__"):
            generator = result
        elif hasattr(result, "__iter__"):

            async def async_wrapper_sync_fn() -> AsyncGenerator[Any, None]:
                for value in result:
                    yield value

            generator = async_wrapper_sync_fn()
        else:
            invoker.active = False
            raise create_error(
                ErrorCode.INTERNAL_ERROR, "Handler must return a generator for pull-callback iterator"
            )

    if not hasattr(generator, "__aiter__"):
        invoker.active = False
        raise create_error(ErrorCode.INTERNAL_ERROR, "Failed to create async generator from handler")

    return generator


async def handle_callback_request(
    handler: Callable[..., Any],
    args: list[Any],
    callback_subject: str,
    request_id: str,
    client: RPCClient,
    io_pool: ThreadPoolExecutor,
    on_finished: Callable[[], Any] | None = None,
    probe: SessionProbe | None = None,
) -> Callable[[], Coroutine[Any, Any, None]]:
    _check_control_subject(callback_subject, CALLBACK_SUBJECT_PREFIX)
    ended = False

    async def wrapper_callback(value: Any) -> None:
        if ended or not client.is_connected:
            return
        msg: CallbackMessage = {
            "id": request_id,
            "type": "data",
            "data": value,
        }
        if probe is None:
            await client.publish(callback_subject, msg)
        else:
            probe.sent = True
            await client.publish(callback_subject, msg, reply=probe.reply() if probe.due else None)

    # a sync handler may call back from any pool thread: values queue in call order, one loop task publishes them per burst
    loop = asyncio.get_running_loop()
    loop_thread = threading.get_ident()
    queued: deque[Any] = deque()
    drain_task: asyncio.Task[None] | None = None
    wake_pending = False

    async def drain() -> None:
        nonlocal drain_task
        try:
            while queued:
                await wrapper_callback(queued.popleft())
        finally:
            drain_task = None

    def wake() -> None:
        nonlocal drain_task, wake_pending
        wake_pending = False
        if drain_task is None and queued and not ended:
            drain_task = loop.create_task(drain())

    def sync_wrapper_callback(value: Any) -> None:
        nonlocal wake_pending
        if ended:
            return
        queued.append(value)
        if wake_pending:
            return
        wake_pending = True
        try:
            if threading.get_ident() == loop_thread:
                loop.call_soon(wake)
            else:
                loop.call_soon_threadsafe(wake)
        except RuntimeError:
            # the loop is closed, nobody publishes anymore
            pass

    handler_cleanup = None
    if is_async_function(handler):
        handler_cleanup = await handler(*args, wrapper_callback)
    else:
        loop = asyncio.get_event_loop()
        func = partial(handler, *args, sync_wrapper_callback)
        handler_cleanup = await loop.run_in_executor(io_pool, func)

    async def invoke_cleanup() -> None:
        if handler_cleanup and callable(handler_cleanup):
            if is_async_function(handler_cleanup):
                await cast(Callable[[], Coroutine[Any, Any, None]], handler_cleanup)()
            else:
                handler_cleanup()

    # the cancel message and a later register_handler/disconnect cleanup must not both run the handler cleanup
    cleaned_up = False

    async def run_handler_cleanup() -> None:
        nonlocal cleaned_up
        if cleaned_up:
            return
        cleaned_up = True
        await invoke_cleanup()

    def end() -> None:
        nonlocal ended
        ended = True
        queued.clear()
        if drain_task is not None:
            drain_task.cancel()
        if probe is not None:
            probe.stop()

    async def on_cancel(_msg: Any) -> None:
        end()
        if on_finished is not None:
            with contextlib.suppress(Exception):
                on_finished()
        await run_handler_cleanup()
        # outside this callback's own task, Subscription.unsubscribe() cancels the task it is called from
        asyncio.create_task(cancel_unsub())

    async def ping(reply: str) -> None:
        message: CallbackMessage = {"id": request_id, "type": "ping"}
        with contextlib.suppress(Exception):
            await client.publish(callback_subject, message, reply=reply)

    def orphaned() -> None:
        if not ended:
            run_in_background(on_cancel(None))

    cancel_unsub = await _subscribe_session(client, f"{callback_subject}.cancel", on_cancel)
    if probe is not None:
        probe.start(ping, orphaned)

    async def cleanup() -> None:
        end()
        await cancel_unsub()
        await run_handler_cleanup()

    return cleanup


def session_params(params: Any, marker: str) -> dict[str, Any] | None:
    if isinstance(params, list) and params:
        params = params[0]
    if isinstance(params, dict) and params.get(marker):
        return cast(dict[str, Any], params)
    return None


def _check_control_subject(subject: Any, prefix: str) -> None:
    # the caller names this subject and the handler publishes there with its own credentials
    if not isinstance(subject, str) or len(subject) <= len(prefix) or not subject.startswith(prefix):
        raise create_error(ErrorCode.INVALID_PARAMS, f"Control subject must start with '{prefix}'")
