"""Unary gRPC from async script code, recorded as ``grpc_req_duration``
(tagged with the method and its status code) plus ``data_sent``/``data_received``::

    import k9

    protos = k9.grpc.load("hello.proto", import_paths=["protos"])  # or k9.grpc.reflect(...)
    client = k9.grpc.Client("grpc.example:443", protos)

    async def default():
        res = await client.invoke("hello.Greeter/SayHello", {"name": "k9"})
        k9.check(res, {"status is OK": lambda r: r.status == k9.grpc.StatusOK})

``.proto`` files are compiled when loaded (no ``protoc`` needed); messages go
in and come out as dicts, with protobuf's JSON mapping (64-bit integers are
strings, enums are names, default values are included). A ``Client`` holds
no connection: each VU makes its calls on an HTTP/2 connection of its own,
so one client at module level serves every VU. Plain ``def`` code uses
``k9.sync.grpc``.
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import enum
import json
from collections.abc import AsyncIterator, Generator, Mapping, Sequence
from os import PathLike
from typing import Any, Union

from k9._context import current_group, pending, require_async, require_ctx
from k9._errors import InvalidRequest, K9Error
from k9._native import GrpcMethod as _NativeMethod
from k9._native import GrpcStream as _NativeStream
from k9._native import Protos as _NativeProtos
from k9._native import grpc_target as _grpc_target
from k9.http import Headers

__all__ = [
    "Client",
    "ProtoError",
    "Protos",
    "Response",
    "StatusCode",
    "Stream",
    "StreamClosed",
    "load",
    "reflect",
]

Timeout = Union[float, datetime.timedelta]
Metadata = Mapping[str, Union[str, bytes]]


class ProtoError(K9Error, ValueError):
    """``.proto`` files that can't be loaded or don't compile, or a server
    whose reflection service can't be asked."""


class StreamClosed(K9Error):
    """Sending on a stream that is half-closed or has ended."""


class StatusCode(enum.IntEnum):
    """gRPC status codes."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


# k6's names (`grpc.StatusOK` …).
StatusOK = StatusCode.OK
StatusCanceled = StatusCode.CANCELLED
StatusUnknown = StatusCode.UNKNOWN
StatusInvalidArgument = StatusCode.INVALID_ARGUMENT
StatusDeadlineExceeded = StatusCode.DEADLINE_EXCEEDED
StatusNotFound = StatusCode.NOT_FOUND
StatusAlreadyExists = StatusCode.ALREADY_EXISTS
StatusPermissionDenied = StatusCode.PERMISSION_DENIED
StatusResourceExhausted = StatusCode.RESOURCE_EXHAUSTED
StatusFailedPrecondition = StatusCode.FAILED_PRECONDITION
StatusAborted = StatusCode.ABORTED
StatusOutOfRange = StatusCode.OUT_OF_RANGE
StatusUnimplemented = StatusCode.UNIMPLEMENTED
StatusInternal = StatusCode.INTERNAL
StatusUnavailable = StatusCode.UNAVAILABLE
StatusDataLoss = StatusCode.DATA_LOSS
StatusUnauthenticated = StatusCode.UNAUTHENTICATED


class Protos:
    """Services and messages compiled from ``.proto`` files (see ``load``)."""

    def __init__(self, native: _NativeProtos) -> None:
        self._native = native
        self._methods: dict[str, _NativeMethod] = {}

    @property
    def methods(self) -> list[str]:
        """Every method, as ``package.Service/Method``."""
        return self._native.methods()

    def _method(self, name: str) -> _NativeMethod:
        method = self._methods.get(name)
        if method is None:
            try:
                method = self._native.method(name)
            except ValueError as exc:
                raise InvalidRequest(str(exc)) from None
            self._methods[name] = method
        return method

    def __repr__(self) -> str:
        return f"<Protos {len(self.methods)} methods>"


def load(
    *files: str | PathLike[str], import_paths: Sequence[str | PathLike[str]] | None = None
) -> Protos:
    """Compiles ``.proto`` files and what they import, found in
    ``import_paths`` (default: each file's own directory). Load them once, at
    module level. Raises ``ProtoError``."""
    try:
        native = _NativeProtos.load([str(f) for f in files], [str(p) for p in import_paths or ()])
    except ValueError as exc:
        raise ProtoError(str(exc)) from None
    return Protos(native)


def reflect(target: str, *, plaintext: bool = False, timeout: Timeout | None = None) -> Protos:
    """Asks the server at ``target`` (as for ``Client``) for its services
    through its reflection service. It blocks: call it once, at module
    level. Raises ``ProtoError`` if the server can't tell."""
    try:
        url = _grpc_target(target, plaintext)
    except ValueError as exc:
        raise InvalidRequest(str(exc)) from None
    seconds = timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout
    try:
        return Protos(_NativeProtos.reflect(url, seconds))
    except ValueError as exc:
        raise ProtoError(str(exc)) from None


class Response:
    """A call's outcome. A call that got no answer has status
    ``DEADLINE_EXCEEDED`` (it timed out) or ``UNAVAILABLE``."""

    __slots__ = ("duration", "error", "headers", "message", "method", "status", "trailers")

    def __init__(self, payload: tuple[Any, ...], method: str) -> None:
        status, status_message, message, headers, trailers, duration = payload
        self.method = method
        #: The status code; ``StatusCode.OK`` (0) on success.
        self.status = StatusCode(status) if status in StatusCode._value2member_map_ else status
        #: The response message as a dict (``None`` unless the status is OK).
        self.message: dict[str, Any] | None = json.loads(message) if message is not None else None
        #: The status message (``grpc-message``) or why the call failed;
        #: ``None`` when it succeeded.
        self.error: str | None = status_message if status != 0 else None
        #: Response metadata from the headers and from the trailers.
        self.headers = Headers(headers)
        self.trailers = Headers(trailers)
        #: Milliseconds from sending to the end of the response (``grpc_req_duration``).
        self.duration: float = duration

    @property
    def ok(self) -> bool:
        return self.status == StatusCode.OK

    def __repr__(self) -> str:
        name = self.status.name if isinstance(self.status, StatusCode) else self.status
        return f"<grpc.Response {self.method} {name}>"


class Client:
    """Calls the methods of ``protos`` on ``target``: ``host:port`` over TLS
    (``plaintext=True``: h2c, no TLS), or an ``http://``/``https://`` URL.
    ``compression="gzip"`` compresses the messages sent (gzipped answers are
    read either way); ``max_send_size`` and ``max_receive_size`` (bytes, k6's
    ``maxSendSize``/``maxReceiveSize``) fail larger messages with
    ``RESOURCE_EXHAUSTED``. ``metadata`` goes with every call and stream (a
    call's own metadata wins for the same key). Holds no connection: safe to
    share between VUs."""

    def __init__(
        self,
        target: str,
        protos: Protos,
        *,
        plaintext: bool = False,
        compression: str | None = None,
        max_send_size: int | None = None,
        max_receive_size: int | None = None,
        metadata: Metadata | None = None,
    ) -> None:
        try:
            self._url = _grpc_target(target, plaintext)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        if compression not in (None, "identity", "gzip"):
            raise InvalidRequest(f"unknown compression {compression!r} (expected 'gzip')")
        for what, size in (("max_send_size", max_send_size), ("max_receive_size", max_receive_size)):
            if size is not None and (not isinstance(size, int) or size < 0):
                raise InvalidRequest(f"{what} must be a number of bytes, got {size!r}")
        self.target = target
        self.protos = protos
        self._options = (compression == "gzip", max_send_size, max_receive_size)
        self.metadata: dict[str, str | bytes] = {
            k.lower(): v for k, v in (metadata or {}).items()
        }

    def _prepare(
        self,
        method: str,
        message: Mapping[str, Any] | None,
        metadata: Metadata | None,
        tags: Mapping[str, str] | None,
        name: str | None,
        timeout: Timeout | None,
    ) -> tuple[Any, ...]:
        native = self.protos._method(method)
        merged = dict(self.metadata)
        merged.update({k.lower(): v for k, v in (metadata or {}).items()})
        pairs: list[tuple[str, str]] = []
        for key, value in merged.items():
            if isinstance(value, bytes):
                if not key.lower().endswith("-bin"):
                    raise InvalidRequest(f"metadata {key!r}: bytes values need a key ending in -bin")
                value = base64.b64encode(value).decode()
            pairs.append((str(key), str(value)))
        seconds = timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout
        tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
        body = json.dumps(dict(message or {}))
        return (
            self._url, native, body, pairs, current_group.get(), name, tag_list, seconds,
            self._options,
        )  # fmt: skip

    async def invoke(
        self,
        method: str,
        message: Mapping[str, Any] | None = None,
        *,
        metadata: Metadata | None = None,
        tags: Mapping[str, str] | None = None,
        name: str | None = None,
        timeout: Timeout | None = None,
    ) -> Response:
        """Calls ``method`` (``package.Service/Method``) with ``message``.
        ``metadata`` goes with the request (``bytes`` values for ``-bin``
        keys); ``tags`` and ``name`` tag its metrics; ``timeout`` (default
        60s) is the call's deadline. Raises ``InvalidRequest`` for an unknown
        method or a message that doesn't fit it; a failed call is a response
        with its status."""
        require_async("k9.grpc", "k9.sync.grpc")
        ctx = require_ctx("k9.grpc")
        prepared = self._prepare(method, message, metadata, tags, name, timeout)
        try:
            handle = ctx.grpc_invoke(*prepared)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        future: asyncio.Future[tuple[Any, ...]] = asyncio.get_running_loop().create_future()
        pending[handle.job] = future
        try:
            payload = await future
        except asyncio.CancelledError:
            handle.abort()
            raise
        finally:
            pending.pop(handle.job, None)
        return Response(payload, prepared[1].path)

    def stream(
        self,
        method: str,
        *,
        metadata: Metadata | None = None,
        tags: Mapping[str, str] | None = None,
        name: str | None = None,
        timeout: Timeout | None = None,
    ) -> _OpenStream:
        """Opens a stream (client, server or bidirectional) to ``method``:
        ``await`` it, or use it with ``async with`` (which cancels the stream
        on exit unless it ended). ``timeout`` (default 60s) is the whole
        stream's deadline. Recorded as ``grpc_streams``,
        ``grpc_streams_msgs_sent`` and ``grpc_streams_msgs_received``."""
        require_async("k9.grpc", "k9.sync.grpc")
        ctx = require_ctx("k9.grpc")
        prepared = self._prepare_stream(method, metadata, tags, name, timeout)
        try:
            handle = ctx.grpc_stream(*prepared)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        return _OpenStream(_open_stream(handle, prepared[1].path))

    def _prepare_stream(
        self,
        method: str,
        metadata: Metadata | None,
        tags: Mapping[str, str] | None,
        name: str | None,
        timeout: Timeout | None,
    ) -> tuple[Any, ...]:
        url, native, _, pairs, group, name, tag_list, seconds, options = self._prepare(
            method, None, metadata, tags, name, timeout
        )
        return (url, native, pairs, group, name, tag_list, seconds, options)

    def __repr__(self) -> str:
        return f"<grpc.Client {self.target}>"


def _start(job: int) -> asyncio.Future[Any]:
    """A future for native job ``job`` that stays registered until it's done,
    whoever is (still) waiting for it."""
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    pending[job] = future
    future.add_done_callback(lambda _: pending.pop(job, None))
    return future


def _seconds(timeout: Timeout | None) -> float | None:
    seconds = timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout
    if seconds is not None and not seconds >= 0:
        raise ValueError(f"timeout must be zero or more seconds, got {timeout!r}")
    return seconds


async def _job(job: int) -> Any:
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    pending[job] = future
    try:
        return await future
    finally:
        pending.pop(job, None)


async def _open_stream(handle: Any, method: str) -> Stream:
    try:
        native = await _job(handle.job)
    except asyncio.CancelledError:
        handle.abort()
        raise
    return Stream(native, method)


class _StreamState:
    """What ``Stream`` and ``k9.sync.grpc.Stream`` share: the end's details."""

    def __init__(self, native: _NativeStream, method: str) -> None:
        self._native = native
        self.method = method
        #: Whether the stream has ended (no more messages will arrive).
        self.ended = False
        #: The status once it ended (``None`` before).
        self.status: StatusCode | int | None = None
        #: The status message (or why it failed) when the status isn't OK.
        self.error: str | None = None
        #: Response metadata, once the stream ended.
        self.headers = Headers([])
        self.trailers = Headers([])
        #: A receive that a timeout or cancellation interrupted: the next
        #: ``recv()`` takes its message.
        self._next: asyncio.Future[Any] | None = None

    def close_send(self) -> None:
        """Half-closes the stream: no more messages (answers still arrive)."""
        self._native.close_send()

    def cancel(self) -> None:
        """Ends the stream now, with status ``CANCELLED``."""
        if not self.ended:
            self._native.cancel()
            self._end(StatusCode.CANCELLED, "cancelled by the script", [], [])

    def _sent(self, error: tuple[str, str] | None) -> None:
        if error is not None:
            kind, message = error
            if kind == "invalid":
                raise InvalidRequest(message)
            raise StreamClosed(message)

    def _event(self, event: tuple[Any, ...] | None) -> dict[str, Any] | None:
        if event == ("timeout",):
            raise TimeoutError("no message arrived in time (the stream is still open)")
        if event is None:
            if not self.ended:
                self._end(StatusCode.CANCELLED, "cancelled", [], [])
            return None
        if event[0] == "message":
            return json.loads(event[1])  # type: ignore[no-any-return]
        _, status, status_message, headers, trailers = event
        self._end(status, status_message, headers, trailers)
        return None

    def _end(self, status: int, message: str, headers: Any, trailers: Any) -> None:
        self.ended = True
        self.status = StatusCode(status) if status in StatusCode._value2member_map_ else status
        self.error = message if status != 0 else None
        self.headers = Headers(headers)
        self.trailers = Headers(trailers)

    def __repr__(self) -> str:
        state = getattr(self.status, "name", "open") if self.ended else "open"
        return f"<grpc.Stream {self.method} {state}>"


class Stream(_StreamState):
    """An open stream: ``send()`` messages, ``close_send()`` when done, and
    ``recv()`` (or ``async for``) the answers until the end, after which
    ``status``, ``error``, ``headers`` and ``trailers`` are set."""

    async def send(self, message: Mapping[str, Any]) -> None:
        """Sends a message. Raises ``InvalidRequest`` if it doesn't fit the
        method, ``StreamClosed`` after ``close_send()`` or the end."""
        if self.ended:
            raise StreamClosed("the stream has ended")
        self._sent(await _job(self._native.send(json.dumps(dict(message)))))

    async def recv(self, timeout: Timeout | None = None) -> dict[str, Any] | None:
        """The next message, or ``None`` once the stream has ended. With
        ``timeout`` (seconds or a timedelta), raises ``TimeoutError`` if no
        message arrives in time. The stream stays open and nothing is lost,
        also when ``asyncio.wait_for`` or a cancellation interrupts it: the
        next ``recv()`` gets the message."""
        if self.ended:
            return None
        seconds = _seconds(timeout)
        if self._next is None:
            self._next = _start(self._native.recv())
        try:
            event = await asyncio.wait_for(asyncio.shield(self._next), seconds)
        except asyncio.TimeoutError:
            raise TimeoutError(f"no message within {seconds}s (the stream is still open)") from None
        self._next = None
        return self._event(event)

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[dict[str, Any]]:
        while (message := await self.recv()) is not None:
            yield message

    async def __aenter__(self) -> Stream:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.cancel()


class _OpenStream:
    """What ``Client.stream()`` returns: await it for the ``Stream``, or use
    it with ``async with``."""

    def __init__(self, coro: Any) -> None:
        self._coro = coro
        self._stream: Stream | None = None

    def __await__(self) -> Generator[Any, None, Stream]:
        return self._coro.__await__()  # type: ignore[no-any-return]

    async def __aenter__(self) -> Stream:
        self._stream = await self._coro
        return self._stream

    async def __aexit__(self, *exc: object) -> None:
        if self._stream is not None:
            self._stream.cancel()
