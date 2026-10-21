"""WebSockets from async script code, recorded with k6's metrics
(``ws_sessions``, ``ws_connecting``, ``ws_session_duration``, ``ws_msgs_sent``,
``ws_msgs_received``, ``ws_ping``, plus ``data_sent``/``data_received``)::

    import k9

    async def default():
        async with k9.ws.connect("wss://echo.example/", tags={"room": "a"}) as ws:
            await ws.send("hello")
            reply = await ws.recv(timeout=5)
            k9.check(reply, {"echoed": lambda r: r == "hello"})

Each WebSocket gets a connection of its own. Received messages are queued as
they arrive, so ``recv()`` never misses one, even after a timeout.
"""

from __future__ import annotations

import asyncio
import datetime
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Generator, Mapping
from typing import Any, TypeVar, Union

from k9._context import current_group, pending, require_async, require_ctx
from k9._errors import ConnectionClosed, InvalidRequest, WebSocketError
from k9._native import WebSocket as _NativeWebSocket
from k9.http import Headers

__all__ = ["ConnectionClosed", "WebSocket", "WebSocketError", "connect"]

T = TypeVar("T")
Message = Union[str, bytes]
Timeout = Union[float, datetime.timedelta]

_END = object()


def _seconds(timeout: Timeout | None) -> float | None:
    return timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout


async def _within(awaitable: Awaitable[T], timeout: Timeout | None) -> T:
    """``awaitable``, or the built-in ``TimeoutError`` (also on Python 3.10)."""
    try:
        return await asyncio.wait_for(awaitable, _seconds(timeout))
    except asyncio.TimeoutError:
        raise TimeoutError(f"nothing within {_seconds(timeout)}s") from None


class WebSocket:
    """An open WebSocket. Use it as an async context manager (it closes on
    exit), or call ``close()``. Plain ``def`` code uses ``k9.sync.ws``."""

    def __init__(self, native: _NativeWebSocket) -> None:
        self._native = native
        #: The handshake's status (101).
        self.status: int = native.status
        #: The handshake response's headers.
        self.headers = Headers(native.headers)
        self._messages: asyncio.Queue[Any] = asyncio.Queue()
        self._pongs: deque[asyncio.Future[float]] = deque()
        self._ended: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        #: Whether the WebSocket is closed (no more messages will arrive).
        self.closed = False
        #: The peer's close code and reason, once it closed (``None`` without
        #: a close frame).
        self.close_code: int | None = None
        self.close_reason: str | None = None
        #: Why the session ended, if it failed.
        self.error: str | None = None

    async def send(self, data: Message) -> None:
        """Sends ``str`` as a text message, ``bytes`` as a binary one."""
        if self.closed:
            raise ConnectionClosed(self.close_code, self.close_reason)
        self._check(await self._job(self._native.send(data)))

    async def recv(self, timeout: Timeout | None = None) -> Message:
        """The next message (``str`` or ``bytes``). Raises ``ConnectionClosed``
        once the WebSocket is closed and every message was received, and
        ``TimeoutError`` if nothing arrives within ``timeout`` seconds."""
        if self._messages.empty() and self.closed:
            raise ConnectionClosed(self.close_code, self.close_reason)
        message = await _within(self._messages.get(), timeout)
        if message is _END:
            self._messages.put_nowait(_END)  # for the next recv()
            raise ConnectionClosed(self.close_code, self.close_reason)
        return message  # type: ignore[no-any-return]

    async def ping(self, timeout: Timeout | None = None) -> float:
        """Sends a ping and waits for its pong; returns the round trip in
        milliseconds (also recorded as ``ws_ping``)."""
        if self.closed:
            raise ConnectionClosed(self.close_code, self.close_reason)
        pong: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        self._pongs.append(pong)
        self._check(await self._job(self._native.ping()))
        return await _within(pong, timeout)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Closes the WebSocket and waits for the peer to confirm."""
        if not self.closed:
            error = await self._job(self._native.close(code, reason))
            if error is None:
                await asyncio.shield(self._ended)
        self.closed = True

    def __aiter__(self) -> AsyncIterator[Message]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Message]:
        while True:
            try:
                yield await self.recv()
            except ConnectionClosed:
                return

    async def __aenter__(self) -> WebSocket:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"<WebSocket {state} status={self.status}>"

    def _check(self, error: str | None) -> None:
        if error is not None:
            if self.closed:
                raise ConnectionClosed(self.close_code, self.close_reason)
            raise WebSocketError(error, "websocket")

    @staticmethod
    async def _job(job: int) -> Any:
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        pending[job] = future
        try:
            return await future
        finally:
            pending.pop(job, None)

    def _event(self, kind: str, a: Any, b: Any) -> None:
        """Called by the runtime, in order, for everything the session saw."""
        if kind == "message":
            self._messages.put_nowait(a)
        elif kind == "pong":
            if self._pongs:
                pong = self._pongs.popleft()
                if not pong.done():
                    pong.set_result(a)
        elif kind == "close":
            self.close_code, self.close_reason = a, b
        elif kind == "end":
            self.closed = True
            self.error = a
            self._messages.put_nowait(_END)
            for pong in self._pongs:
                if not pong.done():
                    pong.set_exception(ConnectionClosed(self.close_code, self.close_reason))
            self._pongs.clear()
            if not self._ended.done():
                self._ended.set_result(None)


class _Connect:
    """What ``connect()`` returns: await it for the WebSocket, or use it with
    ``async with`` to close the WebSocket on exit."""

    def __init__(self, coro: Any) -> None:
        self._coro = coro
        self._socket: WebSocket | None = None

    def __await__(self) -> Generator[Any, None, WebSocket]:
        return self._coro.__await__()  # type: ignore[no-any-return]

    async def __aenter__(self) -> WebSocket:
        self._socket = await self._coro
        return self._socket

    async def __aexit__(self, *exc: object) -> None:
        if self._socket is not None:
            await self._socket.close()


def connect(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    name: str | None = None,
    timeout: Timeout | None = None,
) -> _Connect:
    """Opens a WebSocket to a ``ws://`` or ``wss://`` URL. ``headers`` go with
    the handshake; ``tags`` and ``name`` tag its metrics like a request's;
    ``timeout`` (default 60s) covers the handshake. Raises ``WebSocketError``
    if it can't be opened."""
    require_async("k9.ws", "k9.sync.ws")
    ctx = require_ctx("k9.ws")
    header_list = [(str(k), str(v)) for k, v in headers.items()] if headers else []
    tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
    try:
        handle = ctx.ws_connect(
            url, header_list, current_group.get(), name, tag_list, _seconds(timeout)
        )
    except ValueError as exc:
        raise InvalidRequest(f"WebSocket {url}: {exc}") from None
    return _Connect(_open(handle, url))


async def _open(handle: Any, url: str) -> WebSocket:
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    pending[handle.job] = future
    try:
        result = await future
    except asyncio.CancelledError:
        handle.abort()
        raise
    finally:
        pending.pop(handle.job, None)
    if isinstance(result, tuple):
        message, kind = result
        raise WebSocketError(message, kind)
    assert isinstance(result, WebSocket), url
    return result
