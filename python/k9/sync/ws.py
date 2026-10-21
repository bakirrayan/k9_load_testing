"""WebSockets for plain ``def`` functions: ``k9.ws`` with blocking calls::

    from k9.sync import ws

    def default():
        with ws.connect("wss://echo.example/") as sock:
            sock.send("hello")
            reply = sock.recv(timeout=5)

Recorded in the same ``ws_*`` metrics. Every call releases the GIL while it
waits, and returns at once (raising ``k9.Interrupted``) if the VU is
interrupted.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator, Mapping
from typing import Any, Union

from k9._context import current_group, require_ctx, require_sync
from k9._errors import ConnectionClosed, InvalidRequest, WebSocketError
from k9._native import BlockingWebSocket
from k9.http import Headers

__all__ = ["WebSocket", "connect"]

Message = Union[str, bytes]
Timeout = Union[float, datetime.timedelta]


def _seconds(timeout: Timeout | None) -> float | None:
    return timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout


class WebSocket:
    """An open WebSocket; a context manager that closes it on exit."""

    def __init__(self, native: BlockingWebSocket) -> None:
        self._native = native
        #: The handshake's status (101).
        self.status: int = native.status
        #: The handshake response's headers.
        self.headers = Headers(native.headers)
        self.closed = False
        #: The peer's close code and reason, once it closed.
        self.close_code: int | None = None
        self.close_reason: str | None = None
        #: Why the session ended, if it failed.
        self.error: str | None = None

    def send(self, data: Message) -> None:
        """Sends ``str`` as a text message, ``bytes`` as a binary one."""
        if self.closed:
            raise ConnectionClosed(self.close_code, self.close_reason)
        error = self._native.send(data)
        if error is not None:
            raise WebSocketError(error, "websocket")

    def recv(self, timeout: Timeout | None = None) -> Message:
        """The next message. Raises ``ConnectionClosed`` once the WebSocket is
        closed, and ``TimeoutError`` if nothing arrives within ``timeout``."""
        while not self.closed:
            kind, a, b = self._native.recv(_seconds(timeout))
            if kind == "message":
                return a  # type: ignore[no-any-return]
            if kind == "timeout":
                raise TimeoutError(f"nothing within {_seconds(timeout)}s")
            if kind == "close":
                self.close_code, self.close_reason = a, b
            elif kind == "end":
                self.closed = True
                self.error = a
        raise ConnectionClosed(self.close_code, self.close_reason)

    def ping(self, timeout: Timeout | None = None) -> float:
        """Sends a ping and waits for its pong; the round trip in ms."""
        if self.closed:
            raise ConnectionClosed(self.close_code, self.close_reason)
        rtt = self._native.ping(_seconds(timeout))
        if rtt is None:
            raise WebSocketError("no pong: the WebSocket ended or timed out", "websocket")
        return rtt

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Closes the WebSocket and waits for the peer to confirm."""
        if not self.closed:
            frame = self._native.close(code, reason)
            if frame is not None:
                self.close_code, self.close_reason = frame
            self.closed = True

    def __iter__(self) -> Iterator[Message]:
        while True:
            try:
                yield self.recv()
            except ConnectionClosed:
                return

    def __enter__(self) -> WebSocket:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"<WebSocket {state} status={self.status}>"


def connect(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    name: str | None = None,
    timeout: Timeout | None = None,
) -> WebSocket:
    """Opens a WebSocket (``k9.ws.connect``'s arguments). Raises
    ``k9.ws.WebSocketError`` if it can't be opened."""
    require_sync("k9.sync.ws", "k9.ws")
    ctx = require_ctx("k9.sync.ws")
    header_list = [(str(k), str(v)) for k, v in headers.items()] if headers else []
    tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
    try:
        result: Any = ctx.ws_connect_blocking(
            url, header_list, current_group.get(), name, tag_list, _seconds(timeout)
        )
    except ValueError as exc:
        raise InvalidRequest(f"WebSocket {url}: {exc}") from None
    if isinstance(result, tuple):
        message, kind = result
        raise WebSocketError(message, kind)
    return WebSocket(result)
