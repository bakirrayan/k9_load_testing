"""Unary gRPC for plain ``def`` code: ``k9.grpc`` with a blocking ``invoke``::

    import k9
    from k9.sync import grpc

    client = grpc.Client("localhost:50051", grpc.load("hello.proto"), plaintext=True)

    def default():
        res = client.invoke("hello.Greeter/SayHello", {"name": "k9"})
        k9.check(res, {"status is OK": lambda r: r.ok})
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

from k9 import grpc as _grpc
from k9._context import require_ctx, require_sync
from k9._errors import InvalidRequest

from k9.grpc import (
    Metadata,
    ProtoError,
    Protos,
    Response,
    StatusCode,
    StreamClosed,
    Timeout,
    _StreamState,
    load,
    reflect,
)

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


class Stream(_StreamState):
    """``k9.grpc.Stream`` with blocking calls; use it with ``with`` (which
    cancels it on exit unless it ended) and iterate over it for the answers."""

    def send(self, message: Mapping[str, Any]) -> None:
        """Sends a message; see ``k9.grpc.Stream.send``."""
        if self.ended:
            raise StreamClosed("the stream has ended")
        self._sent(self._native.send_blocking(json.dumps(dict(message))))

    def recv(self, timeout: Timeout | None = None) -> dict[str, Any] | None:
        """The next message, or ``None`` once the stream has ended. With
        ``timeout``, raises ``TimeoutError`` if none arrives in time (the stream
        stays open and the message isn't lost)."""
        if self.ended:
            return None
        return self._event(self._native.recv_blocking(_grpc._seconds(timeout)))

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while (message := self.recv()) is not None:
            yield message

    def __enter__(self) -> Stream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cancel()


class Client(_grpc.Client):
    """``k9.grpc.Client`` whose ``invoke`` blocks (other VUs keep running)."""

    def invoke(  # type: ignore[override]
        self,
        method: str,
        message: Mapping[str, Any] | None = None,
        *,
        metadata: Metadata | None = None,
        tags: Mapping[str, str] | None = None,
        name: str | None = None,
        timeout: Timeout | None = None,
    ) -> Response:
        """Calls ``method`` and waits for its response; see ``k9.grpc.Client.invoke``."""
        require_sync("k9.sync.grpc", "await k9.grpc")
        ctx = require_ctx("k9.sync.grpc")
        prepared = self._prepare(method, message, metadata, tags, name, timeout)
        try:
            payload = ctx.grpc_invoke_blocking(*prepared)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        return Response(payload, prepared[1].path)

    def stream(  # type: ignore[override]
        self,
        method: str,
        *,
        metadata: Metadata | None = None,
        tags: Mapping[str, str] | None = None,
        name: str | None = None,
        timeout: Timeout | None = None,
    ) -> Stream:
        """Opens a stream and returns it once open; see ``k9.grpc.Client.stream``."""
        require_sync("k9.sync.grpc", "await k9.grpc")
        ctx = require_ctx("k9.sync.grpc")
        prepared = self._prepare_stream(method, metadata, tags, name, timeout)
        try:
            native = ctx.grpc_stream_blocking(*prepared)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        return Stream(native, prepared[1].path)
