"""The compiled half of the ``k9`` package (``crates/k9-py``). Internal: scripts
use ``k9.http``, ``k9.check`` and friends, which call into this."""

from typing import Any

__version__: str

def cli_main(argv: list[str]) -> int:
    """Run the k9 command line with ``argv`` (including the program name) and return its exit code."""

def metric_error(name: str, kind: str) -> str | None:
    """Why a custom metric named ``name`` of ``kind`` (counter, gauge, rate,
    trend) can't be declared, or None if it can."""

class Context:
    """One VU's (or hook's) connection to the engine: its HTTP client and metrics."""

    @property
    def root_group(self) -> str:
        """The group path this context starts from: ``""``, or ``::setup`` etc."""

    @property
    def throw(self) -> bool:
        """k6's ``throw``: requests that get no response raise ``RequestError``."""

    def request(
        self,
        method: str,
        url: str,
        headers: list[tuple[str, str]],
        body: bytes | None,
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
    ) -> RequestHandle:
        """Starts a request; its response tuple arrives as an ``http_done``
        runtime command. Raises ``ValueError`` for an invalid request."""
    def ws_connect_blocking(
        self,
        url: str,
        headers: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
    ) -> BlockingWebSocket | tuple[str, str]:
        """Opens a WebSocket, blocking with the GIL released."""
    def ws_connect(
        self,
        url: str,
        headers: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
    ) -> RequestHandle:
        """Starts opening a WebSocket; a ``ws_open`` runtime command carries the
        native ``WebSocket`` or ``(message, kind)``. Raises ``ValueError`` for
        an invalid URL or header."""

    def grpc_invoke(
        self,
        url: str,
        method: GrpcMethod,
        message: str,
        metadata: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
        options: tuple[bool, int | None, int | None],
    ) -> RequestHandle:
        """Starts a unary gRPC call (``message`` is JSON); its response tuple
        arrives as an ``http_done`` runtime command. Raises ``ValueError`` for
        an invalid call."""
    def grpc_invoke_blocking(
        self,
        url: str,
        method: GrpcMethod,
        message: str,
        metadata: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
        options: tuple[bool, int | None, int | None],
    ) -> tuple[Any, ...]:
        """Makes a unary gRPC call and waits for its response tuple, with the
        GIL released. Raises ``k9.Interrupted`` if the VU is interrupted."""

    def grpc_stream(
        self,
        url: str,
        method: GrpcMethod,
        metadata: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
        options: tuple[bool, int | None, int | None],
    ) -> RequestHandle:
        """Starts opening a gRPC stream; an ``http_done`` command carries the
        native ``GrpcStream``."""
    def grpc_stream_blocking(
        self,
        url: str,
        method: GrpcMethod,
        metadata: list[tuple[str, str]],
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
        options: tuple[bool, int | None, int | None],
    ) -> GrpcStream:
        """Opens a gRPC stream, blocking with the GIL released."""

    def request_blocking(
        self,
        method: str,
        url: str,
        headers: list[tuple[str, str]],
        body: bytes | None,
        group: str,
        name: str | None,
        tags: list[tuple[str, str]],
        timeout: float | None,
    ) -> tuple[Any, ...]:
        """Sends a request and waits for its response tuple, with the GIL
        released. Raises ``k9.Interrupted`` if the VU is interrupted."""

    def sleep(self, seconds: float) -> None:
        """Sleeps with the GIL released; raises ``k9.Interrupted`` if the VU is interrupted."""

    def check(self, name: str, passed: bool, group: str, tags: list[tuple[str, str]]) -> None: ...
    def group_duration(self, group: str, seconds: float) -> None: ...
    def add(self, metric: str, value: float, group: str, tags: list[tuple[str, str]]) -> None:
        """Adds a sample to a registered metric; ``KeyError`` if there's none by that name."""

def grpc_target(address: str, plaintext: bool) -> str:
    """A gRPC target's base URL; ``ValueError`` if invalid."""

def compress(data: bytes, encodings: str) -> tuple[bytes, str]:
    """``data`` compressed, and the ``Content-Encoding``; ``ValueError`` if unknown."""

def multipart_form(
    parts: list[tuple[str, str | None, str | None, bytes]],
) -> tuple[str, bytes]:
    """A multipart form's ``Content-Type`` and body."""

class Protos:
    """Services and messages compiled from ``.proto`` files."""

    @staticmethod
    def load(files: list[str], import_paths: list[str]) -> Protos:
        """Compiles ``files``; ``ValueError`` if they don't compile."""
    @staticmethod
    def reflect(url: str, timeout: float | None) -> Protos:
        """Asks the server's reflection service; ``ValueError`` if it can't."""
    def methods(self) -> list[str]: ...
    def method(self, name: str) -> GrpcMethod:
        """A method by name; ``ValueError`` if there's none."""

class GrpcStream:
    """An open gRPC stream."""

    def send(self, message: str) -> int:
        """Job: ``None`` or ``(kind, message)``."""
    def recv(self) -> int:
        """Job: ``("message", json)``, ``("end", status, message, headers,
        trailers)`` or ``None``."""
    def send_blocking(self, message: str) -> tuple[str, str] | None: ...
    def recv_blocking(self, timeout: float | None = None) -> tuple[Any, ...] | None: ...
    def close_send(self) -> None: ...
    def cancel(self) -> None: ...

class GrpcMethod:
    @property
    def path(self) -> str: ...

class WebSocket:
    """An open WebSocket's native side; ``k9.ws.WebSocket`` wraps it."""

    @property
    def session(self) -> int: ...
    @property
    def status(self) -> int: ...
    @property
    def headers(self) -> list[tuple[str, str]]: ...
    def send(self, data: str | bytes) -> int:
        """Queues a message; the job's ``ws_done`` command says if it went out."""
    def ping(self) -> int: ...
    def close(self, code: int, reason: str) -> int: ...

class BlockingWebSocket:
    """``k9.sync.ws.WebSocket``'s native side: every call blocks."""

    @property
    def status(self) -> int: ...
    @property
    def headers(self) -> list[tuple[str, str]]: ...
    def send(self, data: str | bytes) -> str | None:
        """An error message, or ``None`` once sent."""
    def recv(self, timeout: float | None = None) -> tuple[str, Any, Any]:
        """``(kind, a, b)``: ``message``, ``close``, ``end`` or ``timeout``."""
    def ping(self, timeout: float | None = None) -> float | None: ...
    def close(self, code: int, reason: str) -> tuple[int, str] | None: ...

class RequestHandle:
    """A request in flight."""

    @property
    def job(self) -> int: ...
    def abort(self) -> None:
        """Stops the request."""
