"""K9's exceptions. Everything k9 raises on purpose derives from ``K9Error``."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from k9._run import RunResult
    from k9.http import Response


class K9Error(Exception):
    """Base class for k9's exceptions."""


class ContextError(K9Error, RuntimeError):
    """A k9 API was used where it isn't available, e.g. ``k9.http`` at module
    level (scripts make requests from iterations and hooks only)."""


class MetricError(K9Error, ValueError):
    """A custom metric is invalid: bad name, clash with an existing metric, or
    declared after the script loaded (declare metrics at module level)."""


class InvalidRequest(K9Error, ValueError):
    """A request can't be sent as written: bad URL, method or header."""


class HttpError(K9Error):
    """Raised by ``Response.raise_for_status()``."""

    def __init__(self, message: str, response: Response) -> None:
        super().__init__(message)
        self.response = response


class RequestError(HttpError):
    """The request got no response (DNS, connect, TLS, timeout, protocol).
    ``kind`` says which, as in ``Response.error_code``."""

    @property
    def kind(self) -> str:
        return self.response.error_code or "unknown"


class StatusError(HttpError):
    """The response has a 4xx or 5xx status."""

    @property
    def status(self) -> int:
        return self.response.status


class WebSocketError(K9Error):
    """A WebSocket couldn't be opened, or failed while open. ``kind`` says
    why, as in ``Response.error_code`` (``websocket`` for a refused upgrade)."""

    def __init__(self, message: str, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class ConnectionClosed(WebSocketError):
    """The WebSocket is closed: raised by ``recv()`` and ``send()`` after the
    close. ``code`` and ``reason`` are the peer's close frame (``None`` without
    one)."""

    def __init__(self, code: int | None, reason: str | None) -> None:
        detail = f" (code {code}{f', {reason}' if reason else ''})" if code is not None else ""
        super().__init__(f"the WebSocket is closed{detail}", "closed")
        self.code = code
        self.reason = reason


class Interrupted(BaseException):
    """Raised by k9 calls in a VU that has been interrupted (its scenario's
    ``gracefulStop`` passed, or Ctrl-C), so a blocked ``def`` iteration stops
    at once. Like ``asyncio.CancelledError`` in async code, it derives from
    ``BaseException`` so ``except Exception`` doesn't swallow it."""


class RunError(K9Error):
    """``k9.run()`` failed. ``exit_code`` is k9's (k6's codes), ``stderr``
    what it printed, and ``result`` the run's results when it got that far."""

    def __init__(
        self, message: str, exit_code: int, stderr: str, result: RunResult | None = None
    ) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.stderr = stderr
        self.result = result


class ConfigError(RunError, ValueError):
    """The test can't run as configured: invalid options or plan, a missing
    file, or bad arguments."""


class ScriptError(RunError):
    """The script raised while loading, or its ``setup()``/``teardown()``
    failed or timed out."""


class ThresholdsFailed(RunError, AssertionError):
    """The run finished but crossed thresholds (exit code 99). An
    ``AssertionError``, so pytest reports it as a test failure."""
