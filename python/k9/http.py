"""``k9.http``: HTTP requests from scripts, sent by k9's Rust client.

Each VU has its own connection pool and cookie jar (emptied at the start of
every iteration, as in k6), and every request is recorded in k6's
``http_req_*`` metrics, tagged with the VU's scenario and the current group::

    import k9

    async def default():
        res = await k9.http.get("https://test.k6.io/", tags={"page": "home"})
        k9.check(res, {"status is 200": lambda r: r.status == 200})

As in k6, a request that gets no response doesn't raise: the response has
status 0 and ``error`` says why. Call ``raise_for_status()`` to raise instead.
Run requests concurrently with ``asyncio.gather``.

These functions are for ``async def`` code; plain ``def`` functions use the
blocking twins in ``k9.sync.http``, which take the same arguments.
"""

from __future__ import annotations

import asyncio
import datetime
import json as _json
import os
import sys
from collections.abc import Coroutine, Iterator, Mapping
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Any, TypedDict, Union
from urllib.parse import urlencode

from k9._context import current_group, pending, require_async, require_ctx
from k9._errors import InvalidRequest, RequestError, StatusError
from k9._native import Context
from k9._native import compress as _native_compress
from k9._native import multipart_form as _native_multipart

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Unpack
    else:
        from typing_extensions import Unpack

__all__ = [
    "Headers",
    "RequestOptions",
    "Response",
    "Timings",
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "request",
]

#: Seconds, or a timedelta.
Timeout = Union[float, datetime.timedelta]
#: Query parameters: a mapping, or (key, value) pairs for repeated keys.
Params = Union[Mapping[str, Any], "list[tuple[str, Any]]"]
#: A request body: raw bytes, text (sent as UTF-8), or a form (URL-encoded).
Data = Union[bytes, str, Mapping[str, Any]]
#: A file to upload: its content (``bytes``, ``str`` or an open file), or
#: ``(filename, content)`` or ``(filename, content, content_type)``.
FileSpec = Union[bytes, str, IO[Any], "tuple[str, Any]", "tuple[str, Any, str]"]
Files = Mapping[str, FileSpec]


class RequestOptions(TypedDict, total=False):
    """Keyword arguments every request function takes (see ``request``)."""

    params: Params
    headers: Mapping[str, str]
    json: Any
    tags: Mapping[str, str]
    name: str
    timeout: Timeout
    files: Files
    compression: Union[str, None]


@dataclass(frozen=True)
class Timings:
    """k6's request phases, in milliseconds."""

    blocked: float
    connecting: float
    tls_handshaking: float
    sending: float
    waiting: float
    receiving: float

    @property
    def duration(self) -> float:
        """k6's ``http_req_duration``: sending + waiting + receiving."""
        return self.sending + self.waiting + self.receiving


class Headers(Mapping[str, str]):
    """Response headers; names are case-insensitive, and repeated headers are
    joined with ``", "`` (see ``get_all`` for the separate values)."""

    __slots__ = ("_pairs", "_joined")

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self._pairs = pairs
        self._joined: dict[str, str] | None = None

    def _map(self) -> dict[str, str]:
        if self._joined is None:
            joined: dict[str, str] = {}
            for name, value in self._pairs:
                key = name.lower()
                joined[key] = f"{joined[key]}, {value}" if key in joined else value
            self._joined = joined
        return self._joined

    def __getitem__(self, name: str) -> str:
        return self._map()[name.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._map())

    def __len__(self) -> int:
        return len(self._map())

    def get_all(self, name: str) -> list[str]:
        """Every value of a header, e.g. each ``Set-Cookie``."""
        key = name.lower()
        return [value for n, value in self._pairs if n.lower() == key]

    def __repr__(self) -> str:
        return f"Headers({self._map()!r})"


class Response:
    """What a request got back. For a request that got no response,
    ``status`` is 0 and ``error``/``error_code`` say why."""

    __slots__ = (
        "status", "proto", "headers", "body", "url", "timings", "error", "error_code", "request",
        "tls_version", "tls_cipher_suite",
    )  # fmt: skip

    def __init__(self, payload: tuple[Any, ...], request: tuple[str, str]) -> None:
        status, headers, body, url, timings, error, error_code, proto, tls, suite = payload
        self.status: int = status
        #: ``HTTP/1.1`` or ``HTTP/2.0`` (k6's names); empty without a response.
        self.proto: str = proto
        #: ``tls1.2`` or ``tls1.3`` over HTTPS (k6's names), else ``None``.
        self.tls_version: str | None = tls
        #: The negotiated cipher suite's IANA name, e.g. ``TLS_AES_128_GCM_SHA256``.
        self.tls_cipher_suite: str | None = suite
        self.headers = Headers(headers)
        self.body: bytes = body
        #: The URL that answered (after redirects); the requested one on errors.
        self.url: str = url or request[1]
        self.timings = Timings(*timings)
        #: Why the request got no response, e.g. ``could not connect to …``.
        self.error: str | None = error
        #: ``dns``, ``connect``, ``tls``, ``timeout``, ``protocol``, ``decompress`` (a body
        #: that isn't what its ``Content-Encoding`` says) or ``blocked`` (``blockHostnames``,
        #: ``blacklistIPs``), with ``error``.
        self.error_code: str | None = error_code
        #: (method, URL) as requested.
        self.request = request

    @property
    def text(self) -> str:
        """The body decoded as UTF-8 (invalid bytes are replaced)."""
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """The body parsed as JSON."""
        return _json.loads(self.body)

    @property
    def ok(self) -> bool:
        """True for a response with a status below 400."""
        return 0 < self.status < 400

    def raise_for_status(self) -> Response:
        """Raises ``RequestError`` if there was no response, ``StatusError`` for
        a 4xx/5xx status; otherwise returns the response."""
        method, url = self.request
        if self.error is not None:
            raise RequestError(f"{method} {url}: {self.error}", self)
        if self.status >= 400:
            raise StatusError(f"{method} {self.url}: status {self.status}", self)
        return self

    def __repr__(self) -> str:
        if self.error is not None:
            return f"<Response [error: {self.error}]>"
        return f"<Response [{self.status}]>"


#: What the native context's ``request`` calls take: method, URL, headers,
#: body, group, name, tags and timeout (seconds).
Prepared = tuple[
    str, str, "list[tuple[str, str]]", Union[bytes, None], str, Union[str, None],
    "list[tuple[str, str]]", Union[float, None],
]


def request(
    method: str,
    url: str,
    *,
    params: Params | None = None,
    headers: Mapping[str, str] | None = None,
    data: Data | None = None,
    json: Any = None,
    tags: Mapping[str, str] | None = None,
    name: str | None = None,
    timeout: Timeout | None = None,
    files: Files | None = None,
    compression: str | None = None,
) -> Coroutine[Any, Any, Response]:
    """Sends a request; await it for the response.

    ``params`` are added to the URL's query. ``data`` is the body: bytes,
    text, or a mapping sent as a form; ``json`` is sent as JSON instead.
    ``files`` sends a ``multipart/form-data`` body: those files, plus
    ``data``'s fields. ``compression`` (``gzip``, ``deflate``, ``br``,
    ``zstd``, or several: ``"gzip, br"``) compresses the body and sets
    ``Content-Encoding``. Compressed responses are decoded.
    ``tags`` are added to the request's metric samples; ``name`` replaces the
    ``name`` tag (by default the URL), which groups URLs with IDs in them into
    one entry. ``timeout`` (default 60s) covers the whole request.
    """
    require_async("k9.http", "k9.sync.http")
    ctx = require_ctx("k9.http")
    prepared = prepare(
        method, url, params, headers, data, json, tags, name, timeout, files, compression
    )
    return _send(ctx, prepared)


def prepare(
    method: str,
    url: str,
    params: Params | None,
    headers: Mapping[str, str] | None,
    data: Data | None,
    json: Any,
    tags: Mapping[str, str] | None,
    name: str | None,
    timeout: Timeout | None,
    files: Files | None = None,
    compression: str | None = None,
) -> Prepared:
    """A request's arguments in the form the native context takes (shared
    with ``k9.sync.http``). The group is the one the request is made in."""
    if params:
        query = urlencode(list(params.items()) if isinstance(params, Mapping) else params)
        url = f"{url}{'&' if '?' in url else '?'}{query}"
    header_list = [(str(k), str(v)) for k, v in headers.items()] if headers else []
    if files:
        body: bytes | None = _multipart(data, json, files, header_list)
    else:
        body = _body(data, json, header_list)
    if compression and body is not None:
        try:
            body, encoding = _native_compress(body, compression)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from None
        header_list = [(k, v) for k, v in header_list if k.lower() != "content-encoding"]
        header_list.append(("Content-Encoding", encoding))
    seconds = timeout.total_seconds() if isinstance(timeout, datetime.timedelta) else timeout
    tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
    return (method, url, header_list, body, current_group.get(), name, tag_list, seconds)


async def _send(ctx: Context, prepared: Prepared) -> Response:
    method, url = prepared[0], prepared[1]
    try:
        handle = ctx.request(*prepared)
    except ValueError as exc:
        raise InvalidRequest(f"{method} {url}: {exc}") from None
    loop = asyncio.get_running_loop()
    future: asyncio.Future[tuple[Any, ...]] = loop.create_future()
    pending[handle.job] = future
    try:
        payload = await future
    except asyncio.CancelledError:
        handle.abort()
        raise
    finally:
        pending.pop(handle.job, None)
    return finish(ctx, payload, method, url)


def finish(ctx: Context, payload: tuple[Any, ...], method: str, url: str) -> Response:
    """The response for ``payload``; with the ``throw`` option, raises
    ``RequestError`` if there was none."""
    res = Response(payload, (method.upper(), url))
    if res.error is not None and ctx.throw:
        raise RequestError(f"{method.upper()} {url}: {res.error}", res)
    return res


def _multipart(
    data: Data | None, json: Any, files: Files, headers: list[tuple[str, str]]
) -> bytes:
    if json is not None:
        raise InvalidRequest("pass either json or files, not both")
    if data is not None and not isinstance(data, Mapping):
        raise InvalidRequest("with files, data must be a mapping of form fields")
    parts: list[tuple[str, str | None, str | None, bytes]] = [
        (str(k), None, None, str(v).encode()) for k, v in (data or {}).items()
    ]
    for field, spec in files.items():
        content_type = None
        if isinstance(spec, tuple):
            filename, content, *rest = spec
            content_type = rest[0] if rest else None
        else:
            content = spec
            filename = os.path.basename(getattr(spec, "name", "") or "") or str(field)
        if hasattr(content, "read"):
            content = content.read()
        if isinstance(content, str):
            content = content.encode()
        parts.append((str(field), str(filename), content_type, bytes(content)))
    content_type_header, body = _native_multipart(parts)
    headers[:] = [(k, v) for k, v in headers if k.lower() != "content-type"]
    headers.append(("Content-Type", content_type_header))
    return body


def _body(data: Data | None, json: Any, headers: list[tuple[str, str]]) -> bytes | None:
    def default_type(content_type: str) -> None:
        if not any(k.lower() == "content-type" for k, _ in headers):
            headers.append(("Content-Type", content_type))

    if json is not None:
        if data is not None:
            raise InvalidRequest("pass either data or json, not both")
        default_type("application/json")
        return _json.dumps(json).encode()
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode()
    default_type("application/x-www-form-urlencoded")
    return urlencode(list(data.items())).encode()


def get(url: str, **kwargs: Unpack[RequestOptions]) -> Coroutine[Any, Any, Response]:
    """``request("GET", url, ...)``."""
    return request("GET", url, **kwargs)


def head(url: str, **kwargs: Unpack[RequestOptions]) -> Coroutine[Any, Any, Response]:
    """``request("HEAD", url, ...)``."""
    return request("HEAD", url, **kwargs)


def options(url: str, **kwargs: Unpack[RequestOptions]) -> Coroutine[Any, Any, Response]:
    """``request("OPTIONS", url, ...)``."""
    return request("OPTIONS", url, **kwargs)


def delete(url: str, **kwargs: Unpack[RequestOptions]) -> Coroutine[Any, Any, Response]:
    """``request("DELETE", url, ...)``."""
    return request("DELETE", url, **kwargs)


def post(
    url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]
) -> Coroutine[Any, Any, Response]:
    """``request("POST", url, data=data, ...)``."""
    return request("POST", url, data=data, **kwargs)


def put(
    url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]
) -> Coroutine[Any, Any, Response]:
    """``request("PUT", url, data=data, ...)``."""
    return request("PUT", url, data=data, **kwargs)


def patch(
    url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]
) -> Coroutine[Any, Any, Response]:
    """``request("PATCH", url, data=data, ...)``."""
    return request("PATCH", url, data=data, **kwargs)
