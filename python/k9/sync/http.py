"""``k9.sync.http``: blocking requests for plain ``def`` functions. Same
functions and arguments as ``k9.http``, but they return the ``Response``."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from k9._context import require_ctx, require_sync
from k9._errors import InvalidRequest
from k9.http import Data, Files, Params, RequestOptions, Response, Timeout, finish, prepare

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Unpack
    else:
        from typing_extensions import Unpack

__all__ = ["delete", "get", "head", "options", "patch", "post", "put", "request"]


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
) -> Response:
    """Sends a request and waits for its response; see ``k9.http.request``."""
    require_sync("k9.sync.http", "await k9.http")
    ctx = require_ctx("k9.sync.http")
    prepared = prepare(
        method, url, params, headers, data, json, tags, name, timeout, files, compression
    )
    try:
        payload = ctx.request_blocking(*prepared)
    except ValueError as exc:
        raise InvalidRequest(f"{method} {prepared[1]}: {exc}") from None
    return finish(ctx, payload, method, prepared[1])


def get(url: str, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("GET", url, ...)``."""
    return request("GET", url, **kwargs)


def head(url: str, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("HEAD", url, ...)``."""
    return request("HEAD", url, **kwargs)


def options(url: str, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("OPTIONS", url, ...)``."""
    return request("OPTIONS", url, **kwargs)


def delete(url: str, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("DELETE", url, ...)``."""
    return request("DELETE", url, **kwargs)


def post(url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("POST", url, data=data, ...)``."""
    return request("POST", url, data=data, **kwargs)


def put(url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("PUT", url, data=data, ...)``."""
    return request("PUT", url, data=data, **kwargs)


def patch(url: str, data: Data | None = None, **kwargs: Unpack[RequestOptions]) -> Response:
    """``request("PATCH", url, data=data, ...)``."""
    return request("PATCH", url, data=data, **kwargs)
