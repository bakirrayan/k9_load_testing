"""Tier 1: requests described as data, run by the engine with no Python.

Nothing here sends a request. Each function returns a spec, and a script
that defines a module-level ``plan`` hands the whole thing to the Rust engine
once, at load time. It's Tier 1 (declarative), like a YAML plan: the fastest
tier, since no Python runs during the test. For logic between requests, write
``default()`` with :mod:`k9.http` instead (Tier 3).

Values that change per request are ``{{ … }}`` templates, which the
references below produce as plain strings (f-strings work)::

    import k9
    import k9.plan as p

    users = p.feeder("users", file="users.csv", mode="per-vu")
    token = p.var("token")

    plan = p.Plan(
        [
            p.post(
                k9.env["BASE"] + "/login",
                json={"username": users.username, "password": users.password},
                extract={"token": p.from_json("$.token")},
            ),
            p.get(
                k9.env["BASE"] + "/me",
                headers={"Authorization": f"Bearer {token}"},
                checks={"ok": p.status(200)},
            ),
        ],
        options={"vus": 10, "duration": "30s"},
        feeders=[users],
    )

Names are checked when the run starts: using a variable no extractor sets,
or a column the feeder doesn't have, is an error that points at the step.
"""

from __future__ import annotations

import datetime
import json as _json
import re
from collections.abc import Mapping, Sequence
from os import PathLike
from typing import Any

__all__ = [
    "ITER",
    "VU",
    "Feeder",
    "Plan",
    "batch",
    "body_contains",
    "delete",
    "duration_below",
    "env",
    "feeder",
    "file",
    "from_header",
    "from_json",
    "from_regex",
    "get",
    "group",
    "grpc",
    "head",
    "patch",
    "post",
    "put",
    "random_int",
    "random_string",
    "request",
    "sleep",
    "status",
    "timestamp",
    "uuid",
    "var",
    "websocket",
    "ws_close",
    "ws_ping",
    "ws_receive",
    "ws_send",
]

Step = dict[str, Any]
Check = dict[str, Any]
Extractor = dict[str, str]
Duration = str | int | float | datetime.timedelta

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
_MODES = ("sequential", "random", "per-vu")

# ---------------------------------------------------------------------------
# References: template strings
# ---------------------------------------------------------------------------

#: The VU's id (1-based), k6's ``__VU``.
VU = "{{ __VU }}"
#: The VU's iteration number (0-based), k6's ``__ITER``.
ITER = "{{ __ITER }}"


def var(name: str) -> str:
    """A variable set by an ``extract`` earlier in the flow."""
    if not _IDENTIFIER.match(name) or name.startswith("__"):
        raise ValueError(f"{name!r} isn't a variable name (letters, digits and _)")
    return f"{{{{ {name} }}}}"


def env(name: str) -> str:
    """An environment variable, read when the request is sent (``__ENV``)."""
    return f"{{{{ __ENV.{name} }}}}"


def uuid() -> str:
    """A random UUID (version 4), new for each request."""
    return "{{ uuid() }}"


def timestamp() -> str:
    """The time the request is sent, in Unix milliseconds."""
    return "{{ timestamp() }}"


def random_int(min: int, max: int) -> str:  # noqa: A002 - reads best
    """A random whole number from ``min`` to ``max`` inclusive."""
    return f"{{{{ randomInt({int(min)}, {int(max)}) }}}}"


def random_string(length: int) -> str:
    """``length`` random letters and digits."""
    return f"{{{{ randomString({int(length)}) }}}}"


class Feeder:
    """Rows of test data; each iteration that uses it gets one row.

    Columns are template strings: ``users.username`` or ``users["username"]``.
    """

    def __init__(
        self,
        name: str,
        *,
        file: str | None = None,
        rows: Sequence[Mapping[str, Any]] | None = None,
        mode: str = "sequential",
    ) -> None:
        if not _IDENTIFIER.match(name):
            raise ValueError(f"{name!r} isn't a feeder name (letters, digits and _)")
        if (file is None) == (rows is None):
            raise ValueError(f"feeder {name!r}: give either `file` or `rows`")
        if mode not in _MODES:
            raise ValueError(f"feeder {name!r}: mode must be one of {', '.join(_MODES)}")
        self._name = name
        self._spec: dict[str, Any] = {"mode": mode}
        if file is not None:
            self._spec = {"file": str(file), **self._spec}
        else:
            self._spec = {"rows": [dict(row) for row in rows or ()], **self._spec}

    def __getattr__(self, column: str) -> str:
        if column.startswith("_"):
            raise AttributeError(column)
        return self[column]

    def __getitem__(self, column: str) -> str:
        return f"{{{{ {self._name}.{column} }}}}"

    def __repr__(self) -> str:
        return f"feeder({self._name!r}, {self._spec!r})"


def feeder(
    name: str,
    *,
    file: str | None = None,
    rows: Sequence[Mapping[str, Any]] | None = None,
    mode: str = "sequential",
) -> Feeder:
    """Test data from ``file`` (``.csv`` with a header row, or ``.json``, an
    array of objects; relative to the script) or inline ``rows``.

    ``mode``: ``sequential`` (the next row, split between workers), ``random``,
    or ``per-vu`` (VU *n* always gets row *n − 1*).
    """
    return Feeder(name, file=file, rows=rows, mode=mode)


# ---------------------------------------------------------------------------
# Checks and extractors
# ---------------------------------------------------------------------------


def status(*codes: int) -> Check:
    """The response status is one of ``codes``."""
    if not codes:
        raise ValueError("status() needs at least one code")
    return {"status": codes[0] if len(codes) == 1 else list(codes)}


def body_contains(text: str) -> Check:
    return {"bodyContains": text}


def duration_below(limit: Duration) -> Check:
    """``http_req_duration`` is below ``limit`` (seconds, or ``"200ms"``)."""
    return {"durationBelow": _duration(limit, "duration_below")}


def from_json(path: str) -> Extractor:
    """A JSON path into the body: ``$.user.name``, ``$.items[0]``, ``$['a-b']``."""
    return {"json": path}


def from_regex(pattern: str) -> Extractor:
    """The pattern's first group in the body (or the whole match)."""
    return {"regex": pattern}


def from_header(name: str) -> Extractor:
    return {"header": name}


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def request(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: str | None = None,
    json: Any = None,
    name: str | None = None,
    tags: Mapping[str, str] | None = None,
    checks: Mapping[str, Check] | None = None,
    extract: Mapping[str, Extractor] | None = None,
    multipart: Mapping[str, str | dict[str, str]] | None = None,
    compression: str | None = None,
) -> Step:
    """One request. ``json`` sends a value as a JSON body (with its
    ``Content-Type``); template strings inside it stay JSON strings, so use
    ``body`` to template a number. ``multipart`` sends a form instead: text
    fields (templates allowed) and ``file(...)`` parts. ``compression``
    (``gzip``, ``deflate``, ``br``, ``zstd`` or several, ``"gzip, br"``)
    compresses the body. ``name`` groups dynamic URLs in metrics (a templated
    URL is named after its template by default)."""
    method = method.upper()
    if method not in _METHODS:
        raise ValueError(f"method must be one of {', '.join(_METHODS)}, not {method!r}")
    if sum(x is not None for x in (body, json, multipart)) > 1:
        raise ValueError("give only one of `body`, `json` and `multipart`")
    spec: dict[str, Any] = {"method": method, "url": str(url)}
    all_headers = dict(headers or {})
    if json is not None:
        body = _json.dumps(json)
        if not any(k.lower() == "content-type" for k in all_headers):
            all_headers["Content-Type"] = "application/json"
    for key, value in [
        ("headers", all_headers),
        ("body", body),
        ("multipart", dict(multipart or {})),
        ("compression", compression),
        ("name", name),
        ("tags", dict(tags or {})),
        ("checks", dict(checks or {})),
        ("extract", dict(extract or {})),
    ]:
        if value:
            spec[key] = value
    for variable in spec.get("extract", {}):
        var(variable)
    return {"request": spec}


def file(
    path: str | PathLike[str], *, filename: str | None = None, content_type: str | None = None
) -> dict[str, str]:
    """A file part for ``multipart``: read once before the run (relative to
    the script). ``filename`` defaults to the file's name, ``content_type``
    to ``application/octet-stream``."""
    part = {"file": str(path)}
    if filename is not None:
        part["filename"] = filename
    if content_type is not None:
        part["contentType"] = content_type
    return part


def get(url: str, **kwargs: Any) -> Step:
    return request("GET", url, **kwargs)


def post(url: str, **kwargs: Any) -> Step:
    return request("POST", url, **kwargs)


def put(url: str, **kwargs: Any) -> Step:
    return request("PUT", url, **kwargs)


def patch(url: str, **kwargs: Any) -> Step:
    return request("PATCH", url, **kwargs)


def delete(url: str, **kwargs: Any) -> Step:
    return request("DELETE", url, **kwargs)


def head(url: str, **kwargs: Any) -> Step:
    return request("HEAD", url, **kwargs)


def sleep(duration: Duration) -> Step:
    """A pause: seconds, a :class:`~datetime.timedelta`, or ``"1m30s"``."""
    return {"sleep": _duration(duration, "sleep")}


def batch(requests: Sequence[Step]) -> Step:
    """Requests sent in parallel (k6's ``http.batch``), within the
    ``batch`` (default 20) and ``batchPerHost`` (default 6) options; each is
    recorded, checked and extracted from as usual once all have finished."""
    if not requests:
        raise ValueError("a batch needs at least one request")
    specs = []
    for step in requests:
        if not isinstance(step, dict) or set(step) != {"request"}:
            raise ValueError(f"a batch holds request steps (get(), post(), ...), not {step!r}")
        specs.append(step["request"])
    return {"batch": specs}


def group(name: str, steps: Sequence[Step]) -> Step:
    """Steps whose metrics are tagged ``group: ::name`` (and timed together)."""
    return {"group": {"name": name, "steps": list(steps)}}


def websocket(
    url: str,
    steps: Sequence[Step | str],
    *,
    headers: Mapping[str, str] | None = None,
    name: str | None = None,
    tags: Mapping[str, str] | None = None,
    checks: Mapping[str, Check] | None = None,
) -> Step:
    """A WebSocket session: the handshake to a ``ws://``/``wss://`` URL, then
    ``steps`` (``ws_send``, ``ws_receive``, ``ws_ping``, ``sleep``,
    ``ws_close``) in order, then a normal close. ``checks`` apply to the
    handshake: ``status`` (101, or 0 if it failed) and ``duration_below``."""
    spec: dict[str, Any] = {"url": str(url)}
    for key, value in [
        ("headers", dict(headers or {})),
        ("name", name),
        ("tags", dict(tags or {})),
        ("checks", dict(checks or {})),
    ]:
        if value:
            spec[key] = value
    spec["steps"] = list(steps)
    return {"websocket": spec}


def grpc(
    target: str,
    method: str,
    message: Mapping[str, Any] | None = None,
    *,
    messages: Sequence[Mapping[str, Any]] | None = None,
    protos: Sequence[str | PathLike[str]] | None = None,
    import_paths: Sequence[str | PathLike[str]] | None = None,
    plaintext: bool = False,
    metadata: Mapping[str, str] | None = None,
    name: str | None = None,
    tags: Mapping[str, str] | None = None,
    timeout: Duration | None = None,
    checks: Mapping[str, Check] | None = None,
    extract: Mapping[str, Extractor] | None = None,
) -> Step:
    """A unary gRPC call of ``method`` (``package.Service/Method``) on
    ``target`` (``host:port``, TLS unless ``plaintext``), recorded in
    ``grpc_req_duration``. ``protos`` (``.proto`` files, found in
    ``import_paths``) define it; without any, the server's reflection service
    is asked before the run. Strings in ``message`` and ``metadata`` values
    may hold templates. ``checks``: ``status`` (the gRPC code),
    ``body_contains`` (the response message as JSON) and ``duration_below``;
    ``extract``: ``from_json``/``from_regex`` (the message) or
    ``from_header`` (response metadata). For a streaming method, ``messages``
    are sent in order, then the stream is half-closed and every answer read;
    checks and extractors see the answers as a JSON array."""
    spec: dict[str, Any] = {"target": str(target), "method": str(method)}
    for key, value in [
        ("message", dict(message or {})),
        ("messages", [dict(m) for m in messages or ()]),
        ("plaintext", plaintext),
        ("protos", [str(f) for f in protos or ()]),
        ("importPaths", [str(d) for d in import_paths or ()]),
        ("metadata", dict(metadata or {})),
        ("name", name),
        ("tags", dict(tags or {})),
        ("timeout", None if timeout is None else _duration(timeout, "grpc")),
        ("checks", dict(checks or {})),
        ("extract", dict(extract or {})),
    ]:
        if value:
            spec[key] = value
    for variable in extract or {}:
        var(variable)
    return {"grpc": spec}


def ws_send(text: str) -> Step:
    """Sends a text message (templates allowed)."""
    return {"send": str(text)}


def ws_receive(
    *,
    timeout: Duration | None = None,
    checks: Mapping[str, Check] | None = None,
    extract: Mapping[str, Extractor] | None = None,
) -> Step:
    """Waits (default 60s) for the next message. ``checks``: ``body_contains``
    and ``duration_below`` (the wait); ``extract``: ``from_json`` or
    ``from_regex``. No message ends the session (and fails the checks)."""
    spec: dict[str, Any] = {}
    if timeout is not None:
        spec["timeout"] = _duration(timeout, "ws_receive")
    if checks:
        spec["checks"] = dict(checks)
    if extract:
        spec["extract"] = dict(extract)
        for variable in extract:
            var(variable)
    return {"receive": spec}


def ws_ping() -> str:
    """Sends a ping and waits for its pong (``ws_ping``)."""
    return "ping"


def ws_close(code: int = 1000, reason: str = "") -> Step:
    """Closes the WebSocket now; later steps are skipped."""
    return {"close": {"code": code, "reason": reason}}


def _duration(value: Duration, what: str) -> str:
    if isinstance(value, str):
        return value
    seconds = value.total_seconds() if isinstance(value, datetime.timedelta) else value
    if isinstance(seconds, bool) or not isinstance(seconds, int | float) or seconds < 0:
        raise ValueError(f"{what}() takes seconds ≥ 0, a timedelta or a string like '1s'")
    ms = seconds * 1000
    return f"{int(ms)}ms" if ms == int(ms) else f"{round(seconds * 1e6)}us"


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


class Plan:
    """What the engine runs: ``steps`` (the ``default`` flow) or named
    ``flows`` (a scenario's ``exec`` picks one), k6 ``options`` (or the
    script's module-level ``options``), and the ``feeders`` the flows use."""

    def __init__(
        self,
        steps: Sequence[Step] | None = None,
        *,
        flows: Mapping[str, Sequence[Step]] | None = None,
        options: Mapping[str, Any] | None = None,
        feeders: Sequence[Feeder] = (),
    ) -> None:
        if (steps is None) == (flows is None):
            raise ValueError("give a Plan either steps or flows")
        self.flows = {"default": list(steps)} if steps is not None else {
            name: list(f) for name, f in (flows or {}).items()
        }
        self.options = dict(options) if options is not None else None
        for f in feeders:
            if not isinstance(f, Feeder):
                raise TypeError(f"feeders must come from k9.plan.feeder(), not {f!r}")
        self.feeders = list(feeders)

    def to_dict(self) -> dict[str, Any]:
        """The plan in k9's plan format (as YAML or JSON plans are written)."""
        plan: dict[str, Any] = {}
        if self.options is not None:
            plan["options"] = self.options
        plan["flows"] = self.flows
        if self.feeders:
            plan["feeders"] = {f._name: f._spec for f in self.feeders}
        return plan
