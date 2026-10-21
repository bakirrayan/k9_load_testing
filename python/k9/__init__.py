"""K9: load testing with k6's UX, a Rust engine and a Python API.

A script is a Python file with an ``options`` dict and an ``async def
default()`` that each VU runs in a loop::

    import k9

    options = {"vus": 10, "duration": "30s"}

    async def default():
        res = await k9.http.get("https://test.k6.io/")
        k9.check(res, {"status is 200": lambda r: r.status == 200})
        await k9.sleep(1)

Run it with ``k9 run script.py``. Plain ``def default()`` works too, with the
blocking APIs in ``k9.sync`` (each VU then runs on its own thread).
"""

from k9 import grpc, http, metrics, plan, sync, ws
from k9._api import check, group, sleep, text_summary
from k9._context import env, vu
from k9._errors import (
    ConfigError,
    ContextError,
    HttpError,
    Interrupted,
    InvalidRequest,
    K9Error,
    MetricError,
    RequestError,
    RunError,
    ScriptError,
    StatusError,
    ThresholdsFailed,
)
from k9._native import __version__
from k9._run import RunResult, run

__all__ = [
    "ConfigError",
    "ContextError",
    "HttpError",
    "Interrupted",
    "InvalidRequest",
    "K9Error",
    "MetricError",
    "RequestError",
    "RunError",
    "RunResult",
    "ScriptError",
    "StatusError",
    "ThresholdsFailed",
    "__version__",
    "check",
    "env",
    "group",
    "grpc",
    "http",
    "metrics",
    "plan",
    "run",
    "sleep",
    "sync",
    "text_summary",
    "vu",
    "ws",
]
