"""Blocking twins of k9's async APIs, for plain ``def`` functions.

A script whose iteration function is a plain ``def`` runs each VU on its own
thread (``async def`` VUs share one event loop instead)::

    import k9
    from k9.sync import grpc, http, sleep, ws

    def default():
        res = http.get("https://test.k6.io/")
        k9.check(res, {"status is 200": lambda r: r.status == 200})
        sleep(1)

``k9.check``, ``k9.group`` and ``k9.metrics`` work the same in both kinds of
function. While these calls wait, other VUs run: the GIL is released. When a
VU is interrupted (``gracefulStop`` passed, Ctrl-C), its next k9 call raises
``k9.Interrupted``, and one that is waiting returns at once; so prefer
``k9.sync.sleep`` to ``time.sleep``.
"""

from __future__ import annotations

import datetime

from k9._context import require_ctx, require_sync
from k9.sync import grpc, http, ws

__all__ = ["grpc", "http", "sleep", "ws"]


def sleep(seconds: float | datetime.timedelta) -> None:
    """Pauses this VU (k6's ``sleep()``); other VUs keep running."""
    require_sync("k9.sync.sleep", "await k9.sleep")
    if isinstance(seconds, datetime.timedelta):
        seconds = seconds.total_seconds()
    require_ctx("k9.sync.sleep").sleep(seconds)
