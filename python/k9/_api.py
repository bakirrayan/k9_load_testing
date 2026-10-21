"""``k9.check``, ``k9.sleep``, ``k9.group`` and ``k9.text_summary``."""

from __future__ import annotations

import asyncio
import datetime
import time
from collections.abc import Callable, Coroutine, Mapping
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, TypeVar, Union

from k9._context import current_group, require_async, require_ctx
from k9._errors import ContextError

T = TypeVar("T")

#: A check: a predicate on the checked value, or a plain result.
Check = Union[Callable[[T], Any], bool]


def check(
    value: T,
    checks: Mapping[str, Check[T]],
    tags: Mapping[str, str] | None = None,
) -> bool:
    """Runs each named check on ``value`` and records the results in the
    ``checks`` metric (k6's ``check()``); returns True if all passed.

    A check is a function of ``value`` whose result is taken as true or false,
    or a plain bool. Failed checks don't stop the iteration; an exception
    raised by a check function does::

        k9.check(res, {
            "status is 200": lambda r: r.status == 200,
            "has a token": lambda r: "token" in r.json(),
        })
    """
    ctx = require_ctx("k9.check()")
    group = current_group.get()
    tag_list = [(str(k), str(v)) for k, v in tags.items()] if tags else []
    all_passed = True
    for name, predicate in checks.items():
        passed = bool(predicate(value)) if callable(predicate) else bool(predicate)
        ctx.check(name, passed, group, tag_list)
        all_passed = all_passed and passed
    return all_passed


def sleep(seconds: float | datetime.timedelta) -> Coroutine[Any, Any, None]:
    """Pauses this VU (k6's ``sleep()``); other VUs keep running. Await it;
    plain ``def`` functions use ``k9.sync.sleep``."""
    require_async("k9.sleep", "k9.sync.sleep")
    if isinstance(seconds, datetime.timedelta):
        seconds = seconds.total_seconds()
    return asyncio.sleep(seconds)


class group:
    """Tags the requests and checks made inside it with a group, and records
    how long it took in ``group_duration`` (k6's ``group()``)::

        with k9.group("login"):
            await k9.http.post(url, json=credentials)

    Groups nest: an inner group's tag is ``::outer::inner``. ``async with``
    works too.
    """

    def __init__(self, name: str) -> None:
        if not name or "::" in name:
            raise ValueError(f"invalid group name {name!r}: it must be non-empty, without '::'")
        self.name = name
        self._entered: list[tuple[Token[str], str, float]] = []

    def __enter__(self) -> group:
        require_ctx("k9.group()")
        path = f"{current_group.get()}::{self.name}"
        self._entered.append((current_group.set(path), path, time.perf_counter()))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        token, path, started = self._entered.pop()
        current_group.reset(token)
        require_ctx("k9.group()").group_duration(path, time.perf_counter() - started)

    async def __aenter__(self) -> group:
        return self.__enter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.__exit__(exc_type, exc, tb)


#: The default summary text, while ``handle_summary()`` runs.
summary_text: ContextVar[str] = ContextVar("k9_summary_text")


def text_summary() -> str:
    """The end-of-test summary k9 prints by default; only available inside
    ``handle_summary()``, e.g. to keep it on stdout while also writing a file::

        def handle_summary(data):
            return {"stdout": k9.text_summary(), "summary.json": json.dumps(data)}
    """
    text = summary_text.get(None)
    if text is None:
        raise ContextError("k9.text_summary() is only available inside handle_summary()")
    return text
