"""Run context visible to scripts: ``k9.env``, ``k9.vu``, and the state that
``k9.http``, ``check``, ``group`` and metrics record through."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator, Mapping
from contextvars import ContextVar
from typing import Any

from k9._errors import ContextError
from k9._native import Context


class VuState:
    """What the runtime knows about one VU (internal)."""

    __slots__ = ("id", "scenario", "exec_name", "env", "iteration", "data", "ctx", "state")

    def __init__(
        self,
        id: int,
        scenario: str,
        exec_name: str,
        env: dict[str, str],
        data: Any,
        ctx: Context,
    ) -> None:
        self.id = id
        self.scenario = scenario
        self.exec_name = exec_name
        self.env = env
        self.iteration = 0
        self.data = data
        self.ctx = ctx
        self.state: dict[str, Any] = {}


# Each iteration or hook runs as its own asyncio task, and tasks copy the
# context, so setting these inside one is local to it.
current_vu: ContextVar[VuState] = ContextVar("k9_current_vu")
#: Where requests, checks and metrics go: the VU's (or hook's) native context.
current_ctx: ContextVar[Context] = ContextVar("k9_current_ctx")
#: The group path requests and checks are tagged with, e.g. ``::login``.
current_group: ContextVar[str] = ContextVar("k9_current_group", default="")

#: Whether the running script code is ``async def`` (on the event loop) or
#: plain ``def`` (on its own thread); unset outside script code.
current_mode: ContextVar[str] = ContextVar("k9_current_mode", default="")

#: Requests in flight: job id → the future their response resolves.
pending: dict[int, asyncio.Future[Any]] = {}

#: Open WebSockets (``k9.ws.WebSocket``) by session id, for their events.
ws_sessions: dict[int, Any] = {}


def require_ctx(what: str) -> Context:
    ctx = current_ctx.get(None)
    if ctx is None:
        raise ContextError(
            f"{what} can only be used while k9 runs a VU iteration or a "
            "setup/teardown/vu_init hook, not at module level"
        )
    return ctx


def require_async(what: str, sync_version: str) -> None:
    """Refuses an async API in a plain ``def`` function, pointing to its sync twin."""
    if current_mode.get() == "sync":
        raise ContextError(f"{what} is async: in a plain def function use {sync_version}")


def require_sync(what: str, async_version: str) -> None:
    """Refuses a blocking API in an ``async def`` function, which would stall
    every VU on the event loop."""
    if current_mode.get() == "async":
        raise ContextError(
            f"{what} blocks the event loop: in an async def function use {async_version}"
        )


class Env(Mapping[str, str]):
    """``k9.env``: the process environment, plus ``-e KEY=VALUE`` flags, plus
    the running scenario's ``env`` (each layer overriding the one before, like
    k6's ``__ENV``). Setting ``-e`` doesn't change ``os.environ``."""

    def __init__(self) -> None:
        self._flags: dict[str, str] = {}

    def _set_flags(self, flags: Mapping[str, str]) -> None:
        self._flags = dict(flags)

    def _layers(self) -> list[Mapping[str, str]]:
        layers: list[Mapping[str, str]] = []
        vu = current_vu.get(None)
        if vu is not None:
            layers.append(vu.env)
        layers.append(self._flags)
        layers.append(os.environ)
        return layers

    def __getitem__(self, key: str) -> str:
        for layer in self._layers():
            if key in layer:
                return layer[key]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for layer in self._layers():
            for key in layer:
                if key not in seen:
                    seen.add(key)
                    yield key

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __repr__(self) -> str:
        return f"k9.env({dict(self)!r})"


class Vu:
    """``k9.vu``: the VU running the current iteration (or ``vu_init``)."""

    @staticmethod
    def _state() -> VuState:
        state = current_vu.get(None)
        if state is None:
            raise ContextError("k9.vu is only available while a VU runs an iteration or vu_init")
        return state

    @property
    def id(self) -> int:
        """Unique across the run's scenarios, starting at 1 (k6's ``__VU``)."""
        return self._state().id

    @property
    def scenario(self) -> str:
        """Name of the scenario the VU belongs to."""
        return self._state().scenario

    @property
    def iteration(self) -> int:
        """This VU's iteration number in its scenario, starting at 0."""
        return self._state().iteration

    @property
    def state(self) -> dict[str, Any]:
        """A dict that belongs to this VU and lasts for the whole run, e.g. for
        a session token that ``vu_init()`` stores and iterations use."""
        return self._state().state

    def __repr__(self) -> str:
        state = current_vu.get(None)
        if state is None:
            return "k9.vu(<outside an iteration>)"
        return f"k9.vu(id={state.id}, scenario={state.scenario!r}, iteration={state.iteration})"


env = Env()
vu = Vu()
