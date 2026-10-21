"""Loads test scripts and runs their code: ``async def`` code on one asyncio
event loop, plain ``def`` code on threads (one per VU).

k9-py drives this module: ``load()`` imports the script, then the Rust side
sends batches of commands to ``Runtime.submit()`` from its bridge thread. Each
iteration and hook runs as an asyncio task on the loop thread, and reports back
through a ``reply(error, details, result)`` callable when it finishes.
Responses to ``k9.http`` requests arrive the same way (``http_done``) and
resolve the future the request awaits.

VUs whose iteration function is a plain ``def`` each get a thread; their
commands go straight from the bridge thread to it, not through the loop.
Sync hooks run on a thread of their own too, so they never stall the loop.
"""

from __future__ import annotations

import asyncio
import contextvars
import datetime
import functools
import importlib.util
import inspect
import json
import queue
import socket
import sys
import threading
import traceback
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from k9 import metrics
from k9 import plan as k9_plan
from k9._api import summary_text
from k9._context import (
    VuState,
    current_ctx,
    current_group,
    current_mode,
    current_vu,
    env,
    pending,
    ws_sessions,
)
from k9._native import Context

HOOKS = ("setup", "teardown", "vu_init", "handle_summary")

_PACKAGE_DIR = str(Path(__file__).resolve().parent)

Reply = Callable[..., None]


class LoadError(Exception):
    """The script couldn't be loaded; the message is ready to show."""


class Loaded:
    def __init__(
        self,
        options_json: str | None,
        functions: list[tuple[str, bool]],
        hooks: list[str],
        metrics: list[tuple[str, str, bool]],
        runtime: Runtime,
        plan_json: str | None = None,
    ) -> None:
        #: A Tier 1 script's ``k9.plan.Plan``, in the plan format.
        self.plan_json = plan_json
        self.options_json = options_json
        self.functions = functions
        self.hooks = hooks
        #: Custom metrics: (name, kind, is_time).
        self.metrics = metrics
        self.runtime = runtime


def load(path: str, env_flags: list[tuple[str, str]]) -> Loaded:
    """Imports the script at ``path`` (its init code runs once, here)."""
    env._set_flags(dict(env_flags))
    script = Path(path).resolve()
    if not script.is_file():
        raise LoadError(f"no such file: {path}")
    # Let the script import modules that sit next to it.
    folder = str(script.parent)
    if folder not in sys.path:
        sys.path.insert(0, folder)
    spec = importlib.util.spec_from_file_location("__k9_script__", script)
    if spec is None or spec.loader is None:
        raise LoadError(f"can't import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    metrics._reset()
    try:
        spec.loader.exec_module(module)
    except BaseException as exc:  # noqa: BLE001 - reported to the user as is
        raise LoadError(format_traceback(exc)) from None
    declared = metrics._freeze()

    options = getattr(module, "options", None)
    options_json = None
    if options is not None:
        if not isinstance(options, dict):
            raise LoadError(f"`options` must be a dict, not {type(options).__name__}")
        try:
            options_json = json.dumps(options, default=_json_default)
        except (TypeError, ValueError) as exc:
            raise LoadError(f"`options` can't be converted to JSON: {exc}") from None

    functions: list[tuple[str, bool]] = []
    for name, value in vars(module).items():
        if name.startswith("_") or name in HOOKS:
            continue
        if inspect.isfunction(value) and value.__module__ == module.__name__:
            functions.append((name, inspect.iscoroutinefunction(value)))
    hooks = [name for name in HOOKS if callable(getattr(module, name, None))]
    plan_json = _tier1_plan(module, options, hooks)
    return Loaded(
        options_json, functions, hooks, declared, Runtime(module, str(script)), plan_json
    )


def _tier1_plan(module: Any, options: Any, hooks: list[str]) -> str | None:
    """The script's module-level ``plan``, if it's a :class:`k9.plan.Plan`."""
    plan = getattr(module, "plan", None)
    if not isinstance(plan, k9_plan.Plan):
        return None
    if callable(getattr(module, "default", None)):
        raise LoadError(
            "the script defines both `plan` (Tier 1) and `default()` (Tier 3); keep one"
        )
    if hooks:
        names = ", ".join(f"{h}()" for h in hooks)
        raise LoadError(f"Tier 1 plans don't run {names}: no Python runs during the test")
    spec = plan.to_dict()
    if options is not None:
        if "options" in spec:
            raise LoadError("`options` twice: in the module and in `Plan(options=…)`")
        spec["options"] = options
    try:
        return json.dumps(spec, default=_json_default)
    except (TypeError, ValueError) as exc:
        raise LoadError(f"`plan` can't be converted to JSON: {exc}") from None


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime.timedelta):
        return value / datetime.timedelta(milliseconds=1)  # k9 reads numbers as ms
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _takes_argument(fn: Callable[..., Any]) -> bool:
    """Whether ``fn`` can be passed the setup data (k6 passes it to every hook
    and iteration function; Python functions may leave the parameter out)."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL) for p in params
    )


def _user_frames(tb: traceback.StackSummary) -> list[traceback.FrameSummary]:
    return [
        frame
        for frame in tb
        if not frame.filename.startswith(_PACKAGE_DIR)
        and not frame.filename.startswith("<frozen importlib")
    ]


def format_traceback(exc: BaseException) -> str:
    """A traceback without k9's and importlib's own frames."""
    frames = _user_frames(traceback.extract_tb(exc.__traceback__))
    lines = ["Traceback (most recent call last):\n"] if frames else []
    lines += traceback.format_list(frames)
    lines += traceback.format_exception_only(type(exc), exc)
    return "".join(lines).rstrip()


def describe(exc: BaseException) -> str:
    """One line for an exception raised by an iteration, e.g.
    ``ZeroDivisionError: division by zero (script.py:12 in default)``."""
    message = " ".join(str(exc).split())
    text = f"{type(exc).__name__}: {message}" if message else type(exc).__name__
    frames = _user_frames(traceback.extract_tb(exc.__traceback__))
    if frames:
        frame = frames[-1]
        text += f" ({Path(frame.filename).name}:{frame.lineno} in {frame.name})"
    return text


class Runtime:
    """One asyncio event loop, on its own thread, running every VU's iterations."""

    def __init__(self, module: ModuleType, path: str) -> None:
        self.module = module
        self.path = path
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.tasks: dict[int, asyncio.Task[None]] = {}
        self.vus: dict[int, VuState] = {}
        #: Job queues of the VUs that run plain ``def`` code on their own thread.
        self.sync_vus: dict[int, queue.SimpleQueue[tuple[Any, ...] | None]] = {}
        self.data_json = "null"
        #: The Rust side's command queue and the socket that signals it.
        self.commands: Any = None
        self.wake: socket.socket | None = None

    # --- called from Rust, once, on the first command -------------------------

    def connect(self, commands: Any) -> None:
        """Takes the Rust command queue: a byte on its socket means there are
        commands to take (on the loop thread, see ``_drain``)."""
        loop = self._start()
        self.commands = commands
        self.wake = socket.socket(fileno=commands.fileno())
        self.wake.setblocking(False)
        loop.call_soon_threadsafe(loop.add_reader, self.wake, self._drain)

    # --- on the loop thread, when woken ---------------------------------------

    def _drain(self) -> None:
        assert self.wake is not None
        try:
            while self.wake.recv(4096):
                pass
        except (BlockingIOError, InterruptedError):
            pass
        commands = self.commands.take()
        for_loop = [command for command in commands if not self._dispatch_sync(command)]
        if for_loop:
            self._apply(for_loop)

    def _dispatch_sync(self, command: tuple[Any, ...]) -> bool:
        """Hands a command for a plain-``def`` VU to its thread; False if the
        command is for the loop."""
        op = command[0]
        if op == "vu":
            _, vu_id, scenario, exec_name, scenario_env, ctx = command
            if inspect.iscoroutinefunction(getattr(self.module, exec_name)):
                return False
            data = json.loads(self.data_json)
            vu = VuState(vu_id, scenario, exec_name, scenario_env, data, ctx)
            jobs: queue.SimpleQueue[tuple[Any, ...] | None] = queue.SimpleQueue()
            self.vus[vu_id] = vu
            self.sync_vus[vu_id] = jobs
            threading.Thread(
                target=self._sync_vu, args=(vu, jobs), name=f"k9-vu-{vu_id}", daemon=True
            ).start()
            return True
        if op in ("iterate", "init"):
            jobs_of = self.sync_vus.get(command[2])
            if jobs_of is None:
                return False
            if op == "init" and inspect.iscoroutinefunction(self.module.vu_init):
                return False  # an async vu_init runs on the loop
            jobs_of.put(command)
            return True
        if op == "drop_vu":
            jobs_of = self.sync_vus.pop(command[1], None)
            if jobs_of is None:
                return False
            self.vus.pop(command[1], None)
            jobs_of.put(None)
            return True
        return False

    def close(self) -> None:
        for jobs in self.sync_vus.values():
            jobs.put(None)
        self.sync_vus.clear()
        if self.loop is None or self.thread is None:
            return
        if self.thread.is_alive():
            drained = asyncio.run_coroutine_threadsafe(self._cancel_all(), self.loop)
            try:
                drained.result(timeout=5)
            except Exception:  # noqa: BLE001 - best effort on shutdown
                pass
            if self.wake is not None:
                self.loop.call_soon_threadsafe(self.loop.remove_reader, self.wake)
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=5)
        if not self.loop.is_running():
            self.loop.close()
        if self.wake is not None:
            self.wake.close()
            self.wake = None

    # --- on the loop thread -------------------------------------------------

    def _start(self) -> asyncio.AbstractEventLoop:
        if self.loop is None:
            # A selector loop everywhere: it watches the wake-up socket
            # (Windows' default proactor loop can't).
            loop = asyncio.SelectorEventLoop()
            self.loop = loop
            self.thread = threading.Thread(
                target=self._run, args=(loop,), name="k9-asyncio", daemon=True
            )
            self.thread.start()
        return self.loop

    @staticmethod
    def _run(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def _apply(self, commands: list[tuple[Any, ...]]) -> None:
        for command in commands:
            op = command[0]
            if op == "http_done":
                _, job, payload = command
                future = pending.pop(job, None)
                if future is not None and not future.done():
                    future.set_result(payload)
            elif op == "ws_open":
                _, job, result = command
                self._ws_open(job, result)
            elif op == "ws_done":
                _, job, error = command
                future = pending.pop(job, None)
                if future is not None and not future.done():
                    future.set_result(error)
            elif op == "ws":
                _, session, (kind, a, b) = command
                socket = ws_sessions.get(session)
                if socket is not None:
                    socket._event(kind, a, b)
                    if kind == "end":
                        del ws_sessions[session]
            elif op == "iterate":
                _, job, vu_id, reply, fn_name, counted = command
                self._spawn(job, self._iterate(self.vus[vu_id], fn_name, counted), reply)
            elif op == "vu":
                _, vu_id, scenario, exec_name, scenario_env, ctx = command
                data = json.loads(self.data_json)
                self.vus[vu_id] = VuState(vu_id, scenario, exec_name, scenario_env, data, ctx)
            elif op == "init":
                _, job, vu_id, reply = command
                self._spawn(job, self._vu_init(self.vus[vu_id]), reply)
            elif op == "drop_vu":
                self.vus.pop(command[1], None)
            elif op == "hook":
                _, job, name, ctx, args, reply = command
                self._spawn(job, self._hook(name, ctx, args), reply, full_traceback=True)
            elif op == "cancel":
                task = self.tasks.get(command[1])
                if task is not None:
                    task.cancel()
            else:
                raise ValueError(f"unknown runtime command {op!r}")

    @staticmethod
    def _ws_open(job: int, result: Any) -> None:
        """Registers a new WebSocket before any of its events are applied."""
        future = pending.pop(job, None)
        if isinstance(result, tuple):  # (message, kind): it didn't open
            if future is not None and not future.done():
                future.set_result(result)
            return
        if future is None or future.done():  # nobody waits for it anymore
            result.close(1000, "")
            return
        from k9.ws import WebSocket

        socket = WebSocket(result)
        ws_sessions[result.session] = socket
        future.set_result(socket)

    def _spawn(self, job: int, coro: Any, reply: Reply, full_traceback: bool = False) -> None:
        assert self.loop is not None
        task: asyncio.Task[Any] = self.loop.create_task(coro)
        self.tasks[job] = task

        def done(task: asyncio.Task[None]) -> None:
            self.tasks.pop(job, None)
            if task.cancelled():
                reply("cancelled")
                return
            exc = task.exception()
            if exc is None:
                reply(None, None, task.result())
            elif full_traceback:
                reply(format_traceback(exc))
            else:
                # Counted by the one-line description; the traceback is shown
                # the first time it occurs.
                reply(describe(exc), format_traceback(exc))

        task.add_done_callback(done)

    # --- on a plain-def VU's thread -----------------------------------------

    def _sync_vu(self, vu: VuState, jobs: queue.SimpleQueue[tuple[Any, ...] | None]) -> None:
        """Runs a VU's ``vu_init`` and iterations, one job at a time, until
        the VU is dropped."""
        current_vu.set(vu)
        current_ctx.set(vu.ctx)
        current_mode.set("sync")
        while (command := jobs.get()) is not None:
            op, _job, _vu_id, reply = command[:4]
            try:
                if op == "init":
                    current_group.set("::vu_init")
                    fn = self.module.vu_init
                    fn(*((vu.data,) if _takes_argument(fn) else ()))
                else:
                    fn_name, counted = command[4:]
                    current_group.set("")
                    fn = getattr(self.module, fn_name)
                    try:
                        fn(*((vu.data,) if _takes_argument(fn) else ()))
                    finally:
                        vu.iteration += counted
            except BaseException as exc:  # noqa: BLE001 - reported, like async ones
                # (k9.Interrupted lands here too; nobody waits for that reply.)
                reply(describe(exc), format_traceback(exc))
            else:
                reply(None)

    # --- on the loop thread -------------------------------------------------

    async def _iterate(self, vu: VuState, fn_name: str, counted: bool) -> None:
        """Runs ``fn_name`` for ``vu``: an iteration if ``counted``, else
        its scenario's ``onStart``/``onStop``."""
        current_vu.set(vu)
        current_ctx.set(vu.ctx)
        current_mode.set("async")
        fn = getattr(self.module, fn_name)
        try:
            if _takes_argument(fn):
                await fn(vu.data)
            else:
                await fn()
        finally:
            vu.iteration += counted

    async def _vu_init(self, vu: VuState) -> None:
        current_vu.set(vu)
        current_ctx.set(vu.ctx)
        current_group.set("::vu_init")
        fn = self.module.vu_init
        await _call(fn, (vu.data,) if _takes_argument(fn) else ())

    async def _hook(
        self, name: str, ctx: Context | None, args: list[str]
    ) -> list[tuple[str, bytes]] | None:
        fn = getattr(self.module, name)
        if ctx is not None:
            current_ctx.set(ctx)
            current_group.set(ctx.root_group)
        if name == "handle_summary":
            data_json, text = args
            summary_text.set(text)
            return _summary_outputs(await _call(fn, (json.loads(data_json),)))
        call_args = (
            () if name == "setup" or not _takes_argument(fn) else (json.loads(self.data_json),)
        )
        result = await _call(fn, call_args)
        if name == "setup":
            try:
                # Like k6, VUs get their own copy of the data, via JSON; this
                # also keeps it ready to send to worker processes.
                self.data_json = json.dumps(result, default=_json_default)
            except (TypeError, ValueError) as exc:
                raise TypeError(f"setup() must return JSON-serializable data: {exc}") from None
        return None

    async def _cancel_all(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=2)


async def _call(fn: Callable[..., Any], args: tuple[Any, ...]) -> Any:
    """Calls a hook: ``async def`` ones on the loop, plain ``def`` ones on a
    thread of their own (so a slow one doesn't stall the loop)."""
    if inspect.iscoroutinefunction(fn):
        current_mode.set("async")
        try:
            return await fn(*args)
        finally:
            # The engine writes to the same terminal; keep the output in order.
            sys.stdout.flush()
    current_mode.set("sync")
    result = await _in_thread(fn, args)
    if inspect.isawaitable(result):  # a def that returns a coroutine
        current_mode.set("async")
        result = await result
    return result


async def _in_thread(fn: Callable[..., Any], args: tuple[Any, ...]) -> Any:
    """Runs ``fn`` on a new daemon thread, with the caller's context vars.
    Daemon, so a hook still stuck after its timeout can't keep k9 running."""
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()
    context = contextvars.copy_context()

    def settle(outcome: Callable[[], None]) -> None:
        if not future.done():  # not cancelled (e.g. by the hook's timeout)
            outcome()

    def target() -> None:
        # (functools.partial binds the value now: `except ... as exc` unbinds
        # `exc` when the block ends, so a closure over it would fail later.)
        outcome: Callable[[], None]
        try:
            outcome = functools.partial(future.set_result, context.run(fn, *args))
        except (KeyboardInterrupt, SystemExit) as exc:
            # These would stop the event loop if re-raised there.
            error = RuntimeError(f"{fn.__name__}() raised {type(exc).__name__}")
            error.__cause__ = exc
            outcome = functools.partial(future.set_exception, error)
        except BaseException as exc:  # noqa: BLE001 - re-raised in the awaiting task
            outcome = functools.partial(future.set_exception, exc)
        finally:
            sys.stdout.flush()
        try:
            loop.call_soon_threadsafe(settle, outcome)
        except RuntimeError:
            pass  # the loop is closed: the run is over

    threading.Thread(target=target, name=f"k9-{fn.__name__}", daemon=True).start()
    return await future


def _summary_outputs(result: Any) -> list[tuple[str, bytes]]:
    """``handle_summary()``'s return value as (target, content) pairs."""
    if result is None:
        return []
    if not isinstance(result, dict):
        raise TypeError(
            "handle_summary() must return a dict of {target: content}, where target is "
            f"'stdout', 'stderr' or a file path; got {type(result).__name__}"
        )
    outputs = []
    for target, content in result.items():
        if isinstance(content, str):
            content = content.encode()
        if not isinstance(target, str) or not isinstance(content, bytes):
            raise TypeError(
                f"handle_summary() returned {target!r}: {type(content).__name__}; "
                "targets must be str and contents str or bytes"
            )
        outputs.append((target, content))
    return outputs
