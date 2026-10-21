"""`k9 run --tui` in a real (pseudo) terminal: the dashboard takes the screen,
`q` stops the run, and the summary still prints."""

from __future__ import annotations

import os
import select
import signal
import struct
import sys
import time
from pathlib import Path

import pytest

from conftest import Server

pty = pytest.importorskip("pty")
fcntl = pytest.importorskip("fcntl")
termios = pytest.importorskip("termios")
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX pty")


def read_until(fd: int, needle: bytes, seconds: float, seen: bytearray) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        ready, _, _ = select.select([fd], [], [], 0.1)
        if ready:
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                return needle in seen
            if not chunk:
                return needle in seen
            seen.extend(chunk)
            if needle in seen:
                return True
    return needle in seen


def wait_reading(pid: int, fd: int, seconds: float, seen: bytearray) -> int:
    """Waits for the child to exit, reading what it writes meanwhile: with the
    pty's buffer full (small on macOS) it would block before exiting."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            return status
        ready, _, _ = select.select([fd], [], [], 0.1)
        if ready:
            try:
                seen.extend(os.read(fd, 65536))
            except OSError:
                pass
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError(f"k9 didn't exit: {seen[-2000:]!r}")


def test_the_dashboard_runs_and_q_stops_the_test(tmp_path: Path, server: Server) -> None:
    plan = tmp_path / "plan.yaml"
    plan.write_text(
        "options: { vus: 2, duration: 30s }\n"
        f"flows: {{ default: [ {{ request: {{ url: '{server.url('/delay/10')}',"
        " checks: { ok: { status: 200 } } } } ] }\n",
        encoding="utf-8",
    )
    pid, fd = pty.fork()
    if pid == 0:  # the child: k9 on the pty, 120×40
        fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
        os.environ["COLORTERM"] = "truecolor"
        os.environ.pop("NO_COLOR", None)
        os.execvp(sys.executable, [sys.executable, "-m", "k9", "run", "--tui", str(plan)])
    seen = bytearray()
    status = None
    try:
        # The alternate screen, then the dashboard with a first frame.
        assert read_until(fd, b"\x1b[?1049h", 20, seen), seen[-2000:]
        assert read_until(fd, b"Requests/s", 10, seen), seen[-2000:]
        assert read_until(fd, b"ok", 10, seen), seen[-2000:]
        assert b"\x1b[38;2;" in seen, "24-bit colours"
        os.write(fd, b"q")
        # Redraws only send changed cells, so look for the log line (all new).
        assert read_until(fd, b"stopping the test run", 10, seen), seen[-2000:]
        # Back to the normal screen, then k6's summary.
        assert read_until(fd, b"\x1b[?1049l", 40, seen), seen[-2000:]
        assert read_until(fd, b"http_req_duration", 10, seen), seen[-2000:]
        status = wait_reading(pid, fd, 20, seen)
    finally:
        if status is None:  # a failed assertion: don't leave k9 running
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        os.close(fd)
    # Stopped by the user: k6's exit code for an external abort, as with Ctrl-C.
    assert os.waitstatus_to_exitcode(status) == 105, seen[-3000:].decode(errors="replace")
