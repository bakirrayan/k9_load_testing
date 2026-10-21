"""Scripts with plain `def` functions: one thread per VU, the blocking
`k9.sync` API, sync hooks, and interruption."""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import Server
from k9test import iterations, k9, metric, report, run, write


def out(tmp_path: Path) -> str:
    return f"OUT={tmp_path / 'out.json'}"


def test_sync_vus_run_on_their_own_threads_with_real_http(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json, threading
        import k9
        from k9.sync import http, sleep

        options = {
            "scenarios": {"s": {"executor": "per-vu-iterations", "vus": 4, "iterations": 3}},
            "thresholds": {"http_reqs{group:::api}": ["count==12"], "checks": ["rate==1"]},
        }
        threads = {}

        def setup():
            return {"base": k9.env["BASE"]}

        def default(data):
            threads.setdefault(k9.vu.id, set()).add(threading.get_ident())
            with k9.group("api"):
                res = http.post(data["base"] + "/echo", json={"vu": k9.vu.id}, params={"i": k9.vu.iteration})
            k9.check(res, {"echoed": lambda r: r.json()["body"] == json.dumps({"vu": k9.vu.id})})
            sleep(0.01)

        def teardown(data):
            ids = [sorted(t) for t in threads.values()]
            with open(k9.env["OUT"], "w") as f:
                json.dump({
                    "one_thread_per_vu": all(len(t) == 1 for t in ids),
                    "distinct": len({t[0] for t in ids}),
                    "main": threading.main_thread().ident in {t[0] for t in ids},
                }, f)
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr + result.stdout
    assert "ERROR" not in result.stderr, result.stderr
    assert iterations(summary) == 12
    assert report(tmp_path) == {"one_thread_per_vu": True, "distinct": 4, "main": False}


def test_async_and_sync_scenarios_share_a_script(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9
        from k9.sync import http as sync_http

        options = {"scenarios": {
            "fast": {"executor": "shared-iterations", "vus": 3, "iterations": 9},
            "legacy": {"executor": "shared-iterations", "vus": 2, "iterations": 4, "exec": "legacy"},
        }, "thresholds": {
            "http_reqs{scenario:fast}": ["count==9"], "http_reqs{scenario:legacy}": ["count==4"],
        }}

        async def default():
            await k9.http.get(k9.env["BASE"] + "/ok")

        def legacy():
            sync_http.get(k9.env["BASE"] + "/ok")
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert iterations(summary) == 13


def test_sync_vu_init_runs_on_the_vus_thread(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json, threading
        import k9
        from k9.sync import http

        options = {"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 3, "iterations": 2}},
                   "thresholds": {"http_reqs{group:::vu_init}": ["count==3"]}}
        seen = []

        def vu_init():
            http.get(k9.env["BASE"] + "/ok")
            k9.vu.state["init_thread"] = threading.get_ident()

        def default():
            seen.append(k9.vu.state["init_thread"] == threading.get_ident())

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, _ = run(script, "-e", f"BASE={server.url('')}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == [True] * 6


def test_sync_hooks_dont_block_the_event_loop(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import time
        import k9

        options = {"vus": 20, "duration": "1s"}

        def vu_init():
            time.sleep(0.5)  # 20 VUs × 0.5 s would take 10 s if they ran one after another

        async def default():
            await k9.sleep(0.1)
        """,
    )
    started = time.monotonic()
    result, summary = run(script)
    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 5
    assert iterations(summary) >= 20 * 3


def test_interrupted_sync_vus_stop_at_once_even_mid_request(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import datetime, json
        import k9
        from k9.sync import http, sleep

        options = {"scenarios": {"s": {"executor": "constant-vus", "vus": 4, "duration": "300ms",
                                       "gracefulStop": datetime.timedelta(milliseconds=200)}}}
        interrupted = []

        def default():
            try:
                if k9.vu.id % 2:
                    http.get(k9.env["BASE"] + "/delay/900")
                else:
                    sleep(30)
            except k9.Interrupted:
                interrupted.append(k9.vu.id)
                raise

        def teardown():
            k9.sync.sleep(1)  # an unaborted request would finish (and be recorded) by now
            with open(k9.env["OUT"], "w") as f:
                json.dump(sorted(interrupted), f)
        """,
    )
    started = time.monotonic()
    result, summary = run(script, "-e", f"BASE={server.url('')}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 5
    assert "0 complete and 4 interrupted iterations" in result.stdout
    assert report(tmp_path) == [1, 2, 3, 4]
    assert "http_reqs" not in summary["metrics"], "the requests were aborted"


def test_a_sync_setup_past_its_timeout_doesnt_hold_up_the_exit(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import time
        options = {"setupTimeout": "200ms"}

        def setup():
            time.sleep(30)

        def default():
            pass
        """,
    )
    started = time.monotonic()
    result = k9("run", script)
    assert result.returncode == 100, result.stderr
    assert time.monotonic() - started < 5


def test_mixing_up_the_async_and_sync_apis_is_explained(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"scenarios": {
            "a": {"executor": "shared-iterations", "iterations": 1},
            "b": {"executor": "shared-iterations", "iterations": 1, "exec": "blocking_in_async"},
            "c": {"executor": "shared-iterations", "iterations": 1, "exec": "sleep_in_sync"},
        }}

        def default():
            k9.http.get(k9.env["BASE"] + "/ok")

        async def blocking_in_async():
            k9.sync.http.get(k9.env["BASE"] + "/ok")

        def sleep_in_sync():
            k9.sleep(1)
        """,
    )
    result = k9("run", script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    stderr = result.stderr
    assert "ContextError: k9.http is async: in a plain def function use k9.sync.http" in stderr
    assert "ContextError: k9.sync.http blocks the event loop: in an async def function use await k9.http" in stderr
    assert "ContextError: k9.sleep is async: in a plain def function use k9.sync.sleep" in stderr


def test_sync_iteration_exceptions_are_reported_like_async_ones(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"vus": 2, "iterations": 6}

        def default():
            if k9.vu.iteration % 2:
                raise ValueError(f"bad {k9.vu.iteration % 2}")
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0
    assert iterations(summary) == 6
    assert result.stderr.count("Traceback (most recent call last)") == 1
    assert "script raised" in result.stderr and "ValueError: bad 1 (script.py:7 in default)" in result.stderr


def test_more_than_500_sync_vus_get_a_warning(tmp_path: Path) -> None:
    script = write(tmp_path, 'options = {"vus": 501, "iterations": 501}\ndef default():\n    pass\n')
    result, summary = run(script)
    assert result.returncode == 0, result.stderr
    assert iterations(summary) == 501
    assert "WARN 501 VUs run plain def functions" in result.stderr
    small = write(tmp_path, 'options = {"vus": 500, "iterations": 500}\ndef default():\n    pass\n',
                  name="small.py")
    assert "WARN" not in k9("run", small).stderr


def test_hundreds_of_sync_vus_make_real_requests(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9
        from k9.sync import http

        options = {"vus": 300, "duration": "3s"}

        def default():
            res = http.get(k9.env["BASE"] + "/delay/100")
            k9.check(res, {"ok": lambda r: r.status == 200})
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    # 300 VUs × 3 s / ~100 ms ≈ 9,000; leave room for slow CI runners.
    assert iterations(summary) >= 300 * 15, metric(summary, "iterations")
    assert metric(summary, "checks")["fails"] == 0
    assert 95 <= metric(summary, "http_req_duration")["med"] <= 150


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")
def test_ctrl_c_stops_blocked_sync_vus_and_runs_teardown(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"vus": 3, "duration": "1m"}

        def teardown():
            print("teardown ran")

        def default():
            k9.sync.sleep(30)
        """,
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "k9", "run", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
    )
    time.sleep(1.5)
    started = time.monotonic()
    proc.send_signal(signal.SIGINT)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 105, stderr
    assert time.monotonic() - started < 5
    assert "0 complete and 3 interrupted iterations" in stdout
    assert "teardown ran" in stdout
