"""`k9 run` / `k9 inspect` on Python scripts, end to end."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from k9test import iterations, k9, report, run, write


def test_runs_an_async_default_with_the_script_options(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import asyncio
        options = {"vus": 5, "iterations": 20}

        async def default():
            await asyncio.sleep(0.01)
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0, result.stderr
    assert iterations(summary) == 20
    assert "20 complete and 0 interrupted iterations" in result.stdout


def test_setup_data_reaches_iterations_and_teardown_as_copies(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 3, "iterations": 2}}}
        seen = []

        def setup():
            return {"token": "abc", "hits": 0}

        async def default(data):
            data["hits"] += 1  # each VU has its own copy
            seen.append((k9.vu.id, k9.vu.iteration, k9.vu.scenario, data["token"], data["hits"]))

        async def teardown(data):
            with open(k9.env["OUT"], "w") as f:
                json.dump({"seen": sorted(seen), "data": data}, f)
        """,
    )
    result, _ = run(script, "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr
    out = report(tmp_path)
    assert out["data"] == {"token": "abc", "hits": 0}, "teardown gets the original data"
    assert out["seen"] == [
        [vu, iteration, "s", "abc", iteration + 1] for vu in (1, 2, 3) for iteration in (0, 1)
    ]


def test_scenarios_run_their_exec_function_with_their_env(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"scenarios": {
            "browse": {"executor": "shared-iterations", "vus": 1, "iterations": 2},
            "buy": {"executor": "shared-iterations", "vus": 1, "iterations": 1,
                    "exec": "checkout", "env": {"ROLE": "buyer"}},
        }}
        seen = []

        async def default():
            seen.append(["default", k9.vu.scenario, k9.env.get("ROLE"), k9.env["FLAG"]])

        async def checkout():
            seen.append(["checkout", k9.vu.scenario, k9.env.get("ROLE"), k9.env["FLAG"]])

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(sorted(seen), f)
        """,
    )
    result, _ = run(script, "-e", f"OUT={tmp_path / 'out.json'}", "-e", "FLAG=on")
    assert result.returncode == 0, result.stderr
    assert report(tmp_path) == [
        ["checkout", "buy", "buyer", "on"],
        ["default", "browse", None, "on"],
        ["default", "browse", None, "on"],
    ]


def test_flags_override_script_options(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"vus": 2, "iterations": 4}
        async def default():
            pass
        """,
    )
    result, summary = run(script, "-i", "7")
    assert result.returncode == 0, result.stderr
    assert iterations(summary) == 7


def test_iteration_exceptions_are_counted_by_message_and_location(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"vus": 1, "iterations": 6}

        async def default():
            if k9.vu.iteration % 2:
                1 / 0
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0, result.stderr
    assert iterations(summary) == 6, "like k6, failed iterations still complete"
    assert (
        "ERROR script raised 3×: ZeroDivisionError: division by zero (script.py:7 in default)"
        in result.stderr
    )


def test_exceptions_while_loading_exit_107_with_the_script_traceback(tmp_path: Path) -> None:
    script = write(tmp_path, "options = {}\nx = undefined_name\n")
    result = k9("run", script)
    assert result.returncode == 107
    assert f'File "{script.resolve()}", line 2, in <module>' in result.stderr
    assert "NameError: name 'undefined_name' is not defined" in result.stderr
    assert "_runtime.py" not in result.stderr
    assert "importlib" not in result.stderr


def test_setup_exceptions_exit_107_without_running_iterations(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        async def setup():
            raise KeyError("token")

        async def default():
            raise AssertionError("must not run")
        """,
    )
    result, summary = run(script)
    assert result.returncode == 107, result.stderr
    assert "error: setup() failed: Traceback" in result.stderr
    assert "KeyError: 'token'" in result.stderr
    assert summary == {}


def test_setup_data_must_be_json_serializable(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        def setup():
            return object()

        async def default(data):
            pass
        """,
    )
    result = k9("run", script)
    assert result.returncode == 107
    assert "setup() must return JSON-serializable data" in result.stderr


def test_teardown_exceptions_exit_107_after_the_summary(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        async def default():
            pass

        def teardown():
            raise RuntimeError("cleanup failed")
        """,
    )
    result, summary = run(script)
    assert result.returncode == 107
    assert iterations(summary) == 1
    assert "ERROR teardown() failed: Traceback" in result.stderr
    assert "RuntimeError: cleanup failed" in result.stderr


def test_setup_timeout_exits_100(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import asyncio
        options = {"setupTimeout": "200ms"}

        async def setup():
            await asyncio.sleep(30)

        async def default():
            pass
        """,
    )
    started = time.monotonic()
    result = k9("run", script)
    assert result.returncode == 100, result.stderr
    assert "setup() timed out after 200ms" in result.stderr
    assert time.monotonic() - started < 10


def test_interrupted_iterations_are_cancelled_in_python(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import asyncio, datetime, json
        import k9

        options = {"scenarios": {"s": {"executor": "constant-vus", "vus": 4,
            "duration": "300ms", "gracefulStop": datetime.timedelta(milliseconds=200)}}}
        cancelled = []

        async def default():
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(k9.vu.id)
                raise

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(sorted(cancelled), f)
        """,
    )
    started = time.monotonic()
    result, _ = run(script, "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 10
    assert "0 complete and 4 interrupted iterations" in result.stdout
    assert report(tmp_path) == [1, 2, 3, 4]


def test_thousands_of_async_vus_share_one_event_loop(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import asyncio, json, threading
        import k9

        options = {"vus": 2000, "duration": "2s"}
        threads = set()

        async def default():
            threads.add(threading.get_ident())
            await asyncio.sleep(0.5)

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(len(threads), f)
        """,
    )
    result, summary = run(script, "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr
    # 2000 VUs × 4 iterations of 0.5s in 2s; allow for start-up on slow CI runners.
    assert iterations(summary) >= 2000 * 3, summary["metrics"]["iterations"]
    assert summary["metrics"]["vus_max"]["value"] == 2000
    assert report(tmp_path) == 1, "every iteration ran on the one loop thread"


def test_inspect_reports_the_tier_hooks_and_resolved_options(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"vus": 3, "duration": "10s", "thresholds": {"iteration_duration": ["p(95)<1000"]}}
        def setup(): pass
        async def default(): pass
        def helper(): pass
        """,
    )
    result = k9("inspect", script, "-u", "4")
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout)
    assert out["tier"] == "async"
    assert out["hooks"] == ["setup"]
    default = out["plan"]["options"]["scenarios"]["default"]
    assert (default["executor"], default["vus"], default["exec"]) == ("constant-vus", 4, "default")
    assert out["plan"]["options"]["thresholds"] == {"iteration_duration": ["p(95)<1000"]}

    sync = write(tmp_path, "def default(): pass\n", name="sync.py")
    assert json.loads(k9("inspect", sync).stdout)["tier"] == "sync"


def test_invalid_options_exit_104(tmp_path: Path) -> None:
    script = write(tmp_path, 'options = {"vus": 1, "duraton": "1s"}\nasync def default(): pass\n')
    result = k9("run", script)
    assert result.returncode == 104
    assert "duraton" in result.stderr


def test_missing_exec_function_is_reported(tmp_path: Path) -> None:
    script = write(tmp_path, "async def browse(): pass\n")
    result = k9("run", script)
    assert result.returncode == 104
    assert "the script has no function `default` (functions it defines: `browse`)" in result.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")
def test_ctrl_c_stops_the_run_runs_teardown_and_exits_105(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import asyncio
        options = {"vus": 3, "duration": "1m"}

        def teardown():
            print("teardown ran")

        async def default():
            await asyncio.sleep(5)
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
    proc.send_signal(signal.SIGINT)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 105, stderr
    assert "0 complete and 3 interrupted iterations" in stdout
    assert stdout.index("teardown ran") < stdout.index("vus_max"), "hook output is flushed"
    assert "KeyboardInterrupt" not in stderr
