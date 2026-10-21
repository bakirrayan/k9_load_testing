"""`k9 run script.py --workers N`: scripts across processes."""

from __future__ import annotations

import os
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


def test_workers_split_a_script_and_hooks_run_once(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json, os
        import k9
        from k9.metrics import Counter

        orders = Counter("orders")
        options = {
            "scenarios": {"s": {"executor": "shared-iterations", "vus": 6, "iterations": 60}},
            "thresholds": {"orders": ["count==120"], "checks": ["rate==1"]},
        }

        # The hooks' file, and one per VU: appends from several processes
        # can interleave on Windows.
        def log(line, name="hooks"):
            with open(os.path.join(k9.env["DIR"], name + ".txt"), "a") as f:
                f.write(line + "\\n")

        def setup():
            log(f"setup {os.getpid()}")
            return {"token": "abc"}

        async def default(data):
            res = await k9.http.get(k9.env["BASE"] + "/ok")
            k9.check(res, {"ok": lambda r: r.status == 200 and data["token"] == "abc"})
            orders.add(2)
            log(f"vu {k9.vu.id} {os.getpid()}", f"vu-{os.getpid()}-{k9.vu.id}")

        def teardown(data):
            log(f"teardown {os.getpid()} {data['token']}")

        def handle_summary(data):
            log(f"summary {os.getpid()} {data['metrics']['iterations']['values']['count']}")
            return {"stdout": k9.text_summary()}
        """,
    )
    result, summary = run(
        script, "--workers", "3", "-e", f"BASE={server.url('')}", "-e", f"DIR={tmp_path}"
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert "execution: local (3 workers)" in result.stdout
    assert iterations(summary) == 60
    assert metric(summary, "http_reqs")["count"] == 60
    lines = (tmp_path / "hooks.txt").read_text(encoding="utf-8").splitlines()
    manager = {line.split()[1] for line in lines}
    assert [line.split()[0] for line in lines] == [
        "setup", "teardown", "summary"
    ], "each hook once, in the manager"
    assert len(manager) == 1
    assert "teardown" in lines[-2] and lines[-2].endswith(" abc")
    assert lines[-1].endswith(" 60.0") or lines[-1].endswith(" 60")
    vus = [
        line.split()
        for path in tmp_path.glob("vu-*.txt")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(vus) == 60
    assert {int(v[1]) for v in vus} == set(range(1, 7)), "VU ids 1–6 across workers"
    worker_pids = {v[2] for v in vus}
    assert len(worker_pids) == 3 and not worker_pids & manager, "3 worker processes"


def test_sync_and_async_scripts_both_work(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9
        from k9.sync import http

        options = {"scenarios": {
            "a": {"executor": "per-vu-iterations", "vus": 2, "iterations": 5},
            "s": {"executor": "per-vu-iterations", "vus": 2, "iterations": 5, "exec": "blocking"},
        }}

        async def default():
            await k9.http.get(k9.env["BASE"] + "/ok")

        def blocking():
            http.get(k9.env["BASE"] + "/ok")
        """,
    )
    result, summary = run(script, "--workers", "2", "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    assert metric(summary, "http_reqs")["count"] == 20


@pytest.mark.skipif((os.cpu_count() or 1) < 4, reason="needs 4 cores")
def test_cpu_bound_scripts_scale_across_cores(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"vus": 8, "duration": "2s"}

        def default():
            n = 0
            for i in range(20_000):  # ~1 ms of pure Python
                n += i * i
        """,
    )
    # One process can't pass 1× (the GIL); two workers reach ~2× on separate
    # cores, ~1.5× on hosted CI runners, whose vCPUs share physical cores.
    _, one = run(script)
    _, two = run(script, "--workers", "2")
    ratio = iterations(two) / iterations(one)
    assert ratio > 1.3, f"1 process: {iterations(one)}, 2 workers: {iterations(two)}"


def test_a_crashed_worker_is_reported(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import os, time
        import k9
        options = {"vus": 4, "duration": "30s"}

        def default():
            time.sleep(0.05)
            if k9.vu.id == 1 and k9.vu.iteration == 3:
                os._exit(3)
        """,
    )
    started = time.monotonic()
    result = k9("run", script, "--workers", "2")
    assert result.returncode == 103, result.stderr
    assert time.monotonic() - started < 20
    assert "exited with code 3 before finishing" in result.stderr
    assert "iterations...." in result.stdout, "the summary still prints"


def test_a_worker_that_cant_load_the_script_stops_the_run(tmp_path: Path) -> None:
    # Workers run `python -m k9 worker …`; the manager `python -m k9 run …`.
    script = write(
        tmp_path,
        """
        import sys
        if sys.argv[1:2] == ["worker"]:
            raise ImportError("only in workers")
        options = {"vus": 2, "iterations": 2}
        async def default():
            pass
        """,
    )
    result = subprocess.run(
        [sys.executable, "-m", "k9", "run", str(script), "--workers", "2"],
        capture_output=True,
        encoding="utf-8",
        timeout=60,
    )
    assert result.returncode == 103, result.stderr
    assert "couldn't start: the script raised while loading" in result.stderr
    assert "ImportError: only in workers" in result.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")
def test_ctrl_c_in_the_terminal_stops_everything_once(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"vus": 4, "duration": "1m"}

        def teardown():
            print("teardown ran")

        def default():
            k9.sync.sleep(30)
        """,
    )
    # A terminal sends Ctrl-C to the whole process group: manager and workers.
    proc = subprocess.Popen(
        [sys.executable, "-m", "k9", "run", str(script), "--workers", "2"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
        start_new_session=True,
    )
    time.sleep(3)
    started = time.monotonic()
    os.killpg(proc.pid, signal.SIGINT)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 105, stderr
    assert time.monotonic() - started < 10
    assert "0 complete and 4 interrupted iterations" in stdout
    assert stdout.count("teardown ran") == 1
    assert "KeyboardInterrupt" not in stderr
