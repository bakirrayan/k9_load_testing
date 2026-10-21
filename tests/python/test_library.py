"""`k9.run()` and the pytest plugin."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from conftest import Server
from k9test import write

import k9
import k9.plan as p

pytest_plugins = ["pytester"]


def test_run_returns_the_results(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9
        from k9 import http
        options = {"vus": 2, "iterations": 7}
        async def default():
            res = await http.get(k9.env["BASE"] + "/ok")
            k9.check(res, {"ok": lambda r: r.status == 200})
        """,
    )
    result = k9.run(script, env={"BASE": server.url("")})
    assert result.ok and result.exit_code == 0
    assert result.iterations == 7
    assert result.metric("http_reqs")["count"] == 7
    assert (result.checks_passed, result.checks_failed) == (7, 0)
    assert result.failed_thresholds == []
    assert "http_reqs" in result.stdout
    with pytest.raises(KeyError, match="no metric `nope`"):
        result.metric("nope")
    every = {"count", "avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "p(99.9)"}
    duration = result.metric("http_req_duration")
    assert set(duration) == every, "every trend stat, whatever the summary shows"
    assert duration["count"] == 7 and duration["p(99.9)"] >= duration["p(99)"]
    assert "p(99)=" not in result.stdout
    tail = k9.run(script, env={"BASE": server.url("")}, summary_trend_stats=["p(99.99)"])
    assert set(tail.metric("http_req_duration")) == every | {"p(99.99)"}
    assert "p(99.99)=" in tail.stdout and "p(95)=" not in tail.stdout
    report = tmp_path / "report.html"
    k9.run(script, env={"BASE": server.url("")}, report=report)
    assert "<svg" in report.read_text(encoding="utf-8")


def test_run_takes_a_plan_and_option_overrides(tmp_path: Path, server: Server) -> None:
    (tmp_path / "ids.csv").write_text("id\n1\n2\n", encoding="utf-8")
    plan = p.Plan(
        [p.get(server.url("/ok?id=") + p.feeder("ids", file="ids.csv").id)],
        feeders=[p.feeder("ids", file="ids.csv")],
    )
    with pytest.MonkeyPatch.context() as mp:
        # Relative feeder files are relative to the working directory.
        mp.chdir(tmp_path)
        result = k9.run(plan, vus=3, iterations=12)
    assert result.iterations == 12
    assert result.metric("vus_max")["value"] == 3
    timed = k9.run(p.Plan([p.get(server.url("/ok")), p.sleep(0.05)]), vus=1, duration=datetime.timedelta(seconds=0.3))
    assert 3 <= timed.iterations <= 7


def test_crossed_thresholds_raise_unless_asked_not_to(server: Server) -> None:
    plan = p.Plan(
        [p.get(server.url("/delay/30"))],
        options={
            "iterations": 3,
            "thresholds": {
                "http_req_duration": ["p(95)<10", "max<5000"],
                "http_req_failed": ["rate==0"],
            },
        },
    )
    with pytest.raises(k9.ThresholdsFailed) as caught:
        k9.run(plan)
    error = caught.value
    assert isinstance(error, AssertionError) and isinstance(error, k9.RunError)
    assert error.exit_code == 99
    assert "http_req_duration: p(95)<10" in str(error)
    assert "max<5000" not in str(error)
    assert error.result is not None and error.result.iterations == 3
    result = k9.run(plan, check_thresholds=False)
    assert not result.ok
    assert result.failed_thresholds == ["http_req_duration: p(95)<10"]
    assert result.thresholds["http_req_duration"] == {"p(95)<10": False, "max<5000": True}


def test_config_and_script_errors(tmp_path: Path) -> None:
    bad = write(tmp_path, "options:\n  scenarios:\n    s: { executor: warp-speed }\n", "bad.yaml")
    with pytest.raises(k9.ConfigError, match="unknown executor `warp-speed`") as caught:
        k9.run(bad)
    assert caught.value.exit_code == 104 and caught.value.result is None
    broken = write(tmp_path, "raise RuntimeError('boom at import')\n", "broken.py")
    with pytest.raises(k9.ScriptError, match="boom at import"):
        k9.run(broken)
    with pytest.raises(k9.ConfigError, match="no such file"):
        k9.run(tmp_path / "missing.py")


def test_the_pytest_plugin(pytester: pytest.Pytester, server: Server) -> None:
    (pytester.path / "load").mkdir()
    (pytester.path / "load" / "smoke.yaml").write_text(
        "options:\n  vus: 2\n  iterations: 4\n  thresholds:\n    http_req_failed: ['rate==0']\n"
        f"flows:\n  default:\n    - request: {{ url: '{server.url('/ok')}' }}\n",
        encoding="utf-8",
    )
    (pytester.path / "load" / "slow.yaml").write_text(
        "options:\n  iterations: 2\n  thresholds:\n    http_req_duration: ['max<1']\n"
        f"flows:\n  default:\n    - request: {{ url: '{server.url('/delay/20')}' }}\n",
        encoding="utf-8",
    )
    pytester.makepyfile(
        test_load="""
        import pytest

        @pytest.mark.k9("load/smoke.yaml")
        def test_marker_alone():
            pass

        @pytest.mark.k9("load/smoke.yaml", iterations=6)
        def test_marker_with_result(k9_result):
            assert k9_result.iterations == 6

        @pytest.mark.k9("load/slow.yaml")
        def test_crossed_thresholds_fail_the_test():
            pass

        def test_runner_fixture(k9_run):
            assert k9_run("load/smoke.yaml").iterations == 4

        def test_result_needs_a_marker(k9_result):
            pass
        """
    )
    outcome = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    outcome.assert_outcomes(passed=3, failed=1, errors=1)
    output = outcome.stdout.str()
    assert "test_crossed_thresholds_fail_the_test FAILED" in output or (
        "FAILED test_load.py::test_crossed_thresholds_fail_the_test" in output
    )
    assert "http_req_duration: max<1" in output
    assert "k9_result needs a @pytest.mark.k9(...) marker" in output
