"""Scripts using k9.http, check, group, sleep, metrics and the vu_init /
handle_summary hooks, end to end against k9-testserver."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from conftest import Server
from k9test import iterations, k9, metric, report, run, write


def out(tmp_path: Path) -> str:
    return f"OUT={tmp_path / 'out.json'}"


def test_requests_and_responses(tmp_path: Path, server: Server, refused_url: str) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1}
        seen = {}

        async def default():
            base = k9.env["BASE"]
            res = await k9.http.get(f"{base}/echo?a=1", params={"b": "2 3"},
                                    headers={"X-Token": "t1"})
            echo = res.json()
            seen["get"] = [res.status, res.ok, echo["method"], echo["query"],
                           echo["headers"]["x-token"], res.headers["content-type"],
                           res.headers["Content-Type"], res.url == f"{base}/echo?a=1&b=2+3",
                           res.timings.duration > 0, res.error]
            res = await k9.http.post(f"{base}/echo", json={"n": 1})
            echo = res.json()
            seen["post_json"] = [echo["method"], echo["body"], echo["headers"]["content-type"]]
            res = await k9.http.put(f"{base}/echo", {"a": "x y"})
            seen["put_form"] = [res.json()["body"], res.json()["headers"]["content-type"]]
            res = await k9.http.request("patch", f"{base}/echo", data=b"raw")
            seen["patch_bytes"] = [res.json()["method"], res.text.count("raw")]
            res = await k9.http.get(f"{base}/redirect/2")
            seen["redirects"] = [res.status, res.text, res.url]

            res = await k9.http.get(f"{base}/status/503")
            try:
                res.raise_for_status()
            except k9.StatusError as e:
                seen["status_error"] = [e.status, isinstance(e, k9.HttpError), str(e)]
            res = await k9.http.get(k9.env["REFUSED"])
            try:
                res.raise_for_status()
            except k9.RequestError as e:
                seen["request_error"] = [res.status, res.ok, e.kind, "could not connect" in res.error]
            try:
                await k9.http.get("ftp://example.com/")
            except k9.InvalidRequest as e:
                seen["invalid"] = [isinstance(e, ValueError), "unsupported scheme" in str(e)]

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    base = server.url("")
    result, summary = run(
        script, "-e", f"BASE={base}", "-e", f"REFUSED={refused_url}", "-e", out(tmp_path)
    )
    assert result.returncode == 0, result.stderr
    assert "ERROR" not in result.stderr, result.stderr
    seen = report(tmp_path)
    assert seen["get"] == [
        200, True, "GET", "a=1&b=2+3", "t1", "application/json", "application/json",
        True, True, None,
    ]
    assert seen["post_json"] == ["POST", '{"n": 1}', "application/json"]
    assert seen["put_form"] == ["a=x+y", "application/x-www-form-urlencoded"]
    assert seen["patch_bytes"] == ["PATCH", 1]
    assert seen["redirects"] == [200, "redirects done", f"{base}/redirect/0"]
    assert seen["status_error"] == [503, True, f"GET {base}/status/503: status 503"]
    assert seen["request_error"] == [0, False, "connect", True]
    assert seen["invalid"] == [True, True]
    # 5 requests + 2 redirect hops + 503 + the failed one; the invalid one never went out.
    assert metric(summary, "http_reqs")["count"] == 9
    assert metric(summary, "http_req_failed")["passes"] == 2
    assert "WARN request failed: could not connect" in result.stderr, "live, the first time"


def test_requests_checks_and_groups_are_tagged_for_thresholds(
    tmp_path: Path, server: Server
) -> None:
    script = write(
        tmp_path,
        """
        import k9

        options = {
            "vus": 2, "iterations": 6,
            "thresholds": {
                "http_reqs{group:::shop::cart}": ["count==6"],
                "http_reqs{name:item}": ["count==6"],
                "http_reqs{kind:api}": ["count==6"],
                "checks{check:status is 200}": ["rate==1"],
                "checks{check:always fails}": ["rate==0"],
                "group_duration{group:::shop}": ["avg>0"],
            },
        }

        async def default():
            base = k9.env["BASE"]
            with k9.group("shop"):
                async with k9.group("cart"):
                    res = await k9.http.get(f"{base}/echo")
                ok = k9.check(res, {"status is 200": lambda r: r.status == 200})
                assert ok is True
            await k9.http.get(f"{base}/ok?id={k9.vu.iteration}", name="item", tags={"kind": "api"})
            assert k9.check(1, {"always fails": False, "status is 200": True}) is False
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert "ERROR" not in result.stderr, result.stderr
    assert "█ shop" in result.stdout
    assert "✓ status is 200" in result.stdout
    assert "✗ always fails" in result.stdout
    assert metric(summary, "checks")["passes"] == 12
    assert metric(summary, "checks")["fails"] == 6


def test_custom_metrics_with_thresholds(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        from k9.metrics import Counter, Gauge, Rate, Trend

        orders = Counter("orders")
        queue = Gauge("queue_depth")
        success = Rate("success")
        latency = Trend("latency", time=True)

        options = {
            "vus": 1, "iterations": 4,
            "thresholds": {
                "orders": ["count==8"], "orders{tier:gold}": ["count==2"],
                "queue_depth": ["value==3"], "success": ["rate==0.5"],
                "latency": ["avg==15", "max<=20"],
            },
        }

        async def default():
            n = k9.vu.iteration
            orders.add(2, tags={"tier": "gold" if n == 0 else "free"})
            queue.add(n)
            success.add(n % 2 == 0)
            latency.add(10 + n * 10 / 3)
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "orders")["count"] == 8
    assert metric(summary, "latency")["max"] == 20
    assert "latency" in result.stdout and "ms" in result.stdout.split("latency", 1)[1][:80]

    failing = write(tmp_path, "from k9.metrics import Counter\nc = Counter('c')\n"
                    "options = {'thresholds': {'c': ['count>5']}}\n"
                    "async def default():\n    c.add()\n", name="failing.py")
    assert k9("run", failing).returncode == 99


def test_metrics_must_be_valid_and_declared_at_module_level(tmp_path: Path) -> None:
    script = write(tmp_path, "from k9.metrics import Trend\nt = Trend('bad name')\n")
    result = k9("run", script)
    assert result.returncode == 107
    assert "MetricError: invalid metric name" in result.stderr

    script = write(
        tmp_path,
        """
        from k9.metrics import Counter
        options = {"iterations": 2}

        async def default():
            Counter("late")
        """,
        name="late.py",
    )
    result = k9("run", script)
    assert result.returncode == 0
    assert "ERROR script raised 2×: MetricError: metric 'late' must be declared at module level" in result.stderr


def test_k9_apis_outside_a_vu_raise_context_errors(tmp_path: Path, server: Server) -> None:
    script = write(tmp_path, f"import asyncio, k9\nasyncio.run(k9.http.get('{server.url('/ok')}'))\n")
    result = k9("run", script)
    assert result.returncode == 107
    assert "ContextError: k9.http can only be used while k9 runs a VU iteration" in result.stderr


def test_cookies_last_one_iteration_of_one_vu(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import k9

        options = {"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 2, "iterations": 3}}}

        async def default():
            base = k9.env["BASE"]
            before = await k9.http.get(f"{base}/cookies/has/sid")
            await k9.http.get(f"{base}/cookies/set/sid/{k9.vu.id}")
            after = await k9.http.get(f"{base}/cookies/has/sid")
            assert (before.status, after.status) == (404, 200), (before.status, after.status)
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    assert "ERROR" not in result.stderr, result.stderr
    assert iterations(summary) == 6


def test_vu_init_runs_once_per_vu_and_can_keep_vu_state(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {
            "scenarios": {"s": {"executor": "per-vu-iterations", "vus": 3, "iterations": 2}},
            "thresholds": {"http_reqs{group:::vu_init}": ["count==3"]},
        }
        seen = []

        async def vu_init(data):
            await k9.http.get(k9.env["BASE"] + "/ok")
            k9.vu.state["token"] = f"token-{k9.vu.id}"
            seen.append(["init", k9.vu.id, data])

        async def default(data):
            seen.append(["iteration", k9.vu.id, k9.vu.state["token"]])

        def setup():
            return {"from": "setup"}

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, _ = run(script, "-e", f"BASE={server.url('')}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    inits = sorted(s for s in seen if s[0] == "init")
    assert inits == [["init", vu, {"from": "setup"}] for vu in (1, 2, 3)]
    for vu in (1, 2, 3):
        mine = [s for s in seen if s[1] == vu]
        assert mine[0][0] == "init", seen
        assert mine[1:] == [["iteration", vu, f"token-{vu}"]] * 2


def test_a_vu_whose_vu_init_raises_runs_no_iterations(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"vus": 2, "duration": "300ms"}

        def vu_init():
            raise PermissionError("login refused")

        async def default():
            pass
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0
    assert "0 complete and 0 interrupted iterations" in result.stdout
    assert "iterations" not in summary["metrics"]
    assert result.stderr.count("Traceback (most recent call last)") == 1, "live, once"
    assert (
        "ERROR script raised 2×: PermissionError: login refused (script.py:5 in vu_init)"
        in result.stderr
    )


def test_the_first_occurrence_of_each_exception_is_printed_live(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9
        options = {"vus": 1, "iterations": 6}

        def parse(n):
            return {"a": 1}[n]

        async def default():
            parse("a" if k9.vu.iteration % 3 else "b")
        """,
    )
    result = k9("run", script)
    assert result.returncode == 0
    stderr = result.stderr
    assert stderr.count("Traceback (most recent call last)") == 1, stderr
    assert 'line 9, in default\n' in stderr and 'line 6, in parse\n' in stderr
    assert stderr.index("KeyError: 'b'") < stderr.index("ERROR script raised 2×")
    assert "ERROR script raised 2×: KeyError: 'b' (script.py:6 in parse)" in stderr


def test_handle_summary_decides_what_is_written(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import json, os
        import k9

        options = {"iterations": 3, "thresholds": {"iterations": ["count==3"]}}

        async def default():
            pass

        def handle_summary(data):
            report = os.path.join(k9.env["DIR"], "reports", "summary.json")
            return {
                "stdout": "custom summary\\n" + k9.text_summary(),
                report: json.dumps(data),
                "stderr": b"to stderr\\n",
            }
        """,
    )
    result = k9("run", script, "-e", f"DIR={tmp_path}")
    assert result.returncode == 0, result.stderr
    assert "custom summary\n" in result.stdout
    assert result.stdout.count("iterations....") == 1, "only the summary handle_summary kept"
    assert "to stderr" in result.stderr
    data: Any = json.loads((tmp_path / "reports" / "summary.json").read_text(encoding="utf-8"))
    it = data["metrics"]["iterations"]
    assert (it["type"], it["values"]["count"]) == ("counter", 3)
    assert it["thresholds"] == {"count==3": {"ok": True}}
    assert data["state"]["isStdOutTTY"] is False
    assert data["options"]["noColor"] is False
    result = k9("run", script, "-e", f"DIR={tmp_path}", "--no-color")
    data = json.loads((tmp_path / "reports" / "summary.json").read_text(encoding="utf-8"))
    assert data["options"]["noColor"] is True

    replaced = write(tmp_path, "async def default(): pass\ndef handle_summary(data): return {}\n",
                     name="quiet.py")
    result = k9("run", replaced)
    assert result.returncode == 0
    assert "iterations...." not in result.stdout, "no summary unless handle_summary returns one"


def test_a_failing_handle_summary_falls_back_to_the_default_summary(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        async def default():
            pass

        def handle_summary(data):
            return {"stdout": data["no such key"]}
        """,
    )
    result = k9("run", script)
    assert result.returncode == 107
    assert "iterations...." in result.stdout
    assert "ERROR handle_summary() failed: Traceback" in result.stderr
    assert "KeyError: 'no such key'" in result.stderr


def test_interruption_aborts_requests_in_flight(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import asyncio, datetime, json
        import k9

        options = {"scenarios": {"s": {"executor": "constant-vus", "vus": 3,
            "duration": "300ms", "gracefulStop": datetime.timedelta(milliseconds=200)}}}
        cancelled = []

        async def default():
            try:
                await k9.http.get(k9.env["BASE"] + "/delay/900")
            except asyncio.CancelledError:
                cancelled.append(k9.vu.id)
                raise

        async def teardown():
            # Requests that weren't aborted would finish (and be recorded) by now.
            await asyncio.sleep(1)
            with open(k9.env["OUT"], "w") as f:
                json.dump(sorted(cancelled), f)
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr
    assert "0 complete and 3 interrupted iterations" in result.stdout
    assert report(tmp_path) == [1, 2, 3]
    assert "http_reqs" not in summary["metrics"], "the requests were aborted"


def test_thousands_of_vus_make_real_requests_from_one_process(
    tmp_path: Path, server: Server
) -> None:
    script = write(
        tmp_path,
        """
        import k9

        options = {"vus": 1000, "duration": "3s"}

        async def default():
            res = await k9.http.get(k9.env["BASE"] + "/delay/200")
            k9.check(res, {"ok": lambda r: r.status == 200})
        """,
    )
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    assert metric(summary, "checks")["fails"] == 0
    duration = metric(summary, "http_req_duration")
    assert duration["min"] >= 195, duration
    if sys.platform != "linux":
        # Hosted macOS and Windows runners (3-4 shared CPUs, the debug test
        # server beside k9) can't hold this rate; the benchmarks run on Linux.
        assert iterations(summary) >= 1000, metric(summary, "iterations")
        return
    # 1000 VUs × 3 s / ~200 ms ≈ 15,000; leave room for slow CI runners.
    assert iterations(summary) >= 1000 * 8, metric(summary, "iterations")
    assert duration["med"] <= 300, duration


def test_https_speaks_http2_and_responses_say_so(
    tmp_path: Path, server: Server, refused_url: str
) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {
            "iterations": 1,
            "insecureSkipTLSVerify": True,
            "thresholds": {"http_reqs{proto:HTTP/2.0}": ["count==1"]},
        }
        seen = {}

        async def default():
            seen["https"] = (await k9.http.get(k9.env["HTTPS"] + "/ok")).proto
            seen["http"] = (await k9.http.get(k9.env["HTTP"] + "/ok")).proto
            seen["error"] = (await k9.http.get(k9.env["REFUSED"])).proto

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, _ = run(
        script, "-e", f"HTTPS={server.https}", "-e", f"HTTP={server.http}",
        "-e", f"REFUSED={refused_url}", "-e", out(tmp_path),
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == {"https": "HTTP/2.0", "http": "HTTP/1.1", "error": ""}


def test_http3_when_asked_for(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1, "insecureSkipTLSVerify": True, "httpVersion": "3"}

        async def default():
            res = await k9.http.post(k9.env["HTTPS"] + "/echo", json={"n": 1})
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.proto, res.json()["body"]], f)
        """,
    )
    result, _ = run(script, "-e", f"HTTPS={server.https}", "-e", out(tmp_path))
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == ["HTTP/3.0", '{"n": 1}']
