#!/usr/bin/env python3
"""Compare k9 with k6, oha and Locust against the same local target.

Starts k9-testserver (release build) pinned to its own CPUs, then runs every
tool, pinned to the other CPUs, through the same cases:

- throughput: 64 connections/VUs (`--vus`) sending `GET /ok` as fast as they can;
- rate: 2,000 requests/s (arrival rate) to `/delay/10`, which answers after
  10 ms, so latency above 10 ms is what the tool (and the machine) add.

For each run it records what the tool reports (requests, requests/s, p50/p95/
p99 latency, failures), what the server saw (its request count), and what the
tool cost: CPU time and peak memory of its process tree (from `wait4`).

Needs: `cargo build --release -p k9-testserver -p k9-cli`, the wheel built in
release mode in `.venv` (`maturin develop --release`) for the Python cases,
and `k6`, `oha` and `locust` on the PATH.

    python3 benchmarks/compare.py --duration 15s --json results.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
PORT = 18080
STATS = ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"]
RATE = 2000
#: VUs (connections, users) in the throughput case; `--vus`.
VUS = 64


@dataclass
class Result:
    case: str
    tool: str
    requests: int
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    failed: int
    server_requests: int
    cpu_s: float
    peak_rss_mb: float


def options(case: str, duration: str) -> dict[str, Any]:
    if case == "throughput":
        return {"vus": VUS, "duration": duration, "summaryTrendStats": STATS}
    return {
        "scenarios": {
            "rate": {
                "executor": "constant-arrival-rate",
                "rate": RATE,
                "timeUnit": "1s",
                "duration": duration,
                "preAllocatedVUs": 100,
                "maxVUs": 400,
            }
        },
        "summaryTrendStats": STATS,
    }


def path(case: str) -> str:
    return "/ok" if case == "throughput" else "/delay/10"


def seconds(duration: str) -> float:
    assert duration.endswith("s"), "durations are given in seconds, e.g. 15s"
    return float(duration[:-1])


# --- Tools: each writes its files into `work` and returns the command to run
# --- and a parser for what it wrote.


def k9_plan(case: str, base: str, duration: str, work: Path) -> tuple[list[str], Any]:
    plan = {
        "options": options(case, duration),
        "flows": {"default": [{"request": {"url": base + path(case)}}]},
    }
    (work / "plan.json").write_text(json.dumps(plan))
    export = work / "summary.json"
    command = [str(ROOT / "target/release/k9"), "run", "plan.json", "--summary-export", str(export)]
    return command, lambda: summary_export(export)


def k9_python(workers: int | None):
    def tool(case: str, base: str, duration: str, work: Path) -> tuple[list[str], Any]:
        script = f"""import json
import k9

options = json.loads({json.dumps(json.dumps(options(case, duration)))})
URL = {json.dumps(base + path(case))}


async def default():
    await k9.http.get(URL)
"""
        (work / "script.py").write_text(script)
        export = work / "summary.json"
        command = [str(ROOT / ".venv/bin/k9"), "run", "script.py", "--summary-export", str(export)]
        if workers:
            command += ["--workers", str(workers)]
        return command, lambda: summary_export(export)

    return tool


def k6(case: str, base: str, duration: str, work: Path) -> tuple[list[str], Any]:
    script = f"""import http from 'k6/http';

export const options = {json.dumps(options(case, duration))};

export default function () {{
  http.get({json.dumps(base + path(case))});
}}
"""
    (work / "script.js").write_text(script)
    export = work / "summary.json"
    command = ["k6", "run", "--quiet", "script.js", "--summary-export", str(export)]
    return command, lambda: summary_export(export)


def oha(case: str, base: str, duration: str, work: Path) -> tuple[list[str], Any]:
    out = work / "oha.json"
    command = ["oha", "--no-tui", "--output-format", "json", "-o", str(out), "-z", duration]
    command += ["-c", str(VUS)] if case == "throughput" else ["-c", "100", "-q", str(RATE)]
    command.append(base + path(case))

    def parse() -> dict[str, float]:
        data = json.loads(out.read_text())
        total = sum(data["statusCodeDistribution"].values())
        # Requests still in flight when `-z` runs out aren't failures.
        errors = sum(
            n for error, n in data["errorDistribution"].items() if error != "aborted due to deadline"
        )
        ok = data["statusCodeDistribution"].get("200", 0)
        p = data["latencyPercentiles"]
        return {
            "requests": total + errors,
            "rps": data["summary"]["requestsPerSec"],
            "p50_ms": p["p50"] * 1e3,
            "p95_ms": p["p95"] * 1e3,
            "p99_ms": p["p99"] * 1e3,
            "failed": total + errors - ok,
        }

    return command, parse


def locust(processes: int | None):
    def tool(case: str, base: str, duration: str, work: Path) -> tuple[list[str], Any]:
        users = VUS if case == "throughput" else 100
        wait = "constant(0)" if case == "throughput" else f"constant_throughput({RATE / users})"
        (work / "locustfile.py").write_text(
            f"""from locust import FastHttpUser, constant, constant_throughput, task


class User(FastHttpUser):
    wait_time = {wait}

    @task
    def get(self):
        self.client.get({json.dumps(path(case))})
"""
        )
        command = [
            "locust", "-f", "locustfile.py", "--headless", "--only-summary",
            "-u", str(users), "-r", str(users * 10), "-t", duration,
            "--host", base, "--csv", "locust", "--loglevel", "WARNING",
        ]  # fmt: skip
        if processes:
            command += ["--processes", str(processes)]

        def parse() -> dict[str, float]:
            with open(work / "locust_stats.csv") as f:
                row = next(r for r in csv.DictReader(f) if r["Name"] == "Aggregated")
            return {
                "requests": int(row["Request Count"]),
                "rps": float(row["Requests/s"]),
                "p50_ms": float(row["50%"]),
                "p95_ms": float(row["95%"]),
                "p99_ms": float(row["99%"]),
                "failed": int(row["Failure Count"]),
            }

        return command, parse

    return tool


def summary_export(path: Path) -> dict[str, float]:
    """k6's `--summary-export` layout, which k9 writes too."""
    metrics = json.loads(path.read_text())["metrics"]
    duration = metrics["http_req_duration"]
    failed = metrics.get("http_req_failed", {})
    return {
        "requests": int(metrics["http_reqs"]["count"]),
        "rps": metrics["http_reqs"]["rate"],
        "p50_ms": duration["med"],
        "p95_ms": duration["p(95)"],
        "p99_ms": duration["p(99)"],
        "failed": int(failed.get("passes", 0)),
    }


# id: (label, tool)
TOOLS = {
    "k9": ("k9 (YAML plan)", k9_plan),
    "k9-py": ("k9 (Python, 1 process)", k9_python(None)),
    "k9-py-w8": ("k9 (Python, --workers 8)", k9_python(8)),
    "k6": ("k6", k6),
    "oha": ("oha", oha),
    "locust": ("Locust (1 process)", locust(None)),
    "locust-p8": ("Locust (--processes 8)", locust(8)),
}


def server_requests(base: str) -> int:
    with urllib.request.urlopen(base + "/control/stats") as response:
        return int(json.load(response)["requests"])


def run_tool(name: str, case: str, base: str, duration: str, cpus: str) -> Result:
    with tempfile.TemporaryDirectory(prefix="k9-compare-") as tmp:
        work = Path(tmp)
        label, tool = TOOLS[name]
        command, parse = tool(case, base, duration, work)
        before = server_requests(base)
        with open(work / "log.txt", "w") as log:
            child = subprocess.Popen(
                ["taskset", "-c", cpus, *command], cwd=work, stdout=log, stderr=subprocess.STDOUT
            )
            _, status, usage = os.wait4(child.pid, 0)
        # The server counts its own stats request too.
        seen = server_requests(base) - before - 1
        code = os.waitstatus_to_exitcode(status)
        if code != 0:
            sys.exit(f"{label} ({case}) exited {code}:\n{(work / 'log.txt').read_text()[-3000:]}")
        reported = parse()
    return Result(
        case=case,
        tool=label,
        server_requests=seen,
        cpu_s=usage.ru_utime + usage.ru_stime,
        peak_rss_mb=usage.ru_maxrss / 1024,
        **reported,
    )


def table(results: list[Result]) -> str:
    lines = [
        "| Case | Tool | Requests/s | p50 ms | p95 ms | p99 ms | Failed | Server saw | CPU s | CPU ms / 1k req | Peak RSS MB |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        per_k = r.cpu_s * 1e6 / r.server_requests if r.server_requests else 0.0
        lines.append(
            f"| {r.case} | {r.tool} | {r.rps:,.0f} | {r.p50_ms:.2f} | {r.p95_ms:.2f} | {r.p99_ms:.2f} "
            f"| {r.failed} | {r.server_requests:,} | {r.cpu_s:.1f} | {per_k:.0f} | {r.peak_rss_mb:.0f} |"
        )
    return "\n".join(lines)


def main() -> None:
    global VUS
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--duration", default="15s")
    parser.add_argument("--vus", type=int, default=VUS, help="VUs in the throughput case")
    parser.add_argument("--server-cpus", default="0-7")
    parser.add_argument("--tool-cpus", default="8-31")
    parser.add_argument("--cases", default="throughput,rate")
    parser.add_argument("--tools", default=",".join(TOOLS), help=f"comma-separated, from: {', '.join(TOOLS)}")
    parser.add_argument("--json", type=Path, help="also write the results here")
    args = parser.parse_args()
    seconds(args.duration)
    VUS = args.vus

    server = subprocess.Popen(
        [
            "taskset", "-c", args.server_cpus, str(ROOT / "target/release/k9-testserver"),
            "--port", str(PORT), "--tls-port", str(PORT + 1),
        ],
        stdout=subprocess.DEVNULL,
    )  # fmt: skip
    base = f"http://127.0.0.1:{PORT}"
    try:
        for _ in range(50):
            try:
                server_requests(base)
                break
            except OSError:
                time.sleep(0.1)
        results = []
        for case in args.cases.split(","):
            for name in args.tools.split(","):
                print(f"{case}: {name} …", file=sys.stderr, flush=True)
                results.append(run_tool(name, case, base, args.duration, args.tool_cpus))
    finally:
        server.terminate()
        server.wait()

    print(table(results))
    if args.json:
        machine = {"platform": platform.platform(), "cpus": os.cpu_count()}
        summary = {"machine": machine, "duration": args.duration, "vus": VUS}
        args.json.write_text(json.dumps({**summary, "results": [asdict(r) for r in results]}, indent=2))


if __name__ == "__main__":
    main()
