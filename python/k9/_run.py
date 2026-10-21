"""``k9.run()``: a load test as a library call, e.g. inside a pytest test."""

from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from k9 import plan as k9_plan
from k9._errors import ConfigError, RunError, ScriptError, ThresholdsFailed

# k9's exit codes (k6's).
_THRESHOLDS_FAILED = 99
_CONFIG = (2, 104)
_SCRIPT = (100, 101, 107)


@dataclass(frozen=True)
class RunResult:
    """What a run measured: ``metrics`` as in ``--summary-export`` (k6's
    layout: ``{"http_reqs": {"count": 7, "rate": …}, …}``), plus k9's exit
    code and output."""

    exit_code: int
    metrics: Mapping[str, Mapping[str, Any]] = field(repr=False)
    #: The end-of-test summary, as printed.
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)

    @property
    def ok(self) -> bool:
        """True if the run finished with no crossed threshold (exit code 0)."""
        return self.exit_code == 0

    def metric(self, name: str) -> Mapping[str, Any]:
        """A metric's values, e.g. ``metric("http_req_duration")["p(95)"]``
        (milliseconds). Sub-metrics with thresholds are there too, e.g.
        ``http_req_duration{status:200}``."""
        try:
            return self.metrics[name]
        except KeyError:
            known = ", ".join(sorted(self.metrics)) or "none"
            raise KeyError(f"no metric `{name}` (the run has: {known})") from None

    @property
    def iterations(self) -> int:
        return int(self.metrics.get("iterations", {}).get("count", 0))

    @property
    def checks_passed(self) -> int:
        return int(self.metrics.get("checks", {}).get("passes", 0))

    @property
    def checks_failed(self) -> int:
        return int(self.metrics.get("checks", {}).get("fails", 0))

    @property
    def thresholds(self) -> dict[str, dict[str, bool]]:
        """Every threshold: metric → expression → passed."""
        return {
            name: {expr: not crossed for expr, crossed in values["thresholds"].items()}
            for name, values in self.metrics.items()
            if "thresholds" in values
        }

    @property
    def failed_thresholds(self) -> list[str]:
        """Crossed thresholds, as ``"metric: expression"``."""
        return [
            f"{name}: {expr}"
            for name, exprs in self.thresholds.items()
            for expr, passed in exprs.items()
            if not passed
        ]


def run(
    target: str | os.PathLike[str] | k9_plan.Plan,
    *,
    vus: int | None = None,
    duration: str | float | datetime.timedelta | None = None,
    iterations: int | None = None,
    env: Mapping[str, str] | None = None,
    workers: int | str | None = None,
    summary_trend_stats: Sequence[str] | None = None,
    report: str | os.PathLike[str] | None = None,
    args: Sequence[str] = (),
    check_thresholds: bool = True,
    timeout: float | None = None,
) -> RunResult:
    """Runs a script (``.py``), a plan file (``.yaml``, ``.toml``, ``.json``)
    or a :class:`k9.plan.Plan`, as ``k9 run`` would, and returns its results.

    ``vus``, ``duration`` and ``iterations`` override the test's options
    (``duration`` in seconds, as a timedelta, or like ``"30s"``); ``env`` is
    what the script sees as ``k9.env``; ``summary_trend_stats`` picks the
    values the printed summary shows for trends (e.g. ``["p(99)", "count"]``);
    the result has them plus ``count``, ``avg``, ``min``, ``med``, ``max``,
    ``p(90)``, ``p(95)``, ``p(99)`` and ``p(99.9)`` either way; ``report``
    writes the HTML report there; ``args`` are extra ``k9 run`` flags.
    The run happens in a child process, so it can't disturb the caller.

    Raises :class:`ThresholdsFailed` if thresholds were crossed (unless
    ``check_thresholds`` is false), :class:`ConfigError` or
    :class:`ScriptError` if the test couldn't run, and :class:`RunError` for
    other failures (e.g. a worker crashed).
    """
    with tempfile.TemporaryDirectory(prefix="k9-run-") as tmp:
        if isinstance(target, k9_plan.Plan):
            path = Path(tmp) / "plan.json"
            path.write_text(json.dumps(_absolute_files(target.to_dict())), encoding="utf-8")
        else:
            path = Path(target)
            if not path.is_file():
                raise ConfigError(f"no such file: {path}", 104, "")
        export = Path(tmp) / "summary.json"
        command = [sys.executable, "-m", "k9", "run", str(path), "--summary-export", str(export),
                   "--export-all-trend-stats"]  # fmt: skip
        if vus is not None:
            command += ["--vus", str(vus)]
        if duration is not None:
            command += ["--duration", k9_plan._duration(duration, "run")]
        if iterations is not None:
            command += ["--iterations", str(iterations)]
        if workers is not None:
            command += ["--workers", str(workers)]
        if report is not None:
            command += ["--report", str(Path(report).resolve())]
        if summary_trend_stats is not None:
            command += ["--summary-trend-stats", ",".join(summary_trend_stats)]
        for key, value in (env or {}).items():
            command += ["-e", f"{key}={value}"]
        command += list(args)
        proc = subprocess.run(
            command,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
        metrics = json.loads(export.read_text(encoding="utf-8"))["metrics"] if export.exists() else None

    result = None
    if metrics is not None:
        result = RunResult(proc.returncode, metrics, proc.stdout, proc.stderr)
    code = proc.returncode
    if code == 0 or (code == _THRESHOLDS_FAILED and not check_thresholds and result):
        assert result is not None
        return result
    message = _error_message(proc.stderr) or f"k9 run exited with code {code}"
    if code == _THRESHOLDS_FAILED and result is not None:
        raise thresholds_failed(result)
    if code in _CONFIG:
        raise ConfigError(message, code, proc.stderr, result)
    if code in _SCRIPT:
        raise ScriptError(message, code, proc.stderr, result)
    raise RunError(message, code, proc.stderr, result)


def thresholds_failed(result: RunResult) -> ThresholdsFailed:
    """The error for ``result``'s crossed thresholds, with measured values."""
    crossed = [_with_value(t, result) for t in result.failed_thresholds]
    return ThresholdsFailed(
        "thresholds crossed:\n  " + "\n  ".join(crossed),
        result.exit_code,
        result.stderr,
        result,
    )


def _absolute_files(spec: dict[str, Any]) -> dict[str, Any]:
    """Makes a plan's relative feeder files relative to the working directory
    (the plan itself goes to a temporary directory)."""
    for feeder in spec.get("feeders", {}).values():
        if "file" in feeder:
            feeder["file"] = str(Path(feeder["file"]).resolve())
    return spec


def _error_message(stderr: str) -> str:
    """What k9 printed from its first ``error:`` line on."""
    lines = stderr.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("error: "):
            return "\n".join([line.removeprefix("error: "), *lines[i + 1 :]]).strip()
    return stderr.strip()


_AGGREGATE = re.compile(r"\s*(count|rate|value|avg|min|med|max|p\(\d+(?:\.\d+)?\))")


def _with_value(threshold: str, result: RunResult) -> str:
    """``"http_req_duration: p(95)<10"`` plus the measured ``p(95)``."""
    name, expr = threshold.split(": ", 1)
    match = _AGGREGATE.match(expr)
    values = result.metrics.get(name, {})
    if match is None:
        return threshold
    aggregate = match.group(1)
    value = values.get("value" if aggregate == "rate" else aggregate)
    if value is None:
        return threshold
    return f"{threshold} ({aggregate} = {value:.4g})"
