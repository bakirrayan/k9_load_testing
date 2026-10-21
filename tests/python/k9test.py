"""Helpers for running k9 on scripts in end-to-end tests."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any


def write(tmp_path: Path, source: str, name: str = "script.py") -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


def k9(*args: str | Path, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "k9", *map(str, args)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=timeout,
    )


def run(script: Path, *args: str) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    """Runs a script with --summary-export; returns the process and the export."""
    export = script.with_suffix(".summary.json")
    result = k9("run", script, "--summary-export", str(export), *args)
    summary = json.loads(export.read_text(encoding="utf-8")) if export.exists() else {}
    return result, summary


def iterations(summary: dict[str, Any]) -> int:
    return int(summary["metrics"]["iterations"]["count"])


def report(tmp_path: Path) -> Any:
    """What the script's teardown() wrote to $OUT."""
    return json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))


def metric(summary: dict[str, Any], name: str) -> dict[str, Any]:
    """A metric (or ``name{tag:value}`` sub-metric) from a ``--summary-export``."""
    value: dict[str, Any] = summary["metrics"][name]
    return value
