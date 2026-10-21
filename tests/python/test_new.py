"""`k9 new`: the Python templates (Tier 1 `k9.plan`, async and sync Tier 3)
inspect as their tier and run against the test server."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import Server
from k9test import k9


@pytest.mark.parametrize(
    ("template", "tier"),
    [("plan", "declarative"), ("async", "async"), ("sync", "sync")],
)
def test_templates_run(tmp_path: Path, server: Server, template: str, tier: str) -> None:
    script = tmp_path / f"{template}_test.py"
    out = k9("new", script, "--template", template, "--url", server.url("/ok"))
    assert out.returncode == 0, out.stderr
    assert f"k9 run {script.name}" in script.read_text(encoding="utf-8")

    out = k9("inspect", script)
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout)["tier"] == tier

    export = tmp_path / "summary.json"
    out = k9("run", script, "--vus", "2", "--iterations", "4", "--summary-export", export)
    assert out.returncode == 0, out.stderr
    assert json.loads(export.read_text(encoding="utf-8"))["metrics"]["checks"]["passes"] == 4
