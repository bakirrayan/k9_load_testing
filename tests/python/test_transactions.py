"""Weighted transactions with onStart/onStop in scripts."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import Server
from k9test import iterations, k9, run, write

SCRIPT = """
import os
import k9
{imports}

options = {{
    "scenarios": {{
        "shop": {{
            "executor": "per-vu-iterations",
            "vus": 2,
            "iterations": 6,
            "transactions": {{"browse": 2, "buy": 1}},
            "onStart": "login",
            "onStop": "logout",
        }}
    }},
    "thresholds": {{
        "checks": ["rate==1"],
        "iterations{{transaction:browse}}": ["count==8"],
        "iterations{{transaction:buy}}": ["count==4"],
    }},
}}

# A file per VU: appends from several threads can interleave on Windows.
def log(name):
    with open(os.path.join(k9.env["DIR"], f"vu-{{k9.vu.id}}.txt"), "a") as f:
        f.write(f"{{k9.vu.id}} {{name}} {{k9.vu.iteration}}\\n")

{define} login():
    {wait}http.get(k9.env["BASE"] + "/cookies/set/sid/abc")
    log("login")

{define} browse():
    res = {wait}http.get(k9.env["BASE"] + "/cookies/has/sid")
    k9.check(res, {{"session kept": lambda r: r.status == 200}})
    log("browse")

{define} buy():
    log("buy")

{define} logout():
    log("logout")
"""

VARIANTS = {
    "async": {"imports": "from k9 import http", "define": "async def", "wait": "await "},
    "sync": {"imports": "from k9.sync import http", "define": "def", "wait": ""},
}


@pytest.mark.parametrize("variant", VARIANTS)
def test_vus_run_their_mix_between_on_start_and_on_stop(
    tmp_path: Path, server: Server, variant: str
) -> None:
    script = write(tmp_path, SCRIPT.format(**VARIANTS[variant]))
    result, summary = run(script, "-e", f"BASE={server.url('')}", "-e", f"DIR={tmp_path}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert iterations(summary) == 12
    assert "transactions: browse×2 buy×1" in result.stdout
    for vu in ("1", "2"):
        lines = (tmp_path / f"vu-{vu}.txt").read_text(encoding="utf-8").splitlines()
        mine = [(name, int(i)) for v, name, i in (line.split() for line in lines)]
        assert {v for v, _, _ in (line.split() for line in lines)} == {vu}
        assert mine[0] == ("login", 0), mine
        assert mine[-1] == ("logout", 6), "onStart/onStop aren't iterations"
        middle = mine[1:-1]
        assert [i for _, i in middle] == list(range(6))
        assert sorted(name for name, _ in middle) == ["browse"] * 4 + ["buy"] * 2


def test_a_scenario_cant_mix_async_and_plain_functions(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"scenarios": {"s": {
            "executor": "shared-iterations", "transactions": {"a": 1, "b": 1}
        }}}
        async def a(): pass
        def b(): pass
        """,
    )
    result = k9("run", script)
    assert result.returncode == 104, result.stderr
    assert "scenario `s` mixes `async def` and plain `def` functions" in result.stderr


def test_unknown_functions_are_reported(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"scenarios": {"s": {
            "executor": "shared-iterations", "transactions": {"a": 1}, "onStart": "nope"
        }}}
        async def a(): pass
        """,
    )
    result = k9("run", script)
    assert result.returncode == 104, result.stderr
    assert "options.scenarios.s.onStart: the script has no function `nope`" in result.stderr
