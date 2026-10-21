"""The package's types (and the stubs for its native half) let a type checker
catch mistakes in scripts."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytest.importorskip("mypy")


def mypy(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "mypy", "--no-incremental", *map(str, args)],
        capture_output=True,
        encoding="utf-8",
        check=False,
        cwd=ROOT,
    )


def test_the_package_examples_and_templates_type_check_strictly() -> None:
    templates = [f"crates/k9-cli/templates/{t}.py" for t in ("plan", "async", "sync")]
    result = mypy("--strict", "python/k9", "examples/script.py", "examples/sync_script.py", *templates)
    assert result.returncode == 0, result.stdout


def test_mistakes_in_scripts_are_caught(tmp_path: Path) -> None:
    script = tmp_path / "script.py"
    script.write_text(
        "import k9\n"
        "async def default():\n"
        "    res = await k9.http.get('http://x', header={'a': 'b'})\n"
        "    k9.check(res, {'ok': lambda r: r.status_code == 200})\n",
        encoding="utf-8",
    )
    result = mypy("--check-untyped-defs", script)
    assert 'Unexpected keyword argument "header" for "get"' in result.stdout, result.stdout
    assert '"Response" has no attribute "status_code"' in result.stdout, result.stdout
