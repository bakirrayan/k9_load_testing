import json
import subprocess
import sys
from pathlib import Path

import k9

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "basic.yaml"


def k9_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "k9", *args], capture_output=True, text=True, check=False
    )


def test_version_attribute() -> None:
    assert k9.__version__.count(".") == 2


def test_version_command_matches_package() -> None:
    result = k9_cli("version")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"k9 {k9.__version__}"


def test_inspect_prints_resolved_plan() -> None:
    result = k9_cli("inspect", str(EXAMPLE))
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout)
    assert out["tier"] == "declarative"
    scenarios = out["plan"]["options"]["scenarios"]
    assert scenarios["checkout"]["executor"] == "constant-arrival-rate"
    assert scenarios["checkout"]["maxVUs"] == 50


def test_invalid_plan_exits_with_k6_config_code(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("options:\n  scenarios:\n    s: { executor: constant-vus }\nflows: {}\n", encoding="utf-8")
    result = k9_cli("inspect", str(bad))
    assert result.returncode == 104
    assert "options.scenarios.s.duration: required for constant-vus" in result.stderr


def test_usage_error_exits_2() -> None:
    assert k9_cli("no-such-command").returncode == 2
