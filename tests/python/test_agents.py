"""`k9 run --agents N` with a Python script: the manager sends the script to
agents that can't read it."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from conftest import Server
from k9test import iterations, metric, write

TOKEN = "py-agent-token"


def test_agents_run_a_script_they_only_get_from_the_manager(
    tmp_path: Path, server: Server
) -> None:
    script = write(
        tmp_path,
        """
        import k9

        options = {
            "scenarios": {"s": {"executor": "shared-iterations", "vus": 4, "iterations": 20}},
            "thresholds": {"checks": ["rate==1"]},
        }

        def setup():
            return {"path": "/ok"}

        async def default(data):
            res = await k9.http.get(k9.env["BASE"] + data["path"])
            k9.check(res, {"ok": lambda r: r.status == 200})
        """,
        name="shipped.py",
    )
    export = tmp_path / "summary.json"
    manager = subprocess.Popen(
        [
            sys.executable, "-m", "k9", "run", str(script),
            "--agents", "2", "--listen", "127.0.0.1:0", "--agent-token", TOKEN,
            "--summary-export", str(export), "-e", f"BASE={server.url('')}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )  # fmt: skip
    assert manager.stdout is not None
    address = ""
    header = []
    for line in manager.stdout:
        header.append(line)
        if " agents on " in line:
            address = line.split(" agents on ")[1].strip().rstrip(")")
            break
    assert address, "".join(header)
    # The agents can only get the script from the manager.
    script.unlink()
    elsewhere = tmp_path / "agent"
    elsewhere.mkdir()
    agents = [
        subprocess.Popen(
            [sys.executable, "-m", "k9", "agent", "--manager", address, "--token", TOKEN, "--once"],
            cwd=elsewhere,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        for _ in range(2)
    ]
    out, err = manager.communicate(timeout=60)
    assert manager.returncode == 0, err + out
    for agent in agents:
        _, agent_err = agent.communicate(timeout=30)
        assert agent.returncode == 0, agent_err
    summary = json.loads(export.read_text(encoding="utf-8"))
    assert iterations(summary) == 20
    assert metric(summary, "checks")["passes"] == 20
    assert "distributed (2 agents on" in "".join(header)
    for agent in agents:
        leftovers = list(Path(tempfile.gettempdir()).glob(f"k9-agent-{agent.pid}-*"))
        assert leftovers == [], "shipped scripts are cleaned up"


def test_agents_get_the_scripts_local_modules(tmp_path: Path, server: Server) -> None:
    (tmp_path / "helpers.py").write_text("PATH = '/ok'\n", encoding="utf-8")
    pkg = tmp_path / "lib"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("from .checks import ok\n", encoding="utf-8")
    (pkg / "checks.py").write_text("from . import names\n\ndef ok(r):\n    return r.status == 200\n", encoding="utf-8")
    (pkg / "names.py").write_text("OK = 'ok'\n", encoding="utf-8")
    (tmp_path / "unused.py").write_text("raise RuntimeError('never imported')\n", encoding="utf-8")
    script = write(
        tmp_path,
        """
        import k9
        import helpers
        from lib import ok
        from lib.names import OK

        options = {"scenarios": {"s": {"executor": "shared-iterations", "vus": 2, "iterations": 6}}}

        async def default():
            res = await k9.http.get(k9.env["BASE"] + helpers.PATH)
            k9.check(res, {OK: ok})
        """,
        name="modular.py",
    )
    export = tmp_path / "summary.json"
    manager = subprocess.Popen(
        [
            sys.executable, "-m", "k9", "run", str(script),
            "--agents", "2", "--listen", "127.0.0.1:0", "--agent-token", TOKEN,
            "--summary-export", str(export), "-e", f"BASE={server.url('')}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )  # fmt: skip
    assert manager.stdout is not None
    address = ""
    for line in manager.stdout:
        if " agents on " in line:
            address = line.split(" agents on ")[1].strip().rstrip(")")
            break
    assert address
    # Only the manager has the modules.
    shutil.rmtree(pkg)
    (tmp_path / "helpers.py").unlink()
    elsewhere = tmp_path / "agent"
    elsewhere.mkdir()
    agents = [
        subprocess.Popen(
            [sys.executable, "-m", "k9", "agent", "--manager", address, "--token", TOKEN, "--once"],
            cwd=elsewhere,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        for _ in range(2)
    ]
    out, err = manager.communicate(timeout=60)
    assert manager.returncode == 0, err + out
    for agent in agents:
        _, agent_err = agent.communicate(timeout=30)
        assert agent.returncode == 0, agent_err
    summary = json.loads(export.read_text(encoding="utf-8"))
    assert metric(summary, "checks")["passes"] == 6


def test_local_modules_are_found_by_reading_imports(tmp_path: Path) -> None:
    from k9._bundle import local_modules

    (tmp_path / "lib").mkdir()
    (tmp_path / "ns").mkdir()
    (tmp_path / "s.py").write_text(
        "import os, helpers\nfrom lib import ok\nimport ns.deep\nfrom broken import x\n",
        encoding="utf-8",
    )
    (tmp_path / "helpers.py").write_text("", encoding="utf-8")
    (tmp_path / "lib" / "__init__.py").write_text("from .checks import ok\n", encoding="utf-8")
    (tmp_path / "lib" / "checks.py").write_text("from .. import top\n", encoding="utf-8")
    (tmp_path / "top.py").write_text("", encoding="utf-8")
    (tmp_path / "ns" / "deep.py").write_text("", encoding="utf-8")
    (tmp_path / "broken.py").write_text("def (:\n", encoding="utf-8")
    (tmp_path / "unused.py").write_text("", encoding="utf-8")
    assert local_modules(str(tmp_path / "s.py")) == [
        "broken.py", "helpers.py", "lib/__init__.py", "lib/checks.py", "ns/deep.py", "top.py",
    ]  # fmt: skip


def test_agents_run_several_workers_and_shares_follow_weights(
    tmp_path: Path, server: Server
) -> None:
    script = write(
        tmp_path,
        """
        import os
        import k9

        options = {"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 6, "iterations": 1}}}

        async def default():
            await k9.http.get(k9.env["BASE"] + "/ok", tags={"pid": str(os.getpid())})
        """,
        name="spread.py",
    )
    export = tmp_path / "summary.json"
    manager = subprocess.Popen(
        [
            sys.executable, "-m", "k9", "run", str(script),
            "--agents", "2", "--listen", "127.0.0.1:0", "--agent-token", TOKEN,
            "--summary-export", str(export), "-e", f"BASE={server.url('')}",
            "--out", f"json={tmp_path / 'samples.jsonl'}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )  # fmt: skip
    assert manager.stdout is not None
    address = ""
    for line in manager.stdout:
        if " agents on " in line:
            address = line.split(" agents on ")[1].strip().rstrip(")")
            break
    assert address
    agents = [
        subprocess.Popen(
            [sys.executable, "-m", "k9", "agent", "--manager", address, "--token", TOKEN,
             "--once", *extra],
            cwd=tmp_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        for extra in (["--workers", "2"], ["--weight", "2"])
    ]  # fmt: skip
    out, err = manager.communicate(timeout=60)
    assert manager.returncode == 0, err + out
    for agent in agents:
        _, agent_err = agent.communicate(timeout=30)
        assert agent.returncode == 0, agent_err
    assert "joined with 2 workers: 1/3 of the test" in err, err
    assert "joined with 1 worker: 2/3 of the test" in err, err
    assert iterations(json.loads(export.read_text(encoding="utf-8"))) == 6
    per_process: dict[str, int] = {}
    for line in (tmp_path / "samples.jsonl").read_text(encoding="utf-8").splitlines():
        sample = json.loads(line)
        if sample.get("metric") == "http_reqs" and sample["type"] == "Point":
            pid = sample["data"]["tags"]["pid"]
            per_process[pid] = per_process.get(pid, 0) + 1
    assert sorted(per_process.values()) == [1, 1, 4], per_process
