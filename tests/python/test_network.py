"""k6's network options from Python scripts: `hosts`, `blockHostnames`,
`localIPs`, `noConnectionReuse`."""

from __future__ import annotations

from pathlib import Path

from conftest import Server
from k9test import report, run, write


def test_hosts_blocks_and_local_ips(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {
            "iterations": 1,
            "hosts": {"api.k9.test": "127.0.0.1", "x.blocked.test": "127.0.0.1"},
            "blockHostnames": ["*.blocked.test"],
            "localIPs": "127.0.0.2",
            "noConnectionReuse": True,
            "dns": {"select": "first"},
        }

        async def default():
            port = k9.env["PORT"]
            res = await k9.http.get(f"http://api.k9.test:{port}/ip")
            blocked = await k9.http.get(f"http://x.blocked.test:{port}/ok")
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.status, res.text, blocked.status, blocked.error_code,
                           "*.blocked.test" in blocked.error], f)
        """,
    )  # fmt: skip
    result, _ = run(
        script,
        "-e", f"PORT={server.http.rsplit(':', 1)[1]}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == [200, "127.0.0.2", 0, "blocked", True]


def test_bad_network_options_stop_the_run(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        options = {"iterations": 1, "localIPs": ["10.0.0.1", "nope"]}

        async def default():
            pass
        """,
    )
    result, _ = run(script)
    assert result.returncode != 0
    assert "options.localIPs[1]" in result.stderr + result.stdout
