"""k6's `throw` option in Python scripts."""

from __future__ import annotations

from pathlib import Path

from conftest import Server
from k9test import report, run, write


def test_throw_raises_for_requests_without_a_response(
    tmp_path: Path, server: Server, refused_url: str
) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1, "throw": True}
        seen = {}

        async def default():
            res = await k9.http.get(k9.env["HTTP"] + "/status/500")
            seen["status"] = res.status
            try:
                await k9.http.get(k9.env["DOWN"])
            except k9.RequestError as e:
                seen["async"] = [e.kind, e.response.status]

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, _ = run(
        script,
        "-e", f"HTTP={server.http}",
        "-e", f"DOWN={refused_url}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == {"status": 500, "async": ["connect", 0]}


def test_throw_in_sync_scripts(tmp_path: Path, refused_url: str) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import http

        options = {"iterations": 1, "throw": True}

        def default():
            try:
                http.get(k9.env["DOWN"])
            except k9.RequestError as e:
                with open(k9.env["OUT"], "w") as f:
                    json.dump(e.kind, f)
        """,
    )
    result, _ = run(script, "-e", f"DOWN={refused_url}", "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == "connect"


def test_without_throw_failed_requests_return(tmp_path: Path, refused_url: str) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1}

        async def default():
            res = await k9.http.get(k9.env["DOWN"])
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.status, res.error_code], f)
        """,
    )
    result, _ = run(script, "-e", f"DOWN={refused_url}", "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == [0, "connect"]
