"""`files=` (multipart uploads) and `compression=` in `k9.http` and
`k9.sync.http`, and compressed responses."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import Server
from k9test import report, run, write


def test_uploads_and_compression(tmp_path: Path, server: Server) -> None:
    (tmp_path / "notes.txt").write_text("from a file", encoding="utf-8")
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import http as sync_http

        options = {"iterations": 1}
        seen = {}

        async def default():
            base = k9.env["BASE"]
            with open(k9.env["NOTES"], "rb") as notes:
                res = await k9.http.post(
                    base + "/multipart",
                    data={"user": "zoë"},
                    files={
                        "raw": b"\\x00raw",
                        "named": ("a.csv", "x,y", "text/csv"),
                        "notes": notes,
                    },
                )
            form = res.json()
            seen["form"] = [
                form["fields"],
                {k: [v["filename"], v["contentType"], v["content"]] for k, v in form["files"].items()},
            ]
            res = await k9.http.post(base + "/echo", data="squeezed", compression="gzip, br")
            seen["compressed"] = [res.json()["body"], res.json()["headers"]["content-encoding"]]
            res = await k9.http.get(base + "/compressed/zstd")
            seen["decoded"] = [res.json()["compressed"], res.json()["acceptEncoding"]]
            for bad in [dict(data="x", files={"f": b"1"}), dict(json={}, files={"f": b"1"}),
                        dict(data="x", compression="lzma")]:
                try:
                    await k9.http.post(base + "/echo", **bad)
                except k9.InvalidRequest as e:
                    seen.setdefault("invalid", []).append(str(e))

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )  # fmt: skip
    result, _ = run(
        script,
        "-e", f"BASE={server.url('')}",
        "-e", f"NOTES={tmp_path / 'notes.txt'}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["form"] == [
        {"user": "zoë"},
        {
            "raw": ["raw", "application/octet-stream", "\x00raw"],
            "named": ["a.csv", "text/csv", "x,y"],
            "notes": ["notes.txt", "application/octet-stream", "from a file"],
        },
    ]
    assert seen["compressed"] == ["squeezed", "gzip, br"]
    assert seen["decoded"] == ["zstd", "gzip, deflate"]
    files_with_text, files_with_json, unknown = seen["invalid"]
    assert "mapping" in files_with_text
    assert "json" in files_with_json
    assert "lzma" in unknown


def test_sync_uploads(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import http

        options = {"iterations": 1}

        def default():
            res = http.post(k9.env["BASE"] + "/multipart", files={"f": ("f.txt", b"sync")},
                            compression=None)
            with open(k9.env["OUT"], "w") as f:
                json.dump(res.json()["files"]["f"]["content"], f)
        """,
    )  # fmt: skip
    result, _ = run(script, "-e", f"BASE={server.url('')}", "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert json.loads((tmp_path / "out.json").read_text(encoding="utf-8")) == "sync"
