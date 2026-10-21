"""Shared fixtures: a running k9-testserver (see crates/k9-testserver)."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


class Server:
    """The target server's base URLs."""

    def __init__(self, http: str, https: str, mtls: str = "", identity: Path | None = None) -> None:
        self.http = http
        self.https = https
        #: HTTPS that needs ``identity``'s client certificate (``client.pem``,
        #: ``client-key.pem``).
        self.mtls = mtls
        self.identity = identity

    def url(self, path: str) -> str:
        return f"{self.http}{path}"


@pytest.fixture(scope="session")
def server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Server]:
    """Builds (if needed) and starts k9-testserver on free ports. Set
    K9_TESTSERVER to use a prebuilt binary instead."""
    binary = os.environ.get("K9_TESTSERVER")
    if binary is None:
        subprocess.run(
            ["cargo", "build", "--quiet", "-p", "k9-testserver"], cwd=ROOT, check=True
        )
        exe = "k9-testserver.exe" if sys.platform == "win32" else "k9-testserver"
        binary = str(ROOT / "target" / "debug" / exe)
    identity = tmp_path_factory.mktemp("identity")
    # As a context manager, Popen closes the pipe once the server is gone.
    with subprocess.Popen(
        [binary, "--port", "0", "--tls-port", "0", "--identity-dir", str(identity)],
        stdout=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        try:
            assert proc.stdout is not None
            lines = [proc.stdout.readline() for _ in range(4)]
            urls = [line.split()[0] for line in lines[1:]]
            assert urls[0].startswith("http://") and urls[1].startswith("https://"), lines
            yield Server(urls[0], urls[1], urls[2], identity)
        finally:
            proc.terminate()
            proc.wait(timeout=10)


@pytest.fixture
def refused_url() -> str:
    """A URL nothing listens on (the port was just released), so connecting
    fails at once."""
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
