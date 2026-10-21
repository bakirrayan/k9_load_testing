"""k6's TLS options from Python scripts (`tlsAuth`, `tlsVersion`,
`tlsCipherSuites`) and `res.tls_version`/`res.tls_cipher_suite`."""

from __future__ import annotations

import shutil
from pathlib import Path

from conftest import Server
from k9test import report, run, write


def test_client_certificates_and_tls_details(tmp_path: Path, server: Server) -> None:
    assert server.identity is not None
    for name in ["client.pem", "client-key.pem"]:
        shutil.copy(server.identity / name, tmp_path / name)
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {
            "iterations": 1,
            "insecureSkipTLSVerify": True,
            "tlsAuth": [{"domains": ["localhost"], "cert": "client.pem", "key": "client-key.pem"}],
            "tlsVersion": "tls1.2",
            "tlsCipherSuites": ["TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"],
        }

        async def default():
            res = await k9.http.get(k9.env["MTLS"] + "/ok")
            plain = await k9.http.get(k9.env["HTTP"] + "/ok")
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.status, res.tls_version, res.tls_cipher_suite,
                           plain.tls_version], f)
        """,
    )  # fmt: skip
    result, _ = run(
        script,
        "-e", f"MTLS={server.mtls}",
        "-e", f"HTTP={server.http}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == [200, "tls1.2", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", None]


def test_without_a_client_certificate_the_handshake_fails(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1, "insecureSkipTLSVerify": True}

        async def default():
            res = await k9.http.get(k9.env["MTLS"] + "/ok")
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.status, res.error_code], f)
        """,
    )
    result, _ = run(script, "-e", f"MTLS={server.mtls}", "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr + result.stdout
    status, code = report(tmp_path)
    assert status == 0 and code in ("tls", "protocol"), code
