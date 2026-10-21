"""`k9.grpc` and `k9.sync.grpc`: unary gRPC from script code."""

from __future__ import annotations

from pathlib import Path

from conftest import Server
from k9test import metric, report, run, write

PROTO = Path(__file__).parents[2] / "crates/k9-testserver/proto/k9/test/echo.proto"


def test_async_grpc(tmp_path: Path, server: Server, refused_url: str) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 2, "summaryTrendStats": ["count", "max"]}
        protos = k9.grpc.load(k9.env["PROTO"])
        client = k9.grpc.Client(k9.env["TARGET"], protos, plaintext=True)
        seen = {}

        async def default():
            seen["invalid"] = []
            res = await client.invoke(
                "k9.test.Echo/Say",
                {"text": "hello there", "delay_ms": 1},
                metadata={"x-user": "vu", "x-raw-bin": b"\\x00\\x01"},
                tags={"room": "a"},
            )
            k9.check(res, {"is OK": lambda r: r.status == k9.grpc.StatusOK})
            seen["ok"] = [res.status, res.status.name, res.ok, res.message,
                          res.headers["x-served-by"], res.trailers["grpc-status"],
                          res.error]

            res = await client.invoke("/k9.test.Echo/Say", {"text": "nobody", "status": 5})
            seen["failed"] = [res.status == k9.grpc.StatusCode.NOT_FOUND, res.ok,
                              res.error, res.message]

            down = k9.grpc.Client(k9.env["DOWN"], protos)
            res = await down.invoke("k9.test.Echo/Say", name="down")
            seen["down"] = [res.status.name, bool(res.error)]

            res = await client.invoke("k9.test.Echo/Say", {"delay_ms": 500}, timeout=0.05)
            seen["deadline"] = res.status.name

            for method, message in [("k9.test.Echo/Nope", {}),
                                     ("k9.test.Echo/Say", {"txt": 1}),
                                     ("k9.test.Echo/Chat", {})]:
                try:
                    await client.invoke(method, message)
                except k9.InvalidRequest as e:
                    seen["invalid"].append(str(e))
            try:
                k9.grpc.load("missing.proto")
            except k9.grpc.ProtoError as e:
                seen["missing"] = ["missing.proto" in str(e), isinstance(e, k9.K9Error)]
            try:
                k9.grpc.Client("ftp://x:1", protos)
            except k9.InvalidRequest as e:
                seen["target"] = "ftp" in str(e)

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, summary = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"DOWN={refused_url}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["ok"] == [
        0,
        "OK",
        True,
        {
            "text": "hello there",
            "metadata": {"x-user": "vu"},
            "length": "11",
            "empty": False,
            "kind": "KIND_LONG",
            "words": ["hello", "there"],
        },
        "k9-testserver",
        "0",
        None,
    ]
    assert seen["failed"] == [True, False, "nobody", None]
    assert seen["down"] == ["UNAVAILABLE", True]
    assert seen["deadline"] == "DEADLINE_EXCEEDED"
    nope, typo, chat = seen["invalid"]
    assert "k9.test.Echo/Nope" in nope and "k9.test.Echo/Say" in nope, nope
    assert "txt" in typo, typo
    assert "streaming" in chat, chat
    assert seen["missing"] == [True, True]
    assert seen["target"] is True
    assert metric(summary, "grpc_req_duration")["count"] == 8
    assert metric(summary, "checks")["passes"] == 2
    assert metric(summary, "data_received")["count"] > 0


def test_sync_grpc(tmp_path: Path, server: Server, refused_url: str) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import grpc

        options = {"vus": 2, "iterations": 4, "summaryTrendStats": ["count"]}
        client = grpc.Client(k9.env["TARGET"], grpc.load(k9.env["PROTO"]), plaintext=True)
        reflected = grpc.Client(k9.env["TARGET"], grpc.reflect(k9.env["TARGET"], plaintext=True),
                                plaintext=True)
        seen = {"methods": reflected.protos.methods}
        try:
            k9.grpc.reflect(k9.env["DOWN"], timeout=0.5)
        except k9.grpc.ProtoError as e:
            seen["down"] = "reflection" in str(e)

        def default():
            res = client.invoke("k9.test.Echo/Say", {"text": "hi"}, metadata={"x-a": "1"})
            seen["ok"] = [res.status.name, res.message["text"], res.message["metadata"]]
            seen["failed"] = client.invoke("k9.test.Echo/Say", {"status": 16}).status.name
            seen["reflected"] = reflected.invoke("k9.test.Echo/Say", {"text": "r"}).message["text"]
            try:
                import asyncio
                asyncio.run(k9.grpc.Client(k9.env["TARGET"], client.protos).invoke("x/y"))
            except k9.ContextError as e:
                seen["async_in_sync"] = "k9.sync.grpc" in str(e)

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, summary = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"DOWN={refused_url}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["ok"] == ["OK", "hi", {"x-a": "1"}]
    assert seen["failed"] == "UNAUTHENTICATED"
    assert seen["async_in_sync"] is True
    assert seen["methods"] == ["k9.test.Echo/Say", "k9.test.Echo/Chat"]
    assert seen["down"] is True
    assert seen["reflected"] == "r"
    assert metric(summary, "grpc_req_duration")["count"] == 12


def test_async_streams(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 2}
        client = k9.grpc.Client(k9.env["TARGET"], k9.grpc.load(k9.env["PROTO"]), plaintext=True)
        seen = {}

        async def default():
            async with client.stream("k9.test.Echo/Chat", metadata={"x-s": "1"},
                                     tags={"room": "a"}) as s:
                await s.send({"text": "one"})
                first = await s.recv()
                await s.send({"text": "two"})
                s.close_send()
                rest = [m["text"] async for m in s]
            seen["chat"] = [first["text"], first["metadata"], rest, s.status.name, s.ended,
                            s.trailers["grpc-status"], await s.recv()]

            s = await client.stream("k9.test.Echo/Chat")
            await s.send({"text": "ok"})
            await s.send({"text": "no", "status": 9})
            s.close_send()
            got = [m async for m in s]
            seen["failed"] = [len(got), s.status.name, s.error]
            try:
                await s.send({"text": "late"})
            except k9.grpc.StreamClosed:
                seen["late"] = True

            s = await client.stream("k9.test.Echo/Chat")
            try:
                await s.send({"txt": 1})
            except k9.InvalidRequest as e:
                seen["invalid"] = "txt" in str(e)
            s.cancel()
            seen["cancelled"] = [s.ended, s.status.name]

            s = await client.stream("k9.test.Echo/Chat", timeout=0.1)
            await s.send({"text": "slow", "delay_ms": 1000})
            seen["deadline"] = [await s.recv(), s.status.name]

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )  # fmt: skip
    result, summary = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["chat"] == ["one", {"x-s": "1"}, ["two"], "OK", True, "0", None]
    assert seen["failed"] == [1, "FAILED_PRECONDITION", "no"]
    assert seen["late"] is True
    assert seen["invalid"] is True
    assert seen["cancelled"] == [True, "CANCELLED"]
    assert seen["deadline"] == [None, "DEADLINE_EXCEEDED"]
    assert metric(summary, "grpc_streams")["count"] == 8
    assert metric(summary, "grpc_streams_msgs_sent")["count"] == 10
    assert metric(summary, "grpc_streams_msgs_received")["count"] == 6


def test_sync_streams(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import grpc

        options = {"vus": 2, "iterations": 4}
        client = grpc.Client(k9.env["TARGET"], grpc.load(k9.env["PROTO"]), plaintext=True)
        seen = {}

        def default():
            with client.stream("k9.test.Echo/Chat") as s:
                s.send({"text": "a"})
                first = s.recv()["text"]
                s.send({"text": "b"})
                s.close_send()
                seen["chat"] = [first, [m["text"] for m in s], s.status.name, s.recv()]
            s = client.stream("k9.test.Echo/Chat", timeout=0.1)
            s.send({"text": "slow", "delay_ms": 1000})
            seen["deadline"] = [s.recv(), s.status.name]

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    result, summary = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["chat"] == ["a", ["b"], "OK", None]
    assert seen["deadline"] == [None, "DEADLINE_EXCEEDED"]
    assert metric(summary, "grpc_streams")["count"] == 8


def test_compression_and_message_sizes(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import grpc as sync_grpc

        options = {"iterations": 1}
        protos = k9.grpc.load(k9.env["PROTO"])
        big = "k9 " * 2000
        seen = {}

        def client(**kw):
            return k9.grpc.Client(k9.env["TARGET"], protos, plaintext=True, **kw)

        async def default():
            res = await client(compression="gzip", max_send_size=10_000).invoke(
                "k9.test.Echo/Say", {"text": big})
            seen["gzip"] = [res.status.name, res.message["length"]]
            res = await client(max_receive_size=1000).invoke("k9.test.Echo/Say", {"text": big})
            seen["receive"] = [res.status.name, "larger than max" in res.error]
            res = await client(max_send_size=100).invoke("k9.test.Echo/Say", {"text": big})
            seen["send"] = res.status.name
            async with client(compression="gzip", max_receive_size=1000).stream(
                    "k9.test.Echo/Chat") as s:
                await s.send({"text": big})
                s.close_send()
                seen["stream"] = [[m async for m in s], s.status.name]
            try:
                client(compression="br")
            except k9.InvalidRequest as e:
                seen["bad"] = "br" in str(e)

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )  # fmt: skip
    result, _ = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["gzip"] == ["OK", "6000"]
    assert seen["receive"] == ["RESOURCE_EXHAUSTED", True]
    assert seen["send"] == "RESOURCE_EXHAUSTED"
    assert seen["stream"] == [[], "RESOURCE_EXHAUSTED"]
    assert seen["bad"] is True


def test_sync_clients_take_the_same_options(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import grpc

        options = {"iterations": 1}
        client = grpc.Client(k9.env["TARGET"], grpc.load(k9.env["PROTO"]), plaintext=True,
                             compression="gzip", max_receive_size=50)

        def default():
            res = client.invoke("k9.test.Echo/Say", {"text": "x" * 100})
            with client.stream("k9.test.Echo/Chat") as s:
                s.send({"text": "x" * 100})
                s.close_send()
                list(s)
            with open(k9.env["OUT"], "w") as f:
                json.dump([res.status.name, s.status.name], f)
        """,
    )
    result, _ = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == ["RESOURCE_EXHAUSTED", "RESOURCE_EXHAUSTED"]


def test_recv_timeouts_lose_no_messages_and_clients_have_default_metadata(
    tmp_path: Path, server: Server
) -> None:
    script = write(
        tmp_path,
        """
        import asyncio
        import json
        import k9

        options = {"iterations": 1}
        client = k9.grpc.Client(k9.env["TARGET"], k9.grpc.load(k9.env["PROTO"]), plaintext=True,
                                metadata={"x-team": "k9", "x-env": "test"})
        seen = {}

        async def default():
            res = await client.invoke("k9.test.Echo/Say", {"text": "hi"}, metadata={"x-env": "ci"})
            seen["call"] = res.message["metadata"]
            async with client.stream("k9.test.Echo/Chat") as s:
                await s.send({"text": "slow", "delay_ms": 300})
                try:
                    await s.recv(timeout=0.05)
                except TimeoutError:
                    seen["timeout"] = True
                try:
                    await asyncio.wait_for(s.recv(), 0.05)
                except asyncio.TimeoutError:  # not the built-in one before 3.11
                    seen["wait_for"] = True
                late = await s.recv(timeout=5)
                seen["late"] = [late["text"], late["metadata"]]
                s.close_send()
                seen["end"] = [await s.recv(timeout=5), s.status.name]

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )  # fmt: skip
    result, summary = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
        "--summary-trend-stats", "count,min",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["call"] == {"x-team": "k9", "x-env": "ci"}
    assert seen["timeout"] is True and seen["wait_for"] is True
    assert seen["late"] == ["slow", {"x-team": "k9", "x-env": "test"}]
    assert seen["end"] == [None, "OK"]
    assert metric(summary, "grpc_streams_msgs_received")["count"] == 1
    assert metric(summary, "grpc_stream_duration")["count"] == 1
    assert metric(summary, "grpc_stream_duration")["min"] >= 300


def test_sync_recv_timeouts(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import grpc

        options = {"iterations": 1}
        client = grpc.Client(k9.env["TARGET"], grpc.load(k9.env["PROTO"]), plaintext=True)

        def default():
            with client.stream("k9.test.Echo/Chat") as s:
                s.send({"text": "slow", "delay_ms": 300})
                try:
                    s.recv(timeout=0.05)
                    timed_out = False
                except TimeoutError:
                    timed_out = True
                late = s.recv(timeout=5)["text"]
            with open(k9.env["OUT"], "w") as f:
                json.dump([timed_out, late], f)
        """,
    )
    result, _ = run(
        script,
        "-e", f"PROTO={PROTO}",
        "-e", f"TARGET={server.http.removeprefix('http://')}",
        "-e", f"OUT={tmp_path / 'out.json'}",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr + result.stdout
    assert report(tmp_path) == [True, "slow"]
