"""`k9.ws`: WebSockets from async script code."""

from __future__ import annotations

from pathlib import Path

from conftest import Server
from k9test import metric, report, run, write


def test_async_websockets(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9

        options = {"iterations": 1}
        seen = {}

        async def default():
            base = k9.env["WS"]
            async with k9.ws.connect(base + "/ws/echo", tags={"room": "a"}) as ws:
                await ws.send("hello")
                await ws.send(b"\\x01\\x02")
                texts = [await ws.recv(), await ws.recv()]
                rtt = await ws.ping()
            seen["echo"] = [ws.status, texts[0], list(texts[1]), rtt > 0, ws.closed,
                            ws.close_code]

            ws = await k9.ws.connect(base + "/ws/push/3")
            seen["push"] = [[m async for m in ws], ws.close_code]

            ws = await k9.ws.connect(base + "/ws/echo")
            await ws.send("close")
            try:
                await ws.recv()
            except k9.ws.ConnectionClosed as e:
                seen["closed"] = [e.code, e.reason, ws.close_code, ws.close_reason]
            try:
                await ws.send("too late")
            except k9.ws.ConnectionClosed:
                seen["send_after_close"] = True

            try:
                await k9.ws.connect(base + "/status/404")
            except k9.ws.WebSocketError as e:
                seen["refused"] = ["404" in str(e), e.kind, isinstance(e, k9.K9Error)]

            ws = await k9.ws.connect(base + "/ws/echo")
            try:
                await ws.recv(timeout=0.1)
            except TimeoutError:
                seen["timeout"] = True
            await ws.close()
            seen["closed_by_us"] = ws.close_code

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    ws_base = server.url("").replace("http://", "ws://")
    result, summary = run(
        script, "-e", f"WS={ws_base}", "-e", f"OUT={tmp_path / 'out.json'}"
    )
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["echo"] == [101, "hello", [1, 2], True, True, 1000]
    assert seen["push"] == [["1", "2", "3"], 1000]
    assert seen["closed"] == [4000, "bye", 4000, "bye"]
    assert seen["send_after_close"] is True
    assert seen["refused"] == [True, "websocket", True]
    assert seen["timeout"] is True
    assert seen["closed_by_us"] == 1000
    assert metric(summary, "ws_sessions")["count"] == 4
    assert metric(summary, "ws_msgs_sent")["count"] == 3
    assert metric(summary, "ws_msgs_received")["count"] == 5
    assert metric(summary, "ws_ping")["max"] > 0
    assert metric(summary, "ws_session_duration")["max"] > 0
    assert "the server answered 404" in result.stderr, "the refusal is reported live"


def test_sync_websockets(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        """
        import json
        import k9
        from k9.sync import ws

        options = {"iterations": 1}
        seen = {}

        def default():
            base = k9.env["WS"]
            with ws.connect(base + "/ws/echo") as sock:
                sock.send("hello")
                rtt = sock.ping()
                sock.send(b"\\x01")
                seen["echo"] = [sock.status, sock.recv(), list(sock.recv()), rtt > 0]
            seen["closed"] = [sock.closed, sock.close_code]

            sock = ws.connect(base + "/ws/push/2")
            seen["push"] = [list(sock), sock.close_code]

            sock = ws.connect(base + "/ws/echo")
            try:
                sock.recv(timeout=0.1)
            except TimeoutError:
                seen["timeout"] = True
            sock.send("close")
            try:
                sock.recv()
            except k9.ws.ConnectionClosed as e:
                seen["server_closed"] = [e.code, e.reason]

            try:
                ws.connect(base + "/status/404")
            except k9.ws.WebSocketError as e:
                seen["refused"] = e.kind
            try:
                import asyncio
                asyncio.run(k9.ws.connect(base + "/ws/echo"))
            except k9.ContextError as e:
                seen["async_in_sync"] = "k9.sync.ws" in str(e)

        def teardown():
            with open(k9.env["OUT"], "w") as f:
                json.dump(seen, f)
        """,
    )
    ws_base = server.url("").replace("http://", "ws://")
    result, summary = run(script, "-e", f"WS={ws_base}", "-e", f"OUT={tmp_path / 'out.json'}")
    assert result.returncode == 0, result.stderr + result.stdout
    seen = report(tmp_path)
    assert seen["echo"] == [101, "hello", [1], True]
    assert seen["closed"] == [True, 1000]
    assert seen["push"] == [["1", "2"], 1000]
    assert seen["timeout"] is True
    assert seen["server_closed"] == [4000, "bye"]
    assert seen["refused"] == "websocket"
    assert seen["async_in_sync"] is True
    assert metric(summary, "ws_sessions")["count"] == 3
    assert metric(summary, "ws_msgs_received")["count"] == 4
