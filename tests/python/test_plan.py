"""Tier 1: `k9.plan` specs that the engine runs with no Python."""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

import pytest
from conftest import Server
from k9test import iterations, k9, metric, run, write

import k9.plan as p


def users_seen(server: Server) -> dict[str, int]:
    with urllib.request.urlopen(server.url("/control/stats")) as res:
        users: dict[str, int] = json.load(res)["users"]
    return users


LOGIN = """
import k9
import k9.plan as p

BASE = k9.env["BASE"]
users = p.feeder("users", file="users.csv", mode="per-vu")
token = p.var("token")

plan = p.Plan(
    [
        p.post(
            BASE + "/login",
            json={"username": users.username, "password": users.password},
            extract={"token": p.from_json("$.token")},
        ),
        p.group("account", [
            p.get(
                BASE + "/me",
                headers={"Authorization": f"Bearer {token}"},
                checks={"ok": p.status(200)},
            ),
            p.sleep(0.01),
        ]),
    ],
    options={"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 3, "iterations": 2}}},
    feeders=[users],
)
"""


def login_script(tmp_path: Path, prefix: str) -> Path:
    (tmp_path / "users.csv").write_text(
        "username,password\n" + "".join(f"{prefix}{i},secret\n" for i in range(3)),
        encoding="utf-8",
    )
    return write(tmp_path, LOGIN)


def test_a_login_flow_runs_from_a_python_plan(tmp_path: Path, server: Server) -> None:
    script = login_script(tmp_path, "py-login-")
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert iterations(summary) == 6
    assert metric(summary, "checks")["passes"] == 6
    seen = {u: n for u, n in users_seen(server).items() if u.startswith("py-login-")}
    assert seen == {"py-login-0": 2, "py-login-1": 2, "py-login-2": 2}


def test_inspect_shows_a_declarative_plan_without_python(tmp_path: Path, server: Server) -> None:
    script = login_script(tmp_path, "py-inspect-")
    result = k9("inspect", script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr
    inspection = json.loads(result.stdout)
    assert inspection["tier"] == "declarative"
    assert "hooks" not in inspection
    plan = inspection["plan"]
    assert plan["feeders"]["users"] == {
        "file": str(tmp_path / "users.csv"),
        "mode": "per-vu",
    }
    login, group = plan["flows"]["default"]
    assert login["request"]["body"] == (
        '{"username": "{{ users.username }}", "password": "{{ users.password }}"}'
    )
    assert login["request"]["headers"] == {"Content-Type": "application/json"}
    me = group["group"]["steps"][0]["request"]
    assert me["headers"] == {"Authorization": "Bearer {{ token }}"}
    assert group["group"]["steps"][1] == {"sleep": "10ms"}


def test_workers_run_a_python_plan(tmp_path: Path, server: Server) -> None:
    script = login_script(tmp_path, "py-workers-")
    result, summary = run(script, "--workers", "2", "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "checks")["passes"] == 6
    seen = {u: n for u, n in users_seen(server).items() if u.startswith("py-workers-")}
    assert seen == {"py-workers-0": 2, "py-workers-1": 2, "py-workers-2": 2}


def test_module_options_apply_when_the_plan_has_none(tmp_path: Path, server: Server) -> None:
    script = write(
        tmp_path,
        f"""
        import k9.plan as p
        options = {{"vus": 2, "iterations": 5}}
        plan = p.Plan([p.get("{server.url('/ok')}")])
        """,
    )
    result, summary = run(script)
    assert result.returncode == 0, result.stderr
    assert iterations(summary) == 5


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "plan = p.Plan([p.get('http://x/')])\nasync def default(): pass",
            "both `plan` (Tier 1) and `default()` (Tier 3)",
        ),
        (
            "plan = p.Plan([p.get('http://x/')])\ndef setup(): pass",
            "Tier 1 plans don't run setup()",
        ),
        (
            "options = {'vus': 1}\nplan = p.Plan([p.get('http://x/')], options={'vus': 2})",
            "`options` twice",
        ),
    ],
)
def test_mixing_tiers_is_an_error(tmp_path: Path, source: str, expected: str) -> None:
    script = write(tmp_path, "import k9.plan as p\n" + source)
    result = k9("run", script)
    assert result.returncode == 107, result.stderr
    assert expected in result.stderr, result.stderr


def test_engine_errors_point_at_the_step(tmp_path: Path) -> None:
    script = write(
        tmp_path,
        """
        import k9.plan as p
        plan = p.Plan([p.get("http://x/"), p.get(f"http://x/{p.var('missing')}")])
        """,
    )
    result = k9("run", script)
    assert result.returncode == 104, result.stderr
    assert "flows.default[1]: `{{ missing }}`: no extractor sets `missing`" in result.stderr


# ---------------------------------------------------------------------------
# The builder alone: specs are plain data
# ---------------------------------------------------------------------------


def test_references_are_template_strings() -> None:
    users = p.feeder("users", rows=[{"name": "a", "id": 1}])
    assert users.name == "{{ users.name }}"
    assert users["id"] == "{{ users.id }}"
    assert p.var("t") == "{{ t }}"
    assert p.env("HOST") == "{{ __ENV.HOST }}"
    assert (p.VU, p.ITER) == ("{{ __VU }}", "{{ __ITER }}")
    assert p.uuid() == "{{ uuid() }}"
    assert p.timestamp() == "{{ timestamp() }}"
    assert p.random_int(1, 9) == "{{ randomInt(1, 9) }}"
    assert p.random_string(8) == "{{ randomString(8) }}"


def test_steps_serialize_to_the_plan_format() -> None:
    step = p.request(
        "put",
        "http://x/a",
        headers={"content-type": "text/plain"},
        body="b",
        name="A",
        tags={"k": "v"},
        checks={
            "s": p.status(200, 201),
            "b": p.body_contains("ok"),
            "d": p.duration_below(0.25),
        },
        extract={"h": p.from_header("X-H"), "r": p.from_regex("id=(\\d+)")},
    )
    assert step == {
        "request": {
            "method": "PUT",
            "url": "http://x/a",
            "headers": {"content-type": "text/plain"},
            "body": "b",
            "name": "A",
            "tags": {"k": "v"},
            "checks": {
                "s": {"status": [200, 201]},
                "b": {"bodyContains": "ok"},
                "d": {"durationBelow": "250ms"},
            },
            "extract": {"h": {"header": "X-H"}, "r": {"regex": "id=(\\d+)"}},
        }
    }
    assert p.get("http://x/") == {"request": {"method": "GET", "url": "http://x/"}}
    assert p.status(204) == {"status": 204}
    assert p.sleep("1m") == {"sleep": "1m"}
    users = p.feeder("users", rows=[{"a": 1}], mode="random")
    plan = p.Plan(flows={"other": [p.get("http://x/")]}, feeders=[users])
    assert plan.to_dict() == {
        "flows": {"other": [{"request": {"method": "GET", "url": "http://x/"}}]},
        "feeders": {"users": {"rows": [{"a": 1}], "mode": "random"}},
    }


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda: p.feeder("users"), "`file` or `rows`"),
        (lambda: p.feeder("users", file="a.csv", rows=[{}]), "`file` or `rows`"),
        (lambda: p.feeder("users", rows=[{}], mode="shuffle"), "mode"),
        (lambda: p.feeder("my users", rows=[{}]), "feeder name"),
        (lambda: p.var("a.b"), "variable name"),
        (lambda: p.request("FETCH", "http://x/"), "method"),
        (lambda: p.post("http://x/", body="a", json={}), "only one of `body`, `json`"),
        (lambda: p.sleep(-1), "sleep"),
        (lambda: p.Plan(), "steps or flows"),
        (lambda: p.Plan([], flows={}), "steps or flows"),
        (lambda: p.Plan([p.get("http://x/")], feeders=["users"]), "k9.plan.feeder"),
    ],
)
def test_mistakes_fail_where_they_are_made(build: object, expected: str) -> None:
    with pytest.raises((ValueError, TypeError), match=expected):
        build()  # type: ignore[operator]


def test_websocket_steps_serialize_and_run(tmp_path: Path, server: Server) -> None:
    step = p.websocket(
        "ws://x/chat",
        [
            p.ws_send("hi"),
            p.ws_receive(timeout=2, checks={"b": p.body_contains("hi")},
                         extract={"w": p.from_regex("(h)")}),
            p.ws_ping(),
            p.sleep("10ms"),
            p.ws_close(4000, "bye"),
        ],
        headers={"X": "1"},
        name="chat",
        tags={"k": "v"},
        checks={"up": p.status(101)},
    )  # fmt: skip
    assert step == {
        "websocket": {
            "url": "ws://x/chat",
            "headers": {"X": "1"},
            "name": "chat",
            "tags": {"k": "v"},
            "checks": {"up": {"status": 101}},
            "steps": [
                {"send": "hi"},
                {"receive": {"timeout": "2000ms", "checks": {"b": {"bodyContains": "hi"}},
                             "extract": {"w": {"regex": "(h)"}}}},
                "ping",
                {"sleep": "10ms"},
                {"close": {"code": 4000, "reason": "bye"}},
            ],
        }
    }  # fmt: skip
    assert p.ws_receive() == {"receive": {}}
    assert p.ws_close() == {"close": {"code": 1000, "reason": ""}}

    script = write(
        tmp_path,
        """
        import k9
        import k9.plan as p

        plan = p.Plan(
            [
                p.websocket(
                    k9.env["WS"] + "/ws/echo",
                    [
                        p.ws_send(f"hello {p.VU}"),
                        p.ws_receive(checks={"echoed": p.body_contains("hello")}),
                    ],
                )
            ],
            options={"vus": 2, "iterations": 4},
        )
        """,
    )
    result, summary = run(script, "-e", f"WS={server.url('').replace('http', 'ws', 1)}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "checks")["passes"] == 4
    assert metric(summary, "ws_sessions")["count"] == 4
    assert metric(summary, "ws_msgs_received")["count"] == 4


def test_grpc_steps_serialize_and_run(tmp_path: Path, server: Server) -> None:
    step = p.grpc(
        "h:1",
        "a.B/C",
        {"text": p.VU},
        protos=["x.proto"],
        import_paths=[Path("defs")],
        plaintext=True,
        metadata={"x-a": "1"},
        name="c",
        tags={"k": "v"},
        timeout=2,
        checks={"ok": p.status(0)},
        extract={"t": p.from_json("$.text")},
    )
    assert step == {
        "grpc": {
            "target": "h:1",
            "method": "a.B/C",
            "message": {"text": "{{ __VU }}"},
            "plaintext": True,
            "protos": ["x.proto"],
            "importPaths": ["defs"],
            "metadata": {"x-a": "1"},
            "name": "c",
            "tags": {"k": "v"},
            "timeout": "2000ms",
            "checks": {"ok": {"status": 0}},
            "extract": {"t": {"json": "$.text"}},
        }
    }
    assert p.grpc("h:1", "a.B/C") == {"grpc": {"target": "h:1", "method": "a.B/C"}}
    assert p.grpc("h:1", "a.B/S", messages=[{"a": 1}, {}])["grpc"]["messages"] == [{"a": 1}, {}]

    proto = Path(__file__).parents[2] / "crates/k9-testserver/proto/k9/test/echo.proto"
    script = write(
        tmp_path,
        """
        import k9
        import k9.plan as p

        target = k9.env["TARGET"]
        plan = p.Plan(
            [
                p.grpc(target, "k9.test.Echo/Say", {"text": f"hi {p.VU}"}, plaintext=True,
                       protos=[k9.env["PROTO"]], checks={"ok": p.status(0)},
                       extract={"said": p.from_json("$.text")}),
                p.grpc(target, "k9.test.Echo/Say", {"text": p.var("said")}, plaintext=True,
                       checks={"again": p.body_contains("hi ")}),
            ],
            options={"vus": 2, "iterations": 4},
        )
        """,
    )  # fmt: skip
    target = server.http.removeprefix("http://")
    result, summary = run(script, "-e", f"TARGET={target}", "-e", f"PROTO={proto}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "checks")["passes"] == 8
    assert metric(summary, "checks")["fails"] == 0


def test_multipart_and_compression_serialize_and_run(tmp_path: Path, server: Server) -> None:
    step = p.post(
        "http://h/",
        multipart={"a": "1", "f": p.file("x.bin", filename="y.bin", content_type="image/png")},
        compression="gzip",
    )
    assert step == {
        "request": {
            "method": "POST",
            "url": "http://h/",
            "multipart": {
                "a": "1",
                "f": {"file": "x.bin", "filename": "y.bin", "contentType": "image/png"},
            },
            "compression": "gzip",
        }
    }
    with pytest.raises(ValueError, match="only one"):
        p.post("http://h/", body="x", multipart={"a": "1"})

    (tmp_path / "doc.txt").write_text("in a file", encoding="utf-8")
    script = write(
        tmp_path,
        """
        import k9
        import k9.plan as p

        plan = p.Plan(
            [
                p.post(k9.env["BASE"] + "/multipart",
                       multipart={"who": f"vu{p.VU}", "doc": p.file("doc.txt")},
                       checks={"sent": p.body_contains('"content":"in a file"')}),
                p.post(k9.env["BASE"] + "/echo", body="small", compression="br",
                       checks={"squeezed": p.body_contains('"body":"small"')}),
            ],
            options={"vus": 2, "iterations": 2},
        )
        """,
    )  # fmt: skip
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "checks")["passes"] == 4


def test_batch_steps_serialize_and_run(tmp_path: Path, server: Server) -> None:
    step = p.batch([p.get("http://h/a"), p.post("http://h/b", body="x")])
    assert step == {
        "batch": [
            {"method": "GET", "url": "http://h/a"},
            {"method": "POST", "url": "http://h/b", "body": "x"},
        ]
    }
    with pytest.raises(ValueError, match="request steps"):
        p.batch([p.sleep(1)])
    with pytest.raises(ValueError, match="at least one"):
        p.batch([])

    script = write(
        tmp_path,
        """
        import k9
        import k9.plan as p

        base = k9.env["BASE"]
        plan = p.Plan(
            [p.batch([p.get(base + "/delay/200", checks={"a": p.status(200)}),
                      p.get(base + "/delay/200", checks={"b": p.status(200)})])],
            options={"iterations": 2, "batchPerHost": 2},
        )
        """,
    )  # fmt: skip
    result, summary = run(script, "-e", f"BASE={server.url('')}")
    assert result.returncode == 0, result.stderr + result.stdout
    assert metric(summary, "checks")["passes"] == 4
    assert metric(summary, "iteration_duration")["max"] < 390
