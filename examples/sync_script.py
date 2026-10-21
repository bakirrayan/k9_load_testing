"""A Tier 3 script with plain `def` functions: each VU runs on its own thread
and uses the blocking API in `k9.sync`. Handy for porting code that already
uses blocking clients; for high VU counts prefer `async def` (examples/script.py).

    cargo run -p k9-testserver                # a target to test (port 8080)
    k9 run examples/sync_script.py
"""

from typing import Any

import k9
from k9.sync import http, sleep

options = {
    "vus": 20,
    "duration": "10s",
    "thresholds": {"http_req_failed": ["rate<0.01"], "checks": ["rate>0.99"]},
}


def setup() -> dict[str, Any]:
    return {"target": k9.env.get("TARGET", "http://localhost:8080")}


def vu_init(data: dict[str, Any]) -> None:
    # Runs on the VU's own thread, before its first iteration.
    res = http.post(f"{data['target']}/echo", json={"user": f"user-{k9.vu.id}"})
    k9.vu.state["session"] = res.json()["body"]


def default(data: dict[str, Any]) -> None:
    with k9.group("browse"):
        res = http.get(f"{data['target']}/delay/50", headers={"X-Session": k9.vu.state["session"]})
        k9.check(res, {"status is 200": lambda r: r.status == 200})
    # k9.sync.sleep, not time.sleep: it returns at once if the VU is interrupted.
    sleep(0.5)
