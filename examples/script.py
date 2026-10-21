"""A Tier 3 script: `async def default()` runs on every iteration, and HTTP
requests go out on k9's Rust client.

    cargo run -p k9-testserver                # a target to test (port 8080)
    k9 run examples/script.py                 # or -e TARGET=https://your.site
    k9 inspect examples/script.py             # tier, hooks and the resolved options
"""

import asyncio
import json
from typing import Any

import k9
from k9.metrics import Counter, Trend

options = {
    "scenarios": {
        "browse": {"executor": "constant-vus", "vus": 50, "duration": "10s"},
        "checkout": {
            "executor": "per-vu-iterations",
            "vus": 5,
            "iterations": 3,
            "exec": "checkout",
            "env": {"CART": "large"},
        },
    },
    "thresholds": {
        "http_req_failed": ["rate<0.01"],
        "http_req_duration{scenario:browse}": ["p(95)<300"],
        "checks": ["rate>0.99"],
        "checkout_time": ["p(95)<1000"],
    },
}

# Custom metrics are declared at module level (k6's init context).
orders = Counter("orders")
checkout_time = Trend("checkout_time", time=True)


def setup() -> dict[str, Any]:
    # Runs once. The returned data (JSON-serializable, as in k6) is copied to every VU.
    return {"target": k9.env.get("TARGET", "http://localhost:8080")}


async def vu_init(data: dict[str, Any]) -> None:
    # Runs once per VU, before its first iteration: e.g. log in, keep the token.
    k9.vu.state["user"] = f"user-{k9.vu.id}"


async def default(data: dict[str, Any]) -> None:
    target = data["target"]
    with k9.group("home"):
        res = await k9.http.get(f"{target}/delay/50", tags={"page": "home"})
        k9.check(res, {"status is 200": lambda r: r.status == 200})
    # Concurrent requests from one VU; `name` groups the item URLs into one entry.
    items = await asyncio.gather(
        *(k9.http.get(f"{target}/bytes/{n}", name="item") for n in (512, 1024, 2048))
    )
    k9.check(items, {"all items loaded": lambda rs: all(r.ok for r in rs)})
    await k9.sleep(0.5)


async def checkout(data: dict[str, Any]) -> None:
    with k9.group("checkout"):
        res = await k9.http.post(
            f"{data['target']}/echo",
            json={"user": k9.vu.state["user"], "cart": k9.env["CART"]},
        )
        k9.check(res, {"order accepted": lambda r: r.json()["method"] == "POST"})
        orders.add(1)
        checkout_time.add(res.timings.duration)


def teardown(data: dict[str, Any]) -> None:
    print(f"done testing {data['target']}")


def handle_summary(data: dict[str, Any]) -> dict[str, str]:
    # Keep the usual summary and also save the data as JSON (k6's handleSummary).
    return {"stdout": k9.text_summary(), "summary.json": json.dumps(data, indent=2)}
