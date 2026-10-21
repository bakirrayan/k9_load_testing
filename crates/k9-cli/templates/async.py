"""A Tier 3 script: `async def default()` runs on every iteration, with all
VUs on one event loop; requests go out on k9's Rust client.

    k9 run @FILE@                   # or -e TARGET=https://your.service
    k9 inspect @FILE@               # tier, hooks and the resolved options

Hooks are optional: setup(), teardown(), vu_init() and handle_summary().
"""

import k9

TARGET = k9.env.get("TARGET", "@URL@")

options = {
    "vus": 10,
    "duration": "30s",
    "thresholds": {
        "http_req_failed": ["rate<0.01"],
        "http_req_duration": ["p(95)<500"],
    },
}


async def default() -> None:
    res = await k9.http.get(TARGET)
    k9.check(res, {"status is 200": lambda r: r.status == 200})
    await k9.sleep(1)
