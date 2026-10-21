"""A Tier 3 script with plain `def` functions: each VU runs on its own thread
and uses the blocking API in `k9.sync`. For many VUs, prefer `async def`
(`k9 new --template async`).

    k9 run @FILE@                   # or -e TARGET=https://your.service
    k9 inspect @FILE@               # tier, hooks and the resolved options

Hooks are optional: setup(), teardown(), vu_init() and handle_summary().
"""

import k9
from k9.sync import http, sleep

TARGET = k9.env.get("TARGET", "@URL@")

options = {
    "vus": 10,
    "duration": "30s",
    "thresholds": {
        "http_req_failed": ["rate<0.01"],
        "http_req_duration": ["p(95)<500"],
    },
}


def default() -> None:
    res = http.get(TARGET)
    k9.check(res, {"status is 200": lambda r: r.status == 200})
    # k9.sync.sleep, not time.sleep: it returns at once if the VU is interrupted.
    sleep(1)
