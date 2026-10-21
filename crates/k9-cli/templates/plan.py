"""A Tier 1 script: Python builds the plan once, when the script loads, and
the test runs as a declarative plan, at full Rust speed.

    k9 run @FILE@                   # or -e TARGET=https://your.service
    k9 inspect @FILE@               # "tier": "declarative"
"""

import k9
import k9.plan as p

TARGET = k9.env.get("TARGET", "@URL@")

plan = p.Plan(
    [
        p.get(TARGET, checks={"status is 200": p.status(200)}),
        p.sleep(1),
    ],
    options={
        "vus": 10,
        "duration": "30s",
        "thresholds": {
            "http_req_failed": ["rate<0.01"],
            "http_req_duration": ["p(95)<500"],
        },
    },
)
