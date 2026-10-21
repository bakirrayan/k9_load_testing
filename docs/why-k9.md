# Why K9

K9 lets you write load tests in Python, with the script shape, executors, checks,
thresholds and metric names you know from k6. The part that sends requests and
measures time is written in Rust. Python only describes the work, so most tests
never pay for the GIL.

```python
# A K9 script: k9 run script.py
import k9

options = {
    "vus": 50,
    "duration": "1m",
    "thresholds": {"http_req_duration": ["p(95)<300"]},
}

async def default():
    res = await k9.http.get("https://test.k6.io/")
    k9.check(res, {"status is 200": lambda r: r.status == 200})
```

## Why another load tester?

|                | k6 | Locust | Goose | **K9** |
|----------------|----|--------|-------|--------|
| Test language  | JavaScript | Python | Rust (compiled) | **Python** |
| Engine         | Go | Python (gevent) | Rust | **Rust** |
| Open-model (arrival-rate) executors | ✅ | ❌ (closed model) | ❌ | ✅ |
| Coordinated-omission handling | Avoided by arrival-rate executors | ❌ | ✅ corrected | ✅ both |
| Load tests inside pytest | ❌ | ❌ | ❌ | ✅ |
| License        | AGPL-3.0 | MIT | Apache-2.0 | MIT / Apache-2.0 |

- **From k6:** the script shape, the seven executors, checks, the threshold syntax,
  metric names and exit code 99. Your Grafana dashboards and CI configs carry over.
- **From Goose:** weighted transactions, coordinated-omission correction, live run
  control and the manager/worker distributed mode.
- **Compared with Locust:** Locust is the established Python load tester, and if
  it serves you well, keep using it. K9 targets cases where the load generator's
  own overhead or measurement accuracy matters. In Locust the Python process does
  the HTTP work. In K9 it doesn't. Measured on one machine
  ([docs/benchmarks.md](benchmarks.md)):
  - declarative plans send as many requests/s as oha, or more, and 1.4–1.5× k6's,
    for the least CPU per request;
  - one Python process gives 3–4× Locust's single process;
  - with `--workers`, Python scripts give 1.4× Locust's 8 processes at 256 VUs.

## How Python is used: three tiers

| Tier | What runs in Python | Cost |
|------|---------------------|------|
| 1. Declarative | Building the plan, before the run | Full Rust speed |
| 2. Hooks | `setup`, `teardown`, `vu_init`, `handle_summary` | Negligible |
| 3. Iteration functions | `default()` on every iteration | Async: one event loop per process. Sync: one thread per VU. Scaled across cores with process workers. |

`k9 inspect` tells you which tier a script runs in.
