<p align="center">
  <img src="https://raw.githubusercontent.com/bakirrayan/k9_load_testing/main/docs/assets/logo-256.png" alt="K9-Load logo" width="220">
</p>

<h1 align="center">K9</h1>

<p align="center">
  <b>Load testing with k6's UX, a Rust engine and a Python API.</b>
</p>

> **Status: v0.1 (alpha)** (see the [changelog](https://github.com/bakirrayan/k9_load_testing/blob/main/CHANGELOG.md)).
> All of k6's executors, checks and thresholds work, from YAML plans or Python scripts,
> with HTTP/1.1, HTTP/2, HTTP/3, WebSockets and gRPC. Tests scale across cores and
> machines, send results to k6's outputs, and run inside pytest.

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
  ([docs/benchmarks.md](https://bakirrayan.github.io/k9_load_testing/benchmarks/)):
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

## Quick start

```console
$ pip install k9-load                                # or: uv tool install k9-load
$ k9 new --url http://localhost:3000/health          # a starter script.py for your service
$ k9 run script.py -u 5 -d 10s                       # run it, print k6's summary
```

Wheels cover Linux, macOS and Windows on Python 3.10 or newer. To build from
source, see [Development](https://bakirrayan.github.io/k9_load_testing/development/).

`k9 new load.yaml` writes a declarative plan instead, and `k9 convert
recording.har` turns a browser recording into one.

## Documentation

The full docs are at [bakirrayan.github.io/k9_load_testing](https://bakirrayan.github.io/k9_load_testing/)
(sources in `docs/`; `uv run --only-group docs mkdocs serve` builds them locally):

| Page | What's in it |
|------|--------------|
| [Getting started](https://bakirrayan.github.io/k9_load_testing/getting-started/) | Install, a first test, checks and thresholds, overriding options |
| [Why K9](https://bakirrayan.github.io/k9_load_testing/why-k9/) | k6, Locust and Goose compared; the three tiers |
| [Writing tests](https://bakirrayan.github.io/k9_load_testing/authoring/) | `k9 new` starters and `k9 convert` from HAR recordings |
| [Declarative plans](https://bakirrayan.github.io/k9_load_testing/plans/) | Templates, extractors, feeders, batch, WebSocket and gRPC steps, transactions, `k9.plan` |
| [Python scripts](https://bakirrayan.github.io/k9_load_testing/scripts/) | `k9.http`, `k9.ws`, `k9.grpc`, checks, groups, metrics, hooks, sync VUs, `--workers` |
| [HTTP, TLS and network](https://bakirrayan.github.io/k9_load_testing/protocols/) | HTTP versions, k6's TLS and network options |
| [Running tests](https://bakirrayan.github.io/k9_load_testing/running/) | The live dashboard, run options, arrival rates, the control API |
| [Results and outputs](https://bakirrayan.github.io/k9_load_testing/outputs/) | JSON, CSV, HTML report, Prometheus, InfluxDB, StatsD/Datadog, OpenTelemetry |
| [Scaling out](https://bakirrayan.github.io/k9_load_testing/scaling/) | `--workers`, `--agents` and execution segments |
| [Library and pytest](https://bakirrayan.github.io/k9_load_testing/library/) | `k9.run()` and the pytest plugin |
| [Benchmarks](https://bakirrayan.github.io/k9_load_testing/benchmarks/) | Hot paths, and k9 against k6, oha and Locust |

## Development

```bash
cargo test --workspace                      # Rust tests
uv sync && uv run pytest                    # Python tests (builds the extension)
uv run --only-group docs mkdocs serve            # the docs site, with the landing page
```

More in [docs/development.md](https://bakirrayan.github.io/k9_load_testing/development/): the crate layout, benchmarks
and the test server.

## License

Dual-licensed under [MIT](https://github.com/bakirrayan/k9_load_testing/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/bakirrayan/k9_load_testing/blob/main/LICENSE-APACHE), at your option.
