# Development

You need Rust 1.99 or newer, a C toolchain (`build-essential`) and [uv](https://docs.astral.sh/uv/).

```bash
cargo test --workspace                      # Rust tests
uv sync                                     # venv + dev tools; builds the extension
uv run pytest                               # Python tests
uv run maturin develop                      # rebuild after changing Rust code
cargo run -p k9-http --example phases      # print phase timings against the test server
cargo run -p k9-testserver                  # standalone target on :8080 (HTTP) and :8443 (HTTPS)
cargo bench -p k9-metrics                   # hot-path benchmarks (also k9-core, k9-http)
```

Benchmark results and the machine they ran on are in [docs/benchmarks.md](benchmarks.md).

Layout:

| Path | Role |
|------|------|
| `crates/k9-core` | Plan model, engine (executors, VUs, run controller), the `Script` trait that Python plugs into |
| `crates/k9-http` | HTTP client with k6's per-phase timings (blocked, connecting, TLS, sending, waiting, receiving) |
| `crates/k9-metrics` | k6's metrics (Counter, Gauge, Rate, Trend on HDR histograms), tag interning, per-thread recording, aggregation |
| `crates/k9-output` | The end-of-test summary, the HTML report and the `--out` outputs |
| `crates/k9-cli` | The `k9` command (library and standalone binary) |
| `crates/k9-grpc` | The manager ↔ worker protocol over gRPC (`proto/k9/worker/v1/worker.proto`) |
| `crates/k9-py` | PyO3 bindings, built as `k9._native`: the script host and the bridge to the asyncio loop |
| `crates/k9-tui` | The live dashboard (`k9 run --tui`), on ratatui |
| `crates/k9-testserver` | Target server for testing K9 itself: delays, error rates, stalls, big bodies |
| `python/k9` | The Python package |
| `docs` | This documentation (mkdocs) and the landing page |

## Continuous integration

Pull requests run the tests on Linux, macOS and Windows (Python 3.10 and
3.14, and the minimum supported Rust).
