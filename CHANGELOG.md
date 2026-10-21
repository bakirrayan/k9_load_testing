# Changelog

## 0.1.0 (2026-10-21)

The first release: pytest-native, accurate and fast.

- **k6-compatible core.** Plans and scripts use k6's option names, all seven
  executors, checks, thresholds (exit code 99,
  `abortOnFail`), k6's metric names, its end-of-test summary, `--summary-export`,
  JSON Lines output and option overrides (flags > `K9_*` environment > script).
- **Accurate HTTP.** hyper with a timed connector: k6's blocked, connecting,
  TLS, sending, waiting and receiving phases, redirects, cookies and TLS
  options. Arrival-rate executors with `dropped_iterations`, plus
  `iteration_duration_corrected` for coordinated omission.
- **Python, in three tiers.** Tier 1: `k9.plan` builds declarative flows with
  templates, extractors, feeders and generators, which run with no Python.
  Tier 2: `setup`, `teardown`, `vu_init` and `handle_summary` hooks. Tier 3:
  `async def default()` on one event loop, or plain `def` on a thread per VU,
  with `k9.http`, `check`, `group`, `sleep` and custom metrics.
- **Scale.** `--workers N` splits a test across processes with k6's execution
  segments. Totals stay exact, and hooks run once.
- **From Goose.** Weighted transactions with `onStart`/`onStop`, with an exact mix
  per VU.
- **Library and pytest.** `k9.run()` returns a `RunResult`, and raises
  `ThresholdsFailed`, `ConfigError` or `ScriptError`, all `K9Error`s. The pytest
  plugin adds `@pytest.mark.k9(...)` and the `k9_result` and `k9_run` fixtures.
- **Requirements.** Python 3.10 or newer; wheels for Linux (x86_64, aarch64),
  macOS (arm64, x86_64) and Windows (x86_64). Building from source needs Rust
  1.99 or newer.

### Also in this release

- `k9 new [FILE] [--template yaml|plan|async|sync] [--url URL] [--force]`: a
  starter plan or script for each tier, ready to run.
- `k9 convert recording.har [-O plan.yaml]`: a HAR recording into a plan, with
  pages as groups, pauses as sleeps and status checks. Values a response
  handed out (tokens, ids, CSRF fields) are correlated into `json`, `header`
  or `regex` extractors and `{{ variable }}` templates. `--only`/`--skip`
  filter hosts, `--no-static` leaves out assets, `--no-correlate` keeps
  recorded values.
- `--out statsd[=ADDR]` and `--out datadog[=ADDR]` (DogStatsD tags): samples as
  StatsD lines over UDP, like k6's outputs, with `K6_STATSD_*`/`K6_DATADOG_*`
  settings.
- `--out prometheus-rw`: `TREND_AS_NATIVE_HISTOGRAM` sends trends as native
  histograms (`k6_<name>_seconds`), and `STALE_MARKERS` marks every series
  stale at the end of the test, as in k6.
- `--out influxdb[=URL]` (InfluxDB v1, like k6's output) and
  `--out xk6-influxdb[=URL]` (InfluxDB v2, like k6's extension): every sample
  as a line-protocol point named like k6's, with `tagsAsFields`, precision,
  retention policy, credentials and tokens from `K6_INFLUXDB_*`/`K9_INFLUXDB_*`.
  The test server receives InfluxDB writes.
- gRPC streams: `recv(timeout=)` (async and sync) raises `TimeoutError`
  without losing the message, also when `asyncio.wait_for` interrupts
  `recv()`; a `grpc_stream_duration` trend. `k9.grpc.Client(metadata=)` sends
  default metadata with every call and stream.
- gRPC: gzip message compression (`compression: gzip`, `Client(compression=)`;
  gzipped answers are always read) and k6's `maxSendSize`/`maxReceiveSize`
  (`RESOURCE_EXHAUSTED` past them), for calls and streams. The test server's
  Echo service accepts and sends gzip.
- `k9 agent --workers N|auto` runs an agent's part of the test in several
  processes, and `k9 agent --weight W|auto` gives agents unequal shares (e.g.
  by core count); the manager prints each agent's share.
- Agents get a script's local modules (found by reading its imports) and a
  plan's `.proto` files (compiled on the manager) and multipart uploads, so
  they need none of the manager's files. The worker protocol is now version 4.
- `--http-debug[=full]` (and the `httpDebug` option, `K9_HTTP_DEBUG`) dumps
  requests and responses to stderr; `-q`/`--quiet`, `--no-summary` and
  `--no-color` (`NO_COLOR`). `k9.run()` results carry every trend stat
  (`count`, `p(99)`, `p(99.9)`, …) whatever the summary shows.
- k6's run options `minIterationDuration`, `noCookiesReset`, `throw` (Python
  requests with no response raise `RequestError`), `systemTags` (drop the
  engine's tags not listed) and `summaryTimeUnit` (`s`, `ms`, `us`), with
  k6's CLI flags (`-w` for `--throw`) and `K9_*` variables.
- k6's network options: `hosts` (name, `name:port` or `*.domain` to an IP or
  IP:port), `dns` (`ttl`, `select`, `policy`; object or k6's text form),
  `blockHostnames`, `blacklistIPs` (IPs, CIDR blocks, `first-last` spans),
  `localIPs` (handed out to VUs in turn), `noConnectionReuse` and
  `noVUConnectionReuse`, with k6's CLI flags and `K9_*` variables. Blocked
  requests fail with error code `blocked`. The test server has `GET /ip`.
- k6's TLS options: `tlsAuth` (client certificates per domain, PEM text or
  files; HTTP, WebSockets and gRPC), `tlsVersion` (one version or
  `{min, max}`; TLS 1.2 and 1.3) and `tlsCipherSuites` (IANA names). HTTPS
  samples get k6's `tls_version` tag; Python responses have `tls_version`
  and `tls_cipher_suite`. The test server has a mutual-TLS listener
  (`--identity-dir` writes the client certificate it accepts).

- `batch` steps send requests in parallel (k6's `http.batch`), within k6's
  `batch` and `batchPerHost` options (`--batch`, `--batch-per-host`,
  `K9_BATCH`, `K9_BATCH_PER_HOST`); `k9.plan.batch()`.

- Compression, as in k6: requests send `Accept-Encoding: gzip, deflate` by
  default and compressed responses (gzip, deflate, br, zstd) are decoded
  (`data_received` still counts the wire bytes); a body that doesn't decode
  fails the request (`decompress`). Request bodies can be compressed:
  `compression=` in `k9.http`, `compression:` in plans.
- Multipart uploads: `files=` in `k9.http`/`k9.sync.http`, `multipart:` in
  plans (text fields with templates, files read before the run), and
  `k9.plan.file()`. k9-http `compression` and `multipart` modules; the test
  server gained `/compressed/{encodings}` and `/multipart`.

- gRPC streams (client, server, bidirectional): `client.stream()` in
  `k9.grpc` (async: `send`, `close_send`, `recv`, `async for`, `cancel`) and
  `k9.sync.grpc`; `messages: [...]` on plan steps for streaming methods.
  Recorded in k6's `grpc_streams`, `grpc_streams_msgs_sent` and
  `grpc_streams_msgs_received`; `rpc_type` tags tell the kinds apart. k9-http
  `Client::grpc_stream` multiplexes streams on the shared HTTP/2 connection
  (a channel-fed request body).

- gRPC steps in declarative plans (`grpc:` with `target`, `method`, templated
  `message` and `metadata`, `status`/`bodyContains`/`durationBelow` checks,
  `json`/`regex`/`header` extractors), also from `k9.plan.grpc()`. Protos come
  from files (relative to the plan) or the server's reflection service (v1,
  falling back to v1alpha), loaded once before the run; `k9.grpc.reflect()`
  for scripts. The test server serves reflection too.

- Unary gRPC from Python: `k9.grpc.load()` compiles `.proto` files at run time
  (protox, no `protoc`), `k9.grpc.Client(target, protos, plaintext=)` and
  `await client.invoke(method, message, metadata=, tags=, name=, timeout=)`
  with dict messages (protobuf JSON mapping); `k9.sync.grpc` for plain `def`
  code. Recorded as k6's `grpc_req_duration` (tags `method`, `name`, `url`,
  `rpc_type`, `status`). k9-http: `grpc` module (`Protos`, `GrpcRequest`,
  `Client::grpc()`/`grpc_invoke`) over its own HTTP/2 (h2c or TLS with ALPN
  h2), and `Response::trailers`. The test server serves `k9.test.Echo` (tonic).

- `k9 run --tui`: a live terminal dashboard (ratatui) with stat tiles, charts
  of requests and response times, checks, thresholds and a log; `p` pauses,
  `+`/`-` change VUs, `q` stops. `--theme brand|neon`, 24-bit colour with
  256-colour and `NO_COLOR` fallbacks. The engine publishes a `LiveFrame`
  each second (`RunControl::frames()`).

- WebSocket steps in declarative plans (`websocket:` with `send`, `receive`
  checks/extractors, `ping`, `sleep`, `close`; handshake checks), also from
  `k9.plan.websocket()`. `k9.sync.ws` for plain `def` scripts.

- WebSockets from async Python: `k9.ws.connect()` (awaitable or `async with`),
  `send`, `recv(timeout=)`, `async for`, `ping()`, `close()`, recorded in k6's
  `ws_*` metrics. k9-http's `Client::websocket` (tokio-tungstenite, dialed and
  timed like requests, HTTP/1.1 ALPN) and k9-core's `ScriptContext::websocket`.

- HTTP/2: HTTPS negotiates it with ALPN (falling back to HTTP/1.1), and
  `httpVersion` (`auto`, `1.1`, `2`; `--http-version`, `K9_HTTP_VERSION`)
  forces a version, with h2c prior knowledge over plain HTTP. Concurrent
  requests share one connection as streams with per-stream timings. The
  `proto` tag and `res.proto` report the version actually used.
- HTTP/3 (experimental): `httpVersion: '3'` sends HTTPS requests over QUIC
  (quinn + h3), one connection per host shared by concurrent streams. The test
  server answers HTTP/3 on its HTTPS port.

- Distributed runs: `k9 run --agents N` runs the test on `k9 agent`s on other
  machines. The token is required (`--agent-token` / `K9_AGENT_TOKEN`), and TLS
  is optional (`--tls-cert`, `--tls-key`; agents' `--tls-ca`). Agents receive
  the script and the feeder rows.
- Workers talk to the manager over gRPC (`crates/k9-grpc`, a typed
  `worker.proto`) instead of postcard frames; protocol version 3.
- Python scripts: the asyncio loop now takes commands straight from a queue,
  woken through a socket, instead of through a bridge thread. That's 1.6× the
  requests/s in one process. Worker processes size their Tokio runtimes to
  their share of the CPUs. `benchmarks/compare.py --vus`.
- `benchmarks/compare.py`: k9 against k6, oha and Locust on one target;
  results in `docs/benchmarks.md`.
- criterion micro-benchmarks of the hot paths (`cargo bench`); results in
  `docs/benchmarks.md`.
- `/v1/metrics[/{id}]` on the control API, and the `k9 status`, `stats`,
  `pause`, `resume`, `scale` and `stop` commands.
- Warnings when the load generator is the bottleneck: open-file limit (raised
  automatically when possible), ephemeral ports, TIME_WAIT sockets, CPU.
- The `externally-controlled` executor, and a control API: `--address` serves
  k6's `GET`/`PATCH /v1/status` (pause, resume, scale `vus`, stop), and
  `--paused` starts a run paused.
- `--out opentelemetry[=URL]` (alias `experimental-opentelemetry`): OTLP metrics
  over HTTP/protobuf, mapped like k6's OpenTelemetry output; `K6_OTEL_*` /
  `K9_OTEL_*` settings.
- `--out prometheus-rw[=URL]` (alias `experimental-prometheus-rw`): Prometheus
  remote write with k6's metric names, so k6's Grafana dashboards work;
  `K6_PROMETHEUS_RW_*` / `K9_PROMETHEUS_RW_*` settings.
- `--report PATH` (`k9.run(report=…)`): a self-contained HTML report with the
  key numbers, thresholds, checks, per-second charts and every metric.
- `--out csv=PATH`: every sample as CSV, in k6's columns.
- `--out` works with `--workers`: workers forward their samples to the
  manager's outputs (worker protocol v2).
- `summaryTrendStats` (plan option, `--summary-trend-stats`,
  `K9_SUMMARY_TREND_STATS`, `k9.run(summary_trend_stats=…)`) picks the values
  trends report, such as `p(99)` or `count`.
