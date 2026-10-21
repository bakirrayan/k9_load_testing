# Python scripts

`k9 run script.py` imports the script once, reads its `options` dict (same keys
as a plan's `options`), runs `setup()`, then every VU iterates `async def default(data)`
(or a scenario's `exec` function) as a task on one asyncio event loop, then
`teardown(data)`. See [`examples/script.py`](../examples/script.py), which uses all of
the following against `cargo run -p k9-testserver`.

- **`await k9.http.get(url, ...)`** (also `post`, `put`, `patch`, `delete`, `head`,
  `options`, `request`) sends the request on K9's Rust client: each VU has its own
  connections and cookie jar (emptied every iteration), and every request is recorded
  in k6's `http_req_*` metrics with k6's tags. Options: `params`, `headers`, `data`
  (bytes, text or a form), `json`, `files` (a multipart upload: `{"doc": open(...)}`,
  `{"doc": ("a.csv", data, "text/csv")}`), `compression` (`gzip`, `deflate`, `br`,
  `zstd`, or `"gzip, br"`), `tags`, `name` (groups URLs with IDs into one entry) and
  `timeout`. Run requests concurrently with `asyncio.gather`.
- Like k6, requests send `Accept-Encoding: gzip, deflate` unless they set their own,
  and compressed responses (gzip, deflate, br, zstd) are decoded: `res.body` is the
  decoded body, while `data_received` counts what came over the wire. A body that
  isn't what its `Content-Encoding` says fails the request (`error_code` `decompress`).
- As in k6, a request that gets no response returns status 0 with `res.error`
  instead of raising; `res.raise_for_status()` raises `k9.RequestError` or
  `k9.StatusError`. The response has `status`, `headers`, `body`, `text`, `json()`,
  `url` and `timings`.
- **`async with k9.ws.connect(url, ...) as ws:`** opens a WebSocket (`ws://` or
  `wss://`, on a connection of its own; `headers`, `tags`, `name`, `timeout`).
  `await ws.send(str | bytes)`, `await ws.recv(timeout=...)`, `async for message in
  ws`, `await ws.ping()` (round trip in ms) and `await ws.close(code, reason)`.
  Messages are queued as they arrive, so a timed-out `recv()` loses nothing; after
  the close, `recv()` raises `k9.ws.ConnectionClosed` (with the peer's `code` and
  `reason`). A refused upgrade raises `k9.ws.WebSocketError`. It's recorded in k6's
  `ws_sessions`, `ws_connecting`, `ws_session_duration`, `ws_msgs_sent`,
  `ws_msgs_received` and `ws_ping`, plus `data_sent`/`data_received`. In plain
  `def` code, `k9.sync.ws.connect()` gives the same with blocking calls (`with …
  as sock`, `for message in sock`).
- **gRPC (unary):** `protos = k9.grpc.load("hello.proto", import_paths=[...])` compiles
  `.proto` files when the script loads (no `protoc` needed), and
  `client = k9.grpc.Client("host:443", protos)` (`plaintext=True` for h2c) can be
  shared by every VU: each VU calls on an HTTP/2 connection of its own.
  `res = await client.invoke("pkg.Service/Method", {...}, metadata=..., timeout=...)`
  takes and returns dicts (protobuf's JSON mapping); `res.status` is a
  `k9.grpc.StatusCode` (`k9.grpc.StatusOK` …), with `res.message`, `res.error`,
  `res.headers` and `res.trailers`. A call that gets no answer has status
  `DEADLINE_EXCEEDED` or `UNAVAILABLE` rather than raising. Recorded as
  `grpc_req_duration`, tagged with the method and `status`. `k9.sync.grpc` blocks
  instead. `k9.grpc.reflect(target, plaintext=...)` gets the protos from the server's
  reflection service instead of files (at module level: it blocks). See
  [`examples/grpc.py`](../examples/grpc.py).
- **gRPC streams** (client, server or bidirectional): `async with client.stream(method,
  metadata=..., timeout=...) as s:` then `await s.send({...})`, `s.close_send()`, and
  `await s.recv()` or `async for message in s` until the end (`recv()` gives `None`);
  then `s.status`, `s.error`, `s.headers` and `s.trailers` are set. Sending and
  receiving can run in different tasks. `s.recv(timeout=1.5)` raises `TimeoutError`
  if nothing arrives in time; the stream stays open and the message isn't lost
  (nor when `asyncio.wait_for` interrupts `recv()`). Recorded in k6's
  `grpc_streams`, `grpc_streams_msgs_sent` and `grpc_streams_msgs_received`, and
  k9's `grpc_stream_duration` (open to end, or to cancel). `k9.sync.grpc` streams
  block instead (`with client.stream(...) as s`, `for message in s`).
  `k9.grpc.Client(..., metadata={...})` sends that metadata with every call and
  stream (a call's own wins for the same key).
- **`k9.check(res, {"name": lambda r: ...})`** records the `checks` metric;
  **`with k9.group("name"):`** tags what runs inside it and records `group_duration`;
  **`await k9.sleep(seconds)`** pauses the VU.
- **`k9.metrics`**: `Counter`, `Gauge`, `Rate` and `Trend`, declared at module level,
  appear in the summary and can have thresholds.
- **Hooks:** `setup()` returns JSON-serializable data and each VU gets its own copy;
  `vu_init(data)` runs once per VU before its first iteration (`k9.vu.state` keeps
  per-VU values such as a login token); `teardown(data)` runs at the end, even after
  Ctrl-C; `handle_summary(data)` gets k6's summary data and returns what to write
  where (`"stdout"`, `"stderr"` or a file path; `k9.text_summary()` is the default).
  Hooks may be `def` or `async def`.
- `k9.env` is the environment plus `-e KEY=VALUE` flags plus the scenario's `env`.
  `k9.vu.id`, `k9.vu.scenario` and `k9.vu.iteration` describe the running VU.
- **Exceptions:** an exception ends its iteration (which still counts, as in k6). The
  first occurrence of each is printed with its traceback while the test runs; after the
  summary, each is listed once with a count, e.g.
  `ERROR script raised 5×: ValueError: bad response (script.py:16 in default)`.
  K9's own exceptions derive from `k9.K9Error`.
- Exit codes follow k6: 107 when the script raises while loading or in a hook other
  than `vu_init`; 100/101 when `setup()`/`teardown()` time out.
- The package is fully typed: a type checker catches `k9.http.get(url, header=...)`.
- **Plain `def` functions** work too: each such VU runs on its own thread and uses
  the blocking twins in `k9.sync` (`from k9.sync import http, sleep`; same arguments,
  returns the response). `check`, `group` and metrics are the same in both. A script
  can mix `async def` and `def` scenarios. When a sync VU is interrupted, its next k9
  call raises `k9.Interrupted`, and a request or `k9.sync.sleep` in progress returns
  at once. Sync hooks run on their own thread, so a slow `vu_init` doesn't hold up
  other VUs. Using the async API in a `def` function (or `k9.sync` in an `async def`)
  raises a `ContextError` that says which to use. Above 500 sync VUs, K9 warns: they
  share the GIL. See [`examples/sync_script.py`](../examples/sync_script.py).
- `k9 inspect script.py` prints the tier (`async`, or `sync` when a scenario runs a
  plain `def`), the hooks it found and the resolved options.
- **`--workers N`** (or `auto`, one per core) runs the test in N worker processes,
  one [execution segment](scaling.md#by-hand-execution-segments) each, so a
  script gets N Python interpreters. `setup()`, `teardown()` and `handle_summary()`
  run once, in the manager, and `setup()`'s data is sent to every worker. You get one
  live status line and one summary; thresholds (and `abortOnFail`) judge all
  workers' samples together, and totals stay exact (`shared-iterations` included).
  Ctrl-C stops every worker; a worker that crashes is reported (exit code 103).
  `print()` in scripts still shows, and `--out` outputs get every worker's samples.

Measured with a release build on one machine, from one Python process: 1,000 VUs
requesting a 200 ms endpoint complete 99% of the theoretical iterations with p95
203.6 ms, at 54% CPU; 100 VUs against a fast endpoint send about 70,000 requests/s
(the same load as a declarative plan: 135,000/s). With `--workers`, pure-Python CPU work scales almost
linearly (3.75× with 4 workers, 6× with 8 on a 32-core machine). HTTP-bound async
scripts scale when each worker has enough VUs in flight: 8 workers send 153,000
requests/s at 256 VUs, and only 79,000 at 64 VUs. See
[docs/benchmarks.md](benchmarks.md). Plain `def` VUs are slower, since
they hand the GIL back and forth: 100 of them send 10,900 requests/s, and 500 on a
200 ms endpoint reach 97% of the theoretical iterations (p95 204 ms). One Python
process is the limit; `--workers` spreads scripts across cores.
