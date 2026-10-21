# Benchmarks

This page has two parts:
- micro-benchmarks of k9's hot paths, with
  [criterion](https://github.com/bheisler/criterion.rs);
- a comparison against k6, oha and Locust on the same target, in
  [Comparison with other tools](#comparison-with-other-tools).

```sh
cargo bench -p k9-metrics --bench metrics   # recording, interning, aggregation
cargo bench -p k9-core    --bench engine    # whole runs: per-iteration overhead
cargo bench -p k9-http    --bench client    # the HTTP client against a local server
```

The results below come from an Intel i9-13900K (24 cores, 32 threads) with 62 GB
of RAM, running Linux 6.18 under WSL2, with rustc 1.98.1 and the release profile.
Each figure is criterion's median.

## Metrics (`k9-metrics`)

| Benchmark | Time | Throughput |
|---|---|---|
| `record/counter`: one sample | 22.5 ns | 44 M/s |
| `record/trend`: one sample | 25.0 ns | 40 M/s |
| `record/counter/with_samples`: raw samples also kept, for `--out` | 49.0 ns | 20 M/s |
| `record/trend/with_samples` | 53.7 ns | 19 M/s |
| `record_request_threads/1`: what one request records (8 trends, 2 counters, 1 rate) | 265 ns per request | 3.8 M requests/s |
| `record_request_threads/8` | 6.9 ms per 160k requests | 23 M requests/s |
| `record_request_threads/32` | 13.1 ms per 640k requests | 49 M requests/s |
| `intern/existing`: format a URL and look up a known 5-tag set | 115 ns | — |
| `aggregate/tick_10k_requests`: the 1 s tick, with 110k samples over 20 series | 0.80 ms | 12.6 M requests/s |

Recording stays per thread, so it scales with threads: 32 threads record 13× what
one does, on 24 physical cores with hyper-threading. The metrics of one request
cost about a quarter of a microsecond. Keeping raw samples for `--out` roughly
doubles the cost of each sample. The once-a-second aggregation takes under a
millisecond for a second of 10,000 requests.

## Engine (`k9-core`)

Each benchmark is a whole `k9_core::engine::run`, including start-up and teardown.

| Benchmark | Time per run | Iterations/s |
|---|---|---|
| `empty_flow`: 10 VUs, 20,000 iterations of an empty flow | 3.97 ms | 5.0 M (≈200 ns each) |
| `one_request`: 64 VUs, 6,400 iterations of `GET /ok` | 44.1 ms | 145 k |
| `login_flow`: 64 VUs, 3,200 iterations of 2 requests (templated body, JSON extraction, templated header, check) | 44.5 ms | 72 k (144 k requests/s) |

With the same 64 connections, the engine matches the bare HTTP client
(`http/concurrent_64_clients` below, 145 k requests/s). The local server and the
loopback set the pace, not k9's scheduling, metrics or Tier 1 templates. A
`login_flow` iteration costs exactly two requests: templates are parsed once, and
extracting and checking add nothing measurable.

## HTTP client (`k9-http`)

The server is k9-testserver (axum) on the same machine and the same Tokio runtime.

| Benchmark | Time | Requests/s |
|---|---|---|
| `sequential_keep_alive`: one request at a time, reused connection | 157 µs | 6.4 k |
| `sequential_64k_body` | 280 µs | 3.6 k |
| `concurrent_64_clients`: 64 connection pools × 50 requests | 22.0 ms | 145 k |

On this machine, loopback latency dominates the sequential figure. In throwaway
probes (not kept in the repo):
- a bare Tokio TCP echo takes 105–114 µs per round trip under WSL2;
- raw HTTP/1.1 over one TCP stream to the same server takes 131 µs;
- k9's client takes 160 µs.

So the client adds about 30 µs over raw TCP, and the machine's loopback accounts
for the rest. Connections are reused: 3 connections for 4,201 requests.

## Comparison with other tools

`benchmarks/compare.py` runs every tool against the same k9-testserver and
collects its numbers:

```sh
cargo build --release -p k9-testserver -p k9-cli
uv run maturin develop --release        # for the Python cases
python3 benchmarks/compare.py --duration 20s --json results.json
```

**Setup.**
- **CPUs:** the server is pinned to CPUs 0–7 and each tool to CPUs 8–31
  (24 logical CPUs), with `taskset`.
- **Machine:** the one described at the top of this page. Everything runs on one
  machine, over WSL2's loopback.
- **Versions:** k6 v2.3.0, oha 1.16.0, Locust 2.46.6 (`FastHttpUser`), and k9
  0.1, release build.
- **Duration:** 20 s per run.
- **Raw results:** [benchmarks/results/](../benchmarks/results/), at 64 and
  256 VUs.

The two cases:
- **throughput:** 64 VUs (or connections, or users) send `GET /ok` back to back,
  with no pause between requests.
- **rate:** 2,000 requests/s (arrival rate) to `/delay/10`, which answers after
  10 ms. On this machine the server's timer and the loopback bring that to about
  11.2 ms, so anything above that is added by the tool.

Column notes:
- **Requests/s:** the rate the tool reports.
- **Server saw:** the server's own request count for the run.
- **CPU s and peak RSS:** the tool's process tree, from `wait4`. Peak RSS is the
  largest single process, so with several processes the total is higher.

At 64 VUs, both cases:

| Case | Tool | Requests/s | p50 ms | p95 ms | p99 ms | Failed | Server saw | CPU s | CPU ms / 1k req | Peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|
| throughput | k9 (YAML plan) | 153,533 | 0.35 | 0.62 | 0.78 | 0 | 3,072,880 | 49.0 | 16 | 75 |
| throughput | k9 (Python, 1 process) | 69,684 | 0.25 | 0.43 | 0.53 | 0 | 1,394,126 | 54.8 | 39 | 66 |
| throughput | k9 (Python, --workers 8) | 79,105 | 0.35 | 0.58 | 0.72 | 0 | 1,583,727 | 238.8 | 151 | 32 |
| throughput | k6 | 99,547 | 0.55 | 1.16 | 1.77 | 0 | 1,991,035 | 153.8 | 77 | 455 |
| throughput | oha | 154,535 | 0.38 | 0.73 | 0.96 | 0 | 3,091,435 | 84.0 | 27 | 1020 |
| throughput | Locust (1 process) | 21,363 | 2.00 | 3.00 | 3.00 | 0 | 425,610 | 20.1 | 47 | 57 |
| throughput | Locust (--processes 8) | 89,244 | 1.00 | 1.00 | 2.00 | 0 | 1,787,433 | 152.8 | 86 | 55 |
| rate | k9 (YAML plan) | 1,999 | 11.31 | 11.85 | 12.20 | 0 | 40,000 | 2.1 | 53 | 28 |
| rate | k9 (Python, 1 process) | 1,999 | 10.96 | 12.03 | 12.23 | 0 | 40,000 | 5.4 | 134 | 38 |
| rate | k9 (Python, --workers 8) | 1,997 | 11.27 | 11.72 | 12.12 | 0 | 40,000 | 9.7 | 243 | 34 |
| rate | k6 | 1,999 | 11.37 | 11.95 | 12.26 | 0 | 40,002 | 5.3 | 132 | 78 |
| rate | oha | 2,000 | 11.27 | 11.86 | 12.26 | 0 | 40,001 | 1.5 | 38 | 29 |
| rate | Locust (1 process) | 1,999 | 13.00 | 15.00 | 16.00 | 0 | 39,900 | 2.7 | 68 | 58 |
| rate | Locust (--processes 8) | 2,000 | 12.00 | 13.00 | 13.00 | 0 | 40,013 | 4.5 | 112 | 55 |

Throughput at 256 VUs (`--vus 256 --cases throughput`):

| Tool | Requests/s | p50 ms | p95 ms | p99 ms | Failed | Server saw | CPU s | CPU ms / 1k req | Peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|
| k9 (YAML plan) | 245,958 | 0.82 | 1.84 | 2.55 | 0 | 4,924,743 | 85.0 | 17 | 116 |
| k9 (Python, 1 process) | 77,037 | 0.35 | 0.61 | 0.78 | 0 | 1,541,359 | 56.5 | 37 | 86 |
| k9 (Python, --workers 8) | 152,777 | 0.61 | 1.69 | 2.92 | 0 | 3,058,997 | 299.3 | 98 | 34 |
| k6 | 179,328 | 1.15 | 2.89 | 4.54 | 0 | 3,586,754 | 290.9 | 81 | 895 |
| oha | 237,024 | 0.89 | 2.40 | 3.58 | 0 | 4,741,914 | 136.1 | 29 | 1180 |
| Locust (1 process) | 19,665 | 8.00 | 12.00 | 18.00 | 0 | 392,010 | 20.0 | 51 | 65 |
| Locust (--processes 8) | 112,581 | 2.00 | 4.00 | 5.00 | 0 | 2,256,190 | 160.8 | 71 | 55 |

What this shows:
- **Declarative plans (Tier 1) are the fastest.**
  - At 64 VUs they match oha, a bare HTTP benchmarking tool (153k requests/s).
  - At 256 VUs they lead: 246k against oha's 237k and k6's 179k, with the lowest
    tail latency.
  - They cost the least CPU per request (16–17 ms per 1,000 requests, against
    oha's 27–29 and k6's 77–81) and a fraction of oha's and k6's memory.
- **Latency matches the fastest tools.** In the rate case, k9's percentiles are
  within 0.1–0.4 ms of oha's and k6's. Locust reports whole milliseconds only,
  and adds up to 2 ms at p50.
- **One Python process gives 3–4× Locust's single process** (70–77k against
  20–21k requests/s), for less CPU per request and a fraction of its latency.
- **Python scripts with `--workers`** give 1.4× Locust's 8 processes at 256 VUs
  (153k against 113k requests/s), with about half its p99.
- **At 64 VUs, `--workers 8` adds little over one process** (79k against 70k).
  Each worker only has 8 VUs, and an async iteration still crosses threads about
  four times: VU task → asyncio loop → Tokio for the request → loop → VU task.
  At roughly 100 µs per wake-up on this machine, a worker waits on wake-ups more
  than it computes. Locust's gevent stays on one thread. With more VUs per worker
  in flight, those waits overlap, which is why 256 VUs scale.
- **Every tool held the fixed rate.** The server saw 40,000 requests (±13) from
  every tool except single-process Locust (39,900).

Caveats:
- Everything ran on one machine, and WSL2's loopback adds about 110 µs per round
  trip, so absolute latencies are higher than between two real hosts.
- Each figure is from one 20 s run. Repeated runs (3 s, 15 s, 20 s) gave the same
  ranking, within a few percent.
- oha counts requests still in flight at its deadline as errors ("aborted due to
  deadline"); the script leaves them out.
- Locust's percentiles are rounded to whole milliseconds.
