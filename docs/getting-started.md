# Getting started

## Install

```console
$ pip install k9-load         # or: uv tool install k9-load, pipx install k9-load
$ k9 version
```

Wheels cover Linux (x86_64, aarch64), macOS (arm64, x86_64) and Windows
(x86_64), for Python 3.10 or newer; no Rust needed. Each GitHub release also
has a standalone `k9` binary that runs declarative plans without Python.

To build from source (Rust 1.99 or newer, a C toolchain and
[uv](https://docs.astral.sh/uv/)), see [Development](development.md).

## Your first test

`k9 new` writes a starter that runs as is (see [Writing tests](authoring.md)).
Point it at a service you run, never at one you don't own:

```console
$ k9 new --url http://localhost:3000/health   # script.py: async def default()
$ k9 run script.py -u 5 -d 10s
```

Without `--url` the starter targets `http://localhost:8080/ok`, the test server
in this repository (`cargo run -p k9-testserver`).

## Plans, checks and thresholds

Declarative plans use k6's option names. See [`examples/basic.yaml`](../examples/basic.yaml).

```console
$ cargo run -p k9-testserver &              # a local target on :8080
$ k9 run examples/local.yaml                # run a test, print the summary
$ k9 inspect examples/basic.yaml            # print the fully resolved plan as JSON
$ k9 run plan.yaml -u 20 -d 1m              # override the plan's load shape, as in k6
$ k9 run plan.yaml -s 30s:10 -s 1m:10 -s 15s:0 --insecure-skip-tls-verify
```

For CI, add checks and thresholds (see [`examples/ci.yaml`](../examples/ci.yaml)).
A crossed threshold exits with code 99, as in k6:

```text
     █ api

       ✗ api ok
        ↳  90% — ✓ 270 / ✗ 30

   ✗ checks.........................: 95.00% ✓ 570        ✗ 30
   ✓ http_req_duration..............: avg=51.12ms  min=203.69µs med=1.14ms   max=102.78ms p(90)=102.24ms p(95)=102.37ms
   ✓   { group:::api }..............: avg=471.57µs min=203.69µs med=442µs    max=1.14ms   p(90)=707µs    p(95)=797µs
   ✗ http_req_failed................: 5.00%  ✓ 30         ✗ 570

ERROR thresholds on metrics 'http_req_failed, checks' have been crossed
```

## Overriding options

Options resolve like k6's: flags (`-u/--vus`, `-d/--duration`, `-i/--iterations`,
`-s/--stage`, `--max-redirects`, `--insecure-skip-tls-verify`, `--user-agent`,
`--discard-response-bodies`) beat `K9_*` environment variables (`K9_VUS`,
`K9_DURATION`, `K9_STAGES=30s:10,1m:0`, …), which beat the plan.

```text
running (00m10.1s), 00/10 VUs, 990 complete and 0 interrupted iterations

     data_received..................: 129 kB 13 kB/s
     data_sent......................: 70 kB  7.0 kB/s
     http_req_duration..............: avg=101.88ms min=100.77ms med=101.79ms max=103.13ms p(90)=102.5ms p(95)=102.62ms
       { expected_response:true }...: avg=101.88ms min=100.77ms med=101.79ms max=103.13ms p(90)=102.5ms p(95)=102.62ms
     http_req_failed................: 0.00%  ✓ 0         ✗ 990
     http_reqs......................: 990    98.100557/s
     iteration_duration.............: avg=101.9ms  min=100.78ms med=101.86ms max=103.13ms p(90)=102.5ms p(95)=102.62ms
     iterations.....................: 990    98.100557/s
     vus............................: 10     min=10      max=10
     vus_max........................: 10     min=10      max=10
```

## When a plan is wrong

Invalid plans list every problem and exit with code 104, as k6 does:

```text
error: bad.yaml: invalid plan (2 problems):
  - options.scenarios.fast.rate: not valid for constant-vus (valid: `vus`, `duration`)
  - options.scenarios.fast.duration: required for constant-vus
```
