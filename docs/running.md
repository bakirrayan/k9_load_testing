# Running tests

## Live dashboard

`k9 run --tui` replaces the status line with a live dashboard: the state and
progress, tiles (VUs, requests per second, failed %, p95, iterations, data),
braille charts of requests (and failures) per second and of p95/p99/avg response
times, a VUs sparkline, each check's pass rate, each threshold's current verdict
(✓/✗), and a log of warnings and errors. Keys: `p` pauses or resumes (a run whose
scenarios are all `externally-controlled`), `+`/`-` change the VUs, `q` (or Ctrl-C)
stops, twice to exit at once. The summary prints as usual when it ends. Colours:
`--theme brand` (the logo's gold and blue, the default) or `--theme neon`, in 24-bit
colour when `COLORTERM` is `truecolor`/`24bit`, the nearest of 256 colours
otherwise, and none with `NO_COLOR`. Without a terminal, `--tui` falls back to the
usual output.

## Run options

More of k6's run options, from plans, scripts' `options`, flags or `K9_*` variables:

```yaml
options:
  minIterationDuration: 1s        # shorter iterations sleep the rest (--min-iteration-duration)
  noCookiesReset: true            # VUs keep cookies across iterations (--no-cookies-reset)
  throw: true                     # script requests with no response raise k9.RequestError (-w)
  systemTags: [status, method, name, check, scenario]   # the tags samples keep (--system-tags)
```

Output flags: `--http-debug` prints each request and response (headers) on
stderr, `--http-debug=full` their bodies too (also the `httpDebug` option:
`headers` or `full`, and `K9_HTTP_DEBUG`); `-q`/`--quiet` hides the progress line;
`--no-summary` skips the end-of-test summary, `handle_summary()` and
`--summary-export`; `--no-color` (or `NO_COLOR`) keeps the dashboard to the
terminal's own colours and sets `noColor` in `handle_summary`'s data.

`systemTags` drops the engine's tags that aren't listed (k6's default list is the
default); `vu`, `iter`, `ip` and `ocsp_status` aren't supported yet. `throw` only
affects requests that get no response (DNS, connect, TLS, timeout); a 4xx or 5xx
still returns, as in k6. The padding from `minIterationDuration` counts in
`iteration_duration`.

## Arrival rates and coordinated omission

Arrival-rate executors (`constant-arrival-rate`, `ramping-arrival-rate`) start
iterations on schedule, allocate VUs up to `maxVUs`, and count `dropped_iterations`
when none is free, as in k6. They also report `iteration_duration_corrected`,
measured from when each iteration was *due* (dropped ones included, as if they had
waited for the next free VU): when the server stalls, `iteration_duration` only
shows the few iterations in flight, while the corrected metric shows the stall
(coordinated omission).

## Live control

`--address HOST:PORT` serves a control API with k6's `/v1/status` and
`/v1/metrics[/{id}]` endpoints (JSON:API). Metrics show each metric's totals so
far, updated every second. `GET /v1/status` reports `paused`, `running`,
`stopped`, `vus` and `vus-max`.
`PATCH` with `{"data": {"attributes": {…}}}` changes the run:
- `paused: true/false` pauses or resumes it;
- `vus: N` scales an `externally-controlled` scenario, up to its `maxVUs`;
- `stopped: true` stops it, like Ctrl-C.

`--paused` starts the run paused (any executor), waiting for a resume before
`setup()`. Once running, only a test whose scenarios are all externally-controlled
can pause. A pause holds new iterations and that scenario's `duration` clock.
Like k6 2.0, the API is off unless `--address` is given. k6 2.0 dropped
`externally-controlled`, but k9 keeps it for k6 1.x scripts. An
externally-controlled scenario can't be split across `--workers` or execution
segments, and `vus-max` can't change mid-run.

The commands `k9 status`, `k9 stats`, `k9 pause`, `k9 resume`,
`k9 scale --vus N` and `k9 stop` call that API (`--address`, default
`localhost:6565`). They print the run's status as YAML, or one line per metric.

## When the load generator is the bottleneck

k9 warns when the load generator itself may be the bottleneck:
- **Open files:** it raises the open-file soft limit to the hard one, and warns if
  that still can't give every VU a socket.
- **Ports:** it warns when there are more VUs than ephemeral ports, or (live) when
  TIME_WAIT sockets fill half the port range.
- **CPU:** it warns (live) when the machine's CPU stays at least 90% busy for 3
  seconds.

The port and CPU checks read `/proc`, so they only run on Linux.
