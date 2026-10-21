# Declarative plans

A plan (YAML, TOML or JSON) describes the whole test: `options` with k6's names,
and `flows` of steps each iteration runs. Nothing runs in Python, so plans go at
full Rust speed (Tier 1).

## Templates, extractors and feeders

Declarative flows can carry data without any Python in the hot path. `{{ … }}`
templates in URLs, header values and bodies are parsed once. Extractors
(`json`, `regex`, `header`) set per-VU variables. Feeders (inline `rows`, or a
`.csv`/`.json` file next to the plan) give each iteration one row: the next
one (`sequential`, split between workers), a random one, or one per VU
(`per-vu`). Generators include `uuid()`, `timestamp()`, `randomInt(min, max)`
and `randomString(n)`, plus k6's `__VU`, `__ITER` and `__ENV.NAME`:

```yaml
feeders:
  users: { file: users.csv, mode: per-vu }
flows:
  default:
    - request:
        method: POST
        url: https://example.com/login
        body: '{"username": "{{ users.username }}", "password": "{{ users.password }}"}'
        extract: { token: { json: $.token } }
    - request:
        url: https://example.com/me
        headers: { Authorization: 'Bearer {{ token }}' }
        checks: { ok: { status: 200 } }
```

If an extractor finds nothing, the rest of that iteration is skipped and the
error is reported.

A request can send a `multipart` form instead of a `body` (text fields may hold
templates; files are read once, relative to the plan), and `compression` compresses
its body:

```yaml
    - request:
        method: POST
        url: 'https://example.com/upload'
        multipart:
          owner: 'vu{{ __VU }}'
          report: { file: data/report.pdf, contentType: application/pdf }
        compression: gzip        # or deflate, br, zstd, "gzip, br"
```

A `batch` step sends its requests in parallel, like k6's `http.batch`: at most
`batch` (default 20) at once, and `batchPerHost` (default 6, 0 for no limit) per
host (both also `--batch`/`--batch-per-host` and `K9_BATCH`/`K9_BATCH_PER_HOST`).
Once all have finished, each is recorded, checked and extracted from as usual, so
later steps can use what any of them returned. From Python, `k9.plan.batch([p.get(...),
p.post(...)])`.

```yaml
    - batch:
        - { url: 'https://example.com/api/profile' }
        - { url: 'https://example.com/api/cart', extract: { cart: { json: '$.id' } } }
        - { url: 'https://cdn.example.com/logo.png' }
```

A `websocket` step opens a WebSocket and runs its own steps on it, still with
no Python: `send` (templated text), `receive` (waits up to `timeout`, default
60s; `bodyContains`/`durationBelow` checks and `json`/`regex` extractors on the
message), `ping` (waits for the pong), `sleep` and `close`. It closes normally at
the end. Its `checks` apply to the handshake (`status: 101`). A `receive` that
gets nothing (timeout, or the server closed) fails its checks, reports the error
and ends that WebSocket; the flow goes on. From Python, `k9.plan.websocket(url,
[p.ws_send(...), p.ws_receive(...), p.ws_ping(), p.ws_close()])` builds the same.

```yaml
    - websocket:
        url: 'wss://example.com/chat?token={{ token }}'
        checks: { upgraded: { status: 101 } }
        steps:
          - send: '{"join": "room-{{ __VU }}"}'
          - receive: { timeout: 5s, checks: { joined: { bodyContains: joined } } }
          - ping
```

A `grpc` step makes a unary gRPC call. `protos` (`.proto` files, relative to the
plan, or named within `importPaths`) define the method; without them, the server's
reflection service is asked once before the run. The `message` is protobuf's JSON
mapping, and its strings (like `metadata` values) may hold templates. Checks are
`status` (the gRPC code: 0 is OK), `bodyContains` (the response message as JSON)
and `durationBelow`; extractors read the message (`json`, `regex`) or its metadata
(`header`, then the trailers). It's recorded in `grpc_req_duration`. A streaming
method takes `messages: [...]`: they're sent in order, the stream is half-closed and
every answer read; checks see the final status, `bodyContains` and extractors see the
answers as a JSON array (`$[0].text`), and it's recorded in `grpc_streams*`. From Python,
`k9.plan.grpc(target, method, message, ...)`. See
[`examples/grpc.yaml`](../examples/grpc.yaml).

```yaml
    - grpc:
        target: api.example.com:443        # `plaintext: true` for h2c
        protos: [protos/hello.proto]
        method: hello.Greeter/SayHello
        message: { name: 'vu{{ __VU }}' }
        metadata: { authorization: 'Bearer {{ token }}' }
        checks: { ok: { status: 0 } }
        extract: { greeting: { json: '$.message' } }
```

`compression: gzip` on a step (or `k9.grpc.Client(..., compression="gzip")`)
compresses the messages sent; answers in gzip are read either way, and k9 tells
servers it accepts them. `maxSendSize` and `maxReceiveSize` (bytes; the
client's `max_send_size`/`max_receive_size`) fail a call or stream with
`RESOURCE_EXHAUSTED` when a message is larger, as k6's connect options do; by
default there's no limit.

With `--agents`, a plan's `.proto` files are compiled on the manager and travel
in the plan, so agents don't need them; files a script loads itself
(`k9.grpc.load`) must exist on each agent (or use reflection).

Taken from Goose, a scenario can run a weighted mix of transactions (flows, or
functions in a script) in place of `exec`, with `onStart` and `onStop` run once
per VU:

```yaml
options:
  scenarios:
    shop:
      executor: constant-vus
      vus: 50
      duration: 5m
      transactions: { browse: 5, search: 3, checkout: 1 }
      onStart: login
      onStop: logout
```

Each VU works through shuffled rounds of 9 iterations (5 browse, 3 search,
1 checkout), so the mix is exact, not just right on average. Iteration metrics
carry a `transaction` tag, so a threshold like `iteration_duration{transaction:checkout}`
targets one of them. `onStart` and `onStop` aren't iterations. `onStop` runs
when a VU stops gracefully, not when it's interrupted. With `onStart`, a VU
keeps its cookies between iterations, so a login session carries over (without
it, cookies are cleared before each iteration, as in k6). In a script, a
scenario's functions must all be `async def` or all plain `def`.

## From Python: `k9.plan`

A script that defines a module-level `plan` built with `k9.plan` is Tier 1.
Python only builds the plan, once, when the script loads. The run is then as
declarative as a YAML plan (`k9 inspect` says `"tier": "declarative"`), so it
goes at full Rust speed, and `--workers` works the same way. The helpers return
plain data, and variables, feeder columns and generators are `{{ … }}` template
strings, so f-strings work:

```python
import k9
import k9.plan as p

users = p.feeder("users", file="users.csv", mode="per-vu")  # next to the script
token = p.var("token")

plan = p.Plan(
    [
        p.post(
            k9.env["BASE"] + "/login",
            json={"username": users.username, "password": users.password},
            extract={"token": p.from_json("$.token")},
        ),
        p.get(
            k9.env["BASE"] + "/me",
            headers={"Authorization": f"Bearer {token}"},
            checks={"ok": p.status(200)},
        ),
        p.sleep(1),
    ],
    options={"vus": 10, "duration": "30s"},
    feeders=[users],
)
```

`k9.plan` is a separate namespace from Tier 3's `k9.http`, because its
functions describe requests instead of sending them. A script can't mix the
two: it defines `plan` or `default()`, not both, and Tier 1 has no hooks.
