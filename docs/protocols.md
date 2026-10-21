# HTTP, TLS and network

## HTTP versions

HTTPS speaks HTTP/2 when the server offers it (ALPN), as browsers and k6 do, and
HTTP/1.1 otherwise; plain HTTP stays HTTP/1.1. `httpVersion: '1.1'` or `'2'` (or
`--http-version`, `K9_HTTP_VERSION`) forces one; `'2'` over plain HTTP is h2c with
prior knowledge. Each VU keeps its own connection per host, so on HTTP/2 a VU's
concurrent requests are streams on one connection, each with its own timings
(sending until the body is handed over, waiting until the stream's response
headers). `httpVersion: '3'` speaks HTTP/3 over QUIC (HTTPS only, UDP on the URL's
port), with the same sharing and per-stream timings; the whole QUIC handshake counts
as `http_req_connecting`, since it includes TLS. HTTP/3 is experimental (the `h3`
crate is pre-1.0), and `auto` doesn't switch to it from `Alt-Svc` yet. The `proto`
tag (`HTTP/1.1`, `HTTP/2.0`, `HTTP/3.0`) and Python's `res.proto` say which was used.

## TLS

k6's TLS options work in plans and in scripts' `options`:

```yaml
options:
  tlsAuth:                       # client certificates (mutual TLS), by domain
    - { domains: [api.example.com, '*.internal.example.com'], cert: client.pem, key: client-key.pem }
  tlsVersion: { min: tls1.2, max: tls1.3 }   # or just tls1.2
  tlsCipherSuites: [TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, TLS_AES_128_GCM_SHA256]
```

`cert` and `key` are PEM text or files (relative to the plan or script); an entry
without `domains` applies to every host. The certificate goes with HTTP, WebSocket
and gRPC connections (not HTTP/3 yet). rustls speaks TLS 1.2 and 1.3 only, so
`tls1.0`/`tls1.1` are rejected, as are encrypted keys (`password`). HTTPS samples
get k6's `tls_version` tag, and Python responses have `res.tls_version` and
`res.tls_cipher_suite`.

## Network options

So do k6's network options:

```yaml
options:
  hosts: { api.example.com: 10.0.0.5, 'web.example.com:443': '10.0.0.6:8443', '*.test.local': 127.0.0.1 }
  dns: { ttl: 5m, select: random, policy: preferIPv4 }   # or 'ttl=1m,select=roundRobin'
  blockHostnames: ['*.ads.example.com']
  blacklistIPs: [10.0.0.0/8]
  localIPs: 192.168.1.10-192.168.1.20      # VU n sends from the n-th, wrapping around
  noConnectionReuse: false                 # true: a new connection per request
  noVUConnectionReuse: false               # true: a VU's connections close between iterations
```

`dns.ttl` caps how long lookups are cached (`0`: not at all, `inf`: forever);
`select` picks the address to try first, the others following if it doesn't
answer. Blocked hosts and IPs fail the request before it connects (Python's
`res.error_code` is `blocked`). The CLI has `--dns`, `--block-hostnames`,
`--blacklist-ip`, `--local-ips`, `--no-connection-reuse` and
`--no-vu-connection-reuse`, and the environment `K9_DNS`, `K9_BLOCK_HOSTNAMES`,
`K9_BLACKLIST_IPS`, `K9_LOCAL_IPS`, `K9_NO_CONNECTION_REUSE` and
`K9_NO_VU_CONNECTION_REUSE`. `localIPs` doesn't apply to HTTP/3 yet.

`localIPs` must be addresses of this machine's interfaces. To try it on
loopback, Linux and Windows answer on all of `127.0.0.0/8`; macOS only on
`127.0.0.1` until you add others (`sudo ifconfig lo0 alias 127.0.0.2 up`).
