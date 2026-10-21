# Results and outputs

## Files

`--out json=results.jsonl` and `--out csv=results.csv` stream every sample in
k6's JSON and CSV formats (also with `--workers`, whose samples all go through
the manager). `--summary-export summary.json` writes k6's end-of-test JSON, and
`--no-thresholds` skips threshold evaluation. k6's `summaryTrendStats` option
(or `--summary-trend-stats avg,med,p(99),count`) picks the values trends show in
the summary and in both JSON summaries; `summaryTimeUnit` (`s`, `ms` or `us`,
`--summary-time-unit`) shows every time in the summary in one unit.

## HTML report

`--report report.html` (or `k9.run(..., report="report.html")`) writes the run as one
self-contained HTML page that works offline: the key numbers, thresholds, checks,
per-second charts (VUs, requests and failures per second, response-time avg/p(95)/p(99),
iterations per second) and every metric. It has light and dark themes, and it's
small enough (about 30 KB) to keep as a CI artifact.

## Prometheus remote write

`--out prometheus-rw[=URL]` (or k6's name, `experimental-prometheus-rw`) pushes
metrics to Prometheus remote write with k6's names and units:
- counters are `k6_<name>_total`, gauges `k6_<name>`, and rates `k6_<name>_rate`;
- each trend stat gets its own series, like `k6_http_req_duration_p99`, with times
  in seconds.

That way k6's Grafana dashboards work unchanged. It takes k6's
`K6_PROMETHEUS_RW_*` settings (`SERVER_URL`, `PUSH_INTERVAL`, `TREND_STATS`,
`USERNAME`/`PASSWORD`, `BEARER_TOKEN`, `HEADERS_<name>`, `HTTP_HEADERS`, `LABELS`,
`INSECURE_SKIP_TLS_VERIFY`, `TREND_AS_NATIVE_HISTOGRAM`, `STALE_MARKERS`), also spelled
`K9_PROMETHEUS_RW_*`. With `TREND_AS_NATIVE_HISTOGRAM`, trends are sent as native
histograms (`k6_http_req_duration_seconds`) in place of their stats; with
`STALE_MARKERS`, a last push marks every series stale when the test ends. Not yet
supported: client certificates and SigV4. Pushes go out on a thread of their own; a
failing endpoint is reported but doesn't fail the run.

## InfluxDB

`--out influxdb[=URL]` sends every sample to InfluxDB v1 like k6's output does: the
URL names the database (`http://localhost:8086/k6` by default, created first), each
point is named after its metric with a `value` field, and the tags in `TAGS_AS_FIELDS`
(`vu:int,iter:int,url` by default) become fields. That way k6's InfluxDB dashboards
work unchanged. `--out xk6-influxdb[=URL]` does the same for InfluxDB v2 (k6's
extension), with `ORGANIZATION`, `BUCKET` and `TOKEN`. Both take k6's `K6_INFLUXDB_*`
settings (`ADDR`, `DB`, `USERNAME`/`PASSWORD`, `PUSH_INTERVAL`, `PRECISION`,
`RETENTION`, `CONSISTENCY`, `INSECURE`), also spelled `K9_INFLUXDB_*`.

## StatsD and Datadog

`--out statsd[=ADDR]` sends samples over UDP as StatsD lines, like k6's output:
counters as counts, gauges as gauges, trends as timings in ms, and checks as
`check.<name>.pass`/`.fail` counts. It takes k6's `K6_STATSD_*` settings (`ADDR`,
`NAMESPACE`, `PUSH_INTERVAL`, `BUFFER_SIZE`, `ENABLE_TAGS`, `TAG_BLOCKLIST`).
`--out datadog[=ADDR]` is the same with DogStatsD tags on, for the Datadog agent
(`K6_DATADOG_*`).

## OpenTelemetry

`--out opentelemetry[=URL]` (or k6's `experimental-opentelemetry`) exports OTLP
metrics over HTTP (protobuf) to a collector, the way k6's OpenTelemetry output does:
- counters become monotonic sums, and gauges stay gauges;
- rates become one counter with a `condition` attribute (`zero` or `nonzero`);
- trends become histograms in `ms` or `By`.

Values are cumulative, and tags become attributes. It takes k6's `K6_OTEL_*`
settings (`HTTP_EXPORTER_ENDPOINT`, `HTTP_EXPORTER_URL_PATH`,
`HTTP_EXPORTER_INSECURE`, `EXPORT_INTERVAL`, `METRIC_PREFIX`, `SERVICE_NAME`,
`SERVICE_VERSION`, `HEADERS`, `HTTP_EXPORTER_USERNAME`/`PASSWORD`,
`TLS_INSECURE_SKIP_VERIFY`), also spelled `K9_OTEL_*`. Unlike k6, it only speaks
`http/protobuf` (no gRPC yet), and `service.name` defaults to `k9`.
