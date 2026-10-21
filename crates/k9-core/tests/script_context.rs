//! What script code can do through its [`ScriptContext`]: HTTP requests on the
//! VU's client, checks, groups and custom metrics, `vu_init`, and live errors.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_core::engine::{
    self, BoxFuture, MetricDecl, RequestMeta, RunControl, RunEvent, RunResult, Script,
    ScriptContext, ScriptError, ScriptVu, VuInfo,
};
use k9_core::{Plan, PlanConfig};
use k9_http::Request;
use k9_metrics::{MetricId, MetricKind, Value, ValueType};
use k9_testserver::TestServer;

type Step =
    Arc<dyn Fn(ScriptContext, u64) -> BoxFuture<'static, Result<(), ScriptError>> + Send + Sync>;

fn step<F, Fut>(f: F) -> Step
where
    F: Fn(ScriptContext, u64) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ScriptError>> + Send + 'static,
{
    Arc::new(move |ctx, n| Box::pin(f(ctx, n)))
}

/// A script made of closures, each given the context the engine passed in
/// (and, for iterations, the VU's iteration number).
#[derive(Default, Clone)]
struct Fake {
    metrics: Vec<MetricDecl>,
    setup: Option<Step>,
    teardown: Option<Step>,
    vu_init: Option<Step>,
    iteration: Option<Step>,
    /// Root groups of the contexts the engine handed out, by hook.
    roots: Arc<Mutex<Vec<String>>>,
}

impl Script for Fake {
    fn metrics(&self) -> Vec<MetricDecl> {
        self.metrics.clone()
    }

    fn setup(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.roots
            .lock()
            .unwrap()
            .push(format!("setup {:?}", ctx.root_group()));
        match &self.setup {
            Some(f) => f(ctx, 0),
            None => Box::pin(async { Ok(()) }),
        }
    }

    fn teardown(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.roots
            .lock()
            .unwrap()
            .push(format!("teardown {:?}", ctx.root_group()));
        match &self.teardown {
            Some(f) => f(ctx, 0),
            None => Box::pin(async { Ok(()) }),
        }
    }

    fn vu(&self, _vu: VuInfo, ctx: ScriptContext) -> Box<dyn ScriptVu> {
        Box::new(FakeVu {
            ctx,
            n: 0,
            script: self.clone(),
        })
    }
}

struct FakeVu {
    ctx: ScriptContext,
    n: u64,
    script: Fake,
}

impl ScriptVu for FakeVu {
    fn init(&mut self) -> BoxFuture<'_, Result<(), ScriptError>> {
        match &self.script.vu_init {
            Some(f) => f(self.ctx.clone(), 0),
            None => Box::pin(async { Ok(()) }),
        }
    }

    fn iteration(&mut self, _function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        let n = self.n;
        self.n += 1;
        match &self.script.iteration {
            Some(f) => f(self.ctx.clone(), n),
            None => Box::pin(async { Ok(()) }),
        }
    }
}

fn plan(options: serde_json::Value) -> Plan {
    PlanConfig {
        options: serde_json::from_value(options).unwrap(),
        ..Default::default()
    }
    .resolve_script(&["default"])
    .unwrap_or_else(|e| panic!("{e}"))
}

async fn run(options: serde_json::Value, script: Fake) -> RunResult {
    engine::run_script(
        &plan(options),
        RunControl::new(),
        Vec::new(),
        Arc::new(script),
    )
    .await
    .unwrap()
}

fn merged(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> Option<Value> {
    let m = result.aggregator.metrics();
    result
        .aggregator
        .totals()
        .merged(metric, filter, m.interner())
}

fn count(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> f64 {
    match merged(result, metric, filter) {
        Some(Value::Counter(c)) => c.sum,
        Some(Value::Rate(r)) => r.total as f64,
        Some(Value::Trend(t)) => t.count as f64,
        Some(Value::Gauge(g)) => g.value,
        None => 0.0,
    }
}

fn passes(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> u64 {
    match merged(result, metric, filter) {
        Some(Value::Rate(r)) => r.trues,
        other => panic!("not a rate: {other:?}"),
    }
}

fn metric(result: &RunResult, name: &str) -> MetricId {
    result
        .aggregator
        .metrics()
        .registry()
        .get(name)
        .unwrap_or_else(|| panic!("no metric {name}"))
}

/// A URL nothing listens on (the port was just released), so connecting fails fast.
fn refused_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}/", listener.local_addr().unwrap())
}

fn get(url: &str) -> Request {
    Request::get(url).unwrap()
}

#[tokio::test]
async fn requests_are_sent_on_the_vus_client_and_tagged_like_flow_requests() {
    let server = TestServer::start().await.unwrap();
    let ok = server.url("/ok");
    let failing = server.url("/status/503");
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let (ok, failing) = (ok.clone(), failing.clone());
            async move {
                let res = ctx
                    .request(
                        &get(&ok),
                        &RequestMeta {
                            group: "::api",
                            ..RequestMeta::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(res.status, 200);
                let res = ctx
                    .request(
                        &get(&failing),
                        &RequestMeta {
                            name: Some("unavailable"),
                            tags: &[("kind", "flaky")],
                            ..RequestMeta::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(res.status, 503);
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 2, "iterations": 6}), script).await;
    let b = result.builtins;
    let ok = server.url("/ok");
    let failing = server.url("/status/503");

    assert_eq!(result.iterations_complete, 6);
    assert_eq!(count(&result, b.http_reqs, &[]), 12.0);
    let tags = [
        ("scenario", "default"),
        ("group", "::api"),
        ("method", "GET"),
        ("url", ok.as_str()),
        ("name", ok.as_str()),
        ("status", "200"),
        ("expected_response", "true"),
        ("proto", "HTTP/1.1"),
    ];
    assert_eq!(count(&result, b.http_reqs, &tags), 6.0);
    assert_eq!(count(&result, b.http_req_duration, &tags), 6.0);
    let tags = [
        ("group", ""),
        ("url", failing.as_str()),
        ("name", "unavailable"),
        ("kind", "flaky"),
        ("status", "503"),
        ("expected_response", "false"),
    ];
    assert_eq!(count(&result, b.http_reqs, &tags), 6.0);
    assert_eq!(passes(&result, b.http_req_failed, &[]), 6, "the 503s");
    assert!(count(&result, b.data_received, &[("scenario", "default")]) > 0.0);
    assert_eq!(
        server.connections(),
        2,
        "each VU keeps its own keep-alive connection"
    );
}

#[tokio::test]
async fn failed_requests_record_status_0_and_count_the_error() {
    let refused = refused_url();
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let refused = refused.clone();
            async move {
                let err = ctx
                    .request(&get(&refused), &RequestMeta::default())
                    .await
                    .unwrap_err();
                assert!(err.to_string().contains("could not connect"), "{err}");
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 1, "iterations": 3}), script).await;
    let b = result.builtins;
    assert_eq!(count(&result, b.http_reqs, &[("status", "0")]), 3.0);
    assert_eq!(passes(&result, b.http_req_failed, &[]), 3);
    assert_eq!(result.request_errors.len(), 1);
    assert_eq!(result.request_errors[0].1, 3);
}

#[tokio::test]
async fn a_request_timeout_applies_to_that_request_only() {
    let server = TestServer::start().await.unwrap();
    let slow = server.url("/delay/2000");
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let slow = slow.clone();
            async move {
                let started = std::time::Instant::now();
                let err = ctx
                    .request(
                        &get(&slow),
                        &RequestMeta {
                            timeout: Some(Duration::from_millis(100)),
                            ..RequestMeta::default()
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(started.elapsed() < Duration::from_secs(1));
                assert_eq!(err.to_string(), "request timed out after 100ms");
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 1, "iterations": 1}), script).await;
    assert_eq!(result.request_errors[0].0, "request timed out after 100ms");
}

#[tokio::test]
async fn cookies_last_for_one_iteration_of_one_vu() {
    let server = TestServer::start().await.unwrap();
    let set = server.url("/cookies/set/session/abc");
    let has = server.url("/cookies/has/session");
    let script = Fake {
        iteration: Some(step(move |ctx, n| {
            let (set, has) = (set.clone(), has.clone());
            async move {
                let meta = RequestMeta::default();
                let before = ctx.request(&get(&has), &meta).await.unwrap().status;
                assert_eq!(before, 404, "iteration {n} starts with an empty jar");
                ctx.request(&get(&set), &meta).await.unwrap();
                let after = ctx.request(&get(&has), &meta).await.unwrap().status;
                assert_eq!(after, 200);
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(
        serde_json::json!({"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 2, "iterations": 2}}}),
        script,
    )
    .await;
    assert_eq!(result.iterations_complete, 4);
    assert!(result.script_errors.is_empty());
}

#[tokio::test]
async fn checks_groups_and_custom_metrics_carry_the_vus_tags() {
    let script = Fake {
        metrics: vec![
            MetricDecl::new("orders", MetricKind::Counter, ValueType::Default),
            MetricDecl::new("cart_time", MetricKind::Trend, ValueType::Time),
        ],
        iteration: Some(step(|ctx, n| async move {
            ctx.check("status is 200", true, "::shop", &[]);
            ctx.check("fast", n % 2 == 0, "", &[("kind", "latency")]);
            ctx.group_duration("::shop", Duration::from_millis(5));
            ctx.add("orders", 2.0, "::shop", &[("tier", "gold")])
                .unwrap();
            ctx.add("cart_time", 12.5, "", &[]).unwrap();
            let err = ctx.add("nope", 1.0, "", &[]).unwrap_err();
            assert_eq!(err.to_string(), "unknown metric `nope`");
            Ok(())
        })),
        ..Fake::default()
    };
    let result = run(
        serde_json::json!({
            "vus": 1, "iterations": 4, "tags": {"env": "ci"},
            "thresholds": {"orders{tier:gold}": ["count==8"], "cart_time": ["avg==12.5"]},
        }),
        script,
    )
    .await;
    let b = result.builtins;
    let vu = [("scenario", "default"), ("env", "ci")];

    let shop = [
        vu[0],
        vu[1],
        ("group", "::shop"),
        ("check", "status is 200"),
    ];
    assert_eq!(
        (
            passes(&result, b.checks, &shop),
            count(&result, b.checks, &shop)
        ),
        (4, 4.0)
    );
    let fast = [("group", ""), ("check", "fast"), ("kind", "latency")];
    assert_eq!(
        (
            passes(&result, b.checks, &fast),
            count(&result, b.checks, &fast)
        ),
        (2, 4.0)
    );
    assert_eq!(
        count(&result, b.group_duration, &[vu[0], ("group", "::shop")]),
        4.0
    );

    let orders = metric(&result, "orders");
    assert_eq!(
        count(
            &result,
            orders,
            &[vu[0], vu[1], ("group", "::shop"), ("tier", "gold")]
        ),
        8.0
    );
    let def = result
        .aggregator
        .metrics()
        .registry()
        .def(metric(&result, "cart_time"))
        .clone();
    assert_eq!(
        (def.kind, def.value_type),
        (MetricKind::Trend, ValueType::Time)
    );
    assert!(
        result.thresholds.iter().all(|t| t.passed),
        "thresholds on custom metrics: {:?}",
        result.thresholds
    );
}

#[tokio::test]
async fn hooks_get_contexts_rooted_in_their_own_group() {
    let server = TestServer::start().await.unwrap();
    let ok = server.url("/ok");
    let hook = move |ctx: ScriptContext, _| {
        let ok = ok.clone();
        async move {
            let group = ctx.root_group().to_owned();
            ctx.request(
                &get(&ok),
                &RequestMeta {
                    group: &group,
                    ..RequestMeta::default()
                },
            )
            .await
            .unwrap();
            ctx.check("hook ran", true, &group, &[]);
            Ok(())
        }
    };
    let script = Fake {
        setup: Some(step(hook.clone())),
        teardown: Some(step(hook)),
        ..Fake::default()
    };
    let roots = script.roots.clone();
    let result = run(serde_json::json!({"vus": 1, "iterations": 1}), script).await;
    let b = result.builtins;
    assert_eq!(
        *roots.lock().unwrap(),
        ["setup \"::setup\"", "teardown \"::teardown\""]
    );
    assert_eq!(count(&result, b.http_reqs, &[("group", "::setup")]), 1.0);
    assert_eq!(count(&result, b.http_reqs, &[("group", "::teardown")]), 1.0);
    assert_eq!(count(&result, b.checks, &[("group", "::setup")]), 1.0);
    assert!(
        merged(
            &result,
            b.http_reqs,
            &[("group", "::setup"), ("scenario", "default")]
        )
        .is_none(),
        "hooks run outside any scenario"
    );
}

#[tokio::test]
async fn vu_init_runs_once_per_vu_before_its_iterations() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (init_log, iter_log) = (log.clone(), log.clone());
    let script = Fake {
        vu_init: Some(step(move |_, _| {
            let log = init_log.clone();
            async move {
                log.lock().unwrap().push("init".to_owned());
                Ok(())
            }
        })),
        iteration: Some(step(move |_, n| {
            let log = iter_log.clone();
            async move {
                log.lock().unwrap().push(format!("iteration {n}"));
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(
        serde_json::json!({"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 3, "iterations": 2}}}),
        script,
    )
    .await;
    assert_eq!(result.iterations_complete, 6);
    let log = log.lock().unwrap();
    assert_eq!(log.iter().filter(|l| *l == "init").count(), 3);
    let first_iteration = log.iter().position(|l| l.starts_with("iteration")).unwrap();
    assert!(
        log[..first_iteration].iter().any(|l| l == "init"),
        "init before iterations: {log:?}"
    );
}

#[tokio::test]
async fn a_vu_whose_init_fails_runs_no_iterations() {
    let script = Fake {
        vu_init: Some(step(|_, _| async {
            Err(ScriptError::new(
                "PermissionError: login refused (script.py:3 in vu_init)",
            ))
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 2, "duration": "200ms"}), script).await;
    assert_eq!(result.iterations_complete, 0);
    assert_eq!(
        result.script_errors,
        [(
            "PermissionError: login refused (script.py:3 in vu_init)".to_owned(),
            2
        )]
    );
}

#[tokio::test]
async fn the_first_occurrence_of_each_error_is_published_live() {
    let refused = refused_url();
    let script = Fake {
        iteration: Some(step(move |ctx, n| {
            let refused = refused.clone();
            async move {
                if n == 0 {
                    // Fails to connect: a request error.
                    let _ = ctx.request(&get(&refused), &RequestMeta::default()).await;
                }
                Err(ScriptError::with_details(
                    format!("ValueError: bad {}", n % 2),
                    format!(
                        "Traceback (most recent call last):\nValueError: bad {}",
                        n % 2
                    ),
                ))
            }
        })),
        ..Fake::default()
    };
    let control = RunControl::new();
    let mut events = control.events();
    let result = engine::run_script(
        &plan(serde_json::json!({"vus": 1, "iterations": 6})),
        control,
        Vec::new(),
        Arc::new(script),
    )
    .await
    .unwrap();
    assert_eq!(result.script_errors.len(), 2);

    let mut seen = Vec::new();
    while let Ok(event) = events.try_recv() {
        seen.push(match event {
            RunEvent::ScriptError(err) => format!("script: {}", err.details.unwrap()),
            RunEvent::RequestError(message) => format!("request: {message}"),
        });
    }
    assert_eq!(seen.len(), 3, "{seen:?}");
    assert!(
        seen[0].starts_with("request: could not connect to 127.0.0.1:"),
        "{seen:?}"
    );
    assert_eq!(
        seen[1..],
        [
            "script: Traceback (most recent call last):\nValueError: bad 0",
            "script: Traceback (most recent call last):\nValueError: bad 1",
        ]
    );
}

#[tokio::test]
async fn contexts_kept_by_the_script_dont_keep_the_script_alive() {
    // Python keeps each VU's context in its own objects, which the script owns;
    // if contexts referenced the script, that cycle would never be freed, and
    // the script's last reference could drop on the wrong thread (k9-py).
    let kept: Arc<Mutex<Vec<ScriptContext>>> = Arc::default();
    let keep = kept.clone();
    let script = Arc::new(Fake {
        iteration: Some(step(move |ctx, _| {
            let keep = keep.clone();
            async move {
                keep.lock().unwrap().push(ctx);
                Ok(())
            }
        })),
        ..Fake::default()
    });
    engine::run_script(
        &plan(serde_json::json!({"vus": 2, "iterations": 4})),
        RunControl::new(),
        Vec::new(),
        script.clone(),
    )
    .await
    .unwrap();
    assert_eq!(kept.lock().unwrap().len(), 4);
    assert_eq!(
        Arc::strong_count(&script),
        1,
        "only the test holds the script"
    );
}

#[tokio::test]
async fn websocket_sessions_record_k6s_ws_metrics() {
    use k9_http::{WsEvent, WsMessage, WsRequest};
    let server = TestServer::start().await.unwrap();
    let url = server.url("/ws/echo").replacen("http", "ws", 1);
    let script = Fake {
        iteration: Some(step({
            let url = url.clone();
            move |ctx, _| {
                let url = url.clone();
                async move {
                    let meta = RequestMeta {
                        group: "::chat",
                        tags: &[("room", "a")],
                        ..RequestMeta::default()
                    };
                    let mut ws = ctx
                        .websocket(&WsRequest::new(&url).unwrap(), &meta)
                        .await
                        .unwrap();
                    ws.send(WsMessage::Text("hi".into())).await.unwrap();
                    ws.send(WsMessage::Text("there".into())).await.unwrap();
                    for _ in 0..2 {
                        let event = ws.recv().await.unwrap().unwrap();
                        assert!(matches!(event, WsEvent::Message(_)), "{event:?}");
                    }
                    ws.ping().await.unwrap();
                    assert!(matches!(
                        ws.recv().await.unwrap().unwrap(),
                        WsEvent::Pong(_)
                    ));
                    ws.close(1000, "").await.unwrap();
                    while ws.recv().await.is_some() {}
                    Ok(())
                }
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 2, "iterations": 4}), script).await;
    let m = |name| metric(&result, name);
    let tags = [
        ("scenario", "default"),
        ("group", "::chat"),
        ("room", "a"),
        ("url", url.as_str()),
        ("name", url.as_str()),
        ("status", "101"),
    ];
    assert_eq!(count(&result, m("ws_sessions"), &tags), 4.0);
    assert_eq!(count(&result, m("ws_connecting"), &tags), 4.0);
    assert_eq!(count(&result, m("ws_session_duration"), &tags), 4.0);
    assert_eq!(count(&result, m("ws_msgs_sent"), &tags), 8.0);
    assert_eq!(count(&result, m("ws_msgs_received"), &tags), 8.0);
    assert_eq!(count(&result, m("ws_ping"), &tags), 4.0);
    let b = result.builtins;
    assert!(count(&result, b.data_sent, &[]) > 0.0);
    assert!(count(&result, b.data_received, &[]) > 0.0);
    assert_eq!(count(&result, b.http_reqs, &[]), 0.0, "not HTTP requests");
}

#[tokio::test]
async fn a_refused_websocket_counts_as_an_error() {
    use k9_http::WsRequest;
    let server = TestServer::start().await.unwrap();
    let url = server.url("/status/404").replacen("http", "ws", 1);
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let url = url.clone();
            async move {
                let err = ctx
                    .websocket(&WsRequest::new(&url).unwrap(), &RequestMeta::default())
                    .await
                    .expect_err("404");
                assert!(err.to_string().contains("404"));
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"iterations": 2}), script).await;
    assert_eq!(count(&result, metric(&result, "ws_sessions"), &[]), 0.0);
    assert_eq!(result.request_errors.iter().map(|(_, n)| n).sum::<u64>(), 2);
}

#[tokio::test]
async fn grpc_calls_record_grpc_req_duration_with_their_status() {
    use k9_http::grpc::{GrpcRequest, Protos, target};
    let server = TestServer::start().await.unwrap();
    let proto = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../k9-testserver/proto/k9/test/echo.proto"
    );
    let protos = Protos::load(&[proto], &[] as &[&str]).unwrap();
    let method = protos.method("k9.test.Echo/Say").unwrap();
    let url = target(&server.http_addr().to_string(), true).unwrap();
    let ok = GrpcRequest::new(&url, &method, &serde_json::json!({"text": "hi"})).unwrap();
    let missing = GrpcRequest::new(
        &url,
        &method,
        &serde_json::json!({"text": "gone", "status": 5}),
    )
    .unwrap();
    let refused_target = target(refused_url().trim_end_matches('/'), true).unwrap();
    let refused =
        GrpcRequest::new(&refused_target, &method, &serde_json::json!({"text": "hi"})).unwrap();
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let (ok, missing, refused) = (ok.clone(), missing.clone(), refused.clone());
            async move {
                let meta = RequestMeta {
                    group: "::rpc",
                    tags: &[("room", "a")],
                    ..RequestMeta::default()
                };
                let res = ctx.grpc_invoke(&ok, &meta).await;
                assert_eq!(res.status, 0, "{}", res.status_message);
                assert_eq!(res.message.unwrap()["text"], "hi");
                assert_eq!(ctx.grpc_invoke(&missing, &meta).await.status, 5);
                let named = RequestMeta {
                    name: Some("down"),
                    ..RequestMeta::default()
                };
                assert_eq!(ctx.grpc_invoke(&refused, &named).await.status, 14);
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 2, "iterations": 4}), script).await;
    let duration = metric(&result, "grpc_req_duration");
    let path = "/k9.test.Echo/Say";
    let say_url = server.url(path);
    let tags = |status| {
        [
            ("scenario", "default"),
            ("group", "::rpc"),
            ("room", "a"),
            ("url", say_url.as_str()),
            ("method", path),
            ("name", path),
            ("rpc_type", "unary"),
            ("status", status),
        ]
    };
    assert_eq!(count(&result, duration, &tags("0")), 4.0);
    assert_eq!(count(&result, duration, &tags("5")), 4.0);
    assert_eq!(
        count(&result, duration, &[("name", "down"), ("status", "14")]),
        4.0
    );
    let b = result.builtins;
    assert!(count(&result, b.data_sent, &[]) > 0.0);
    assert!(count(&result, b.data_received, &[]) > 0.0);
    assert_eq!(count(&result, b.http_reqs, &[]), 0.0, "not HTTP requests");
    assert_eq!(result.request_errors.iter().map(|(_, n)| n).sum::<u64>(), 4);
}

#[tokio::test]
async fn grpc_streams_record_k6s_stream_metrics() {
    use k9_http::grpc::{GrpcEvent, Protos, target};
    let server = TestServer::start().await.unwrap();
    let proto = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../k9-testserver/proto/k9/test/echo.proto"
    );
    let protos = Protos::load(&[proto], &[] as &[&str]).unwrap();
    let chat = protos.method("k9.test.Echo/Chat").unwrap();
    let url = target(&server.http_addr().to_string(), true).unwrap();
    let refused = target(refused_url().trim_end_matches('/'), true).unwrap();
    let script = Fake {
        iteration: Some(step(move |ctx, _| {
            let (chat, url, refused) = (chat.clone(), url.clone(), refused.clone());
            async move {
                let meta = RequestMeta {
                    group: "::chat",
                    tags: &[("room", "a")],
                    ..RequestMeta::default()
                };
                let stream = ctx
                    .grpc_stream(
                        &url,
                        &chat,
                        http::HeaderMap::new(),
                        &Default::default(),
                        &meta,
                    )
                    .await;
                let (mut tx, mut rx) = stream.split();
                // Receive on another task while this one sends.
                let received = tokio::spawn(async move {
                    let mut messages = 0;
                    while let Some(event) = rx.recv().await {
                        match event {
                            GrpcEvent::Message(_) => messages += 1,
                            GrpcEvent::End(end) => assert_eq!(end.status, 0),
                        }
                    }
                    messages
                });
                for text in ["a", "b", "c"] {
                    tx.send(&serde_json::json!({ "text": text })).await.unwrap();
                }
                tx.close();
                assert_eq!(received.await.unwrap(), 3);

                let mut down = ctx
                    .grpc_stream(
                        &refused,
                        &chat,
                        http::HeaderMap::new(),
                        &Default::default(),
                        &meta,
                    )
                    .await;
                match down.recv().await {
                    Some(GrpcEvent::End(end)) => assert_eq!(end.status, 14),
                    other => panic!("{other:?}"),
                }
                Ok(())
            }
        })),
        ..Fake::default()
    };
    let result = run(serde_json::json!({"vus": 2, "iterations": 4}), script).await;
    let m = |name| metric(&result, name);
    let tags = [
        ("scenario", "default"),
        ("group", "::chat"),
        ("room", "a"),
        ("method", "/k9.test.Echo/Chat"),
        ("name", "/k9.test.Echo/Chat"),
        ("rpc_type", "bidi_streaming"),
    ];
    assert_eq!(count(&result, m("grpc_streams"), &tags), 8.0);
    assert_eq!(count(&result, m("grpc_streams_msgs_sent"), &tags), 12.0);
    assert_eq!(count(&result, m("grpc_streams_msgs_received"), &tags), 12.0);
    assert_eq!(
        count(&result, m("grpc_req_duration"), &[]),
        0.0,
        "unary only"
    );
    let b = result.builtins;
    assert!(count(&result, b.data_sent, &[]) > 0.0);
    assert_eq!(
        result.request_errors.iter().map(|(_, n)| n).sum::<u64>(),
        4,
        "the refused streams"
    );
}
