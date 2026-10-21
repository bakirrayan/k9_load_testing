//! The manager ↔ worker protocol over a real gRPC connection.

use k9_core::engine::{RunEvent, ScriptError};
use k9_core::protocol::{
    AgentGroup, Assignment, FromWorker, PROTOCOL_VERSION, ToWorker, WorkerProgress, WorkerSummary,
};
use k9_core::{Format, Segment, load_plan};
use k9_grpc::{ClientOptions, Error, Listener, ServerOptions, Tls, connect};
use k9_metrics::{
    PortableSample, PortableSamples, PortableSeries, PortableSnapshot, PortableValue,
};

const TOKEN: &str = "t0k3n";

async fn bind() -> Listener {
    Listener::bind("127.0.0.1:0", &ServerOptions::new(TOKEN))
        .await
        .unwrap()
}

fn client() -> ClientOptions {
    ClientOptions::new(TOKEN)
}

fn metrics() -> PortableSnapshot {
    let series = |metric: &str, value| PortableSeries {
        metric: metric.into(),
        tags: vec![("status".into(), "200".into())],
        value,
    };
    PortableSnapshot {
        series: vec![
            series(
                "http_req_duration",
                PortableValue::Trend {
                    count: 2,
                    sum: 30.5,
                    min: 10.0,
                    max: 20.5,
                    clamped: 1,
                    buckets: vec![(10_000, 1), (20_500, 1)],
                },
            ),
            series("http_reqs", PortableValue::Counter { sum: 2.0 }),
            series(
                "vus",
                PortableValue::Gauge {
                    value: 3.0,
                    min: 1.0,
                    max: 4.0,
                },
            ),
            series("checks", PortableValue::Rate { trues: 5, total: 7 }),
        ],
    }
}

fn messages() -> (Vec<ToWorker>, Vec<FromWorker>) {
    let plan = load_plan(
        "options: { vus: 3, duration: 5s }\nflows:\n  default:\n    - request: { url: 'http://x/' }\n",
        Format::Yaml,
    )
    .unwrap();
    let mut assignment = Assignment::new(
        &plan,
        Segment::sequence("0,1/3,2/3,1").unwrap().remove(1),
        "/tmp/script.py".into(),
        vec![("A".into(), "1".into()), ("B".into(), "".into())],
        Some(r#"{"token": "abc"}"#.into()),
    );
    assignment.forward_samples = true;
    assignment.script_source = Some("import k9\n".into());
    let to_worker = vec![
        ToWorker::Assign(assignment),
        ToWorker::Start,
        ToWorker::Stop,
    ];
    let from_worker = vec![
        FromWorker::Hello {
            protocol: PROTOCOL_VERSION,
            version: "0.0.1".into(),
            pid: 42,
            slot: 3,
            token: "t0k3n".into(),
            group: AgentGroup {
                agent: "a1".into(),
                workers: 4,
                weight: 8,
            },
        },
        FromWorker::Ready,
        FromWorker::Tick {
            elapsed_ms: 1000,
            metrics: metrics(),
            progress: WorkerProgress {
                active_vus: 3,
                peak_vus: 3,
                iterations_complete: 10,
                iterations_interrupted: 0,
            },
        },
        FromWorker::Event(RunEvent::ScriptError(ScriptError::with_details(
            "ValueError: x",
            "Traceback ...",
        ))),
        FromWorker::Event(RunEvent::ScriptError(ScriptError::new("no details"))),
        FromWorker::Event(RunEvent::RequestError("could not connect".into())),
        FromWorker::Samples(PortableSamples {
            new_metrics: vec!["http_reqs".into()],
            new_tag_sets: vec![vec![("url".into(), "http://x/".into())], vec![]],
            samples: vec![PortableSample {
                metric: 0,
                tags: 1,
                micros: 1_700_000_000_000_000,
                value: 1.0,
            }],
        }),
        FromWorker::Done(WorkerSummary {
            iterations_complete: 10,
            iterations_interrupted: 1,
            script_errors: vec![("ValueError: x".into(), 3)],
            request_errors: vec![],
            metrics: metrics(),
        }),
        FromWorker::Failed {
            message: "setup data didn't parse".into(),
        },
    ];
    (to_worker, from_worker)
}

#[tokio::test]
async fn every_message_travels_both_ways() {
    let (to_worker, from_worker) = messages();
    let mut listener = bind().await;
    let address = listener.local_addr().to_string();

    let worker = tokio::spawn({
        let (to_worker, from_worker) = (to_worker.clone(), from_worker.clone());
        async move {
            let (tx, mut rx) = connect(&address, &client()).await.unwrap();
            for message in from_worker {
                tx.send(message).await.unwrap();
            }
            for sent in &to_worker {
                assert_eq!(&rx.recv().await.unwrap().unwrap(), sent);
            }
            assert!(rx.recv().await.is_none(), "the manager ended its stream");
            // Ending ours tells the manager we're done.
        }
    });

    let mut conn = listener.accept().await.expect("a worker connects");
    for message in &to_worker {
        conn.tx.send(message.clone()).await.unwrap();
    }
    for sent in &from_worker {
        assert_eq!(&conn.rx.recv().await.unwrap().unwrap(), sent);
    }
    drop(conn.tx);
    worker.await.unwrap();
    assert!(
        conn.rx.recv().await.is_none(),
        "the worker ended its stream"
    );

    let ToWorker::Assign(assignment) = &to_worker[0] else {
        unreachable!()
    };
    assert_eq!(
        assignment.segment.to_string(),
        "1/3:2/3",
        "the segment travels with its sequence"
    );
}

#[tokio::test]
async fn a_worker_that_disappears_ends_the_stream() {
    let mut listener = bind().await;
    let address = listener.local_addr().to_string();
    let worker = tokio::spawn(async move {
        let (tx, rx) = connect(&address, &client()).await.unwrap();
        tx.send(FromWorker::Ready).await.unwrap();
        // Dropped without a goodbye, as when the process dies.
        drop((tx, rx));
    });
    let mut conn = listener.accept().await.unwrap();
    assert_eq!(conn.rx.recv().await.unwrap().unwrap(), FromWorker::Ready);
    worker.await.unwrap();
    // Either a clean end or an error: never a hang.
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), conn.rx.recv())
        .await
        .expect("the manager notices");
    assert!(!matches!(next, Some(Ok(_))), "{next:?}");
}

#[tokio::test]
async fn connecting_to_nothing_is_an_error() {
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = free.local_addr().unwrap().to_string();
    drop(free);
    let err = connect(&address, &client())
        .await
        .expect_err("nothing listens");
    assert!(err.to_string().contains(&address), "{err}");
}

#[tokio::test]
async fn a_wrong_token_is_refused_before_any_message() {
    let mut listener = bind().await;
    let address = listener.local_addr().to_string();
    let err = connect(&address, &ClientOptions::new("guess"))
        .await
        .expect_err("refused");
    assert!(matches!(err, Error::Unauthenticated { .. }), "{err}");
    assert!(err.to_string().contains("token"), "{err}");
    // The listener never sees it.
    let accepted =
        tokio::time::timeout(std::time::Duration::from_millis(200), listener.accept()).await;
    assert!(accepted.is_err(), "no connection is handed over");
}

fn certificate() -> Tls {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    Tls {
        cert_pem: cert.cert.pem().into_bytes(),
        key_pem: cert.signing_key.serialize_pem().into_bytes(),
    }
}

#[tokio::test]
async fn tls_carries_the_stream_to_workers_that_trust_the_certificate() {
    let tls = certificate();
    let ca = tls.cert_pem.clone();
    let options = ServerOptions {
        tls: Some(tls),
        ..ServerOptions::new(TOKEN)
    };
    let mut listener = Listener::bind("127.0.0.1:0", &options).await.unwrap();
    let address = format!("https://localhost:{}", listener.local_addr().port());

    // Without the CA, the certificate isn't trusted; without TLS, no stream.
    let untrusted = connect(&address, &client()).await;
    assert!(
        untrusted.is_err(),
        "a self-signed certificate isn't trusted by default"
    );
    let plain = format!("localhost:{}", listener.local_addr().port());
    assert!(
        connect(&plain, &client()).await.is_err(),
        "plain HTTP/2 to a TLS port"
    );

    let trusting = ClientOptions {
        ca_pem: Some(ca),
        ..client()
    };
    let (tx, mut rx) = connect(&address, &trusting).await.unwrap();
    tx.send(FromWorker::Ready).await.unwrap();
    let mut conn = listener.accept().await.unwrap();
    assert_eq!(conn.rx.recv().await.unwrap().unwrap(), FromWorker::Ready);
    conn.tx.send(ToWorker::Start).await.unwrap();
    assert_eq!(rx.recv().await.unwrap().unwrap(), ToWorker::Start);
}
