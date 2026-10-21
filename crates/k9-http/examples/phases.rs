//! Prints k6-style phase timings against a local k9-testserver.
//!
//!     cargo run -p k9-http --example phases

use std::time::Duration;

use k9_http::{Client, ClientConfig, Request, Timings};
use k9_testserver::TestServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = TestServer::start().await?;
    let client = Client::new(ClientConfig {
        extra_root_certs: vec![server.cert_der().clone()],
        ..ClientConfig::default()
    })?;

    println!(
        "{:<28} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "request", "blocked", "connect", "tls", "sending", "waiting", "receiv.", "duration"
    );
    for (label, url) in [
        ("http  /delay/100 (new)", server.url("/delay/100")),
        ("http  /delay/100 (reused)", server.url("/delay/100")),
        ("http  /bytes/5MB (reused)", server.url("/bytes/5000000")),
        ("https /delay/10 (new)", server.https_url("/delay/10")),
        ("https /delay/10 (reused)", server.https_url("/delay/10")),
        (
            "http  localhost (new)",
            format!("http://localhost:{}/ok", server.http_addr().port()),
        ),
    ] {
        let res = client.send(&Request::get(&url)?).await?;
        print_row(label, &res.timings);
    }
    Ok(())
}

fn print_row(label: &str, t: &Timings) {
    let ms = |d: Duration| format!("{:.3}ms", d.as_secs_f64() * 1e3);
    println!(
        "{label:<28} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        ms(t.blocked),
        ms(t.connecting),
        ms(t.tls_handshaking),
        ms(t.sending),
        ms(t.waiting),
        ms(t.receiving),
        ms(t.duration()),
    );
}
