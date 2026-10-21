use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use k9_testserver::TestServer;

/// Target server for testing K9 (see the crate docs for endpoints).
#[derive(Parser)]
#[command(name = "k9-testserver", version)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value_t = 8443)]
    tls_port: u16,
    /// Where to write the client certificate (client.pem) and key
    /// (client-key.pem) the mutual-TLS port accepts
    #[arg(long)]
    identity_dir: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let server = TestServer::bind(
        SocketAddr::new(args.host, args.port),
        SocketAddr::new(args.host, args.tls_port),
    )
    .await?;
    println!("k9-testserver listening on");
    println!("  http://{}", server.http_addr());
    println!(
        "  https://{} (self-signed; HTTP/3 on the same UDP port)",
        server.https_addr()
    );
    println!(
        "  https://localhost:{} (mutual TLS: needs the client certificate)",
        server.mtls_addr().port()
    );
    if let Some(dir) = args.identity_dir {
        let id = server.client_identity();
        std::fs::write(dir.join("client.pem"), &id.cert_pem)?;
        std::fs::write(dir.join("client-key.pem"), &id.key_pem)?;
    }
    tokio::signal::ctrl_c().await?;
    Ok(())
}
