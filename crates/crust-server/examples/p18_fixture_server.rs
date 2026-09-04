//! P18-only protocol/event benchmark server with deterministic fake media.

use std::sync::Arc;

use crust::media::MantleAdapter;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::FakeMantle;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ServerConfig::default();
    // The 100-player event burst holds one orchestration permit per active
    // fake source. No socket or source HTTP is created by this test double.
    config.max_outbound_connections = 256;
    let adapter: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let server = CrustServer::bind_with_adapter(config, adapter).await?;
    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();
    let signal = tokio::spawn(async move {
        shutdown_signal().await;
        signal_token.cancel();
    });
    let result = server.serve(shutdown).await;
    signal.abort();
    let _ = signal.await;
    result.map_err(Into::into)
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        result = tokio::signal::ctrl_c() => { let _ = result; }
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
