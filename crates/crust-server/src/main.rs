use std::path::PathBuf;
use std::sync::Arc;

use crust::media::MantleAdapter;
use crust::routeplanner::RoutePlanner;
use crust_mantle_adapter::RealMantleAdapter;
use crust_oto_adapter::OtoVoiceBackend;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config_path = config_path()?;
    let config = ServerConfig::load(config_path.as_deref())?;
    let shutdown_timeout = config.shutdown_timeout;
    let mantle: Arc<dyn MantleAdapter> = Arc::new(RealMantleAdapter::with_defaults(
        RoutePlanner::new(std::iter::empty()),
    )?);
    let voice = Arc::new(OtoVoiceBackend::with_defaults(
        config.max_players,
        config.max_concurrent_voice_connects,
    )?);
    let server = CrustServer::bind_with_backends(config, mantle, voice).await?;
    let address = server.local_address()?;
    tracing::info!(%address, "Crust server listening");
    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();
    let signal = tokio::spawn(async move {
        shutdown_signal().await;
        signal_token.cancel();
    });
    let result = server.serve(shutdown).await;
    signal.abort();
    let _ = signal.await;
    if result
        .as_ref()
        .is_err_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
    {
        tracing::error!(?shutdown_timeout, "graceful shutdown deadline elapsed");
    }
    result.map_err(Into::into)
}

fn config_path() -> Result<Option<PathBuf>, String> {
    let mut arguments = std::env::args_os().skip(1);
    let Some(argument) = arguments.next() else {
        return Ok(None);
    };
    if argument != "--config" {
        return Err("usage: crust-server [--config PATH]".to_owned());
    }
    let path = arguments
        .next()
        .ok_or_else(|| "--config requires a path".to_owned())?;
    if arguments.next().is_some() {
        return Err("usage: crust-server [--config PATH]".to_owned());
    }
    Ok(Some(path.into()))
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
