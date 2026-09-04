use std::path::PathBuf;
use std::sync::Arc;

use crust::media::MantleAdapter;
use crust_mantle_adapter::{MantleAdapterOptions, RealMantleAdapter};
use crust_oto_adapter::OtoVoiceBackend;
use crust_server::CrustServer;
use crust_server::config::{CliOverrides, ServerConfig};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let (config_path, cli) = cli_options()?;
    let config = ServerConfig::load_with_cli(config_path.as_deref(), &cli)?;
    let limits = config.resource_limits()?;
    let shutdown_timeout = limits.shutdown_timeout();
    let route_planner = config.route_planner()?;
    let mantle_options = MantleAdapterOptions {
        allow_youtube_search: config.search.youtube_enabled,
        max_playlist_pages: config.search.youtube_playlist_load_limit,
        connect_timeout: config.http_source.timeouts.connect,
        request_timeout: config.http_source.timeouts.socket,
    };
    let mantle: Arc<dyn MantleAdapter> = Arc::new(RealMantleAdapter::with_options(
        route_planner.clone(),
        mantle_options,
    )?);
    let voice = Arc::new(OtoVoiceBackend::with_defaults(
        limits.max_players.get(),
        limits.max_concurrent_voice_connects.get(),
    )?);
    let server =
        CrustServer::bind_with_backends_and_route_planner(config, mantle, voice, route_planner)
            .await?;
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

fn cli_options() -> Result<(Option<PathBuf>, CliOverrides), String> {
    let mut arguments = std::env::args_os().skip(1);
    let mut path = None;
    let mut overrides = CliOverrides::default();
    while let Some(argument) = arguments.next() {
        match argument.to_string_lossy().as_ref() {
            "--config" => {
                path = Some(
                    arguments
                        .next()
                        .ok_or_else(|| "--config requires a path".to_owned())?
                        .into(),
                );
            }
            "--address" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--address requires an IP address".to_owned())?;
                overrides.address = Some(
                    value
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "--address requires an IP address".to_owned())?,
                );
            }
            "--port" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--port requires a port".to_owned())?;
                overrides.port = Some(
                    value
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "--port requires a valid port".to_owned())?,
                );
            }
            "--password" => {
                overrides.password = Some(
                    arguments
                        .next()
                        .ok_or_else(|| "--password requires a value".to_owned())?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            "--http2" => overrides.http2 = Some(true),
            "--no-http2" => overrides.http2 = Some(false),
            _ => return Err("usage: crust-server [--config PATH] [--address IP] [--port PORT] [--password VALUE] [--http2|--no-http2]".to_owned()),
        }
    }
    Ok((path, overrides))
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
