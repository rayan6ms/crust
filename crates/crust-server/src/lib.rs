pub mod config;
mod json_limits;
pub mod player;
pub mod session;
mod stats;
mod track;

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::rejection::QueryRejection;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use crust::extensions::{ExtensionDescriptor, ExtensionKind, ExtensionRegistry, StaticExtension};
use crust::media::{
    AdapterError, AdapterErrorKind, EncodedTrack, LoadOutcome, LoadRequest, MantleAdapter,
    SourceRoute,
};
use crust::resources::ResourceLimits;
use crust::routeplanner::{RoutePlanner, RoutePlannerDetails, RoutePlannerSnapshot};
use crust::voice::VoiceBackend;
use crust_protocol::{PatchField, PlayerUpdate, SessionUpdate};
use futures_util::{Sink, SinkExt, StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use config::ServerConfig;
use json_limits::{
    JsonPolicy, parse_bounded_json, validate_json_object, validate_load_outcome,
    validate_media_track,
};
use player::{
    PlayerError, PlayerExecutor, PlayerHandle, PlayerLoadAdmission, disabled_filter_names,
    validate_update,
};
use session::{
    PlayerAdmissionError, PrepareError, PreparedSession, SessionClock, SessionPlayer,
    SessionRegistry, SessionSettingsUpdate, SystemSessionClock,
};
use stats::{MetricsRegistry, StatsCollector, StatsSnapshot};

pub const LAVALINK_VERSION: &str = "4.2.2";
pub const MANTLE_REVISION: &str = "29aee076d294856d39b03e6dccdadf5672f0250a";
pub const CRUST_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const OTO_VERSION: &str = "1.0.0";
const MAX_ROUTE_PLANNER_BODY_BYTES: usize = 4 * 1024;

struct AppState {
    config: ServerConfig,
    limits: ResourceLimits,
    password_verifier: PasswordVerifier,
    next_request_id: AtomicU64,
    sessions: SessionRegistry,
    players: PlayerExecutor,
    adapter: Option<Arc<dyn MantleAdapter>>,
    load_requests: Arc<Semaphore>,
    source_requests: Arc<Semaphore>,
    outbound_connections: Arc<Semaphore>,
    stats: StatsCollector,
    metrics: MetricsRegistry,
    extensions: ExtensionRegistry,
    route_planner: RoutePlanner,
    accepting: std::sync::atomic::AtomicBool,
}

impl AppState {
    fn new(
        config: ServerConfig,
        limits: ResourceLimits,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
        voice: Option<Arc<dyn VoiceBackend>>,
        route_planner: RoutePlanner,
    ) -> Self {
        let has_voice = voice.is_some();
        let password_verifier = PasswordVerifier::new(config.password());
        let sessions = SessionRegistry::new(
            limits.max_sessions.get(),
            limits.max_players.get(),
            limits.max_players_per_session.get(),
            limits.max_concurrent_session_resumes.get(),
            limits.websocket_critical_capacity.get(),
            clock,
        );
        let load_requests = Arc::new(Semaphore::new(limits.max_concurrent_loads.get()));
        let source_requests = Arc::new(Semaphore::new(limits.max_concurrent_source_requests.get()));
        let outbound_connections = Arc::new(Semaphore::new(limits.max_outbound_connections.get()));
        let players = PlayerExecutor::new(
            config.player_executor_shards,
            limits.player_command_capacity.get(),
            limits.max_players.get(),
            JsonPolicy::from(&limits),
            PlayerLoadAdmission::new(
                Arc::clone(&load_requests),
                Arc::clone(&source_requests),
                Arc::clone(&outbound_connections),
            ),
            adapter.clone(),
            voice,
        );
        let extensions = registered_extensions(adapter.is_some(), has_voice);
        Self {
            config,
            limits,
            password_verifier,
            next_request_id: AtomicU64::new(1),
            sessions,
            players,
            adapter,
            load_requests,
            source_requests,
            outbound_connections,
            stats: StatsCollector::new(),
            metrics: MetricsRegistry::default(),
            extensions,
            route_planner,
            accepting: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

#[derive(Clone)]
pub struct CrustServer {
    listener: Arc<TcpListener>,
    state: Arc<AppState>,
}

impl CrustServer {
    pub async fn bind(config: ServerConfig) -> io::Result<Self> {
        Self::bind_with_clock_adapter_and_voice(
            config,
            Arc::new(SystemSessionClock::new()),
            None,
            None,
        )
        .await
    }

    pub async fn bind_with_adapter(
        config: ServerConfig,
        adapter: Arc<dyn MantleAdapter>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_and_voice(
            config,
            Arc::new(SystemSessionClock::new()),
            Some(adapter),
            None,
        )
        .await
    }

    pub async fn bind_with_backends(
        config: ServerConfig,
        adapter: Arc<dyn MantleAdapter>,
        voice: Arc<dyn VoiceBackend>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_and_voice(
            config,
            Arc::new(SystemSessionClock::new()),
            Some(adapter),
            Some(voice),
        )
        .await
    }

    pub async fn bind_with_backends_and_route_planner(
        config: ServerConfig,
        adapter: Arc<dyn MantleAdapter>,
        voice: Arc<dyn VoiceBackend>,
        route_planner: RoutePlanner,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_voice_and_route_planner(
            config,
            Arc::new(SystemSessionClock::new()),
            Some(adapter),
            Some(voice),
            route_planner,
        )
        .await
    }

    pub async fn bind_with_clock(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_and_voice(config, clock, None, None).await
    }

    pub async fn bind_with_clock_and_adapter(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_and_voice(config, clock, adapter, None).await
    }

    pub async fn bind_with_clock_adapter_and_voice(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
        voice: Option<Arc<dyn VoiceBackend>>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_adapter_voice_and_route_planner(
            config,
            clock,
            adapter,
            voice,
            RoutePlanner::disabled(),
        )
        .await
    }

    pub async fn bind_with_clock_adapter_voice_and_route_planner(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
        voice: Option<Arc<dyn VoiceBackend>>,
        route_planner: RoutePlanner,
    ) -> io::Result<Self> {
        let limits = config
            .resource_limits()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if route_planner
            .failure_capacity()
            .is_some_and(|capacity| capacity > limits.max_route_planner_failures.get())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RoutePlanner failure capacity exceeds the central resource limit",
            ));
        }
        let listener = TcpListener::bind(config.socket_address()).await?;
        Ok(Self {
            listener: Arc::new(listener),
            state: Arc::new(AppState::new(
                config,
                limits,
                clock,
                adapter,
                voice,
                route_planner,
            )),
        })
    }

    pub fn local_address(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn router(&self) -> Router {
        router(Arc::clone(&self.state))
    }

    pub async fn serve(self, shutdown: CancellationToken) -> io::Result<()> {
        let application = self.router();
        let listener = Arc::try_unwrap(self.listener)
            .map_err(|_| io::Error::other("server listener still has another owner"))?;
        let shutdown_for_server = shutdown.clone();
        let sessions = self.state.sessions.clone();
        let players = self.state.players.clone();
        let state_for_shutdown = Arc::clone(&self.state);
        let graceful = axum::serve(listener, application.into_make_service())
            .with_graceful_shutdown(async move {
                shutdown_for_server.cancelled().await;
                state_for_shutdown.accepting.store(false, Ordering::Release);
                sessions.shutdown();
            });
        let shutdown_sequence = async move {
            let result = graceful.await;
            players.shutdown().await;
            result
        };
        tokio::pin!(shutdown_sequence);
        tokio::select! {
            result = &mut shutdown_sequence => result,
            () = async {
                shutdown.cancelled().await;
                tokio::time::sleep(self.state.limits.shutdown_timeout()).await;
            } => Err(io::Error::new(io::ErrorKind::TimedOut, "server shutdown deadline elapsed")),
        }
    }

    pub async fn spawn(config: ServerConfig) -> io::Result<RunningServer> {
        let server = Self::bind(config).await?;
        let address = server.local_address()?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { server.serve(task_shutdown).await });
        Ok(RunningServer {
            address,
            shutdown,
            task: Some(task),
        })
    }
}

pub struct RunningServer {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl RunningServer {
    #[must_use]
    pub const fn local_address(&self) -> SocketAddr {
        self.address
    }

    pub async fn shutdown(mut self, deadline: Duration) -> io::Result<()> {
        self.shutdown.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match timeout(deadline, &mut task).await {
            Ok(result) => result.map_err(io::Error::other)?,
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "server owner shutdown deadline elapsed",
                ))
            }
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn router(state: Arc<AppState>) -> Router {
    let metrics_path = state.config.metrics.endpoint.clone();
    let metrics_enabled = state.config.metrics.prometheus.enabled;
    let mut router = Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/version", get(version))
        .route("/v4/info", get(info))
        .route("/v4/stats", get(stats))
        .route("/v4/routeplanner/status", get(route_planner_status))
        .route(
            "/v4/routeplanner/free/address",
            post(route_planner_free_address),
        )
        .route("/v4/routeplanner/free/all", post(route_planner_free_all))
        .route("/crust/v1/info", get(crust_info))
        .route("/v4/loadtracks", get(load_tracks))
        .route("/v4/decodetrack", get(decode_track))
        .route("/v4/decodetracks", post(decode_tracks))
        .route("/v4/websocket", get(websocket))
        .route("/v4/sessions/{session_id}", patch(patch_session))
        .route("/v4/sessions/{session_id}/players", get(list_players))
        .route(
            "/v4/sessions/{session_id}/players/{guild_id}",
            get(get_player).patch(patch_player).delete(delete_player),
        );
    if metrics_enabled {
        router = router.route(&metrics_path, get(prometheus_metrics));
    }
    router
        .fallback(fallback)
        .layer(axum::extract::DefaultBodyLimit::max(
            state.limits.max_request_body_bytes.get(),
        ))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request| {
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    path = %request.uri().path(),
                )
            }),
        )
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            observe_request,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            authorize,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            response_headers,
        ))
        .with_state(state)
}

async fn liveness() -> Response {
    (StatusCode::OK, "ok").into_response()
}

async fn readiness(State(state): State<Arc<AppState>>) -> Response {
    if state.accepting.load(Ordering::Acquire) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response()
    }
}

async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> Response {
    let failures = state
        .route_planner
        .snapshot()
        .map_or(0, |snapshot| snapshot.failing_addresses.len());
    let body = state.metrics.render_prometheus(
        &state.sessions,
        &state.stats,
        state.route_planner.is_enabled(),
        failures,
    );
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn observe_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let client = request
        .headers()
        .get("user-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response = next.run(request).await;
    let status = response.status();
    state
        .metrics
        .observe_rest(started.elapsed(), status.as_u16());
    if state.config.logging.request.enabled {
        tracing::info!(
            %method,
            %path,
            status = status.as_u16(),
            latency_ms = started.elapsed().as_secs_f64() * 1_000.0,
            user_id = ?client,
            "request completed"
        );
    }
    response
}

async fn response_headers(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let request_id = state.next_request_id.fetch_add(1, Ordering::Relaxed);
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert("lavalink-api-version", HeaderValue::from_static("4"));
    response
        .headers_mut()
        .insert("x-crust-version", HeaderValue::from_static(CRUST_VERSION));
    if let Ok(value) = HeaderValue::from_str(env!("CRUST_BUILD_COMMIT")) {
        response.headers_mut().insert("x-crust-commit", value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("{request_id:016x}")) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn authorize(State(state): State<Arc<AppState>>, request: Request, next: Next) -> Response {
    let path = request.uri().path();
    // Health probes and an explicitly enabled Prometheus scrape endpoint are
    // intentionally unauthenticated so orchestrators can observe a node
    // before Lavalink clients are configured. All protocol routes retain the
    // exact Authorization behavior below.
    if path == "/health/live"
        || path == "/health/ready"
        || (state.config.metrics.prometheus.enabled && path == state.config.metrics.endpoint)
    {
        return next.run(request).await;
    }
    let supplied = request.headers().get(AUTHORIZATION);
    let websocket = path == "/v4/websocket";
    let Some(supplied) = supplied else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let valid = supplied
        .to_str()
        .ok()
        .is_some_and(|supplied| state.password_verifier.verify(supplied));
    if !valid {
        return if websocket {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::FORBIDDEN
        }
        .into_response();
    }
    next.run(request).await
}

struct PasswordVerifier([u8; 32]);

impl PasswordVerifier {
    fn new(password: &str) -> Self {
        Self(Sha256::digest(password.as_bytes()).into())
    }

    fn verify(&self, supplied: &str) -> bool {
        let supplied: [u8; 32] = Sha256::digest(supplied.as_bytes()).into();
        bool::from(supplied.ct_eq(&self.0))
    }
}

async fn version() -> Response {
    let mut response = Response::new(Body::from(LAVALINK_VERSION));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain;charset=ISO-8859-1"),
    );
    response
}

async fn info(State(state): State<Arc<AppState>>) -> Json<Value> {
    let source_managers = enabled_source_managers(&state.config);
    let filters = enabled_filters(&state.config);
    Json(json!({
        "version": {
            "semver": LAVALINK_VERSION,
            "major": 4,
            "minor": 2,
            "patch": 2,
            "preRelease": ""
        },
        "buildTime": 0,
        "git": {
            "branch": env!("CRUST_BUILD_BRANCH"),
            "commit": env!("CRUST_BUILD_COMMIT"),
            "commitTime": build_commit_time_ms(),
        },
        "jvm": format!("Rust {}", env!("CARGO_PKG_RUST_VERSION")),
        "lavaplayer": format!("Mantle {}", &MANTLE_REVISION[..12]),
        "sourceManagers": source_managers,
        "filters": filters,
        "plugins": []
    }))
}

async fn crust_info(State(state): State<Arc<AppState>>) -> Json<Value> {
    let source_managers = enabled_source_managers(&state.config);
    let filters = enabled_filters(&state.config);
    Json(json!({
        "name": "Crust",
        "version": CRUST_VERSION,
        "build": {
            "profile": env!("CRUST_BUILD_PROFILE"),
            "git": {
                "branch": env!("CRUST_BUILD_BRANCH"),
                "commit": env!("CRUST_BUILD_COMMIT"),
                "commitTime": build_commit_time_ms(),
                "dirty": env!("CRUST_BUILD_DIRTY").parse::<bool>().ok(),
            }
        },
        "protocol": {
            "name": "Lavalink",
            "version": LAVALINK_VERSION,
            "apiVersion": 4,
        },
        "runtime": {
            "name": "Rust",
            "minimumVersion": env!("CARGO_PKG_RUST_VERSION"),
        },
        "media": {
            "name": "Mantle",
            "version": "0.0.0",
            "revision": MANTLE_REVISION,
        },
        "voice": {
            "name": "Oto",
            "version": OTO_VERSION,
            "voiceGatewayVersion": 8,
            "daveProtocolVersions": [1],
        },
        "statistics": {
            "runtimeProvider": runtime_stats_provider(),
            "runtimeRefreshIntervalMs": 30_000,
            "websocketIntervalMs": u64::try_from(state.config.stats_interval.as_millis())
                .unwrap_or(u64::MAX),
        },
        "enabled": {
            "sourceManagers": source_managers,
            "filters": filters,
        },
        "extensions": extension_values(&state.extensions),
        "limits": {
            "requestBodyBytes": state.limits.max_request_body_bytes.get(),
            "websocketMessageBytes": state.limits.max_websocket_message_bytes.get(),
            "websocketSendTimeoutMs": u64::try_from(
                state.limits.websocket_send_timeout().as_millis()
            ).unwrap_or(u64::MAX),
            "sessions": state.limits.max_sessions.get(),
            "players": state.limits.max_players.get(),
            "playersPerSession": state.limits.max_players_per_session.get(),
            "playerCommandCapacity": state.limits.player_command_capacity.get(),
            "websocketCriticalCapacity": state.limits.websocket_critical_capacity.get(),
            "concurrentLoads": state.limits.max_concurrent_loads.get(),
            "batchDecodeTracks": state.limits.max_batch_decode_tracks.get(),
            "sourceRequests": state.limits.max_concurrent_source_requests.get(),
            "outboundConnections": state.limits.max_outbound_connections.get(),
            "retainedJsonBytes": state.limits.max_retained_json_bytes.get(),
            "jsonDepth": state.limits.max_json_depth.get(),
            "jsonElements": state.limits.max_json_elements.get(),
            "routePlannerFailures": state.limits.max_route_planner_failures.get(),
        }
    }))
}

/// Register only first-party adapters attached to this server. These entries
/// are Crust diagnostics, not Lavalink Java plugin registrations, so the
/// standard `/v4/info.plugins` field remains truthful and client-compatible.
fn registered_extensions(has_mantle: bool, has_oto: bool) -> ExtensionRegistry {
    let mut registry = ExtensionRegistry::new();
    if has_mantle {
        let descriptor = ExtensionDescriptor::new(
            "crust.mantle",
            MANTLE_REVISION,
            ExtensionKind::Media,
            ["source:youtube", "media:opus", "media:pcm-filters"],
        )
        .expect("built-in Mantle extension descriptor is valid");
        registry
            .register(StaticExtension::new(descriptor))
            .expect("built-in Mantle extension id is unique");
    }
    if has_oto {
        let descriptor = ExtensionDescriptor::new(
            "crust.oto",
            OTO_VERSION,
            ExtensionKind::Voice,
            ["voice:discord", "voice:dave", "voice:opus-pacing"],
        )
        .expect("built-in Oto extension descriptor is valid");
        registry
            .register(StaticExtension::new(descriptor))
            .expect("built-in Oto extension id is unique");
    }
    registry
}

fn extension_values(registry: &ExtensionRegistry) -> Vec<Value> {
    registry
        .descriptors()
        .into_iter()
        .map(|descriptor| {
            json!({
                "id": descriptor.id,
                "version": descriptor.version,
                "kind": descriptor.kind.as_str(),
                "capabilities": descriptor.capabilities,
            })
        })
        .collect()
}

/// Return only source managers that Crust actually implements. Mantle owns
/// the Youtube source; the other Lavalink names remain accepted configuration
/// keys for migration but are not advertised until a corresponding adapter is
/// admitted.
fn enabled_source_managers(config: &ServerConfig) -> Vec<&'static str> {
    config
        .sources
        .youtube
        .then_some("youtube")
        .into_iter()
        .collect()
}

fn enabled_filters(config: &ServerConfig) -> Vec<&'static str> {
    [
        ("volume", config.filters.volume),
        ("equalizer", config.filters.equalizer),
        ("karaoke", config.filters.karaoke),
        ("timescale", config.filters.timescale),
        ("tremolo", config.filters.tremolo),
        ("vibrato", config.filters.vibrato),
        ("distortion", config.filters.distortion),
        ("rotation", config.filters.rotation),
        ("channelMix", config.filters.channel_mix),
        ("lowPass", config.filters.low_pass),
    ]
    .into_iter()
    .filter_map(|(name, enabled)| enabled.then_some(name))
    .collect()
}

const fn runtime_stats_provider() -> &'static str {
    if cfg!(target_os = "linux") {
        "linux-procfs-cgroup"
    } else {
        "unavailable-zero-fallback"
    }
}

fn build_commit_time_ms() -> u64 {
    env!("CRUST_BUILD_COMMIT_TIME")
        .parse::<u64>()
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .unwrap_or(0)
}

async fn stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(stats_value(state.stats.rest(&state.sessions), false))
}

async fn route_planner_status(State(state): State<Arc<AppState>>) -> Response {
    let Some(snapshot) = state.route_planner.snapshot() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    // Lavalink 4.2.2 dereferences RotatingIpRoutePlanner.currentAddress before
    // its first selection. Preserve that observed 500 boundary while keeping
    // the panic/null bug outside Crust itself.
    if matches!(
        snapshot.details,
        RoutePlannerDetails::Rotating {
            current_address: None,
            ..
        }
    ) {
        return reference_internal_error("/v4/routeplanner/status");
    }
    Json(route_planner_value(snapshot)).into_response()
}

#[derive(Debug, Deserialize)]
struct RoutePlannerFreeAddress {
    address: String,
}

async fn route_planner_free_address(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    request: Request,
) -> Response {
    if !request_has_json_content_type(&request) {
        return protocol_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
            uri.path(),
            false,
        );
    }
    let body_limit = MAX_ROUTE_PLANNER_BODY_BYTES.min(state.limits.max_request_body_bytes.get());
    let bytes = match axum::body::to_bytes(request.into_body(), body_limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return protocol_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "RoutePlanner request body exceeds its limit",
                uri.path(),
                false,
            );
        }
    };
    let body = match parse_bounded_json::<RoutePlannerFreeAddress>(
        &bytes,
        JsonPolicy::from(&state.limits),
    ) {
        Ok(body) => body,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid RoutePlanner address body",
                uri.path(),
                false,
            );
        }
    };
    if !state.route_planner.is_enabled() {
        return protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Can't access disabled route planner",
            uri.path(),
            false,
        );
    }
    let address = match body.address.parse() {
        Ok(address) => address,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid RoutePlanner IP address",
                uri.path(),
                false,
            );
        }
    };
    state.route_planner.free_address(address);
    StatusCode::NO_CONTENT.into_response()
}

async fn route_planner_free_all(State(state): State<Arc<AppState>>, uri: Uri) -> Response {
    if !state.route_planner.is_enabled() {
        return protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Can't access disabled route planner",
            uri.path(),
            false,
        );
    }
    state.route_planner.free_all_addresses();
    StatusCode::NO_CONTENT.into_response()
}

fn route_planner_value(snapshot: RoutePlannerSnapshot) -> Value {
    let RoutePlannerSnapshot {
        strategy,
        ip_block_type,
        ip_block_size,
        failing_addresses,
        details: strategy_details,
    } = snapshot;
    let mut details = serde_json::Map::new();
    details.insert(
        "ipBlock".to_owned(),
        json!({
            "type": ip_block_type.wire_name(),
            "size": ip_block_size,
        }),
    );
    details.insert(
        "failingAddresses".to_owned(),
        Value::Array(
            failing_addresses
                .into_iter()
                .map(|failure| {
                    json!({
                        "failingAddress": format!("/{}", failure.address),
                        "failingTimestamp": failure.failing_timestamp,
                        "failingTime": failing_time(failure.failing_timestamp),
                    })
                })
                .collect(),
        ),
    );
    match strategy_details {
        RoutePlannerDetails::Rotating {
            rotate_index,
            ip_index,
            current_address,
        } => {
            details.insert("rotateIndex".to_owned(), Value::String(rotate_index));
            details.insert("ipIndex".to_owned(), Value::String(ip_index));
            details.insert(
                "currentAddress".to_owned(),
                Value::String(
                    current_address
                        .map_or_else(|| "null".to_owned(), |address| format!("/{address}")),
                ),
            );
        }
        RoutePlannerDetails::Nano {
            current_address_index,
        } => {
            details.insert(
                "currentAddressIndex".to_owned(),
                Value::String(current_address_index),
            );
        }
        RoutePlannerDetails::RotatingNano {
            block_index,
            current_address_index,
        } => {
            details.insert("blockIndex".to_owned(), Value::String(block_index));
            details.insert(
                "currentAddressIndex".to_owned(),
                Value::String(current_address_index),
            );
        }
        RoutePlannerDetails::Balancing => {}
    }
    json!({
        "class": strategy.class_name(),
        "details": Value::Object(details),
    })
}

fn failing_time(timestamp_ms: u64) -> String {
    let timestamp_nanos = i128::from(timestamp_ms).saturating_mul(1_000_000);
    OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn reference_internal_error(path: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "timestamp": timestamp_ms(),
            "status": 500,
            "error": "Internal Server Error",
            "path": path,
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct LoadTracksQuery {
    identifier: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodeTrackQuery {
    encoded_track: Option<String>,
    track: Option<String>,
}

struct RequestCancellation(CancellationToken);

impl RequestCancellation {
    fn new() -> Self {
        Self(CancellationToken::new())
    }

    fn token(&self) -> CancellationToken {
        self.0.clone()
    }
}

impl Drop for RequestCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn load_tracks(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    query: Result<Query<LoadTracksQuery>, QueryRejection>,
) -> Response {
    let trace = trace_requested(&uri);
    let query = match query {
        Ok(Query(query)) => query,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid query parameters",
                uri.path(),
                trace,
            );
        }
    };
    let Some(identifier) = query.identifier else {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "Required parameter 'identifier' is not present.",
            uri.path(),
            trace,
        );
    };
    // Mantle is Crust's only admitted source manager. Preserve Lavalink's
    // disabled-manager behavior (a load that has no matching manager yields an
    // empty result) without trying to classify identifiers or duplicating
    // source loading in Crust.
    if !state.config.sources.youtube {
        return Json(json!({"loadType": "empty", "data": null})).into_response();
    }
    let (adapter, _load_permit, _source_permit, _outbound_permit) =
        match acquire_load_request(&state) {
            Ok(admission) => admission,
            Err(error) => {
                state.metrics.load_shed();
                return load_admission_error(error, uri.path());
            }
        };
    let cancellation = RequestCancellation::new();
    let started = Instant::now();
    state.metrics.load_started();
    let result = adapter
        .load(
            LoadRequest {
                identifier,
                route: SourceRoute::default(),
            },
            cancellation.token(),
        )
        .await;
    state
        .metrics
        .load_finished(started.elapsed(), result.is_err());
    match result {
        Ok(outcome) => {
            if validate_load_outcome(&outcome, JsonPolicy::from(&state.limits)).is_err() {
                state.metrics.mantle_error();
                return adapter_json_limit_error(uri.path());
            }
            Json(load_result_value(outcome)).into_response()
        }
        Err(error) if error.kind == AdapterErrorKind::LoadFailed => {
            state.metrics.mantle_error();
            Json(load_failed_value(error)).into_response()
        }
        Err(error) => {
            state.metrics.mantle_error();
            media_adapter_error(error, uri.path())
        }
    }
}

async fn decode_track(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    query: Result<Query<DecodeTrackQuery>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid query parameters",
                uri.path(),
                trace_requested(&uri),
            );
        }
    };
    let Some(encoded) = query.encoded_track.or(query.track) else {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "No track to decode provided",
            uri.path(),
            trace_requested(&uri),
        );
    };
    let Some(adapter) = adapter(&state) else {
        return adapter_unavailable(uri.path());
    };
    let cancellation = RequestCancellation::new();
    match adapter
        .decode(EncodedTrack::new(encoded), cancellation.token())
        .await
    {
        Ok(track) => {
            if validate_media_track(&track, JsonPolicy::from(&state.limits)).is_err() {
                state.metrics.mantle_error();
                return adapter_json_limit_error(uri.path());
            }
            Json(track::loaded_track_value(&track)).into_response()
        }
        Err(error) => {
            state.metrics.mantle_error();
            media_adapter_error(error, uri.path())
        }
    }
}

async fn decode_tracks(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let trace = trace_requested(request.uri());
    if !request_has_json_content_type(&request) {
        return protocol_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
            &path,
            trace,
        );
    }
    let bytes = match axum::body::to_bytes(
        request.into_body(),
        state.limits.max_request_body_bytes.get(),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            return protocol_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body exceeds the configured limit",
                &path,
                trace,
            );
        }
    };
    let encoded_tracks: Vec<String> =
        match parse_bounded_json(&bytes, JsonPolicy::from(&state.limits)) {
            Ok(tracks) => tracks,
            Err(_) => {
                return protocol_error(
                    StatusCode::BAD_REQUEST,
                    "Invalid encoded tracks",
                    &path,
                    trace,
                );
            }
        };
    if encoded_tracks.is_empty() {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "No tracks to decode provided",
            &path,
            trace,
        );
    }
    if encoded_tracks.len() > state.limits.max_batch_decode_tracks.get() {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "Too many tracks to decode",
            &path,
            trace,
        );
    }
    let Some(adapter) = adapter(&state) else {
        return adapter_unavailable(&path);
    };
    let cancellation = RequestCancellation::new();
    let mut tracks = Vec::with_capacity(encoded_tracks.len());
    for encoded in encoded_tracks {
        match adapter
            .decode(EncodedTrack::new(encoded), cancellation.token())
            .await
        {
            Ok(track) => {
                if validate_media_track(&track, JsonPolicy::from(&state.limits)).is_err() {
                    state.metrics.mantle_error();
                    return adapter_json_limit_error(&path);
                }
                tracks.push(track::loaded_track_value(&track));
            }
            Err(error) => {
                state.metrics.mantle_error();
                return media_adapter_error(error, &path);
            }
        }
    }
    Json(Value::Array(tracks)).into_response()
}

fn acquire_load_request(
    state: &Arc<AppState>,
) -> Result<
    (
        Arc<dyn MantleAdapter>,
        OwnedSemaphorePermit,
        OwnedSemaphorePermit,
        OwnedSemaphorePermit,
    ),
    LoadAdmissionError,
> {
    let adapter = state
        .adapter
        .clone()
        .ok_or(LoadAdmissionError::AdapterUnavailable)?;
    let load_permit = Arc::clone(&state.load_requests)
        .try_acquire_owned()
        .map_err(|_| LoadAdmissionError::ConcurrentLoads)?;
    let source_permit = Arc::clone(&state.source_requests)
        .try_acquire_owned()
        .map_err(|_| LoadAdmissionError::SourceRequests)?;
    let outbound_permit = Arc::clone(&state.outbound_connections)
        .try_acquire_owned()
        .map_err(|_| LoadAdmissionError::OutboundConnections)?;
    Ok((adapter, load_permit, source_permit, outbound_permit))
}

#[derive(Clone, Copy)]
enum LoadAdmissionError {
    AdapterUnavailable,
    ConcurrentLoads,
    SourceRequests,
    OutboundConnections,
}

fn load_admission_error(error: LoadAdmissionError, path: &str) -> Response {
    let message = match error {
        LoadAdmissionError::AdapterUnavailable => "Mantle adapter unavailable",
        LoadAdmissionError::ConcurrentLoads => "Concurrent load capacity reached",
        LoadAdmissionError::SourceRequests => "Source request capacity reached",
        LoadAdmissionError::OutboundConnections => "Outbound connection capacity reached",
    };
    protocol_error(StatusCode::SERVICE_UNAVAILABLE, message, path, false)
}

fn adapter(state: &AppState) -> Option<Arc<dyn MantleAdapter>> {
    state.adapter.clone()
}

fn adapter_unavailable(path: &str) -> Response {
    protocol_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "Mantle adapter unavailable",
        path,
        false,
    )
}

fn adapter_json_limit_error(path: &str) -> Response {
    protocol_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Mantle returned JSON outside the configured retention policy",
        path,
        false,
    )
}

fn load_result_value(outcome: LoadOutcome) -> Value {
    match outcome {
        LoadOutcome::Track(track) => json!({
            "loadType": "track",
            "data": track::loaded_track_value(&track),
        }),
        LoadOutcome::Search(tracks) => json!({
            "loadType": "search",
            "data": tracks.iter().map(track::loaded_track_value).collect::<Vec<_>>(),
        }),
        LoadOutcome::Playlist {
            info,
            plugin_info,
            tracks,
        } => json!({
            "loadType": "playlist",
            "data": {
                "info": {
                    "name": info.name,
                    "selectedTrack": info.selected_track.map_or(-1_i64, |index| {
                        i64::try_from(index).unwrap_or(i64::MAX)
                    }),
                },
                "pluginInfo": plugin_info,
                "tracks": tracks.iter().map(track::loaded_track_value).collect::<Vec<_>>(),
            },
        }),
        LoadOutcome::NoMatches => json!({"loadType": "empty", "data": null}),
    }
}

fn load_failed_value(error: AdapterError) -> Value {
    json!({
        "loadType": "error",
        "data": {
            "message": error.message,
            "severity": "suspicious",
            "cause": error.message,
            "causeStackTrace": error.message,
        },
    })
}

fn media_adapter_error(error: AdapterError, path: &str) -> Response {
    let status = match error.kind {
        AdapterErrorKind::Cancelled => StatusCode::REQUEST_TIMEOUT,
        AdapterErrorKind::Shutdown | AdapterErrorKind::Overloaded => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        AdapterErrorKind::LoadFailed
        | AdapterErrorKind::InvalidTrack
        | AdapterErrorKind::InvalidOperation => StatusCode::INTERNAL_SERVER_ERROR,
    };
    protocol_error(status, error.message, path, false)
}

fn trace_requested(uri: &Uri) -> bool {
    uri.query()
        .is_some_and(|query| query.split('&').any(|field| field == "trace=true"))
}

fn stats_value(snapshot: StatsSnapshot, websocket: bool) -> Value {
    let mut value = serde_json::to_value(snapshot).expect("stats serialization");
    if websocket {
        value
            .as_object_mut()
            .expect("static object")
            .insert("op".to_owned(), Value::String("stats".to_owned()));
    }
    value
}

async fn websocket(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    uri: Uri,
) -> Response {
    let Some(user_id) = headers.get("user-id").and_then(|value| value.to_str().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if user_id.is_empty() || user_id.parse::<i128>().is_ok_and(|value| value == 0) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(upgrade) = upgrade else {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "WebSocket upgrade required",
            uri.path(),
            false,
        );
    };
    let client_name = headers
        .get("client-name")
        .and_then(|value| value.to_str().ok());
    let requested_session_id = headers
        .get("session-id")
        .and_then(|value| value.to_str().ok());
    let prepared = match state
        .sessions
        .prepare(user_id, client_name, requested_session_id)
    {
        Ok(prepared) => prepared,
        Err(PrepareError::Full | PrepareError::ResumeOverloaded) => {
            return protocol_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Session capacity reached",
                uri.path(),
                false,
            );
        }
        Err(PrepareError::IdentityGeneration) => {
            return protocol_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Session identity generation failed",
                uri.path(),
                false,
            );
        }
        Err(PrepareError::ShuttingDown) => {
            return protocol_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Server is shutting down",
                uri.path(),
                false,
            );
        }
    };
    let resumed = prepared.resumed();
    let player_update_interval = state.config.player_update_interval;
    let stats_interval = state.config.stats_interval;
    let websocket_message_bytes = state.limits.max_websocket_message_bytes.get();
    let websocket_state = Arc::clone(&state);
    tracing::info!(
        user_id,
        client_name = ?client_name,
        resumed,
        "WebSocket session admitted"
    );
    let mut response = upgrade
        .max_message_size(websocket_message_bytes)
        .max_frame_size(websocket_message_bytes)
        .on_upgrade(move |socket| {
            websocket_session(
                socket,
                prepared,
                websocket_state,
                player_update_interval,
                stats_interval,
            )
        })
        .into_response();
    response.headers_mut().insert(
        "session-resumed",
        HeaderValue::from_static(if resumed { "true" } else { "false" }),
    );
    response
}

async fn websocket_session(
    socket: WebSocket,
    prepared: PreparedSession,
    state: Arc<AppState>,
    player_update_interval: Duration,
    stats_interval: Duration,
) {
    let Ok(mut connection) = prepared.attach() else {
        return;
    };
    let session_id = connection.id().to_owned();
    let resumed = connection.resumed();
    let handle = connection.handle();
    let cancellation = connection.cancellation();
    let initial_critical = connection.take_initial_critical();
    let initial_state = connection.take_initial_state();
    let mut outgoing = connection.take_critical_receiver();
    let (mut sink, mut stream) = socket.split();
    let ready = serde_json::to_string(&Ready {
        op: "ready",
        resumed,
        session_id: &session_id,
    })
    .expect("ready serialization");
    if !send_websocket(
        &mut sink,
        Message::Text(ready.into()),
        &state,
        &cancellation,
    )
    .await
    {
        return;
    }
    for payload in initial_critical.into_iter().chain(initial_state) {
        if !send_websocket(
            &mut sink,
            Message::Text(payload.to_string().into()),
            &state,
            &cancellation,
        )
        .await
        {
            return;
        }
    }
    if !resumed {
        let initial_stats = stats_value(state.stats.websocket(&state.sessions, &handle), true);
        if !send_websocket(
            &mut sink,
            Message::Text(initial_stats.to_string().into()),
            &state,
            &cancellation,
        )
        .await
        {
            return;
        }
    }
    let mut update_tick = tokio::time::interval(player_update_interval);
    update_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval`'s first tick is immediate; initial/resumed snapshots above
    // already cover that state, so the first periodic update waits one full
    // configured interval.
    update_tick.tick().await;
    let mut stats_tick = tokio::time::interval(stats_interval);
    stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The initial stats message above corresponds to Lavalink's immediate
    // scheduled run. Periodic sampling starts after one full interval.
    stats_tick.tick().await;
    let mut refreshes = FuturesUnordered::new();
    let mut refreshing_guilds = HashSet::new();
    loop {
        tokio::select! {
            outgoing_message = outgoing.recv() => {
                let Some(message) = outgoing_message else { break };
                if !send_websocket(
                    &mut sink,
                    Message::Text(message.payload.to_string().into()),
                    &state,
                    &cancellation,
                ).await {
                    break;
                }
                if let Some(delivered) = message.delivered {
                    let _ = delivered.send(());
                }
            }
            () = handle.coalesced_notified() => {
                for payload in handle.take_coalesced() {
                    if !send_websocket(
                        &mut sink,
                        Message::Text(payload.to_string().into()),
                        &state,
                        &cancellation,
                    ).await {
                        return;
                    }
                }
            }
            _ = update_tick.tick() => {
                for player in handle.players() {
                    if !player.wants_periodic_updates() {
                        continue;
                    }
                    let guild_id = player.guild_id().to_owned();
                    if refreshing_guilds.insert(guild_id) {
                        refreshes.push(player.refresh());
                    }
                }
            }
            _ = stats_tick.tick() => {
                let payload = Arc::from(stats_value(
                    state.stats.websocket(&state.sessions, &handle),
                    true,
                ).to_string());
                if handle.publish_stats(payload).is_err() {
                    break;
                }
            }
            Some((guild_id, payload)) = refreshes.next(), if !refreshes.is_empty() => {
                refreshing_guilds.remove(&guild_id);
                let _ = handle.publish_player_update(&guild_id, payload);
            }
            () = cancellation.cancelled() => {
                close_websocket(&mut sink, &state).await;
                break;
            }
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(_))) => {}
                Some(Ok(Message::Ping(payload))) => {
                    if !send_websocket(
                        &mut sink,
                        Message::Pong(payload),
                        &state,
                        &cancellation,
                    ).await {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Binary(_))) => break,
            }
        }
    }
}

async fn send_websocket<S>(
    sink: &mut S,
    message: Message,
    state: &AppState,
    cancellation: &CancellationToken,
) -> bool
where
    S: Sink<Message> + Unpin,
{
    match bounded_websocket_send(
        sink,
        message,
        state.limits.websocket_send_timeout(),
        cancellation,
    )
    .await
    {
        WebSocketSendOutcome::Delivered => true,
        WebSocketSendOutcome::TimedOut => {
            state.metrics.websocket_slow_consumer();
            tracing::warn!("slow WebSocket consumer disconnected");
            cancellation.cancel();
            false
        }
        WebSocketSendOutcome::Cancelled | WebSocketSendOutcome::Closed => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WebSocketSendOutcome {
    Delivered,
    Cancelled,
    Closed,
    TimedOut,
}

async fn bounded_websocket_send<S>(
    sink: &mut S,
    message: Message,
    deadline: Duration,
    cancellation: &CancellationToken,
) -> WebSocketSendOutcome
where
    S: Sink<Message> + Unpin,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => WebSocketSendOutcome::Cancelled,
        result = timeout(deadline, sink.send(message)) => match result {
            Ok(Ok(())) => WebSocketSendOutcome::Delivered,
            Ok(Err(_)) => WebSocketSendOutcome::Closed,
            Err(_) => WebSocketSendOutcome::TimedOut,
        },
    }
}

async fn close_websocket<S>(sink: &mut S, state: &AppState)
where
    S: Sink<Message> + Unpin,
{
    if timeout(
        state.limits.websocket_send_timeout(),
        sink.send(Message::Close(None)),
    )
    .await
    .is_err()
    {
        state.metrics.websocket_slow_consumer();
    }
}

async fn patch_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    request: Request,
) -> Response {
    let path = request.uri().path().to_owned();
    if !request_has_json_content_type(&request) {
        return protocol_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
            &path,
            false,
        );
    }
    let bytes = match axum::body::to_bytes(
        request.into_body(),
        state.limits.max_request_body_bytes.get(),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            return protocol_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body exceeds the configured limit",
                &path,
                false,
            );
        }
    };
    let update: SessionUpdate = match parse_bounded_json(&bytes, JsonPolicy::from(&state.limits)) {
        Ok(update) => update,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid session update",
                &path,
                false,
            );
        }
    };
    let resuming = match update.resuming {
        PatchField::Omitted => None,
        PatchField::Value(value) => Some(value),
        PatchField::Null => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "resuming must not be null",
                &path,
                false,
            );
        }
    };
    let timeout_seconds = match update.timeout {
        PatchField::Omitted => None,
        PatchField::Value(value) => Some(value),
        PatchField::Null => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "timeout must not be null",
                &path,
                false,
            );
        }
    };
    match state.sessions.update_settings(
        &session_id,
        SessionSettingsUpdate {
            resuming,
            timeout_seconds,
        },
    ) {
        Ok(settings) => Json(json!({
            "resuming": settings.resuming,
            "timeout": settings.timeout_seconds,
        }))
        .into_response(),
        Err(_) => protocol_error(StatusCode::NOT_FOUND, "Session not found", &path, false),
    }
}

async fn list_players(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    uri: Uri,
) -> Response {
    state.sessions.cleanup_expired();
    let Some(session) = state.sessions.session(&session_id) else {
        return protocol_error(
            StatusCode::NOT_FOUND,
            "Session not found",
            uri.path(),
            false,
        );
    };
    let mut owned_players = session.players();
    owned_players.sort_by(|left, right| {
        let left = concrete_player(left).map(|player| player.guild_id().to_owned());
        let right = concrete_player(right).map(|player| player.guild_id().to_owned());
        left.cmp(&right)
    });
    let mut players = Vec::new();
    for player in owned_players {
        let Some(player) = concrete_player(&player) else {
            return protocol_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid player owner",
                uri.path(),
                false,
            );
        };
        match player.snapshot().await {
            Ok(value) => players.push(value),
            Err(error) => return player_error(error, uri.path()),
        }
    }
    Json(Value::Array(players)).into_response()
}

async fn get_player(
    State(state): State<Arc<AppState>>,
    Path((session_id, guild_id)): Path<(String, String)>,
    uri: Uri,
) -> Response {
    let guild_id = match canonical_guild_id(&guild_id) {
        Ok(guild_id) => guild_id,
        Err(message) => return protocol_error(StatusCode::BAD_REQUEST, message, uri.path(), false),
    };
    state.sessions.cleanup_expired();
    let Some(session) = state.sessions.session(&session_id) else {
        return protocol_error(
            StatusCode::NOT_FOUND,
            "Session not found",
            uri.path(),
            false,
        );
    };
    let Some(player) = session.player(&guild_id) else {
        return protocol_error(StatusCode::NOT_FOUND, "Player not found", uri.path(), false);
    };
    let Some(player) = concrete_player(&player) else {
        return protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Invalid player owner",
            uri.path(),
            false,
        );
    };
    match player.snapshot().await {
        Ok(value) => Json(value).into_response(),
        Err(error) => player_error(error, uri.path()),
    }
}

async fn patch_player(
    State(state): State<Arc<AppState>>,
    Path((session_id, guild_id)): Path<(String, String)>,
    request: Request,
) -> Response {
    let path = request.uri().path().to_owned();
    let guild_id = match canonical_guild_id(&guild_id) {
        Ok(guild_id) => guild_id,
        Err(message) => return protocol_error(StatusCode::BAD_REQUEST, message, &path, false),
    };
    let no_replace = match parse_no_replace(request.uri()) {
        Ok(value) => value,
        Err(message) => return protocol_error(StatusCode::BAD_REQUEST, message, &path, false),
    };
    if !request_has_json_content_type(&request) {
        return protocol_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
            &path,
            false,
        );
    }
    state.sessions.cleanup_expired();
    let Some(session) = state.sessions.session(&session_id) else {
        return protocol_error(StatusCode::NOT_FOUND, "Session not found", &path, false);
    };
    let bytes = match axum::body::to_bytes(
        request.into_body(),
        state.limits.max_request_body_bytes.get(),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            return protocol_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body exceeds the configured limit",
                &path,
                false,
            );
        }
    };
    let update: PlayerUpdate = match parse_bounded_json(&bytes, JsonPolicy::from(&state.limits)) {
        Ok(update) => update,
        Err(_) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "Invalid player update",
                &path,
                false,
            );
        }
    };
    if let Err(error) = validate_update(&update) {
        return player_error(error, &path);
    }
    if validate_player_update_retention(&update, JsonPolicy::from(&state.limits)).is_err() {
        return protocol_error(
            StatusCode::BAD_REQUEST,
            "Player JSON exceeds the configured retention limit",
            &path,
            false,
        );
    }
    if let PatchField::Value(filters) = &update.filters {
        let disabled = disabled_filter_names(filters, &state.config.filters);
        if !disabled.is_empty() {
            let message = format!(
                "Following filters are disabled in the config: {}",
                disabled.join(", ")
            );
            return protocol_error(StatusCode::BAD_REQUEST, &message, &path, false);
        }
    }

    let owned = if let Some(existing) = session.player(&guild_id) {
        existing
    } else {
        let candidate = state.players.handle(&session_id, guild_id.clone());
        let candidate: Arc<dyn SessionPlayer> = Arc::new(candidate);
        match state
            .sessions
            .get_or_add_player(&session_id, guild_id.clone(), candidate)
        {
            Ok(player) => player,
            Err(PlayerAdmissionError::SessionNotFound) => {
                return protocol_error(StatusCode::NOT_FOUND, "Session not found", &path, false);
            }
            Err(PlayerAdmissionError::Full | PlayerAdmissionError::SessionFull) => {
                return protocol_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Player capacity reached",
                    &path,
                    false,
                );
            }
        }
    };
    let Some(player) = concrete_player(&owned) else {
        return protocol_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Invalid player owner",
            &path,
            false,
        );
    };
    match player.apply(session, update, no_replace).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => player_error(error, &path),
    }
}

fn validate_player_update_retention(
    update: &PlayerUpdate,
    policy: JsonPolicy,
) -> Result<(), json_limits::JsonLimitError> {
    if let PatchField::Value(track) = &update.track
        && let PatchField::Value(user_data) = &track.user_data
    {
        validate_json_object(user_data, policy)?;
    }
    if let PatchField::Value(filters) = &update.filters {
        validate_json_object(&filters.plugin_filters, policy)?;
    }
    Ok(())
}

async fn delete_player(
    State(state): State<Arc<AppState>>,
    Path((session_id, guild_id)): Path<(String, String)>,
    uri: Uri,
) -> Response {
    let guild_id = match canonical_guild_id(&guild_id) {
        Ok(guild_id) => guild_id,
        Err(message) => return protocol_error(StatusCode::BAD_REQUEST, message, uri.path(), false),
    };
    state.sessions.cleanup_expired();
    let Some(session) = state.sessions.session(&session_id) else {
        return protocol_error(
            StatusCode::NOT_FOUND,
            "Session not found",
            uri.path(),
            false,
        );
    };
    if let Some(owned) = session.player(&guild_id) {
        let Some(player) = concrete_player(&owned) else {
            return protocol_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid player owner",
                uri.path(),
                false,
            );
        };
        if let Err(error) = player.destroy(session.clone()).await {
            return player_error(error, uri.path());
        }
    }
    match state.sessions.remove_player(&session_id, &guild_id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(PlayerAdmissionError::SessionNotFound) => protocol_error(
            StatusCode::NOT_FOUND,
            "Session not found",
            uri.path(),
            false,
        ),
        Err(PlayerAdmissionError::Full | PlayerAdmissionError::SessionFull) => protocol_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Player capacity reached",
            uri.path(),
            false,
        ),
    }
}

fn concrete_player(player: &Arc<dyn SessionPlayer>) -> Option<PlayerHandle> {
    player.as_any().downcast_ref::<PlayerHandle>().cloned()
}

fn canonical_guild_id(guild_id: &str) -> Result<String, &'static str> {
    guild_id
        .parse::<i64>()
        .map(|guild_id| guild_id.to_string())
        .map_err(|_| "Invalid guild ID")
}

fn parse_no_replace(uri: &Uri) -> Result<bool, &'static str> {
    let mut no_replace = false;
    let mut seen = false;
    for parameter in uri.query().unwrap_or_default().split('&') {
        if parameter.is_empty() {
            continue;
        }
        let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        if name != "noReplace" {
            continue;
        }
        if seen {
            return Err("noReplace must be supplied at most once");
        }
        seen = true;
        no_replace = match value {
            "true" => true,
            "false" => false,
            _ => return Err("noReplace must be true or false"),
        };
    }
    Ok(no_replace)
}

fn request_has_json_content_type(request: &Request) -> bool {
    request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .and_then(|media_type| media_type.trim().split_once('/'))
        .is_some_and(|(kind, subtype)| {
            kind.eq_ignore_ascii_case("application")
                && (subtype.eq_ignore_ascii_case("json")
                    || subtype.rsplit_once('+').is_some_and(|(name, suffix)| {
                        !name.is_empty() && suffix.eq_ignore_ascii_case("json")
                    }))
        })
}

fn player_error(error: PlayerError, path: &str) -> Response {
    let status = match error {
        PlayerError::Invalid(_) => StatusCode::BAD_REQUEST,
        PlayerError::NotFound => StatusCode::NOT_FOUND,
        PlayerError::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
        PlayerError::Media(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    protocol_error(status, &error.to_string(), path, false)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Ready<'a> {
    op: &'static str,
    resumed: bool,
    session_id: &'a str,
}

async fn fallback(uri: Uri) -> Response {
    let trace = trace_requested(&uri);
    protocol_error(
        StatusCode::NOT_FOUND,
        "No route for this request",
        uri.path(),
        trace,
    )
}

fn protocol_error(status: StatusCode, message: &str, path: &str, trace: bool) -> Response {
    let mut body = serde_json::Map::new();
    body.insert("timestamp".to_owned(), json!(timestamp_ms()));
    body.insert("status".to_owned(), json!(status.as_u16()));
    body.insert(
        "error".to_owned(),
        Value::String(status.canonical_reason().unwrap_or("Error").to_owned()),
    );
    if trace {
        body.insert(
            "trace".to_owned(),
            Value::String("Crust protocol boundary rejection".to_owned()),
        );
    }
    body.insert("message".to_owned(), Value::String(message.to_owned()));
    body.insert("path".to_owned(), Value::String(path.to_owned()));
    (status, Json(Value::Object(body))).into_response()
}

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod security_tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    struct PendingSink;

    impl Sink<Message> for PendingSink {
        type Error = ();

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("pending sink never admits a message")
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn websocket_sink_writes_are_deadline_and_cancellation_bounded() {
        let cancellation = CancellationToken::new();
        assert_eq!(
            bounded_websocket_send(
                &mut PendingSink,
                Message::Text("blocked".into()),
                Duration::from_millis(1),
                &cancellation,
            )
            .await,
            WebSocketSendOutcome::TimedOut
        );

        cancellation.cancel();
        assert_eq!(
            bounded_websocket_send(
                &mut PendingSink,
                Message::Text("cancelled".into()),
                Duration::from_secs(60),
                &cancellation,
            )
            .await,
            WebSocketSendOutcome::Cancelled
        );
    }

    #[test]
    fn password_verifier_compares_fixed_length_digests() {
        let verifier = PasswordVerifier::new("correct-horse-battery-staple");
        assert!(verifier.verify("correct-horse-battery-staple"));
        assert!(!verifier.verify("short"));
        assert!(!verifier.verify("correct-horse-battery-staplef"));
    }
}
