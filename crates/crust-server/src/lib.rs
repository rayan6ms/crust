pub mod config;
pub mod player;
pub mod session;
mod track;

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
use crust::media::{
    AdapterError, AdapterErrorKind, EncodedTrack, LoadOutcome, LoadRequest, MantleAdapter,
    SourceRoute,
};
use crust_protocol::{PatchField, PlayerUpdate, SessionUpdate};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use config::ServerConfig;
use player::{PlayerError, PlayerExecutor, PlayerHandle, validate_update};
use session::{
    PlayerAdmissionError, PrepareError, PreparedSession, SessionClock, SessionPlayer,
    SessionRegistry, SessionSettingsUpdate, SystemSessionClock,
};

pub const LAVALINK_VERSION: &str = "4.2.2";
pub const MANTLE_REVISION: &str = "0c042705e64e956d7eb58634c5d4b73b365fe5d0";

struct AppState {
    config: ServerConfig,
    started: Instant,
    next_request_id: AtomicU64,
    sessions: SessionRegistry,
    players: PlayerExecutor,
    adapter: Option<Arc<dyn MantleAdapter>>,
    load_requests: Arc<Semaphore>,
    source_requests: Arc<Semaphore>,
}

impl AppState {
    fn new(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
    ) -> Self {
        let sessions = SessionRegistry::new(
            config.max_sessions,
            config.max_players,
            config.max_concurrent_session_resumes,
            config.websocket_critical_capacity,
            clock,
        );
        let players = PlayerExecutor::new(
            config.player_executor_shards,
            config.player_command_capacity,
            adapter.clone(),
        );
        let load_requests = Arc::new(Semaphore::new(config.max_concurrent_loads));
        let source_requests = Arc::new(Semaphore::new(config.max_concurrent_source_requests));
        Self {
            config,
            started: Instant::now(),
            next_request_id: AtomicU64::new(1),
            sessions,
            players,
            adapter,
            load_requests,
            source_requests,
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
        Self::bind_with_clock_and_adapter(config, Arc::new(SystemSessionClock::new()), None).await
    }

    pub async fn bind_with_adapter(
        config: ServerConfig,
        adapter: Arc<dyn MantleAdapter>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_and_adapter(
            config,
            Arc::new(SystemSessionClock::new()),
            Some(adapter),
        )
        .await
    }

    pub async fn bind_with_clock(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
    ) -> io::Result<Self> {
        Self::bind_with_clock_and_adapter(config, clock, None).await
    }

    pub async fn bind_with_clock_and_adapter(
        config: ServerConfig,
        clock: Arc<dyn SessionClock>,
        adapter: Option<Arc<dyn MantleAdapter>>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(config.socket_address()).await?;
        Ok(Self {
            listener: Arc::new(listener),
            state: Arc::new(AppState::new(config, clock, adapter)),
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
        let graceful = axum::serve(listener, application.into_make_service())
            .with_graceful_shutdown(async move {
                shutdown_for_server.cancelled().await;
                sessions.shutdown();
            });
        let graceful = graceful.into_future();
        tokio::pin!(graceful);
        let result = tokio::select! {
            result = &mut graceful => result,
            () = async {
                shutdown.cancelled().await;
                tokio::time::sleep(self.state.config.shutdown_timeout).await;
            } => Err(io::Error::new(io::ErrorKind::TimedOut, "server shutdown deadline elapsed")),
        };
        players.shutdown().await;
        result
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
    Router::new()
        .route("/version", get(version))
        .route("/v4/info", get(info))
        .route("/v4/stats", get(stats))
        .route("/v4/loadtracks", get(load_tracks))
        .route("/v4/decodetrack", get(decode_track))
        .route("/v4/decodetracks", post(decode_tracks))
        .route("/v4/websocket", get(websocket))
        .route("/v4/sessions/{session_id}", patch(patch_session))
        .route("/v4/sessions/{session_id}/players", get(list_players))
        .route(
            "/v4/sessions/{session_id}/players/{guild_id}",
            get(get_player).patch(patch_player).delete(delete_player),
        )
        .fallback(fallback)
        .layer(axum::extract::DefaultBodyLimit::max(
            state.config.max_request_body_bytes,
        ))
        .layer(TraceLayer::new_for_http())
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
    if let Ok(value) = HeaderValue::from_str(&format!("{request_id:016x}")) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn authorize(State(state): State<Arc<AppState>>, request: Request, next: Next) -> Response {
    let supplied = request.headers().get(AUTHORIZATION);
    let websocket = request.uri().path() == "/v4/websocket";
    let Some(supplied) = supplied else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let valid = supplied
        .to_str()
        .ok()
        .is_some_and(|supplied| constant_time_equal(supplied, state.config.password()));
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

fn constant_time_equal(left: &str, right: &str) -> bool {
    left.len() == right.len() && bool::from(left.as_bytes().ct_eq(right.as_bytes()))
}

async fn version() -> Response {
    let mut response = Response::new(Body::from(LAVALINK_VERSION));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain;charset=ISO-8859-1"),
    );
    response
}

async fn info() -> Json<Value> {
    Json(json!({
        "version": {
            "semver": LAVALINK_VERSION,
            "major": 4,
            "minor": 2,
            "patch": 2,
            "preRelease": ""
        },
        "buildTime": 0,
        "git": {"branch": "unknown", "commit": "unknown", "commitTime": 0},
        "jvm": format!("Rust {}", env!("CARGO_PKG_RUST_VERSION")),
        "lavaplayer": format!("Mantle {}", &MANTLE_REVISION[..12]),
        "sourceManagers": ["youtube"],
        "filters": [],
        "plugins": []
    }))
}

async fn stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(stats_value(&state, false))
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
    let (adapter, _load_permit, _source_permit) = match acquire_load_request(&state) {
        Ok(admission) => admission,
        Err(error) => return load_admission_error(error, uri.path()),
    };
    let cancellation = RequestCancellation::new();
    match adapter
        .load(
            LoadRequest {
                identifier,
                route: SourceRoute::default(),
            },
            cancellation.token(),
        )
        .await
    {
        Ok(outcome) => Json(load_result_value(outcome)).into_response(),
        Err(error) if error.kind == AdapterErrorKind::LoadFailed => {
            Json(load_failed_value(error)).into_response()
        }
        Err(error) => media_adapter_error(error, uri.path()),
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
        Ok(track) => Json(track::loaded_track_value(&track)).into_response(),
        Err(error) => media_adapter_error(error, uri.path()),
    }
}

async fn decode_tracks(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let trace = trace_requested(request.uri());
    let bytes = match axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
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
    let encoded_tracks: Vec<String> = match serde_json::from_slice(&bytes) {
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
    if encoded_tracks.len() > state.config.max_batch_decode_tracks {
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
            Ok(track) => tracks.push(track::loaded_track_value(&track)),
            Err(error) => return media_adapter_error(error, &path),
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
    Ok((adapter, load_permit, source_permit))
}

#[derive(Clone, Copy)]
enum LoadAdmissionError {
    AdapterUnavailable,
    ConcurrentLoads,
    SourceRequests,
}

fn load_admission_error(error: LoadAdmissionError, path: &str) -> Response {
    let message = match error {
        LoadAdmissionError::AdapterUnavailable => "Mantle adapter unavailable",
        LoadAdmissionError::ConcurrentLoads => "Concurrent load capacity reached",
        LoadAdmissionError::SourceRequests => "Source request capacity reached",
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

fn stats_value(state: &AppState, websocket: bool) -> Value {
    let counts = state.sessions.counts();
    let mut value = json!({
        "frameStats": null,
        "players": counts.players,
        "playingPlayers": 0,
        "uptime": u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "memory": {"free": 0, "used": 0, "allocated": 0, "reservable": 0},
        "cpu": {
            "cores": std::thread::available_parallelism().map_or(1, usize::from),
            "systemLoad": 0.0,
            "lavalinkLoad": 0.0
        }
    });
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
    let session_id = prepared.id();
    tracing::info!(
        user_id,
        client_name = ?client_name,
        requested_session_id = ?requested_session_id,
        session_id = %session_id,
        "WebSocket session admitted"
    );
    let mut response = upgrade
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| websocket_session(socket, prepared))
        .into_response();
    response.headers_mut().insert(
        "session-resumed",
        HeaderValue::from_static(if resumed { "true" } else { "false" }),
    );
    response
}

async fn websocket_session(socket: WebSocket, prepared: PreparedSession) {
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
    if sink.send(Message::Text(ready.into())).await.is_err() {
        return;
    }
    for payload in initial_critical.into_iter().chain(initial_state) {
        if sink
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }
    loop {
        tokio::select! {
            outgoing_message = outgoing.recv() => {
                let Some(message) = outgoing_message else { break };
                if sink.send(Message::Text(message.payload.to_string().into())).await.is_err() {
                    break;
                }
                if let Some(delivered) = message.delivered {
                    let _ = delivered.send(());
                }
            }
            () = handle.coalesced_notified() => {
                for payload in handle.take_coalesced() {
                    if sink.send(Message::Text(payload.to_string().into())).await.is_err() {
                        return;
                    }
                }
            }
            () = cancellation.cancelled() => {
                let _ = sink.send(Message::Close(None)).await;
                break;
            }
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(_))) => {}
                Some(Ok(Message::Ping(payload))) => {
                    if sink.send(Message::Pong(payload)).await.is_err() {
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

async fn patch_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    request: Request,
) -> Response {
    let path = request.uri().path().to_owned();
    let bytes = match axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
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
    let update: SessionUpdate = match serde_json::from_slice(&bytes) {
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
    state.sessions.cleanup_expired();
    let Some(session) = state.sessions.session(&session_id) else {
        return protocol_error(StatusCode::NOT_FOUND, "Session not found", &path, false);
    };
    let bytes = match axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
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
    let update: PlayerUpdate = match serde_json::from_slice(&bytes) {
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
            Err(PlayerAdmissionError::Full) => {
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
        if let Err(error) = player.destroy().await {
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
        Err(PlayerAdmissionError::Full) => protocol_error(
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
