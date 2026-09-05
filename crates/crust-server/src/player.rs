//! Fixed-shard single-writer player execution selected by ADR-0001.

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crust::filters::{
    ChannelMix as RuntimeChannelMix, Distortion as RuntimeDistortion, FilterConfiguration,
    Karaoke as RuntimeKaraoke, Modulation, Timescale as RuntimeTimescale,
};
use crust::media::{
    AdapterErrorKind, EncodedTrack, LoadOutcome, LoadRequest, MantleAdapter, MantlePlayer,
    MediaEvent, MediaTrack, PlayerStatus, SourceRoute, TrackEndReason,
};
use crust::voice::{
    TimedOpusFrame, VoiceBackend, VoiceClose, VoiceConnection, VoiceConnectionInfo, VoiceError,
    VoiceErrorKind, VoiceEvent, VoiceFrameSource, VoiceFuture, VoiceSecret, VoiceSnapshot,
};
use crust_protocol::{Filters, JsonObject, PatchField, PlayerUpdate, VoiceState};
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::FilterConfig;
use crate::json_limits::{JsonPolicy, validate_media_track};
use crate::session::{
    PublishError, SessionHandle, SessionPlayer, SessionPlayerRefresh, SessionPlayerStats,
};
use crate::track::track_value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerError {
    Invalid(&'static str),
    NotFound,
    Overloaded,
    Media(&'static str),
}

impl fmt::Display for PlayerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Media(message) => formatter.write_str(message),
            Self::NotFound => formatter.write_str("Player not found"),
            Self::Overloaded => formatter.write_str("Player executor capacity reached"),
        }
    }
}

impl std::error::Error for PlayerError {}

#[derive(Clone)]
pub struct PlayerExecutor {
    inner: Arc<ExecutorInner>,
}

impl fmt::Debug for PlayerExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlayerExecutor")
            .field("shards", &self.inner.senders.len())
            .finish_non_exhaustive()
    }
}

struct ExecutorInner {
    senders: Vec<(mpsc::Sender<PlayerCommand>, mpsc::Sender<PlayerHandle>)>,
    adapter: Option<Arc<dyn MantleAdapter>>,
    voice: Option<Arc<dyn VoiceBackend>>,
    cancellation: CancellationToken,
    workers: Mutex<Option<Vec<JoinHandle<()>>>>,
}

/// Shared admission for the deprecated player-identifier load path. It uses
/// the same semaphores as `/v4/loadtracks`, so player commands cannot bypass
/// global Mantle/outbound-work limits.
#[derive(Clone)]
pub(crate) struct PlayerLoadAdmission {
    loads: Arc<Semaphore>,
    source_requests: Arc<Semaphore>,
    outbound_connections: Arc<Semaphore>,
}

impl PlayerLoadAdmission {
    pub(crate) const fn new(
        loads: Arc<Semaphore>,
        source_requests: Arc<Semaphore>,
        outbound_connections: Arc<Semaphore>,
    ) -> Self {
        Self {
            loads,
            source_requests,
            outbound_connections,
        }
    }

    fn try_acquire(&self) -> Result<PlayerLoadPermits, PlayerError> {
        let load = Arc::clone(&self.loads)
            .try_acquire_owned()
            .map_err(|_| PlayerError::Overloaded)?;
        let source = Arc::clone(&self.source_requests)
            .try_acquire_owned()
            .map_err(|_| PlayerError::Overloaded)?;
        let outbound = Arc::clone(&self.outbound_connections)
            .try_acquire_owned()
            .map_err(|_| PlayerError::Overloaded)?;
        Ok(PlayerLoadPermits {
            _load: load,
            _source: source,
            outbound: Some(outbound),
        })
    }

    fn try_acquire_outbound(&self) -> Result<OwnedSemaphorePermit, PlayerError> {
        Arc::clone(&self.outbound_connections)
            .try_acquire_owned()
            .map_err(|_| PlayerError::Overloaded)
    }
}

struct PlayerLoadPermits {
    _load: OwnedSemaphorePermit,
    _source: OwnedSemaphorePermit,
    outbound: Option<OwnedSemaphorePermit>,
}

impl PlayerLoadPermits {
    fn take_outbound(&mut self) -> OwnedSemaphorePermit {
        self.outbound
            .take()
            .expect("load admission always owns an outbound permit")
    }
}

#[derive(Clone)]
struct PlayerServices {
    adapter: Option<Arc<dyn MantleAdapter>>,
    voice: Option<Arc<dyn VoiceBackend>>,
    json_policy: JsonPolicy,
    load_admission: PlayerLoadAdmission,
}

impl Drop for ExecutorInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(workers) = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            for worker in workers {
                worker.abort();
            }
        }
    }
}

#[derive(Clone)]
pub struct PlayerHandle {
    inner: Arc<PlayerInner>,
}

impl fmt::Debug for PlayerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlayerHandle")
            .field("guild_id", &self.inner.guild_id)
            .finish_non_exhaustive()
    }
}

struct PlayerInner {
    execution_key: String,
    guild_id: String,
    sender: mpsc::Sender<PlayerCommand>,
    shutdown_sender: mpsc::Sender<PlayerHandle>,
    shutdown_requested: AtomicBool,
    deferred_audio: AtomicBool,
    periodic_updates: AtomicBool,
    stats: Mutex<SessionPlayerStats>,
    state: tokio::sync::Mutex<PlayerState>,
    cached_update: Mutex<Arc<str>>,
    cancellation: CancellationToken,
    voice_monitor: Mutex<Option<VoiceMonitor>>,
}

struct VoiceMonitor {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for VoiceMonitor {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

struct PlayerState {
    mantle: Option<Arc<dyn MantlePlayer>>,
    outbound_connection: Option<OwnedSemaphorePermit>,
    voice_connection: Option<Arc<dyn VoiceConnection>>,
    audio_generation: u64,
    track: Option<ActiveTrack>,
    volume: i32,
    paused: bool,
    position_ms: u64,
    end_time_ms: Option<u64>,
    filters: Value,
    filter_configuration: FilterConfiguration,
    voice: VoiceState,
    destroyed: bool,
}

struct ActiveTrack {
    media: MediaTrack,
    user_data: JsonObject,
}

enum PlayerCommand {
    Apply {
        handle: PlayerHandle,
        session: SessionHandle,
        update: Box<PlayerUpdate>,
        no_replace: bool,
        reply: oneshot::Sender<Result<Value, PlayerError>>,
    },
    Snapshot {
        handle: PlayerHandle,
        reply: oneshot::Sender<Result<Value, PlayerError>>,
    },
    Destroy {
        handle: PlayerHandle,
        session: SessionHandle,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    Shutdown {
        handle: PlayerHandle,
    },
    SourceTerminal {
        handle: PlayerHandle,
        session: SessionHandle,
        generation: u64,
        failure: Option<VoiceError>,
    },
    VoiceClosed {
        handle: PlayerHandle,
        session: SessionHandle,
        connection: Arc<dyn VoiceConnection>,
        close: VoiceClose,
    },
    VoiceReady {
        handle: PlayerHandle,
        session: SessionHandle,
        connection: Arc<dyn VoiceConnection>,
    },
}

impl PlayerExecutor {
    #[must_use]
    pub(crate) fn new(
        shard_count: usize,
        command_capacity: usize,
        max_players: usize,
        json_policy: JsonPolicy,
        load_admission: PlayerLoadAdmission,
        adapter: Option<Arc<dyn MantleAdapter>>,
        voice: Option<Arc<dyn VoiceBackend>>,
    ) -> Self {
        assert!(shard_count > 0);
        assert!(command_capacity > 0);
        assert!(max_players > 0);
        let cancellation = CancellationToken::new();
        let services = PlayerServices {
            adapter: adapter.clone(),
            voice: voice.clone(),
            json_policy,
            load_admission,
        };
        let mut senders = Vec::with_capacity(shard_count);
        let mut workers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (sender, receiver) = mpsc::channel(command_capacity);
            // Every admitted player can enqueue at most one shutdown request.
            // This separate lane ensures lifecycle cleanup remains admissible
            // even when ordinary player commands saturate their queue.
            let (shutdown_sender, shutdown_receiver) = mpsc::channel(max_players);
            let worker_cancellation = cancellation.clone();
            let worker_services = services.clone();
            senders.push((sender, shutdown_sender));
            workers.push(tokio::spawn(run_shard(
                receiver,
                shutdown_receiver,
                worker_cancellation,
                command_capacity,
                worker_services,
            )));
        }
        Self {
            inner: Arc::new(ExecutorInner {
                senders,
                adapter,
                voice,
                cancellation,
                workers: Mutex::new(Some(workers)),
            }),
        }
    }

    #[must_use]
    pub fn handle(&self, session_id: &str, guild_id: String) -> PlayerHandle {
        let mut hash = DefaultHasher::new();
        session_id.hash(&mut hash);
        guild_id.hash(&mut hash);
        let index = usize::try_from(hash.finish()).unwrap_or(usize::MAX) % self.inner.senders.len();
        let execution_key = format!("{session_id}\0{guild_id}");
        let (sender, shutdown_sender) = &self.inner.senders[index];
        PlayerHandle::new(
            execution_key,
            guild_id,
            sender.clone(),
            shutdown_sender.clone(),
        )
    }

    pub async fn shutdown(&self) {
        self.inner.cancellation.cancel();
        let workers = self
            .inner
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(workers) = workers {
            for worker in workers {
                let _ = worker.await;
            }
            if let Some(voice) = &self.inner.voice {
                let _ = voice.shutdown().await;
            }
            if let Some(adapter) = &self.inner.adapter {
                let _ = adapter.shutdown().await;
            }
        }
    }
}

impl PlayerHandle {
    fn new(
        execution_key: String,
        guild_id: String,
        sender: mpsc::Sender<PlayerCommand>,
        shutdown_sender: mpsc::Sender<PlayerHandle>,
    ) -> Self {
        let cached_update = Arc::from(
            json!({
                "op": "playerUpdate",
                "guildId": guild_id,
                "state": default_player_state(),
            })
            .to_string(),
        );
        Self {
            inner: Arc::new(PlayerInner {
                execution_key,
                guild_id,
                sender,
                shutdown_sender,
                shutdown_requested: AtomicBool::new(false),
                deferred_audio: AtomicBool::new(false),
                periodic_updates: AtomicBool::new(false),
                stats: Mutex::new(SessionPlayerStats::default()),
                state: tokio::sync::Mutex::new(PlayerState::default()),
                cached_update: Mutex::new(cached_update),
                cancellation: CancellationToken::new(),
                voice_monitor: Mutex::new(None),
            }),
        }
    }

    #[must_use]
    pub fn guild_id(&self) -> &str {
        &self.inner.guild_id
    }

    fn cached_update(&self) -> Arc<str> {
        Arc::clone(
            &self
                .inner
                .cached_update
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub async fn apply(
        &self,
        session: SessionHandle,
        update: PlayerUpdate,
        no_replace: bool,
    ) -> Result<Value, PlayerError> {
        let (reply, response) = oneshot::channel();
        self.send(PlayerCommand::Apply {
            handle: self.clone(),
            session,
            update: Box::new(update),
            no_replace,
            reply,
        })?;
        response.await.map_err(|_| PlayerError::NotFound)?
    }

    pub async fn snapshot(&self) -> Result<Value, PlayerError> {
        let (reply, response) = oneshot::channel();
        self.send(PlayerCommand::Snapshot {
            handle: self.clone(),
            reply,
        })?;
        response.await.map_err(|_| PlayerError::NotFound)?
    }

    pub async fn destroy(&self, session: SessionHandle) -> Result<(), PlayerError> {
        let (reply, response) = oneshot::channel();
        self.send(PlayerCommand::Destroy {
            handle: self.clone(),
            session,
            reply,
        })?;
        response.await.map_err(|_| PlayerError::NotFound)?
    }

    fn send(&self, command: PlayerCommand) -> Result<(), PlayerError> {
        if self.inner.cancellation.is_cancelled() {
            return Err(PlayerError::NotFound);
        }
        self.inner
            .sender
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PlayerError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => PlayerError::NotFound,
            })
    }

    fn start_voice_monitor(&self, session: SessionHandle, connection: Arc<dyn VoiceConnection>) {
        let mut monitor = self
            .inner
            .voice_monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if monitor
            .as_ref()
            .is_some_and(|monitor| !monitor.task.is_finished())
        {
            return;
        }
        // A backend event stream may end before fresh voice information
        // arrives. Dispose of that completed owner so the updated connection
        // is monitored again.
        drop(monitor.take());
        let cancellation = self.inner.cancellation.child_token();
        let task_cancellation = cancellation.clone();
        let weak = Arc::downgrade(&self.inner);
        let task_connection = Arc::clone(&connection);
        let task = tokio::spawn(async move {
            let mut connected = false;
            loop {
                let event = tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => return,
                    event = task_connection.next_event(task_cancellation.clone()) => event,
                };
                match event {
                    Ok(Some(VoiceEvent::Closed(close))) => {
                        // Oto reports the old transport as replaced when fresh
                        // voice information is installed. That is an internal
                        // generation transition, not Lavalink's
                        // WebSocketClosedEvent.
                        if close.code == 0 && close.reason.as_ref() == "voice information replaced"
                        {
                            continue;
                        }
                        let Some(inner) = weak.upgrade() else {
                            return;
                        };
                        let handle = PlayerHandle { inner };
                        let command = PlayerCommand::VoiceClosed {
                            handle: handle.clone(),
                            session: session.clone(),
                            connection: Arc::clone(&task_connection),
                            close,
                        };
                        tokio::select! {
                            biased;
                            () = task_cancellation.cancelled() => {}
                            _ = handle.inner.sender.send(command) => {}
                        }
                        return;
                    }
                    Ok(Some(VoiceEvent::PhaseChanged(phase))) => {
                        if phase == crust::voice::VoicePhase::Connected && !connected {
                            connected = true;
                            let Some(inner) = weak.upgrade() else {
                                return;
                            };
                            let handle = PlayerHandle { inner };
                            let command = PlayerCommand::VoiceReady {
                                handle: handle.clone(),
                                session: session.clone(),
                                connection: Arc::clone(&task_connection),
                            };
                            tokio::select! {
                                biased;
                                () = task_cancellation.cancelled() => return,
                                _ = handle.inner.sender.send(command) => {}
                            }
                        } else if phase != crust::voice::VoicePhase::Connected {
                            connected = false;
                        }
                    }
                    Ok(Some(VoiceEvent::SourceFailed(_))) => {}
                    Ok(None) | Err(_) => return,
                }
            }
        });
        *monitor = Some(VoiceMonitor { cancellation, task });
    }

    fn stop_voice_monitor(&self) {
        let monitor = self
            .inner
            .voice_monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(monitor);
    }
}

impl SessionPlayer for PlayerHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn guild_id(&self) -> &str {
        self.guild_id()
    }

    fn wants_periodic_updates(&self) -> bool {
        self.inner.periodic_updates.load(Ordering::Acquire)
    }

    fn stats(&self) -> SessionPlayerStats {
        *self
            .inner
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn snapshot(&self) -> Arc<str> {
        Arc::clone(
            &self
                .inner
                .cached_update
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn refresh(&self) -> SessionPlayerRefresh {
        let handle = self.clone();
        Box::pin(async move {
            // Refreshing updates both Mantle position and the current voice
            // ping/connection state. If the player disappears while this
            // refresh is queued, retain the last durable coalesced snapshot.
            let _ = handle.snapshot().await;
            (handle.guild_id().to_owned(), handle.cached_update())
        })
    }

    fn shutdown(&self) {
        self.inner.cancellation.cancel();
        if !self.inner.shutdown_requested.swap(true, Ordering::AcqRel) {
            // Capacity equals the global admitted-player bound and each
            // player contributes at most one request, so Full is unreachable
            // while the executor is live. Closed means the executor's own
            // global backend shutdown has already taken ownership.
            let _ = self.inner.shutdown_sender.try_send(self.clone());
        }
    }
}

impl Default for PlayerState {
    fn default() -> Self {
        Self {
            mantle: None,
            outbound_connection: None,
            voice_connection: None,
            audio_generation: 0,
            track: None,
            volume: 100,
            paused: false,
            position_ms: 0,
            end_time_ms: None,
            filters: json!({}),
            filter_configuration: FilterConfiguration::default(),
            voice: VoiceState {
                token: String::new(),
                endpoint: String::new(),
                session_id: String::new(),
                channel_id: None,
            },
            destroyed: false,
        }
    }
}

async fn run_shard(
    mut receiver: mpsc::Receiver<PlayerCommand>,
    mut shutdown_receiver: mpsc::Receiver<PlayerHandle>,
    cancellation: CancellationToken,
    max_queued: usize,
    services: PlayerServices,
) {
    let mut active = HashSet::<String>::new();
    let mut queued = HashMap::<String, VecDeque<PlayerCommand>>::new();
    let mut queued_count = 0usize;
    let mut executing = futures_util::stream::FuturesUnordered::new();
    let mut receiving = true;
    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                receiver.close();
                while let Some(command) = receiver.recv().await {
                    reject_command(command);
                }
                for (_, commands) in queued {
                    for command in commands {
                        reject_command(command);
                    }
                }
                break;
            }
            completed = executing.next(), if !executing.is_empty() => {
                let execution_key = completed.expect("guarded non-empty executor set");
                if let Some(commands) = queued.get_mut(&execution_key)
                    && let Some(command) = commands.pop_front()
                {
                    queued_count -= 1;
                    executing.push(execute_command(command, services.clone()));
                    if commands.is_empty() {
                        queued.remove(&execution_key);
                    }
                } else {
                    active.remove(&execution_key);
                    queued.remove(&execution_key);
                    if !receiving && executing.is_empty() {
                        break;
                    }
                }
            }
            command = receiver.recv(), if receiving && queued_count < max_queued => {
                let Some(command) = command else {
                    receiving = false;
                    if executing.is_empty() {
                        break;
                    }
                    continue;
                };
                let execution_key = command.execution_key().to_owned();
                if active.insert(execution_key.clone()) {
                    executing.push(execute_command(command, services.clone()));
                } else {
                    queued.entry(execution_key).or_default().push_back(command);
                    queued_count += 1;
                }
            }
            handle = shutdown_receiver.recv() => {
                let Some(handle) = handle else { continue };
                let command = PlayerCommand::Shutdown { handle };
                let execution_key = command.execution_key().to_owned();
                if active.insert(execution_key.clone()) {
                    executing.push(execute_command(command, services.clone()));
                } else {
                    queued.entry(execution_key).or_default().push_back(command);
                    queued_count += 1;
                }
            }
        }
    }
}

impl PlayerCommand {
    fn execution_key(&self) -> &str {
        match self {
            Self::Apply { handle, .. }
            | Self::Snapshot { handle, .. }
            | Self::Destroy { handle, .. }
            | Self::Shutdown { handle }
            | Self::SourceTerminal { handle, .. }
            | Self::VoiceClosed { handle, .. }
            | Self::VoiceReady { handle, .. } => &handle.inner.execution_key,
        }
    }
}

fn execute_command(command: PlayerCommand, services: PlayerServices) -> BoxFuture<'static, String> {
    let execution_key = command.execution_key().to_owned();
    async move {
        execute(command, services).await;
        execution_key
    }
    .boxed()
}

fn reject_command(command: PlayerCommand) {
    match command {
        PlayerCommand::Apply { reply, .. } | PlayerCommand::Snapshot { reply, .. } => {
            let _ = reply.send(Err(PlayerError::NotFound));
        }
        PlayerCommand::Destroy { reply, .. } => {
            let _ = reply.send(Err(PlayerError::NotFound));
        }
        PlayerCommand::Shutdown { .. }
        | PlayerCommand::SourceTerminal { .. }
        | PlayerCommand::VoiceClosed { .. }
        | PlayerCommand::VoiceReady { .. } => {}
    }
}

async fn execute(command: PlayerCommand, services: PlayerServices) {
    match command {
        PlayerCommand::Apply {
            handle,
            session,
            update,
            no_replace,
            reply,
        } => {
            let result = apply_update(&handle, &session, *update, no_replace, &services).await;
            let _ = reply.send(result);
        }
        PlayerCommand::Snapshot { handle, reply } => {
            let result = snapshot(&handle).await;
            let _ = reply.send(result);
        }
        PlayerCommand::Destroy {
            handle,
            session,
            reply,
        } => {
            let result = destroy(&handle, Some(&session)).await;
            let _ = reply.send(result);
        }
        PlayerCommand::Shutdown { handle } => {
            let _ = destroy(&handle, None).await;
        }
        PlayerCommand::SourceTerminal {
            handle,
            session,
            generation,
            failure,
        } => {
            let _ = source_terminal(&handle, &session, generation, failure).await;
        }
        PlayerCommand::VoiceClosed {
            handle,
            session,
            connection,
            close,
        } => {
            let _ = voice_closed(&handle, &session, connection, close).await;
        }
        PlayerCommand::VoiceReady {
            handle,
            session,
            connection,
        } => {
            let _ = voice_ready(&handle, &session, connection).await;
        }
    }
}

pub fn validate_update(update: &PlayerUpdate) -> Result<(), PlayerError> {
    if !update.track.is_omitted()
        && (!update.encoded_track.is_omitted() || !update.identifier.is_omitted())
    {
        return Err(PlayerError::Invalid(
            "Cannot specify both track and encodedTrack/identifier",
        ));
    }
    if matches!(update.track, PatchField::Null) {
        return Err(PlayerError::Invalid("track must not be null"));
    }
    if matches!(update.identifier, PatchField::Null) {
        return Err(PlayerError::Invalid("identifier must not be null"));
    }
    if matches!(update.position, PatchField::Null) {
        return Err(PlayerError::Invalid("position must not be null"));
    }
    if matches!(update.volume, PatchField::Null) {
        return Err(PlayerError::Invalid("volume must not be null"));
    }
    if matches!(update.paused, PatchField::Null) {
        return Err(PlayerError::Invalid("paused must not be null"));
    }
    if matches!(update.filters, PatchField::Null) {
        return Err(PlayerError::Invalid("filters must not be null"));
    }
    if let PatchField::Value(filters) = &update.filters {
        validate_filters(filters)?;
    }
    if matches!(update.voice, PatchField::Null) {
        return Err(PlayerError::Invalid("voice must not be null"));
    }
    if let PatchField::Value(track) = &update.track {
        if !track.encoded.is_omitted() && !track.identifier.is_omitted() {
            return Err(PlayerError::Invalid(
                "Cannot specify both encodedTrack and identifier",
            ));
        }
        if matches!(track.identifier, PatchField::Null) {
            return Err(PlayerError::Invalid("identifier must not be null"));
        }
        if matches!(track.user_data, PatchField::Null) {
            return Err(PlayerError::Invalid("userData must not be null"));
        }
    }
    if let PatchField::Value(end_time) = update.end_time
        && end_time <= 0
    {
        return Err(PlayerError::Invalid("End time must be greater than 0"));
    }
    if let PatchField::Value(voice) = &update.voice
        && (voice.token.trim().is_empty()
            || voice.endpoint.trim().is_empty()
            || voice.session_id.trim().is_empty()
            || voice
                .channel_id
                .as_deref()
                .is_none_or(|channel| channel.trim().is_empty()))
    {
        return Err(PlayerError::Invalid(
            "token, endpoint, sessionId and channelId must be provided in voice state",
        ));
    }
    Ok(())
}

/// Returns the protocol names of filters present in a request but disabled by
/// server configuration. The caller owns formatting the reference-compatible
/// error message.
#[must_use]
pub fn disabled_filter_names(filters: &Filters, enabled: &FilterConfig) -> Vec<&'static str> {
    let disabled = [
        (
            "volume",
            enabled.volume,
            matches!(filters.volume, PatchField::Value(_)),
        ),
        (
            "equalizer",
            enabled.equalizer,
            matches!(filters.equalizer, PatchField::Value(_)),
        ),
        (
            "karaoke",
            enabled.karaoke,
            matches!(filters.karaoke, PatchField::Value(_)),
        ),
        (
            "timescale",
            enabled.timescale,
            matches!(filters.timescale, PatchField::Value(_)),
        ),
        (
            "tremolo",
            enabled.tremolo,
            matches!(filters.tremolo, PatchField::Value(_)),
        ),
        (
            "vibrato",
            enabled.vibrato,
            matches!(filters.vibrato, PatchField::Value(_)),
        ),
        (
            "distortion",
            enabled.distortion,
            matches!(filters.distortion, PatchField::Value(_)),
        ),
        (
            "rotation",
            enabled.rotation,
            matches!(filters.rotation, PatchField::Value(_)),
        ),
        (
            "channelMix",
            enabled.channel_mix,
            matches!(filters.channel_mix, PatchField::Value(_)),
        ),
        (
            "lowPass",
            enabled.low_pass,
            matches!(filters.low_pass, PatchField::Value(_)),
        ),
    ];
    disabled
        .into_iter()
        .filter_map(|(name, is_enabled, present)| (!is_enabled && present).then_some(name))
        .collect()
}

async fn apply_update(
    handle: &PlayerHandle,
    session: &SessionHandle,
    update: PlayerUpdate,
    no_replace: bool,
    services: &PlayerServices,
) -> Result<Value, PlayerError> {
    validate_update(&update)?;
    let voice_changed = matches!(update.voice, PatchField::Value(_));
    let filters_changed = matches!(update.filters, PatchField::Value(_));
    let volume_changed = matches!(update.volume, PatchField::Value(_));
    let pause_changed = matches!(update.paused, PatchField::Value(_));
    let position_changed = matches!(update.position, PatchField::Value(_));
    let updated_voice = if let PatchField::Value(voice) = &update.voice {
        prepare_voice_connection(handle, session, voice, services.voice.as_ref()).await?
    } else {
        None
    };
    let mut state = handle.inner.state.lock().await;
    if state.destroyed || handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    if state.mantle.is_none()
        && let Some(adapter) = &services.adapter
    {
        state.mantle = Some(
            adapter
                .create_player(handle.inner.cancellation.child_token())
                .await
                .map_err(map_adapter_error)?,
        );
    }

    if volume_changed || filters_changed {
        let volume = match update.volume {
            PatchField::Value(volume) => volume,
            _ => state.volume,
        };
        let (wire, mut configuration) = if let PatchField::Value(filters) = &update.filters {
            normalize_filters(filters)?
        } else {
            (state.filters.clone(), state.filter_configuration.clone())
        };
        configuration.player_volume =
            Some(u16::try_from(volume.clamp(0, 1000)).expect("clamped player volume"));
        if let Some(mantle) = &state.mantle {
            mantle
                .set_filters(
                    configuration.clone(),
                    handle.inner.cancellation.child_token(),
                )
                .await
                .map_err(map_adapter_error)?;
        }
        state.filters = wire;
        state.filter_configuration = configuration;
        state.volume = volume;
    }
    if let PatchField::Value(voice) = &update.voice {
        state.voice = voice.clone();
        state.voice_connection = updated_voice;
    }
    let (track_request, user_data) = normalized_track(&update)?;
    let replacing = !matches!(track_request, TrackRequest::Omitted);
    let requests_track_start = matches!(
        track_request,
        TrackRequest::Encoded(_) | TrackRequest::Identifier(_)
    );
    let publish_player_update = matches!(update.filters, PatchField::Value(_))
        || (!replacing && matches!(update.position, PatchField::Value(_)) && state.track.is_some());
    if !replacing {
        if let PatchField::Value(paused) = update.paused {
            if let Some(mantle) = &state.mantle {
                mantle
                    .pause(paused, handle.inner.cancellation.child_token())
                    .await
                    .map_err(map_adapter_error)?;
            }
            state.paused = paused;
        }
        if let PatchField::Value(position) = update.position
            && state.track.is_some()
        {
            let position = u64::try_from(position).unwrap_or(0);
            if let Some(mantle) = &state.mantle {
                mantle
                    .seek(position, handle.inner.cancellation.child_token())
                    .await
                    .map_err(map_adapter_error)?;
            }
            state.position_ms = position;
        }
        if let Some(user_data) = &user_data
            && let Some(track) = &mut state.track
        {
            track.user_data.clone_from(user_data);
        }
    }
    match update.end_time {
        PatchField::Omitted => {}
        PatchField::Null if !replacing => state.end_time_ms = None,
        PatchField::Value(end_time) if !replacing && state.track.is_some() => {
            state.end_time_ms = u64::try_from(end_time).ok();
        }
        PatchField::Null | PatchField::Value(_) => {}
    }

    let replace_allowed = replacing && !(no_replace && state.track.is_some());
    let publish_player_update = publish_player_update || (replace_allowed && requests_track_start);
    if replace_allowed {
        match track_request {
            TrackRequest::Omitted => {}
            TrackRequest::Stop => {
                let user_data = state
                    .track
                    .as_ref()
                    .map_or_else(JsonObject::new, |track| track.user_data.clone());
                if let Some(mantle) = &state.mantle {
                    mantle
                        .stop(handle.inner.cancellation.child_token())
                        .await
                        .map_err(map_adapter_error)?;
                }
                state.track = None;
                state.outbound_connection = None;
                state.position_ms = 0;
                state.end_time_ms = None;
                state.paused = false;
                drain_events(handle, session, &state.mantle, &user_data, None, None).await?;
            }
            TrackRequest::Encoded(encoded) => {
                let outbound = if state.outbound_connection.is_none() {
                    Some(services.load_admission.try_acquire_outbound()?)
                } else {
                    None
                };
                let adapter = services
                    .adapter
                    .as_ref()
                    .ok_or(PlayerError::Invalid("invalid encoded track"))?;
                let track = adapter
                    .decode(
                        EncodedTrack::new(encoded),
                        handle.inner.cancellation.child_token(),
                    )
                    .await
                    .map_err(map_adapter_error)?;
                validate_media_track(&track, services.json_policy)
                    .map_err(|_| PlayerError::Media("source pluginInfo exceeds resource limits"))?;
                play_track(handle, session, &mut state, track, user_data, &update).await?;
                if let Some(outbound) = outbound {
                    state.outbound_connection = Some(outbound);
                }
            }
            TrackRequest::Identifier(identifier) => {
                let adapter = services
                    .adapter
                    .as_ref()
                    .ok_or(PlayerError::Invalid("identifier loading is not configured"))?;
                let mut load_permits = services.load_admission.try_acquire()?;
                let loaded = adapter
                    .load(
                        LoadRequest {
                            identifier,
                            route: SourceRoute::default(),
                        },
                        handle.inner.cancellation.child_token(),
                    )
                    .await
                    .map_err(map_adapter_error)?;
                let track = match loaded {
                    LoadOutcome::Track(track) => track,
                    LoadOutcome::NoMatches => {
                        return Err(PlayerError::Invalid("No matches found for identifier"));
                    }
                    LoadOutcome::Search(_) | LoadOutcome::Playlist { .. } => {
                        return Err(PlayerError::Invalid(
                            "Cannot play a playlist or search result",
                        ));
                    }
                };
                validate_media_track(&track, services.json_policy)
                    .map_err(|_| PlayerError::Media("source pluginInfo exceeds resource limits"))?;
                play_track(handle, session, &mut state, track, user_data, &update).await?;
                if state.outbound_connection.is_none() {
                    state.outbound_connection = Some(load_permits.take_outbound());
                }
            }
        }
    }

    let refresh_audio = voice_changed
        || filters_changed
        || volume_changed
        || pause_changed
        || position_changed
        || replace_allowed;
    let voice_connection = state.voice_connection.clone();
    let mantle_for_snapshot = state.mantle.clone();
    let audio_action = if refresh_audio {
        state.audio_generation = state
            .audio_generation
            .checked_add(1)
            .ok_or(PlayerError::Media("audio source generation exhausted"))?;
        let generation = state.audio_generation;
        voice_connection.as_ref().map(|connection| {
            if state.track.is_some() && !state.paused {
                state.mantle.as_ref().map_or_else(
                    || VoiceAudioAction::Stop(Arc::clone(connection)),
                    |mantle| VoiceAudioAction::Set {
                        connection: Arc::clone(connection),
                        source: Arc::new(MantleVoiceSource {
                            mantle: Arc::clone(mantle),
                            handle: handle.clone(),
                            session: session.clone(),
                            generation,
                        }),
                    },
                )
            } else {
                VoiceAudioAction::Stop(Arc::clone(connection))
            }
        })
    } else {
        None
    };
    drop(state);
    if voice_changed && let Some(connection) = &voice_connection {
        handle.start_voice_monitor(session.clone(), Arc::clone(connection));
    }
    if let Some(action) = audio_action
        && let Err(error) = apply_voice_audio(action, handle.inner.cancellation.child_token()).await
    {
        if is_deferred_voice_audio_error(&error) {
            // A fresh Discord voice generation can expose transport before
            // DAVE reaches Connected. Keep the player alive and let the voice
            // monitor attach the current Mantle source at the exact readiness
            // transition; never send plaintext or fail the Lavalink voice
            // update with a transient 500.
            handle.inner.deferred_audio.store(true, Ordering::Release);
        } else {
            return Err(map_voice_error(error));
        }
    }
    let mantle_snapshot = if let Some(mantle) = mantle_for_snapshot {
        Some(mantle.snapshot().await.map_err(map_adapter_error)?)
    } else {
        None
    };
    let voice_snapshot = if let Some(connection) = &voice_connection {
        Some(connection.snapshot().await.map_err(map_voice_error)?)
    } else {
        None
    };
    let mut state = handle.inner.state.lock().await;
    if state.destroyed || handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    if let Some(snapshot) = mantle_snapshot {
        apply_mantle_snapshot(&mut state, snapshot);
    }
    let value = player_value(&handle.inner.guild_id, &state, voice_snapshot.as_ref());
    cache_update(handle, &state, voice_snapshot.as_ref());
    if publish_player_update {
        let payload = SessionPlayer::snapshot(handle);
        session
            .publish_player_update(handle.guild_id(), payload)
            .map_err(map_publish_error)?;
    }
    Ok(value)
}

async fn prepare_voice_connection(
    handle: &PlayerHandle,
    session: &SessionHandle,
    voice: &VoiceState,
    backend: Option<&Arc<dyn VoiceBackend>>,
) -> Result<Option<Arc<dyn VoiceConnection>>, PlayerError> {
    let existing = {
        let state = handle.inner.state.lock().await;
        if state.destroyed || handle.inner.cancellation.is_cancelled() {
            return Err(PlayerError::NotFound);
        }
        state.voice_connection.clone()
    };
    let Some(backend) = backend else {
        return Ok(existing);
    };
    let info = voice_connection_info(handle, session, voice)?;
    let cancellation = handle.inner.cancellation.child_token();
    if let Some(connection) = existing {
        connection
            .update(info, cancellation)
            .await
            .map_err(map_voice_error)?;
        Ok(Some(connection))
    } else {
        backend
            .connect(info, cancellation)
            .await
            .map(Some)
            .map_err(map_voice_error)
    }
}

fn voice_connection_info(
    handle: &PlayerHandle,
    session: &SessionHandle,
    voice: &VoiceState,
) -> Result<VoiceConnectionInfo, PlayerError> {
    let guild_id = handle
        .guild_id()
        .parse()
        .map_err(|_| PlayerError::Invalid("guild ID is not a Discord snowflake"))?;
    let user_id = session
        .user_id()
        .parse()
        .map_err(|_| PlayerError::Invalid("user ID is not a Discord snowflake"))?;
    let channel_id = voice
        .channel_id
        .as_deref()
        .ok_or(PlayerError::Invalid("voice channel ID is required"))?
        .parse()
        .map_err(|_| PlayerError::Invalid("channel ID is not a Discord snowflake"))?;
    Ok(VoiceConnectionInfo {
        guild_id,
        user_id,
        channel_id,
        endpoint: voice.endpoint.clone(),
        session_id: VoiceSecret::new(voice.session_id.clone()),
        token: VoiceSecret::new(voice.token.clone()),
    })
}

enum VoiceAudioAction {
    Set {
        connection: Arc<dyn VoiceConnection>,
        source: Arc<dyn VoiceFrameSource>,
    },
    Stop(Arc<dyn VoiceConnection>),
}

async fn apply_voice_audio(
    action: VoiceAudioAction,
    cancellation: CancellationToken,
) -> Result<(), VoiceError> {
    match action {
        VoiceAudioAction::Set { connection, source } => {
            connection.set_source(source, cancellation).await
        }
        VoiceAudioAction::Stop(connection) => connection.stop_audio().await,
    }
}

fn is_deferred_voice_audio_error(error: &VoiceError) -> bool {
    error.kind == VoiceErrorKind::NotReady
}

async fn voice_ready(
    handle: &PlayerHandle,
    session: &SessionHandle,
    connection: Arc<dyn VoiceConnection>,
) -> Result<(), PlayerError> {
    if !handle.inner.deferred_audio.swap(false, Ordering::AcqRel) {
        return Ok(());
    }
    let (mantle, generation) = {
        let mut state = handle.inner.state.lock().await;
        if state.destroyed
            || handle.inner.cancellation.is_cancelled()
            || !state
                .voice_connection
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &connection))
            || state.paused
        {
            return Ok(());
        }
        let Some(mantle) = state.mantle.clone() else {
            return Ok(());
        };
        if state.track.is_none() {
            return Ok(());
        }
        state.audio_generation = state
            .audio_generation
            .checked_add(1)
            .ok_or(PlayerError::Media("audio source generation exhausted"))?;
        (mantle, state.audio_generation)
    };
    let source = Arc::new(MantleVoiceSource {
        mantle,
        handle: handle.clone(),
        session: session.clone(),
        generation,
    });
    match connection
        .set_source(source, handle.inner.cancellation.child_token())
        .await
    {
        Ok(()) => Ok(()),
        Err(error) if is_deferred_voice_audio_error(&error) => {
            handle.inner.deferred_audio.store(true, Ordering::Release);
            Ok(())
        }
        Err(error) => Err(map_voice_error(error)),
    }
}

struct MantleVoiceSource {
    mantle: Arc<dyn MantlePlayer>,
    handle: PlayerHandle,
    session: SessionHandle,
    generation: u64,
}

impl VoiceFrameSource for MantleVoiceSource {
    fn next_frame(
        &self,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
        Box::pin(async move {
            let frame = match self.mantle.next_frame(cancellation.clone()).await {
                Ok(frame) => frame
                    .map(|frame| {
                        TimedOpusFrame::from_packet(
                            frame.sequence,
                            Duration::from_millis(frame.sequence.saturating_mul(20)),
                            Duration::from_millis(u64::from(frame.duration_ms)),
                            frame.payload,
                        )
                        .map_err(|_| {
                            VoiceError::new(
                                VoiceErrorKind::Protocol,
                                "Mantle produced an invalid Discord Opus frame",
                            )
                        })
                    })
                    .transpose(),
                Err(error) => Err(map_adapter_voice_error(error)),
            };
            match frame {
                Ok(Some(frame)) => Ok(Some(frame)),
                Ok(None) => {
                    self.handle
                        .report_source_terminal(
                            self.session.clone(),
                            self.generation,
                            None,
                            cancellation,
                        )
                        .await;
                    Ok(None)
                }
                Err(error) => {
                    self.handle
                        .report_source_terminal(
                            self.session.clone(),
                            self.generation,
                            Some(error.clone()),
                            cancellation,
                        )
                        .await;
                    Err(error)
                }
            }
        })
    }
}

impl PlayerHandle {
    async fn report_source_terminal(
        &self,
        session: SessionHandle,
        generation: u64,
        failure: Option<VoiceError>,
        cancellation: CancellationToken,
    ) {
        let command = PlayerCommand::SourceTerminal {
            handle: self.clone(),
            session,
            generation,
            failure,
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {}
            () = self.inner.cancellation.cancelled() => {}
            _ = self.inner.sender.send(command) => {}
        }
    }
}

fn validate_filters(filters: &Filters) -> Result<(), PlayerError> {
    if matches!(filters.volume, PatchField::Null) {
        return Err(PlayerError::Invalid("volume filter must not be null"));
    }
    if matches!(filters.equalizer, PatchField::Null) {
        return Err(PlayerError::Invalid("equalizer filter must not be null"));
    }
    if let PatchField::Value(volume) = filters.volume
        && !(0.0..=5.0).contains(&volume)
    {
        return Err(PlayerError::Invalid(
            "filter volume must be between 0 and 5",
        ));
    }
    if let PatchField::Value(bands) = &filters.equalizer {
        for band in bands {
            if !(0..15).contains(&band.band) {
                return Err(PlayerError::Invalid(
                    "equalizer band must be between 0 and 14",
                ));
            }
        }
    }
    if let PatchField::Value(timescale) = filters.timescale
        && (timescale.speed <= 0.0 || timescale.pitch <= 0.0 || timescale.rate <= 0.0)
    {
        return Err(PlayerError::Invalid(
            "timescale speed, pitch and rate must be greater than 0",
        ));
    }
    if let PatchField::Value(timescale) = filters.timescale {
        let timescale = RuntimeTimescale {
            speed: timescale.speed,
            pitch: timescale.pitch,
            rate: timescale.rate,
        };
        if !timescale.has_supported_duration_ratio() {
            return Err(PlayerError::Invalid(
                "combined timescale speed and rate are outside the supported range",
            ));
        }
    }
    for modulation in [&filters.tremolo, &filters.vibrato] {
        if let PatchField::Value(modulation) = modulation
            && (modulation.frequency <= 0.0 || !(0.0..=1.0).contains(&modulation.depth))
        {
            return Err(PlayerError::Invalid(
                "modulation frequency must be positive and depth between 0 and 1",
            ));
        }
    }
    if let PatchField::Value(vibrato) = filters.vibrato
        && vibrato.frequency > 14.0
    {
        return Err(PlayerError::Invalid("vibrato frequency must not exceed 14"));
    }
    Ok(())
}

fn normalize_filters(filters: &Filters) -> Result<(Value, FilterConfiguration), PlayerError> {
    validate_filters(filters)?;
    let mut equalizer = [0.0; 15];
    if let PatchField::Value(bands) = &filters.equalizer {
        for band in bands {
            equalizer[usize::try_from(band.band).expect("validated non-negative band")] = band.gain;
        }
    }
    let configuration = FilterConfiguration {
        player_volume: None,
        volume: value(&filters.volume),
        equalizer: matches!(filters.equalizer, PatchField::Value(_)).then_some(equalizer),
        karaoke: value(&filters.karaoke).map(|value| RuntimeKaraoke {
            level: value.level,
            mono_level: value.mono_level,
            filter_band: value.filter_band,
            filter_width: value.filter_width,
        }),
        timescale: value(&filters.timescale).map(|value| RuntimeTimescale {
            speed: value.speed,
            pitch: value.pitch,
            rate: value.rate,
        }),
        tremolo: value(&filters.tremolo).map(|value| Modulation {
            frequency: value.frequency,
            depth: value.depth,
        }),
        vibrato: value(&filters.vibrato).map(|value| Modulation {
            frequency: value.frequency,
            depth: value.depth,
        }),
        distortion: value(&filters.distortion).map(|value| RuntimeDistortion {
            sin_offset: value.sin_offset,
            sin_scale: value.sin_scale,
            cos_offset: value.cos_offset,
            cos_scale: value.cos_scale,
            tan_offset: value.tan_offset,
            tan_scale: value.tan_scale,
            offset: value.offset,
            scale: value.scale,
        }),
        rotation_hz: value(&filters.rotation).map(|value| value.rotation_hz),
        channel_mix: value(&filters.channel_mix).map(|value| RuntimeChannelMix {
            left_to_left: value.left_to_left,
            left_to_right: value.left_to_right,
            right_to_left: value.right_to_left,
            right_to_right: value.right_to_right,
        }),
        low_pass_smoothing: value(&filters.low_pass).map(|value| value.smoothing),
    };
    let normalized = Filters {
        volume: values_only(&filters.volume),
        equalizer: values_only(&filters.equalizer),
        karaoke: values_only(&filters.karaoke),
        timescale: values_only(&filters.timescale),
        tremolo: values_only(&filters.tremolo),
        vibrato: values_only(&filters.vibrato),
        distortion: values_only(&filters.distortion),
        rotation: values_only(&filters.rotation),
        channel_mix: values_only(&filters.channel_mix),
        low_pass: values_only(&filters.low_pass),
        plugin_filters: filters.plugin_filters.clone(),
    };
    // `Value::from(f32)` first widens to the exact `f64` value, exposing binary
    // noise such as `0.800000011920929` on the Lavalink wire. Serialize the
    // protocol model directly so serde_json emits the shortest decimal that
    // round-trips to the original `f32`, then retain that JSON representation.
    let serialized =
        serde_json::to_string(&normalized).map_err(|_| PlayerError::Invalid("invalid filters"))?;
    let wire =
        serde_json::from_str(&serialized).map_err(|_| PlayerError::Invalid("invalid filters"))?;
    Ok((wire, configuration))
}

fn value<T: Copy>(field: &PatchField<T>) -> Option<T> {
    match field {
        PatchField::Value(value) => Some(*value),
        PatchField::Omitted | PatchField::Null => None,
    }
}

fn values_only<T: Clone>(field: &PatchField<T>) -> PatchField<T> {
    match field {
        PatchField::Value(value) => PatchField::Value(value.clone()),
        PatchField::Omitted | PatchField::Null => PatchField::Omitted,
    }
}

async fn play_track(
    handle: &PlayerHandle,
    session: &SessionHandle,
    state: &mut PlayerState,
    track: MediaTrack,
    user_data: Option<JsonObject>,
    update: &PlayerUpdate,
) -> Result<(), PlayerError> {
    let paused = match update.paused {
        PatchField::Value(value) => value,
        _ => false,
    };
    let position = match update.position {
        PatchField::Value(value) => u64::try_from(value).unwrap_or(0),
        _ => 0,
    };
    let previous_user_data = state.track.as_ref().map(|track| track.user_data.clone());
    let user_data = user_data.unwrap_or_default();
    if let Some(mantle) = &state.mantle {
        mantle
            .play(track.clone(), handle.inner.cancellation.child_token())
            .await
            .map_err(map_adapter_error)?;
        if position > 0 {
            mantle
                .seek(position, handle.inner.cancellation.child_token())
                .await
                .map_err(map_adapter_error)?;
        }
        if paused {
            mantle
                .pause(true, handle.inner.cancellation.child_token())
                .await
                .map_err(map_adapter_error)?;
        }
    }
    state.track = Some(ActiveTrack {
        media: track,
        user_data: user_data.clone(),
    });
    state.paused = paused;
    state.position_ms = position;
    state.end_time_ms = match update.end_time {
        PatchField::Value(value) => u64::try_from(value).ok(),
        _ => None,
    };
    drain_events(
        handle,
        session,
        &state.mantle,
        &user_data,
        previous_user_data.as_ref(),
        None,
    )
    .await?;
    Ok(())
}

async fn drain_events(
    handle: &PlayerHandle,
    session: &SessionHandle,
    mantle: &Option<Arc<dyn MantlePlayer>>,
    user_data: &JsonObject,
    replaced_user_data: Option<&JsonObject>,
    end_reason_override: Option<TrackEndReason>,
) -> Result<(), PlayerError> {
    let Some(mantle) = mantle else {
        return Ok(());
    };
    while let Some(mut event) = mantle
        .next_event(handle.inner.cancellation.child_token())
        .await
        .map_err(map_adapter_error)?
    {
        if let (
            Some(reason),
            MediaEvent::TrackEnd {
                reason: event_reason,
                ..
            },
        ) = (end_reason_override, &mut event)
        {
            *event_reason = reason;
        }
        let event_user_data = if matches!(
            event,
            MediaEvent::TrackEnd {
                reason: TrackEndReason::Replaced,
                ..
            }
        ) {
            replaced_user_data.unwrap_or(user_data)
        } else {
            user_data
        };
        let payload =
            Arc::from(event_value(&handle.inner.guild_id, event, event_user_data).to_string());
        session
            .publish_critical_delivered(payload)
            .await
            .map_err(map_publish_error)?;
    }
    Ok(())
}

async fn source_terminal(
    handle: &PlayerHandle,
    session: &SessionHandle,
    generation: u64,
    failure: Option<VoiceError>,
) -> Result<(), PlayerError> {
    let (mantle, voice, track, user_data) = {
        let state = handle.inner.state.lock().await;
        if state.destroyed
            || handle.inner.cancellation.is_cancelled()
            || state.audio_generation != generation
        {
            return Ok(());
        }
        let Some(mantle) = state.mantle.clone() else {
            return Ok(());
        };
        (
            mantle,
            state.voice_connection.clone(),
            state.track.as_ref().map(|track| track.media.clone()),
            state
                .track
                .as_ref()
                .map_or_else(JsonObject::new, |track| track.user_data.clone()),
        )
    };

    if let (Some(failure), Some(track)) = (&failure, track) {
        let payload = Arc::from(
            event_value(
                &handle.inner.guild_id,
                MediaEvent::TrackError {
                    track,
                    message: failure.message.to_owned(),
                },
                &user_data,
            )
            .to_string(),
        );
        session
            .publish_critical_delivered(payload)
            .await
            .map_err(map_publish_error)?;
        mantle
            .stop(handle.inner.cancellation.child_token())
            .await
            .map_err(map_adapter_error)?;
    }

    drain_events(
        handle,
        session,
        &Some(Arc::clone(&mantle)),
        &user_data,
        None,
        failure.as_ref().map(|_| TrackEndReason::LoadFailed),
    )
    .await?;
    let mantle_snapshot = mantle.snapshot().await.map_err(map_adapter_error)?;
    let voice_snapshot = if let Some(connection) = voice {
        Some(connection.snapshot().await.map_err(map_voice_error)?)
    } else {
        None
    };

    let mut state = handle.inner.state.lock().await;
    if state.destroyed
        || handle.inner.cancellation.is_cancelled()
        || state.audio_generation != generation
    {
        return Ok(());
    }
    apply_mantle_snapshot(&mut state, mantle_snapshot);
    cache_update(handle, &state, voice_snapshot.as_ref());
    drop(state);
    session
        .publish_player_update(handle.guild_id(), SessionPlayer::snapshot(handle))
        .map(|_| ())
        .map_err(map_publish_error)
}

async fn voice_closed(
    handle: &PlayerHandle,
    session: &SessionHandle,
    connection: Arc<dyn VoiceConnection>,
    close: VoiceClose,
) -> Result<(), PlayerError> {
    {
        let mut state = handle.inner.state.lock().await;
        if state.destroyed || handle.inner.cancellation.is_cancelled() {
            return Ok(());
        }
        let Some(current) = state.voice_connection.as_ref() else {
            return Ok(());
        };
        if !Arc::ptr_eq(current, &connection) {
            return Ok(());
        }
        state.voice_connection = None;
        state.audio_generation = state
            .audio_generation
            .checked_add(1)
            .ok_or(PlayerError::Media("audio source generation exhausted"))?;
    }

    handle.stop_voice_monitor();
    let _ = connection.stop_audio().await;
    let payload = Arc::from(
        json!({
            "op": "event",
            "type": "WebSocketClosedEvent",
            "guildId": handle.guild_id(),
            "code": i32::from(close.code),
            "reason": close.reason.as_ref(),
            "byRemote": close.by_remote,
        })
        .to_string(),
    );
    session
        .publish_critical_delivered(payload)
        .await
        .map_err(map_publish_error)?;

    let state = handle.inner.state.lock().await;
    cache_update(handle, &state, None);
    drop(state);
    session
        .publish_player_update(handle.guild_id(), handle.cached_update())
        .map(|_| ())
        .map_err(map_publish_error)
}

async fn snapshot(handle: &PlayerHandle) -> Result<Value, PlayerError> {
    let (mantle, voice_connection) = {
        let state = handle.inner.state.lock().await;
        if state.destroyed || handle.inner.cancellation.is_cancelled() {
            return Err(PlayerError::NotFound);
        }
        (state.mantle.clone(), state.voice_connection.clone())
    };
    let mantle_snapshot = if let Some(mantle) = mantle {
        Some(mantle.snapshot().await.map_err(map_adapter_error)?)
    } else {
        None
    };
    let voice_snapshot = if let Some(connection) = voice_connection {
        Some(connection.snapshot().await.map_err(map_voice_error)?)
    } else {
        None
    };
    let mut state = handle.inner.state.lock().await;
    if state.destroyed || handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    if let Some(snapshot) = mantle_snapshot {
        apply_mantle_snapshot(&mut state, snapshot);
    }
    let value = player_value(&handle.inner.guild_id, &state, voice_snapshot.as_ref());
    cache_update(handle, &state, voice_snapshot.as_ref());
    Ok(value)
}

async fn destroy(
    handle: &PlayerHandle,
    session: Option<&SessionHandle>,
) -> Result<(), PlayerError> {
    let (voice, mantle, cleanup_payload) = {
        let mut state = handle.inner.state.lock().await;
        if state.destroyed {
            return Ok(());
        }
        let cleanup_payload = session.and_then(|_| {
            state.track.as_ref().map(|active| {
                Arc::from(
                    event_value(
                        handle.guild_id(),
                        MediaEvent::TrackEnd {
                            track: active.media.clone(),
                            reason: TrackEndReason::Cleanup,
                        },
                        &active.user_data,
                    )
                    .to_string(),
                )
            })
        });
        handle.inner.cancellation.cancel();
        state.destroyed = true;
        state.track = None;
        state.outbound_connection = None;
        handle
            .inner
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .playing = false;
        (
            state.voice_connection.take(),
            state.mantle.take(),
            cleanup_payload,
        )
    };
    handle.stop_voice_monitor();
    let publish_result = if let (Some(session), Some(payload)) = (session, cleanup_payload) {
        session
            .publish_critical_delivered(payload)
            .await
            .map_err(map_publish_error)
    } else {
        Ok(())
    };
    if let Some(voice) = voice {
        voice.shutdown().await.map_err(map_voice_error)?;
    }
    if let Some(mantle) = mantle {
        mantle.shutdown().await.map_err(map_adapter_error)?;
    }
    publish_result
}

fn apply_mantle_snapshot(state: &mut PlayerState, snapshot: crust::media::PlayerSnapshot) {
    state.position_ms = snapshot.position_ms;
    state.paused = snapshot.status == PlayerStatus::Paused;
    if snapshot.track.is_none()
        && matches!(snapshot.status, PlayerStatus::Idle | PlayerStatus::Stopped)
    {
        state.track = None;
        state.outbound_connection = None;
    }
}

enum TrackRequest {
    Omitted,
    Stop,
    Encoded(String),
    Identifier(String),
}

fn normalized_track(
    update: &PlayerUpdate,
) -> Result<(TrackRequest, Option<JsonObject>), PlayerError> {
    let track = match &update.track {
        PatchField::Value(track) => Some(track),
        PatchField::Omitted => None,
        PatchField::Null => return Err(PlayerError::Invalid("track must not be null")),
    };
    let encoded = track.map_or(&update.encoded_track, |track| &track.encoded);
    let identifier = track.map_or(&update.identifier, |track| &track.identifier);
    let user_data = track.and_then(|track| match &track.user_data {
        PatchField::Value(value) => Some(value.clone()),
        _ => None,
    });
    let request = match (encoded, identifier) {
        (PatchField::Value(value), PatchField::Omitted) => TrackRequest::Encoded(value.clone()),
        (PatchField::Null, PatchField::Omitted) => TrackRequest::Stop,
        (PatchField::Omitted, PatchField::Value(value)) => TrackRequest::Identifier(value.clone()),
        (PatchField::Omitted, PatchField::Omitted) => TrackRequest::Omitted,
        _ => {
            return Err(PlayerError::Invalid(
                "Cannot specify both encodedTrack and identifier",
            ));
        }
    };
    Ok((request, user_data))
}

fn player_value(
    guild_id: &str,
    state: &PlayerState,
    voice_snapshot: Option<&VoiceSnapshot>,
) -> Value {
    let connected = voice_snapshot
        .is_some_and(|snapshot| matches!(snapshot.phase, crust::voice::VoicePhase::Connected));
    let ping = voice_snapshot
        .and_then(|snapshot| snapshot.ping)
        .and_then(|ping| i64::try_from(ping.as_millis()).ok())
        .unwrap_or(-1);
    json!({
        "guildId": guild_id,
        "track": state
            .track
            .as_ref()
            .map(|track| track_value(&track.media, state.position_ms, &track.user_data)),
        "volume": state.volume,
        "paused": state.paused,
        "state": {
            "time": timestamp_ms(),
            "position": state.position_ms,
            "connected": connected,
            "ping": ping,
        },
        "voice": state.voice,
        "filters": state.filters,
    })
}

fn cache_update(
    handle: &PlayerHandle,
    state: &PlayerState,
    voice_snapshot: Option<&VoiceSnapshot>,
) {
    handle
        .inner
        .periodic_updates
        .store(state.track.is_some(), Ordering::Release);
    {
        let mut stats = handle
            .inner
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stats.playing = state.track.is_some() && !state.paused && !state.destroyed;
        if let Some(snapshot) = voice_snapshot {
            stats.sent = snapshot.counters.sent;
            stats.nulled = snapshot.counters.nulled;
            stats.deficit = snapshot.counters.deficit;
        }
    }
    let connected = voice_snapshot
        .is_some_and(|snapshot| matches!(snapshot.phase, crust::voice::VoicePhase::Connected));
    let ping = voice_snapshot
        .and_then(|snapshot| snapshot.ping)
        .and_then(|ping| i64::try_from(ping.as_millis()).ok())
        .unwrap_or(-1);
    let update: Arc<str> = Arc::from(
        json!({
            "op": "playerUpdate",
            "guildId": handle.inner.guild_id,
            "state": {
                "time": timestamp_ms(),
                "position": state.position_ms,
                "connected": connected,
                "ping": ping,
            }
        })
        .to_string(),
    );
    *handle
        .inner
        .cached_update
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = update;
}

fn default_player_state() -> Value {
    json!({
        "time": timestamp_ms(),
        "position": 0,
        "connected": false,
        "ping": -1,
    })
}

fn event_value(guild_id: &str, event: MediaEvent, user_data: &JsonObject) -> Value {
    match event {
        MediaEvent::TrackStart(track) => json!({
            "op": "event",
            "type": "TrackStartEvent",
            "guildId": guild_id,
            "track": track_value(&track, 0, user_data),
        }),
        MediaEvent::TrackEnd { track, reason } => json!({
            "op": "event",
            "type": "TrackEndEvent",
            "guildId": guild_id,
            "track": track_value(&track, 0, user_data),
            "reason": match reason {
                TrackEndReason::Finished => "finished",
                TrackEndReason::LoadFailed => "loadFailed",
                TrackEndReason::Stopped => "stopped",
                TrackEndReason::Replaced => "replaced",
                TrackEndReason::Cleanup => "cleanup",
            },
        }),
        MediaEvent::TrackError { track, message } => {
            // Lavalink v4 requires both root-cause strings even when the
            // backend exposes only one bounded, redacted failure message.
            // Reusing that message preserves the complete wire shape without
            // manufacturing a native stack trace or leaking backend types.
            json!({
                "op": "event",
                "type": "TrackExceptionEvent",
                "guildId": guild_id,
                "track": track_value(&track, 0, user_data),
                "exception": {
                    "message": message,
                    "severity": "fault",
                    "cause": message,
                    "causeStackTrace": message,
                },
            })
        }
        MediaEvent::TrackStuck {
            track,
            threshold_ms,
        } => json!({
            "op": "event",
            "type": "TrackStuckEvent",
            "guildId": guild_id,
            "track": track_value(&track, 0, user_data),
            "thresholdMs": threshold_ms,
        }),
    }
}

fn map_adapter_error(error: crust::media::AdapterError) -> PlayerError {
    match error.kind {
        AdapterErrorKind::InvalidTrack | AdapterErrorKind::LoadFailed => {
            PlayerError::Invalid(error.message)
        }
        AdapterErrorKind::Overloaded => PlayerError::Overloaded,
        _ => PlayerError::Media(error.message),
    }
}

fn map_adapter_voice_error(error: crust::media::AdapterError) -> VoiceError {
    let kind = match error.kind {
        AdapterErrorKind::Cancelled => VoiceErrorKind::Cancelled,
        AdapterErrorKind::Shutdown => VoiceErrorKind::Shutdown,
        AdapterErrorKind::Overloaded => VoiceErrorKind::Overloaded,
        AdapterErrorKind::LoadFailed
        | AdapterErrorKind::InvalidTrack
        | AdapterErrorKind::InvalidOperation => VoiceErrorKind::Protocol,
    };
    VoiceError::new(kind, "Mantle frame production failed")
}

fn map_voice_error(error: VoiceError) -> PlayerError {
    match error.kind {
        VoiceErrorKind::Overloaded => PlayerError::Overloaded,
        VoiceErrorKind::InvalidState => PlayerError::Invalid(error.message),
        VoiceErrorKind::Cancelled
        | VoiceErrorKind::Shutdown
        | VoiceErrorKind::NotReady
        | VoiceErrorKind::ConnectionFailed
        | VoiceErrorKind::Protocol => PlayerError::Media(error.message),
    }
}

fn map_publish_error(error: PublishError) -> PlayerError {
    match error {
        PublishError::Full(_) => PlayerError::Overloaded,
        PublishError::Disconnected(_) => PlayerError::NotFound,
    }
}

fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
