//! Bounded session ownership and resumable WebSocket attachment.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::stats::{FrameStats, FrameWindow};

pub const DEFAULT_RESUME_TIMEOUT_SECONDS: i64 = 60;

pub trait SessionClock: Send + Sync + 'static {
    fn now(&self) -> Duration;
}

#[derive(Debug)]
pub struct SystemSessionClock {
    origin: Instant,
}

impl SystemSessionClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemSessionClock {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionClock for SystemSessionClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

pub trait SessionPlayer: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn guild_id(&self) -> &str;
    fn wants_periodic_updates(&self) -> bool;
    fn stats(&self) -> SessionPlayerStats;
    fn snapshot(&self) -> Arc<str>;
    fn refresh(&self) -> SessionPlayerRefresh;
    fn shutdown(&self);
}

pub type SessionPlayerRefresh = Pin<Box<dyn Future<Output = (String, Arc<str>)> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSettings {
    pub resuming: bool,
    pub timeout_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionSettingsUpdate {
    pub resuming: Option<bool>,
    pub timeout_seconds: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionCounts {
    pub connected: usize,
    pub resumable: usize,
    pub players: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionPlayerStats {
    pub playing: bool,
    pub sent: u64,
    pub nulled: u64,
    pub deficit: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareError {
    Full,
    ResumeOverloaded,
    IdentityGeneration,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateError {
    NotFound,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PublishError {
    Full(Arc<str>),
    Disconnected(Arc<str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerAdmissionError {
    SessionNotFound,
    Full,
    SessionFull,
}

#[derive(Clone)]
pub struct SessionRegistry {
    core: Arc<RegistryCore>,
}

impl fmt::Debug for SessionRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionRegistry")
            .field("counts", &self.counts())
            .finish_non_exhaustive()
    }
}

struct RegistryCore {
    clock: Arc<dyn SessionClock>,
    max_sessions: usize,
    max_players: usize,
    max_players_per_session: usize,
    max_concurrent_resumes: usize,
    critical_capacity: usize,
    resume_in_flight: AtomicUsize,
    player_count: Arc<AtomicUsize>,
    state: Mutex<RegistryState>,
}

struct RegistryState {
    shutting_down: bool,
    sessions: HashMap<String, Arc<Session>>,
}

struct Session {
    id: String,
    user_id: String,
    client_name: Option<String>,
    critical_capacity: usize,
    coalesced_notify: Notify,
    frame_window: Mutex<FrameWindow>,
    inner: Mutex<SessionInner>,
}

struct SessionInner {
    generation: u64,
    lifecycle: SessionLifecycle,
    settings: SessionSettings,
    output: Option<ConnectionOutput>,
    critical_backlog: VecDeque<Arc<str>>,
    player_updates: BTreeMap<String, Arc<str>>,
    stats: Option<Arc<str>>,
    players: BTreeMap<String, PlayerEntry>,
}

#[derive(Clone, Copy)]
enum SessionLifecycle {
    Connecting { previous_deadline: Option<Duration> },
    Connected,
    DisconnectedResumable { deadline: Duration },
    Expired,
    ShuttingDown,
}

struct ConnectionOutput {
    critical: mpsc::Sender<CriticalMessage>,
    cancellation: CancellationToken,
}

pub(crate) struct CriticalMessage {
    pub(crate) payload: Arc<str>,
    pub(crate) delivered: Option<oneshot::Sender<()>>,
}

struct PlayerEntry {
    player: Arc<dyn SessionPlayer>,
    total: Arc<AtomicUsize>,
}

impl Drop for PlayerEntry {
    fn drop(&mut self) {
        self.total.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ResumePermit(Arc<RegistryCore>);

impl Drop for ResumePermit {
    fn drop(&mut self) {
        self.0.resume_in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct PreparedSession {
    registry: SessionRegistry,
    session: Arc<Session>,
    generation: u64,
    resumed: bool,
    resume_permit: Option<ResumePermit>,
    committed: bool,
}

impl fmt::Debug for PreparedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedSession")
            .field("resumed", &self.resumed)
            .finish_non_exhaustive()
    }
}

pub struct AttachedSession {
    registry: SessionRegistry,
    session: Arc<Session>,
    generation: u64,
    resumed: bool,
    critical: Option<mpsc::Receiver<CriticalMessage>>,
    initial_critical: Vec<Arc<str>>,
    initial_state: Vec<Arc<str>>,
    cancellation: CancellationToken,
}

impl fmt::Debug for AttachedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachedSession")
            .field("resumed", &self.resumed)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct SessionHandle(Arc<Session>);

impl fmt::Debug for SessionHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionHandle")
            .finish_non_exhaustive()
    }
}

impl SessionRegistry {
    #[must_use]
    pub fn new(
        max_sessions: usize,
        max_players: usize,
        max_players_per_session: usize,
        max_concurrent_resumes: usize,
        critical_capacity: usize,
        clock: Arc<dyn SessionClock>,
    ) -> Self {
        assert!(max_sessions > 0);
        assert!(max_players > 0);
        assert!(max_players_per_session > 0);
        assert!(max_concurrent_resumes > 0);
        assert!(critical_capacity > 0);
        Self {
            core: Arc::new(RegistryCore {
                clock,
                max_sessions,
                max_players,
                max_players_per_session,
                max_concurrent_resumes,
                critical_capacity,
                resume_in_flight: AtomicUsize::new(0),
                player_count: Arc::new(AtomicUsize::new(0)),
                state: Mutex::new(RegistryState {
                    shutting_down: false,
                    sessions: HashMap::new(),
                }),
            }),
        }
    }

    pub fn prepare(
        &self,
        user_id: &str,
        client_name: Option<&str>,
        requested_session_id: Option<&str>,
    ) -> Result<PreparedSession, PrepareError> {
        self.cleanup_expired();
        let now = self.core.clock.now();
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.shutting_down {
            return Err(PrepareError::ShuttingDown);
        }

        if let Some(requested) = requested_session_id
            && let Some(session) = state.sessions.get(requested).cloned()
        {
            let mut inner = session
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if session.user_id == user_id
                && let SessionLifecycle::DisconnectedResumable { deadline } = inner.lifecycle
                && deadline > now
            {
                let permit = self.acquire_resume_permit()?;
                inner.generation = inner.generation.wrapping_add(1);
                let generation = inner.generation;
                inner.lifecycle = SessionLifecycle::Connecting {
                    previous_deadline: Some(deadline),
                };
                drop(inner);
                drop(state);
                return Ok(PreparedSession {
                    registry: self.clone(),
                    session,
                    generation,
                    resumed: true,
                    resume_permit: Some(permit),
                    committed: false,
                });
            }
        }

        if state.sessions.len() >= self.core.max_sessions {
            return Err(PrepareError::Full);
        }
        let id = unique_session_id(&state.sessions)?;
        let session = Arc::new(Session {
            id: id.clone(),
            user_id: user_id.to_owned(),
            client_name: client_name.map(str::to_owned),
            critical_capacity: self.critical_capacity(),
            coalesced_notify: Notify::new(),
            frame_window: Mutex::new(FrameWindow::default()),
            inner: Mutex::new(SessionInner {
                generation: 1,
                lifecycle: SessionLifecycle::Connecting {
                    previous_deadline: None,
                },
                settings: SessionSettings {
                    resuming: false,
                    timeout_seconds: DEFAULT_RESUME_TIMEOUT_SECONDS,
                },
                output: None,
                critical_backlog: VecDeque::new(),
                player_updates: BTreeMap::new(),
                stats: None,
                players: BTreeMap::new(),
            }),
        });
        state.sessions.insert(id, Arc::clone(&session));
        drop(state);
        Ok(PreparedSession {
            registry: self.clone(),
            session,
            generation: 1,
            resumed: false,
            resume_permit: None,
            committed: false,
        })
    }

    pub fn update_settings(
        &self,
        session_id: &str,
        update: SessionSettingsUpdate,
    ) -> Result<SessionSettings, UpdateError> {
        self.cleanup_expired();
        let session = {
            let state = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sessions.get(session_id).cloned()
        }
        .ok_or(UpdateError::NotFound)?;
        let mut inner = session
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(inner.lifecycle, SessionLifecycle::Connected) {
            return Err(UpdateError::NotFound);
        }
        if let Some(resuming) = update.resuming {
            inner.settings.resuming = resuming;
        }
        if let Some(timeout_seconds) = update.timeout_seconds {
            inner.settings.timeout_seconds = timeout_seconds;
        }
        Ok(inner.settings)
    }

    pub fn get_or_add_player(
        &self,
        session_id: &str,
        guild_id: impl Into<String>,
        player: Arc<dyn SessionPlayer>,
    ) -> Result<Arc<dyn SessionPlayer>, PlayerAdmissionError> {
        let session = self
            .session(session_id)
            .ok_or(PlayerAdmissionError::SessionNotFound)?;
        let guild_id = guild_id.into();
        let mut inner = session
            .0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(inner.lifecycle, SessionLifecycle::Connected) {
            return Err(PlayerAdmissionError::SessionNotFound);
        }
        if let Some(existing) = inner.players.get(&guild_id) {
            return Ok(Arc::clone(&existing.player));
        }
        if inner.players.len() >= self.core.max_players_per_session {
            return Err(PlayerAdmissionError::SessionFull);
        }
        self.acquire_player_slot()?;
        inner.players.insert(
            guild_id,
            PlayerEntry {
                player: Arc::clone(&player),
                total: Arc::clone(&self.core.player_count),
            },
        );
        Ok(player)
    }

    pub fn remove_player(
        &self,
        session_id: &str,
        guild_id: &str,
    ) -> Result<(), PlayerAdmissionError> {
        let session = self
            .session(session_id)
            .ok_or(PlayerAdmissionError::SessionNotFound)?;
        let entry = {
            let mut inner = session
                .0
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !matches!(inner.lifecycle, SessionLifecycle::Connected) {
                return Err(PlayerAdmissionError::SessionNotFound);
            }
            inner.player_updates.remove(guild_id);
            inner.players.remove(guild_id)
        };
        if let Some(entry) = entry {
            entry.player.shutdown();
        }
        Ok(())
    }

    #[must_use]
    pub fn session(&self, session_id: &str) -> Option<SessionHandle> {
        self.core
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .get(session_id)
            .cloned()
            .map(SessionHandle)
    }

    pub fn cleanup_expired(&self) {
        let now = self.core.clock.now();
        let removed = {
            let mut state = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let expired = state
                .sessions
                .iter()
                .filter_map(|(id, session)| {
                    let inner = session
                        .inner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match inner.lifecycle {
                        SessionLifecycle::DisconnectedResumable { deadline } if deadline <= now => {
                            Some(id.clone())
                        }
                        SessionLifecycle::Expired => Some(id.clone()),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>();
            expired
                .into_iter()
                .filter_map(|id| state.sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        for session in removed {
            shutdown_session(&session);
        }
    }

    pub fn shutdown(&self) {
        let sessions = {
            let mut state = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.shutting_down = true;
            state
                .sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        for session in sessions {
            shutdown_session(&session);
        }
    }

    #[must_use]
    pub fn counts(&self) -> SessionCounts {
        self.cleanup_expired();
        let state = self
            .core
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut connected = 0;
        let mut resumable = 0;
        for session in state.sessions.values() {
            match session
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .lifecycle
            {
                SessionLifecycle::Connected | SessionLifecycle::Connecting { .. } => connected += 1,
                SessionLifecycle::DisconnectedResumable { .. } => resumable += 1,
                SessionLifecycle::Expired | SessionLifecycle::ShuttingDown => {}
            }
        }
        SessionCounts {
            connected,
            resumable,
            players: self.core.player_count.load(Ordering::Acquire),
        }
    }

    #[must_use]
    pub fn player_stats(&self) -> Vec<SessionPlayerStats> {
        self.cleanup_expired();
        let players = {
            let state = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .sessions
                .values()
                .flat_map(|session| {
                    session
                        .inner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .players
                        .values()
                        .map(|entry| Arc::clone(&entry.player))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        players.into_iter().map(|player| player.stats()).collect()
    }

    fn disconnect(&self, session: &Arc<Session>, generation: u64) {
        let now = self.core.clock.now();
        let remove = {
            let mut inner = session
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if inner.generation != generation
                || !matches!(inner.lifecycle, SessionLifecycle::Connected)
            {
                return;
            }
            inner.output = None;
            if inner.settings.resuming {
                let duration = u64::try_from(inner.settings.timeout_seconds)
                    .map_or(Duration::ZERO, Duration::from_secs);
                inner.lifecycle = SessionLifecycle::DisconnectedResumable {
                    deadline: now.saturating_add(duration),
                };
                false
            } else {
                inner.lifecycle = SessionLifecycle::Expired;
                true
            }
        };
        if remove {
            let removed = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .sessions
                .remove(&session.id);
            if let Some(removed) = removed {
                shutdown_session(&removed);
            }
        }
    }

    fn abandon(&self, session: &Arc<Session>, generation: u64) {
        let now = self.core.clock.now();
        let remove = {
            let mut inner = session
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if inner.generation != generation {
                return;
            }
            let SessionLifecycle::Connecting { previous_deadline } = inner.lifecycle else {
                return;
            };
            if let Some(deadline) = previous_deadline
                && deadline > now
            {
                inner.lifecycle = SessionLifecycle::DisconnectedResumable { deadline };
                false
            } else {
                inner.lifecycle = SessionLifecycle::Expired;
                true
            }
        };
        if remove {
            let removed = self
                .core
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .sessions
                .remove(&session.id);
            if let Some(removed) = removed {
                shutdown_session(&removed);
            }
        }
    }

    fn acquire_resume_permit(&self) -> Result<ResumePermit, PrepareError> {
        self.core
            .resume_in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.core.max_concurrent_resumes).then_some(active + 1)
            })
            .map_err(|_| PrepareError::ResumeOverloaded)?;
        Ok(ResumePermit(Arc::clone(&self.core)))
    }

    fn acquire_player_slot(&self) -> Result<(), PlayerAdmissionError> {
        self.core
            .player_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.core.max_players).then_some(active + 1)
            })
            .map(|_| ())
            .map_err(|_| PlayerAdmissionError::Full)
    }

    fn critical_capacity(&self) -> usize {
        self.core.critical_capacity
    }
}

impl PreparedSession {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.session.id
    }

    #[must_use]
    pub const fn resumed(&self) -> bool {
        self.resumed
    }

    pub fn attach(mut self) -> Result<AttachedSession, AttachError> {
        let (critical_tx, critical_rx) = mpsc::channel(self.session.critical_capacity);
        let cancellation = CancellationToken::new();
        let (initial_critical, initial_state) = {
            let mut inner = self
                .session
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if inner.generation != self.generation
                || !matches!(inner.lifecycle, SessionLifecycle::Connecting { .. })
            {
                return Err(AttachError);
            }
            inner.lifecycle = SessionLifecycle::Connected;
            inner.output = Some(ConnectionOutput {
                critical: critical_tx,
                cancellation: cancellation.clone(),
            });
            let critical = inner.critical_backlog.drain(..).collect::<Vec<_>>();
            let mut state = if self.resumed {
                // Player snapshots are the canonical latest state on resume;
                // queued coalesced updates describe the same guilds and would
                // otherwise duplicate those snapshots.
                inner.player_updates.clear();
                inner
                    .players
                    .values()
                    .map(|entry| entry.player.snapshot())
                    .collect::<Vec<_>>()
            } else {
                let updates = inner.player_updates.values().cloned().collect::<Vec<_>>();
                inner.player_updates.clear();
                updates
            };
            if let Some(stats) = inner.stats.take() {
                state.push(stats);
            }
            (critical, state)
        };
        self.committed = true;
        self.resume_permit.take();
        Ok(AttachedSession {
            registry: self.registry.clone(),
            session: Arc::clone(&self.session),
            generation: self.generation,
            resumed: self.resumed,
            critical: Some(critical_rx),
            initial_critical,
            initial_state,
            cancellation,
        })
    }
}

impl Drop for PreparedSession {
    fn drop(&mut self) {
        if !self.committed {
            self.registry.abandon(&self.session, self.generation);
        }
    }
}

impl AttachedSession {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.session.id
    }

    #[must_use]
    pub const fn resumed(&self) -> bool {
        self.resumed
    }

    #[must_use]
    pub fn handle(&self) -> SessionHandle {
        SessionHandle(Arc::clone(&self.session))
    }

    pub(crate) fn take_critical_receiver(&mut self) -> mpsc::Receiver<CriticalMessage> {
        self.critical
            .take()
            .expect("critical receiver already taken")
    }

    pub fn take_initial_critical(&mut self) -> Vec<Arc<str>> {
        std::mem::take(&mut self.initial_critical)
    }

    pub fn take_initial_state(&mut self) -> Vec<Arc<str>> {
        std::mem::take(&mut self.initial_state)
    }

    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for AttachedSession {
    fn drop(&mut self) {
        self.registry.disconnect(&self.session, self.generation);
    }
}

impl SessionHandle {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.0.id
    }

    #[must_use]
    pub fn user_id(&self) -> &str {
        &self.0.user_id
    }

    #[must_use]
    pub fn client_name(&self) -> Option<&str> {
        self.0.client_name.as_deref()
    }

    #[must_use]
    pub fn player(&self, guild_id: &str) -> Option<Arc<dyn SessionPlayer>> {
        self.0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .players
            .get(guild_id)
            .map(|entry| Arc::clone(&entry.player))
    }

    #[must_use]
    pub fn players(&self) -> Vec<Arc<dyn SessionPlayer>> {
        self.0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .players
            .values()
            .map(|entry| Arc::clone(&entry.player))
            .collect()
    }

    #[must_use]
    pub fn player_stats(&self) -> Vec<(String, SessionPlayerStats)> {
        self.players()
            .into_iter()
            .map(|player| (player.guild_id().to_owned(), player.stats()))
            .collect()
    }

    pub(crate) fn frame_stats(&self) -> Option<FrameStats> {
        let players = self.player_stats();
        self.0
            .frame_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .update(players)
    }

    pub fn publish_critical(&self, payload: Arc<str>) -> Result<(), PublishError> {
        self.publish_critical_message(payload, None).map(|_| ())
    }

    pub async fn publish_critical_delivered(&self, payload: Arc<str>) -> Result<(), PublishError> {
        let (delivered, delivery) = oneshot::channel();
        let wait_for_delivery = self.publish_critical_message(payload.clone(), Some(delivered))?;
        if wait_for_delivery {
            delivery
                .await
                .map_err(|_| PublishError::Disconnected(payload))?;
        }
        Ok(())
    }

    fn publish_critical_message(
        &self,
        payload: Arc<str>,
        delivered: Option<oneshot::Sender<()>>,
    ) -> Result<bool, PublishError> {
        let mut inner = self
            .0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match inner.lifecycle {
            SessionLifecycle::Connected => {
                let output = inner.output.as_ref().expect("connected output");
                let message = CriticalMessage { payload, delivered };
                match output.critical.try_send(message) {
                    Ok(()) => Ok(true),
                    Err(mpsc::error::TrySendError::Full(message)) => {
                        output.cancellation.cancel();
                        Err(PublishError::Full(message.payload))
                    }
                    Err(mpsc::error::TrySendError::Closed(message)) => {
                        Err(PublishError::Disconnected(message.payload))
                    }
                }
            }
            SessionLifecycle::DisconnectedResumable { .. }
            | SessionLifecycle::Connecting { .. } => {
                if inner.critical_backlog.len() >= self.0.critical_capacity {
                    inner.lifecycle = SessionLifecycle::Expired;
                    Err(PublishError::Full(payload))
                } else {
                    inner.critical_backlog.push_back(payload);
                    Ok(false)
                }
            }
            SessionLifecycle::Expired | SessionLifecycle::ShuttingDown => {
                Err(PublishError::Disconnected(payload))
            }
        }
    }

    pub fn publish_player_update(
        &self,
        guild_id: &str,
        payload: Arc<str>,
    ) -> Result<bool, PublishError> {
        let mut inner = self
            .0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            inner.lifecycle,
            SessionLifecycle::Expired | SessionLifecycle::ShuttingDown
        ) {
            return Err(PublishError::Disconnected(payload));
        }
        if !inner.players.contains_key(guild_id) {
            return Err(PublishError::Disconnected(payload));
        }
        let replaced = inner
            .player_updates
            .insert(guild_id.to_owned(), payload)
            .is_some();
        drop(inner);
        self.0.coalesced_notify.notify_one();
        Ok(replaced)
    }

    pub fn publish_stats(&self, payload: Arc<str>) -> Result<bool, PublishError> {
        let mut inner = self
            .0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            inner.lifecycle,
            SessionLifecycle::Expired | SessionLifecycle::ShuttingDown
        ) {
            return Err(PublishError::Disconnected(payload));
        }
        let replaced = inner.stats.replace(payload).is_some();
        drop(inner);
        self.0.coalesced_notify.notify_one();
        Ok(replaced)
    }

    pub async fn coalesced_notified(&self) {
        self.0.coalesced_notify.notified().await;
    }

    pub fn take_coalesced(&self) -> Vec<Arc<str>> {
        let mut inner = self
            .0
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut output = inner.player_updates.values().cloned().collect::<Vec<_>>();
        inner.player_updates.clear();
        if let Some(stats) = inner.stats.take() {
            output.push(stats);
        }
        output
    }
}

fn unique_session_id(sessions: &HashMap<String, Arc<Session>>) -> Result<String, PrepareError> {
    for _ in 0..16 {
        let mut bytes = [0_u8; 12];
        getrandom::fill(&mut bytes).map_err(|_| PrepareError::IdentityGeneration)?;
        let candidate = URL_SAFE_NO_PAD.encode(bytes);
        if !sessions.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(PrepareError::IdentityGeneration)
}

fn shutdown_session(session: &Session) {
    let (cancellation, players) = {
        let mut inner = session
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.lifecycle = SessionLifecycle::ShuttingDown;
        let cancellation = inner.output.take().map(|output| output.cancellation);
        inner.critical_backlog.clear();
        inner.player_updates.clear();
        inner.stats = None;
        let players = std::mem::take(&mut inner.players);
        (cancellation, players)
    };
    if let Some(cancellation) = cancellation {
        cancellation.cancel();
    }
    for (_, entry) in players {
        entry.player.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use super::*;

    #[derive(Default)]
    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn advance(&self, duration: Duration) {
            self.0.fetch_add(
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                Ordering::AcqRel,
            );
        }
    }

    impl SessionClock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_millis(self.0.load(Ordering::Acquire))
        }
    }

    struct FakePlayer {
        shutdown: Arc<AtomicBool>,
    }

    impl SessionPlayer for FakePlayer {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn guild_id(&self) -> &str {
            "7"
        }

        fn wants_periodic_updates(&self) -> bool {
            true
        }

        fn stats(&self) -> SessionPlayerStats {
            SessionPlayerStats::default()
        }

        fn snapshot(&self) -> Arc<str> {
            Arc::from(r#"{"op":"playerUpdate","guildId":"7"}"#)
        }

        fn refresh(&self) -> SessionPlayerRefresh {
            let snapshot = self.snapshot();
            Box::pin(async move { ("7".to_owned(), snapshot) })
        }

        fn shutdown(&self) {
            self.shutdown.store(true, Ordering::Release);
        }
    }

    fn registry(clock: Arc<ManualClock>) -> SessionRegistry {
        SessionRegistry::new(2, 1, 1, 1, 2, clock)
    }

    #[test]
    fn creation_update_resume_and_player_survival_are_owned() {
        let clock = Arc::new(ManualClock::default());
        let registry = registry(clock);
        let prepared = registry.prepare("user", Some("client"), None).unwrap();
        let id = prepared.id().to_owned();
        assert!(!prepared.resumed());
        let connection = prepared.attach().unwrap();
        let handle = connection.handle();
        assert_eq!(handle.user_id(), "user");
        assert_eq!(handle.client_name(), Some("client"));
        assert_eq!(
            registry.update_settings(&id, SessionSettingsUpdate::default()),
            Ok(SessionSettings {
                resuming: false,
                timeout_seconds: 60,
            })
        );
        registry
            .update_settings(
                &id,
                SessionSettingsUpdate {
                    resuming: Some(true),
                    timeout_seconds: Some(15),
                },
            )
            .unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        registry
            .get_or_add_player(
                &id,
                "7",
                Arc::new(FakePlayer {
                    shutdown: Arc::clone(&stopped),
                }),
            )
            .unwrap();
        drop(connection);
        assert!(!stopped.load(Ordering::Acquire));
        assert_eq!(registry.counts().resumable, 1);

        let resumed = registry.prepare("user", None, Some(&id)).unwrap();
        assert!(resumed.resumed());
        assert_eq!(resumed.id(), id);
        let mut resumed = resumed.attach().unwrap();
        assert_eq!(resumed.take_initial_state().len(), 1);
        registry
            .update_settings(
                &id,
                SessionSettingsUpdate {
                    resuming: Some(false),
                    timeout_seconds: None,
                },
            )
            .unwrap();
        drop(resumed);
        assert!(stopped.load(Ordering::Acquire));
        assert!(registry.session(&id).is_none());
    }

    #[test]
    fn expiry_cross_user_and_simultaneous_attempts_cannot_steal_a_session() {
        let clock = Arc::new(ManualClock::default());
        let registry = registry(Arc::clone(&clock));
        let first = registry.prepare("owner", None, None).unwrap();
        let id = first.id().to_owned();
        let first = first.attach().unwrap();
        registry
            .update_settings(
                &id,
                SessionSettingsUpdate {
                    resuming: Some(true),
                    timeout_seconds: Some(1),
                },
            )
            .unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        registry
            .get_or_add_player(
                &id,
                "7",
                Arc::new(FakePlayer {
                    shutdown: Arc::clone(&stopped),
                }),
            )
            .unwrap();
        drop(first);

        let attacker = registry.prepare("attacker", None, Some(&id)).unwrap();
        assert!(!attacker.resumed());
        assert_ne!(attacker.id(), id);
        drop(attacker);

        let owner = registry.prepare("owner", None, Some(&id)).unwrap();
        assert!(owner.resumed());
        let simultaneous = registry.prepare("owner", None, Some(&id)).unwrap();
        assert!(!simultaneous.resumed());
        assert_ne!(simultaneous.id(), id);
        drop(simultaneous);
        drop(owner);

        clock.advance(Duration::from_secs(2));
        registry.cleanup_expired();
        assert!(registry.session(&id).is_none());
        assert!(stopped.load(Ordering::Acquire));
    }

    #[test]
    fn per_session_player_admission_recovers_without_consuming_global_capacity() {
        let registry = SessionRegistry::new(2, 2, 1, 1, 2, Arc::new(ManualClock::default()));
        let first = registry.prepare("one", None, None).unwrap();
        let first_id = first.id().to_owned();
        let first = first.attach().unwrap();
        let second = registry.prepare("two", None, None).unwrap();
        let second_id = second.id().to_owned();
        let _second = second.attach().unwrap();
        let player = || {
            Arc::new(FakePlayer {
                shutdown: Arc::new(AtomicBool::new(false)),
            }) as Arc<dyn SessionPlayer>
        };

        registry
            .get_or_add_player(&first_id, "1", player())
            .unwrap();
        assert!(matches!(
            registry.get_or_add_player(&first_id, "2", player()),
            Err(PlayerAdmissionError::SessionFull)
        ));
        registry
            .get_or_add_player(&second_id, "3", player())
            .unwrap();
        assert_eq!(registry.counts().players, 2);

        registry.remove_player(&first_id, "1").unwrap();
        registry
            .get_or_add_player(&first_id, "2", player())
            .unwrap();
        assert_eq!(registry.counts().players, 2);
        drop(first);
    }

    #[test]
    fn session_debug_representations_never_disclose_session_ids() {
        let registry = registry(Arc::new(ManualClock::default()));
        let prepared = registry.prepare("user", None, None).unwrap();
        let id = prepared.id().to_owned();
        assert!(!format!("{prepared:?}").contains(&id));
        let attached = prepared.attach().unwrap();
        assert!(!format!("{attached:?}").contains(&id));
        assert!(!format!("{:?}", attached.handle()).contains(&id));
    }

    #[test]
    fn thousand_slow_session_backlogs_saturate_at_fixed_bounds_and_recover() {
        const SESSIONS: usize = 1_024;
        let registry =
            SessionRegistry::new(SESSIONS, 1, 1, 64, 1, Arc::new(ManualClock::default()));
        let mut attached = Vec::with_capacity(SESSIONS);
        for index in 0..SESSIONS {
            let prepared = registry
                .prepare(&format!("user-{index}"), None, None)
                .unwrap();
            let connection = prepared.attach().unwrap();
            let handle = connection.handle();
            assert!(handle.publish_critical(Arc::from("one")).is_ok());
            assert!(matches!(
                handle.publish_critical(Arc::from("two")),
                Err(PublishError::Full(_))
            ));
            attached.push(connection);
        }
        assert_eq!(
            registry.prepare("overflow", None, None).unwrap_err(),
            PrepareError::Full
        );

        drop(attached.pop());
        assert!(registry.prepare("recovered", None, None).is_ok());
    }

    #[test]
    fn critical_backlog_is_bounded_and_state_updates_coalesce() {
        let clock = Arc::new(ManualClock::default());
        let registry = registry(clock);
        let prepared = registry.prepare("user", None, None).unwrap();
        let id = prepared.id().to_owned();
        let connection = prepared.attach().unwrap();
        registry
            .update_settings(
                &id,
                SessionSettingsUpdate {
                    resuming: Some(true),
                    timeout_seconds: None,
                },
            )
            .unwrap();
        let handle = connection.handle();
        drop(connection);
        assert_eq!(handle.publish_stats(Arc::from("old")), Ok(false));
        assert_eq!(handle.publish_stats(Arc::from("new")), Ok(true));
        assert!(handle.publish_critical(Arc::from("one")).is_ok());
        assert!(handle.publish_critical(Arc::from("two")).is_ok());
        let mut resumed = registry
            .prepare("user", None, Some(&id))
            .unwrap()
            .attach()
            .unwrap();
        assert_eq!(
            resumed.take_initial_critical(),
            vec![Arc::from("one"), Arc::from("two")]
        );
        assert_eq!(resumed.take_initial_state(), vec![Arc::from("new")]);
        drop(resumed);
        assert!(handle.publish_critical(Arc::from("one")).is_ok());
        assert!(handle.publish_critical(Arc::from("two")).is_ok());
        assert!(matches!(
            handle.publish_critical(Arc::from("three")),
            Err(PublishError::Full(_))
        ));
        assert!(handle.publish_stats(Arc::from("old")).is_err());
    }

    #[tokio::test]
    async fn acknowledged_critical_publish_waits_for_websocket_delivery() {
        let registry = registry(Arc::new(ManualClock::default()));
        let prepared = registry.prepare("user", None, None).unwrap();
        let mut connection = prepared.attach().unwrap();
        let handle = connection.handle();
        let mut receiver = connection.take_critical_receiver();
        let publisher = tokio::spawn(async move {
            handle
                .publish_critical_delivered(Arc::from("track-start"))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!publisher.is_finished());
        let message = receiver.recv().await.unwrap();
        assert_eq!(&*message.payload, "track-start");
        assert!(!publisher.is_finished());
        message.delivered.unwrap().send(()).unwrap();
        assert_eq!(publisher.await.unwrap(), Ok(()));
    }

    #[test]
    fn concurrent_resume_admission_is_bounded_without_mutating_the_second_session() {
        let clock = Arc::new(ManualClock::default());
        let registry = SessionRegistry::new(3, 1, 1, 1, 2, clock);
        let mut ids = Vec::new();
        for user in ["one", "two"] {
            let prepared = registry.prepare(user, None, None).unwrap();
            let id = prepared.id().to_owned();
            let connection = prepared.attach().unwrap();
            registry
                .update_settings(
                    &id,
                    SessionSettingsUpdate {
                        resuming: Some(true),
                        timeout_seconds: None,
                    },
                )
                .unwrap();
            drop(connection);
            ids.push((user, id));
        }

        let first = registry.prepare(ids[0].0, None, Some(&ids[0].1)).unwrap();
        assert!(first.resumed());
        assert_eq!(
            registry
                .prepare(ids[1].0, None, Some(&ids[1].1))
                .unwrap_err(),
            PrepareError::ResumeOverloaded
        );
        drop(first);
        assert!(
            registry
                .prepare(ids[1].0, None, Some(&ids[1].1))
                .unwrap()
                .resumed()
        );
    }

    #[test]
    fn shutdown_invalidates_an_in_flight_handshake_without_panicking() {
        let clock = Arc::new(ManualClock::default());
        let registry = registry(clock);
        let prepared = registry.prepare("user", None, None).unwrap();
        registry.shutdown();
        assert!(prepared.attach().is_err());
        assert_eq!(
            registry.prepare("user", None, None).unwrap_err(),
            PrepareError::ShuttingDown
        );
        assert_eq!(
            registry.counts(),
            SessionCounts {
                connected: 0,
                resumable: 0,
                players: 0,
            }
        );
    }
}
