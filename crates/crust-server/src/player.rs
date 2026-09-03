//! Fixed-shard single-writer player execution selected by ADR-0001.

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use crust::filters::{
    ChannelMix as RuntimeChannelMix, Distortion as RuntimeDistortion, FilterConfiguration,
    Karaoke as RuntimeKaraoke, Modulation, Timescale as RuntimeTimescale,
};
use crust::media::{
    AdapterErrorKind, EncodedTrack, LoadOutcome, LoadRequest, MantleAdapter, MantlePlayer,
    MediaEvent, MediaTrack, PlayerStatus, SourceRoute, TrackEndReason,
};
use crust_protocol::{Filters, JsonObject, PatchField, PlayerUpdate, VoiceState};
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::session::{PublishError, SessionHandle, SessionPlayer};
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
    senders: Vec<mpsc::Sender<PlayerCommand>>,
    adapter: Option<Arc<dyn MantleAdapter>>,
    cancellation: CancellationToken,
    workers: Mutex<Option<Vec<JoinHandle<()>>>>,
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
    state: tokio::sync::Mutex<PlayerState>,
    cached_update: Mutex<Arc<str>>,
    cancellation: CancellationToken,
}

struct PlayerState {
    mantle: Option<Arc<dyn MantlePlayer>>,
    track: Option<ActiveTrack>,
    volume: i32,
    paused: bool,
    position_ms: u64,
    end_time_ms: Option<u64>,
    filters: Value,
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
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
}

impl PlayerExecutor {
    #[must_use]
    pub fn new(
        shard_count: usize,
        command_capacity: usize,
        adapter: Option<Arc<dyn MantleAdapter>>,
    ) -> Self {
        assert!(shard_count > 0);
        assert!(command_capacity > 0);
        let cancellation = CancellationToken::new();
        let mut senders = Vec::with_capacity(shard_count);
        let mut workers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (sender, receiver) = mpsc::channel(command_capacity);
            let worker_cancellation = cancellation.clone();
            let worker_adapter = adapter.clone();
            senders.push(sender);
            workers.push(tokio::spawn(run_shard(
                receiver,
                worker_adapter,
                worker_cancellation,
                command_capacity,
            )));
        }
        Self {
            inner: Arc::new(ExecutorInner {
                senders,
                adapter,
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
        PlayerHandle::new(execution_key, guild_id, self.inner.senders[index].clone())
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
            if let Some(adapter) = &self.inner.adapter {
                let _ = adapter.shutdown().await;
            }
        }
    }
}

impl PlayerHandle {
    fn new(execution_key: String, guild_id: String, sender: mpsc::Sender<PlayerCommand>) -> Self {
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
                state: tokio::sync::Mutex::new(PlayerState::default()),
                cached_update: Mutex::new(cached_update),
                cancellation: CancellationToken::new(),
            }),
        }
    }

    #[must_use]
    pub fn guild_id(&self) -> &str {
        &self.inner.guild_id
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

    pub async fn destroy(&self) -> Result<(), PlayerError> {
        let (reply, response) = oneshot::channel();
        self.send(PlayerCommand::Destroy {
            handle: self.clone(),
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
}

impl SessionPlayer for PlayerHandle {
    fn as_any(&self) -> &dyn Any {
        self
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

    fn shutdown(&self) {
        self.inner.cancellation.cancel();
    }
}

impl Default for PlayerState {
    fn default() -> Self {
        Self {
            mantle: None,
            track: None,
            volume: 100,
            paused: false,
            position_ms: 0,
            end_time_ms: None,
            filters: json!({}),
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
    adapter: Option<Arc<dyn MantleAdapter>>,
    cancellation: CancellationToken,
    max_queued: usize,
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
                    executing.push(execute_command(command, adapter.clone()));
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
                    executing.push(execute_command(command, adapter.clone()));
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
            | Self::Destroy { handle, .. } => &handle.inner.execution_key,
        }
    }
}

fn execute_command(
    command: PlayerCommand,
    adapter: Option<Arc<dyn MantleAdapter>>,
) -> BoxFuture<'static, String> {
    let execution_key = command.execution_key().to_owned();
    async move {
        execute(command, adapter).await;
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
    }
}

async fn execute(command: PlayerCommand, adapter: Option<Arc<dyn MantleAdapter>>) {
    match command {
        PlayerCommand::Apply {
            handle,
            session,
            update,
            no_replace,
            reply,
        } => {
            let result =
                apply_update(&handle, &session, *update, no_replace, adapter.as_ref()).await;
            let _ = reply.send(result);
        }
        PlayerCommand::Snapshot { handle, reply } => {
            let result = snapshot(&handle).await;
            let _ = reply.send(result);
        }
        PlayerCommand::Destroy { handle, reply } => {
            let result = destroy(&handle).await;
            let _ = reply.send(result);
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

async fn apply_update(
    handle: &PlayerHandle,
    session: &SessionHandle,
    update: PlayerUpdate,
    no_replace: bool,
    adapter: Option<&Arc<dyn MantleAdapter>>,
) -> Result<Value, PlayerError> {
    validate_update(&update)?;
    let mut state = handle.inner.state.lock().await;
    if state.destroyed || handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    if state.mantle.is_none()
        && let Some(adapter) = adapter
    {
        state.mantle = Some(
            adapter
                .create_player(handle.inner.cancellation.child_token())
                .await
                .map_err(map_adapter_error)?,
        );
    }

    if let PatchField::Value(volume) = update.volume {
        state.volume = volume;
    }
    if let PatchField::Value(filters) = &update.filters {
        let (wire, configuration) = normalize_filters(filters)?;
        if let Some(mantle) = &state.mantle {
            mantle
                .set_filters(configuration, handle.inner.cancellation.child_token())
                .await
                .map_err(map_adapter_error)?;
        }
        state.filters = wire;
    }
    if let PatchField::Value(voice) = &update.voice {
        state.voice = voice.clone();
    }
    let (track_request, user_data) = normalized_track(&update)?;
    let replacing = !matches!(track_request, TrackRequest::Omitted);
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

    if replacing && !(no_replace && state.track.is_some()) {
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
                state.position_ms = 0;
                state.end_time_ms = None;
                state.paused = false;
                drain_events(handle, session, &state.mantle, &user_data, None).await?;
            }
            TrackRequest::Encoded(encoded) => {
                let adapter = adapter.ok_or(PlayerError::Invalid("invalid encoded track"))?;
                let track = adapter
                    .decode(
                        EncodedTrack::new(encoded),
                        handle.inner.cancellation.child_token(),
                    )
                    .await
                    .map_err(map_adapter_error)?;
                play_track(handle, session, &mut state, track, user_data, &update).await?;
            }
            TrackRequest::Identifier(identifier) => {
                let adapter =
                    adapter.ok_or(PlayerError::Invalid("identifier loading is not configured"))?;
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
                play_track(handle, session, &mut state, track, user_data, &update).await?;
            }
        }
    }

    refresh_from_mantle(handle, &mut state).await?;
    let value = player_value(&handle.inner.guild_id, &state);
    cache_update(handle, &state);
    if publish_player_update {
        let payload = SessionPlayer::snapshot(handle);
        session
            .publish_player_update(handle.guild_id(), payload)
            .map_err(map_publish_error)?;
    }
    Ok(value)
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
    let wire =
        serde_json::to_value(normalized).map_err(|_| PlayerError::Invalid("invalid filters"))?;
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
) -> Result<(), PlayerError> {
    let Some(mantle) = mantle else {
        return Ok(());
    };
    while let Some(event) = mantle
        .next_event(handle.inner.cancellation.child_token())
        .await
        .map_err(map_adapter_error)?
    {
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

async fn snapshot(handle: &PlayerHandle) -> Result<Value, PlayerError> {
    let mut state = handle.inner.state.lock().await;
    if state.destroyed || handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    refresh_from_mantle(handle, &mut state).await?;
    let value = player_value(&handle.inner.guild_id, &state);
    cache_update(handle, &state);
    Ok(value)
}

async fn destroy(handle: &PlayerHandle) -> Result<(), PlayerError> {
    let mut state = handle.inner.state.lock().await;
    if state.destroyed {
        return Ok(());
    }
    handle.inner.cancellation.cancel();
    if let Some(mantle) = state.mantle.take() {
        mantle.shutdown().await.map_err(map_adapter_error)?;
    }
    state.destroyed = true;
    state.track = None;
    Ok(())
}

async fn refresh_from_mantle(
    handle: &PlayerHandle,
    state: &mut PlayerState,
) -> Result<(), PlayerError> {
    let Some(mantle) = &state.mantle else {
        return Ok(());
    };
    let snapshot = mantle.snapshot().await.map_err(map_adapter_error)?;
    state.position_ms = snapshot.position_ms;
    state.paused = snapshot.status == PlayerStatus::Paused;
    if snapshot.track.is_none()
        && matches!(snapshot.status, PlayerStatus::Idle | PlayerStatus::Stopped)
    {
        state.track = None;
    }
    if handle.inner.cancellation.is_cancelled() {
        return Err(PlayerError::NotFound);
    }
    Ok(())
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

fn player_value(guild_id: &str, state: &PlayerState) -> Value {
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
            "connected": false,
            "ping": -1,
        },
        "voice": state.voice,
        "filters": state.filters,
    })
}

fn cache_update(handle: &PlayerHandle, state: &PlayerState) {
    let update: Arc<str> = Arc::from(
        json!({
            "op": "playerUpdate",
            "guildId": handle.inner.guild_id,
            "state": {
                "time": timestamp_ms(),
                "position": state.position_ms,
                "connected": false,
                "ping": -1,
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
                TrackEndReason::Stopped => "stopped",
                TrackEndReason::Replaced => "replaced",
            },
        }),
        MediaEvent::TrackError { track, message } => json!({
            "op": "event",
            "type": "TrackExceptionEvent",
            "guildId": guild_id,
            "track": track_value(&track, 0, user_data),
            "exception": {"message": message, "severity": "fault", "cause": null},
        }),
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
