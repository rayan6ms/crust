//! The sole Crust crate that depends on Mantle's detailed source/media APIs.

use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use crust::filters::FilterConfiguration;
use crust::media::{
    AdapterError, AdapterErrorKind, AdapterFuture, EncodedTrack, FrameFormat, LoadOutcome,
    LoadRequest, MantleAdapter, MantlePlayer, MediaEvent, MediaFrame, MediaTrack, PlayerSnapshot,
    PlayerStatus, PlaylistInfo, ProcessingMode, TrackEndReason, TrackMetadata,
};
use crust::routeplanner::{RouteEntry, RouteOutcome, RoutePlanner};
use crust::voice::OpusPacket;
use mantle_audio::EncodedFrameSlot;
use mantle_core::{
    DecodedSourceTrack, LoadedSourceItem, SerializationLimits, SourceCancellation, SourceLoad,
    SourceManager, SourceReference, SourceRegistry, SourceRegistryError, SourceRegistryLimits,
    TrackInfo, decode_source_track, encode_source_track,
};
use mantle_media::{
    HttpRangeOptions, MediaCancellation, MediaLimits, OutboundRoute, OutboundRouteContext,
    OutboundRouteOutcome, OutboundRoutePolicy, RemoteHttpOptions, StagedPlaybackInput,
    YoutubeAudioSourceManager, YoutubeAuthentication, YoutubeErrorKind, YoutubeLivePlaybackOptions,
    YoutubeLivePlaybackPoll, YoutubeLivePlaybackSession, YoutubePlaybackError,
    YoutubePlaybackErrorKind, YoutubePlaybackFormatKind, YoutubePlaybackMode,
    YoutubePlaybackSession, YoutubeSourceItem, YoutubeSourceOptions, YoutubeSourceTrack,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod filters;

use filters::CrustFilterFactory;

const PLAYER_COMMAND_CAPACITY: usize = 32;
const PLAYER_EVENT_CAPACITY: usize = 32;
const FRAME_DURATION_MS: u16 = 20;
// 320 ms of encoded audio absorbs finite-source range-request jitter. Full
// means stop reading; never drop frames. Allocate only for active playback.
const MEDIA_READ_AHEAD_FRAMES: usize = 16;

/// Operator-facing source options mapped by the Crust server into Mantle's
/// validated YouTube and HTTP policy. Codec and DSP settings remain Mantle-owned.
#[derive(Clone, Copy, Debug)]
pub struct MantleAdapterOptions {
    /// Stage compressed objects up to this size; zero disables it, maximum 64 MiB.
    /// Larger objects and live sources continue streaming.
    pub staging_max_bytes: u64,
    pub allow_youtube_search: bool,
    pub max_playlist_pages: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for MantleAdapterOptions {
    fn default() -> Self {
        Self {
            staging_max_bytes: 0,
            allow_youtube_search: true,
            max_playlist_pages: 6,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

thread_local! {
    static THREAD_ROUTE: Cell<Option<RouteEntry>> = const { Cell::new(None) };
}

/// Mantle policy backed by the same state used by Crust RoutePlanner operations.
#[derive(Clone)]
pub struct CrustOutboundRoutePolicy {
    planner: RoutePlanner,
}

impl CrustOutboundRoutePolicy {
    #[must_use]
    pub const fn new(planner: RoutePlanner) -> Self {
        Self { planner }
    }

    fn begin_operation(&self) {
        THREAD_ROUTE.with(|route| route.set(None));
    }

    fn report_source(&self, outcome: RouteOutcome) {
        THREAD_ROUTE.with(|route| {
            if let Some(route) = route.take() {
                self.planner.report(route, outcome);
            }
        });
    }
}

impl fmt::Debug for CrustOutboundRoutePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrustOutboundRoutePolicy")
            .finish_non_exhaustive()
    }
}

impl OutboundRoutePolicy for CrustOutboundRoutePolicy {
    fn select_route(&self, context: OutboundRouteContext<'_>) -> Option<OutboundRoute> {
        let route = self.planner.select_for_authority(context.authority)?;
        THREAD_ROUTE.with(|selected| selected.set(Some(route)));
        Some(OutboundRoute {
            local_ip: route.local_address,
            identity: route.identity,
        })
    }

    fn report_outcome(&self, route: OutboundRoute, outcome: OutboundRouteOutcome) {
        let outcome = match outcome {
            OutboundRouteOutcome::ConnectionEstablished => RouteOutcome::ConnectionEstablished,
            OutboundRouteOutcome::DestinationDenied => RouteOutcome::DestinationDenied,
            OutboundRouteOutcome::Timeout => RouteOutcome::Timeout,
            OutboundRouteOutcome::TransportFailure => RouteOutcome::TransportFailure,
        };
        self.planner.report(
            RouteEntry {
                identity: route.identity,
                local_address: route.local_ip,
            },
            outcome,
        );
    }
}

struct SharedYoutubeManager(Arc<YoutubeAudioSourceManager>);

impl SourceManager<YoutubeSourceItem> for SharedYoutubeManager {
    fn source_name(&self) -> &str {
        "youtube"
    }

    fn load(
        &self,
        reference: &SourceReference,
    ) -> Result<Option<SourceLoad<YoutubeSourceItem>>, SourceRegistryError> {
        self.0.load(reference)
    }

    fn load_with_cancellation(
        &self,
        reference: &SourceReference,
        cancellation: &SourceCancellation,
    ) -> Result<Option<SourceLoad<YoutubeSourceItem>>, SourceRegistryError> {
        self.0.load_with_cancellation(reference, cancellation)
    }

    fn is_encodable(&self, item: &YoutubeSourceItem) -> bool {
        self.0.is_encodable(item)
    }

    fn encode(&self, item: &YoutubeSourceItem) -> Result<Vec<u8>, SourceRegistryError> {
        self.0.encode(item)
    }

    fn decode(&self, payload: &[u8]) -> Result<YoutubeSourceItem, SourceRegistryError> {
        self.0.decode(payload)
    }

    fn decode_with_info(
        &self,
        info: &TrackInfo,
        payload: &[u8],
    ) -> Result<YoutubeSourceItem, SourceRegistryError> {
        self.0.decode_with_info(info, payload)
    }

    fn shutdown(&self) {
        self.0.shutdown();
    }
}

struct AdapterInner {
    staging_max_bytes: u64,
    manager: Arc<YoutubeAudioSourceManager>,
    registry: Arc<SourceRegistry<YoutubeSourceItem>>,
    route_policy: Arc<CrustOutboundRoutePolicy>,
    shutdown: CancellationToken,
    players: Mutex<Vec<Weak<RealMantlePlayer>>>,
}

/// Real production adapter. All source HTTP, decode, filtering, and Opus work stays in Mantle.
#[derive(Clone)]
pub struct RealMantleAdapter {
    inner: Arc<AdapterInner>,
}

impl RealMantleAdapter {
    /// Creates the production adapter with Mantle's default YouTube options.
    pub fn with_defaults(planner: RoutePlanner) -> Result<Self, AdapterError> {
        Self::with_options(planner, MantleAdapterOptions::default())
    }

    /// Creates the production adapter with the bounded, typed source policy
    /// selected by Crust configuration. Mantle remains the owner of codecs,
    /// resampling, filters, and playback state.
    pub fn with_options(
        planner: RoutePlanner,
        settings: MantleAdapterOptions,
    ) -> Result<Self, AdapterError> {
        if settings.staging_max_bytes > 64 * 1024 * 1024 {
            return Err(invalid_operation("source staging ceiling exceeds 64 MiB"));
        }
        let options = YoutubeSourceOptions {
            allow_search: settings.allow_youtube_search,
            max_playlist_pages: settings.max_playlist_pages,
            http: RemoteHttpOptions {
                connect_timeout: settings.connect_timeout,
                request_timeout: settings.request_timeout,
                ..RemoteHttpOptions::default()
            },
            ..YoutubeSourceOptions::default()
        };
        let mut adapter = Self::new(planner, options, YoutubeAuthentication::default())?;
        Arc::get_mut(&mut adapter.inner)
            .expect("new adapter is uniquely owned")
            .staging_max_bytes = settings.staging_max_bytes;
        Ok(adapter)
    }

    /// Creates a YouTube manager and registers it for Mantle track serialization.
    pub fn new(
        planner: RoutePlanner,
        options: YoutubeSourceOptions,
        authentication: YoutubeAuthentication,
    ) -> Result<Self, AdapterError> {
        let route_policy = Arc::new(CrustOutboundRoutePolicy::new(planner));
        // Routed clients intentionally cannot reuse a connection across route changes.
        // A disabled planner needs the ordinary pooled client, not a no-op policy.
        let manager = if route_policy.planner.is_enabled() {
            YoutubeAudioSourceManager::with_route_policy(
                options,
                authentication,
                route_policy.clone(),
            )
        } else {
            YoutubeAudioSourceManager::new(options, authentication)
        };
        let manager = Arc::new(
            manager.map_err(|_| invalid_operation("invalid Mantle YouTube configuration"))?,
        );
        let mut registry = SourceRegistry::new(SourceRegistryLimits::default());
        registry
            .register(Box::new(SharedYoutubeManager(Arc::clone(&manager))))
            .map_err(|_| invalid_operation("failed to register Mantle YouTube source"))?;
        Ok(Self {
            inner: Arc::new(AdapterInner {
                staging_max_bytes: 0,
                manager,
                registry: Arc::new(registry),
                route_policy,
                shutdown: CancellationToken::new(),
                players: Mutex::new(Vec::new()),
            }),
        })
    }

    #[must_use]
    pub fn route_policy(&self) -> Arc<CrustOutboundRoutePolicy> {
        Arc::clone(&self.inner.route_policy)
    }
}

impl MantleAdapter for RealMantleAdapter {
    fn load(
        &self,
        request: LoadRequest,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<LoadOutcome, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_tokens(&inner.shutdown, &cancellation)?;
            #[cfg(test)]
            if request.identifier.starts_with("fixture:") {
                return fixture_load(&request.identifier);
            }
            let registry = Arc::clone(&inner.registry);
            let route_policy = Arc::clone(&inner.route_policy);
            let identifier = request.identifier;
            let mantle_cancel = SourceCancellation::new();
            let worker_cancel = mantle_cancel.clone();
            let cancellation_watcher = tokio::spawn({
                let shutdown = inner.shutdown.clone();
                async move {
                    tokio::select! {
                        () = cancellation.cancelled() => {}
                        () = shutdown.cancelled() => {}
                    }
                    mantle_cancel.cancel();
                }
            });
            let result = tokio::task::spawn_blocking(move || {
                route_policy.begin_operation();
                let reference = SourceReference::new(Some(identifier), false);
                let result = registry.load_with_cancellation(&reference, &worker_cancel);
                match result {
                    Ok(Some(item)) => {
                        route_policy.report_source(RouteOutcome::SourceSuccess);
                        loaded_outcome(&registry, item)
                    }
                    Ok(None) if worker_cancel.is_cancelled() => Err(cancelled()),
                    Ok(None) => Ok(LoadOutcome::NoMatches),
                    Err(SourceRegistryError::Shutdown) => Err(shutdown()),
                    Err(_) => {
                        route_policy.report_source(RouteOutcome::SourceFailure);
                        Err(load_failed())
                    }
                }
            })
            .await
            .map_err(|_| invalid_operation("Mantle load worker failed"));
            cancellation_watcher.abort();
            let _ = cancellation_watcher.await;
            result?
        })
    }

    fn decode(
        &self,
        encoded: EncodedTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<MediaTrack, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_tokens(&inner.shutdown, &cancellation)?;
            #[cfg(test)]
            if let Ok(track) = decode_fixture_track(&encoded) {
                return Ok(track);
            }
            decode_media_track(&inner.registry, encoded)
        })
    }

    fn encode(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<EncodedTrack, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_tokens(&inner.shutdown, &cancellation)?;
            #[cfg(test)]
            if track.metadata.identifier.starts_with("fixture:") {
                return encode_fixture_track(&track.metadata);
            }
            let decoded = decode_encoded(&inner.registry, &track.encoded)?;
            let info = metadata_to_info(&track.metadata);
            encode_item(&inner.registry, &info, decoded.item)
        })
    }

    fn create_player(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Arc<dyn MantlePlayer>, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_tokens(&inner.shutdown, &cancellation)?;
            let player = RealMantlePlayer::new(
                Arc::clone(&inner.manager),
                Arc::clone(&inner.registry),
                Arc::clone(&inner.route_policy),
                inner.staging_max_bytes,
                inner.shutdown.child_token(),
            );
            inner
                .players
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(Arc::downgrade(&player));
            Ok(player as Arc<dyn MantlePlayer>)
        })
    }

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let players = {
                let mut tracked = inner
                    .players
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let players = tracked.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
                tracked.clear();
                players
            };
            for player in players {
                player.shutdown().await?;
            }
            inner.shutdown.cancel();
            inner.registry.shutdown();
            Ok(())
        })
    }
}

fn loaded_outcome(
    registry: &SourceRegistry<YoutubeSourceItem>,
    loaded: LoadedSourceItem<YoutubeSourceItem>,
) -> Result<LoadOutcome, AdapterError> {
    match loaded.item {
        YoutubeSourceItem::Track(track) => Ok(LoadOutcome::Track(source_track(
            registry,
            loaded.registration,
            track,
        )?)),
        YoutubeSourceItem::Playlist(playlist) => {
            let tracks = playlist
                .tracks
                .into_iter()
                .map(|track| source_track(registry, loaded.registration, track))
                .collect::<Result<Vec<_>, _>>()?;
            if playlist.is_search_result {
                Ok(LoadOutcome::Search(tracks))
            } else {
                Ok(LoadOutcome::Playlist {
                    info: PlaylistInfo {
                        name: playlist.name,
                        selected_track: playlist.selected_track,
                    },
                    plugin_info: Default::default(),
                    tracks,
                })
            }
        }
    }
}

fn source_track(
    registry: &SourceRegistry<YoutubeSourceItem>,
    registration: mantle_core::SourceRegistrationId,
    track: YoutubeSourceTrack,
) -> Result<MediaTrack, AdapterError> {
    let encoded = encode_item(
        registry,
        &track.info,
        LoadedSourceItem {
            registration,
            item: YoutubeSourceItem::Track(track.clone()),
        },
    )?;
    Ok(MediaTrack {
        encoded,
        metadata: info_to_metadata(&track.info, "youtube"),
        plugin_info: Default::default(),
    })
}

fn encode_item(
    registry: &SourceRegistry<YoutubeSourceItem>,
    info: &TrackInfo,
    item: LoadedSourceItem<YoutubeSourceItem>,
) -> Result<EncodedTrack, AdapterError> {
    let bytes = encode_source_track(
        info,
        Duration::ZERO,
        &item,
        registry,
        SerializationLimits::default(),
    )
    .map_err(|_| invalid_track())?;
    Ok(EncodedTrack::new(BASE64.encode(bytes)))
}

fn decode_encoded(
    registry: &SourceRegistry<YoutubeSourceItem>,
    encoded: &EncodedTrack,
) -> Result<DecodedSourceTrack<YoutubeSourceItem>, AdapterError> {
    let bytes = BASE64
        .decode(encoded.as_str())
        .map_err(|_| invalid_track())?;
    decode_source_track(&bytes, registry, SerializationLimits::default())
        .map_err(|_| invalid_track())?
        .ok_or_else(invalid_track)
}

fn decode_media_track(
    registry: &SourceRegistry<YoutubeSourceItem>,
    encoded: EncodedTrack,
) -> Result<MediaTrack, AdapterError> {
    let decoded = decode_encoded(registry, &encoded)?;
    let source_name = registry
        .source_name(decoded.item.registration)
        .ok_or_else(invalid_track)?;
    Ok(MediaTrack {
        encoded,
        metadata: info_to_metadata(&decoded.info, source_name),
        plugin_info: Default::default(),
    })
}

fn info_to_metadata(info: &TrackInfo, source_name: &str) -> TrackMetadata {
    TrackMetadata {
        identifier: info.identifier.clone(),
        title: info.title.clone(),
        author: info.author.clone(),
        duration_ms: u64::try_from(info.duration.as_millis()).unwrap_or(u64::MAX),
        seekable: !info.is_stream,
        stream: info.is_stream,
        source_name: source_name.to_owned(),
        uri: info.uri.clone(),
        artwork_url: info.artwork_url.clone(),
        isrc: info.isrc.clone(),
    }
}

fn metadata_to_info(metadata: &TrackMetadata) -> TrackInfo {
    TrackInfo {
        title: metadata.title.clone(),
        author: metadata.author.clone(),
        duration: Duration::from_millis(metadata.duration_ms),
        identifier: metadata.identifier.clone(),
        is_stream: metadata.stream,
        uri: metadata.uri.clone(),
        artwork_url: metadata.artwork_url.clone(),
        isrc: metadata.isrc.clone(),
    }
}

enum PlaybackSession {
    Finite(YoutubePlaybackSession),
    Live {
        session: Box<YoutubeLivePlaybackSession>,
        now: Duration,
    },
    #[cfg(test)]
    Fixture(Box<FixtureSession>),
}

// Exactly one completed compressed input per player, without decoder/DSP state.
enum CompletedInput {
    Staged(StagedPlaybackInput),
    #[cfg(test)]
    Fixture(Box<TrackMetadata>),
}

impl CompletedInput {
    fn open(self, cancellation: CancellationToken) -> Result<PlaybackSession, AdapterError> {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        match self {
            Self::Staged(input) => input
                .open(MediaCancellation::linked(move || {
                    cancellation.is_cancelled()
                }))
                .map(PlaybackSession::Finite)
                .map_err(map_playback_error),
            #[cfg(test)]
            Self::Fixture(metadata) => FixtureSession::new(&metadata)
                .map(Box::new)
                .map(PlaybackSession::Fixture),
        }
    }
}

type EncodedPlaybackFrame = (OpusPacket, Duration);

// The frame is an inline bounded Opus packet. Boxing this variant would add a
// heap allocation to every Mantle-to-Crust frame, so the intentional size
// tradeoff is retained and covered by the P13 allocation benchmark.
#[allow(clippy::large_enum_variant)]
enum PlaybackFramePoll {
    Frame(EncodedPlaybackFrame),
    Wait(Duration),
    Ended,
}

impl PlaybackSession {
    fn mode(&self) -> ProcessingMode {
        let mode = match self {
            Self::Finite(session) => session.mode(),
            Self::Live { session, .. } => session.mode(),
            #[cfg(test)]
            Self::Fixture(session) => return session.processing,
        };
        match mode {
            YoutubePlaybackMode::OpusPassthrough => ProcessingMode::Passthrough,
            YoutubePlaybackMode::Transcode => ProcessingMode::Pcm,
        }
    }

    fn seek(&mut self, position: Duration) -> Result<Duration, AdapterError> {
        match self {
            Self::Finite(session) => session
                .seek(position)
                .map(|result| result.actual.unwrap_or(result.requested))
                .map_err(map_playback_error),
            Self::Live { .. } => Err(invalid_operation("live playback is not seekable")),
            #[cfg(test)]
            Self::Fixture(session) => session.seek(position),
        }
    }

    fn set_filters(&mut self, configuration: &FilterConfiguration) -> Result<(), AdapterError> {
        let factory = CrustFilterFactory(configuration.clone());
        let factory = configuration
            .is_effective()
            .then_some(&factory as &dyn mantle_audio::PcmFilterFactory);
        match self {
            Self::Finite(session) => session
                .set_filter_factory(factory)
                .map_err(map_playback_error),
            Self::Live { session, .. } => session
                .set_filter_factory(factory)
                .map_err(map_playback_error),
            #[cfg(test)]
            Self::Fixture(session) => {
                session.processing = if configuration.is_effective() {
                    ProcessingMode::Pcm
                } else {
                    ProcessingMode::Passthrough
                };
                Ok(())
            }
        }
    }

    fn source_media_position(&self) -> Option<Duration> {
        match self {
            Self::Finite(session) => session.source_media_position(),
            Self::Live { session, .. } => session.source_media_position(),
            #[cfg(test)]
            Self::Fixture(_) => None,
        }
    }

    fn read_frame(&mut self) -> Result<PlaybackFramePoll, AdapterError> {
        let mut output = EncodedFrameSlot::new();
        let poll = match self {
            Self::Finite(session) => {
                if session
                    .read_frame(&mut output)
                    .map_err(map_playback_error)?
                {
                    PlaybackFramePoll::Frame((
                        OpusPacket::copy_from(output.data()).map_err(|_| {
                            invalid_operation("Mantle produced an invalid Discord Opus frame")
                        })?,
                        output.timestamp().unwrap_or_default(),
                    ))
                } else {
                    PlaybackFramePoll::Ended
                }
            }
            Self::Live { session, now } => {
                let polled_at = *now;
                let poll = session
                    .poll_frame(polled_at, &mut output)
                    .map_err(map_playback_error)?;
                match poll {
                    YoutubeLivePlaybackPoll::Frame => PlaybackFramePoll::Frame((
                        OpusPacket::copy_from(output.data()).map_err(|_| {
                            invalid_operation("Mantle produced an invalid Discord Opus frame")
                        })?,
                        output.timestamp().unwrap_or_default(),
                    )),
                    YoutubeLivePlaybackPoll::WaitUntil(deadline) => {
                        *now = deadline;
                        PlaybackFramePoll::Wait(deadline.saturating_sub(polled_at))
                    }
                    YoutubeLivePlaybackPoll::Ended | YoutubeLivePlaybackPoll::Exhausted => {
                        PlaybackFramePoll::Ended
                    }
                }
            }
            #[cfg(test)]
            Self::Fixture(session) => {
                return session
                    .read_frame()
                    .map(|frame| frame.map_or(PlaybackFramePoll::Ended, PlaybackFramePoll::Frame));
            }
        };
        Ok(poll)
    }
}

// See `PlaybackFramePoll`: keeping the bounded media frame inline preserves
// the measured no-extra-media-allocation path.
#[allow(clippy::large_enum_variant)]
enum PlayerFramePoll {
    Frame(MediaFrame),
    Wait(Duration),
    Ended,
}

enum PlayerCommand {
    Play {
        track: Box<MediaTrack>,
        cancellation: CancellationToken,
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
    Pause {
        paused: bool,
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
    Seek {
        position: Duration,
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
    Stop {
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
    SetFilters {
        configuration: Box<FilterConfiguration>,
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
    Snapshot {
        reply: oneshot::Sender<Result<PlayerSnapshot, AdapterError>>,
    },
    NextFrame {
        reply: oneshot::Sender<Result<PlayerFramePoll, AdapterError>>,
    },
    NextEvent {
        reply: oneshot::Sender<Result<Option<MediaEvent>, AdapterError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), AdapterError>>,
    },
}

struct PlayerActor {
    staging_max_bytes: u64,
    completed_input: Option<(EncodedTrack, CompletedInput)>,
    manager: Arc<YoutubeAudioSourceManager>,
    registry: Arc<SourceRegistry<YoutubeSourceItem>>,
    route_policy: Arc<CrustOutboundRoutePolicy>,
    session: Option<PlaybackSession>,
    track: Option<MediaTrack>,
    status: PlayerStatus,
    paused: bool,
    position: Duration,
    sequence: u64,
    events: VecDeque<MediaEvent>,
    filters: FilterConfiguration,
    buffered: VecDeque<Result<BufferedFrame, AdapterError>>,
    // Filter changes wait for the owned read without blocking frame delivery.
    // Full rejects the new update explicitly; no accepted reply is dropped.
    pending_filters: VecDeque<FilterUpdate>,
    reading: Option<JoinHandle<ReadResult>>,
    read_ahead_halted: bool,
    processing: ProcessingMode,
}

struct BufferedFrame {
    poll: PlaybackFramePoll,
    source_position: Option<Duration>,
}

type ReadResult = (PlaybackSession, Result<BufferedFrame, AdapterError>);

struct FilterUpdate {
    configuration: Box<FilterConfiguration>,
    reply: oneshot::Sender<Result<(), AdapterError>>,
}

impl PlayerActor {
    async fn run(mut self, mut commands: mpsc::Receiver<PlayerCommand>) {
        loop {
            self.start_read_ahead();
            tokio::select! {
                biased;
                command = commands.recv() => {
                    let Some(command) = command else { break; };
                    let terminal = matches!(command, PlayerCommand::Shutdown { .. });
                    self.handle(command).await;
                    if terminal { break; }
                }
                result = async { self.reading.as_mut().expect("guarded read").await }, if self.reading.is_some() => {
                    self.reading = None;
                    self.accept_read(result);
                    self.apply_pending_filters().await;
                }
            }
        }
        self.settle_read().await;
        self.session = None;
        self.status = PlayerStatus::Shutdown;
    }

    fn start_read_ahead(&mut self) {
        if self.reading.is_some()
            || self.paused
            || self.track.is_none()
            || self.read_ahead_halted
            || self.buffered.len() >= MEDIA_READ_AHEAD_FRAMES
            || self
                .session
                .as_ref()
                .is_none_or(|s| matches!(s, PlaybackSession::Live { .. }))
        {
            return;
        }
        let mut session = self.session.take().expect("active finite session");
        self.reading = Some(tokio::task::spawn_blocking(move || {
            let result = session.read_frame().map(|poll| BufferedFrame {
                poll,
                source_position: session.source_media_position(),
            });
            (session, result)
        }));
    }

    fn accept_read(&mut self, result: Result<ReadResult, tokio::task::JoinError>) {
        let frame = match result {
            Ok((session, frame)) => {
                self.session = Some(session);
                frame
            }
            Err(_) => Err(invalid_operation("Mantle playback worker failed")),
        };
        self.read_ahead_halted = !matches!(
            &frame,
            Ok(BufferedFrame {
                poll: PlaybackFramePoll::Frame(_),
                ..
            })
        );
        self.buffered.push_back(frame);
    }

    async fn settle_read(&mut self) {
        if let Some(task) = self.reading.take() {
            self.accept_read(task.await);
        }
        self.apply_pending_filters().await;
    }

    async fn apply_pending_filters(&mut self) {
        while let Some(update) = self.pending_filters.pop_front() {
            self.apply_filters(update).await;
        }
        // This queue is used only when a control overlaps a source read.
        self.pending_filters = VecDeque::new();
    }

    async fn apply_filters(&mut self, update: FilterUpdate) {
        let result = if self.session.is_some() {
            let installed = update.configuration.clone();
            self.with_settled_session(move |session| session.set_filters(&installed))
                .await
        } else {
            Ok(())
        };
        if result.is_ok() {
            self.filters = *update.configuration;
            self.processing = self
                .session
                .as_ref()
                .map_or(ProcessingMode::Passthrough, PlaybackSession::mode);
        }
        let _ = update.reply.send(result);
    }

    fn clear_read_ahead(&mut self) {
        self.buffered.clear();
        self.read_ahead_halted = false;
    }

    async fn handle(&mut self, command: PlayerCommand) {
        match command {
            PlayerCommand::Play {
                track,
                cancellation,
                reply,
            } => {
                let result = self.play(*track, cancellation).await;
                let _ = reply.send(result);
            }
            PlayerCommand::Pause { paused, reply } => {
                let result = if self.track.is_none() {
                    Err(invalid_operation("no active track"))
                } else {
                    self.paused = paused;
                    self.status = if paused {
                        PlayerStatus::Paused
                    } else {
                        PlayerStatus::Playing
                    };
                    Ok(())
                };
                let _ = reply.send(result);
            }
            PlayerCommand::Seek { position, reply } => {
                let result = self
                    .with_session(move |session| session.seek(position))
                    .await;
                if let Ok(position) = result {
                    self.position = position;
                    self.clear_read_ahead();
                }
                let _ = reply.send(result.map(|_| ()));
            }
            PlayerCommand::Stop { reply } => {
                self.settle_read().await;
                let result = self.stop(TrackEndReason::Stopped);
                let _ = reply.send(result);
            }
            PlayerCommand::SetFilters {
                configuration,
                reply,
            } => {
                if self.reading.is_some() {
                    if self.pending_filters.len() == PLAYER_COMMAND_CAPACITY {
                        let _ = reply.send(Err(overloaded()));
                    } else {
                        self.pending_filters.push_back(FilterUpdate {
                            configuration,
                            reply,
                        });
                    }
                } else {
                    self.apply_filters(FilterUpdate {
                        configuration,
                        reply,
                    })
                    .await;
                }
            }
            PlayerCommand::Snapshot { reply } => {
                let _ = reply.send(Ok(PlayerSnapshot {
                    status: self.status,
                    track: self.track.clone(),
                    position_ms: u64::try_from(self.position.as_millis()).unwrap_or(u64::MAX),
                    processing: self.processing,
                }));
            }
            PlayerCommand::NextFrame { reply } => {
                let result = self.next_frame().await;
                let _ = reply.send(result);
            }
            PlayerCommand::NextEvent { reply } => {
                let _ = reply.send(Ok(self.events.pop_front()));
            }
            PlayerCommand::Shutdown { reply } => {
                self.settle_read().await;
                self.clear_read_ahead();
                self.session = None;
                self.completed_input = None;
                self.track = None;
                self.status = PlayerStatus::Shutdown;
                let _ = reply.send(Ok(()));
            }
        }
    }

    async fn play(
        &mut self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> Result<(), AdapterError> {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        if self.events.len().saturating_add(2) > PLAYER_EVENT_CAPACITY {
            return Err(overloaded());
        }
        self.settle_read().await;
        let manager = Arc::clone(&self.manager);
        let registry = Arc::clone(&self.registry);
        let policy = Arc::clone(&self.route_policy);
        let encoded = track.encoded.clone();
        let completed = self.completed_input.take();
        let staging_max_bytes = self.staging_max_bytes;
        let cancellation_for_open = cancellation.clone();
        let mut session = tokio::task::spawn_blocking(move || {
            if cancellation_for_open.is_cancelled() {
                return Err(cancelled());
            }
            if let Some((previous, input)) = completed
                && previous == encoded
            {
                return input.open(cancellation_for_open);
            }
            open_playback(
                manager,
                registry,
                policy,
                encoded,
                cancellation_for_open,
                staging_max_bytes,
            )
        })
        .await
        .map_err(|_| invalid_operation("Mantle playback worker failed"))??;
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        session.set_filters(&self.filters)?;
        if let Some(previous) = self.track.replace(track.clone()) {
            self.events.push_back(MediaEvent::TrackEnd {
                track: previous,
                reason: TrackEndReason::Replaced,
            });
        }
        self.events.push_back(MediaEvent::TrackStart(track.clone()));
        #[cfg(test)]
        if track.metadata.identifier == "fixture:event-error" {
            self.events.push_back(MediaEvent::TrackError {
                track: track.clone(),
                message: "synthetic playback failure".into(),
            });
            self.status = PlayerStatus::Stopped;
            self.track = None;
            self.session = None;
            return Ok(());
        }
        #[cfg(test)]
        if track.metadata.identifier == "fixture:event-stuck" {
            self.events.push_back(MediaEvent::TrackStuck {
                track: track.clone(),
                threshold_ms: 5_000,
            });
        }
        self.clear_read_ahead();
        self.buffered.reserve(MEDIA_READ_AHEAD_FRAMES);
        self.processing = session.mode();
        self.session = Some(session);
        self.paused = false;
        self.status = PlayerStatus::Playing;
        self.position = Duration::ZERO;
        self.sequence = 0;
        Ok(())
    }

    fn stop(&mut self, reason: TrackEndReason) -> Result<(), AdapterError> {
        if self.track.is_some() && self.events.len() == PLAYER_EVENT_CAPACITY {
            return Err(overloaded());
        }
        // Natural EOF retains compressed bytes only. Stop, replacement, error,
        // shutdown, and the owning player's idle cleanup release the object.
        let completed = self.session.take().and_then(|session| match session {
            PlaybackSession::Finite(session) => {
                session.into_staged_input().map(CompletedInput::Staged)
            }
            PlaybackSession::Live { .. } => None,
            #[cfg(test)]
            PlaybackSession::Fixture(_) => self
                .track
                .as_ref()
                .filter(|track| track.metadata.identifier == "fixture:staged")
                .map(|track| CompletedInput::Fixture(Box::new(track.metadata.clone()))),
        });
        self.completed_input =
            if matches!(reason, TrackEndReason::Finished) && self.staging_max_bytes != 0 {
                self.track
                    .as_ref()
                    .and_then(|track| completed.map(|input| (track.encoded.clone(), input)))
            } else {
                None
            };
        self.clear_read_ahead();
        self.buffered = VecDeque::new();
        self.processing = ProcessingMode::Passthrough;
        if let Some(track) = self.track.take() {
            self.events
                .push_back(MediaEvent::TrackEnd { track, reason });
        }
        self.status = PlayerStatus::Stopped;
        self.position = Duration::ZERO;
        Ok(())
    }

    async fn next_frame(&mut self) -> Result<PlayerFramePoll, AdapterError> {
        if self.paused || self.track.is_none() {
            return Ok(PlayerFramePoll::Ended);
        }
        if self.buffered.is_empty() {
            self.settle_read().await;
        }
        let frame = match self.buffered.pop_front() {
            Some(frame) => frame?,
            None => {
                self.with_session(|session| {
                    session.read_frame().map(|poll| BufferedFrame {
                        poll,
                        source_position: session.source_media_position(),
                    })
                })
                .await?
            }
        };
        match frame.poll {
            PlaybackFramePoll::Frame((payload, timestamp)) => {
                self.position = frame.source_position.unwrap_or(timestamp);
                let frame = MediaFrame {
                    sequence: self.sequence,
                    duration_ms: FRAME_DURATION_MS,
                    format: FrameFormat::OpusLike,
                    payload,
                };
                self.sequence = self.sequence.saturating_add(1);
                Ok(PlayerFramePoll::Frame(frame))
            }
            PlaybackFramePoll::Wait(duration) => Ok(PlayerFramePoll::Wait(duration)),
            PlaybackFramePoll::Ended => {
                self.stop(TrackEndReason::Finished)?;
                Ok(PlayerFramePoll::Ended)
            }
        }
    }

    async fn with_session<T: Send + 'static>(
        &mut self,
        operation: impl FnOnce(&mut PlaybackSession) -> Result<T, AdapterError> + Send + 'static,
    ) -> Result<T, AdapterError> {
        self.settle_read().await;
        self.with_settled_session(operation).await
    }

    async fn with_settled_session<T: Send + 'static>(
        &mut self,
        operation: impl FnOnce(&mut PlaybackSession) -> Result<T, AdapterError> + Send + 'static,
    ) -> Result<T, AdapterError> {
        let mut session = self
            .session
            .take()
            .ok_or_else(|| invalid_operation("no active track"))?;
        let (session, result) = tokio::task::spawn_blocking(move || {
            let result = operation(&mut session);
            (session, result)
        })
        .await
        .map_err(|_| invalid_operation("Mantle playback worker failed"))?;
        self.session = Some(session);
        result
    }
}

struct RealMantlePlayer {
    commands: mpsc::Sender<PlayerCommand>,
    task: Mutex<Option<JoinHandle<()>>>,
    shutdown: CancellationToken,
}

impl RealMantlePlayer {
    fn new(
        manager: Arc<YoutubeAudioSourceManager>,
        registry: Arc<SourceRegistry<YoutubeSourceItem>>,
        route_policy: Arc<CrustOutboundRoutePolicy>,
        staging_max_bytes: u64,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(PLAYER_COMMAND_CAPACITY);
        let actor = PlayerActor {
            staging_max_bytes,
            completed_input: None,
            manager,
            registry,
            route_policy,
            session: None,
            track: None,
            status: PlayerStatus::Idle,
            paused: false,
            position: Duration::ZERO,
            sequence: 0,
            events: VecDeque::with_capacity(PLAYER_EVENT_CAPACITY),
            filters: FilterConfiguration::default(),
            buffered: VecDeque::new(),
            pending_filters: VecDeque::new(),
            reading: None,
            read_ahead_halted: false,
            processing: ProcessingMode::Passthrough,
        };
        let task = tokio::spawn(actor.run(receiver));
        Arc::new(Self {
            commands,
            task: Mutex::new(Some(task)),
            shutdown,
        })
    }

    async fn request<T>(
        &self,
        cancellation: Option<&CancellationToken>,
        make: impl FnOnce(oneshot::Sender<Result<T, AdapterError>>) -> PlayerCommand,
    ) -> Result<T, AdapterError> {
        if self.shutdown.is_cancelled() {
            return Err(shutdown());
        }
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        let (reply, receive) = oneshot::channel();
        self.commands
            .try_send(make(reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => overloaded(),
                mpsc::error::TrySendError::Closed(_) => shutdown(),
            })?;
        if let Some(cancellation) = cancellation {
            tokio::select! {
                result = receive => result.map_err(|_| shutdown())?,
                () = cancellation.cancelled() => Err(cancelled()),
                () = self.shutdown.cancelled() => Err(shutdown()),
            }
        } else {
            tokio::select! {
                result = receive => result.map_err(|_| shutdown())?,
                () = self.shutdown.cancelled() => Err(shutdown()),
            }
        }
    }

    async fn finish_task(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for RealMantlePlayer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

impl MantlePlayer for RealMantlePlayer {
    fn play(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let command_cancellation = cancellation.clone();
            self.request(Some(&cancellation), |reply| PlayerCommand::Play {
                track: Box::new(track),
                cancellation: command_cancellation,
                reply,
            })
            .await
        })
    }

    fn pause(
        &self,
        paused: bool,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.request(Some(&cancellation), |reply| PlayerCommand::Pause {
                paused,
                reply,
            })
            .await
        })
    }

    fn seek(
        &self,
        position_ms: u64,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.request(Some(&cancellation), |reply| PlayerCommand::Seek {
                position: Duration::from_millis(position_ms),
                reply,
            })
            .await
        })
    }

    fn stop(&self, cancellation: CancellationToken) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.request(Some(&cancellation), |reply| PlayerCommand::Stop { reply })
                .await
        })
    }

    fn set_filters(
        &self,
        configuration: FilterConfiguration,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.request(Some(&cancellation), |reply| PlayerCommand::SetFilters {
                configuration: Box::new(configuration),
                reply,
            })
            .await
        })
    }

    fn snapshot(&self) -> AdapterFuture<'_, Result<PlayerSnapshot, AdapterError>> {
        Box::pin(async move {
            self.request(None, |reply| PlayerCommand::Snapshot { reply })
                .await
        })
    }

    fn next_frame(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaFrame>, AdapterError>> {
        Box::pin(async move {
            loop {
                let poll = self
                    .request(Some(&cancellation), |reply| PlayerCommand::NextFrame {
                        reply,
                    })
                    .await?;
                match poll {
                    PlayerFramePoll::Frame(frame) => return Ok(Some(frame)),
                    PlayerFramePoll::Ended => return Ok(None),
                    PlayerFramePoll::Wait(duration) => {
                        tokio::select! {
                            biased;
                            () = cancellation.cancelled() => return Err(cancelled()),
                            () = self.shutdown.cancelled() => return Err(shutdown()),
                            () = tokio::time::sleep(duration) => {}
                        }
                    }
                }
            }
        })
    }

    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaEvent>, AdapterError>> {
        Box::pin(async move {
            self.request(Some(&cancellation), |reply| PlayerCommand::NextEvent {
                reply,
            })
            .await
        })
    }

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            if !self.shutdown.is_cancelled() {
                let result = self
                    .request(None, |reply| PlayerCommand::Shutdown { reply })
                    .await;
                self.shutdown.cancel();
                self.finish_task().await;
                result
            } else {
                self.finish_task().await;
                Ok(())
            }
        })
    }
}

fn open_playback(
    manager: Arc<YoutubeAudioSourceManager>,
    registry: Arc<SourceRegistry<YoutubeSourceItem>>,
    route_policy: Arc<CrustOutboundRoutePolicy>,
    encoded: EncodedTrack,
    cancellation: CancellationToken,
    staging_max_bytes: u64,
) -> Result<PlaybackSession, AdapterError> {
    #[cfg(test)]
    if let Ok(track) = decode_fixture_track(&encoded) {
        return FixtureSession::new(&track.metadata)
            .map(Box::new)
            .map(PlaybackSession::Fixture);
    }
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    route_policy.begin_operation();
    let decoded = decode_encoded(&registry, &encoded)?;
    let YoutubeSourceItem::Track(track) = decoded.item.item else {
        return Err(invalid_track());
    };
    let media_cancel = MediaCancellation::linked(move || cancellation.is_cancelled());
    let formats = manager
        .discover_playback_formats(&track.info.identifier, &media_cancel)
        .map_err(|error| {
            route_policy.report_source(map_youtube_source_outcome(error.kind()));
            map_youtube_error(error.kind())
        })?;
    let opened = if formats.selected().kind() == Some(YoutubePlaybackFormatKind::HlsMpegTsAac) {
        let session = if route_policy.planner.is_enabled() {
            manager.open_selected_live_playback_routed(
                &formats,
                YoutubeLivePlaybackOptions::default(),
                media_cancel,
                route_policy.clone(),
            )
        } else {
            manager.open_selected_live_playback(
                &formats,
                YoutubeLivePlaybackOptions::default(),
                media_cancel,
            )
        };
        session.map(|session| PlaybackSession::Live {
            session: Box::new(session),
            now: Duration::ZERO,
        })
    } else {
        let session = if route_policy.planner.is_enabled() {
            manager.open_selected_playback_routed(
                &formats,
                HttpRangeOptions {
                    staging_max_bytes,
                    ..HttpRangeOptions::default()
                },
                MediaLimits::default(),
                media_cancel,
                route_policy.clone(),
            )
        } else {
            manager.open_selected_playback(
                &formats,
                HttpRangeOptions {
                    staging_max_bytes,
                    ..HttpRangeOptions::default()
                },
                MediaLimits::default(),
                media_cancel,
            )
        };
        session.map(PlaybackSession::Finite)
    };
    match opened {
        Ok(session) => {
            route_policy.report_source(RouteOutcome::SourceSuccess);
            Ok(session)
        }
        Err(error) => {
            route_policy.report_source(map_playback_source_outcome(error.kind()));
            Err(map_playback_error(error))
        }
    }
}

fn map_youtube_source_outcome(kind: YoutubeErrorKind) -> RouteOutcome {
    match kind {
        YoutubeErrorKind::RateLimited => RouteOutcome::SourceRateLimited,
        YoutubeErrorKind::Unavailable | YoutubeErrorKind::LoginRequired => {
            RouteOutcome::SourceUnavailable
        }
        _ => RouteOutcome::SourceFailure,
    }
}

fn map_playback_source_outcome(kind: YoutubePlaybackErrorKind) -> RouteOutcome {
    match kind {
        YoutubePlaybackErrorKind::Source(kind) => map_youtube_source_outcome(kind),
        _ => RouteOutcome::SourceFailure,
    }
}

fn map_youtube_error(kind: YoutubeErrorKind) -> AdapterError {
    match kind {
        YoutubeErrorKind::Cancelled => cancelled(),
        _ => load_failed(),
    }
}

fn map_playback_error(error: YoutubePlaybackError) -> AdapterError {
    match error.kind() {
        YoutubePlaybackErrorKind::Cancelled
        | YoutubePlaybackErrorKind::Source(YoutubeErrorKind::Cancelled) => cancelled(),
        YoutubePlaybackErrorKind::InvalidOptions | YoutubePlaybackErrorKind::IncompatibleFormat => {
            invalid_operation("Mantle rejected the playback operation")
        }
        _ => load_failed(),
    }
}

fn check_tokens(
    shutdown_token: &CancellationToken,
    cancellation: &CancellationToken,
) -> Result<(), AdapterError> {
    if cancellation.is_cancelled() {
        Err(cancelled())
    } else if shutdown_token.is_cancelled() {
        Err(shutdown())
    } else {
        Ok(())
    }
}

const fn cancelled() -> AdapterError {
    AdapterError::new(AdapterErrorKind::Cancelled, "operation cancelled")
}

const fn shutdown() -> AdapterError {
    AdapterError::new(AdapterErrorKind::Shutdown, "adapter shut down")
}

const fn load_failed() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::LoadFailed,
        "Mantle source or playback failed",
    )
}

const fn invalid_track() -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidTrack, "invalid encoded track")
}

const fn invalid_operation(message: &'static str) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidOperation, message)
}

const fn overloaded() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::Overloaded,
        "Mantle player command or event queue full",
    )
}

#[cfg(test)]
struct FixtureSession {
    engine: mantle_core::Engine<mantle_core::SystemClock>,
    player: mantle_core::PlayerId,
    track: mantle_core::TrackId,
    processing: ProcessingMode,
}

#[cfg(test)]
impl FixtureSession {
    fn new(metadata: &TrackMetadata) -> Result<Self, AdapterError> {
        let mut engine = mantle_core::Engine::new(
            mantle_core::SystemClock::new(),
            mantle_core::ResourceLimits::default(),
        );
        let manager = engine
            .create_manager()
            .map_err(|_| invalid_operation("Mantle fixture manager failed"))?;
        let player = engine
            .create_player(manager)
            .map_err(|_| invalid_operation("Mantle fixture player failed"))?;
        let frame_count = if metadata.identifier == "fixture:short" {
            1
        } else {
            16
        };
        let frames = (0..frame_count).map(|sequence| {
            mantle_core::Frame::synthetic(
                Duration::from_millis(sequence * u64::from(FRAME_DURATION_MS)),
                sequence.to_be_bytes().to_vec(),
            )
        });
        let track = engine
            .create_track(metadata_to_info(metadata), frames)
            .map_err(|_| invalid_track())?;
        engine
            .start_track(player, track, false)
            .map_err(|_| invalid_operation("Mantle fixture playback failed"))?;
        Ok(Self {
            engine,
            player,
            track,
            processing: ProcessingMode::Passthrough,
        })
    }

    fn seek(&mut self, position: Duration) -> Result<Duration, AdapterError> {
        self.engine
            .seek(self.track, position)
            .map_err(|_| invalid_operation("Mantle fixture seek failed"))?;
        Ok(position)
    }

    fn read_frame(&mut self) -> Result<Option<EncodedPlaybackFrame>, AdapterError> {
        let (frame, _) = self
            .engine
            .provide(self.player, Duration::ZERO)
            .map_err(|_| invalid_operation("Mantle fixture frame failed"))?;
        frame
            .map(|frame| {
                OpusPacket::copy_from(&frame.data)
                    .map(|payload| (payload, frame.timecode))
                    .map_err(|_| invalid_operation("Mantle fixture produced an invalid Opus frame"))
            })
            .transpose()
    }
}

#[cfg(test)]
fn fixture_load(identifier: &str) -> Result<LoadOutcome, AdapterError> {
    match identifier {
        "fixture:none" => Ok(LoadOutcome::NoMatches),
        "fixture:load-error" => Err(load_failed()),
        "fixture:search" => Ok(LoadOutcome::Search(vec![
            fixture_track("fixture:search/one")?,
            fixture_track("fixture:search/two")?,
        ])),
        "fixture:playlist" => Ok(LoadOutcome::Playlist {
            info: PlaylistInfo {
                name: "Synthetic playlist".into(),
                selected_track: Some(0),
            },
            plugin_info: serde_json::from_value(serde_json::json!({
                "type": "fixture-playlist"
            }))
            .expect("static fixture object"),
            tracks: vec![
                fixture_track("fixture:playlist/one")?,
                fixture_track("fixture:playlist/two")?,
            ],
        }),
        value if value.starts_with("fixture:") => Ok(LoadOutcome::Track(fixture_track(value)?)),
        _ => Ok(LoadOutcome::NoMatches),
    }
}

#[cfg(test)]
fn fixture_track(identifier: &str) -> Result<MediaTrack, AdapterError> {
    let metadata = TrackMetadata {
        identifier: identifier.into(),
        title: format!("Synthetic {identifier}"),
        author: "Crust testkit".into(),
        duration_ms: if identifier == "fixture:short" {
            40
        } else {
            10_000
        },
        seekable: true,
        stream: false,
        source_name: "fixture".into(),
        uri: None,
        artwork_url: None,
        isrc: None,
    };
    Ok(MediaTrack {
        encoded: encode_fixture_track(&metadata)?,
        metadata,
        plugin_info: serde_json::from_value(serde_json::json!({
            "fixture": {"identifier": identifier, "nested": [1, null, true]}
        }))
        .expect("static fixture object"),
    })
}

#[cfg(test)]
fn encode_fixture_track(metadata: &TrackMetadata) -> Result<EncodedTrack, AdapterError> {
    let mut engine = mantle_core::Engine::new(
        mantle_core::SystemClock::new(),
        mantle_core::ResourceLimits::default(),
    );
    let track = engine
        .create_track(metadata_to_info(metadata), std::iter::empty())
        .map_err(|_| invalid_track())?;
    let bytes = mantle_core::encode_synthetic_track(
        engine.track(track).map_err(|_| invalid_track())?,
        SerializationLimits::default(),
    )
    .map_err(|_| invalid_track())?;
    Ok(EncodedTrack::new(BASE64.encode(bytes)))
}

#[cfg(test)]
fn decode_fixture_track(encoded: &EncodedTrack) -> Result<MediaTrack, AdapterError> {
    let bytes = BASE64
        .decode(encoded.as_str())
        .map_err(|_| invalid_track())?;
    let decoded = mantle_core::decode_synthetic_track(&bytes, SerializationLimits::default())
        .map_err(|_| invalid_track())?;
    let metadata = info_to_metadata(&decoded.info, "fixture");
    Ok(MediaTrack {
        encoded: encoded.clone(),
        plugin_info: serde_json::from_value(serde_json::json!({
            "fixture": {"identifier": metadata.identifier, "nested": [1, null, true]}
        }))
        .expect("static fixture object"),
        metadata,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::path::PathBuf;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Instant;

    use crust_testkit::{ADAPTER_CONFORMANCE_CHECKS, run_adapter_conformance};
    use mantle_media::{
        HttpNetworkAccess, MediaSession, RemoteHttpClient, RemoteHttpOptions, RemoteHttpRequest,
    };

    use super::*;

    fn buffered_fixture_actor() -> PlayerActor {
        let adapter = RealMantleAdapter::with_defaults(RoutePlanner::disabled()).unwrap();
        let track = fixture_track("fixture:buffered").unwrap();
        let session = FixtureSession::new(&track.metadata).unwrap();
        PlayerActor {
            staging_max_bytes: 0,
            completed_input: None,
            manager: Arc::clone(&adapter.inner.manager),
            registry: Arc::clone(&adapter.inner.registry),
            route_policy: Arc::clone(&adapter.inner.route_policy),
            session: Some(PlaybackSession::Fixture(Box::new(session))),
            track: Some(track),
            status: PlayerStatus::Playing,
            paused: false,
            position: Duration::ZERO,
            sequence: 0,
            events: VecDeque::new(),
            filters: FilterConfiguration::default(),
            buffered: VecDeque::new(),
            pending_filters: VecDeque::new(),
            reading: None,
            read_ahead_halted: false,
            processing: ProcessingMode::Passthrough,
        }
    }

    fn staged_fixture_actor() -> PlayerActor {
        let mut actor = buffered_fixture_actor();
        actor.staging_max_bytes = 16 * 1024 * 1024;
        let track = actor.track.as_mut().unwrap();
        track.metadata.identifier = "fixture:staged".into();
        // Reopening this identity normally fails. Success therefore requires
        // consuming the completed input, not the regular source opener.
        track.encoded = EncodedTrack::new("requires-completed-input");
        actor
    }

    #[tokio::test]
    async fn completed_staged_input_repeats_and_resets_sequence_without_reopening_source() {
        let mut actor = staged_fixture_actor();
        let track = actor.track.clone().unwrap();
        for _ in 0..16 {
            assert!(matches!(
                actor.next_frame().await.unwrap(),
                PlayerFramePoll::Frame(_)
            ));
        }
        assert!(matches!(
            actor.next_frame().await.unwrap(),
            PlayerFramePoll::Ended
        ));
        assert!(actor.session.is_none(), "EOF must release decoder buffers");
        assert!(actor.completed_input.is_some());
        actor.play(track, CancellationToken::new()).await.unwrap();
        let PlayerFramePoll::Frame(frame) = actor.next_frame().await.unwrap() else {
            panic!("repeat has no frame");
        };
        assert_eq!(frame.sequence, 0);
        actor.stop(TrackEndReason::Stopped).unwrap();
        assert!(actor.session.is_none());
        assert!(actor.completed_input.is_none());
    }

    #[tokio::test]
    async fn staged_input_cleanup_cancellation_and_stopped_controls() {
        let mut actor = staged_fixture_actor();
        let track = actor.track.clone().unwrap();
        actor.stop(TrackEndReason::Finished).unwrap();
        let (reply, result) = oneshot::channel();
        actor
            .handle(PlayerCommand::Seek {
                position: Duration::ZERO,
                reply,
            })
            .await;
        assert!(result.await.unwrap().is_err());
        let token = CancellationToken::new();
        token.cancel();
        assert!(actor.play(track.clone(), token).await.is_err());
        assert!(
            actor.completed_input.is_some(),
            "cancelled preflight must preserve cache"
        );
        let mut invalid = track;
        invalid.encoded = EncodedTrack::new("invalid");
        assert!(actor.play(invalid, CancellationToken::new()).await.is_err());
        assert!(
            actor.completed_input.is_none(),
            "replacement failure must release old cache"
        );
        let mut actor = staged_fixture_actor();
        actor.stop(TrackEndReason::Finished).unwrap();
        let (reply, result) = oneshot::channel();
        actor.handle(PlayerCommand::Shutdown { reply }).await;
        result.await.unwrap().unwrap();
        assert!(actor.completed_input.is_none());
    }

    #[tokio::test]
    async fn completed_streaming_fallback_does_not_retain_a_decoder_or_source() {
        let mut actor = buffered_fixture_actor();
        actor.staging_max_bytes = 16 * 1024 * 1024;
        actor.stop(TrackEndReason::Finished).unwrap();
        assert!(actor.session.is_none());
        assert!(actor.completed_input.is_none());
    }

    #[tokio::test]
    async fn read_ahead_is_bounded_and_eof_waits_for_the_last_delivered_frame() {
        let mut actor = buffered_fixture_actor();
        for _ in 0..(MEDIA_READ_AHEAD_FRAMES * 2) {
            actor.start_read_ahead();
            actor.settle_read().await;
        }
        assert_eq!(actor.buffered.len(), MEDIA_READ_AHEAD_FRAMES);
        assert_eq!(
            actor.position,
            Duration::ZERO,
            "decoded position must not become delivered position"
        );
        assert!(actor.events.is_empty());
        assert_eq!(actor.status, PlayerStatus::Playing);
        for expected in 0..16 {
            let PlayerFramePoll::Frame(frame) = actor.next_frame().await.unwrap() else {
                panic!("early EOF");
            };
            assert_eq!(frame.sequence, expected);
            assert_eq!(frame.payload.as_slice(), &expected.to_be_bytes());
            assert_eq!(actor.position, Duration::from_millis(expected * 20));
            actor.start_read_ahead();
            actor.settle_read().await;
            assert!(
                actor.events.is_empty(),
                "prefetched EOF must not terminate the queued tail"
            );
        }
        assert!(matches!(
            actor.next_frame().await.unwrap(),
            PlayerFramePoll::Ended
        ));
        assert!(matches!(
            actor.events.pop_front(),
            Some(MediaEvent::TrackEnd {
                reason: TrackEndReason::Finished,
                ..
            })
        ));
        assert_eq!(actor.status, PlayerStatus::Stopped);
        assert_eq!(
            actor.buffered.capacity(),
            0,
            "stopped players release their buffer"
        );
    }

    #[tokio::test]
    async fn pause_retains_read_ahead_and_resume_delivers_in_order() {
        let mut actor = buffered_fixture_actor();
        for _ in 0..8 {
            actor.start_read_ahead();
            actor.settle_read().await;
        }
        let (reply, response) = oneshot::channel();
        actor
            .handle(PlayerCommand::Pause {
                paused: true,
                reply,
            })
            .await;
        response.await.unwrap().unwrap();
        let buffered = actor.buffered.len();
        actor.start_read_ahead();
        assert!(actor.reading.is_none());
        assert!(matches!(
            actor.next_frame().await.unwrap(),
            PlayerFramePoll::Ended
        ));
        assert_eq!(actor.buffered.len(), buffered);
        let (reply, response) = oneshot::channel();
        actor
            .handle(PlayerCommand::Pause {
                paused: false,
                reply,
            })
            .await;
        response.await.unwrap().unwrap();
        let PlayerFramePoll::Frame(frame) = actor.next_frame().await.unwrap() else {
            panic!("resume lost the queued audio");
        };
        assert_eq!(frame.payload.as_slice(), &0_u64.to_be_bytes());
        let (reply, response) = oneshot::channel();
        actor.handle(PlayerCommand::Stop { reply }).await;
        response.await.unwrap().unwrap();
        assert!(actor.buffered.is_empty());
        assert!(actor.reading.is_none());
    }

    #[tokio::test]
    async fn buffered_audio_remains_available_while_next_read_is_blocked() {
        let mut actor = buffered_fixture_actor();
        for _ in 0..8 {
            actor.start_read_ahead();
            actor.settle_read().await;
        }
        let session = actor.session.take().unwrap();
        let (release, wait) = oneshot::channel();
        actor.reading = Some(tokio::spawn(async move {
            let _ = wait.await;
            (session, Err(load_failed()))
        }));
        // No timer advances and the pending read cannot finish. Existing frames
        // and pause acknowledgements must still be available immediately.
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..4 {
            let mut request = Box::pin(actor.next_frame());
            assert!(matches!(
                request.as_mut().poll(&mut cx),
                Poll::Ready(Ok(PlayerFramePoll::Frame(_)))
            ));
        }
        let (reply, mut response) = oneshot::channel();
        {
            let mut pause = Box::pin(actor.handle(PlayerCommand::Pause {
                paused: true,
                reply,
            }));
            assert!(matches!(pause.as_mut().poll(&mut cx), Poll::Ready(())));
        }
        response.try_recv().unwrap().unwrap();
        assert_eq!(actor.status, PlayerStatus::Paused);
        let (reply, response) = oneshot::channel();
        actor
            .handle(PlayerCommand::Pause {
                paused: false,
                reply,
            })
            .await;
        response.await.unwrap().unwrap();
        release.send(()).unwrap();
        actor.settle_read().await;
        for _ in 0..4 {
            assert!(matches!(
                actor.next_frame().await.unwrap(),
                PlayerFramePoll::Frame(_)
            ));
        }
        assert_eq!(
            actor.next_frame().await.err().unwrap(),
            load_failed(),
            "source errors follow already buffered audio"
        );
    }

    #[tokio::test]
    async fn filter_update_waiting_on_a_source_read_does_not_block_buffered_audio() {
        let mut actor = buffered_fixture_actor();
        for _ in 0..8 {
            actor.start_read_ahead();
            actor.settle_read().await;
        }
        let session = actor.session.take().unwrap();
        let (release, wait) = oneshot::channel();
        actor.reading = Some(tokio::spawn(async move {
            let _ = wait.await;
            (session, Err(load_failed()))
        }));
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        let (reply, mut response) = oneshot::channel();
        {
            let mut update = Box::pin(actor.handle(PlayerCommand::SetFilters {
                configuration: Box::default(),
                reply,
            }));
            assert!(
                matches!(update.as_mut().poll(&mut cx), Poll::Ready(())),
                "a pending HTTP read must not hold the actor inside a filter command"
            );
        }
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        for _ in 0..4 {
            let mut frame = Box::pin(actor.next_frame());
            assert!(matches!(
                frame.as_mut().poll(&mut cx),
                Poll::Ready(Ok(PlayerFramePoll::Frame(_)))
            ));
        }
        release.send(()).unwrap();
        actor.settle_read().await;
        response.await.unwrap().unwrap();
        assert_eq!(actor.buffered.len(), 5);
    }

    #[tokio::test]
    async fn deferred_filters_are_bounded_ordered_and_drained_before_stop() {
        let mut actor = buffered_fixture_actor();
        actor.start_read_ahead();
        let mut replies = Vec::new();
        for volume in 0..PLAYER_COMMAND_CAPACITY {
            let (reply, response) = oneshot::channel();
            actor
                .handle(PlayerCommand::SetFilters {
                    configuration: Box::new(FilterConfiguration {
                        player_volume: Some(volume as u16),
                        ..FilterConfiguration::default()
                    }),
                    reply,
                })
                .await;
            replies.push(response);
        }
        let (reply, response) = oneshot::channel();
        actor
            .handle(PlayerCommand::SetFilters {
                configuration: Box::default(),
                reply,
            })
            .await;
        assert_eq!(response.await.unwrap(), Err(overloaded()));
        assert_eq!(actor.pending_filters.len(), PLAYER_COMMAND_CAPACITY);
        let (reply, response) = oneshot::channel();
        actor.handle(PlayerCommand::Stop { reply }).await;
        response.await.unwrap().unwrap();
        for response in replies {
            response.await.unwrap().unwrap();
        }
        assert_eq!(
            actor.filters.player_volume,
            Some((PLAYER_COMMAND_CAPACITY - 1) as u16)
        );
        assert_eq!(actor.pending_filters.capacity(), 0);
        assert_eq!(actor.status, PlayerStatus::Stopped);
    }

    #[tokio::test]
    async fn seek_and_replacement_discard_frames_from_the_previous_position() {
        let mut actor = buffered_fixture_actor();
        for _ in 0..8 {
            actor.start_read_ahead();
            actor.settle_read().await;
        }
        actor.start_read_ahead();
        let (reply, response) = oneshot::channel();
        actor
            .handle(PlayerCommand::Seek {
                position: Duration::from_millis(120),
                reply,
            })
            .await;
        response.await.unwrap().unwrap();
        assert!(actor.buffered.is_empty());
        assert!(actor.reading.is_none());
        assert_eq!(actor.position, Duration::from_millis(120));
        actor.start_read_ahead();
        actor.settle_read().await;
        actor
            .play(
                fixture_track("fixture:short").unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let PlayerFramePoll::Frame(frame) = actor.next_frame().await.unwrap() else {
            panic!("replacement did not start");
        };
        assert_eq!(frame.payload.as_slice(), &0_u64.to_be_bytes());
        assert!(matches!(
            actor.next_frame().await.unwrap(),
            PlayerFramePoll::Ended
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_adapter_passes_the_shared_contract_through_mantle() {
        let adapter = RealMantleAdapter::new(
            RoutePlanner::disabled(),
            YoutubeSourceOptions::default(),
            YoutubeAuthentication::default(),
        )
        .unwrap();
        let report = run_adapter_conformance(&adapter).await.unwrap();
        assert_eq!(report.checks, ADAPTER_CONFORMANCE_CHECKS);
    }

    #[test]
    fn production_source_http_defaults_to_mantles_public_internet_policy() {
        assert_eq!(
            RemoteHttpOptions::default().network_access,
            HttpNetworkAccess::PublicInternetOnly
        );
    }

    #[test]
    fn outbound_policy_preserves_identity_and_rejects_known_family_mismatch() {
        let planner = RoutePlanner::configured(crust::routeplanner::RoutePlannerConfig::new(
            crust::routeplanner::RoutePlannerStrategy::RotateOnBan,
            ["::1/128"],
        ))
        .unwrap();
        let policy = CrustOutboundRoutePolicy::new(planner.clone());
        let selected = policy
            .select_route(OutboundRouteContext {
                scheme: "http",
                authority: "[::1]:8080",
            })
            .unwrap();
        assert_eq!(selected.local_ip, IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(selected.identity, 1);
        policy.report_outcome(selected, OutboundRouteOutcome::Timeout);
        assert!(planner.snapshot().unwrap().failing_addresses.is_empty());
        policy.report_outcome(selected, OutboundRouteOutcome::TransportFailure);
        assert_eq!(planner.snapshot().unwrap().failing_addresses.len(), 1);
    }

    #[test]
    fn disabled_route_planner_reuses_source_http_connection() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut connections = 0;
            let mut requests = 0;
            while requests < 2 && Instant::now() < deadline {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                };
                connections += 1;
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                while requests < 2 {
                    let mut header = Vec::new();
                    let mut byte = [0];
                    while header.len() < 8192 && !header.ends_with(b"\r\n\r\n") {
                        if stream.read_exact(&mut byte).is_err() {
                            break;
                        }
                        header.push(byte[0]);
                    }
                    if !header.ends_with(b"\r\n\r\n") {
                        break;
                    }
                    let header = String::from_utf8(header).unwrap();
                    let length: usize = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 8192);
                    stream.read_exact(&mut vec![0; length]).unwrap();
                    let body = br#"{"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"dQw4w9WgXcQ","title":"Fixture","author":"Artist","lengthSeconds":"213"}}"#;
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len()).unwrap();
                    stream.write_all(body).unwrap();
                    requests += 1;
                }
            }
            (connections, requests)
        });
        let adapter = RealMantleAdapter::new(
            RoutePlanner::disabled(),
            YoutubeSourceOptions {
                api_base_url: format!("http://{address}"),
                http: RemoteHttpOptions {
                    network_access: HttpNetworkAccess::AllowPrivateNetworks,
                    max_retries: 0,
                    request_timeout: Duration::from_secs(3),
                    ..RemoteHttpOptions::default()
                },
                ..YoutubeSourceOptions::default()
            },
            YoutubeAuthentication::default(),
        )
        .unwrap();
        for _ in 0..2 {
            assert!(
                adapter
                    .inner
                    .manager
                    .load(&SourceReference::new(Some("dQw4w9WgXcQ".into()), false))
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            server.join().unwrap(),
            (1, 2),
            "two source requests reuse one TCP connection"
        );
    }

    #[test]
    fn routed_mantle_requests_bind_the_selected_local_ip_without_cross_route_reuse() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut peers = Vec::new();
            for stream in listener.incoming().take(2) {
                let mut stream = stream.unwrap();
                peers.push(stream.peer_addr().unwrap().ip());
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .unwrap();
            }
            peers
        });
        let planner = RoutePlanner::configured(crust::routeplanner::RoutePlannerConfig::new(
            crust::routeplanner::RoutePlannerStrategy::RotateOnBan,
            ["127.0.0.2/31"],
        ))
        .unwrap();
        let policy = Arc::new(CrustOutboundRoutePolicy::new(planner));
        let options = RemoteHttpOptions {
            network_access: HttpNetworkAccess::AllowPrivateNetworks,
            max_retries: 0,
            ..RemoteHttpOptions::default()
        };
        let client = RemoteHttpClient::with_route_policy(options, policy.clone()).unwrap();
        let request = RemoteHttpRequest::get(format!("http://{address}/route")).unwrap();

        assert_eq!(client.execute(&request).unwrap().body(), b"ok");
        policy.report_source(RouteOutcome::SourceRateLimited);
        assert_eq!(client.execute(&request).unwrap().body(), b"ok");

        assert_eq!(
            server.join().unwrap(),
            [
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)),
            ]
        );
    }

    struct P18MediaWorkerResult {
        frames: usize,
        deficits: usize,
        tracks_advancing: usize,
        passthrough_sessions: usize,
        transcode_sessions: usize,
        pacing_jitter_nanos: Vec<u64>,
    }

    fn p18_process_pss_kib() -> u64 {
        std::fs::read_to_string("/proc/self/smaps_rollup")
            .expect("process PSS is readable")
            .lines()
            .find_map(|line| {
                line.strip_prefix("Pss:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("PSS is present")
    }

    fn p18_process_threads() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .expect("process status is readable")
            .lines()
            .find_map(|line| {
                line.strip_prefix("Threads:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("thread count is present")
    }

    fn p18_process_cpu_ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").expect("process stat is readable");
        let fields = stat
            .rsplit_once(") ")
            .expect("process command terminator is present")
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let user: u64 = fields[11].parse().expect("user CPU ticks");
        let system: u64 = fields[12].parse().expect("system CPU ticks");
        user + system
    }

    fn p18_percentile(samples: &mut [u64], permille: usize) -> u64 {
        if samples.is_empty() {
            return 0;
        }
        samples.sort_unstable();
        samples[(samples.len() - 1).saturating_mul(permille) / 1_000]
    }

    fn p18_sleep_until(target: Instant) {
        if let Some(remaining) = target.checked_duration_since(Instant::now()) {
            thread::sleep(remaining);
        }
    }

    fn p18_run_media_ticks(
        sessions: &mut [YoutubePlaybackSession],
        outputs: &mut [EncodedFrameSlot],
        ticks: usize,
        seek: bool,
    ) -> (usize, usize, Vec<u64>) {
        let frame_duration = Duration::from_millis(20);
        let mut target = Instant::now();
        let mut frames = 0;
        let mut deficits = 0;
        let mut jitter = Vec::with_capacity(ticks);
        for tick in 0..ticks {
            target += frame_duration;
            p18_sleep_until(target);
            let actual = Instant::now();
            let error = if actual >= target {
                actual.duration_since(target)
            } else {
                target.duration_since(actual)
            };
            jitter.push(u64::try_from(error.as_nanos()).unwrap_or(u64::MAX));
            if seek && tick.is_multiple_of(50) {
                let position = Duration::from_secs(30 + u64::try_from(tick / 50).unwrap());
                for session in &mut *sessions {
                    session
                        .seek(position)
                        .expect("offline fixture seek succeeds");
                }
            }
            for (session, output) in sessions.iter_mut().zip(outputs.iter_mut()) {
                if session
                    .read_frame(output)
                    .expect("Mantle offline playback produces a bounded result")
                {
                    frames += 1;
                } else {
                    deficits += 1;
                }
            }
        }
        (frames, deficits, jitter)
    }

    #[test]
    #[ignore = "release-only P18 Mantle full-playback performance gate"]
    #[allow(clippy::too_many_lines)]
    fn p18_offline_mantle_playback_benchmark_report() {
        let fixture = PathBuf::from(
            std::env::var_os("CRUST_P18_MEDIA_FIXTURE").expect("fixture path is configured"),
        );
        let codec = std::env::var("CRUST_P18_MEDIA_CODEC").expect("codec is configured");
        let mode = std::env::var("CRUST_P18_MEDIA_MODE").expect("mode is configured");
        let players: usize = std::env::var("CRUST_P18_MEDIA_PLAYERS")
            .expect("player count is configured")
            .parse()
            .expect("player count is numeric");
        assert!(matches!(codec.as_str(), "opus" | "mp3" | "aac" | "flac"));
        assert!(matches!(mode.as_str(), "plain" | "filter" | "seek"));
        assert!((1..=100).contains(&players));

        let workers = players.min(4);
        let barrier = Arc::new(Barrier::new(workers + 1));
        let baseline_pss_kib = p18_process_pss_kib();
        let baseline_threads = p18_process_threads();
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let assigned = players / workers + usize::from(worker < players % workers);
            let fixture = fixture.clone();
            let mode = mode.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let factory = CrustFilterFactory(FilterConfiguration {
                    timescale: Some(crust::filters::Timescale {
                        speed: 1.10,
                        pitch: 1.0,
                        rate: 1.0,
                    }),
                    ..FilterConfiguration::default()
                });
                let mut sessions = Vec::with_capacity(assigned);
                let mut outputs = Vec::with_capacity(assigned);
                for _ in 0..assigned {
                    let media = MediaSession::open_file(&fixture, MediaLimits::default())
                        .expect("fixture probes through Mantle");
                    let mut playback = YoutubePlaybackSession::from_probed_media_session(media)
                        .expect("fixture enters Mantle's complete playback pipeline");
                    if mode == "filter" {
                        playback
                            .set_filter_factory(Some(&factory))
                            .expect("Crust filter factory installs through Mantle");
                    }
                    sessions.push(playback);
                    outputs.push(EncodedFrameSlot::new());
                }
                let passthrough_sessions = sessions
                    .iter()
                    .filter(|session| session.mode() == YoutubePlaybackMode::OpusPassthrough)
                    .count();
                let transcode_sessions = assigned - passthrough_sessions;

                barrier.wait();
                barrier.wait();
                let _ = p18_run_media_ticks(&mut sessions, &mut outputs, 250, false);
                barrier.wait();
                barrier.wait();
                let (frames, deficits, pacing_jitter_nanos) =
                    p18_run_media_ticks(&mut sessions, &mut outputs, 500, mode == "seek");
                barrier.wait();

                P18MediaWorkerResult {
                    frames,
                    deficits,
                    tracks_advancing: sessions
                        .iter()
                        .filter(|session| {
                            session
                                .source_media_position()
                                .is_some_and(|position| !position.is_zero())
                        })
                        .count(),
                    passthrough_sessions,
                    transcode_sessions,
                    pacing_jitter_nanos,
                }
            }));
        }

        barrier.wait();
        let staged_pss_kib = p18_process_pss_kib();
        let staged_threads = p18_process_threads();
        barrier.wait();
        barrier.wait();
        let warmed_pss_kib = p18_process_pss_kib();
        let warmed_threads = p18_process_threads();
        let cpu_before = p18_process_cpu_ticks();
        let measurement_started = Instant::now();
        barrier.wait();
        barrier.wait();
        let measurement_elapsed = measurement_started.elapsed();
        let cpu_ticks = p18_process_cpu_ticks().saturating_sub(cpu_before);
        let active_pss_kib = p18_process_pss_kib();
        let active_threads = p18_process_threads();

        let mut frames = 0;
        let mut deficits = 0;
        let mut tracks_advancing = 0;
        let mut passthrough_sessions = 0;
        let mut transcode_sessions = 0;
        let mut pacing_jitter_nanos = Vec::with_capacity(workers * 500);
        for handle in handles {
            let result = handle.join().expect("media worker does not panic");
            frames += result.frames;
            deficits += result.deficits;
            tracks_advancing += result.tracks_advancing;
            passthrough_sessions += result.passthrough_sessions;
            transcode_sessions += result.transcode_sessions;
            pacing_jitter_nanos.extend(result.pacing_jitter_nanos);
        }
        let after_join_threads = p18_process_threads();
        let p50_jitter = p18_percentile(&mut pacing_jitter_nanos, 500);
        let p95_jitter = p18_percentile(&mut pacing_jitter_nanos, 950);
        let p99_jitter = p18_percentile(&mut pacing_jitter_nanos, 990);
        let expected_frames = players * 500;
        assert_eq!(frames, expected_frames, "all paced frames are produced");
        assert_eq!(deficits, 0, "no active track has a frame deficit");
        assert_eq!(tracks_advancing, players, "every source position advances");
        if codec == "opus" && mode != "filter" {
            assert_eq!(passthrough_sessions, players);
        } else {
            assert_eq!(transcode_sessions, players);
        }

        let elapsed_seconds = measurement_elapsed.as_secs_f64();
        let report = serde_json::json!({
            "schemaVersion": 1,
            "benchmarkId": "mantle-offline-complete-playback",
            "scope": "Mantle-owned finite playback through YoutubePlaybackSession::read_frame",
            "codec": codec,
            "mode": mode,
            "players": players,
            "workers": workers,
            "warmupSeconds": 5,
            "measurementSeconds": elapsed_seconds,
            "frames": frames,
            "expectedFrames": expected_frames,
            "deficits": deficits,
            "tracksAdvancing": tracks_advancing,
            "modes": {
                "opusPassthrough": passthrough_sessions,
                "transcode": transcode_sessions
            },
            "cpu": {
                "processTicks": cpu_ticks,
                "clockTicksPerSecond": 100,
                "processCpuCoreEquivalent": cpu_ticks as f64 / 100.0 / elapsed_seconds
            },
            "memoryKiB": {
                "baselinePss": baseline_pss_kib,
                "stagedPss": staged_pss_kib,
                "warmedPss": warmed_pss_kib,
                "activePss": active_pss_kib
            },
            "threads": {
                "baseline": baseline_threads,
                "staged": staged_threads,
                "warmed": warmed_threads,
                "active": active_threads,
                "afterJoin": after_join_threads
            },
            "pacingJitterNanos": {
                "p50": p50_jitter,
                "p95": p95_jitter,
                "p99": p99_jitter
            }
        });
        println!("P18_MANTLE_MEDIA={report}");
    }
}
