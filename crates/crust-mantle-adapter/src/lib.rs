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
    OutboundRouteOutcome, OutboundRoutePolicy, RemoteHttpOptions, YoutubeAudioSourceManager,
    YoutubeAuthentication, YoutubeErrorKind, YoutubeLivePlaybackOptions, YoutubeLivePlaybackPoll,
    YoutubeLivePlaybackSession, YoutubePlaybackError, YoutubePlaybackErrorKind,
    YoutubePlaybackFormatKind, YoutubePlaybackMode, YoutubePlaybackSession, YoutubeSourceItem,
    YoutubeSourceOptions, YoutubeSourceTrack,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod filters;

use filters::CrustFilterFactory;

const PLAYER_COMMAND_CAPACITY: usize = 32;
const PLAYER_EVENT_CAPACITY: usize = 32;
const FRAME_DURATION_MS: u16 = 20;

/// Operator-facing source options mapped by the Crust server into Mantle's
/// validated YouTube and HTTP policy. Codec and DSP settings remain Mantle-owned.
#[derive(Clone, Copy, Debug)]
pub struct MantleAdapterOptions {
    pub allow_youtube_search: bool,
    pub max_playlist_pages: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for MantleAdapterOptions {
    fn default() -> Self {
        Self {
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
        Self::new(planner, options, YoutubeAuthentication::default())
    }

    /// Creates a routed YouTube manager and registers it for Mantle track serialization.
    pub fn new(
        planner: RoutePlanner,
        options: YoutubeSourceOptions,
        authentication: YoutubeAuthentication,
    ) -> Result<Self, AdapterError> {
        let route_policy = Arc::new(CrustOutboundRoutePolicy::new(planner));
        let mantle_policy: Arc<dyn OutboundRoutePolicy> = route_policy.clone();
        let manager = Arc::new(
            YoutubeAudioSourceManager::with_route_policy(options, authentication, mantle_policy)
                .map_err(|_| invalid_operation("invalid Mantle YouTube configuration"))?,
        );
        let mut registry = SourceRegistry::new(SourceRegistryLimits::default());
        registry
            .register(Box::new(SharedYoutubeManager(Arc::clone(&manager))))
            .map_err(|_| invalid_operation("failed to register Mantle YouTube source"))?;
        Ok(Self {
            inner: Arc::new(AdapterInner {
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
}

impl PlayerActor {
    async fn run(mut self, mut commands: mpsc::Receiver<PlayerCommand>) {
        while let Some(command) = commands.recv().await {
            let terminal = matches!(command, PlayerCommand::Shutdown { .. });
            self.handle(command).await;
            if terminal {
                break;
            }
        }
        self.session = None;
        self.status = PlayerStatus::Shutdown;
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
                }
                let _ = reply.send(result.map(|_| ()));
            }
            PlayerCommand::Stop { reply } => {
                let result = self.stop(TrackEndReason::Stopped);
                let _ = reply.send(result);
            }
            PlayerCommand::SetFilters {
                configuration,
                reply,
            } => {
                let result = if self.session.is_some() {
                    let installed = configuration.clone();
                    self.with_session(move |session| session.set_filters(&installed))
                        .await
                } else {
                    Ok(())
                };
                if result.is_ok() {
                    self.filters = *configuration;
                }
                let _ = reply.send(result);
            }
            PlayerCommand::Snapshot { reply } => {
                let processing = self
                    .session
                    .as_ref()
                    .map_or(ProcessingMode::Passthrough, PlaybackSession::mode);
                let _ = reply.send(Ok(PlayerSnapshot {
                    status: self.status,
                    track: self.track.clone(),
                    position_ms: u64::try_from(self.position.as_millis()).unwrap_or(u64::MAX),
                    processing,
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
                self.session = None;
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
        if self.events.len().saturating_add(2) > PLAYER_EVENT_CAPACITY {
            return Err(overloaded());
        }
        let manager = Arc::clone(&self.manager);
        let registry = Arc::clone(&self.registry);
        let policy = Arc::clone(&self.route_policy);
        let encoded = track.encoded.clone();
        let cancellation_for_open = cancellation.clone();
        let mut session = tokio::task::spawn_blocking(move || {
            open_playback(manager, registry, policy, encoded, cancellation_for_open)
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
        self.session = None;
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
        match self.with_session(PlaybackSession::read_frame).await? {
            PlaybackFramePoll::Frame((payload, timestamp)) => {
                self.position = self
                    .session
                    .as_ref()
                    .and_then(PlaybackSession::source_media_position)
                    .unwrap_or(timestamp);
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
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(PLAYER_COMMAND_CAPACITY);
        let actor = PlayerActor {
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
    let mantle_policy: Arc<dyn OutboundRoutePolicy> = route_policy.clone();
    let opened = if formats.selected().kind() == Some(YoutubePlaybackFormatKind::HlsMpegTsAac) {
        manager
            .open_selected_live_playback_routed(
                &formats,
                YoutubeLivePlaybackOptions::default(),
                media_cancel,
                mantle_policy,
            )
            .map(|session| PlaybackSession::Live {
                session: Box::new(session),
                now: Duration::ZERO,
            })
    } else {
        manager
            .open_selected_playback_routed(
                &formats,
                HttpRangeOptions::default(),
                MediaLimits::default(),
                media_cancel,
                mantle_policy,
            )
            .map(PlaybackSession::Finite)
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
    use std::thread;

    use crust_testkit::{ADAPTER_CONFORMANCE_CHECKS, run_adapter_conformance};
    use mantle_media::{HttpNetworkAccess, RemoteHttpClient, RemoteHttpOptions, RemoteHttpRequest};

    use super::*;

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
}
