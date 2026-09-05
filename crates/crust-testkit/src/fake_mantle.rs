use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crust::filters::FilterConfiguration;
use crust::media::{
    AdapterError, AdapterErrorKind, AdapterFuture, EncodedTrack, FrameFormat, JsonObject,
    LoadOutcome, LoadRequest, MantleAdapter, MantlePlayer, MediaEvent, MediaFrame, MediaTrack,
    PlayerSnapshot, PlayerStatus, PlaylistInfo, ProcessingMode, SourceRoute, TrackEndReason,
    TrackMetadata,
};
use crust::voice::OpusPacket;
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const FRAME_DURATION_MS: u16 = 20;

#[derive(Debug, Default)]
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    pub fn advance_ms(&self, amount: u64) {
        self.now_ms.fetch_add(amount, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FakeMantleConfig {
    pub event_capacity: NonZeroUsize,
}

impl Default for FakeMantleConfig {
    fn default() -> Self {
        Self {
            event_capacity: NonZeroUsize::new(8).expect("nonzero constant"),
        }
    }
}

#[derive(Debug)]
struct FakeInner {
    clock: Arc<ManualClock>,
    event_capacity: NonZeroUsize,
    shutdown: CancellationToken,
    hold_loads: AtomicBool,
    active_loads: AtomicUsize,
    load_release: Notify,
    last_route: Mutex<Option<SourceRoute>>,
    last_identifier: Mutex<Option<String>>,
    snapshots: Arc<SnapshotProbe>,
}

#[derive(Debug, Default)]
struct SnapshotProbe {
    last_filters: Mutex<Option<FilterConfiguration>>,
    hold: AtomicBool,
    calls: AtomicUsize,
    active: AtomicUsize,
    maximum_active: AtomicUsize,
    release: Notify,
}

#[derive(Debug, Clone)]
pub struct FakeMantle {
    inner: Arc<FakeInner>,
}

impl FakeMantle {
    #[must_use]
    pub fn last_filters(&self) -> Option<FilterConfiguration> {
        self.inner
            .snapshots
            .last_filters
            .lock()
            .expect("filter probe lock poisoned")
            .clone()
    }
    #[must_use]
    pub fn new(config: FakeMantleConfig) -> Self {
        Self {
            inner: Arc::new(FakeInner {
                clock: Arc::new(ManualClock::default()),
                event_capacity: config.event_capacity,
                shutdown: CancellationToken::new(),
                hold_loads: AtomicBool::new(false),
                active_loads: AtomicUsize::new(0),
                load_release: Notify::new(),
                last_route: Mutex::new(None),
                last_identifier: Mutex::new(None),
                snapshots: Arc::new(SnapshotProbe::default()),
            }),
        }
    }

    #[must_use]
    pub fn clock(&self) -> Arc<ManualClock> {
        Arc::clone(&self.inner.clock)
    }

    pub fn hold_loads(&self) {
        self.inner.hold_loads.store(true, Ordering::SeqCst);
    }

    pub fn release_loads(&self) {
        self.inner.hold_loads.store(false, Ordering::SeqCst);
        self.inner.load_release.notify_waiters();
    }

    #[must_use]
    pub fn active_loads(&self) -> usize {
        self.inner.active_loads.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn last_identifier(&self) -> Option<String> {
        self.inner
            .last_identifier
            .lock()
            .expect("identifier lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn last_route(&self) -> Option<SourceRoute> {
        *self.inner.last_route.lock().expect("route lock poisoned")
    }

    pub fn hold_snapshots(&self) {
        self.inner.snapshots.hold.store(true, Ordering::Release);
    }

    pub fn release_snapshots(&self) {
        self.inner.snapshots.hold.store(false, Ordering::Release);
        self.inner.snapshots.release.notify_waiters();
    }

    #[must_use]
    pub fn snapshot_calls(&self) -> usize {
        self.inner.snapshots.calls.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn active_snapshots(&self) -> usize {
        self.inner.snapshots.active.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn maximum_active_snapshots(&self) -> usize {
        self.inner.snapshots.maximum_active.load(Ordering::Acquire)
    }
}

impl Default for FakeMantle {
    fn default() -> Self {
        Self::new(FakeMantleConfig::default())
    }
}

impl MantleAdapter for FakeMantle {
    fn load(
        &self,
        request: LoadRequest,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<LoadOutcome, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let _active_load = ActiveLoad::new(Arc::clone(&inner));
            *inner
                .last_identifier
                .lock()
                .expect("identifier lock poisoned") = Some(request.identifier.clone());
            wait_for_load_release(&inner, &cancellation).await?;
            *inner.last_route.lock().expect("route lock poisoned") = Some(request.route);
            match request.identifier.as_str() {
                "fixture:none" => Ok(LoadOutcome::NoMatches),
                "fixture:load-error" => Err(error(
                    AdapterErrorKind::LoadFailed,
                    "synthetic load failure",
                )),
                "fixture:search" => Ok(LoadOutcome::Search(vec![
                    track_for("fixture:search/one"),
                    track_for("fixture:search/two"),
                ])),
                "fixture:playlist" => Ok(LoadOutcome::Playlist {
                    info: PlaylistInfo {
                        name: "Synthetic playlist".into(),
                        selected_track: Some(0),
                    },
                    plugin_info: json_object(json!({"type": "fixture-playlist"})),
                    tracks: vec![
                        track_for("fixture:playlist/one"),
                        track_for("fixture:playlist/two"),
                    ],
                }),
                "fixture:playlist-none" => Ok(LoadOutcome::Playlist {
                    info: PlaylistInfo {
                        name: "Synthetic unselected playlist".into(),
                        selected_track: None,
                    },
                    plugin_info: JsonObject::new(),
                    tracks: vec![track_for("fixture:playlist-none/one")],
                }),
                identifier if identifier.starts_with("fixture:") => {
                    Ok(LoadOutcome::Track(track_for(identifier)))
                }
                _ => Ok(LoadOutcome::NoMatches),
            }
        })
    }

    fn decode(
        &self,
        encoded: EncodedTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<MediaTrack, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_available(&inner, &cancellation)?;
            let identifier = encoded
                .as_str()
                .strip_prefix("fake-v1:")
                .ok_or_else(|| error(AdapterErrorKind::InvalidTrack, "invalid fake track"))?;
            if !identifier.starts_with("fixture:") {
                return Err(error(AdapterErrorKind::InvalidTrack, "invalid fake track"));
            }
            Ok(track_for(identifier))
        })
    }

    fn encode(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<EncodedTrack, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_available(&inner, &cancellation)?;
            Ok(EncodedTrack::new(format!(
                "fake-v1:{}",
                track.metadata.identifier
            )))
        })
    }

    fn create_player(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Arc<dyn MantlePlayer>, AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            check_available(&inner, &cancellation)?;
            Ok(Arc::new(FakePlayer::new(
                Arc::clone(&inner.clock),
                inner.event_capacity,
                inner.shutdown.child_token(),
                Arc::clone(&inner.snapshots),
            )) as Arc<dyn MantlePlayer>)
        })
    }

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.shutdown.cancel();
            inner.load_release.notify_waiters();
            Ok(())
        })
    }
}

struct ActiveLoad {
    inner: Arc<FakeInner>,
}

impl ActiveLoad {
    fn new(inner: Arc<FakeInner>) -> Self {
        inner.active_loads.fetch_add(1, Ordering::SeqCst);
        Self { inner }
    }
}

impl Drop for ActiveLoad {
    fn drop(&mut self) {
        self.inner.active_loads.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn wait_for_load_release(
    inner: &FakeInner,
    cancellation: &CancellationToken,
) -> Result<(), AdapterError> {
    loop {
        let released = inner.load_release.notified();
        check_available(inner, cancellation)?;
        if !inner.hold_loads.load(Ordering::SeqCst) {
            return Ok(());
        }
        tokio::select! {
            () = released => {}
            () = cancellation.cancelled() => return Err(cancelled()),
            () = inner.shutdown.cancelled() => return Err(shutdown()),
        }
    }
}

fn check_available(
    inner: &FakeInner,
    cancellation: &CancellationToken,
) -> Result<(), AdapterError> {
    if cancellation.is_cancelled() {
        Err(cancelled())
    } else if inner.shutdown.is_cancelled() {
        Err(shutdown())
    } else {
        Ok(())
    }
}

fn json_object(value: serde_json::Value) -> JsonObject {
    serde_json::from_value(value).expect("static fixture JSON object")
}

fn track_for(identifier: &str) -> MediaTrack {
    MediaTrack {
        encoded: EncodedTrack::new(format!("fake-v1:{identifier}")),
        metadata: TrackMetadata {
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
        },
        plugin_info: json_object(json!({
            "fixture": {"identifier": identifier, "nested": [1, null, true]}
        })),
    }
}

fn error(kind: AdapterErrorKind, message: &'static str) -> AdapterError {
    AdapterError::new(kind, message)
}

fn cancelled() -> AdapterError {
    error(AdapterErrorKind::Cancelled, "operation cancelled")
}

fn shutdown() -> AdapterError {
    error(AdapterErrorKind::Shutdown, "adapter shut down")
}

#[derive(Debug)]
struct FakePlayerState {
    status: PlayerStatus,
    track: Option<MediaTrack>,
    base_position_ms: u64,
    anchor_ms: u64,
    processing: ProcessingMode,
    filters: FilterConfiguration,
    next_frame_sequence: u64,
    events: VecDeque<MediaEvent>,
}

#[derive(Debug)]
struct FakePlayer {
    clock: Arc<ManualClock>,
    event_capacity: NonZeroUsize,
    shutdown: CancellationToken,
    snapshots: Arc<SnapshotProbe>,
    state: Mutex<FakePlayerState>,
}

impl FakePlayer {
    fn new(
        clock: Arc<ManualClock>,
        event_capacity: NonZeroUsize,
        shutdown: CancellationToken,
        snapshots: Arc<SnapshotProbe>,
    ) -> Self {
        Self {
            clock,
            event_capacity,
            shutdown,
            snapshots,
            state: Mutex::new(FakePlayerState {
                status: PlayerStatus::Idle,
                track: None,
                base_position_ms: 0,
                anchor_ms: 0,
                processing: ProcessingMode::Passthrough,
                filters: FilterConfiguration::default(),
                next_frame_sequence: 0,
                events: VecDeque::with_capacity(event_capacity.get()),
            }),
        }
    }

    fn check(&self, cancellation: &CancellationToken) -> Result<(), AdapterError> {
        if cancellation.is_cancelled() {
            Err(cancelled())
        } else if self.shutdown.is_cancelled() {
            Err(shutdown())
        } else {
            Ok(())
        }
    }

    fn position(&self, state: &FakePlayerState) -> u64 {
        let position = if state.status == PlayerStatus::Playing {
            state.base_position_ms + self.clock.now_ms().saturating_sub(state.anchor_ms)
        } else {
            state.base_position_ms
        };
        state
            .track
            .as_ref()
            .map_or(position, |track| position.min(track.metadata.duration_ms))
    }

    fn require_event_capacity(
        &self,
        state: &FakePlayerState,
        additional: usize,
    ) -> Result<(), AdapterError> {
        if state.events.len() + additional > self.event_capacity.get() {
            Err(error(
                AdapterErrorKind::Overloaded,
                "critical media event backlog full",
            ))
        } else {
            Ok(())
        }
    }
}

impl MantlePlayer for FakePlayer {
    fn play(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            let mut state = self.state.lock().expect("player lock poisoned");
            let terminal = usize::from(track.metadata.identifier == "fixture:event-error")
                + usize::from(track.metadata.identifier == "fixture:event-stuck");
            self.require_event_capacity(&state, 1 + usize::from(state.track.is_some()) + terminal)?;
            if let Some(previous) = state.track.take() {
                state.events.push_back(MediaEvent::TrackEnd {
                    track: previous,
                    reason: TrackEndReason::Replaced,
                });
            }
            state
                .events
                .push_back(MediaEvent::TrackStart(track.clone()));
            if track.metadata.identifier == "fixture:event-error" {
                state.events.push_back(MediaEvent::TrackError {
                    track: track.clone(),
                    message: "synthetic playback failure".into(),
                });
            } else if track.metadata.identifier == "fixture:event-stuck" {
                state.events.push_back(MediaEvent::TrackStuck {
                    track: track.clone(),
                    threshold_ms: 5_000,
                });
            }
            let terminal_error = track.metadata.identifier == "fixture:event-error";
            state.status = if terminal_error {
                PlayerStatus::Stopped
            } else {
                PlayerStatus::Playing
            };
            state.track = (!terminal_error).then_some(track);
            state.base_position_ms = 0;
            state.anchor_ms = self.clock.now_ms();
            state.next_frame_sequence = 0;
            Ok(())
        })
    }

    fn pause(
        &self,
        paused: bool,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            let mut state = self.state.lock().expect("player lock poisoned");
            if state.track.is_none()
                || matches!(state.status, PlayerStatus::Stopped | PlayerStatus::Shutdown)
            {
                return Err(error(AdapterErrorKind::InvalidOperation, "no active track"));
            }
            if paused && state.status == PlayerStatus::Playing {
                state.base_position_ms = self.position(&state);
                state.status = PlayerStatus::Paused;
            } else if !paused && state.status == PlayerStatus::Paused {
                state.anchor_ms = self.clock.now_ms();
                state.status = PlayerStatus::Playing;
            }
            Ok(())
        })
    }

    fn seek(
        &self,
        position_ms: u64,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            let mut state = self.state.lock().expect("player lock poisoned");
            let duration = state
                .track
                .as_ref()
                .ok_or_else(|| error(AdapterErrorKind::InvalidOperation, "no active track"))?
                .metadata
                .duration_ms;
            state.base_position_ms = position_ms.min(duration);
            state.anchor_ms = self.clock.now_ms();
            Ok(())
        })
    }

    fn stop(&self, cancellation: CancellationToken) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            let mut state = self.state.lock().expect("player lock poisoned");
            if let Some(track) = state.track.clone() {
                self.require_event_capacity(&state, 1)?;
                state.events.push_back(MediaEvent::TrackEnd {
                    track,
                    reason: TrackEndReason::Stopped,
                });
                state.track = None;
            }
            state.status = PlayerStatus::Stopped;
            state.base_position_ms = 0;
            Ok(())
        })
    }

    fn set_filters(
        &self,
        configuration: FilterConfiguration,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            *self
                .snapshots
                .last_filters
                .lock()
                .expect("filter probe lock poisoned") = Some(configuration.clone());
            let mut state = self.state.lock().expect("player lock poisoned");
            state.processing = if configuration.is_effective() {
                ProcessingMode::Pcm
            } else {
                ProcessingMode::Passthrough
            };
            state.filters = configuration;
            Ok(())
        })
    }

    fn snapshot(&self) -> AdapterFuture<'_, Result<PlayerSnapshot, AdapterError>> {
        Box::pin(async move {
            let _activity = SnapshotActivity::new(&self.snapshots);
            loop {
                let released = self.snapshots.release.notified();
                if self.shutdown.is_cancelled() {
                    return Err(shutdown());
                }
                if !self.snapshots.hold.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    () = released => {}
                    () = self.shutdown.cancelled() => return Err(shutdown()),
                }
            }
            let state = self.state.lock().expect("player lock poisoned");
            Ok(PlayerSnapshot {
                status: state.status,
                track: state.track.clone(),
                position_ms: self.position(&state),
                processing: state.processing,
            })
        })
    }

    fn next_frame(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaFrame>, AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            let mut state = self.state.lock().expect("player lock poisoned");
            if state.status != PlayerStatus::Playing {
                return Ok(None);
            }
            let track = state.track.clone().expect("playing track");
            let sequence = state.next_frame_sequence;
            if track.metadata.identifier == "fixture:frame-error" {
                return Err(error(
                    AdapterErrorKind::InvalidOperation,
                    "synthetic frame production failure",
                ));
            }
            let finishes = track.metadata.identifier == "fixture:short" && sequence == 1;
            if finishes {
                self.require_event_capacity(&state, 1)?;
            }
            state.next_frame_sequence += 1;
            let payload = OpusPacket::copy_from(&sequence.to_be_bytes()).expect("bounded fixture");
            let frame = MediaFrame {
                sequence,
                duration_ms: FRAME_DURATION_MS,
                // Mantle always returns Opus: PCM is an internal processing mode,
                // never a second Crust-owned output pipeline.
                format: FrameFormat::OpusLike,
                payload,
            };
            if finishes {
                state.events.push_back(MediaEvent::TrackEnd {
                    track,
                    reason: TrackEndReason::Finished,
                });
                state.status = PlayerStatus::Stopped;
                state.track = None;
            }
            Ok(Some(frame))
        })
    }

    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaEvent>, AdapterError>> {
        Box::pin(async move {
            self.check(&cancellation)?;
            Ok(self
                .state
                .lock()
                .expect("player lock poisoned")
                .events
                .pop_front())
        })
    }

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.shutdown.cancel();
            self.state.lock().expect("player lock poisoned").status = PlayerStatus::Shutdown;
            Ok(())
        })
    }
}

struct SnapshotActivity<'a> {
    probe: &'a SnapshotProbe,
}

impl<'a> SnapshotActivity<'a> {
    fn new(probe: &'a SnapshotProbe) -> Self {
        probe.calls.fetch_add(1, Ordering::AcqRel);
        let active = probe.active.fetch_add(1, Ordering::AcqRel) + 1;
        probe.maximum_active.fetch_max(active, Ordering::AcqRel);
        Self { probe }
    }
}

impl Drop for SnapshotActivity<'_> {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn load_request(identifier: &str) -> LoadRequest {
        LoadRequest {
            identifier: identifier.into(),
            route: SourceRoute::default(),
        }
    }

    async fn load_track(fake: &FakeMantle, identifier: &str) -> MediaTrack {
        match fake
            .load(load_request(identifier), CancellationToken::new())
            .await
            .unwrap()
        {
            LoadOutcome::Track(track) => track,
            other => panic!("expected track, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_clock_controls_position_exactly() {
        let fake = FakeMantle::default();
        let player = fake.create_player(CancellationToken::new()).await.unwrap();
        player
            .play(
                load_track(&fake, "fixture:track").await,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        fake.clock().advance_ms(1_250);
        assert_eq!(player.snapshot().await.unwrap().position_ms, 1_250);
        player.pause(true, CancellationToken::new()).await.unwrap();
        fake.clock().advance_ms(5_000);
        assert_eq!(player.snapshot().await.unwrap().position_ms, 1_250);
    }

    #[tokio::test]
    async fn held_load_is_cancellable_without_a_hidden_retry() {
        let fake = Arc::new(FakeMantle::default());
        fake.hold_loads();
        let cancellation = CancellationToken::new();
        let task = {
            let fake = Arc::clone(&fake);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                fake.load(load_request("fixture:blocked"), cancellation)
                    .await
            })
        };
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert_eq!(
            task.await.unwrap().unwrap_err().kind,
            AdapterErrorKind::Cancelled
        );
    }

    #[tokio::test]
    async fn held_load_resumes_only_after_explicit_release() {
        let fake = Arc::new(FakeMantle::default());
        fake.hold_loads();
        let task = {
            let fake = Arc::clone(&fake);
            tokio::spawn(async move {
                fake.load(load_request("fixture:blocked"), CancellationToken::new())
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        fake.release_loads();
        assert!(matches!(
            task.await.unwrap().unwrap(),
            LoadOutcome::Track(_)
        ));
    }

    #[tokio::test]
    async fn full_critical_event_backlog_rejects_without_mutating_player() {
        let fake = FakeMantle::new(FakeMantleConfig {
            event_capacity: NonZeroUsize::new(1).unwrap(),
        });
        let player = fake.create_player(CancellationToken::new()).await.unwrap();
        player
            .play(
                load_track(&fake, "fixture:track").await,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let rejection = player.stop(CancellationToken::new()).await.unwrap_err();
        assert_eq!(rejection.kind, AdapterErrorKind::Overloaded);
        let unchanged = player.snapshot().await.unwrap();
        assert_eq!(unchanged.status, PlayerStatus::Playing);
        assert_eq!(
            unchanged.track.unwrap().metadata.identifier,
            "fixture:track"
        );
        player.next_event(CancellationToken::new()).await.unwrap();
        player.stop(CancellationToken::new()).await.unwrap();
    }

    #[tokio::test]
    async fn source_route_is_preserved_for_the_future_real_adapter() {
        let fake = FakeMantle::default();
        let route = SourceRoute {
            local_address: Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))),
        };
        fake.load(
            LoadRequest {
                identifier: "fixture:track".into(),
                route,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(fake.last_route(), Some(route));
    }
}
