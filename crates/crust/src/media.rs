//! Crust's narrow boundary to Mantle-owned source/media behavior.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::filters::FilterConfiguration;

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type JsonObject = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncodedTrack(String);

impl EncodedTrack {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackMetadata {
    pub identifier: String,
    pub title: String,
    pub author: String,
    pub duration_ms: u64,
    pub seekable: bool,
    pub stream: bool,
    pub source_name: String,
    pub uri: Option<String>,
    pub artwork_url: Option<String>,
    pub isrc: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MediaTrack {
    pub encoded: EncodedTrack,
    pub metadata: TrackMetadata,
    /// Source/plugin-owned data; Crust must preserve it without coercion.
    pub plugin_info: JsonObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistInfo {
    pub name: String,
    pub selected_track: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoadOutcome {
    Track(MediaTrack),
    Playlist {
        info: PlaylistInfo,
        plugin_info: JsonObject,
        tracks: Vec<MediaTrack>,
    },
    Search(Vec<MediaTrack>),
    NoMatches,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SourceRoute {
    /// Selected local bind identity. Mantle owns applying it to source HTTP.
    pub local_address: Option<IpAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadRequest {
    pub identifier: String,
    pub route: SourceRoute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingMode {
    Passthrough,
    Pcm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerStatus {
    Idle,
    Playing,
    Paused,
    Stopped,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlayerSnapshot {
    pub status: PlayerStatus,
    pub track: Option<MediaTrack>,
    pub position_ms: u64,
    pub processing: ProcessingMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackEndReason {
    Finished,
    Stopped,
    Replaced,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MediaEvent {
    TrackStart(MediaTrack),
    TrackEnd {
        track: MediaTrack,
        reason: TrackEndReason,
    },
    TrackError {
        track: MediaTrack,
        message: String,
    },
    TrackStuck {
        track: MediaTrack,
        threshold_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFormat {
    OpusLike,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaFrame {
    pub sequence: u64,
    pub duration_ms: u16,
    pub format: FrameFormat,
    pub payload: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterErrorKind {
    Cancelled,
    Shutdown,
    LoadFailed,
    InvalidTrack,
    InvalidOperation,
    Overloaded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterError {
    pub kind: AdapterErrorKind,
    pub message: &'static str,
}

impl AdapterError {
    #[must_use]
    pub const fn new(kind: AdapterErrorKind, message: &'static str) -> Self {
        Self { kind, message }
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for AdapterError {}

/// Pull-based frame and event delivery keeps the adapter boundary bounded: one
/// item is returned per caller request, and no hidden Crust queue is implied.
pub trait MantlePlayer: Send + Sync {
    fn play(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>>;

    fn pause(
        &self,
        paused: bool,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>>;

    fn seek(
        &self,
        position_ms: u64,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>>;

    fn stop(&self, cancellation: CancellationToken) -> AdapterFuture<'_, Result<(), AdapterError>>;

    fn set_filters(
        &self,
        configuration: FilterConfiguration,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<(), AdapterError>>;

    fn snapshot(&self) -> AdapterFuture<'_, Result<PlayerSnapshot, AdapterError>>;

    fn next_frame(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaFrame>, AdapterError>>;

    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Option<MediaEvent>, AdapterError>>;

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>>;
}

/// The only contract through which Crust core may use detailed Mantle behavior.
pub trait MantleAdapter: Send + Sync {
    fn load(
        &self,
        request: LoadRequest,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<LoadOutcome, AdapterError>>;

    fn decode(
        &self,
        encoded: EncodedTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<MediaTrack, AdapterError>>;

    fn encode(
        &self,
        track: MediaTrack,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<EncodedTrack, AdapterError>>;

    fn create_player(
        &self,
        cancellation: CancellationToken,
    ) -> AdapterFuture<'_, Result<Arc<dyn MantlePlayer>, AdapterError>>;

    fn shutdown(&self) -> AdapterFuture<'_, Result<(), AdapterError>>;
}
