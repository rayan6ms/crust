//! Backend-neutral Discord voice contract.
//!
//! Crust deliberately exposes neither Songbird nor DAVE implementation types.
//! Mantle is the sole pacing authority: a backend sends each already-paced
//! 20 ms Opus frame immediately and must not introduce another media timer or
//! an unbounded handoff queue.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub const OPUS_FRAME_DURATION: Duration = Duration::from_millis(20);
pub const MAX_OPUS_PACKET_BYTES: usize = 1_275;

pub type VoiceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A credential whose debug representation cannot disclose its contents.
#[derive(Clone, PartialEq, Eq)]
pub struct VoiceSecret(String);

impl VoiceSecret {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for VoiceSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VoiceSecret([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceConnectionInfo {
    pub guild_id: u64,
    pub user_id: u64,
    pub channel_id: u64,
    pub endpoint: String,
    pub session_id: VoiceSecret,
    pub token: VoiceSecret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoicePhase {
    Disconnected,
    ConnectingGateway,
    WaitingReady,
    DiscoveringUdp,
    SelectingProtocol,
    WaitingSessionDescription,
    PreparingDave,
    Connected,
    DaveTransition,
    Reconnecting,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoicePacingAuthority {
    Mantle,
}

pub const VOICE_PACING_AUTHORITY: VoicePacingAuthority = VoicePacingAuthority::Mantle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimedOpusFrame {
    sequence: u64,
    source_position: Duration,
    payload: Arc<[u8]>,
}

impl TimedOpusFrame {
    pub fn new(
        sequence: u64,
        source_position: Duration,
        duration: Duration,
        payload: Arc<[u8]>,
    ) -> Result<Self, VoiceFrameError> {
        if duration != OPUS_FRAME_DURATION {
            return Err(VoiceFrameError::WrongDuration { duration });
        }
        if payload.is_empty() {
            return Err(VoiceFrameError::EmptyPayload);
        }
        if payload.len() > MAX_OPUS_PACKET_BYTES {
            return Err(VoiceFrameError::PayloadTooLarge {
                size: payload.len(),
                maximum: MAX_OPUS_PACKET_BYTES,
            });
        }
        Ok(Self {
            sequence,
            source_position,
            payload,
        })
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn source_position(&self) -> Duration {
        self.source_position
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceFrameError {
    WrongDuration { duration: Duration },
    EmptyPayload,
    PayloadTooLarge { size: usize, maximum: usize },
}

impl fmt::Display for VoiceFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongDuration { .. } => formatter.write_str("voice frames must be exactly 20 ms"),
            Self::EmptyPayload => formatter.write_str("voice frame payload must not be empty"),
            Self::PayloadTooLarge { .. } => {
                formatter.write_str("voice frame exceeds the maximum Opus packet size")
            }
        }
    }
}

impl std::error::Error for VoiceFrameError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceCounters {
    pub sent: u64,
    pub nulled: u64,
    pub deficit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceSnapshot {
    pub phase: VoicePhase,
    pub channel_id: Option<u64>,
    pub ping: Option<Duration>,
    pub counters: VoiceCounters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceClose {
    pub code: u16,
    pub reason: Arc<str>,
    pub by_remote: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceEvent {
    PhaseChanged(VoicePhase),
    Closed(VoiceClose),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceErrorKind {
    Cancelled,
    Shutdown,
    InvalidState,
    ConnectionFailed,
    Protocol,
    Overloaded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceError {
    pub kind: VoiceErrorKind,
    pub message: &'static str,
}

impl VoiceError {
    #[must_use]
    pub const fn new(kind: VoiceErrorKind, message: &'static str) -> Self {
        Self { kind, message }
    }
}

impl fmt::Display for VoiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for VoiceError {}

/// One Discord voice connection owned by a backend.
///
/// `send_opus` is a direct bounded handoff. Implementations must not queue an
/// unbounded number of frames or sleep to pace them; Mantle has already paced
/// the frame before this call.
pub trait VoiceConnection: Send + Sync {
    fn update(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>>;

    fn send_opus(
        &self,
        frame: TimedOpusFrame,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>>;

    fn snapshot(&self) -> VoiceFuture<'_, Result<VoiceSnapshot, VoiceError>>;

    /// Pull-based event delivery avoids an implicit unbounded core queue.
    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Option<VoiceEvent>, VoiceError>>;

    fn disconnect(&self) -> VoiceFuture<'_, Result<(), VoiceError>>;

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>>;
}

/// The only contract through which Crust core may use Discord voice details.
pub trait VoiceBackend: Send + Sync {
    fn connect(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Arc<dyn VoiceConnection>, VoiceError>>;

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_credentials_are_redacted_from_debug_output() {
        let info = VoiceConnectionInfo {
            guild_id: 1,
            user_id: 2,
            channel_id: 3,
            endpoint: "voice.example.invalid".into(),
            session_id: VoiceSecret::new("session-secret"),
            token: VoiceSecret::new("token-secret"),
        };
        let debug = format!("{info:?}");
        assert!(!debug.contains("session-secret"));
        assert!(!debug.contains("token-secret"));
        assert_eq!(debug.matches("[REDACTED]").count(), 2);
    }

    #[test]
    fn only_nonempty_bounded_twenty_millisecond_opus_frames_cross_the_boundary() {
        let payload: Arc<[u8]> = Arc::from([0xf8, 0xff, 0xfe]);
        let frame = TimedOpusFrame::new(
            7,
            Duration::from_millis(120),
            OPUS_FRAME_DURATION,
            payload.clone(),
        )
        .unwrap();
        assert_eq!(frame.sequence(), 7);
        assert_eq!(frame.source_position(), Duration::from_millis(120));
        assert_eq!(frame.payload(), payload.as_ref());
        assert_eq!(VOICE_PACING_AUTHORITY, VoicePacingAuthority::Mantle);

        assert_eq!(
            TimedOpusFrame::new(0, Duration::ZERO, Duration::from_millis(10), payload,),
            Err(VoiceFrameError::WrongDuration {
                duration: Duration::from_millis(10),
            })
        );
        assert_eq!(
            TimedOpusFrame::new(0, Duration::ZERO, OPUS_FRAME_DURATION, Arc::from([]),),
            Err(VoiceFrameError::EmptyPayload)
        );
        assert_eq!(
            TimedOpusFrame::new(
                0,
                Duration::ZERO,
                OPUS_FRAME_DURATION,
                Arc::from(vec![0; MAX_OPUS_PACKET_BYTES + 1]),
            ),
            Err(VoiceFrameError::PayloadTooLarge {
                size: MAX_OPUS_PACKET_BYTES + 1,
                maximum: MAX_OPUS_PACKET_BYTES,
            })
        );
    }
}
