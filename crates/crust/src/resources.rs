//! Explicit resource-limit inputs. P03 intentionally supplies no benchmark-
//! unsupported production defaults; callers must choose every value.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

pub const RESOURCE_LIMIT_KEYS: &[&str] = &[
    "maxSessions",
    "maxPlayers",
    "maxConcurrentLoads",
    "maxBatchDecodeTracks",
    "playerCommandCapacity",
    "websocketCriticalCapacity",
    "maxConcurrentSessionResumes",
    "maxConcurrentSourceRequests",
    "maxConcurrentVoiceConnects",
    "mediaEventCapacity",
    "maxMediaFrameBytes",
    "bestEffortTelemetryCapacity",
    "maxOwnedTasks",
    "shutdownTimeoutMs",
];

/// Complete production policy keys after the P17 resource audit. The P03
/// constant above remains the frozen early-phase overload-manifest vocabulary.
pub const CENTRAL_RESOURCE_LIMIT_KEYS: &[&str] = &[
    "maxRequestBodyBytes",
    "maxWebsocketMessageBytes",
    "maxSessions",
    "maxPlayers",
    "maxPlayersPerSession",
    "maxConcurrentLoads",
    "maxBatchDecodeTracks",
    "playerCommandCapacity",
    "websocketCriticalCapacity",
    "websocketSendTimeoutMs",
    "maxConcurrentSessionResumes",
    "maxConcurrentSourceRequests",
    "maxOutboundConnections",
    "maxConcurrentVoiceConnects",
    "maxRetainedJsonBytes",
    "maxJsonDepth",
    "maxJsonElements",
    "maxRoutePlannerFailures",
    "mediaEventCapacity",
    "maxMediaFrameBytes",
    "bestEffortTelemetryCapacity",
    "maxOwnedTasks",
    "shutdownTimeoutMs",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLimitConfig {
    pub max_request_body_bytes: usize,
    pub max_websocket_message_bytes: usize,
    pub max_sessions: usize,
    pub max_players: usize,
    pub max_players_per_session: usize,
    pub max_concurrent_loads: usize,
    pub max_batch_decode_tracks: usize,
    pub player_command_capacity: usize,
    pub websocket_critical_capacity: usize,
    pub websocket_send_timeout_ms: u64,
    pub max_concurrent_session_resumes: usize,
    pub max_concurrent_source_requests: usize,
    pub max_outbound_connections: usize,
    pub max_concurrent_voice_connects: usize,
    pub max_retained_json_bytes: usize,
    pub max_json_depth: usize,
    pub max_json_elements: usize,
    pub max_route_planner_failures: usize,
    pub media_event_capacity: usize,
    pub max_media_frame_bytes: usize,
    pub best_effort_telemetry_capacity: usize,
    pub max_owned_tasks: usize,
    pub shutdown_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_request_body_bytes: NonZeroUsize,
    pub max_websocket_message_bytes: NonZeroUsize,
    pub max_sessions: NonZeroUsize,
    pub max_players: NonZeroUsize,
    pub max_players_per_session: NonZeroUsize,
    pub max_concurrent_loads: NonZeroUsize,
    pub max_batch_decode_tracks: NonZeroUsize,
    pub player_command_capacity: NonZeroUsize,
    pub websocket_critical_capacity: NonZeroUsize,
    websocket_send_timeout_ms: NonZeroU64,
    pub max_concurrent_session_resumes: NonZeroUsize,
    pub max_concurrent_source_requests: NonZeroUsize,
    pub max_outbound_connections: NonZeroUsize,
    pub max_concurrent_voice_connects: NonZeroUsize,
    pub max_retained_json_bytes: NonZeroUsize,
    pub max_json_depth: NonZeroUsize,
    pub max_json_elements: NonZeroUsize,
    pub max_route_planner_failures: NonZeroUsize,
    pub media_event_capacity: NonZeroUsize,
    pub max_media_frame_bytes: NonZeroUsize,
    pub best_effort_telemetry_capacity: NonZeroUsize,
    pub max_owned_tasks: NonZeroUsize,
    shutdown_timeout_ms: NonZeroU64,
}

impl ResourceLimits {
    #[must_use]
    pub fn websocket_send_timeout(&self) -> Duration {
        Duration::from_millis(self.websocket_send_timeout_ms.get())
    }

    #[must_use]
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidResourceLimit {
    pub field: &'static str,
}

impl fmt::Display for InvalidResourceLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "resource limit {} must be nonzero", self.field)
    }
}

impl std::error::Error for InvalidResourceLimit {}

fn nonzero(value: usize, field: &'static str) -> Result<NonZeroUsize, InvalidResourceLimit> {
    NonZeroUsize::new(value).ok_or(InvalidResourceLimit { field })
}

impl TryFrom<ResourceLimitConfig> for ResourceLimits {
    type Error = InvalidResourceLimit;

    fn try_from(value: ResourceLimitConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            max_request_body_bytes: nonzero(value.max_request_body_bytes, "maxRequestBodyBytes")?,
            max_websocket_message_bytes: nonzero(
                value.max_websocket_message_bytes,
                "maxWebsocketMessageBytes",
            )?,
            max_sessions: nonzero(value.max_sessions, "maxSessions")?,
            max_players: nonzero(value.max_players, "maxPlayers")?,
            max_players_per_session: nonzero(
                value.max_players_per_session,
                "maxPlayersPerSession",
            )?,
            max_concurrent_loads: nonzero(value.max_concurrent_loads, "maxConcurrentLoads")?,
            max_batch_decode_tracks: nonzero(
                value.max_batch_decode_tracks,
                "maxBatchDecodeTracks",
            )?,
            player_command_capacity: nonzero(
                value.player_command_capacity,
                "playerCommandCapacity",
            )?,
            websocket_critical_capacity: nonzero(
                value.websocket_critical_capacity,
                "websocketCriticalCapacity",
            )?,
            websocket_send_timeout_ms: NonZeroU64::new(value.websocket_send_timeout_ms).ok_or(
                InvalidResourceLimit {
                    field: "websocketSendTimeoutMs",
                },
            )?,
            max_concurrent_session_resumes: nonzero(
                value.max_concurrent_session_resumes,
                "maxConcurrentSessionResumes",
            )?,
            max_concurrent_source_requests: nonzero(
                value.max_concurrent_source_requests,
                "maxConcurrentSourceRequests",
            )?,
            max_outbound_connections: nonzero(
                value.max_outbound_connections,
                "maxOutboundConnections",
            )?,
            max_concurrent_voice_connects: nonzero(
                value.max_concurrent_voice_connects,
                "maxConcurrentVoiceConnects",
            )?,
            max_retained_json_bytes: nonzero(
                value.max_retained_json_bytes,
                "maxRetainedJsonBytes",
            )?,
            max_json_depth: nonzero(value.max_json_depth, "maxJsonDepth")?,
            max_json_elements: nonzero(value.max_json_elements, "maxJsonElements")?,
            max_route_planner_failures: nonzero(
                value.max_route_planner_failures,
                "maxRoutePlannerFailures",
            )?,
            media_event_capacity: nonzero(value.media_event_capacity, "mediaEventCapacity")?,
            max_media_frame_bytes: nonzero(value.max_media_frame_bytes, "maxMediaFrameBytes")?,
            best_effort_telemetry_capacity: nonzero(
                value.best_effort_telemetry_capacity,
                "bestEffortTelemetryCapacity",
            )?,
            max_owned_tasks: nonzero(value.max_owned_tasks, "maxOwnedTasks")?,
            shutdown_timeout_ms: NonZeroU64::new(value.shutdown_timeout_ms).ok_or(
                InvalidResourceLimit {
                    field: "shutdownTimeoutMs",
                },
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ResourceLimitConfig {
        ResourceLimitConfig {
            max_request_body_bytes: 1,
            max_websocket_message_bytes: 2,
            max_sessions: 2,
            max_players: 3,
            max_players_per_session: 3,
            max_concurrent_loads: 4,
            max_batch_decode_tracks: 5,
            player_command_capacity: 6,
            websocket_critical_capacity: 7,
            websocket_send_timeout_ms: 8,
            max_concurrent_session_resumes: 8,
            max_concurrent_source_requests: 9,
            max_outbound_connections: 9,
            max_concurrent_voice_connects: 10,
            max_retained_json_bytes: 10,
            max_json_depth: 11,
            max_json_elements: 12,
            max_route_planner_failures: 13,
            media_event_capacity: 11,
            max_media_frame_bytes: 12,
            best_effort_telemetry_capacity: 13,
            max_owned_tasks: 14,
            shutdown_timeout_ms: 15,
        }
    }

    #[test]
    fn all_limits_are_explicit_and_nonzero() {
        let limits = ResourceLimits::try_from(config()).unwrap();
        assert_eq!(limits.max_sessions.get(), 2);
        assert_eq!(limits.max_owned_tasks.get(), 14);
        assert_eq!(limits.websocket_send_timeout(), Duration::from_millis(8));
        assert_eq!(limits.shutdown_timeout(), Duration::from_millis(15));
    }

    #[test]
    fn zero_is_rejected_with_the_exact_field() {
        let mut invalid = config();
        invalid.player_command_capacity = 0;
        assert_eq!(
            ResourceLimits::try_from(invalid).unwrap_err().field,
            "playerCommandCapacity"
        );
    }
}
