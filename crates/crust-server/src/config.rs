use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

const MAX_PASSWORD_BYTES: usize = 16 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_PLAYER_UPDATE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_STATS_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_PLAYER_EXECUTOR_SHARDS: usize = 1024;
const MAX_PLAYER_COMMAND_CAPACITY: usize = 1_048_576;
const MAX_CONCURRENT_LOADS: usize = 1_048_576;
const MAX_BATCH_DECODE_TRACKS: usize = 1_048_576;
const MAX_CONCURRENT_SOURCE_REQUESTS: usize = 1_048_576;
const MAX_CONCURRENT_VOICE_CONNECTS: usize = 1_048_576;

#[derive(Clone)]
pub struct ServerConfig {
    pub listen_address: IpAddr,
    pub port: u16,
    password: String,
    pub max_request_body_bytes: usize,
    pub websocket_critical_capacity: usize,
    pub max_sessions: usize,
    pub max_players: usize,
    pub max_concurrent_session_resumes: usize,
    pub player_executor_shards: usize,
    pub player_command_capacity: usize,
    pub max_concurrent_loads: usize,
    pub max_batch_decode_tracks: usize,
    pub max_concurrent_source_requests: usize,
    pub max_concurrent_voice_connects: usize,
    pub player_update_interval: Duration,
    pub stats_interval: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 2333,
            password: "youshallnotpass".to_owned(),
            max_request_body_bytes: 1024 * 1024,
            websocket_critical_capacity: 64,
            max_sessions: 1024,
            max_players: 4096,
            max_concurrent_session_resumes: 64,
            player_executor_shards: std::thread::available_parallelism()
                .map_or(1, usize::from)
                .clamp(1, 16),
            player_command_capacity: 256,
            max_concurrent_loads: 64,
            max_batch_decode_tracks: 1_000,
            max_concurrent_source_requests: 64,
            max_concurrent_voice_connects: 16,
            player_update_interval: Duration::from_secs(5),
            stats_interval: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("listen_address", &self.listen_address)
            .field("port", &self.port)
            .field("password", &"<redacted>")
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field(
                "websocket_critical_capacity",
                &self.websocket_critical_capacity,
            )
            .field("max_sessions", &self.max_sessions)
            .field("max_players", &self.max_players)
            .field(
                "max_concurrent_session_resumes",
                &self.max_concurrent_session_resumes,
            )
            .field("player_executor_shards", &self.player_executor_shards)
            .field("player_command_capacity", &self.player_command_capacity)
            .field("max_concurrent_loads", &self.max_concurrent_loads)
            .field("max_batch_decode_tracks", &self.max_batch_decode_tracks)
            .field(
                "max_concurrent_source_requests",
                &self.max_concurrent_source_requests,
            )
            .field(
                "max_concurrent_voice_connects",
                &self.max_concurrent_voice_connects,
            )
            .field("player_update_interval", &self.player_update_interval)
            .field("stats_interval", &self.stats_interval)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

impl ServerConfig {
    #[must_use]
    pub fn socket_address(&self) -> SocketAddr {
        SocketAddr::new(self.listen_address, self.port)
    }

    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Result<Self, ConfigError> {
        self.password = password.into();
        self.validate()?;
        Ok(self)
    }

    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        if let Some(path) = path {
            let text = std::fs::read_to_string(path).map_err(ConfigError::Read)?;
            let file: ConfigFile = serde_saphyr::from_str(&text)
                .map_err(|error| ConfigError::Yaml(error.to_string()))?;
            config.apply_file(file);
        }
        config.apply_environment()?;
        config.validate()?;
        Ok(config)
    }

    fn apply_file(&mut self, file: ConfigFile) {
        if let Some(server) = file.server {
            if let Some(address) = server.address {
                self.listen_address = address;
            }
            if let Some(port) = server.port {
                self.port = port;
            }
        }
        if let Some(lavalink) = file.lavalink
            && let Some(server) = lavalink.server
        {
            if let Some(password) = server.password {
                self.password = password;
            }
            if let Some(seconds) = server.player_update_interval {
                self.player_update_interval = Duration::from_secs(seconds);
            }
        }
        if let Some(crust) = file.crust {
            if let Some(value) = crust.max_request_body_bytes {
                self.max_request_body_bytes = value;
            }
            if let Some(value) = crust.websocket_critical_capacity {
                self.websocket_critical_capacity = value;
            }
            if let Some(value) = crust.max_sessions {
                self.max_sessions = value;
            }
            if let Some(value) = crust.max_players {
                self.max_players = value;
            }
            if let Some(value) = crust.max_concurrent_session_resumes {
                self.max_concurrent_session_resumes = value;
            }
            if let Some(value) = crust.player_executor_shards {
                self.player_executor_shards = value;
            }
            if let Some(value) = crust.player_command_capacity {
                self.player_command_capacity = value;
            }
            if let Some(value) = crust.max_concurrent_loads {
                self.max_concurrent_loads = value;
            }
            if let Some(value) = crust.max_batch_decode_tracks {
                self.max_batch_decode_tracks = value;
            }
            if let Some(value) = crust.max_concurrent_source_requests {
                self.max_concurrent_source_requests = value;
            }
            if let Some(value) = crust.max_concurrent_voice_connects {
                self.max_concurrent_voice_connects = value;
            }
            if let Some(value) = crust.player_update_interval_ms {
                self.player_update_interval = Duration::from_millis(value);
            }
            if let Some(value) = crust.stats_interval_ms {
                self.stats_interval = Duration::from_millis(value);
            }
            if let Some(value) = crust.shutdown_timeout_ms {
                self.shutdown_timeout = Duration::from_millis(value);
            }
        }
    }

    fn apply_environment(&mut self) -> Result<(), ConfigError> {
        if let Some(value) = environment("SERVER_ADDRESS") {
            self.listen_address = value
                .parse()
                .map_err(|_| ConfigError::Environment("SERVER_ADDRESS"))?;
        }
        if let Some(value) = environment("SERVER_PORT") {
            self.port = value
                .parse()
                .map_err(|_| ConfigError::Environment("SERVER_PORT"))?;
        }
        if let Some(value) =
            environment("LAVALINK_SERVER_PASSWORD").or_else(|| environment("CRUST_PASSWORD"))
        {
            self.password = value;
        }
        if let Some(value) = environment("LAVALINK_SERVER_PLAYER_UPDATE_INTERVAL") {
            let seconds = value
                .parse()
                .map_err(|_| ConfigError::Environment("LAVALINK_SERVER_PLAYER_UPDATE_INTERVAL"))?;
            self.player_update_interval = Duration::from_secs(seconds);
        }
        if let Some(value) = environment("CRUST_MAX_REQUEST_BODY_BYTES") {
            self.max_request_body_bytes = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_REQUEST_BODY_BYTES"))?;
        }
        if let Some(value) = environment("CRUST_WEBSOCKET_CRITICAL_CAPACITY") {
            self.websocket_critical_capacity = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_WEBSOCKET_CRITICAL_CAPACITY"))?;
        }
        if let Some(value) = environment("CRUST_MAX_SESSIONS") {
            self.max_sessions = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_SESSIONS"))?;
        }
        if let Some(value) = environment("CRUST_MAX_PLAYERS") {
            self.max_players = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_PLAYERS"))?;
        }
        if let Some(value) = environment("CRUST_MAX_CONCURRENT_SESSION_RESUMES") {
            self.max_concurrent_session_resumes = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_CONCURRENT_SESSION_RESUMES"))?;
        }
        if let Some(value) = environment("CRUST_PLAYER_EXECUTOR_SHARDS") {
            self.player_executor_shards = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_PLAYER_EXECUTOR_SHARDS"))?;
        }
        if let Some(value) = environment("CRUST_PLAYER_COMMAND_CAPACITY") {
            self.player_command_capacity = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_PLAYER_COMMAND_CAPACITY"))?;
        }
        if let Some(value) = environment("CRUST_MAX_CONCURRENT_LOADS") {
            self.max_concurrent_loads = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_CONCURRENT_LOADS"))?;
        }
        if let Some(value) = environment("CRUST_MAX_BATCH_DECODE_TRACKS") {
            self.max_batch_decode_tracks = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_BATCH_DECODE_TRACKS"))?;
        }
        if let Some(value) = environment("CRUST_MAX_CONCURRENT_SOURCE_REQUESTS") {
            self.max_concurrent_source_requests = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_CONCURRENT_SOURCE_REQUESTS"))?;
        }
        if let Some(value) = environment("CRUST_MAX_CONCURRENT_VOICE_CONNECTS") {
            self.max_concurrent_voice_connects = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_MAX_CONCURRENT_VOICE_CONNECTS"))?;
        }
        if let Some(value) = environment("CRUST_PLAYER_UPDATE_INTERVAL_MS") {
            let milliseconds = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_PLAYER_UPDATE_INTERVAL_MS"))?;
            self.player_update_interval = Duration::from_millis(milliseconds);
        }
        if let Some(value) = environment("CRUST_STATS_INTERVAL_MS") {
            let milliseconds = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_STATS_INTERVAL_MS"))?;
            self.stats_interval = Duration::from_millis(milliseconds);
        }
        if let Some(value) = environment("CRUST_SHUTDOWN_TIMEOUT_MS") {
            let milliseconds = value
                .parse()
                .map_err(|_| ConfigError::Environment("CRUST_SHUTDOWN_TIMEOUT_MS"))?;
            self.shutdown_timeout = Duration::from_millis(milliseconds);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.password.is_empty() || self.password.len() > MAX_PASSWORD_BYTES {
            return Err(ConfigError::Invalid("password must be 1..=16384 bytes"));
        }
        if self.max_request_body_bytes == 0 || self.max_request_body_bytes > MAX_REQUEST_BODY_BYTES
        {
            return Err(ConfigError::Invalid(
                "max_request_body_bytes must be 1..=16777216",
            ));
        }
        if self.websocket_critical_capacity == 0 {
            return Err(ConfigError::Invalid(
                "websocket_critical_capacity must be non-zero",
            ));
        }
        if self.max_sessions == 0 {
            return Err(ConfigError::Invalid("max_sessions must be non-zero"));
        }
        if self.max_players == 0 {
            return Err(ConfigError::Invalid("max_players must be non-zero"));
        }
        if self.max_concurrent_session_resumes == 0 {
            return Err(ConfigError::Invalid(
                "max_concurrent_session_resumes must be non-zero",
            ));
        }
        if self.player_executor_shards == 0
            || self.player_executor_shards > MAX_PLAYER_EXECUTOR_SHARDS
        {
            return Err(ConfigError::Invalid(
                "player_executor_shards must be 1..=1024",
            ));
        }
        if self.player_command_capacity == 0
            || self.player_command_capacity > MAX_PLAYER_COMMAND_CAPACITY
        {
            return Err(ConfigError::Invalid(
                "player_command_capacity must be 1..=1048576",
            ));
        }
        if self.max_concurrent_loads == 0 || self.max_concurrent_loads > MAX_CONCURRENT_LOADS {
            return Err(ConfigError::Invalid(
                "max_concurrent_loads must be 1..=1048576",
            ));
        }
        if self.max_batch_decode_tracks == 0
            || self.max_batch_decode_tracks > MAX_BATCH_DECODE_TRACKS
        {
            return Err(ConfigError::Invalid(
                "max_batch_decode_tracks must be 1..=1048576",
            ));
        }
        if self.max_concurrent_source_requests == 0
            || self.max_concurrent_source_requests > MAX_CONCURRENT_SOURCE_REQUESTS
        {
            return Err(ConfigError::Invalid(
                "max_concurrent_source_requests must be 1..=1048576",
            ));
        }
        if self.max_concurrent_voice_connects == 0
            || self.max_concurrent_voice_connects > MAX_CONCURRENT_VOICE_CONNECTS
        {
            return Err(ConfigError::Invalid(
                "max_concurrent_voice_connects must be 1..=1048576",
            ));
        }
        if self.player_update_interval.is_zero()
            || self.player_update_interval > MAX_PLAYER_UPDATE_INTERVAL
        {
            return Err(ConfigError::Invalid(
                "player_update_interval must be between 1 ms and 24 hours",
            ));
        }
        if self.stats_interval.is_zero() || self.stats_interval > MAX_STATS_INTERVAL {
            return Err(ConfigError::Invalid(
                "stats_interval must be between 1 ms and 24 hours",
            ));
        }
        if self.shutdown_timeout.is_zero() || self.shutdown_timeout > MAX_SHUTDOWN_TIMEOUT {
            return Err(ConfigError::Invalid(
                "shutdown_timeout must be between 1 ms and 5 minutes",
            ));
        }
        Ok(())
    }
}

fn environment(name: &'static str) -> Option<String> {
    env::var_os(name).map(|value| value.to_string_lossy().into_owned())
}

#[derive(Debug)]
pub enum ConfigError {
    Read(std::io::Error),
    Yaml(String),
    Environment(&'static str),
    Invalid(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "failed to read configuration: {error}"),
            Self::Yaml(error) => write!(formatter, "invalid YAML configuration: {error}"),
            Self::Environment(name) => write!(formatter, "invalid environment value: {name}"),
            Self::Invalid(message) => write!(formatter, "invalid configuration: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    server: Option<FileServer>,
    lavalink: Option<FileLavalink>,
    crust: Option<FileCrust>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileServer {
    address: Option<IpAddr>,
    port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLavalink {
    server: Option<FileLavalinkServer>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileLavalinkServer {
    password: Option<String>,
    player_update_interval: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileCrust {
    max_request_body_bytes: Option<usize>,
    websocket_critical_capacity: Option<usize>,
    max_sessions: Option<usize>,
    max_players: Option<usize>,
    max_concurrent_session_resumes: Option<usize>,
    player_executor_shards: Option<usize>,
    player_command_capacity: Option<usize>,
    max_concurrent_loads: Option<usize>,
    max_batch_decode_tracks: Option<usize>,
    max_concurrent_source_requests: Option<usize>,
    max_concurrent_voice_connects: Option<usize>,
    player_update_interval_ms: Option<u64>,
    stats_interval_ms: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_password() {
        let config = ServerConfig::default()
            .with_password("not-for-logs")
            .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("not-for-logs"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn strict_yaml_rejects_unknown_security_keys() {
        let error = serde_saphyr::from_str::<ConfigFile>(
            "lavalink:\n  server:\n    password: secret\n    passwrod: typo\n",
        )
        .unwrap_err();
        assert!(error.to_string().contains("passwrod"));
    }

    #[test]
    fn p08_media_limits_load_from_yaml_and_reject_zero() {
        let file: ConfigFile = serde_saphyr::from_str(
            "crust:\n  maxConcurrentLoads: 2\n  maxBatchDecodeTracks: 3\n  maxConcurrentSourceRequests: 4\n  maxConcurrentVoiceConnects: 5\n",
        )
        .unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file);
        assert_eq!(config.max_concurrent_loads, 2);
        assert_eq!(config.max_batch_decode_tracks, 3);
        assert_eq!(config.max_concurrent_source_requests, 4);
        assert_eq!(config.max_concurrent_voice_connects, 5);
        assert!(config.validate().is_ok());

        config.max_batch_decode_tracks = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Invalid(
                "max_batch_decode_tracks must be 1..=1048576"
            ))
        ));
    }

    #[test]
    fn p11_player_update_interval_supports_lavalink_seconds_and_crust_milliseconds() {
        let file: ConfigFile = serde_saphyr::from_str(
            "lavalink:\n  server:\n    playerUpdateInterval: 7\ncrust:\n  playerUpdateIntervalMs: 125\n",
        )
        .unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file);
        assert_eq!(config.player_update_interval, Duration::from_millis(125));
        assert!(config.validate().is_ok());

        let file: ConfigFile =
            serde_saphyr::from_str("lavalink:\n  server:\n    playerUpdateInterval: 7\n").unwrap();
        config.apply_file(file);
        assert_eq!(config.player_update_interval, Duration::from_secs(7));

        config.player_update_interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.player_update_interval = MAX_PLAYER_UPDATE_INTERVAL + Duration::from_millis(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn p12_stats_interval_is_bounded_and_configurable() {
        let file: ConfigFile = serde_saphyr::from_str("crust:\n  statsIntervalMs: 25\n").unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file);
        assert_eq!(config.stats_interval, Duration::from_millis(25));
        assert!(config.validate().is_ok());

        config.stats_interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.stats_interval = MAX_STATS_INTERVAL + Duration::from_millis(1);
        assert!(config.validate().is_err());
    }
}
