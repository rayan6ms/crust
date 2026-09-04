use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use crust::routeplanner::{RoutePlanner, RoutePlannerConfig, RoutePlannerStrategy};
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
const MAX_BUFFER_DURATION_MS: u64 = 60_000;
const MAX_FRAME_BUFFER_DURATION_MS: u64 = 120_000;
// Mantle's bounded YouTube source contract accepts at most 64 playlist pages.
const MAX_PLAYLIST_LOAD_LIMIT: usize = 64;
const MAX_PROXY_HOST_BYTES: usize = 4 * 1024;
const MAX_ENDPOINT_BYTES: usize = 256;

/// Quality passed to the media boundary when Mantle exposes the corresponding setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResamplingQuality {
    #[default]
    Low,
    Medium,
    High,
}

impl FromStr for ResamplingQuality {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().as_str() {
            "LOW" => Ok(Self::Low),
            "MEDIUM" => Ok(Self::Medium),
            "HIGH" => Ok(Self::High),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Http2Config {
    pub enabled: bool,
}

/// Lavalink source enablement switches. YouTube remains implemented by Mantle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceConfig {
    pub youtube: bool,
    pub bandcamp: bool,
    pub soundcloud: bool,
    pub twitch: bool,
    pub vimeo: bool,
    pub nico: bool,
    pub http: bool,
    pub local: bool,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            // These are the defaults of Lavalink's typed AudioSourcesConfig.
            // The reference example disables the legacy Youtube manager in
            // its sample YAML, but an omitted key keeps it enabled. Crust's
            // Mantle adapter is the implementation of that manager.
            youtube: true,
            bandcamp: true,
            soundcloud: true,
            twitch: true,
            vimeo: true,
            nico: false,
            http: true,
            local: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterConfig {
    pub volume: bool,
    pub equalizer: bool,
    pub karaoke: bool,
    pub timescale: bool,
    pub tremolo: bool,
    pub vibrato: bool,
    pub distortion: bool,
    pub rotation: bool,
    pub channel_mix: bool,
    pub low_pass: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            volume: true,
            equalizer: true,
            karaoke: true,
            timescale: true,
            tremolo: true,
            vibrato: true,
            distortion: true,
            rotation: true,
            channel_mix: true,
            low_pass: true,
        }
    }
}

/// Media settings owned by the typed operator contract. Mantle owns actual codec and graph work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaConfig {
    pub non_allocating_frame_buffer: bool,
    pub buffer_duration: Duration,
    pub frame_buffer_duration: Duration,
    pub opus_encoding_quality: u8,
    pub resampling_quality: ResamplingQuality,
    pub track_stuck_threshold: Duration,
    pub use_seek_ghosting: bool,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            non_allocating_frame_buffer: false,
            buffer_duration: Duration::from_millis(400),
            frame_buffer_duration: Duration::from_millis(5_000),
            opus_encoding_quality: 10,
            resampling_quality: ResamplingQuality::Low,
            track_stuck_threshold: Duration::from_millis(10_000),
            use_seek_ghosting: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchConfig {
    pub youtube_playlist_load_limit: usize,
    pub youtube_enabled: bool,
    pub soundcloud_enabled: bool,
    pub soundcloud_filter_out_preview_tracks: bool,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            youtube_playlist_load_limit: 6,
            youtube_enabled: true,
            soundcloud_enabled: true,
            soundcloud_filter_out_preview_tracks: false,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    password: Option<String>,
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl ProxyConfig {
    #[must_use]
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
    #[must_use]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeoutConfig {
    pub connect: Duration,
    pub connection_request: Duration,
    pub socket: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect: Duration::from_millis(3_000),
            connection_request: Duration::from_millis(3_000),
            socket: Duration::from_millis(3_000),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpSourceConfig {
    pub proxy: Option<ProxyConfig>,
    pub timeouts: TimeoutConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePlannerSettings {
    pub strategy: RoutePlannerStrategy,
    pub ip_blocks: Vec<String>,
    pub excluded_ips: Vec<IpAddr>,
    pub search_triggers_fail: bool,
    pub retry_limit: Option<i32>,
    pub max_failures: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrometheusConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    pub prometheus: PrometheusConfig,
    pub endpoint: String,
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus: PrometheusConfig::default(),
            endpoint: "/metrics".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestLoggingConfig {
    pub enabled: bool,
    pub include_client_info: bool,
    pub include_headers: bool,
    pub include_query_string: bool,
    pub include_payload: bool,
    pub max_payload_length: usize,
    pub before_request: bool,
}
impl Default for RequestLoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            include_client_info: true,
            include_headers: false,
            include_query_string: true,
            include_payload: true,
            max_payload_length: 10_000,
            before_request: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggingConfig {
    pub request: RequestLoggingConfig,
    pub root_level: String,
    pub lavalink_level: String,
}
impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            request: RequestLoggingConfig::default(),
            root_level: "INFO".to_owned(),
            lavalink_level: "INFO".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CliOverrides {
    pub address: Option<IpAddr>,
    pub port: Option<u16>,
    pub password: Option<String>,
    pub http2: Option<bool>,
}

#[derive(Clone)]
pub struct ServerConfig {
    pub listen_address: IpAddr,
    pub port: u16,
    password: String,
    pub http2: Http2Config,
    pub sources: SourceConfig,
    pub filters: FilterConfig,
    pub media: MediaConfig,
    pub search: SearchConfig,
    pub http_source: HttpSourceConfig,
    pub route_planner: Option<RoutePlannerSettings>,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
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
            http2: Http2Config::default(),
            sources: SourceConfig::default(),
            filters: FilterConfig::default(),
            media: MediaConfig::default(),
            search: SearchConfig::default(),
            http_source: HttpSourceConfig::default(),
            route_planner: None,
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
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
            .field("http2", &self.http2)
            .field("sources", &self.sources)
            .field("filters", &self.filters)
            .field("media", &self.media)
            .field("search", &self.search)
            .field("http_source", &self.http_source)
            .field(
                "route_planner",
                &self.route_planner.as_ref().map(|_| "configured"),
            )
            .field("metrics", &self.metrics)
            .field("logging", &self.logging)
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

    /// Loads defaults, then YAML, then environment, in that explicit order.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_with_cli(path, &CliOverrides::default())
    }

    /// Loads with the documented precedence `defaults < YAML < environment < CLI`.
    pub fn load_with_cli(path: Option<&Path>, cli: &CliOverrides) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        if let Some(path) = path {
            let text = std::fs::read_to_string(path).map_err(ConfigError::Read)?;
            let file: ConfigFile = serde_saphyr::from_str(&text)
                .map_err(|error| ConfigError::Yaml(error.to_string()))?;
            config.apply_file(file)?;
        }
        config.apply_environment()?;
        config.apply_cli(cli)?;
        config.validate()?;
        let _ = config.route_planner()?;
        Ok(config)
    }

    /// Converts the optional Lavalink `ratelimit` block into Crust's bounded planner.
    pub fn route_planner(&self) -> Result<RoutePlanner, ConfigError> {
        let Some(settings) = &self.route_planner else {
            return Ok(RoutePlanner::disabled());
        };
        let mut planner = RoutePlannerConfig::new(settings.strategy, settings.ip_blocks.clone());
        planner.excluded_addresses = settings.excluded_ips.clone();
        planner.search_triggers_fail = settings.search_triggers_fail;
        planner.max_failures = settings.max_failures;
        RoutePlanner::configured(planner)
            .map_err(|error| ConfigError::RoutePlanner(error.to_string()))
    }

    fn apply_file(&mut self, file: ConfigFile) -> Result<(), ConfigError> {
        if let Some(server) = file.server {
            if let Some(address) = server.address {
                self.listen_address = address;
            }
            if let Some(port) = server.port {
                self.port = port;
            }
            if let Some(enabled) = server.http2.and_then(|http2| http2.enabled) {
                self.http2.enabled = enabled;
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
            if let Some(sources) = server.sources {
                sources.apply_to(&mut self.sources);
            }
            if let Some(filters) = server.filters {
                filters.apply_to(&mut self.filters);
            }
            if let Some(value) = server.non_allocating_frame_buffer {
                self.media.non_allocating_frame_buffer = value;
            }
            if let Some(value) = server.buffer_duration_ms {
                self.media.buffer_duration = Duration::from_millis(value);
            }
            if let Some(value) = server.frame_buffer_duration_ms {
                self.media.frame_buffer_duration = Duration::from_millis(value);
            }
            if let Some(value) = server.opus_encoding_quality {
                self.media.opus_encoding_quality = value;
            }
            if let Some(value) = server.resampling_quality {
                self.media.resampling_quality = value.parse().map_err(|()| {
                    ConfigError::Invalid("resampling_quality must be LOW, MEDIUM, or HIGH")
                })?;
            }
            if let Some(value) = server.track_stuck_threshold_ms {
                self.media.track_stuck_threshold = Duration::from_millis(value);
            }
            if let Some(value) = server.use_seek_ghosting {
                self.media.use_seek_ghosting = value;
            }
            if let Some(value) = server.youtube_playlist_load_limit {
                self.search.youtube_playlist_load_limit = value;
            }
            if let Some(value) = server.youtube_search_enabled {
                self.search.youtube_enabled = value;
            }
            if let Some(value) = server.soundcloud_search_enabled {
                self.search.soundcloud_enabled = value;
            }
            if let Some(value) = server.soundcloud_filter_out_preview_tracks {
                self.search.soundcloud_filter_out_preview_tracks = value;
            }
            if let Some(timeouts) = server.timeouts {
                timeouts.apply_to(&mut self.http_source.timeouts);
            }
            if let Some(http) = server.http_config {
                self.http_source.proxy = http.into_proxy()?;
            }
            if let Some(rate_limit) = server.rate_limit {
                self.route_planner = Some(rate_limit.into_settings()?);
            }
        }
        if let Some(metrics) = file.metrics
            && let Some(prometheus) = metrics.prometheus
        {
            if let Some(value) = prometheus.enabled {
                self.metrics.prometheus.enabled = value;
            }
            if let Some(value) = prometheus.endpoint {
                self.metrics.endpoint = value;
            }
        }
        if let Some(logging) = file.logging {
            if let Some(request) = logging.request {
                request.apply_to(&mut self.logging.request);
            }
            if let Some(level) = logging.level {
                if let Some(value) = level.root {
                    self.logging.root_level = value;
                }
                if let Some(value) = level.lavalink {
                    self.logging.lavalink_level = value;
                }
            }
        }
        if let Some(crust) = file.crust {
            crust.apply_to(self);
        }
        Ok(())
    }

    fn apply_environment(&mut self) -> Result<(), ConfigError> {
        if let Some(value) = parse_environment::<IpAddr>("SERVER_ADDRESS")? {
            self.listen_address = value;
        }
        if let Some(value) = parse_environment::<u16>("SERVER_PORT")? {
            self.port = value;
        }
        if let Some(value) =
            environment("LAVALINK_SERVER_PASSWORD").or_else(|| environment("CRUST_PASSWORD"))
        {
            self.password = value;
        }
        if let Some(value) = parse_environment::<bool>("SERVER_HTTP2_ENABLED")?
            .or(parse_environment::<bool>("CRUST_HTTP2_ENABLED")?)
        {
            self.http2.enabled = value;
        }
        if let Some(value) = parse_environment::<u64>("LAVALINK_SERVER_PLAYER_UPDATE_INTERVAL")? {
            self.player_update_interval = Duration::from_secs(value);
        }
        if let Some(value) = parse_environment::<u64>("CRUST_PLAYER_UPDATE_INTERVAL_MS")? {
            self.player_update_interval = Duration::from_millis(value);
        }
        if let Some(value) = parse_environment::<u64>("CRUST_STATS_INTERVAL_MS")? {
            self.stats_interval = Duration::from_millis(value);
        }
        if let Some(value) = parse_environment::<u64>("CRUST_SHUTDOWN_TIMEOUT_MS")? {
            self.shutdown_timeout = Duration::from_millis(value);
        }
        macro_rules! env_field {
            ($name:literal, $field:expr, $type:ty) => {
                if let Some(value) = parse_environment::<$type>($name)? {
                    $field = value;
                }
            };
        }
        env_field!(
            "CRUST_MAX_REQUEST_BODY_BYTES",
            self.max_request_body_bytes,
            usize
        );
        env_field!(
            "CRUST_WEBSOCKET_CRITICAL_CAPACITY",
            self.websocket_critical_capacity,
            usize
        );
        env_field!("CRUST_MAX_SESSIONS", self.max_sessions, usize);
        env_field!("CRUST_MAX_PLAYERS", self.max_players, usize);
        env_field!(
            "CRUST_MAX_CONCURRENT_SESSION_RESUMES",
            self.max_concurrent_session_resumes,
            usize
        );
        env_field!(
            "CRUST_PLAYER_EXECUTOR_SHARDS",
            self.player_executor_shards,
            usize
        );
        env_field!(
            "CRUST_PLAYER_COMMAND_CAPACITY",
            self.player_command_capacity,
            usize
        );
        env_field!(
            "CRUST_MAX_CONCURRENT_LOADS",
            self.max_concurrent_loads,
            usize
        );
        env_field!(
            "CRUST_MAX_BATCH_DECODE_TRACKS",
            self.max_batch_decode_tracks,
            usize
        );
        env_field!(
            "CRUST_MAX_CONCURRENT_SOURCE_REQUESTS",
            self.max_concurrent_source_requests,
            usize
        );
        env_field!(
            "CRUST_MAX_CONCURRENT_VOICE_CONNECTS",
            self.max_concurrent_voice_connects,
            usize
        );
        env_field!(
            "LAVALINK_SERVER_SOURCES_YOUTUBE",
            self.sources.youtube,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_SOURCES_BANDCAMP",
            self.sources.bandcamp,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_SOURCES_SOUNDCLOUD",
            self.sources.soundcloud,
            bool
        );
        env_field!("LAVALINK_SERVER_SOURCES_TWITCH", self.sources.twitch, bool);
        env_field!("LAVALINK_SERVER_SOURCES_VIMEO", self.sources.vimeo, bool);
        env_field!("LAVALINK_SERVER_SOURCES_NICO", self.sources.nico, bool);
        env_field!("LAVALINK_SERVER_SOURCES_HTTP", self.sources.http, bool);
        env_field!("LAVALINK_SERVER_SOURCES_LOCAL", self.sources.local, bool);
        env_field!("LAVALINK_SERVER_FILTERS_VOLUME", self.filters.volume, bool);
        env_field!(
            "LAVALINK_SERVER_FILTERS_EQUALIZER",
            self.filters.equalizer,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_KARAOKE",
            self.filters.karaoke,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_TIMESCALE",
            self.filters.timescale,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_TREMOLO",
            self.filters.tremolo,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_VIBRATO",
            self.filters.vibrato,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_DISTORTION",
            self.filters.distortion,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_FILTERS_ROTATION",
            self.filters.rotation,
            bool
        );
        if let Some(value) = parse_environment::<bool>("LAVALINK_SERVER_FILTERS_CHANNEL_MIX")?.or(
            parse_environment::<bool>("LAVALINK_SERVER_FILTERS_CHANNELMIX")?,
        ) {
            self.filters.channel_mix = value;
        }
        if let Some(value) =
            parse_environment::<bool>("LAVALINK_SERVER_FILTERS_LOW_PASS")?.or(parse_environment::<
                bool,
            >(
                "LAVALINK_SERVER_FILTERS_LOWPASS",
            )?)
        {
            self.filters.low_pass = value;
        }
        env_field!(
            "LAVALINK_SERVER_NON_ALLOCATING_FRAME_BUFFER",
            self.media.non_allocating_frame_buffer,
            bool
        );
        if let Some(value) = parse_environment::<u64>("LAVALINK_SERVER_BUFFER_DURATION_MS")? {
            self.media.buffer_duration = Duration::from_millis(value);
        }
        if let Some(value) = parse_environment::<u64>("LAVALINK_SERVER_FRAME_BUFFER_DURATION_MS")? {
            self.media.frame_buffer_duration = Duration::from_millis(value);
        }
        env_field!(
            "LAVALINK_SERVER_OPUS_ENCODING_QUALITY",
            self.media.opus_encoding_quality,
            u8
        );
        if let Some(value) = environment("LAVALINK_SERVER_RESAMPLING_QUALITY") {
            self.media.resampling_quality = value
                .parse()
                .map_err(|()| ConfigError::Environment("LAVALINK_SERVER_RESAMPLING_QUALITY"))?;
        }
        if let Some(value) = parse_environment::<u64>("LAVALINK_SERVER_TRACK_STUCK_THRESHOLD_MS")? {
            self.media.track_stuck_threshold = Duration::from_millis(value);
        }
        env_field!(
            "LAVALINK_SERVER_USE_SEEK_GHOSTING",
            self.media.use_seek_ghosting,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_YOUTUBE_PLAYLIST_LOAD_LIMIT",
            self.search.youtube_playlist_load_limit,
            usize
        );
        env_field!(
            "LAVALINK_SERVER_YOUTUBE_SEARCH_ENABLED",
            self.search.youtube_enabled,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_SOUNDCLOUD_SEARCH_ENABLED",
            self.search.soundcloud_enabled,
            bool
        );
        env_field!(
            "LAVALINK_SERVER_SOUNDCLOUD_FILTER_OUT_PREVIEW_TRACKS",
            self.search.soundcloud_filter_out_preview_tracks,
            bool
        );
        if let Some(value) =
            parse_environment::<u64>("LAVALINK_SERVER_TIMEOUTS_CONNECT_TIMEOUT_MS")?
        {
            self.http_source.timeouts.connect = Duration::from_millis(value);
        }
        if let Some(value) =
            parse_environment::<u64>("LAVALINK_SERVER_TIMEOUTS_CONNECTION_REQUEST_TIMEOUT_MS")?
        {
            self.http_source.timeouts.connection_request = Duration::from_millis(value);
        }
        if let Some(value) = parse_environment::<u64>("LAVALINK_SERVER_TIMEOUTS_SOCKET_TIMEOUT_MS")?
        {
            self.http_source.timeouts.socket = Duration::from_millis(value);
        }
        if let Some(value) = environment("LAVALINK_SERVER_HTTP_CONFIG_PROXY_HOST") {
            let port = self
                .http_source
                .proxy
                .as_ref()
                .map_or(3128, |proxy| proxy.port);
            self.http_source.proxy = Some(ProxyConfig {
                host: value,
                port,
                username: None,
                password: None,
            });
        }
        if let Some(value) = parse_environment::<u16>("LAVALINK_SERVER_HTTP_CONFIG_PROXY_PORT")? {
            let proxy = self.http_source.proxy.get_or_insert_with(|| ProxyConfig {
                host: "localhost".to_owned(),
                port: 3128,
                username: None,
                password: None,
            });
            proxy.port = value;
        }
        if let Some(value) = environment("LAVALINK_SERVER_HTTP_CONFIG_PROXY_USER") {
            let proxy = self.http_source.proxy.get_or_insert_with(|| ProxyConfig {
                host: "localhost".to_owned(),
                port: 3128,
                username: None,
                password: None,
            });
            proxy.username = Some(value);
        }
        if let Some(value) = environment("LAVALINK_SERVER_HTTP_CONFIG_PROXY_PASSWORD") {
            let proxy = self.http_source.proxy.get_or_insert_with(|| ProxyConfig {
                host: "localhost".to_owned(),
                port: 3128,
                username: None,
                password: None,
            });
            proxy.password = Some(value);
        }
        env_field!(
            "METRICS_PROMETHEUS_ENABLED",
            self.metrics.prometheus.enabled,
            bool
        );
        if let Some(value) = environment("METRICS_PROMETHEUS_ENDPOINT") {
            self.metrics.endpoint = value;
        }
        env_field!(
            "LOGGING_REQUEST_ENABLED",
            self.logging.request.enabled,
            bool
        );
        env_field!(
            "LOGGING_REQUEST_INCLUDE_CLIENT_INFO",
            self.logging.request.include_client_info,
            bool
        );
        env_field!(
            "LOGGING_REQUEST_INCLUDE_HEADERS",
            self.logging.request.include_headers,
            bool
        );
        env_field!(
            "LOGGING_REQUEST_INCLUDE_QUERY_STRING",
            self.logging.request.include_query_string,
            bool
        );
        env_field!(
            "LOGGING_REQUEST_INCLUDE_PAYLOAD",
            self.logging.request.include_payload,
            bool
        );
        env_field!(
            "LOGGING_REQUEST_MAX_PAYLOAD_LENGTH",
            self.logging.request.max_payload_length,
            usize
        );
        env_field!(
            "LOGGING_REQUEST_BEFORE_REQUEST",
            self.logging.request.before_request,
            bool
        );
        if let Some(strategy) = environment("LAVALINK_SERVER_RATELIMIT_STRATEGY") {
            let planner = self
                .route_planner
                .get_or_insert_with(default_route_settings);
            planner.strategy = parse_strategy(&strategy).ok_or(ConfigError::Environment(
                "LAVALINK_SERVER_RATELIMIT_STRATEGY",
            ))?;
        }
        if let Some(blocks) = environment("LAVALINK_SERVER_RATELIMIT_IPBLOCKS")
            .or_else(|| environment("LAVALINK_SERVER_RATELIMIT_IP_BLOCKS"))
        {
            let planner = self
                .route_planner
                .get_or_insert_with(default_route_settings);
            planner.ip_blocks = blocks
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .collect();
        }
        if let Some(excluded) = environment("LAVALINK_SERVER_RATELIMIT_EXCLUDEDIPS")
            .or_else(|| environment("LAVALINK_SERVER_RATELIMIT_EXCLUDED_IPS"))
        {
            let planner = self
                .route_planner
                .get_or_insert_with(default_route_settings);
            planner.excluded_ips = excluded
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|value| {
                    value.parse().map_err(|_| {
                        ConfigError::Environment("LAVALINK_SERVER_RATELIMIT_EXCLUDEDIPS")
                    })
                })
                .collect::<Result<Vec<IpAddr>, _>>()?;
        }
        if let Some(value) = parse_environment::<i32>("LAVALINK_SERVER_RATELIMIT_RETRY_LIMIT")? {
            let planner = self
                .route_planner
                .get_or_insert_with(default_route_settings);
            planner.retry_limit = Some(value);
        }
        if let Some(value) =
            parse_environment::<bool>("LAVALINK_SERVER_RATELIMIT_SEARCH_TRIGGERS_FAIL")?
        {
            let planner = self
                .route_planner
                .get_or_insert_with(default_route_settings);
            planner.search_triggers_fail = value;
        }
        Ok(())
    }

    fn apply_cli(&mut self, cli: &CliOverrides) -> Result<(), ConfigError> {
        if let Some(value) = cli.address {
            self.listen_address = value;
        }
        if let Some(value) = cli.port {
            self.port = value;
        }
        if let Some(value) = &cli.password {
            self.password.clone_from(value);
        }
        if let Some(value) = cli.http2 {
            self.http2.enabled = value;
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.port == 0 {
            return Err(ConfigError::Invalid("port must be non-zero"));
        }
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
        if self.media.buffer_duration > Duration::from_millis(MAX_BUFFER_DURATION_MS)
            || self.media.frame_buffer_duration.is_zero()
            || self.media.frame_buffer_duration
                > Duration::from_millis(MAX_FRAME_BUFFER_DURATION_MS)
            || self.media.opus_encoding_quality > 10
            || self.media.track_stuck_threshold.is_zero()
            || self.media.track_stuck_threshold > MAX_PLAYER_UPDATE_INTERVAL
        {
            return Err(ConfigError::Invalid(
                "media settings are outside their bounded ranges",
            ));
        }
        if self.search.youtube_playlist_load_limit == 0
            || self.search.youtube_playlist_load_limit > MAX_PLAYLIST_LOAD_LIMIT
        {
            return Err(ConfigError::Invalid(
                "youtube_playlist_load_limit must be 1..=64",
            ));
        }
        validate_timeout(self.http_source.timeouts.connect, "connect_timeout")?;
        validate_timeout(
            self.http_source.timeouts.connection_request,
            "connection_request_timeout",
        )?;
        validate_timeout(self.http_source.timeouts.socket, "socket_timeout")?;
        if let Some(proxy) = &self.http_source.proxy
            && (proxy.host.is_empty() || proxy.host.len() > MAX_PROXY_HOST_BYTES || proxy.port == 0)
        {
            return Err(ConfigError::Invalid("proxy host and port are invalid"));
        }
        if self.metrics.endpoint.is_empty()
            || self.metrics.endpoint.len() > MAX_ENDPOINT_BYTES
            || !self.metrics.endpoint.starts_with('/')
            || self.metrics.endpoint.chars().any(|character| {
                character.is_whitespace()
                    || character.is_control()
                    || matches!(character, '{' | '}' | '*')
            })
        {
            return Err(ConfigError::Invalid(
                "metrics endpoint must be an absolute path",
            ));
        }
        if self.logging.request.max_payload_length > MAX_REQUEST_BODY_BYTES {
            return Err(ConfigError::Invalid(
                "max_payload_length exceeds request-body bound",
            ));
        }
        if let Some(route) = &self.route_planner {
            if route.ip_blocks.is_empty() {
                return Err(ConfigError::Invalid(
                    "RoutePlanner requires at least one ip block",
                ));
            }
            if route.retry_limit.is_some_and(|value| value < -1) {
                return Err(ConfigError::Invalid(
                    "RoutePlanner retry_limit must be -1 or non-negative",
                ));
            }
        }
        Ok(())
    }
}

fn validate_timeout(value: Duration, name: &'static str) -> Result<(), ConfigError> {
    if value.is_zero() || value > Duration::from_secs(5 * 60) {
        return Err(ConfigError::Invalid(name));
    }
    Ok(())
}
fn default_route_settings() -> RoutePlannerSettings {
    RoutePlannerSettings {
        strategy: RoutePlannerStrategy::RotateOnBan,
        ip_blocks: Vec::new(),
        excluded_ips: Vec::new(),
        search_triggers_fail: true,
        retry_limit: None,
        max_failures: 4_096,
    }
}
fn parse_strategy(value: &str) -> Option<RoutePlannerStrategy> {
    match value {
        "RotateOnBan" => Some(RoutePlannerStrategy::RotateOnBan),
        "LoadBalance" => Some(RoutePlannerStrategy::LoadBalance),
        "NanoSwitch" => Some(RoutePlannerStrategy::NanoSwitch),
        "RotatingNanoSwitch" => Some(RoutePlannerStrategy::RotatingNanoSwitch),
        _ => None,
    }
}
fn environment(name: &'static str) -> Option<String> {
    env::var_os(name).map(|value| value.to_string_lossy().into_owned())
}
fn parse_environment<T: FromStr>(name: &'static str) -> Result<Option<T>, ConfigError> {
    environment(name)
        .map(|value| value.parse().map_err(|_| ConfigError::Environment(name)))
        .transpose()
}

#[derive(Debug)]
pub enum ConfigError {
    Read(std::io::Error),
    Yaml(String),
    Environment(&'static str),
    Invalid(&'static str),
    RoutePlanner(String),
}
impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "failed to read configuration: {error}"),
            Self::Yaml(error) => write!(formatter, "invalid YAML configuration: {error}"),
            Self::Environment(name) => write!(formatter, "invalid environment value: {name}"),
            Self::Invalid(message) => write!(formatter, "invalid configuration: {message}"),
            Self::RoutePlanner(message) => {
                write!(formatter, "invalid RoutePlanner configuration: {message}")
            }
        }
    }
}
impl std::error::Error for ConfigError {}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    server: Option<FileServer>,
    plugins: Option<serde_json::Value>,
    lavalink: Option<FileLavalink>,
    metrics: Option<FileMetrics>,
    logging: Option<FileLogging>,
    sentry: Option<FileSentry>,
    crust: Option<FileCrust>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileServer {
    address: Option<IpAddr>,
    port: Option<u16>,
    http2: Option<FileHttp2>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileHttp2 {
    enabled: Option<bool>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileLavalink {
    plugins: Option<Vec<serde_json::Value>>,
    plugins_dir: Option<String>,
    default_plugin_repository: Option<String>,
    default_plugin_snapshot_repository: Option<String>,
    server: Option<FileLavalinkServer>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileLavalinkServer {
    password: Option<String>,
    sources: Option<FileSources>,
    filters: Option<FileFilters>,
    non_allocating_frame_buffer: Option<bool>,
    buffer_duration_ms: Option<u64>,
    frame_buffer_duration_ms: Option<u64>,
    opus_encoding_quality: Option<u8>,
    resampling_quality: Option<String>,
    track_stuck_threshold_ms: Option<u64>,
    use_seek_ghosting: Option<bool>,
    youtube_playlist_load_limit: Option<usize>,
    player_update_interval: Option<u64>,
    youtube_search_enabled: Option<bool>,
    soundcloud_search_enabled: Option<bool>,
    soundcloud_filter_out_preview_tracks: Option<bool>,
    #[serde(rename = "gc-warnings")]
    gc_warnings: Option<bool>,
    #[serde(rename = "ratelimit")]
    rate_limit: Option<FileRateLimit>,
    #[serde(rename = "httpConfig")]
    http_config: Option<FileHttpConfig>,
    timeouts: Option<FileTimeouts>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileSources {
    youtube: Option<bool>,
    bandcamp: Option<bool>,
    soundcloud: Option<bool>,
    twitch: Option<bool>,
    vimeo: Option<bool>,
    nico: Option<bool>,
    http: Option<bool>,
    local: Option<bool>,
}
impl FileSources {
    fn apply_to(self, target: &mut SourceConfig) {
        if let Some(value) = self.youtube {
            target.youtube = value;
        }
        if let Some(value) = self.bandcamp {
            target.bandcamp = value;
        }
        if let Some(value) = self.soundcloud {
            target.soundcloud = value;
        }
        if let Some(value) = self.twitch {
            target.twitch = value;
        }
        if let Some(value) = self.vimeo {
            target.vimeo = value;
        }
        if let Some(value) = self.nico {
            target.nico = value;
        }
        if let Some(value) = self.http {
            target.http = value;
        }
        if let Some(value) = self.local {
            target.local = value;
        }
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileFilters {
    volume: Option<bool>,
    equalizer: Option<bool>,
    karaoke: Option<bool>,
    timescale: Option<bool>,
    tremolo: Option<bool>,
    vibrato: Option<bool>,
    distortion: Option<bool>,
    rotation: Option<bool>,
    channel_mix: Option<bool>,
    low_pass: Option<bool>,
}
impl FileFilters {
    fn apply_to(self, target: &mut FilterConfig) {
        if let Some(value) = self.volume {
            target.volume = value;
        }
        if let Some(value) = self.equalizer {
            target.equalizer = value;
        }
        if let Some(value) = self.karaoke {
            target.karaoke = value;
        }
        if let Some(value) = self.timescale {
            target.timescale = value;
        }
        if let Some(value) = self.tremolo {
            target.tremolo = value;
        }
        if let Some(value) = self.vibrato {
            target.vibrato = value;
        }
        if let Some(value) = self.distortion {
            target.distortion = value;
        }
        if let Some(value) = self.rotation {
            target.rotation = value;
        }
        if let Some(value) = self.channel_mix {
            target.channel_mix = value;
        }
        if let Some(value) = self.low_pass {
            target.low_pass = value;
        }
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileTimeouts {
    connect_timeout_ms: Option<u64>,
    connection_request_timeout_ms: Option<u64>,
    socket_timeout_ms: Option<u64>,
}
impl FileTimeouts {
    fn apply_to(self, target: &mut TimeoutConfig) {
        if let Some(value) = self.connect_timeout_ms {
            target.connect = Duration::from_millis(value);
        }
        if let Some(value) = self.connection_request_timeout_ms {
            target.connection_request = Duration::from_millis(value);
        }
        if let Some(value) = self.socket_timeout_ms {
            target.socket = Duration::from_millis(value);
        }
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileRateLimit {
    ip_blocks: Option<Vec<String>>,
    excluded_ips: Option<Vec<IpAddr>>,
    strategy: Option<String>,
    search_triggers_fail: Option<bool>,
    retry_limit: Option<i32>,
}
impl FileRateLimit {
    fn into_settings(self) -> Result<RoutePlannerSettings, ConfigError> {
        let strategy = self.strategy.as_deref().and_then(parse_strategy).ok_or(ConfigError::Invalid("RoutePlanner strategy is required and must be one of RotateOnBan, LoadBalance, NanoSwitch, RotatingNanoSwitch"))?;
        Ok(RoutePlannerSettings {
            strategy,
            ip_blocks: self.ip_blocks.unwrap_or_default(),
            excluded_ips: self.excluded_ips.unwrap_or_default(),
            search_triggers_fail: self.search_triggers_fail.unwrap_or(true),
            retry_limit: self.retry_limit,
            max_failures: 4_096,
        })
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileHttpConfig {
    proxy_host: Option<String>,
    proxy_port: Option<u16>,
    proxy_user: Option<String>,
    proxy_password: Option<String>,
}
impl FileHttpConfig {
    fn into_proxy(self) -> Result<Option<ProxyConfig>, ConfigError> {
        if self.proxy_host.is_none()
            && self.proxy_port.is_none()
            && self.proxy_user.is_none()
            && self.proxy_password.is_none()
        {
            return Ok(None);
        }
        Ok(Some(ProxyConfig {
            host: self.proxy_host.unwrap_or_else(|| "localhost".to_owned()),
            port: self.proxy_port.unwrap_or(3128),
            username: self.proxy_user,
            password: self.proxy_password,
        }))
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMetrics {
    prometheus: Option<FilePrometheus>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FilePrometheus {
    enabled: Option<bool>,
    endpoint: Option<String>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLogging {
    file: Option<FileLoggingFile>,
    level: Option<FileLoggingLevel>,
    request: Option<FileRequestLogging>,
    logback: Option<serde_json::Value>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLoggingFile {
    path: Option<String>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLoggingLevel {
    root: Option<String>,
    lavalink: Option<String>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct FileRequestLogging {
    enabled: Option<bool>,
    include_client_info: Option<bool>,
    include_headers: Option<bool>,
    include_query_string: Option<bool>,
    include_payload: Option<bool>,
    max_payload_length: Option<usize>,
    before_request: Option<bool>,
}
impl FileRequestLogging {
    fn apply_to(self, target: &mut RequestLoggingConfig) {
        if let Some(value) = self.enabled {
            target.enabled = value;
        }
        if let Some(value) = self.include_client_info {
            target.include_client_info = value;
        }
        if let Some(value) = self.include_headers {
            target.include_headers = value;
        }
        if let Some(value) = self.include_query_string {
            target.include_query_string = value;
        }
        if let Some(value) = self.include_payload {
            target.include_payload = value;
        }
        if let Some(value) = self.max_payload_length {
            target.max_payload_length = value;
        }
        if let Some(value) = self.before_request {
            target.before_request = value;
        }
    }
}
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileSentry {
    dsn: Option<String>,
    environment: Option<String>,
    tags: Option<std::collections::BTreeMap<String, String>>,
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
impl FileCrust {
    fn apply_to(self, target: &mut ServerConfig) {
        if let Some(value) = self.max_request_body_bytes {
            target.max_request_body_bytes = value;
        }
        if let Some(value) = self.websocket_critical_capacity {
            target.websocket_critical_capacity = value;
        }
        if let Some(value) = self.max_sessions {
            target.max_sessions = value;
        }
        if let Some(value) = self.max_players {
            target.max_players = value;
        }
        if let Some(value) = self.max_concurrent_session_resumes {
            target.max_concurrent_session_resumes = value;
        }
        if let Some(value) = self.player_executor_shards {
            target.player_executor_shards = value;
        }
        if let Some(value) = self.player_command_capacity {
            target.player_command_capacity = value;
        }
        if let Some(value) = self.max_concurrent_loads {
            target.max_concurrent_loads = value;
        }
        if let Some(value) = self.max_batch_decode_tracks {
            target.max_batch_decode_tracks = value;
        }
        if let Some(value) = self.max_concurrent_source_requests {
            target.max_concurrent_source_requests = value;
        }
        if let Some(value) = self.max_concurrent_voice_connects {
            target.max_concurrent_voice_connects = value;
        }
        if let Some(value) = self.player_update_interval_ms {
            target.player_update_interval = Duration::from_millis(value);
        }
        if let Some(value) = self.stats_interval_ms {
            target.stats_interval = Duration::from_millis(value);
        }
        if let Some(value) = self.shutdown_timeout_ms {
            target.shutdown_timeout = Duration::from_millis(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn debug_redacts_password_and_proxy_secret() {
        let mut config = ServerConfig::default()
            .with_password("not-for-logs")
            .unwrap();
        config.http_source.proxy = Some(ProxyConfig {
            host: "proxy".to_owned(),
            port: 3128,
            username: Some("user".to_owned()),
            password: Some("proxy-secret".to_owned()),
        });
        let debug = format!("{config:?}");
        assert!(!debug.contains("not-for-logs"));
        assert!(!debug.contains("proxy-secret"));
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
    fn representative_lavalink_config_maps() {
        let yaml = "server:\n  port: 2444\n  address: 127.0.0.1\n  http2:\n    enabled: true\nplugins: {}\nlavalink:\n  plugins: []\n  pluginsDir: ./plugins\n  server:\n    password: secret\n    sources: {youtube: true, http: false}\n    filters: {timescale: false, lowPass: false}\n    nonAllocatingFrameBuffer: true\n    bufferDurationMs: 400\n    frameBufferDurationMs: 1000\n    opusEncodingQuality: 7\n    resamplingQuality: HIGH\n    trackStuckThresholdMs: 9000\n    useSeekGhosting: false\n    youtubePlaylistLoadLimit: 8\n    playerUpdateInterval: 4\n    youtubeSearchEnabled: false\n    timeouts: {connectTimeoutMs: 2000, connectionRequestTimeoutMs: 2500, socketTimeoutMs: 3000}\nmetrics:\n  prometheus: {enabled: true, endpoint: /custom-metrics}\nlogging:\n  request: {enabled: false, maxPayloadLength: 500}\n";
        let file: ConfigFile = serde_saphyr::from_str(yaml).unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file).unwrap();
        assert_eq!(config.port, 2444);
        assert!(config.http2.enabled);
        assert!(config.sources.youtube);
        assert!(!config.filters.timescale);
        assert_eq!(config.media.opus_encoding_quality, 7);
        assert_eq!(config.media.resampling_quality, ResamplingQuality::High);
        assert_eq!(config.search.youtube_playlist_load_limit, 8);
        assert_eq!(config.http_source.timeouts.connect, Duration::from_secs(2));
        assert!(config.metrics.prometheus.enabled);
        assert_eq!(config.metrics.endpoint, "/custom-metrics");
        assert!(!config.logging.request.enabled);
    }

    #[test]
    fn frozen_representative_fixture_loads_with_opaque_sections() {
        let file: ConfigFile = serde_saphyr::from_str(include_str!(
            "../tests/fixtures/config.yml"
        ))
        .unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file).unwrap();
        assert!(config.route_planner().unwrap().is_enabled());
        assert_eq!(config.metrics.endpoint, "/metrics");
        assert_eq!(config.logging.root_level, "INFO");
    }

    #[test]
    fn precedence_is_defaults_then_yaml_then_environment_then_cli() {
        use std::sync::{Mutex, OnceLock};

        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let path =
            std::env::temp_dir().join(format!("crust-p14-config-{}.yml", std::process::id()));
        std::fs::write(
            &path,
            "server:\n  port: 2400\nlavalink:\n  server:\n    password: yaml-secret\n",
        )
        .unwrap();
        let old_port = std::env::var_os("SERVER_PORT");
        let old_password = std::env::var_os("LAVALINK_SERVER_PASSWORD");
        // Environment mutation is process-global; the local lock keeps this test deterministic.
        unsafe {
            std::env::set_var("SERVER_PORT", "2500");
            std::env::set_var("LAVALINK_SERVER_PASSWORD", "environment-secret");
        }
        let cli = CliOverrides {
            port: Some(2600),
            password: Some("cli-secret".to_owned()),
            ..CliOverrides::default()
        };
        let config = ServerConfig::load_with_cli(Some(&path), &cli).unwrap();
        assert_eq!(config.port, 2600);
        assert_eq!(config.password(), "cli-secret");
        unsafe {
            match old_port {
                Some(value) => std::env::set_var("SERVER_PORT", value),
                None => std::env::remove_var("SERVER_PORT"),
            }
            match old_password {
                Some(value) => std::env::set_var("LAVALINK_SERVER_PASSWORD", value),
                None => std::env::remove_var("LAVALINK_SERVER_PASSWORD"),
            }
        }
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn routeplanner_yaml_is_typed_and_family_checked() {
        let file: ConfigFile = serde_saphyr::from_str("lavalink:\n  server:\n    ratelimit:\n      ipBlocks: [127.0.0.0/30]\n      excludedIps: [127.0.0.1]\n      strategy: LoadBalance\n      searchTriggersFail: false\n").unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file).unwrap();
        let planner = config.route_planner().unwrap();
        assert_eq!(
            planner.snapshot().unwrap().strategy,
            RoutePlannerStrategy::LoadBalance
        );
        assert!(
            config
                .route_planner
                .as_ref()
                .is_some_and(|value| !value.search_triggers_fail)
        );
    }
    #[test]
    fn p08_media_limits_load_from_yaml_and_reject_zero() {
        let file: ConfigFile = serde_saphyr::from_str("crust:\n  maxConcurrentLoads: 2\n  maxBatchDecodeTracks: 3\n  maxConcurrentSourceRequests: 4\n  maxConcurrentVoiceConnects: 5\n").unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file).unwrap();
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
        let file: ConfigFile = serde_saphyr::from_str("lavalink:\n  server:\n    playerUpdateInterval: 7\ncrust:\n  playerUpdateIntervalMs: 125\n").unwrap();
        let mut config = ServerConfig::default();
        config.apply_file(file).unwrap();
        assert_eq!(config.player_update_interval, Duration::from_millis(125));
        assert!(config.validate().is_ok());
        let file: ConfigFile =
            serde_saphyr::from_str("lavalink:\n  server:\n    playerUpdateInterval: 7\n").unwrap();
        config.apply_file(file).unwrap();
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
        config.apply_file(file).unwrap();
        assert_eq!(config.stats_interval, Duration::from_millis(25));
        assert!(config.validate().is_ok());
        config.stats_interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.stats_interval = MAX_STATS_INTERVAL + Duration::from_millis(1);
        assert!(config.validate().is_err());
    }
}
