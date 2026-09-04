//! Bounded RoutePlanner state shared by the Lavalink API and Mantle transport.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use num_bigint::BigUint;
use num_traits::{One, ToPrimitive, Zero};

const MAX_IP_BLOCKS: usize = 1_024;
const MAX_EXCLUDED_ADDRESSES: usize = 4_096;
const MAX_FAILURE_ENTRIES: usize = 4_096;
const FAILURE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const NANO_BLOCK_BITS: usize = 64;

/// Stable identity and local bind address for one selected connection route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteEntry {
    pub identity: u64,
    pub local_address: IpAddr,
}

/// Transport and source classifications that affect route health.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteOutcome {
    ConnectionEstablished,
    DestinationDenied,
    Timeout,
    TransportFailure,
    SourceSuccess,
    SourceRateLimited,
    SourceSearchRateLimited,
    SourceUnavailable,
    SourceFailure,
}

/// The four RoutePlanner strategies exposed by Lavalink 4.2.2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutePlannerStrategy {
    RotateOnBan,
    LoadBalance,
    NanoSwitch,
    RotatingNanoSwitch,
}

impl RoutePlannerStrategy {
    #[must_use]
    pub const fn class_name(self) -> &'static str {
        match self {
            Self::RotateOnBan => "RotatingIpRoutePlanner",
            Self::LoadBalance => "BalancingIpRoutePlanner",
            Self::NanoSwitch => "NanoIpRoutePlanner",
            Self::RotatingNanoSwitch => "RotatingNanoIpRoutePlanner",
        }
    }
}

/// Bounded construction input. P14 will map Lavalink-style configuration into this contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePlannerConfig {
    pub strategy: RoutePlannerStrategy,
    pub ip_blocks: Vec<String>,
    pub excluded_addresses: Vec<IpAddr>,
    pub search_triggers_fail: bool,
    pub max_failures: usize,
}

impl RoutePlannerConfig {
    #[must_use]
    pub fn new(
        strategy: RoutePlannerStrategy,
        ip_blocks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            strategy,
            ip_blocks: ip_blocks.into_iter().map(Into::into).collect(),
            excluded_addresses: Vec::new(),
            search_triggers_fail: true,
            max_failures: MAX_FAILURE_ENTRIES,
        }
    }
}

/// Typed configuration rejection. Values are safe IP/CIDR material, never credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutePlannerError {
    NoIpBlocks,
    TooManyIpBlocks,
    TooManyExcludedAddresses,
    InvalidFailureLimit,
    InvalidCidr(String),
    MixedAddressFamilies,
    OverlappingIpBlocks,
    NanoStrategyRequiresIpv6Slash64,
    AllAddressesExcluded,
}

impl fmt::Display for RoutePlannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoIpBlocks => formatter.write_str("RoutePlanner requires at least one IP block"),
            Self::TooManyIpBlocks => formatter.write_str("RoutePlanner IP block limit exceeded"),
            Self::TooManyExcludedAddresses => {
                formatter.write_str("RoutePlanner excluded-address limit exceeded")
            }
            Self::InvalidFailureLimit => {
                formatter.write_str("RoutePlanner failure limit must be bounded and non-zero")
            }
            Self::InvalidCidr(cidr) => write!(formatter, "invalid RoutePlanner CIDR: {cidr}"),
            Self::MixedAddressFamilies => {
                formatter.write_str("all RoutePlanner IP blocks must use one address family")
            }
            Self::OverlappingIpBlocks => {
                formatter.write_str("RoutePlanner IP blocks must not overlap")
            }
            Self::NanoStrategyRequiresIpv6Slash64 => formatter.write_str(
                "NanoSwitch strategies require an IPv6 range containing at least 2^64 addresses",
            ),
            Self::AllAddressesExcluded => {
                formatter.write_str("RoutePlanner exclusions remove every configured address")
            }
        }
    }
}

impl std::error::Error for RoutePlannerError {}

/// Lavalink wire name for the configured address family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpBlockType {
    Inet4Address,
    Inet6Address,
}

impl IpBlockType {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Inet4Address => "Inet4Address",
            Self::Inet6Address => "Inet6Address",
        }
    }
}

/// One bounded failure record exposed by the RoutePlanner status endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailingAddress {
    pub address: IpAddr,
    pub failing_timestamp: u64,
}

/// Strategy-specific fields in a RoutePlanner status snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutePlannerDetails {
    Rotating {
        rotate_index: String,
        ip_index: String,
        current_address: Option<IpAddr>,
    },
    Nano {
        current_address_index: String,
    },
    RotatingNano {
        block_index: String,
        current_address_index: String,
    },
    Balancing,
}

/// Typed, allocation-bounded status used by the server's Lavalink serializer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePlannerSnapshot {
    pub strategy: RoutePlannerStrategy,
    pub ip_block_type: IpBlockType,
    pub ip_block_size: String,
    pub failing_addresses: Vec<FailingAddress>,
    pub details: RoutePlannerDetails,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AddressFamily {
    V4,
    V6,
}

impl AddressFamily {
    const fn bits(self) -> u8 {
        match self {
            Self::V4 => 32,
            Self::V6 => 128,
        }
    }

    const fn block_type(self) -> IpBlockType {
        match self {
            Self::V4 => IpBlockType::Inet4Address,
            Self::V6 => IpBlockType::Inet6Address,
        }
    }

    const fn matches(self, address: IpAddr) -> bool {
        matches!(
            (self, address),
            (Self::V4, IpAddr::V4(_)) | (Self::V6, IpAddr::V6(_))
        )
    }
}

#[derive(Clone, Debug)]
struct CidrBlock {
    family: AddressFamily,
    network: u128,
    prefix: u8,
    size: BigUint,
}

impl CidrBlock {
    fn parse(input: &str) -> Result<Self, RoutePlannerError> {
        let (address_text, prefix_text) = input
            .split_once('/')
            .map_or((input, None), |(address, prefix)| (address, Some(prefix)));
        if address_text.is_empty() || prefix_text.is_some_and(|prefix| prefix.contains('/')) {
            return Err(RoutePlannerError::InvalidCidr(input.to_owned()));
        }
        let address = address_text
            .parse::<IpAddr>()
            .map_err(|_| RoutePlannerError::InvalidCidr(input.to_owned()))?;
        let (family, raw) = match address {
            IpAddr::V4(address) => (AddressFamily::V4, u128::from(u32::from(address))),
            IpAddr::V6(address) => (AddressFamily::V6, u128::from(address)),
        };
        let bits = family.bits();
        let prefix = prefix_text
            .map_or(Ok(bits), str::parse::<u8>)
            .map_err(|_| RoutePlannerError::InvalidCidr(input.to_owned()))?;
        if prefix > bits {
            return Err(RoutePlannerError::InvalidCidr(input.to_owned()));
        }
        let host_bits = bits - prefix;
        let network = if prefix == 0 {
            0
        } else if family == AddressFamily::V4 {
            let mask = u32::MAX << host_bits;
            u128::from((raw as u32) & mask)
        } else {
            raw & (u128::MAX << host_bits)
        };
        Ok(Self {
            family,
            network,
            prefix,
            size: BigUint::one() << usize::from(host_bits),
        })
    }

    fn contains(&self, address: IpAddr) -> bool {
        let raw = match (self.family, address) {
            (AddressFamily::V4, IpAddr::V4(address)) => u128::from(u32::from(address)),
            (AddressFamily::V6, IpAddr::V6(address)) => u128::from(address),
            _ => return false,
        };
        let host_bits = self.family.bits() - self.prefix;
        let network = if self.prefix == 0 {
            0
        } else if self.family == AddressFamily::V4 {
            let mask = u32::MAX << host_bits;
            u128::from((raw as u32) & mask)
        } else {
            raw & (u128::MAX << host_bits)
        };
        network == self.network
    }

    fn address_at(&self, index: &BigUint) -> Option<IpAddr> {
        if index >= &self.size {
            return None;
        }
        let offset = index.to_u128()?;
        let raw = self.network.checked_add(offset)?;
        Some(match self.family {
            AddressFamily::V4 => IpAddr::V4(Ipv4Addr::from(u32::try_from(raw).ok()?)),
            AddressFamily::V6 => IpAddr::V6(Ipv6Addr::from(raw)),
        })
    }

    fn last_raw_address(&self) -> u128 {
        self.size
            .to_u128()
            .and_then(|size| self.network.checked_add(size.saturating_sub(1)))
            .unwrap_or(u128::MAX)
    }
}

#[derive(Clone, Debug)]
struct PlannerDefinition {
    strategy: RoutePlannerStrategy,
    blocks: Vec<CidrBlock>,
    family: AddressFamily,
    total_size: BigUint,
    excluded: BTreeSet<IpAddr>,
    search_triggers_fail: bool,
    max_failures: usize,
    failure_retention: Duration,
}

impl PlannerDefinition {
    fn address_at(&self, mut index: BigUint) -> Option<IpAddr> {
        if index >= self.total_size {
            return None;
        }
        for block in &self.blocks {
            if index < block.size {
                return block.address_at(&index);
            }
            index -= &block.size;
        }
        None
    }

    fn contains(&self, address: IpAddr) -> bool {
        self.blocks.iter().any(|block| block.contains(address))
    }
}

#[derive(Clone, Debug)]
struct FailureRecord {
    failed_at_tick: Duration,
    failed_at_unix_ms: u64,
    serial: u64,
}

#[derive(Debug)]
struct PlannerState {
    next_identity: u64,
    failures: BTreeMap<IpAddr, FailureRecord>,
    failure_serial: u64,
    rotate_count: u64,
    rotating_next_index: BigUint,
    rotating_current_index: Option<BigUint>,
    rotating_advance_pending: bool,
    nano_block: BigUint,
    nano_started: Duration,
    rotating_nano_block: BigUint,
    rotating_nano_started: Duration,
    random_state: u64,
}

trait PlannerClock: Send + Sync + 'static {
    fn monotonic(&self) -> Duration;
    fn unix_millis(&self) -> u64;
}

struct SystemPlannerClock {
    started: Instant,
}

impl SystemPlannerClock {
    fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl PlannerClock for SystemPlannerClock {
    fn monotonic(&self) -> Duration {
        self.started.elapsed()
    }

    fn unix_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

struct RoutePlannerInner {
    definition: Option<PlannerDefinition>,
    clock: Arc<dyn PlannerClock>,
    state: Mutex<PlannerState>,
}

/// Caller-owned RoutePlanner. Clones share one bounded strategy/failure state.
#[derive(Clone)]
pub struct RoutePlanner {
    inner: Arc<RoutePlannerInner>,
}

impl fmt::Debug for RoutePlanner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutePlanner")
            .field("enabled", &self.is_enabled())
            .field(
                "strategy",
                &self
                    .inner
                    .definition
                    .as_ref()
                    .map(|definition| definition.strategy),
            )
            .finish_non_exhaustive()
    }
}

impl Default for RoutePlanner {
    fn default() -> Self {
        Self::disabled()
    }
}

impl RoutePlanner {
    /// Builds the explicit disabled state used when no rate-limit block is configured.
    #[must_use]
    pub fn disabled() -> Self {
        let clock: Arc<dyn PlannerClock> = Arc::new(SystemPlannerClock::new());
        let now = clock.monotonic();
        Self {
            inner: Arc::new(RoutePlannerInner {
                definition: None,
                clock,
                state: Mutex::new(PlannerState::new(now, 1, None)),
            }),
        }
    }

    /// Parses and validates a bounded configured planner without materializing its ranges.
    pub fn configured(config: RoutePlannerConfig) -> Result<Self, RoutePlannerError> {
        let clock: Arc<dyn PlannerClock> = Arc::new(SystemPlannerClock::new());
        let seed = system_seed();
        Self::configured_with(config, clock, seed, FAILURE_RETENTION)
    }

    fn configured_with(
        config: RoutePlannerConfig,
        clock: Arc<dyn PlannerClock>,
        seed: u64,
        failure_retention: Duration,
    ) -> Result<Self, RoutePlannerError> {
        let definition = validate_config(config, failure_retention)?;
        let now = clock.monotonic();
        let block_count = nano_block_count(&definition);
        Ok(Self {
            inner: Arc::new(RoutePlannerInner {
                state: Mutex::new(PlannerState::new(now, seed, block_count.as_ref())),
                definition: Some(definition),
                clock,
            }),
        })
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.inner.definition.is_some()
    }

    /// Maximum retained failure records for this planner, or `None` while
    /// disabled. Server wiring uses this to enforce its central resource
    /// policy even when a caller supplies a pre-built planner.
    #[must_use]
    pub fn failure_capacity(&self) -> Option<usize> {
        self.inner
            .definition
            .as_ref()
            .map(|definition| definition.max_failures)
    }

    /// Selects a compatible local route. Enabled planners never silently fall back to an
    /// unbound same-family connection, even if every usable address is temporarily failing.
    #[must_use]
    pub fn select(&self, destination: Option<IpAddr>) -> Option<RouteEntry> {
        let definition = self.inner.definition.as_ref()?;
        if destination.is_some_and(|address| !definition.family.matches(address)) {
            return None;
        }
        let now = self.inner.clock.monotonic();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cleanup_failures(definition, &mut state, now);
        let index = select_index(definition, &mut state, now)?;
        let local_address = definition.address_at(index)?;
        let identity = next_identity(&mut state.next_identity);
        Some(RouteEntry {
            identity,
            local_address,
        })
    }

    /// Selects from Mantle's credential-safe authority context.
    #[must_use]
    pub fn select_for_authority(&self, authority: &str) -> Option<RouteEntry> {
        self.select(authority_ip(authority))
    }

    /// Applies a transport/source result to the exact selected address.
    pub fn report(&self, route: RouteEntry, outcome: RouteOutcome) {
        let Some(definition) = self.inner.definition.as_ref() else {
            return;
        };
        if !definition.contains(route.local_address) {
            return;
        }
        let should_fail = match outcome {
            RouteOutcome::TransportFailure | RouteOutcome::SourceRateLimited => true,
            RouteOutcome::SourceSearchRateLimited => definition.search_triggers_fail,
            RouteOutcome::ConnectionEstablished
            | RouteOutcome::DestinationDenied
            | RouteOutcome::Timeout
            | RouteOutcome::SourceSuccess
            | RouteOutcome::SourceUnavailable
            | RouteOutcome::SourceFailure => false,
        };
        if should_fail {
            self.mark_address_failing(route.local_address);
        }
    }

    /// Marks one configured address as failing and applies strategy-specific rotation.
    pub fn mark_address_failing(&self, address: IpAddr) {
        let Some(definition) = self.inner.definition.as_ref() else {
            return;
        };
        if !definition.contains(address) {
            return;
        }
        let now = self.inner.clock.monotonic();
        let unix_ms = self.inner.clock.unix_millis();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cleanup_failures(definition, &mut state, now);
        if !state.failures.contains_key(&address)
            && state.failures.len() >= definition.max_failures
            && let Some(oldest) = state
                .failures
                .iter()
                .min_by_key(|(_, record)| record.serial)
                .map(|(address, _)| *address)
        {
            state.failures.remove(&oldest);
        }
        let serial = next_failure_serial(&mut state);
        state.failures.insert(
            address,
            FailureRecord {
                failed_at_tick: now,
                failed_at_unix_ms: unix_ms,
                serial,
            },
        );
        match definition.strategy {
            RoutePlannerStrategy::RotateOnBan => {
                let current_matches = state
                    .rotating_current_index
                    .as_ref()
                    .and_then(|index| definition.address_at(index.clone()))
                    == Some(address);
                if current_matches && !state.rotating_advance_pending {
                    state.rotating_advance_pending = true;
                    state.rotate_count = state.rotate_count.wrapping_add(1);
                }
            }
            RoutePlannerStrategy::RotatingNanoSwitch => {
                if let Some(block_count) = nano_block_count(definition) {
                    state.rotating_nano_block += BigUint::one();
                    state.rotating_nano_block %= block_count;
                    state.rotating_nano_started = now;
                }
            }
            RoutePlannerStrategy::LoadBalance | RoutePlannerStrategy::NanoSwitch => {}
        }
    }

    /// Removes one literal address from the failing set. Unknown/out-of-block values are no-op.
    pub fn free_address(&self, address: IpAddr) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failures
            .remove(&address);
    }

    /// Clears all retained failing addresses.
    pub fn free_all_addresses(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failures
            .clear();
    }

    /// Captures one bounded status snapshot, or `None` for the disabled planner.
    #[must_use]
    pub fn snapshot(&self) -> Option<RoutePlannerSnapshot> {
        let definition = self.inner.definition.as_ref()?;
        let now = self.inner.clock.monotonic();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cleanup_failures(definition, &mut state, now);
        let failing_addresses = state
            .failures
            .iter()
            .map(|(address, record)| FailingAddress {
                address: *address,
                failing_timestamp: record.failed_at_unix_ms,
            })
            .collect();
        let details = match definition.strategy {
            RoutePlannerStrategy::RotateOnBan => RoutePlannerDetails::Rotating {
                rotate_index: state.rotate_count.to_string(),
                ip_index: state.rotating_next_index.to_string(),
                current_address: state
                    .rotating_current_index
                    .as_ref()
                    .and_then(|index| definition.address_at(index.clone())),
            },
            RoutePlannerStrategy::LoadBalance => RoutePlannerDetails::Balancing,
            RoutePlannerStrategy::NanoSwitch => RoutePlannerDetails::Nano {
                current_address_index: nano_offset(now, state.nano_started).to_string(),
            },
            RoutePlannerStrategy::RotatingNanoSwitch => RoutePlannerDetails::RotatingNano {
                block_index: state.rotating_nano_block.to_string(),
                current_address_index: nano_offset(now, state.rotating_nano_started).to_string(),
            },
        };
        Some(RoutePlannerSnapshot {
            strategy: definition.strategy,
            ip_block_type: definition.family.block_type(),
            ip_block_size: definition.total_size.to_string(),
            failing_addresses,
            details,
        })
    }
}

impl PlannerState {
    fn new(now: Duration, seed: u64, nano_block_count: Option<&BigUint>) -> Self {
        let mut random_state = seed.max(1);
        let nano_block = nano_block_count
            .map(|count| random_below(count, &mut random_state))
            .unwrap_or_default();
        Self {
            next_identity: 1,
            failures: BTreeMap::new(),
            failure_serial: 0,
            rotate_count: 0,
            rotating_next_index: BigUint::zero(),
            rotating_current_index: None,
            rotating_advance_pending: false,
            nano_block,
            nano_started: now,
            rotating_nano_block: BigUint::zero(),
            rotating_nano_started: now,
            random_state,
        }
    }
}

fn validate_config(
    config: RoutePlannerConfig,
    failure_retention: Duration,
) -> Result<PlannerDefinition, RoutePlannerError> {
    if config.ip_blocks.is_empty() {
        return Err(RoutePlannerError::NoIpBlocks);
    }
    if config.ip_blocks.len() > MAX_IP_BLOCKS {
        return Err(RoutePlannerError::TooManyIpBlocks);
    }
    if config.excluded_addresses.len() > MAX_EXCLUDED_ADDRESSES {
        return Err(RoutePlannerError::TooManyExcludedAddresses);
    }
    if config.max_failures == 0 || config.max_failures > MAX_FAILURE_ENTRIES {
        return Err(RoutePlannerError::InvalidFailureLimit);
    }
    let blocks = config
        .ip_blocks
        .iter()
        .map(|block| CidrBlock::parse(block))
        .collect::<Result<Vec<_>, _>>()?;
    let family = blocks[0].family;
    if blocks.iter().any(|block| block.family != family) {
        return Err(RoutePlannerError::MixedAddressFamilies);
    }
    let mut ranges = blocks
        .iter()
        .map(|block| (block.network, block.last_raw_address()))
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[1].0 <= pair[0].1) {
        return Err(RoutePlannerError::OverlappingIpBlocks);
    }
    let total_size = blocks
        .iter()
        .fold(BigUint::zero(), |total, block| total + &block.size);
    let excluded = config
        .excluded_addresses
        .into_iter()
        .filter(|address| blocks.iter().any(|block| block.contains(*address)))
        .collect::<BTreeSet<_>>();
    if total_size.to_usize().is_some_and(|size| {
        size <= excluded.len()
            && (0..size).all(|index| {
                blocks_address_at(&blocks, BigUint::from(index))
                    .is_none_or(|address| excluded.contains(&address))
            })
    }) {
        return Err(RoutePlannerError::AllAddressesExcluded);
    }
    if matches!(
        config.strategy,
        RoutePlannerStrategy::NanoSwitch | RoutePlannerStrategy::RotatingNanoSwitch
    ) && (family != AddressFamily::V6 || total_size < (BigUint::one() << NANO_BLOCK_BITS))
    {
        return Err(RoutePlannerError::NanoStrategyRequiresIpv6Slash64);
    }
    Ok(PlannerDefinition {
        strategy: config.strategy,
        blocks,
        family,
        total_size,
        excluded,
        search_triggers_fail: config.search_triggers_fail,
        max_failures: config.max_failures,
        failure_retention,
    })
}

fn blocks_address_at(blocks: &[CidrBlock], mut index: BigUint) -> Option<IpAddr> {
    for block in blocks {
        if index < block.size {
            return block.address_at(&index);
        }
        index -= &block.size;
    }
    None
}

fn select_index(
    definition: &PlannerDefinition,
    state: &mut PlannerState,
    now: Duration,
) -> Option<BigUint> {
    if definition.strategy == RoutePlannerStrategy::RotateOnBan
        && let Some(current) = state.rotating_current_index.as_ref()
        && !state.rotating_advance_pending
        && index_available(definition, state, current)
    {
        return Some(current.clone());
    }
    let start = match definition.strategy {
        RoutePlannerStrategy::RotateOnBan => state.rotating_next_index.clone(),
        RoutePlannerStrategy::LoadBalance => {
            random_below(&definition.total_size, &mut state.random_state)
        }
        RoutePlannerStrategy::NanoSwitch => nano_index(
            &state.nano_block,
            now,
            state.nano_started,
            &definition.total_size,
        ),
        RoutePlannerStrategy::RotatingNanoSwitch => nano_index(
            &state.rotating_nano_block,
            now,
            state.rotating_nano_started,
            &definition.total_size,
        ),
    };
    let attempts = definition
        .excluded
        .len()
        .saturating_add(state.failures.len())
        .saturating_add(1);
    let selected = find_index(definition, state, &start, attempts, false)
        // Exhausting a small block must never turn an enabled planner into an
        // unbound connection. Reuse the first non-excluded failing route until
        // an operator frees it or its seven-day record expires.
        .or_else(|| {
            find_index(
                definition,
                state,
                &start,
                definition.excluded.len() + 1,
                true,
            )
        })?;
    if definition.strategy == RoutePlannerStrategy::RotateOnBan {
        state.rotating_current_index = Some(selected.clone());
        state.rotating_next_index = increment_mod(&selected, &definition.total_size);
        state.rotating_advance_pending = false;
    }
    Some(selected)
}

fn find_index(
    definition: &PlannerDefinition,
    state: &PlannerState,
    start: &BigUint,
    attempts: usize,
    allow_failing: bool,
) -> Option<BigUint> {
    let mut index = start.clone();
    for _ in 0..attempts.max(1) {
        let address = definition.address_at(index.clone())?;
        if !definition.excluded.contains(&address)
            && (allow_failing || !state.failures.contains_key(&address))
        {
            return Some(index);
        }
        index = increment_mod(&index, &definition.total_size);
    }
    None
}

fn index_available(definition: &PlannerDefinition, state: &PlannerState, index: &BigUint) -> bool {
    definition.address_at(index.clone()).is_some_and(|address| {
        !definition.excluded.contains(&address) && !state.failures.contains_key(&address)
    })
}

fn increment_mod(index: &BigUint, modulus: &BigUint) -> BigUint {
    let mut next = index + BigUint::one();
    if next >= *modulus {
        next = BigUint::zero();
    }
    next
}

fn nano_block_count(definition: &PlannerDefinition) -> Option<BigUint> {
    matches!(
        definition.strategy,
        RoutePlannerStrategy::NanoSwitch | RoutePlannerStrategy::RotatingNanoSwitch
    )
    .then(|| &definition.total_size >> NANO_BLOCK_BITS)
}

fn nano_index(block: &BigUint, now: Duration, started: Duration, total_size: &BigUint) -> BigUint {
    let span = BigUint::one() << NANO_BLOCK_BITS;
    let offset = nano_offset(now, started) % &span;
    ((block * span) + offset) % total_size
}

fn nano_offset(now: Duration, started: Duration) -> BigUint {
    BigUint::from(now.saturating_sub(started).as_nanos())
}

fn cleanup_failures(definition: &PlannerDefinition, state: &mut PlannerState, now: Duration) {
    state.failures.retain(|_, record| {
        now.saturating_sub(record.failed_at_tick) < definition.failure_retention
    });
}

fn next_identity(next: &mut u64) -> u64 {
    let identity = (*next).max(1);
    *next = identity.wrapping_add(1).max(1);
    identity
}

fn next_failure_serial(state: &mut PlannerState) -> u64 {
    if state.failure_serial == u64::MAX {
        let mut insertion_order = state
            .failures
            .iter()
            .map(|(address, record)| (record.serial, *address))
            .collect::<Vec<_>>();
        insertion_order.sort_unstable();
        for (index, (_, address)) in insertion_order.into_iter().enumerate() {
            if let Some(record) = state.failures.get_mut(&address) {
                record.serial = u64::try_from(index).unwrap_or(u64::MAX - 1) + 1;
            }
        }
        state.failure_serial = u64::try_from(state.failures.len()).unwrap_or(u64::MAX - 1);
    }
    state.failure_serial += 1;
    state.failure_serial
}

fn system_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    ((nanos >> 64) as u64) ^ nanos as u64 ^ u64::from(std::process::id())
}

fn random_below(upper: &BigUint, state: &mut u64) -> BigUint {
    debug_assert!(!upper.is_zero());
    let bit_count = upper.bits().max(1);
    let byte_count = usize::try_from(bit_count.div_ceil(8)).unwrap_or(usize::MAX);
    loop {
        let mut bytes = vec![0_u8; byte_count];
        for chunk in bytes.chunks_mut(8) {
            let random = next_random(state).to_be_bytes();
            let start = random.len() - chunk.len();
            chunk.copy_from_slice(&random[start..]);
        }
        let excess_bits = u8::try_from(byte_count * 8).unwrap_or(u8::MAX)
            - u8::try_from(bit_count).unwrap_or(u8::MAX);
        if excess_bits > 0 {
            bytes[0] &= u8::MAX >> excess_bits;
        }
        let candidate = BigUint::from_bytes_be(&bytes);
        if &candidate < upper {
            return candidate;
        }
    }
}

fn next_random(state: &mut u64) -> u64 {
    let mut value = if *state == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        *state
    };
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    value
}

fn authority_ip(authority: &str) -> Option<IpAddr> {
    authority
        .parse::<SocketAddr>()
        .map(|address| address.ip())
        .ok()
        .or_else(|| authority.trim_matches(['[', ']']).parse().ok())
        .or_else(|| {
            let (host, port) = authority.rsplit_once(':')?;
            port.parse::<u16>().ok()?;
            host.parse().ok()
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use super::*;

    #[derive(Default)]
    struct TestClock {
        nanos: AtomicU64,
        unix_millis: AtomicU64,
    }

    impl TestClock {
        fn advance(&self, duration: Duration) {
            self.nanos.fetch_add(
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
                Ordering::AcqRel,
            );
            self.unix_millis.fetch_add(
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                Ordering::AcqRel,
            );
        }
    }

    impl PlannerClock for TestClock {
        fn monotonic(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::Acquire))
        }

        fn unix_millis(&self) -> u64 {
            self.unix_millis.load(Ordering::Acquire)
        }
    }

    fn configured(
        strategy: RoutePlannerStrategy,
        blocks: &[&str],
        clock: Arc<TestClock>,
    ) -> RoutePlanner {
        RoutePlanner::configured_with(
            RoutePlannerConfig::new(strategy, blocks.iter().copied()),
            clock,
            7,
            FAILURE_RETENTION,
        )
        .unwrap()
    }

    #[test]
    fn cidr_arithmetic_keeps_huge_ipv6_ranges_compact() {
        let clock = Arc::new(TestClock::default());
        let planner = configured(RoutePlannerStrategy::NanoSwitch, &["2001:db8::/48"], clock);
        let snapshot = planner.snapshot().unwrap();
        assert_eq!(snapshot.ip_block_type, IpBlockType::Inet6Address);
        assert_eq!(snapshot.ip_block_size, "1208925819614629174706176");
        assert!(
            planner
                .select(Some("2001:db8::1".parse().unwrap()))
                .unwrap()
                .local_address
                .is_ipv6()
        );
    }

    #[test]
    fn invalid_mixed_and_too_small_nano_configuration_is_rejected() {
        assert!(matches!(
            RoutePlanner::configured(RoutePlannerConfig::new(
                RoutePlannerStrategy::LoadBalance,
                ["127.0.0.1/33"]
            )),
            Err(RoutePlannerError::InvalidCidr(_))
        ));
        assert_eq!(
            RoutePlanner::configured(RoutePlannerConfig::new(
                RoutePlannerStrategy::LoadBalance,
                ["127.0.0.1/32", "::1/128"]
            ))
            .unwrap_err(),
            RoutePlannerError::MixedAddressFamilies
        );
        assert_eq!(
            RoutePlanner::configured(RoutePlannerConfig::new(
                RoutePlannerStrategy::NanoSwitch,
                ["2001:db8::/65"]
            ))
            .unwrap_err(),
            RoutePlannerError::NanoStrategyRequiresIpv6Slash64
        );
    }

    #[test]
    fn configuration_bounds_overlap_exclusions_and_full_ipv6_space_are_explicit() {
        let full_ipv6 = CidrBlock::parse("::/0").unwrap();
        assert_eq!(full_ipv6.size, BigUint::one() << 128_usize);
        assert_eq!(
            full_ipv6.address_at(&BigUint::from(u128::MAX)),
            Some(IpAddr::V6(Ipv6Addr::from(u128::MAX)))
        );

        assert_eq!(
            RoutePlanner::configured(RoutePlannerConfig::new(
                RoutePlannerStrategy::LoadBalance,
                ["127.0.0.0/24", "127.0.0.128/25"]
            ))
            .unwrap_err(),
            RoutePlannerError::OverlappingIpBlocks
        );

        let mut all_excluded =
            RoutePlannerConfig::new(RoutePlannerStrategy::RotateOnBan, ["127.0.0.0/31"]);
        all_excluded.excluded_addresses =
            vec!["127.0.0.0".parse().unwrap(), "127.0.0.1".parse().unwrap()];
        assert_eq!(
            RoutePlanner::configured(all_excluded).unwrap_err(),
            RoutePlannerError::AllAddressesExcluded
        );

        let too_many_blocks = std::iter::repeat_n("127.0.0.1/32", MAX_IP_BLOCKS + 1);
        assert_eq!(
            RoutePlanner::configured(RoutePlannerConfig::new(
                RoutePlannerStrategy::LoadBalance,
                too_many_blocks
            ))
            .unwrap_err(),
            RoutePlannerError::TooManyIpBlocks
        );

        let mut too_many_exclusions =
            RoutePlannerConfig::new(RoutePlannerStrategy::LoadBalance, ["127.0.0.1/32"]);
        too_many_exclusions.excluded_addresses =
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST); MAX_EXCLUDED_ADDRESSES + 1];
        assert_eq!(
            RoutePlanner::configured(too_many_exclusions).unwrap_err(),
            RoutePlannerError::TooManyExcludedAddresses
        );

        let mut invalid_failure_limit =
            RoutePlannerConfig::new(RoutePlannerStrategy::LoadBalance, ["127.0.0.1/32"]);
        invalid_failure_limit.max_failures = MAX_FAILURE_ENTRIES + 1;
        assert_eq!(
            RoutePlanner::configured(invalid_failure_limit).unwrap_err(),
            RoutePlannerError::InvalidFailureLimit
        );
    }

    #[test]
    fn rotate_on_ban_is_stable_skips_exclusions_and_frees_failures() {
        let clock = Arc::new(TestClock::default());
        clock.unix_millis.store(1_000, Ordering::Release);
        let mut config =
            RoutePlannerConfig::new(RoutePlannerStrategy::RotateOnBan, ["127.0.0.0/29"]);
        config.excluded_addresses = vec!["127.0.0.0".parse().unwrap()];
        let planner = RoutePlanner::configured_with(config, clock, 1, FAILURE_RETENTION).unwrap();
        let first = planner.select(Some("127.0.0.9".parse().unwrap())).unwrap();
        assert_eq!(first.local_address, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(
            planner
                .select(Some("127.0.0.9".parse().unwrap()))
                .unwrap()
                .local_address,
            first.local_address
        );
        planner.report(first, RouteOutcome::SourceRateLimited);
        planner.report(first, RouteOutcome::SourceRateLimited);
        let second = planner.select(Some("127.0.0.9".parse().unwrap())).unwrap();
        assert_eq!(second.local_address, "127.0.0.2".parse::<IpAddr>().unwrap());
        let snapshot = planner.snapshot().unwrap();
        assert_eq!(snapshot.failing_addresses[0].failing_timestamp, 1_000);
        assert!(matches!(
            snapshot.details,
            RoutePlannerDetails::Rotating { ref rotate_index, .. } if rotate_index == "1"
        ));
        planner.free_address(first.local_address);
        assert!(planner.snapshot().unwrap().failing_addresses.is_empty());
    }

    #[test]
    fn balancing_and_nano_strategies_advance_without_enumerating_ranges() {
        let clock = Arc::new(TestClock::default());
        let balancing = configured(
            RoutePlannerStrategy::LoadBalance,
            &["127.0.0.0/24"],
            Arc::clone(&clock),
        );
        let selected = (0..32)
            .map(|_| balancing.select(None).unwrap().local_address)
            .collect::<BTreeSet<_>>();
        assert!(selected.len() > 1);

        let nano = configured(
            RoutePlannerStrategy::NanoSwitch,
            &["2001:db8::/64"],
            Arc::clone(&clock),
        );
        let first = nano.select(None).unwrap().local_address;
        clock.advance(Duration::from_nanos(1));
        let second = nano.select(None).unwrap().local_address;
        assert_ne!(first, second);

        let rotating = configured(
            RoutePlannerStrategy::RotatingNanoSwitch,
            &["2001:db8::/63"],
            clock,
        );
        let first = rotating.select(None).unwrap();
        rotating.report(first, RouteOutcome::SourceRateLimited);
        let second = rotating.select(None).unwrap();
        assert_ne!(first.local_address, second.local_address);
        assert!(matches!(
            rotating.snapshot().unwrap().details,
            RoutePlannerDetails::RotatingNano { ref block_index, .. } if block_index == "1"
        ));
    }

    #[test]
    fn failure_state_is_bounded_expires_and_never_causes_unbound_fallback() {
        let clock = Arc::new(TestClock::default());
        let mut config =
            RoutePlannerConfig::new(RoutePlannerStrategy::LoadBalance, ["127.0.0.0/30"]);
        config.max_failures = 2;
        let planner =
            RoutePlanner::configured_with(config, clock.clone(), 9, Duration::from_millis(10))
                .unwrap();
        for address in ["127.0.0.0", "127.0.0.1", "127.0.0.2"] {
            planner.mark_address_failing(address.parse().unwrap());
            clock.advance(Duration::from_millis(1));
        }
        assert_eq!(planner.snapshot().unwrap().failing_addresses.len(), 2);
        for address in ["127.0.0.0", "127.0.0.1", "127.0.0.2", "127.0.0.3"] {
            planner.mark_address_failing(address.parse().unwrap());
        }
        assert!(planner.select(None).is_some());
        clock.advance(Duration::from_millis(11));
        assert!(planner.snapshot().unwrap().failing_addresses.is_empty());
    }

    #[test]
    fn failure_eviction_refresh_and_identity_index_wrap_remain_ordered() {
        let clock = Arc::new(TestClock::default());
        let mut config =
            RoutePlannerConfig::new(RoutePlannerStrategy::LoadBalance, ["127.0.0.0/30"]);
        config.max_failures = 2;
        let planner =
            RoutePlanner::configured_with(config, clock.clone(), 9, FAILURE_RETENTION).unwrap();
        planner.mark_address_failing("127.0.0.0".parse().unwrap());
        clock.advance(Duration::from_nanos(1));
        planner.mark_address_failing("127.0.0.1".parse().unwrap());
        clock.advance(Duration::from_nanos(1));
        planner.mark_address_failing("127.0.0.0".parse().unwrap());
        clock.advance(Duration::from_nanos(1));
        planner.mark_address_failing("127.0.0.2".parse().unwrap());
        let retained = planner
            .snapshot()
            .unwrap()
            .failing_addresses
            .into_iter()
            .map(|failure| failure.address)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            retained,
            ["127.0.0.0".parse().unwrap(), "127.0.0.2".parse().unwrap()]
                .into_iter()
                .collect()
        );

        let rotating = configured(RoutePlannerStrategy::RotateOnBan, &["127.0.0.0/31"], clock);
        {
            let mut state = rotating
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.next_identity = u64::MAX;
            state.failure_serial = u64::MAX;
            state.rotate_count = u64::MAX;
            state.rotating_next_index = BigUint::one();
        }
        let last = rotating.select(None).unwrap();
        assert_eq!(last.identity, u64::MAX);
        assert_eq!(last.local_address, "127.0.0.1".parse::<IpAddr>().unwrap());
        rotating.mark_address_failing(last.local_address);
        let wrapped = rotating.select(None).unwrap();
        assert_eq!(wrapped.identity, 1);
        assert_eq!(
            wrapped.local_address,
            "127.0.0.0".parse::<IpAddr>().unwrap()
        );
        assert!(matches!(
            rotating.snapshot().unwrap().details,
            RoutePlannerDetails::Rotating { ref rotate_index, .. } if rotate_index == "0"
        ));
        let state = rotating
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.failure_serial, 1);
    }

    #[test]
    fn search_failure_policy_family_checks_and_concurrency_are_explicit() {
        let clock = Arc::new(TestClock::default());
        let mut config =
            RoutePlannerConfig::new(RoutePlannerStrategy::RotateOnBan, ["127.0.0.1/32"]);
        config.search_triggers_fail = false;
        let planner = RoutePlanner::configured_with(config, clock, 11, FAILURE_RETENTION).unwrap();
        assert!(
            planner
                .select(Some(IpAddr::V6(Ipv6Addr::LOCALHOST)))
                .is_none()
        );
        let route = planner.select_for_authority("127.0.0.9:80").unwrap();
        planner.report(route, RouteOutcome::SourceSearchRateLimited);
        assert!(planner.snapshot().unwrap().failing_addresses.is_empty());

        let mut workers = Vec::new();
        for _ in 0..8 {
            let planner = planner.clone();
            workers.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    if let Some(route) = planner.select(None) {
                        planner.report(route, RouteOutcome::ConnectionEstablished);
                        planner.free_address(route.local_address);
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    #[ignore = "run through scripts/run_p13_bench.py"]
    fn p13_routeplanner_selection_benchmark_report() {
        const OPERATIONS: u64 = 250_000;
        let cases = [
            (RoutePlannerStrategy::RotateOnBan, "127.0.0.0/8"),
            (RoutePlannerStrategy::LoadBalance, "2001:db8::/48"),
            (RoutePlannerStrategy::NanoSwitch, "2001:db8::/48"),
            (RoutePlannerStrategy::RotatingNanoSwitch, "2001:db8::/48"),
        ];
        let mut results = Vec::new();
        for (strategy, block) in cases {
            let planner =
                RoutePlanner::configured(RoutePlannerConfig::new(strategy, [block])).unwrap();
            for _ in 0..10_000 {
                std::hint::black_box(planner.select(None).unwrap());
            }
            let allocations = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
            let started = Instant::now();
            for _ in 0..OPERATIONS {
                std::hint::black_box(planner.select(None).unwrap());
            }
            let elapsed = started.elapsed();
            let allocation = allocations.change();
            results.push(serde_json::json!({
                "strategy": strategy.class_name(),
                "operations": OPERATIONS,
                "elapsedNanos": u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
                "nanosecondsPerSelection": elapsed.as_nanos() as f64 / OPERATIONS as f64,
                "selectionsPerSecond": OPERATIONS as f64 / elapsed.as_secs_f64(),
                "allocations": allocation.allocations,
                "reallocations": allocation.reallocations,
                "deallocations": allocation.deallocations,
                "bytesAllocated": allocation.bytes_allocated,
                "allocationsPerSelection": allocation.allocations as f64 / OPERATIONS as f64,
            }));
        }
        let report = serde_json::json!({
            "schemaVersion": 1,
            "phase": "P13",
            "benchmarkId": "routeplanner-selection",
            "profile": "release",
            "warmupOperationsPerStrategy": 10_000,
            "results": results,
        });
        println!("CRUST_P13_BENCHMARK={report}");
    }
}
