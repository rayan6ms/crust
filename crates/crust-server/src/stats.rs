//! Lavalink-compatible statistics backed by bounded, native process probes.

use std::collections::BTreeMap;
use std::fs;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::session::{SessionHandle, SessionPlayerStats, SessionRegistry};

const RUNTIME_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FrameStats {
    sent: u32,
    nulled: u32,
    deficit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct MemoryStats {
    free: u64,
    used: u64,
    allocated: u64,
    reservable: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CpuStats {
    cores: u32,
    system_load: f64,
    lavalink_load: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatsSnapshot {
    frame_stats: Option<FrameStats>,
    players: u32,
    playing_players: u32,
    uptime: u64,
    memory: MemoryStats,
    cpu: CpuStats,
}

pub(crate) struct StatsCollector {
    started: Instant,
    runtime: Mutex<RuntimeSampler>,
}

impl std::fmt::Debug for StatsCollector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatsCollector")
            .field("uptime", &self.started.elapsed())
            .finish_non_exhaustive()
    }
}

impl StatsCollector {
    pub(crate) fn new() -> Self {
        Self::with_probe(Arc::new(NativeProbe), RUNTIME_REFRESH_INTERVAL)
    }

    fn with_probe(probe: Arc<dyn RuntimeProbe>, refresh_interval: Duration) -> Self {
        Self {
            started: Instant::now(),
            runtime: Mutex::new(RuntimeSampler::new(probe, refresh_interval)),
        }
    }

    pub(crate) fn rest(&self, sessions: &SessionRegistry) -> StatsSnapshot {
        self.snapshot(sessions.player_stats(), None)
    }

    pub(crate) fn websocket(
        &self,
        sessions: &SessionRegistry,
        session: &SessionHandle,
    ) -> StatsSnapshot {
        let frame_stats = session.frame_stats();
        self.snapshot(sessions.player_stats(), frame_stats)
    }

    fn snapshot(
        &self,
        players: Vec<SessionPlayerStats>,
        frame_stats: Option<FrameStats>,
    ) -> StatsSnapshot {
        let playing_players = players.iter().filter(|player| player.playing).count();
        let runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sample();
        StatsSnapshot {
            frame_stats,
            players: wire_count(players.len()),
            playing_players: wire_count(playing_players),
            uptime: wire_milliseconds(self.started.elapsed()),
            memory: runtime.memory,
            cpu: runtime.cpu,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct FrameWindow {
    previous: BTreeMap<String, SessionPlayerStats>,
}

impl FrameWindow {
    pub(crate) fn update(
        &mut self,
        players: Vec<(String, SessionPlayerStats)>,
    ) -> Option<FrameStats> {
        let current = players.into_iter().collect::<BTreeMap<_, _>>();
        let mut sent = 0_u64;
        let mut nulled = 0_u64;
        let mut deficit = 0_u64;
        let mut eligible = 0_u64;
        for (guild_id, player) in &current {
            let Some(previous) = self.previous.get(guild_id) else {
                continue;
            };
            if !previous.playing || !player.playing {
                continue;
            }
            eligible = eligible.saturating_add(1);
            sent = sent.saturating_add(counter_delta(player.sent, previous.sent));
            nulled = nulled.saturating_add(counter_delta(player.nulled, previous.nulled));
            deficit = deficit.saturating_add(counter_delta(player.deficit, previous.deficit));
        }
        self.previous = current;
        if eligible == 0 {
            return None;
        }
        Some(FrameStats {
            sent: wire_counter(sent / eligible),
            nulled: wire_counter(nulled / eligible),
            deficit: wire_counter(deficit / eligible),
        })
    }
}

fn counter_delta(current: u64, previous: u64) -> u64 {
    if current >= previous {
        current - previous
    } else {
        // Voice source replacement creates a fresh Oto sender and resets its
        // counters. The new generation's current value is its entire delta.
        current
    }
}

fn wire_counter(value: u64) -> u32 {
    value.min(i32::MAX as u64) as u32
}

fn wire_count(value: usize) -> u32 {
    u32::try_from(value)
        .unwrap_or(i32::MAX as u32)
        .min(i32::MAX as u32)
}

fn wire_milliseconds(value: Duration) -> u64 {
    u64::try_from(value.as_millis())
        .unwrap_or(i64::MAX as u64)
        .min(i64::MAX as u64)
}

#[derive(Debug, Clone, Copy, Default)]
struct ProbeSnapshot {
    resident_bytes: Option<u64>,
    reservable_bytes: Option<u64>,
    system_busy: Option<u64>,
    system_total: Option<u64>,
    process_cpu_nanos: Option<u64>,
}

trait RuntimeProbe: Send + Sync + 'static {
    fn read(&self) -> ProbeSnapshot;
}

struct NativeProbe;

impl RuntimeProbe for NativeProbe {
    fn read(&self) -> ProbeSnapshot {
        native_probe()
    }
}

struct RuntimeSampler {
    probe: Arc<dyn RuntimeProbe>,
    refresh_interval: Duration,
    previous: Option<(Instant, ProbeSnapshot)>,
    cached: Option<RuntimeStats>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeStats {
    memory: MemoryStats,
    cpu: CpuStats,
}

impl RuntimeSampler {
    fn new(probe: Arc<dyn RuntimeProbe>, refresh_interval: Duration) -> Self {
        Self {
            probe,
            refresh_interval,
            previous: None,
            cached: None,
        }
    }

    fn sample(&mut self) -> RuntimeStats {
        let now = Instant::now();
        if let (Some((sampled_at, _)), Some(cached)) = (self.previous, self.cached)
            && now.duration_since(sampled_at) < self.refresh_interval
        {
            return cached;
        }
        let current = self.probe.read();
        let cores = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(i32::MAX as usize) as u32;
        let cpu = self.previous.map_or(
            CpuStats {
                cores,
                system_load: 0.0,
                lavalink_load: 0.0,
            },
            |(sampled_at, previous)| CpuStats {
                cores,
                system_load: system_load(previous, current),
                lavalink_load: process_load(
                    previous,
                    current,
                    now.duration_since(sampled_at),
                    cores,
                ),
            },
        );
        let used = current.resident_bytes.unwrap_or(0).min(i64::MAX as u64);
        let reservable = current
            .reservable_bytes
            .unwrap_or(used)
            .max(used)
            .min(i64::MAX as u64);
        let sampled = RuntimeStats {
            // Rust exposes no portable allocator equivalent of the JVM's
            // committed-but-free heap. RSS is therefore both used and
            // allocated, with free explicitly zero.
            memory: MemoryStats {
                free: 0,
                used,
                allocated: used,
                reservable,
            },
            cpu,
        };
        self.previous = Some((now, current));
        self.cached = Some(sampled);
        sampled
    }
}

fn system_load(previous: ProbeSnapshot, current: ProbeSnapshot) -> f64 {
    let (Some(previous_busy), Some(previous_total), Some(current_busy), Some(current_total)) = (
        previous.system_busy,
        previous.system_total,
        current.system_busy,
        current.system_total,
    ) else {
        return 0.0;
    };
    let busy = current_busy.saturating_sub(previous_busy);
    let total = current_total.saturating_sub(previous_total);
    if total == 0 {
        return 0.0;
    }
    finite_load(busy as f64 / total as f64)
}

fn process_load(
    previous: ProbeSnapshot,
    current: ProbeSnapshot,
    elapsed: Duration,
    cores: u32,
) -> f64 {
    let (Some(previous_cpu), Some(current_cpu)) =
        (previous.process_cpu_nanos, current.process_cpu_nanos)
    else {
        return 0.0;
    };
    let elapsed_nanos = elapsed.as_nanos();
    if elapsed_nanos == 0 || cores == 0 {
        return 0.0;
    }
    let cpu_nanos = current_cpu.saturating_sub(previous_cpu) as f64;
    finite_load(cpu_nanos / elapsed_nanos as f64 / f64::from(cores))
}

fn finite_load(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

#[cfg(target_os = "linux")]
fn native_probe() -> ProbeSnapshot {
    let resident_bytes = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| parse_kib_field(&status, "VmRSS:"));
    let physical_bytes = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|memory| parse_kib_field(&memory, "MemTotal:"));
    let cgroup_limit = cgroup_memory_limit();
    let reservable_bytes = match (physical_bytes, cgroup_limit) {
        (Some(physical), Some(limit)) => Some(physical.min(limit)),
        (physical, limit) => physical.or(limit),
    };
    let (system_busy, system_total) = fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|stat| parse_system_cpu(&stat))
        .map_or((None, None), |(busy, total)| (Some(busy), Some(total)));
    let process_cpu_nanos = fs::read_to_string("/proc/self/schedstat")
        .ok()
        .and_then(|stat| stat.split_whitespace().next()?.parse().ok());
    ProbeSnapshot {
        resident_bytes,
        reservable_bytes,
        system_busy,
        system_total,
        process_cpu_nanos,
    }
}

#[cfg(not(target_os = "linux"))]
fn native_probe() -> ProbeSnapshot {
    ProbeSnapshot::default()
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit() -> Option<u64> {
    let hierarchy = fs::read_to_string("/proc/self/cgroup").ok();
    let v2 = hierarchy.as_deref().and_then(|contents| {
        contents.lines().find_map(|line| {
            let mut fields = line.splitn(3, ':');
            let hierarchy = fields.next()?;
            let controllers = fields.next()?;
            let path = fields.next()?.trim_start_matches('/');
            (hierarchy == "0" && controllers.is_empty()).then_some(path)
        })
    });
    let v2_path = v2.map_or_else(
        || PathBuf::from("/sys/fs/cgroup/memory.max"),
        |path| {
            PathBuf::from("/sys/fs/cgroup")
                .join(path)
                .join("memory.max")
        },
    );
    let v1 = hierarchy.as_deref().and_then(|contents| {
        contents.lines().find_map(|line| {
            let mut fields = line.splitn(3, ':');
            fields.next()?;
            let controllers = fields.next()?;
            let path = fields.next()?.trim_start_matches('/');
            controllers
                .split(',')
                .any(|controller| controller == "memory")
                .then_some(path)
        })
    });
    let v1_path = v1.map_or_else(
        || PathBuf::from("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        |path| {
            PathBuf::from("/sys/fs/cgroup/memory")
                .join(path)
                .join("memory.limit_in_bytes")
        },
    );
    let v2 = fs::read_to_string(v2_path)
        .ok()
        .and_then(|value| parse_finite_limit(&value));
    let v1 = fs::read_to_string(v1_path)
        .ok()
        .and_then(|value| parse_finite_limit(&value));
    v2.or(v1)
}

#[cfg(target_os = "linux")]
fn parse_finite_limit(value: &str) -> Option<u64> {
    let value = value.trim().parse::<u64>().ok()?;
    // v1 represents an unlimited hierarchy with a value close to i64::MAX.
    (value < (1_u64 << 60)).then_some(value)
}

#[cfg(target_os = "linux")]
fn parse_kib_field(input: &str, field: &str) -> Option<u64> {
    let value = input
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    value.checked_mul(1024)
}

#[cfg(target_os = "linux")]
fn parse_system_cpu(input: &str) -> Option<(u64, u64)> {
    let fields = input
        .lines()
        .find_map(|line| line.strip_prefix("cpu "))?
        .split_whitespace()
        .take(8)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if fields.len() < 5 {
        return None;
    }
    let total = fields
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(*value))?;
    let idle = fields[3].checked_add(fields[4])?;
    Some((total.saturating_sub(idle), total))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct SequenceProbe {
        index: AtomicUsize,
        samples: Vec<ProbeSnapshot>,
    }

    impl RuntimeProbe for SequenceProbe {
        fn read(&self) -> ProbeSnapshot {
            let index = self.index.fetch_add(1, Ordering::AcqRel);
            self.samples[index.min(self.samples.len() - 1)]
        }
    }

    #[test]
    fn frame_windows_are_per_player_averages_and_survive_counter_reset() {
        let mut window = FrameWindow::default();
        let playing = |sent, nulled, deficit| SessionPlayerStats {
            playing: true,
            sent,
            nulled,
            deficit,
        };
        assert_eq!(
            window.update(vec![
                ("1".to_owned(), playing(10, 2, 1)),
                ("2".to_owned(), playing(20, 4, 3)),
            ]),
            None
        );
        assert_eq!(
            window.update(vec![
                ("1".to_owned(), playing(20, 4, 3)),
                ("2".to_owned(), playing(40, 8, 7)),
            ]),
            Some(FrameStats {
                sent: 15,
                nulled: 3,
                deficit: 3,
            })
        );
        assert_eq!(
            window.update(vec![("1".to_owned(), playing(3, 1, 0))]),
            Some(FrameStats {
                sent: 3,
                nulled: 1,
                deficit: 0,
            })
        );
    }

    #[test]
    fn runtime_sampler_maps_rss_and_bounded_cpu_deltas() {
        let probe = Arc::new(SequenceProbe {
            index: AtomicUsize::new(0),
            samples: vec![
                ProbeSnapshot {
                    resident_bytes: Some(100),
                    reservable_bytes: Some(1_000),
                    system_busy: Some(20),
                    system_total: Some(100),
                    process_cpu_nanos: Some(10),
                },
                ProbeSnapshot {
                    resident_bytes: Some(120),
                    reservable_bytes: Some(1_000),
                    system_busy: Some(70),
                    system_total: Some(200),
                    process_cpu_nanos: Some(u64::MAX),
                },
            ],
        });
        let mut sampler = RuntimeSampler::new(probe, Duration::ZERO);
        let first = sampler.sample();
        assert_eq!(first.memory.used, 100);
        assert_eq!(first.memory.allocated, 100);
        assert_eq!(first.memory.free, 0);
        assert_eq!(first.memory.reservable, 1_000);
        assert_eq!(first.cpu.system_load, 0.0);
        let second = sampler.sample();
        assert_eq!(second.memory.used, 120);
        assert_eq!(second.cpu.system_load, 0.5);
        assert!((0.0..=1.0).contains(&second.cpu.lavalink_load));
    }

    #[test]
    fn runtime_probe_is_cached_for_the_bounded_refresh_interval() {
        let probe = Arc::new(SequenceProbe {
            index: AtomicUsize::new(0),
            samples: vec![ProbeSnapshot {
                resident_bytes: Some(100),
                ..ProbeSnapshot::default()
            }],
        });
        let mut sampler = RuntimeSampler::new(probe.clone(), Duration::from_secs(30));
        assert_eq!(sampler.sample().memory.used, 100);
        assert_eq!(sampler.sample().memory.used, 100);
        assert_eq!(probe.index.load(Ordering::Acquire), 1);
    }

    #[test]
    fn unavailable_runtime_values_and_wire_overflow_degrade_safely() {
        let probe = Arc::new(SequenceProbe {
            index: AtomicUsize::new(0),
            samples: vec![ProbeSnapshot::default()],
        });
        let mut sampler = RuntimeSampler::new(probe, Duration::from_secs(30));
        let sample = sampler.sample();
        assert_eq!(sample.memory.used, 0);
        assert_eq!(sample.memory.allocated, 0);
        assert_eq!(sample.memory.free, 0);
        assert_eq!(sample.memory.reservable, 0);
        assert_eq!(sample.cpu.system_load, 0.0);
        assert_eq!(sample.cpu.lavalink_load, 0.0);
        assert_eq!(wire_counter(u64::MAX), i32::MAX as u32);
        assert_eq!(wire_count(usize::MAX), i32::MAX as u32);
        assert_eq!(wire_milliseconds(Duration::MAX), i64::MAX as u64);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_probe_parsers_reject_malformed_or_unbounded_values() {
        assert_eq!(parse_kib_field("VmRSS: 12 kB\n", "VmRSS:"), Some(12_288));
        assert_eq!(parse_kib_field("VmRSS: nope kB\n", "VmRSS:"), None);
        assert_eq!(
            parse_system_cpu("cpu  10 2 3 80 5 0 0 0\ncpu0 1 1 1 1"),
            Some((15, 100))
        );
        assert_eq!(parse_finite_limit("max"), None);
        assert_eq!(parse_finite_limit("1152921504606846976"), None);
        assert_eq!(parse_finite_limit("4096"), Some(4096));
    }
}
