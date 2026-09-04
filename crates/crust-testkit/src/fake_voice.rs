use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crust::voice::{
    TimedOpusFrame, VoiceBackend, VoiceConnection, VoiceConnectionInfo, VoiceCounters, VoiceError,
    VoiceErrorKind, VoiceEvent, VoiceFrameSource, VoiceFuture, VoicePhase, VoiceSnapshot,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeVoiceRecord {
    Connect {
        guild_id: u64,
        user_id: u64,
        channel_id: u64,
    },
    Update {
        guild_id: u64,
        user_id: u64,
        channel_id: u64,
    },
    SetSource {
        generation: u64,
    },
    StopAudio,
    ShutdownConnection,
    ShutdownBackend,
}

#[derive(Clone)]
pub struct FakeVoiceBackend {
    inner: Arc<BackendInner>,
}

struct BackendInner {
    record_capacity: usize,
    connection_capacity: usize,
    initial_phase: VoicePhase,
    records: Mutex<VecDeque<FakeVoiceRecord>>,
    connections: Mutex<Vec<Weak<FakeVoiceConnection>>>,
    shutdown: AtomicBool,
}

impl std::fmt::Debug for FakeVoiceBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeVoiceBackend")
            .field("records", &self.records().len())
            .field("shutdown", &self.inner.shutdown.load(Ordering::Acquire))
            .finish()
    }
}

impl FakeVoiceBackend {
    pub fn new(record_capacity: usize, connection_capacity: usize) -> Self {
        Self::new_with_phase(record_capacity, connection_capacity, VoicePhase::Connected)
    }

    #[must_use]
    pub fn new_with_phase(
        record_capacity: usize,
        connection_capacity: usize,
        initial_phase: VoicePhase,
    ) -> Self {
        assert!(record_capacity > 0);
        assert!(connection_capacity > 0);
        Self {
            inner: Arc::new(BackendInner {
                record_capacity,
                connection_capacity,
                initial_phase,
                records: Mutex::new(VecDeque::with_capacity(record_capacity)),
                connections: Mutex::new(Vec::with_capacity(connection_capacity)),
                shutdown: AtomicBool::new(false),
            }),
        }
    }

    #[must_use]
    pub fn records(&self) -> Vec<FakeVoiceRecord> {
        self.inner
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn latest_connection(&self) -> Option<Arc<FakeVoiceConnection>> {
        self.inner
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(Weak::upgrade)
    }

    fn record(&self, record: FakeVoiceRecord) -> Result<(), VoiceError> {
        let mut records = self
            .inner
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if records.len() == self.inner.record_capacity {
            return Err(overloaded("fake voice record queue is full"));
        }
        records.push_back(record);
        Ok(())
    }
}

impl VoiceBackend for FakeVoiceBackend {
    fn connect(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Arc<dyn VoiceConnection>, VoiceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(shutdown());
            }
            {
                let mut connections = self
                    .inner
                    .connections
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                connections.retain(|connection| connection.strong_count() != 0);
                if connections.len() == self.inner.connection_capacity {
                    return Err(overloaded("fake voice connection capacity reached"));
                }
            }
            self.record(FakeVoiceRecord::Connect {
                guild_id: info.guild_id,
                user_id: info.user_id,
                channel_id: info.channel_id,
            })?;
            let connection = Arc::new(FakeVoiceConnection::new(
                self.clone(),
                info.channel_id,
                self.inner.initial_phase,
            ));
            self.inner
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(Arc::downgrade(&connection));
            Ok(connection as Arc<dyn VoiceConnection>)
        })
    }

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move {
            if self.inner.shutdown.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.record(FakeVoiceRecord::ShutdownBackend)?;
            let connections = self
                .inner
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for connection in connections {
                connection.shutdown_inner()?;
            }
            Ok(())
        })
    }
}

pub struct FakeVoiceConnection {
    backend: FakeVoiceBackend,
    source: AsyncMutex<Option<Arc<dyn VoiceFrameSource>>>,
    generation: AtomicU64,
    snapshot: Mutex<VoiceSnapshot>,
    events: AsyncMutex<mpsc::Receiver<VoiceEvent>>,
    event_sender: mpsc::Sender<VoiceEvent>,
    shutdown: AtomicBool,
}

impl std::fmt::Debug for FakeVoiceConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeVoiceConnection")
            .field("generation", &self.generation.load(Ordering::Acquire))
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl FakeVoiceConnection {
    fn new(backend: FakeVoiceBackend, channel_id: u64, phase: VoicePhase) -> Self {
        let (event_sender, events) = mpsc::channel(16);
        Self {
            backend,
            source: AsyncMutex::new(None),
            generation: AtomicU64::new(0),
            snapshot: Mutex::new(VoiceSnapshot {
                phase,
                channel_id: Some(channel_id),
                ping: Some(Duration::from_millis(7)),
                counters: VoiceCounters {
                    sent: 0,
                    nulled: 0,
                    deficit: 0,
                },
            }),
            events: AsyncMutex::new(events),
            event_sender,
            shutdown: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn source_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn snapshot(&self) -> VoiceSnapshot {
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn pull_frame(&self) -> Result<Option<TimedOpusFrame>, VoiceError> {
        let source = self.source.lock().await.clone();
        let Some(source) = source else {
            return Ok(None);
        };
        let frame = match source.next_frame(CancellationToken::new()).await {
            Ok(frame) => frame,
            Err(error) => {
                self.try_push_event(VoiceEvent::SourceFailed(error.clone()))?;
                return Err(error);
            }
        };
        if frame.is_some() {
            self.snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .counters
                .sent += 1;
        }
        Ok(frame)
    }

    pub fn set_ping(&self, ping: Option<Duration>) {
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ping = ping;
    }

    pub fn set_counters(&self, counters: VoiceCounters) {
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .counters = counters;
    }

    pub fn try_transition_to(&self, phase: VoicePhase) -> Result<(), VoiceError> {
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = phase;
        self.try_push_event(VoiceEvent::PhaseChanged(phase))
    }

    pub fn try_push_event(&self, event: VoiceEvent) -> Result<(), VoiceError> {
        self.event_sender
            .try_send(event)
            .map_err(|_| overloaded("fake voice event queue is full"))
    }

    fn shutdown_inner(&self) -> Result<(), VoiceError> {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.backend.record(FakeVoiceRecord::ShutdownConnection)?;
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = VoicePhase::Closed;
        Ok(())
    }
}

impl VoiceConnection for FakeVoiceConnection {
    fn update(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Err(shutdown());
            }
            self.backend.record(FakeVoiceRecord::Update {
                guild_id: info.guild_id,
                user_id: info.user_id,
                channel_id: info.channel_id,
            })?;
            self.snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .channel_id = Some(info.channel_id);
            Ok(())
        })
    }

    fn set_source(
        &self,
        source: Arc<dyn VoiceFrameSource>,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Err(shutdown());
            }
            if self
                .snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase
                != VoicePhase::Connected
            {
                return Err(VoiceError::new(
                    VoiceErrorKind::NotReady,
                    "fake voice connection is not ready for audio",
                ));
            }
            let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
            self.backend
                .record(FakeVoiceRecord::SetSource { generation })?;
            *self.source.lock().await = Some(source);
            Ok(())
        })
    }

    fn stop_audio(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move {
            self.backend.record(FakeVoiceRecord::StopAudio)?;
            *self.source.lock().await = None;
            Ok(())
        })
    }

    fn snapshot(&self) -> VoiceFuture<'_, Result<VoiceSnapshot, VoiceError>> {
        Box::pin(async move { Ok(FakeVoiceConnection::snapshot(self)) })
    }

    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Option<VoiceEvent>, VoiceError>> {
        Box::pin(async move {
            let mut events = self.events.lock().await;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(cancelled()),
                event = events.recv() => Ok(event),
            }
        })
    }

    fn disconnect(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.shutdown_inner() })
    }

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.shutdown_inner() })
    }
}

fn cancelled() -> VoiceError {
    VoiceError::new(VoiceErrorKind::Cancelled, "fake voice operation cancelled")
}

fn shutdown() -> VoiceError {
    VoiceError::new(VoiceErrorKind::Shutdown, "fake voice backend is shut down")
}

fn overloaded(message: &'static str) -> VoiceError {
    VoiceError::new(VoiceErrorKind::Overloaded, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crust::voice::{VOICE_PACING_AUTHORITY, VoicePacingAuthority, VoiceSecret};

    fn info(guild_id: u64) -> VoiceConnectionInfo {
        VoiceConnectionInfo {
            guild_id,
            user_id: 2,
            channel_id: 3,
            endpoint: "voice.example.invalid".into(),
            session_id: VoiceSecret::new("session"),
            token: VoiceSecret::new("token"),
        }
    }

    #[tokio::test]
    async fn connection_and_record_bounds_fail_explicitly() {
        let backend = FakeVoiceBackend::new(8, 1);
        let connection = backend
            .connect(info(1), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(VOICE_PACING_AUTHORITY, VoicePacingAuthority::Backend);
        assert_eq!(
            connection.snapshot().await.unwrap().ping,
            Some(Duration::from_millis(7))
        );
        let error = backend
            .connect(info(4), CancellationToken::new())
            .await
            .err()
            .expect("the connection limit must reject a fourth connection");
        assert_eq!(error.kind, VoiceErrorKind::Overloaded);
        connection.shutdown().await.unwrap();
        backend.shutdown().await.unwrap();
    }
}
