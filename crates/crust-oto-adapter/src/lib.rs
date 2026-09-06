//! Crust-owned adapter from the backend-neutral voice contract to Oto.

#![forbid(unsafe_code)]

#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> =
    &stats_alloc::INSTRUMENTED_SYSTEM;

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use crust::voice::{
    TimedOpusFrame, VoiceBackend, VoiceClose, VoiceConnection, VoiceConnectionInfo, VoiceCounters,
    VoiceError, VoiceErrorKind, VoiceEvent, VoiceFrameSource, VoiceFuture, VoicePhase,
    VoiceSnapshot,
};
use futures_util::{StreamExt, stream::FuturesUnordered, task::AtomicWaker};
use oto::{FrameSource, FrameStatus, Oto, PacedAudioSender};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const BRIDGE_RUNNING: u8 = 0;
const BRIDGE_ENDED: u8 = 1;
const BRIDGE_FAILED: u8 = 2;
const PRODUCER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct OtoVoiceBackend {
    inner: Arc<BackendInner>,
}

struct BackendInner {
    oto: Oto,
    shutdown: CancellationToken,
    shutdown_serial: AsyncMutex<()>,
    connection_slots: Arc<Semaphore>,
    connect_slots: Arc<Semaphore>,
    connections: Mutex<Vec<Weak<OtoVoiceConnection>>>,
}

impl fmt::Debug for OtoVoiceBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtoVoiceBackend")
            .field(
                "available_connection_slots",
                &self.inner.connection_slots.available_permits(),
            )
            .field(
                "available_connect_slots",
                &self.inner.connect_slots.available_permits(),
            )
            .field("shutting_down", &self.inner.shutdown.is_cancelled())
            .finish()
    }
}

impl OtoVoiceBackend {
    /// Creates a bounded Crust backend around a shared Oto connector.
    ///
    /// `max_connections` bounds live caller-owned handles. A connection attempt
    /// that cannot immediately acquire either admission bound fails explicitly
    /// with [`VoiceErrorKind::Overloaded`].
    pub fn new(oto: Oto, max_connections: usize, max_concurrent_connects: usize) -> Self {
        assert!(max_connections > 0, "max_connections must be nonzero");
        assert!(
            max_concurrent_connects > 0,
            "max_concurrent_connects must be nonzero"
        );
        Self {
            inner: Arc::new(BackendInner {
                oto,
                shutdown: CancellationToken::new(),
                shutdown_serial: AsyncMutex::new(()),
                connection_slots: Arc::new(Semaphore::new(max_connections)),
                connect_slots: Arc::new(Semaphore::new(max_concurrent_connects)),
                connections: Mutex::new(Vec::with_capacity(max_connections)),
            }),
        }
    }

    pub fn with_defaults(
        max_connections: usize,
        max_concurrent_connects: usize,
    ) -> Result<Self, VoiceError> {
        Oto::builder()
            .build()
            .map(|oto| Self::new(oto, max_connections, max_concurrent_connects))
            .map_err(map_oto_error)
    }

    async fn connect_inner(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> Result<Arc<dyn VoiceConnection>, VoiceError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(shutdown_error());
        }
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let connection_slot = Arc::clone(&self.inner.connection_slots)
            .try_acquire_owned()
            .map_err(|_| overloaded_error("voice connection capacity reached"))?;
        let connect_slot = Arc::clone(&self.inner.connect_slots)
            .try_acquire_owned()
            .map_err(|_| overloaded_error("voice connection admission is busy"))?;
        let channel_id = info.channel_id;
        let oto_info = into_oto_info(info);
        let connecting = self.inner.oto.connect(oto_info);
        tokio::pin!(connecting);
        let connection = tokio::select! {
            biased;
            () = self.inner.shutdown.cancelled() => return Err(shutdown_error()),
            () = cancellation.cancelled() => return Err(cancelled_error()),
            result = &mut connecting => result.map_err(map_oto_error)?,
        };
        drop(connect_slot);
        let events = match connection.subscribe_events() {
            Ok(events) => events,
            Err(error) => {
                let _ = connection.shutdown().await;
                return Err(map_oto_error(error));
            }
        };
        let connection = Arc::new(OtoVoiceConnection {
            connection,
            events: AsyncMutex::new(events),
            audio: Mutex::new(AudioSlot::Idle),
            audio_changed: Notify::new(),
            snapshot_sender: Mutex::new(None),
            channel_id: AtomicU64::new(channel_id),
            closed: AtomicBool::new(false),
            shutdown_serial: AsyncMutex::new(()),
            _connection_slot: connection_slot,
        });
        let registered = {
            let mut connections = self
                .inner
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.inner.shutdown.is_cancelled() {
                false
            } else {
                connections.retain(|connection| connection.strong_count() != 0);
                connections.push(Arc::downgrade(&connection));
                true
            }
        };
        if !registered {
            let _ = connection.shutdown_inner().await;
            return Err(shutdown_error());
        }
        Ok(connection)
    }

    async fn shutdown_inner(&self) -> Result<(), VoiceError> {
        let _shutdown = self.inner.shutdown_serial.lock().await;
        self.inner.shutdown.cancel();
        let connections = {
            let mut registered = self
                .inner
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let connections = registered
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            registered.clear();
            connections
        };
        collect_shutdowns(
            connections
                .into_iter()
                .map(|connection| async move { connection.shutdown_inner().await }),
        )
        .await
    }
}

async fn collect_shutdowns<F>(shutdowns: impl IntoIterator<Item = F>) -> Result<(), VoiceError>
where
    F: Future<Output = Result<(), VoiceError>>,
{
    let mut shutdowns: FuturesUnordered<_> = shutdowns.into_iter().collect();
    let mut first_error = None;
    while let Some(result) = shutdowns.next().await {
        if let Err(error) = result
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

impl VoiceBackend for OtoVoiceBackend {
    fn connect(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Arc<dyn VoiceConnection>, VoiceError>> {
        Box::pin(async move { self.connect_inner(info, cancellation).await })
    }

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.shutdown_inner().await })
    }
}

struct OtoVoiceConnection {
    connection: oto::VoiceConnection,
    events: AsyncMutex<oto::EventSubscriber>,
    audio: Mutex<AudioSlot>,
    audio_changed: Notify,
    snapshot_sender: Mutex<Option<PacedAudioSender>>,
    channel_id: AtomicU64,
    closed: AtomicBool,
    shutdown_serial: AsyncMutex<()>,
    _connection_slot: OwnedSemaphorePermit,
}

impl fmt::Debug for OtoVoiceConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtoVoiceConnection")
            .field("state", &self.connection.state())
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

enum AudioSlot {
    Idle,
    Busy,
    Attached(AudioBinding),
    Closed,
}

struct AudioBinding {
    sender: PacedAudioSender,
    producer: BridgeProducer,
    shared: Arc<BridgeShared>,
}

impl OtoVoiceConnection {
    async fn set_source_inner(
        &self,
        source: Arc<dyn VoiceFrameSource>,
        cancellation: CancellationToken,
    ) -> Result<(), VoiceError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(shutdown_error());
        }
        let previous = {
            let mut audio = self
                .audio
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match std::mem::replace(&mut *audio, AudioSlot::Busy) {
                AudioSlot::Idle => None,
                AudioSlot::Attached(binding) => Some(binding),
                AudioSlot::Busy => {
                    *audio = AudioSlot::Busy;
                    return Err(overloaded_error(
                        "audio lifecycle operation is already active",
                    ));
                }
                AudioSlot::Closed => {
                    *audio = AudioSlot::Closed;
                    return Err(shutdown_error());
                }
            }
        };

        // Once the audio slot is Busy, replacement is admitted and must run to
        // an atomic Oto generation boundary. The caller's cancellation token
        // is intentionally an admission gate rather than a mid-command
        // rollback signal.
        let (oto_source, producer, shared) = FrameBridge::spawn(source);
        let binding = if let Some(previous) = previous {
            match previous.sender.replace_source(oto_source).await {
                Ok(_) => {
                    previous.producer.shutdown().await;
                    AudioBinding {
                        sender: previous.sender,
                        producer,
                        shared,
                    }
                }
                Err(error) => {
                    producer.shutdown().await;
                    self.finish_audio(AudioSlot::Attached(previous));
                    return Err(map_oto_error(error));
                }
            }
        } else {
            match self.connection.start_audio(oto_source).await {
                Ok(sender) => AudioBinding {
                    sender,
                    producer,
                    shared,
                },
                Err(error) => {
                    producer.shutdown().await;
                    self.finish_audio(AudioSlot::Idle);
                    return Err(map_oto_error(error));
                }
            }
        };
        *self
            .snapshot_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(binding.sender.clone());
        self.finish_audio(AudioSlot::Attached(binding));
        Ok(())
    }

    async fn stop_audio_inner(&self) -> Result<(), VoiceError> {
        let binding = {
            let mut audio = self
                .audio
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match std::mem::replace(&mut *audio, AudioSlot::Busy) {
                AudioSlot::Idle => {
                    *audio = AudioSlot::Idle;
                    return Ok(());
                }
                AudioSlot::Attached(binding) => binding,
                AudioSlot::Busy => {
                    *audio = AudioSlot::Busy;
                    return Err(overloaded_error(
                        "audio lifecycle operation is already active",
                    ));
                }
                AudioSlot::Closed => {
                    *audio = AudioSlot::Closed;
                    return Ok(());
                }
            }
        };
        let stop_result = binding.sender.stop().await.map_err(map_oto_error);
        binding.producer.shutdown().await;
        *self
            .snapshot_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.finish_audio(if self.closed.load(Ordering::Acquire) {
            AudioSlot::Closed
        } else {
            AudioSlot::Idle
        });
        stop_result.map(|_| ())
    }

    fn finish_audio(&self, slot: AudioSlot) {
        *self
            .audio
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = slot;
        self.audio_changed.notify_waiters();
    }

    async fn shutdown_inner(&self) -> Result<(), VoiceError> {
        let _shutdown = self.shutdown_serial.lock().await;
        let first = !self.closed.swap(true, Ordering::AcqRel);
        if !first {
            return Ok(());
        }
        loop {
            let wait = self.audio_changed.notified();
            let slot = {
                let mut audio = self
                    .audio
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match std::mem::replace(&mut *audio, AudioSlot::Closed) {
                    AudioSlot::Busy => {
                        *audio = AudioSlot::Busy;
                        None
                    }
                    AudioSlot::Attached(binding) => Some(Some(binding)),
                    AudioSlot::Idle | AudioSlot::Closed => Some(None),
                }
            };
            match slot {
                Some(Some(binding)) => {
                    let _ = binding.sender.stop().await;
                    binding.producer.shutdown().await;
                    break;
                }
                Some(None) => break,
                None => wait.await,
            }
        }
        *self
            .snapshot_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.connection
            .shutdown()
            .await
            .map(|_| ())
            .map_err(map_oto_error)
    }

    fn snapshot_inner(&self) -> VoiceSnapshot {
        let connection = self.connection.state();
        let audio = self
            .snapshot_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(PacedAudioSender::state);
        let stats = audio.map_or_else(oto::AudioStats::default, |audio| audio.stats());
        VoiceSnapshot {
            phase: map_phase(connection.phase()),
            channel_id: Some(self.channel_id.load(Ordering::Acquire)),
            ping: connection.gateway_rtt(),
            counters: VoiceCounters {
                sent: stats.frames_sent(),
                nulled: stats.frames_unavailable(),
                deficit: stats.skipped_deadlines(),
            },
        }
    }

    async fn next_event_inner(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Option<VoiceEvent>, VoiceError> {
        let mut events = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(cancelled_error()),
            events = self.events.lock() => events,
        };
        loop {
            let audio_changed = self.audio_changed.notified();
            // Consult the current durable snapshot, rather than trusting an
            // AudioChanged event that may belong to a replaced source. A dead
            // sender cannot deliver more frames even if the gateway is healthy.
            let failed_audio = self
                .snapshot_sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(PacedAudioSender::state)
                .filter(|state| state.phase() == oto::AudioPhase::Failed);
            if let Some(state) = failed_audio {
                let failure = state.failure();
                tracing::warn!(
                    ?failure,
                    source_overruns = state.stats().source_overruns(),
                    source_overrun_wall_us = state.stats().last_source_overrun_wall().as_micros(),
                    source_overrun_cpu_us = state
                        .stats()
                        .last_source_overrun_cpu()
                        .map(|duration| duration.as_micros() as u64),
                    send_failures = state.stats().send_failures(),
                    skipped_deadlines = state.stats().skipped_deadlines(),
                    "audio sender stopped after a terminal failure"
                );
                return Ok(Some(VoiceEvent::Closed(VoiceClose {
                    code: 0,
                    reason: Arc::from(match failure {
                        Some(kind) => format!("audio sender failed: {kind:?}"),
                        None => "audio sender failed".to_owned(),
                    }),
                    by_remote: false,
                })));
            }
            let shared = {
                let audio = self
                    .audio
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*audio {
                    AudioSlot::Attached(binding) => Some(Arc::clone(&binding.shared)),
                    AudioSlot::Idle | AudioSlot::Busy | AudioSlot::Closed => None,
                }
            };
            let source_changed = async {
                match &shared {
                    Some(shared) => shared.changed.notified().await,
                    None => std::future::pending().await,
                }
            };
            if let Some(error) = shared.as_ref().and_then(|shared| shared.take_failure()) {
                return Ok(Some(VoiceEvent::SourceFailed(error)));
            }
            let event = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled_error()),
                () = audio_changed => continue,
                () = source_changed => continue,
                event = events.recv() => event,
            };
            match event {
                Ok(oto::ConnectionEvent::StateChanged { phase, .. }) => {
                    return Ok(Some(VoiceEvent::PhaseChanged(map_phase(phase))));
                }
                Ok(oto::ConnectionEvent::Closed(reason)) => {
                    return Ok(Some(VoiceEvent::Closed(map_close(reason))));
                }
                Ok(oto::ConnectionEvent::Failure(failure)) => {
                    // Oto publishes a durable failure snapshot when a voice
                    // gateway requires fresh Discord voice information. That
                    // state is recoverable by replacing the connection
                    // generation and must not leak as Lavalink's terminal
                    // WebSocketClosedEvent.
                    if matches!(
                        failure.retry_disposition(),
                        oto::RetryDisposition::RetryingInternally
                            | oto::RetryDisposition::NeedsFreshVoiceInfo
                    ) {
                        continue;
                    }
                    return Ok(Some(VoiceEvent::Closed(VoiceClose {
                        code: 0,
                        reason: Arc::from("terminal voice failure"),
                        by_remote: false,
                    })));
                }
                Ok(
                    oto::ConnectionEvent::ResumeStarted { .. }
                    | oto::ConnectionEvent::ResumeSucceeded { .. }
                    | oto::ConnectionEvent::VoiceInfoReplaced { .. }
                    | oto::ConnectionEvent::AudioChanged { .. },
                ) => {}
                Ok(_) => {}
                Err(oto::EventReceiveError::Lagged { .. }) => {
                    return Err(overloaded_error("voice event subscriber lagged"));
                }
                Err(oto::EventReceiveError::Closed) => return Ok(None),
            }
        }
    }
}

impl VoiceConnection for OtoVoiceConnection {
    fn update(
        &self,
        info: VoiceConnectionInfo,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(shutdown_error());
            }
            let channel_id = info.channel_id;
            self.connection
                .replace_voice_info(into_oto_info(info))
                .await
                .map_err(map_oto_error)?;
            self.channel_id.store(channel_id, Ordering::Release);
            Ok(())
        })
    }

    fn set_source(
        &self,
        source: Arc<dyn VoiceFrameSource>,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.set_source_inner(source, cancellation).await })
    }

    fn stop_audio(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.stop_audio_inner().await })
    }

    fn snapshot(&self) -> VoiceFuture<'_, Result<VoiceSnapshot, VoiceError>> {
        Box::pin(async move { Ok(self.snapshot_inner()) })
    }

    fn next_event(
        &self,
        cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Option<VoiceEvent>, VoiceError>> {
        Box::pin(async move { self.next_event_inner(cancellation).await })
    }

    fn disconnect(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.shutdown_inner().await })
    }

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(async move { self.shutdown_inner().await })
    }
}

struct FrameBridge;

impl FrameBridge {
    fn spawn(
        source: Arc<dyn VoiceFrameSource>,
    ) -> (OtoFrameSource, BridgeProducer, Arc<BridgeShared>) {
        let (frames, receiver) = mpsc::channel(1);
        let shared = Arc::new(BridgeShared {
            consumed: AtomicBool::new(false),
            producer_waker: AtomicWaker::new(),
            changed: Notify::new(),
            state: AtomicU8::new(BRIDGE_RUNNING),
            failure: Mutex::new(None),
        });
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_shared = Arc::clone(&shared);
        let task = tokio::spawn(async move {
            run_producer(source, frames, task_shared, task_cancellation).await;
        });
        (
            OtoFrameSource {
                frames: receiver,
                shared: Arc::clone(&shared),
            },
            BridgeProducer {
                cancellation,
                task: Some(task),
            },
            shared,
        )
    }
}

struct BridgeShared {
    consumed: AtomicBool,
    producer_waker: AtomicWaker,
    changed: Notify,
    state: AtomicU8,
    failure: Mutex<Option<VoiceError>>,
}

impl BridgeShared {
    // Exactly one producer waits for exactly one delivered frame. Notify uses
    // a waiter-list mutex here, and an Oracle trace measured 2.19 ms inside
    // notify_one on the audio callback. Register/recheck the atomic permit
    // instead; an early consumption stays ready until the producer takes it.
    fn frame_consumed(&self) {
        self.consumed.store(true, Ordering::Release);
        self.producer_waker.wake();
    }

    fn poll_consumed(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.consumed.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }
        self.producer_waker.register(cx.waker());
        if self.consumed.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn fail(&self, error: VoiceError) {
        *self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
        self.state.store(BRIDGE_FAILED, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn take_failure(&self) -> Option<VoiceError> {
        self.failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

struct OtoFrameSource {
    frames: mpsc::Receiver<TimedOpusFrame>,
    shared: Arc<BridgeShared>,
}

impl FrameSource for OtoFrameSource {
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
        // Tokio's try_recv can park the thread while a producer is publishing.
        // poll_recv instead registers/rechecks readiness and yields on that
        // race (or an exhausted cooperative budget). Channel closure also wakes
        // a pending receiver after EOF, failure, or producer cancellation.
        match self.frames.poll_recv(cx) {
            Poll::Ready(Some(frame)) => {
                let payload = frame.payload();
                if payload.len() > output.len() {
                    self.shared.fail(VoiceError::new(
                        VoiceErrorKind::Protocol,
                        "Oto frame capacity is smaller than the Crust Opus frame",
                    ));
                    self.shared.frame_consumed();
                    return Poll::Ready(FrameStatus::Ended);
                }
                output[..payload.len()].copy_from_slice(payload);
                self.shared.frame_consumed();
                Poll::Ready(FrameStatus::Frame { len: payload.len() })
            }
            Poll::Ready(None) => Poll::Ready(FrameStatus::Ended),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn run_producer(
    source: Arc<dyn VoiceFrameSource>,
    frames: mpsc::Sender<TimedOpusFrame>,
    shared: Arc<BridgeShared>,
    cancellation: CancellationToken,
) {
    loop {
        let source_cancellation = cancellation.clone();
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            result = source.next_frame(source_cancellation) => result,
        };
        let frame = match result {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                shared.state.store(BRIDGE_ENDED, Ordering::Release);
                return;
            }
            Err(error) => {
                shared.fail(error);
                return;
            }
        };
        let delivered = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            result = frames.send(frame) => result.is_ok(),
        };
        if !delivered {
            return;
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            () = futures_util::future::poll_fn(|cx| shared.poll_consumed(cx)) => {}
        }
        if shared.state.load(Ordering::Acquire) != BRIDGE_RUNNING {
            return;
        }
    }
}

struct BridgeProducer {
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl BridgeProducer {
    async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(PRODUCER_SHUTDOWN_TIMEOUT, &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for BridgeProducer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn into_oto_info(info: VoiceConnectionInfo) -> oto::VoiceConnectInfo {
    oto::VoiceConnectInfo::new(
        info.guild_id,
        info.user_id,
        info.channel_id,
        info.session_id.expose(),
        info.endpoint,
        oto::VoiceToken::new(info.token.expose()),
    )
}

fn map_phase(phase: oto::ConnectionPhase) -> VoicePhase {
    match phase {
        oto::ConnectionPhase::Connecting => VoicePhase::ConnectingGateway,
        oto::ConnectionPhase::Handshaking => VoicePhase::WaitingReady,
        oto::ConnectionPhase::EstablishingTransport => VoicePhase::DiscoveringUdp,
        oto::ConnectionPhase::EstablishingDave => VoicePhase::PreparingDave,
        oto::ConnectionPhase::Connected => VoicePhase::Connected,
        oto::ConnectionPhase::Resuming | oto::ConnectionPhase::Reconnecting => {
            VoicePhase::Reconnecting
        }
        oto::ConnectionPhase::NeedsFreshVoiceInfo => VoicePhase::Reconnecting,
        oto::ConnectionPhase::Closing => VoicePhase::Closing,
        oto::ConnectionPhase::Closed | oto::ConnectionPhase::Failed => VoicePhase::Closed,
        _ => VoicePhase::Closed,
    }
}

fn map_close(reason: oto::CloseReason) -> VoiceClose {
    match reason {
        oto::CloseReason::ExplicitShutdown => VoiceClose {
            code: 1_000,
            reason: Arc::from("explicit shutdown"),
            by_remote: false,
        },
        oto::CloseReason::CleanRemote => VoiceClose {
            code: 1_000,
            reason: Arc::from("clean remote close"),
            by_remote: true,
        },
        oto::CloseReason::RemoteCode(code) => VoiceClose {
            code,
            reason: Arc::from("remote voice close"),
            by_remote: true,
        },
        oto::CloseReason::VoiceInfoReplaced => VoiceClose {
            code: 0,
            reason: Arc::from("voice information replaced"),
            by_remote: false,
        },
        oto::CloseReason::TerminalFailure => VoiceClose {
            code: 0,
            reason: Arc::from("terminal voice failure"),
            by_remote: false,
        },
        _ => VoiceClose {
            code: 0,
            reason: Arc::from("voice connection closed"),
            by_remote: false,
        },
    }
}

fn map_oto_error(error: oto::Error) -> VoiceError {
    let kind = match (error.operation(), error.kind()) {
        (
            oto::Operation::StartAudio,
            oto::ErrorKind::DaveRequired | oto::ErrorKind::EndpointOrTls,
        ) => VoiceErrorKind::NotReady,
        (_, oto::ErrorKind::Shutdown) => VoiceErrorKind::Shutdown,
        (_, oto::ErrorKind::ResourceLimit | oto::ErrorKind::Overloaded) => {
            VoiceErrorKind::Overloaded
        }
        (
            _,
            oto::ErrorKind::InvalidConfiguration
            | oto::ErrorKind::InvalidVoiceInfo
            | oto::ErrorKind::RuntimeUnavailable
            | oto::ErrorKind::Superseded,
        ) => VoiceErrorKind::InvalidState,
        (
            _,
            oto::ErrorKind::EndpointOrTls
            | oto::ErrorKind::CredentialsRejected
            | oto::ErrorKind::HeartbeatTimeout
            | oto::ErrorKind::ResumeRejected
            | oto::ErrorKind::NeedsFreshVoiceInfo
            | oto::ErrorKind::UdpDiscovery
            | oto::ErrorKind::SendIo,
        ) => VoiceErrorKind::ConnectionFailed,
        (
            _,
            oto::ErrorKind::GatewayProtocol
            | oto::ErrorKind::UnsupportedTransport
            | oto::ErrorKind::TransportCrypto
            | oto::ErrorKind::DaveRequired
            | oto::ErrorKind::DaveUnsupported
            | oto::ErrorKind::DaveTransition
            | oto::ErrorKind::FrameSourceContract,
        ) => VoiceErrorKind::Protocol,
        _ => VoiceErrorKind::ConnectionFailed,
    };
    let message = match (error.operation(), error.kind()) {
        (oto::Operation::StartAudio, oto::ErrorKind::DaveRequired) => {
            "Oto DAVE setup is not ready for audio"
        }
        (oto::Operation::StartAudio, oto::ErrorKind::EndpointOrTls) => {
            "Oto transport is not ready for audio"
        }
        (oto::Operation::StartAudio, oto::ErrorKind::NeedsFreshVoiceInfo) => {
            "Oto requires fresh voice information before audio can start"
        }
        _ => "Oto voice operation failed",
    };
    VoiceError::new(kind, message)
}

fn cancelled_error() -> VoiceError {
    VoiceError::new(VoiceErrorKind::Cancelled, "voice operation cancelled")
}

fn shutdown_error() -> VoiceError {
    VoiceError::new(VoiceErrorKind::Shutdown, "voice backend is shut down")
}

fn overloaded_error(message: &'static str) -> VoiceError {
    VoiceError::new(VoiceErrorKind::Overloaded, message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    use std::time::Duration;

    use super::*;
    use crust::voice::{MAX_OPUS_PACKET_BYTES, VoiceSecret};
    use oto_testkit::{
        FakeUdpServer, FakeUdpServerConfig, FakeVoiceGateway, FakeVoiceGatewayConfig, ManualClock,
        TestTls, VoiceClose as TestVoiceClose,
    };

    struct TestSource {
        frames: AsyncMutex<mpsc::Receiver<Option<TimedOpusFrame>>>,
        polls: Arc<AtomicUsize>,
    }

    impl VoiceFrameSource for TestSource {
        fn next_frame(
            &self,
            cancellation: CancellationToken,
        ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
            Box::pin(async move {
                self.polls.fetch_add(1, Ordering::AcqRel);
                let mut frames = self.frames.lock().await;
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => Err(cancelled_error()),
                    frame = frames.recv() => Ok(frame.flatten()),
                }
            })
        }
    }

    struct FailingSource;

    impl VoiceFrameSource for FailingSource {
        fn next_frame(
            &self,
            _cancellation: CancellationToken,
        ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
            Box::pin(async {
                Err(VoiceError::new(
                    VoiceErrorKind::Protocol,
                    "synthetic source failure",
                ))
            })
        }
    }

    struct BenchmarkSource {
        sequence: AtomicU64,
    }

    impl VoiceFrameSource for BenchmarkSource {
        fn next_frame(
            &self,
            _cancellation: CancellationToken,
        ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
            Box::pin(async move {
                let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
                TimedOpusFrame::new(
                    sequence,
                    Duration::from_millis(sequence.saturating_mul(20)),
                    Duration::from_millis(20),
                    [0xf8, 0xff, 0xfe],
                )
                .map(Some)
                .map_err(|_| VoiceError::new(VoiceErrorKind::Protocol, "invalid benchmark frame"))
            })
        }
    }

    struct PendingSource;

    impl VoiceFrameSource for PendingSource {
        fn next_frame(
            &self,
            cancellation: CancellationToken,
        ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err(cancelled_error())
            })
        }
    }

    struct AdmissionSource {
        admitted: Arc<tokio::sync::Barrier>,
    }

    impl VoiceFrameSource for AdmissionSource {
        fn next_frame(
            &self,
            cancellation: CancellationToken,
        ) -> VoiceFuture<'_, Result<Option<TimedOpusFrame>, VoiceError>> {
            Box::pin(async move {
                self.admitted.wait().await;
                cancellation.cancelled().await;
                Err(cancelled_error())
            })
        }
    }

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn poll(source: &mut OtoFrameSource, waker: &Waker, output: &mut [u8]) -> Poll<FrameStatus> {
        source.poll_frame(&mut Context::from_waker(waker), output)
    }

    fn frame(sequence: u64) -> TimedOpusFrame {
        TimedOpusFrame::new(
            sequence,
            Duration::from_millis(sequence.saturating_mul(20)),
            Duration::from_millis(20),
            Arc::from([sequence as u8, 0xff]),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn backend_shutdown_polls_all_connections_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let shutdowns = (0..2).map(|_| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                Ok(())
            }
        });
        tokio::time::timeout(Duration::from_millis(100), collect_shutdowns(shutdowns))
            .await
            .expect("connection shutdown futures must be polled together")
            .unwrap();
    }

    async fn eventually(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    async fn eventually_sent(connection: &Arc<dyn VoiceConnection>, expected: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if connection.snapshot().await.unwrap().counters.sent == expected {
                    break;
                }
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("frame was not sent");
    }

    async fn pull_bridge(source: &mut OtoFrameSource, output: &mut [u8]) {
        let status = futures_util::future::poll_fn(|cx| source.poll_frame(cx, output)).await;
        assert_eq!(status, FrameStatus::Frame { len: 3 });
    }

    fn process_pss_kib() -> u64 {
        std::fs::read_to_string("/proc/self/smaps_rollup")
            .expect("process PSS is readable")
            .lines()
            .find_map(|line| {
                line.strip_prefix("Pss:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("PSS is present")
    }

    fn process_threads() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .expect("process status is readable")
            .lines()
            .find_map(|line| {
                line.strip_prefix("Threads:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("thread count is present")
    }

    fn process_cpu_ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").expect("process stat is readable");
        let fields = stat
            .rsplit_once(") ")
            .expect("process command terminator is present")
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let user: u64 = fields[11].parse().expect("user CPU ticks");
        let system: u64 = fields[12].parse().expect("system CPU ticks");
        user + system
    }

    fn percentile(samples: &mut [u64], permille: usize) -> u64 {
        if samples.is_empty() {
            return 0;
        }
        samples.sort_unstable();
        samples[(samples.len() - 1).saturating_mul(permille) / 1_000]
    }

    async fn peer(mut config: FakeVoiceGatewayConfig) -> (FakeVoiceGateway, FakeUdpServer) {
        let udp = FakeUdpServer::start(FakeUdpServerConfig::localhost(ManualClock::new(
            Duration::ZERO,
        )))
        .await
        .unwrap();
        config.voice_ip = udp.local_addr().ip().to_string();
        config.voice_port = udp.local_addr().port();
        let gateway = FakeVoiceGateway::start(config).await.unwrap();
        (gateway, udp)
    }

    async fn peer_with_tls(
        mut config: FakeVoiceGatewayConfig,
        tls: TestTls,
    ) -> (FakeVoiceGateway, FakeUdpServer) {
        let udp = FakeUdpServer::start(FakeUdpServerConfig::localhost(ManualClock::new(
            Duration::ZERO,
        )))
        .await
        .unwrap();
        config.voice_ip = udp.local_addr().ip().to_string();
        config.voice_port = udp.local_addr().port();
        let gateway = FakeVoiceGateway::start_with_tls(config, tls).await.unwrap();
        (gateway, udp)
    }

    fn voice_info(gateway: &FakeVoiceGateway, channel_id: u64) -> VoiceConnectionInfo {
        VoiceConnectionInfo {
            guild_id: 1,
            user_id: 2,
            channel_id,
            endpoint: format!("localhost:{}", gateway.local_addr().port()),
            session_id: VoiceSecret::new("session"),
            token: VoiceSecret::new("token"),
        }
    }

    #[tokio::test]
    async fn consumption_permit_is_retained_and_replaces_the_producer_waker() {
        let (_bridge, producer, shared) = FrameBridge::spawn(Arc::new(PendingSource));
        let first = Arc::new(WakeCounter::default());
        let second = Arc::new(WakeCounter::default());
        let first_waker = Waker::from(Arc::clone(&first));
        let second_waker = Waker::from(Arc::clone(&second));
        let mut cx = Context::from_waker(&first_waker);
        shared.frame_consumed();
        assert_eq!(shared.poll_consumed(&mut cx), Poll::Ready(()));
        assert_eq!(shared.poll_consumed(&mut cx), Poll::Pending);
        let mut cx = Context::from_waker(&second_waker);
        assert_eq!(shared.poll_consumed(&mut cx), Poll::Pending);
        shared.frame_consumed();
        assert_eq!(first.0.load(Ordering::Acquire), 0);
        assert!(second.0.load(Ordering::Acquire) > 0);
        assert_eq!(shared.poll_consumed(&mut cx), Poll::Ready(()));
        assert_eq!(shared.poll_consumed(&mut cx), Poll::Pending);
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn pending_consumer_wakes_when_the_producer_is_cancelled() {
        let (mut bridge, producer, _) = FrameBridge::spawn(Arc::new(PendingSource));
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut output = [0_u8; 16];
        assert_eq!(poll(&mut bridge, &waker, &mut output), Poll::Pending);
        producer.shutdown().await;
        assert!(counter.0.load(Ordering::Acquire) > 0);
        assert_eq!(
            poll(&mut bridge, &waker, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
    }

    #[tokio::test]
    async fn ready_frame_survives_a_cooperative_yield_without_a_missed_wake() {
        let (send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let (mut bridge, producer, _) = FrameBridge::spawn(source);
        send.send(Some(frame(7))).await.unwrap();
        eventually(|| !bridge.frames.is_empty()).await;
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut output = [0_u8; 16];
        while tokio::task::coop::has_budget_remaining() {
            tokio::task::coop::consume_budget().await;
        }
        assert_eq!(poll(&mut bridge, &waker, &mut output), Poll::Pending);
        tokio::task::yield_now().await;
        assert!(counter.0.load(Ordering::Acquire) > 0);
        assert_eq!(
            poll(&mut bridge, &waker, &mut output),
            Poll::Ready(FrameStatus::Frame { len: 2 })
        );
        assert_eq!(&output[..2], &[7, 0xff]);
        producer.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_publication_preserves_every_frame_and_terminal_order() {
        let (send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let (mut bridge, producer, _) = FrameBridge::spawn(source);
        let publishing = tokio::spawn(async move {
            for sequence in 0..4096 {
                send.send(Some(frame(sequence))).await.unwrap();
            }
            send.send(None).await.unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut output = [0_u8; 16];
            for sequence in 0..4096 {
                assert_eq!(
                    futures_util::future::poll_fn(|cx| bridge.poll_frame(cx, &mut output)).await,
                    FrameStatus::Frame { len: 2 }
                );
                assert_eq!(&output[..2], &[sequence as u8, 0xff]);
            }
            assert_eq!(
                futures_util::future::poll_fn(|cx| bridge.poll_frame(cx, &mut output)).await,
                FrameStatus::Ended
            );
        })
        .await
        .expect("publication race must not strand the consumer");
        publishing.await.unwrap();
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn bridge_is_capacity_one_and_pulls_again_only_after_consumption() {
        let (send, receive) = mpsc::channel(4);
        let polls = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::clone(&polls),
        });
        let (mut bridge, producer, _shared) = FrameBridge::spawn(source);
        send.send(Some(frame(1))).await.unwrap();
        send.send(Some(frame(2))).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::Acquire), 1);

        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut output = [0_u8; 16];
        assert_eq!(
            poll(&mut bridge, &waker, &mut output),
            Poll::Ready(FrameStatus::Frame { len: 2 })
        );
        for _ in 0..100 {
            if polls.load(Ordering::Acquire) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(polls.load(Ordering::Acquire), 2);
        assert_eq!(&output[..2], &[1, 0xff]);
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn empty_path_replaces_the_waker_and_wakes_on_nonempty_transition() {
        let (send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let (mut bridge, producer, _shared) = FrameBridge::spawn(source);
        let first = Arc::new(WakeCounter::default());
        let second = Arc::new(WakeCounter::default());
        let first_waker = Waker::from(Arc::clone(&first));
        let second_waker = Waker::from(Arc::clone(&second));
        let mut output = [0_u8; 16];
        assert_eq!(poll(&mut bridge, &first_waker, &mut output), Poll::Pending);
        assert_eq!(poll(&mut bridge, &second_waker, &mut output), Poll::Pending);

        send.send(Some(frame(7))).await.unwrap();
        for _ in 0..100 {
            if second.0.load(Ordering::Acquire) != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(first.0.load(Ordering::Acquire), 0);
        assert!(second.0.load(Ordering::Acquire) > 0);
        assert_eq!(
            poll(&mut bridge, &second_waker, &mut output),
            Poll::Ready(FrameStatus::Frame { len: 2 })
        );
        assert_eq!(&output[..2], &[7, 0xff]);
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn terminal_transition_wakes_pending_consumer_without_polling() {
        let (send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let (mut bridge, producer, _shared) = FrameBridge::spawn(source);
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut output = [0_u8; 16];
        assert_eq!(poll(&mut bridge, &waker, &mut output), Poll::Pending);
        send.send(None).await.unwrap();
        for _ in 0..100 {
            if counter.0.load(Ordering::Acquire) != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(counter.0.load(Ordering::Acquire) > 0);
        assert_eq!(
            poll(&mut bridge, &waker, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn source_failure_is_retained_for_typed_event_delivery() {
        let (mut bridge, producer, shared) = FrameBridge::spawn(Arc::new(FailingSource));
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut output = [0_u8; 16];
        for _ in 0..100 {
            if matches!(
                poll(&mut bridge, &waker, &mut output),
                Poll::Ready(FrameStatus::Ended)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
        let failure = shared.take_failure().expect("source failure is retained");
        assert_eq!(failure.kind, VoiceErrorKind::Protocol);
        assert_eq!(failure.message, "synthetic source failure");
        producer.shutdown().await;
    }

    #[tokio::test]
    async fn undersized_oto_output_fails_instead_of_emitting_an_empty_frame() {
        let (send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let (mut bridge, producer, shared) = FrameBridge::spawn(source);
        send.send(Some(
            TimedOpusFrame::new(
                1,
                Duration::ZERO,
                Duration::from_millis(20),
                Arc::from([1_u8, 2, 3, 4]),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
        tokio::task::yield_now().await;

        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(counter);
        let mut output = [0_u8; 2];
        assert_eq!(
            poll(&mut bridge, &waker, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
        assert_eq!(
            shared
                .take_failure()
                .expect("capacity failure retained")
                .kind,
            VoiceErrorKind::Protocol
        );
        producer.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_adapter_connects_idles_sends_replaces_stops_and_shuts_down() {
        let (gateway, udp) = peer(FakeVoiceGatewayConfig::local()).await;
        let oto = Oto::builder()
            .test_tls_config(gateway.tls().client_config())
            .build()
            .unwrap();
        let backend = OtoVoiceBackend::new(oto, 2, 1);
        let connection = backend
            .connect(voice_info(&gateway, 3), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            connection.snapshot().await.unwrap().phase,
            VoicePhase::Connected
        );
        assert_eq!(
            udp.capture().len(),
            1,
            "connected idle performs only discovery"
        );
        assert!(gateway.speaking().is_empty());

        let (first_send, first_receive) = mpsc::channel(2);
        let first = Arc::new(TestSource {
            frames: AsyncMutex::new(first_receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        connection
            .set_source(first, CancellationToken::new())
            .await
            .unwrap();
        first_send.send(Some(frame(1))).await.unwrap();
        eventually_sent(&connection, 1).await;
        assert_eq!(udp.capture().len(), 2);
        assert!(!gateway.speaking().is_empty());

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let rejected = connection
            .set_source(Arc::new(PendingSource), cancelled)
            .await
            .unwrap_err();
        assert_eq!(rejected.kind, VoiceErrorKind::Cancelled);
        first_send.send(Some(frame(2))).await.unwrap();
        eventually_sent(&connection, 2).await;

        let admitted = Arc::new(tokio::sync::Barrier::new(2));
        let cancellation = CancellationToken::new();
        let replacement = {
            let connection = Arc::clone(&connection);
            let cancellation = cancellation.clone();
            let source = Arc::new(AdmissionSource {
                admitted: Arc::clone(&admitted),
            });
            tokio::spawn(async move { connection.set_source(source, cancellation).await })
        };
        admitted.wait().await;
        cancellation.cancel();
        replacement
            .await
            .unwrap()
            .expect("an admitted replacement reaches one atomic generation boundary");

        let (second_send, second_receive) = mpsc::channel(2);
        let second = Arc::new(TestSource {
            frames: AsyncMutex::new(second_receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        connection
            .set_source(second, CancellationToken::new())
            .await
            .unwrap();
        second_send.send(Some(frame(3))).await.unwrap();
        eventually_sent(&connection, 3).await;
        let counters = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let counters = connection.snapshot().await.unwrap().counters;
                if counters.nulled >= 5 {
                    break counters;
                }
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the pending source did not produce bounded null-frame accounting");
        assert_eq!(counters.sent, 3);
        assert_eq!(counters.deficit, 0);
        connection.stop_audio().await.unwrap();
        eventually(|| udp.capture().len() >= 8).await;

        backend.shutdown().await.unwrap();
        assert_eq!(
            connection.snapshot().await.unwrap().phase,
            VoicePhase::Closed
        );
        gateway.shutdown().await.unwrap();
        udp.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_audio_failure_is_reported_instead_of_leaving_a_playing_zombie() {
        struct InvalidFrame;
        impl FrameSource for InvalidFrame {
            fn poll_frame(&mut self, _: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
                Poll::Ready(FrameStatus::Frame {
                    len: output.len() + 1,
                })
            }
        }
        let (gateway, udp) = peer(FakeVoiceGatewayConfig::local()).await;
        let oto = Oto::builder()
            .test_tls_config(gateway.tls().client_config())
            .build()
            .unwrap();
        let backend = OtoVoiceBackend::new(oto, 1, 1);
        let _public = backend
            .connect_inner(voice_info(&gateway, 3), CancellationToken::new())
            .await
            .unwrap();
        let connection = backend
            .inner
            .connections
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .upgrade()
            .unwrap();
        connection
            .set_source_inner(Arc::new(PendingSource), CancellationToken::new())
            .await
            .unwrap();
        let sender = connection.snapshot_sender.lock().unwrap().clone().unwrap();
        sender.replace_source(InvalidFrame).await.unwrap();
        eventually(|| sender.state().phase() == oto::AudioPhase::Failed).await;
        let observed = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if let Some(VoiceEvent::Closed(close)) = connection
                    .next_event_inner(CancellationToken::new())
                    .await
                    .unwrap()
                {
                    break close;
                }
            }
        })
        .await;
        // Always release the fake peer, including when the regression fails.
        backend.shutdown_inner().await.unwrap();
        gateway.shutdown().await.unwrap();
        udp.shutdown().await.unwrap();
        let close = observed.expect("terminal sender failure was silently ignored");
        assert!(!close.by_remote);
        assert_eq!(
            close.reason.as_ref(),
            "audio sender failed: FrameSourceContract"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_adapter_never_starts_plaintext_audio_when_dave_is_required() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        let (gateway, udp) = peer(config).await;
        let oto = Oto::builder()
            .test_tls_config(gateway.tls().client_config())
            .build()
            .unwrap();
        let backend = OtoVoiceBackend::new(oto, 1, 1);
        let connection = backend
            .connect(voice_info(&gateway, 4), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            connection.snapshot().await.unwrap().phase,
            VoicePhase::PreparingDave
        );
        let (_send, receive) = mpsc::channel(1);
        let source = Arc::new(TestSource {
            frames: AsyncMutex::new(receive),
            polls: Arc::new(AtomicUsize::new(0)),
        });
        let error = connection
            .set_source(source, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, VoiceErrorKind::NotReady);
        assert_eq!(udp.capture().len(), 1);
        assert!(gateway.speaking().is_empty());

        backend.shutdown().await.unwrap();
        gateway.shutdown().await.unwrap();
        udp.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_adapter_maps_ping_resume_fresh_info_and_terminal_close() {
        let tls = TestTls::generate().unwrap();
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_millis(30);
        let (first_gateway, first_udp) = peer_with_tls(config.clone(), tls.clone()).await;
        let (second_gateway, second_udp) = peer_with_tls(config.clone(), tls.clone()).await;
        let (third_gateway, third_udp) = peer_with_tls(config, tls.clone()).await;
        let oto = Oto::builder()
            .test_tls_config(tls.client_config())
            .build()
            .unwrap();
        let backend = OtoVoiceBackend::new(oto, 1, 1);
        let connection = backend
            .connect(voice_info(&first_gateway, 3), CancellationToken::new())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while connection.snapshot().await.unwrap().ping.is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        connection
            .update(voice_info(&second_gateway, 4), CancellationToken::new())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = connection.snapshot().await.unwrap();
                if snapshot.phase == VoicePhase::Connected && snapshot.channel_id == Some(4) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        second_gateway
            .try_close(TestVoiceClose {
                code: 4015,
                reason: "resumable integration interruption".to_owned(),
            })
            .unwrap();
        eventually(|| {
            second_gateway
                .records()
                .iter()
                .any(|record| matches!(record, oto_testkit::GatewayRecord::Resume { .. }))
        })
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while connection.snapshot().await.unwrap().phase != VoicePhase::Connected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        second_gateway
            .try_close(TestVoiceClose {
                code: 4014,
                reason: "fresh voice information required".to_owned(),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match connection
                    .next_event(CancellationToken::new())
                    .await
                    .unwrap()
                {
                    Some(VoiceEvent::PhaseChanged(VoicePhase::Reconnecting)) => break,
                    Some(VoiceEvent::Closed(close)) => {
                        panic!("recoverable fresh-voice state leaked as terminal close: {close:?}")
                    }
                    Some(VoiceEvent::PhaseChanged(_)) | Some(VoiceEvent::SourceFailed(_)) => {}
                    None => panic!("voice event stream ended while awaiting fresh information"),
                }
            }
        })
        .await
        .expect("fresh-voice requirement was not exposed as reconnecting");

        connection
            .update(voice_info(&third_gateway, 5), CancellationToken::new())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = connection.snapshot().await.unwrap();
                if snapshot.phase == VoicePhase::Connected && snapshot.channel_id == Some(5) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        third_gateway
            .try_close(TestVoiceClose {
                code: 4004,
                reason: "terminal integration close".to_owned(),
            })
            .unwrap();
        let close = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(VoiceEvent::Closed(close)) = connection
                    .next_event(CancellationToken::new())
                    .await
                    .unwrap()
                {
                    break close;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(close.reason.as_ref(), "terminal voice failure");
        assert!(!close.by_remote);

        backend.shutdown().await.unwrap();
        first_gateway.shutdown().await.unwrap();
        first_udp.shutdown().await.unwrap();
        second_gateway.shutdown().await.unwrap();
        second_udp.shutdown().await.unwrap();
        third_gateway.shutdown().await.unwrap();
        third_udp.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual release-profile voice bridge benchmark; run with --ignored --nocapture"]
    async fn p10_bridge_target_scale_benchmark_report() {
        let senders = std::env::var("CRUST_P10_BENCH_SENDERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(250);
        assert!((1..=1_000).contains(&senders));
        let baseline_pss = process_pss_kib();
        let baseline_threads = process_threads();

        let mut bridges = Vec::with_capacity(senders);
        let mut producers = Vec::with_capacity(senders + 1);
        for _ in 0..senders {
            let (bridge, producer, _) = FrameBridge::spawn(Arc::new(BenchmarkSource {
                sequence: AtomicU64::new(0),
            }));
            bridges.push(bridge);
            producers.push(producer);
        }
        let (_slow_bridge, slow_producer, _) = FrameBridge::spawn(Arc::new(PendingSource));
        producers.push(slow_producer);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let staged_pss = process_pss_kib();
        let staged_threads = process_threads();

        let mut output = [0_u8; MAX_OPUS_PACKET_BYTES];
        let warmup_cycles = 150_u64;
        for _ in 0..warmup_cycles {
            for bridge in &mut bridges {
                pull_bridge(bridge, &mut output).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let warmed_pss = process_pss_kib();

        let cycles = 150_u64;
        let period = Duration::from_millis(20);
        let start = tokio::time::Instant::now() + period;
        let wall_start = std::time::Instant::now();
        let cpu_before = process_cpu_ticks();
        let allocations = stats_alloc::Region::new(TEST_ALLOCATOR);
        let mut interval_error = Vec::with_capacity(usize::try_from(cycles).unwrap());
        let mut previous = None;
        for cycle in 0..cycles {
            tokio::time::sleep_until(start + period * u32::try_from(cycle).unwrap()).await;
            let now = std::time::Instant::now();
            if let Some(previous) = previous {
                interval_error
                    .push(now.duration_since(previous).abs_diff(period).as_nanos() as u64);
            }
            previous = Some(now);
            for bridge in &mut bridges {
                pull_bridge(bridge, &mut output).await;
            }
        }
        let allocation = allocations.change();
        let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
        let elapsed = wall_start.elapsed();
        let measured_frames = cycles.saturating_mul(senders as u64);
        let active_pss = process_pss_kib();
        let active_threads = process_threads();
        let mut interval_error_for_p99 = interval_error.clone();
        let mut interval_error_for_p999 = interval_error;
        let p99 = percentile(&mut interval_error_for_p99, 990);
        let p999 = percentile(&mut interval_error_for_p999, 999);

        for producer in producers {
            producer.shutdown().await;
        }
        let result = serde_json::json!({
            "schemaVersion": 1,
            "phase": "P10-Oto-revisit",
            "benchmarkId": "crust-oto-capacity-one-bridge",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "senders": senders,
            "warmupCycles": warmup_cycles,
            "cycles": cycles,
            "measuredFrames": measured_frames,
            "durationMs": elapsed.as_millis(),
            "memoryKiB": {
                "baselinePss": baseline_pss,
                "stagedPss": staged_pss,
                "warmedPss": warmed_pss,
                "activePss": active_pss,
                "stagedIncrementPerSender": staged_pss.saturating_sub(baseline_pss) / senders as u64,
                "warmupIncrementPerSender": warmed_pss.saturating_sub(staged_pss) / senders as u64,
                "measurementGrowthPerSender": active_pss.saturating_sub(warmed_pss) / senders as u64
            },
            "threads": {
                "baseline": baseline_threads,
                "staged": staged_threads,
                "active": active_threads
            },
            "tasks": {
                "producerTasks": senders,
                "slowPendingProducerTasks": 1,
                "slowSourceIsolated": true
            },
            "cpu": {
                "clockTicksPerSecond": 100,
                "processTicks": cpu_ticks,
                "percentOfOneLogicalCore": cpu_ticks as f64 * 1_000.0 / elapsed.as_millis().max(1) as f64
            },
            "allocation": {
                "allocations": allocation.allocations,
                "reallocations": allocation.reallocations,
                "bytesAllocated": allocation.bytes_allocated,
                "allocationsPerFrame": allocation.allocations as f64 / measured_frames.max(1) as f64,
                "note": "includes the object-safe boxed VoiceFrameSource future"
            },
            "timingNanos": {
                "p99BatchIntervalError": p99,
                "p999BatchIntervalError": p999
            }
        });
        if let Some(path) = std::env::var_os("CRUST_P10_BENCH_OUTPUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
        }
        println!("P10_OTO_BRIDGE_BENCHMARK={result}");
        assert_eq!(measured_frames, cycles * senders as u64);
        assert_eq!(
            staged_threads, baseline_threads,
            "bridge tasks add no OS threads"
        );
        assert_eq!(
            active_threads, baseline_threads,
            "active bridges add no OS threads"
        );
    }
}
