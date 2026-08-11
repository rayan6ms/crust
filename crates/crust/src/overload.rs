//! Bounded delivery primitives with observable, nonblocking overload outcomes.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub struct OverloadCounters {
    must_deliver_rejections: AtomicU64,
    coalesced_replacements: AtomicU64,
    best_effort_drops: AtomicU64,
}

impl OverloadCounters {
    #[must_use]
    pub fn snapshot(&self) -> OverloadSnapshot {
        OverloadSnapshot {
            must_deliver_rejections: self.must_deliver_rejections.load(Ordering::Relaxed),
            coalesced_replacements: self.coalesced_replacements.load(Ordering::Relaxed),
            best_effort_drops: self.best_effort_drops.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverloadSnapshot {
    pub must_deliver_rejections: u64,
    pub coalesced_replacements: u64,
    pub best_effort_drops: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubmitError<T> {
    Full(T),
    Closed(T),
}

#[derive(Debug, Clone)]
pub struct MustDeliverSender<T> {
    sender: mpsc::Sender<T>,
    counters: Arc<OverloadCounters>,
}

#[derive(Debug)]
pub struct MailboxReceiver<T> {
    receiver: mpsc::Receiver<T>,
}

#[must_use]
pub fn must_deliver_mailbox<T>(
    capacity: NonZeroUsize,
) -> (
    MustDeliverSender<T>,
    MailboxReceiver<T>,
    Arc<OverloadCounters>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let counters = Arc::new(OverloadCounters::default());
    (
        MustDeliverSender {
            sender,
            counters: Arc::clone(&counters),
        },
        MailboxReceiver { receiver },
        counters,
    )
}

impl<T> MustDeliverSender<T> {
    /// Never waits for capacity. The caller retains a rejected lifecycle
    /// command and can map the explicit error to protocol overload behavior.
    pub fn try_send(&self, value: T) -> Result<(), SubmitError<T>> {
        self.sender.try_send(value).map_err(|error| match error {
            mpsc::error::TrySendError::Full(value) => {
                self.counters
                    .must_deliver_rejections
                    .fetch_add(1, Ordering::Relaxed);
                SubmitError::Full(value)
            }
            mpsc::error::TrySendError::Closed(value) => SubmitError::Closed(value),
        })
    }
}

impl<T> MailboxReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }
}

/// A capacity-one latest-value slot for state that is explicitly coalescible.
#[derive(Debug)]
pub struct CoalescingSlot<T> {
    value: Mutex<Option<T>>,
    counters: Arc<OverloadCounters>,
}

impl<T> CoalescingSlot<T> {
    #[must_use]
    pub fn new(counters: Arc<OverloadCounters>) -> Self {
        Self {
            value: Mutex::new(None),
            counters,
        }
    }

    /// Returns true when an older state value was replaced.
    pub fn publish(&self, value: T) -> bool {
        let mut slot = self.value.lock().expect("coalescing slot poisoned");
        let replaced = slot.replace(value).is_some();
        if replaced {
            self.counters
                .coalesced_replacements
                .fetch_add(1, Ordering::Relaxed);
        }
        replaced
    }

    pub fn take(&self) -> Option<T> {
        self.value.lock().expect("coalescing slot poisoned").take()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BestEffortOutcome {
    Accepted,
    DroppedFull,
    Closed,
}

#[derive(Debug, Clone)]
pub struct BestEffortSender<T> {
    sender: mpsc::Sender<T>,
    counters: Arc<OverloadCounters>,
}

#[must_use]
pub fn best_effort_mailbox<T>(
    capacity: NonZeroUsize,
) -> (
    BestEffortSender<T>,
    MailboxReceiver<T>,
    Arc<OverloadCounters>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let counters = Arc::new(OverloadCounters::default());
    (
        BestEffortSender {
            sender,
            counters: Arc::clone(&counters),
        },
        MailboxReceiver { receiver },
        counters,
    )
}

impl<T> BestEffortSender<T> {
    pub fn try_send(&self, value: T) -> BestEffortOutcome {
        match self.sender.try_send(value) {
            Ok(()) => BestEffortOutcome::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters
                    .best_effort_drops
                    .fetch_add(1, Ordering::Relaxed);
                BestEffortOutcome::DroppedFull
            }
            Err(mpsc::error::TrySendError::Closed(_)) => BestEffortOutcome::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn must_deliver_saturation_rejects_without_loss_or_waiting_and_recovers() {
        let (sender, mut receiver, counters) = must_deliver_mailbox(NonZeroUsize::new(1).unwrap());
        sender.try_send("first").unwrap();
        assert_eq!(sender.try_send("second"), Err(SubmitError::Full("second")));
        assert_eq!(counters.snapshot().must_deliver_rejections, 1);
        assert_eq!(receiver.recv().await, Some("first"));
        sender.try_send("second").unwrap();
        assert_eq!(receiver.recv().await, Some("second"));
    }

    #[test]
    fn coalescing_is_limited_to_an_explicit_latest_value_slot() {
        let counters = Arc::new(OverloadCounters::default());
        let slot = CoalescingSlot::new(Arc::clone(&counters));
        assert!(!slot.publish(1));
        assert!(slot.publish(2));
        assert_eq!(slot.take(), Some(2));
        assert_eq!(counters.snapshot().coalesced_replacements, 1);
    }

    #[tokio::test]
    async fn best_effort_drop_is_counted_and_queue_recovers() {
        let (sender, mut receiver, counters) = best_effort_mailbox(NonZeroUsize::new(1).unwrap());
        assert_eq!(sender.try_send(1), BestEffortOutcome::Accepted);
        assert_eq!(sender.try_send(2), BestEffortOutcome::DroppedFull);
        assert_eq!(counters.snapshot().best_effort_drops, 1);
        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(sender.try_send(3), BestEffortOutcome::Accepted);
    }
}
