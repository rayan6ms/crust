//! Bounded ownership for long-lived Tokio tasks.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPolicy {
    ReportAndContinue,
    CancelOwner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSpec {
    pub owner: &'static str,
    pub name: &'static str,
    pub error_policy: ErrorPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailure {
    pub message: &'static str,
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for TaskFailure {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailureRecord {
    pub spec: TaskSpec,
    pub failure: TaskFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    pub completed: usize,
    pub failures: Vec<TaskFailureRecord>,
    pub panicked: usize,
    pub aborted_at_deadline: usize,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCapacityError {
    pub capacity: NonZeroUsize,
}

struct TaskExit {
    spec: TaskSpec,
    result: Result<(), TaskFailure>,
}

/// Owns a bounded set of long-lived tasks. It is deliberately not a general
/// job executor: completed tasks remain tracked until the owner shuts down.
pub struct OwnedTasks {
    capacity: NonZeroUsize,
    cancellation: CancellationToken,
    tasks: JoinSet<TaskExit>,
}

impl OwnedTasks {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            cancellation: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn spawn<F, Fut>(&mut self, spec: TaskSpec, task: F) -> Result<(), TaskCapacityError>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), TaskFailure>> + Send + 'static,
    {
        if self.tasks.len() >= self.capacity.get() {
            return Err(TaskCapacityError {
                capacity: self.capacity,
            });
        }
        let child = self.cancellation.child_token();
        let owner_cancellation = self.cancellation.clone();
        self.tasks.spawn(async move {
            let result = task(child).await;
            if result.is_err() && spec.error_policy == ErrorPolicy::CancelOwner {
                owner_cancellation.cancel();
            }
            TaskExit { spec, result }
        });
        Ok(())
    }

    pub async fn shutdown(mut self, deadline: Duration) -> ShutdownReport {
        self.cancellation.cancel();
        let mut report = ShutdownReport {
            completed: 0,
            failures: Vec::new(),
            panicked: 0,
            aborted_at_deadline: 0,
            timed_out: false,
        };
        let deadline = Instant::now() + deadline;
        while !self.tasks.is_empty() {
            match timeout_at(deadline, self.tasks.join_next()).await {
                Ok(Some(result)) => record_result(&mut report, result),
                Ok(None) => break,
                Err(_) => {
                    report.timed_out = true;
                    report.aborted_at_deadline = self.tasks.len();
                    self.tasks.abort_all();
                    break;
                }
            }
        }
        report
    }
}

fn record_result(report: &mut ShutdownReport, result: Result<TaskExit, JoinError>) {
    match result {
        Ok(TaskExit { spec, result }) => {
            report.completed += 1;
            if let Err(failure) = result {
                report.failures.push(TaskFailureRecord { spec, failure });
            }
        }
        Err(_) => report.panicked += 1,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    const TASK: TaskSpec = TaskSpec {
        owner: "test-root",
        name: "cancellation-aware",
        error_policy: ErrorPolicy::ReportAndContinue,
    };

    #[tokio::test]
    async fn cancellation_propagates_and_completion_is_tracked() {
        let mut owner = OwnedTasks::new(NonZeroUsize::new(1).unwrap());
        owner
            .spawn(TASK, |cancellation| async move {
                cancellation.cancelled().await;
                Ok(())
            })
            .unwrap();
        let report = owner.shutdown(Duration::from_millis(50)).await;
        assert_eq!(report.completed, 1);
        assert!(!report.timed_out);
        assert!(report.failures.is_empty());
    }

    #[tokio::test]
    async fn capacity_rejects_an_unowned_extra_task() {
        let mut owner = OwnedTasks::new(NonZeroUsize::new(1).unwrap());
        owner.spawn(TASK, |_| pending()).unwrap();
        assert_eq!(
            owner.spawn(TASK, |_| pending()),
            Err(TaskCapacityError {
                capacity: NonZeroUsize::new(1).unwrap()
            })
        );
        let report = owner.shutdown(Duration::from_millis(1)).await;
        assert_eq!(report.aborted_at_deadline, 1);
    }

    #[tokio::test]
    async fn failures_are_reported_and_cancel_owner_policy_propagates() {
        let mut owner = OwnedTasks::new(NonZeroUsize::new(2).unwrap());
        let observer = owner.cancellation();
        owner
            .spawn(
                TaskSpec {
                    error_policy: ErrorPolicy::CancelOwner,
                    ..TASK
                },
                |_| async {
                    Err(TaskFailure {
                        message: "expected failure",
                    })
                },
            )
            .unwrap();
        tokio::task::yield_now().await;
        assert!(observer.is_cancelled());
        let report = owner.shutdown(Duration::from_millis(50)).await;
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].failure.message, "expected failure");
    }
}
