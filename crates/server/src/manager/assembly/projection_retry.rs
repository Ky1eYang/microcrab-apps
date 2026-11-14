//! One background read-projection retry schedule. Notifications stay dirty until
//! certified publication; Busy never replaces the last Hub value.
use microcrab_session::Error;
use std::time::Duration;
use tokio::time::Instant;

const INITIAL_DELAY: Duration = Duration::from_millis(10);
const MAX_DELAY: Duration = Duration::from_millis(250);

#[derive(Default)]
pub(super) struct ProjectionRetry {
    generation: u64,
    published: u64,
    next: Option<Instant>,
    delay: Option<Duration>,
}

impl ProjectionRetry {
    pub(super) fn changed(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    pub(super) fn attempt(&self, now: Instant) -> Option<u64> {
        (self.generation != self.published && self.next.is_none_or(|at| now >= at))
            .then_some(self.generation)
    }

    /// The watcher owns this wait; dropping its select cancels the timer.
    pub(super) async fn wait(deadline: Option<Instant>) {
        match deadline {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.next
    }

    /// Return true only when the existing concrete failure/resync path is needed.
    pub(super) fn finished(
        &mut self,
        generation: u64,
        result: Result<(), &Error>,
        now: Instant,
    ) -> bool {
        if matches!(result, Err(Error::Busy)) {
            let delay = self
                .delay
                .map_or(INITIAL_DELAY, |delay| (delay * 2).min(MAX_DELAY));
            self.delay = Some(delay);
            self.next = Some(now + delay);
            return false;
        }
        self.published = generation;
        self.delay = None;
        self.next = None;
        result.is_err()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn busy_retries_without_another_notification_and_resets_after_success() {
        let mut retry = ProjectionRetry::default();
        retry.changed();
        let first = retry.attempt(Instant::now()).unwrap();
        assert!(!retry.finished(first, Err(&Error::Busy), Instant::now()));
        ProjectionRetry::wait(retry.deadline()).await;
        let second = retry.attempt(Instant::now()).unwrap();
        assert_eq!(second, first);
        assert!(!retry.finished(second, Ok(()), Instant::now()));
        assert!(retry.attempt(Instant::now()).is_none());
        assert!(retry.deadline().is_none());
    }

    #[test]
    fn event_storm_does_not_postpone_retry_and_backoff_is_capped() {
        let mut retry = ProjectionRetry::default();
        let mut now = Instant::now();
        retry.changed();
        for expected in [10, 20, 40, 80, 160, 250, 250, 250] {
            let generation = retry.attempt(now).unwrap();
            assert!(!retry.finished(generation, Err(&Error::Busy), now));
            let deadline = retry.deadline().unwrap();
            assert_eq!(deadline - now, Duration::from_millis(expected));
            for _ in 0..100 {
                retry.changed();
                assert_eq!(retry.deadline(), Some(deadline));
                assert!(retry.attempt(now).is_none());
            }
            now = deadline;
        }
        let generation = retry.attempt(now).unwrap();
        retry.changed();
        assert!(!retry.finished(generation, Ok(()), now));
        assert!(retry.attempt(now).is_some());
        let generation = retry.attempt(now).unwrap();
        assert!(!retry.finished(generation, Err(&Error::Busy), now));
        assert_eq!(retry.deadline().unwrap() - now, INITIAL_DELAY);
    }

    #[tokio::test]
    async fn cancellation_interrupts_timer_without_detached_retry() {
        let mut retry = ProjectionRetry::default();
        retry.changed();
        let now = Instant::now();
        retry.finished(retry.attempt(now).unwrap(), Err(&Error::Busy), now);
        let close = tokio_util::sync::CancellationToken::new();
        close.cancel();
        tokio::select! {
            biased;
            _ = close.cancelled() => {},
            _ = ProjectionRetry::wait(retry.deadline()) => panic!("closed watcher retried"),
        }
    }

    #[tokio::test]
    async fn changes_during_busy_read_remain_dirty_and_timer_publishes_latest_value() {
        // Register before reading, as the real watcher does. A notification
        // arriving during refresh remains pending even without a later change.
        let (changes, mut receiver) = tokio::sync::watch::channel(7);
        let mut certified = 7;
        let mut retry = ProjectionRetry::default();
        retry.changed();
        let generation = retry.attempt(Instant::now()).unwrap();
        changes.send_replace(42);
        assert!(!retry.finished(generation, Err(&Error::Busy), Instant::now()));
        assert_eq!(certified, 7);
        receiver.changed().await.unwrap();
        retry.changed();
        assert!(
            retry
                .attempt(retry.deadline().unwrap() - Duration::from_nanos(1))
                .is_none()
        );
        ProjectionRetry::wait(retry.deadline()).await;
        let generation = retry.attempt(Instant::now()).unwrap();
        certified = *receiver.borrow_and_update();
        assert!(!retry.finished(generation, Ok(()), Instant::now()));
        assert_eq!(certified, 42);
        assert!(!receiver.has_changed().unwrap());
        assert!(retry.attempt(Instant::now()).is_none());
    }

    #[test]
    fn non_busy_failure_keeps_resync_path_without_an_autonomous_retry_loop() {
        let mut retry = ProjectionRetry::default();
        retry.changed();
        let now = Instant::now();
        let generation = retry.attempt(now).unwrap();
        let error = Error::tool("projection storage read failed");
        assert!(retry.finished(generation, Err(&error), now));
        assert!(retry.deadline().is_none());
        assert!(retry.attempt(now).is_none());
        retry.changed();
        assert!(retry.attempt(now).is_some());
    }
}
