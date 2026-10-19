//! Notify for writers to a stream in an external store: an activity's or a client's appends wake
//! the owner's waiting workflow readers through the server, which cannot see the store.

use crate::error::Result;
use crate::proto::api::StreamRef;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

pub type NotifyFn =
    Arc<dyn Fn(StreamRef) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Sends a Notify for appends to each stream at most once per interval, and always one after the
/// stream's last append. A Notify starts at once after an append when none was sent within the
/// interval. A failed Notify (after its retries) fails nothing: it is logged, and the next append
/// sends another. Until one succeeds, readers may not be woken.
pub struct Coalescer {
    interval: Duration,
    notify: NotifyFn,
    /// Streams with a Notify task, and whether an append came after that task's latest Notify
    /// started.
    pending: Mutex<HashMap<String, bool>>,
}

impl Coalescer {
    pub fn new(interval: Duration, notify: NotifyFn) -> Arc<Self> {
        Arc::new(Self {
            interval,
            notify,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Records an append to the stream, starting its Notify task if it has none.
    pub fn appended(self: &Arc<Self>, stream: &StreamRef) {
        let key = format!(
            "{}/{}/{}/{}",
            stream.workflow_id, stream.first_run_id, stream.activity_id, stream.activity_run_id
        );
        let mut pending = self.pending.lock().unwrap();
        if pending.insert(key.clone(), true).is_none() {
            tokio::spawn(self.clone().run(key, stream.clone()));
        }
    }

    async fn run(self: Arc<Self>, key: String, stream: StreamRef) {
        loop {
            {
                let mut pending = self.pending.lock().unwrap();
                if !pending.insert(key.clone(), false).unwrap_or_default() {
                    pending.remove(&key);
                    return;
                }
            }
            let started = Instant::now();
            if let Err(e) = (self.notify)(stream.clone()).await {
                tracing::warn!(error = %e, ?stream, "Notify after a stream append failed");
            }
            sleep_until(started + self.interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::Mutex;
    use tokio::time::{Instant, sleep};

    fn stream() -> StreamRef {
        StreamRef {
            workflow_id: "wf".into(),
            first_run_id: "run".into(),
            ..Default::default()
        }
    }

    /// A coalescer whose notify records when it is called, and fails the calls that `fails` says.
    fn recording(fails: fn(usize) -> bool) -> (Arc<Coalescer>, Arc<Mutex<Vec<Instant>>>) {
        let calls = Arc::new(Mutex::new(vec![]));
        let recorded = calls.clone();
        let coalescer = Coalescer::new(
            Duration::from_millis(100),
            Arc::new(move |_| {
                let mut calls = recorded.lock().unwrap();
                calls.push(Instant::now());
                let result = if fails(calls.len() - 1) {
                    Err(Error::Internal("refused".into()))
                } else {
                    Ok(())
                };
                Box::pin(async move { result })
            }),
        );
        (coalescer, calls)
    }

    #[tokio::test(start_paused = true)]
    async fn notifies_at_most_once_per_interval_and_after_the_last_append() {
        let (coalescer, calls) = recording(|_| false);
        let start = Instant::now();
        for _ in 0..10 {
            coalescer.appended(&stream());
            sleep(Duration::from_millis(25)).await;
        }
        let last_append = start + Duration::from_millis(225);
        sleep(Duration::from_secs(1)).await;
        let calls = calls.lock().unwrap();
        assert!(calls.len() < 10, "{calls:?}");
        assert!(
            calls
                .windows(2)
                .all(|w| w[1] - w[0] >= Duration::from_millis(100)),
            "{calls:?}"
        );
        assert!(*calls.last().unwrap() >= last_append, "{calls:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_notify_is_sent_again_after_the_next_append() {
        let (coalescer, calls) = recording(|call| call == 0);
        coalescer.appended(&stream());
        sleep(Duration::from_secs(1)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        coalescer.appended(&stream());
        sleep(Duration::from_secs(1)).await;
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
}
