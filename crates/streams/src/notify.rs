#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::Mutex;
    use tokio::time::{Instant, sleep};

    fn stream(name: &str) -> StreamRef {
        StreamRef {
            workflow_id: "wf".into(),
            first_run_id: "run".into(),
            name: name.into(),
            ..Default::default()
        }
    }

    /// A coalescer whose notify records the stream and when it is called, and fails the calls that
    /// `fails` says.
    fn recording(fails: fn(usize) -> bool) -> (Arc<Coalescer>, Arc<Mutex<Vec<(String, Instant)>>>) {
        let calls = Arc::new(Mutex::new(vec![]));
        let recorded = calls.clone();
        let coalescer = Coalescer::new(
            Duration::from_millis(100),
            Arc::new(move |stream: StreamRef| {
                let mut calls = recorded.lock().unwrap();
                calls.push((stream.name, Instant::now()));
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
            coalescer.appended(&stream("main"));
            sleep(Duration::from_millis(25)).await;
        }
        let last_append = start + Duration::from_millis(225);
        sleep(Duration::from_secs(1)).await;
        let calls: Vec<Instant> = calls.lock().unwrap().iter().map(|c| c.1).collect();
        assert!(!calls.is_empty() && calls.len() < 10, "{calls:?}");
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
        coalescer.appended(&stream("main"));
        sleep(Duration::from_secs(1)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        coalescer.appended(&stream("main"));
        sleep(Duration::from_secs(1)).await;
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn each_stream_of_an_owner_is_notified() {
        let (coalescer, calls) = recording(|_| false);
        coalescer.appended(&stream("a"));
        coalescer.appended(&stream("b"));
        sleep(Duration::from_secs(1)).await;
        let mut names: Vec<String> = calls.lock().unwrap().iter().map(|c| c.0.clone()).collect();
        names.sort();
        assert_eq!(names, ["a", "b"]);
    }
}
