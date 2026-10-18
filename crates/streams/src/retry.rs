//! Bounded retries of transient failures. The deadline bounds when a retry may start, not how long
//! one attempt runs, so a call can take longer than the deadline.

use crate::error::{Error, Result};
use std::future::Future;
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// How long transient failures are retried before the call fails.
const RETRY_DEADLINE: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Calls `f` until it returns something other than [`Error::Transient`], or the retry deadline
/// passes, in which case the call fails as [`Error::Unavailable`].
pub async fn with_retries<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + RETRY_DEADLINE;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match f().await {
            Err(Error::Transient(message)) => {
                if Instant::now() + backoff > deadline {
                    return Err(Error::Unavailable(message));
                }
                sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            result => return result,
        }
    }
}
