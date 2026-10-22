//! The rules for stream references and settings that every backend applies. The native server
//! applies the same rules itself.

use crate::error::{Error, Result};
use crate::proto::api::StreamRef;
use crate::proto::service::StreamSettings;
use std::time::Duration;

/// The longest name of an owner's stream.
pub const MAX_NAME_BYTES: usize = 255;

/// A standalone stream's idle timeout when its creator sets none.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(7 * 24 * 3600);

/// Checks that a reference names exactly one of a workflow, an activity and a standalone stream,
/// with a name of 1 to MAX_NAME_BYTES bytes for an owner's stream and none for a standalone stream.
pub fn validate(stream: &StreamRef) -> Result<()> {
    let ids = [&stream.workflow_id, &stream.activity_id, &stream.stream_id]
        .iter()
        .filter(|id| !id.is_empty())
        .count();
    if ids != 1 {
        return Err(Error::InvalidArgument(
            "a stream is named by exactly one of a workflow ID, an activity ID and a stream ID"
                .into(),
        ));
    }
    if !stream.stream_id.is_empty() && !stream.name.is_empty() {
        return Err(Error::InvalidArgument(
            "a standalone stream has no name".into(),
        ));
    }
    if stream.stream_id.is_empty() && (stream.name.is_empty() || stream.name.len() > MAX_NAME_BYTES)
    {
        return Err(Error::InvalidArgument(format!(
            "a stream of a workflow or activity needs a name of 1 to {MAX_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

/// The idle timeout a create gives the stream: for a standalone stream, the settings', or
/// DEFAULT_IDLE_TIMEOUT if they set none. A stream of an owner closes with its owner, and does not
/// accept one.
pub fn idle_timeout(stream: &StreamRef, settings: Option<&StreamSettings>) -> Result<Duration> {
    let idle = match settings.and_then(|s| s.idle_timeout) {
        Some(d) => Duration::try_from(d).map_err(|e| {
            Error::InvalidArgument(format!("the idle timeout must not be negative: {e}"))
        })?,
        None => Duration::ZERO,
    };
    match (stream.stream_id.is_empty(), idle.is_zero()) {
        (true, true) => Ok(Duration::ZERO),
        (true, false) => Err(Error::InvalidArgument(
            "a stream of a workflow or activity closes with its owner, and has no idle timeout"
                .into(),
        )),
        (false, true) => Ok(DEFAULT_IDLE_TIMEOUT),
        (false, false) => Ok(idle),
    }
}
