//! The size limits on an append, the same as the native server's. Only payload bytes count, as on
//! the server: topics do not.

use crate::error::{Error, Result};
use crate::proto::service::{AppendRecord, StreamFailureType};
use prost::Message;

pub const MAX_RECORDS_PER_APPEND: usize = 1000;
pub const MAX_RECORD_BYTES: usize = 1 << 20;
pub const MAX_APPEND_BYTES: usize = 4 << 20;

/// Checks that one append fits the limits.
pub fn check(records: &[AppendRecord]) -> Result<()> {
    if records.len() > MAX_RECORDS_PER_APPEND {
        return Err(too_large(format!(
            "{} records exceed the limit of {MAX_RECORDS_PER_APPEND} per append",
            records.len()
        )));
    }
    let mut total = 0;
    for r in records {
        total += record_bytes(r)?;
    }
    if total > MAX_APPEND_BYTES {
        return Err(too_large(format!(
            "an append of {total} bytes exceeds the limit of {MAX_APPEND_BYTES}"
        )));
    }
    Ok(())
}

/// Splits records, in order, into appends that each fit the limits. A record over the per-record
/// limit fails the whole split, so nothing is appended.
pub fn split(records: Vec<AppendRecord>) -> Result<Vec<Vec<AppendRecord>>> {
    let mut appends = vec![];
    let mut current = vec![];
    let mut current_bytes = 0;
    for r in records {
        let bytes = record_bytes(&r)?;
        if current.len() == MAX_RECORDS_PER_APPEND || current_bytes + bytes > MAX_APPEND_BYTES {
            appends.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(r);
    }
    if !current.is_empty() {
        appends.push(current);
    }
    Ok(appends)
}

fn record_bytes(r: &AppendRecord) -> Result<usize> {
    let bytes = r.payload.as_ref().map_or(0, Message::encoded_len);
    if bytes > MAX_RECORD_BYTES {
        return Err(too_large(format!(
            "a record of {bytes} bytes exceeds the limit of {MAX_RECORD_BYTES}"
        )));
    }
    Ok(bytes)
}

fn too_large(message: String) -> Error {
    Error::stream(StreamFailureType::TooLarge, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Payload;

    fn record(bytes: usize) -> AppendRecord {
        AppendRecord {
            topic: String::new(),
            payload: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
        }
    }

    #[test]
    fn split_respects_count_and_bytes() {
        let appends = split((0..2500).map(|_| record(1)).collect()).unwrap();
        assert_eq!(
            appends.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1000, 1000, 500]
        );
        let appends = split((0..5).map(|_| record(MAX_RECORD_BYTES - 16)).collect()).unwrap();
        assert!(appends.iter().all(|a| check(a).is_ok()));
        assert_eq!(appends.iter().map(Vec::len).sum::<usize>(), 5);
    }

    #[test]
    fn oversized_record_is_rejected() {
        assert!(matches!(
            split(vec![record(MAX_RECORD_BYTES + 1)]),
            Err(Error::Stream {
                failure_type: StreamFailureType::TooLarge,
                ..
            })
        ));
    }
}
