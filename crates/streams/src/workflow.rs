//! The workflow append path: what the worker does with a workflow's own records at the end of each
//! activation, and with its stream when the run ends.

use crate::error::Result;
use crate::limits;
use crate::proto::api::{StreamRef, WorkflowEnding, WorkflowProducer};
use crate::proto::service::{AppendRecord, CloseState, CommitModeAppend, StreamClose};
use prost::{Message, Name};
use std::collections::HashMap;
use std::sync::Mutex;
use temporalio_protos::temporal::api::protocol::v1::Message as ProtocolMessage;

/// What the worker knows of a run's appends: its current workflow task attempt, the next seq in
/// that attempt, and a close to write once the server has accepted the run's terminal completion.
#[derive(Default)]
pub struct Runs(Mutex<HashMap<String, Run>>);

pub struct Run {
    pub stream: StreamRef,
    pub owner: WorkflowProducer,
    pub next_seq: i64,
    pub pending_close: Option<StreamClose>,
}

impl Runs {
    /// Returns the run's state for this workflow task attempt, starting seq again at 0 when the
    /// attempt is a new one.
    pub fn for_task(&self, stream: StreamRef, owner: WorkflowProducer) -> Run {
        let mut runs = self.0.lock().unwrap();
        match runs.remove(&owner.run_id) {
            Some(run) if run.owner == owner => run,
            _ => Run {
                stream,
                owner,
                next_seq: 0,
                pending_close: None,
            },
        }
    }

    pub fn put(&self, run: Run) {
        self.0.lock().unwrap().insert(run.owner.run_id.clone(), run);
    }

    pub fn remove(&self, run_id: &str) -> Option<Run> {
        self.0.lock().unwrap().remove(run_id)
    }
}

/// The close to write when the run ends, or none if its run chain continues: a completed cron run,
/// or a failed run with a retry policy, which the server may retry. Whether a retry happens is the
/// server's decision, so a failed run with a retry policy is never closed by the worker.
pub fn close_for_ending(ending: WorkflowEnding) -> Option<StreamClose> {
    let close = ending.close?;
    let chain_continues = match close.state() {
        CloseState::Completed => ending.cron,
        CloseState::Failed => ending.retry_policy,
        _ => false,
    };
    (!chain_continues).then_some(close)
}

/// The records as commit-mode protocol messages, one for each append that fits the size limits,
/// with IDs unique within the run's workflow task attempt.
pub fn commit_mode_messages(
    run: &mut Run,
    records: Vec<AppendRecord>,
) -> Result<Vec<ProtocolMessage>> {
    limits::split(records)?
        .into_iter()
        .map(|records| {
            let owner = &run.owner;
            let id = format!(
                "stream/{}/{}/{}/{}",
                owner.run_id, owner.previous_started_event_id, owner.attempt, run.next_seq
            );
            run.next_seq += 1;
            let append = CommitModeAppend { records };
            let mut message = ProtocolMessage {
                id,
                protocol_instance_id: "stream".into(),
                body: Some(Default::default()),
                ..Default::default()
            };
            if let Some(body) = message.body.as_mut() {
                body.type_url = format!("type.googleapis.com/{}", CommitModeAppend::full_name());
                body.value = append.encode_to_vec();
            }
            Ok(message)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Payload;

    fn run() -> Run {
        Run {
            stream: StreamRef::default(),
            owner: WorkflowProducer {
                run_id: "run".into(),
                previous_started_event_id: 3,
                attempt: 1,
            },
            next_seq: 0,
            pending_close: None,
        }
    }

    fn records(n: usize) -> Vec<AppendRecord> {
        (0..n)
            .map(|i| AppendRecord {
                topic: String::new(),
                payload: Some(Payload {
                    data: i.to_string().into_bytes(),
                    ..Default::default()
                }),
            })
            .collect()
    }

    #[test]
    fn commit_mode_messages_fit_size_limits_and_have_unique_ids() {
        let mut run = run();
        let mut messages = commit_mode_messages(&mut run, records(2500)).unwrap();
        messages.extend(commit_mode_messages(&mut run, records(1)).unwrap());
        let sizes: Vec<usize> = messages
            .iter()
            .map(|m| {
                CommitModeAppend::decode(m.body.as_ref().unwrap().value.as_slice())
                    .unwrap()
                    .records
                    .len()
            })
            .collect();
        assert_eq!(sizes, vec![1000, 1000, 500, 1]);
        let mut ids: Vec<&str> = messages.iter().map(|m| m.id.as_str()).collect();
        ids.dedup();
        assert_eq!(ids.len(), 4);
        assert!(messages.iter().all(|m| m.body.as_ref().unwrap().type_url
            == "type.googleapis.com/temporal.api.nexusservices.streamservice.v1.CommitModeAppend"));
    }
}
