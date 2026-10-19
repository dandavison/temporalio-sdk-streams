//! The workflow append path: what the worker does with a workflow's own records at the end of each
//! activation, and with its stream when the run ends.

use crate::error::Result;
use crate::limits;
use crate::proto::api::{StreamRef, WorkflowEnding, WorkflowProducer};
use crate::proto::service::{
    AppendRecord, CloseState, CommitModeAppend, StreamClose, StreamMode, StreamModeDeclaration,
};
use prost::{Message, Name};
use std::collections::HashMap;
use std::sync::Mutex;
use temporalio_protos::temporal::api::protocol::v1::Message as ProtocolMessage;

/// What the worker knows of a run's appends: its current workflow task attempt, the next seq in
/// that attempt, whether the attempt declared commit mode, whether the workflow closed its stream,
/// and a close to write once the server has accepted the run's terminal completion. Keyed by run ID; an entry is removed when the run is
/// evicted, so a close by an evicted run is forgotten, and the worker then writes the ending's close
/// too, which fails as CLOSED and is logged.
#[derive(Default)]
pub struct Runs(Mutex<HashMap<String, Run>>);

pub struct Run {
    pub stream: StreamRef,
    pub owner: WorkflowProducer,
    pub next_seq: i64,
    pub declared: bool,
    pub closed: bool,
    pub pending_close: Option<StreamClose>,
}

impl Runs {
    /// Returns the run's state for this workflow task attempt, starting seq again at 0 when the
    /// attempt is a new one.
    pub fn for_task(&self, stream: StreamRef, owner: WorkflowProducer) -> Run {
        let mut runs = self.0.lock().unwrap();
        match runs.remove(&owner.run_id) {
            Some(run) if run.owner == owner => run,
            Some(run) => Run {
                stream,
                owner,
                next_seq: 0,
                declared: false,
                closed: run.closed,
                pending_close: None,
            },
            None => Run {
                stream,
                owner,
                next_seq: 0,
                declared: false,
                closed: false,
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

/// The close to write when the run ends, or none if its run chain may continue: a cron run that
/// completes or fails, or a failed run with a retry policy, which the server may retry. Whether a
/// retry happens is the server's decision, so a failed run with a retry policy is never closed by
/// the worker, even when the server does not retry it (attempts exhausted, or a non-retryable
/// failure); its stream stays open until its retention passes. The native server closes such a
/// stream, because it knows whether the chain continues.
pub fn close_for_ending(ending: WorkflowEnding) -> Option<StreamClose> {
    let close = ending.close?;
    let chain_continues = match close.state() {
        CloseState::Completed => ending.cron,
        CloseState::Failed => ending.cron || ending.retry_policy,
        _ => false,
    };
    (!chain_continues).then_some(close)
}

/// The records as commit-mode protocol messages, one for each append that fits the size limits,
/// after a declaration of the run's mode if `declare` is set and the task attempt has not declared
/// it, with IDs unique within the run's workflow task attempt. The server assigns the appends' seqs
/// itself; next_seq only makes the IDs unique. Nothing limits the total: all of a workflow task's
/// messages travel in its one completion, so enough records can exceed the server's request size
/// limit.
pub fn commit_mode_messages(
    run: &mut Run,
    records: Vec<AppendRecord>,
    declare: bool,
) -> Result<Vec<ProtocolMessage>> {
    let mut messages = vec![];
    if declare && !run.declared {
        run.declared = true;
        messages.push(message(
            run,
            &StreamModeDeclaration {
                mode: StreamMode::Commit.into(),
            },
        ));
    }
    for records in limits::split(records)? {
        messages.push(message(run, &CommitModeAppend { records }));
    }
    Ok(messages)
}

fn message<B: Message + Name>(run: &mut Run, body: &B) -> ProtocolMessage {
    let owner = &run.owner;
    let id = format!(
        "stream/{}/{}/{}/{}",
        owner.run_id, owner.previous_started_event_id, owner.attempt, run.next_seq
    );
    run.next_seq += 1;
    let mut message = ProtocolMessage {
        id,
        protocol_instance_id: "stream".into(),
        body: Some(Default::default()),
        ..Default::default()
    };
    if let Some(any) = message.body.as_mut() {
        any.type_url = format!("type.googleapis.com/{}", B::full_name());
        any.value = body.encode_to_vec();
    }
    message
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
            declared: false,
            closed: false,
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

    fn declared(messages: &[ProtocolMessage]) -> Vec<bool> {
        messages
            .iter()
            .map(|m| {
                m.body.as_ref().unwrap().type_url
                    == "type.googleapis.com/temporal.api.nexusservices.streamservice.v1.StreamModeDeclaration"
            })
            .collect()
    }

    #[test]
    fn commit_mode_declaration_comes_first_once_per_task_attempt() {
        let mut run = run();
        let first = commit_mode_messages(&mut run, records(1), true).unwrap();
        assert_eq!(declared(&first), [true, false]);
        let body = StreamModeDeclaration::decode(first[0].body.as_ref().unwrap().value.as_slice())
            .unwrap();
        assert_eq!(body.mode(), StreamMode::Commit);
        assert_eq!(
            declared(&commit_mode_messages(&mut run, vec![], true).unwrap()),
            Vec::<bool>::new()
        );
        assert_eq!(
            declared(&commit_mode_messages(&mut self::run(), vec![], true).unwrap()),
            [true]
        );
        assert_eq!(
            declared(&commit_mode_messages(&mut self::run(), records(1), false).unwrap()),
            [false]
        );
    }

    #[test]
    fn commit_mode_messages_fit_size_limits_and_have_unique_ids() {
        let mut run = run();
        let mut messages = commit_mode_messages(&mut run, records(2500), false).unwrap();
        messages.extend(commit_mode_messages(&mut run, records(1), false).unwrap());
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
