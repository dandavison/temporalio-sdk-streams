//! The workflow append path: what the worker does with a workflow's own records at the end of each
//! activation, and with its stream when the run ends.

use crate::proto::api::{StreamRef, WorkflowEnding, WorkflowProducer};
use crate::proto::service::{CloseState, StreamClose};
use std::collections::HashMap;
use std::sync::Mutex;

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
