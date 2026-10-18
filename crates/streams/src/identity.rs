//! The stored identity of a producer, which both backends store with every record.

use crate::error::{Error, Result};
use crate::proto::api::{Producer, producer::Variant};

/// A producer and its epoch. The epoch is (workflow_task, workflow_task_attempt, epoch), compared
/// in that order; run_id and the workflow task fields are set only for workflow and local activity
/// producers.
#[derive(Debug, Clone, PartialEq)]
pub struct Identity {
    pub producer: String,
    pub run_id: String,
    pub workflow_task: i64,
    pub workflow_task_attempt: i32,
    pub epoch: i64,
}

pub fn identity(p: &Producer) -> Result<Identity> {
    let variant = p
        .variant
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("producer is required".into()))?;
    Ok(match variant {
        Variant::Client(c) => Identity {
            producer: format!("client/{}", c.id),
            run_id: String::new(),
            workflow_task: 0,
            workflow_task_attempt: 0,
            epoch: c.epoch,
        },
        Variant::Activity(a) => Identity {
            producer: if a.workflow_run_id.is_empty() {
                format!("activity/{}", a.activity_run_id)
            } else {
                format!("activity/{}/{}", a.workflow_run_id, a.activity_id)
            },
            run_id: String::new(),
            workflow_task: 0,
            workflow_task_attempt: 0,
            epoch: a.attempt_stamp.into(),
        },
        Variant::Workflow(w) => Identity {
            producer: format!("workflow/{}", w.run_id),
            run_id: w.run_id.clone(),
            workflow_task: w.previous_started_event_id,
            workflow_task_attempt: w.attempt,
            epoch: w.attempt.into(),
        },
        Variant::LocalActivity(la) => Identity {
            producer: format!("local-activity/{}/{}", la.run_id, la.activity_id),
            run_id: la.run_id.clone(),
            workflow_task: la.previous_started_event_id,
            workflow_task_attempt: la.workflow_task_attempt,
            epoch: la.attempt.into(),
        },
    })
}
