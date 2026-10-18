use crate::error::{Error, Result};
use crate::limits;
use crate::native::NativeBackend;
use crate::proto::api::{
    self, AppendResponse, CloseResponse, Producer, WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse, WorkflowEvictedRequest, WorkflowEvictedResponse,
    WorkflowProducer, producer::Variant,
};
use crate::proto::service;
use crate::redis::RedisBackend;
use crate::workflow::{Runs, close_for_ending, commit_mode_messages};
use prost::Message;
use temporalio_client::Connection;
use temporalio_protos::temporal::api::protocol::v1::Message as ProtocolMessage;

enum Backend {
    Native(NativeBackend),
    Redis(RedisBackend),
}

/// One stream backend, and the worker's state of the runs that append to it.
pub struct Streams {
    backend: Backend,
    runs: Runs,
}

impl Streams {
    pub fn native(connection: Connection, options: api::NativeOptions) -> Self {
        Self {
            backend: Backend::Native(NativeBackend::new(connection, options)),
            runs: Runs::default(),
        }
    }

    pub async fn redis(options: api::RedisOptions) -> Result<Self> {
        Ok(Self {
            backend: Backend::Redis(RedisBackend::connect(options).await?),
            runs: Runs::default(),
        })
    }

    /// Runs the named call on its encoded request and returns its encoded response.
    pub async fn call(&self, name: &str, request: &[u8]) -> Result<Vec<u8>> {
        Ok(match name {
            "Append" => AppendResponse {
                cursors: self.append(api::AppendRequest::decode(request)?).await?,
            }
            .encode_to_vec(),
            "Close" => {
                self.close(api::CloseRequest::decode(request)?).await?;
                CloseResponse {}.encode_to_vec()
            }
            "Read" => self
                .read(api::ReadRequest::decode(request)?)
                .await?
                .encode_to_vec(),
            "WorkflowActivationCompleted" => WorkflowActivationCompletedResponse {
                protocol_messages: self
                    .workflow_activation_completed(WorkflowActivationCompletedRequest::decode(
                        request,
                    )?)
                    .await?,
            }
            .encode_to_vec(),
            "WorkflowEvicted" => {
                self.workflow_evicted(WorkflowEvictedRequest::decode(request)?)
                    .await?;
                WorkflowEvictedResponse {}.encode_to_vec()
            }
            _ => return Err(Error::InvalidArgument(format!("unknown call {name}"))),
        })
    }

    async fn append(&self, req: api::AppendRequest) -> Result<Vec<Vec<u8>>> {
        match &self.backend {
            Backend::Native(b) => b.append(req).await,
            Backend::Redis(b) => b.append(req).await,
        }
    }

    async fn close(&self, req: api::CloseRequest) -> Result<()> {
        match &self.backend {
            Backend::Native(b) => b.close(req).await,
            Backend::Redis(b) => b.close(req).await,
        }
    }

    async fn read(&self, req: api::ReadRequest) -> Result<service::ReadResponse> {
        match &self.backend {
            Backend::Native(b) => b.read(req).await,
            Backend::Redis(b) => b.read(req).await,
        }
    }

    /// Appends an activation's records, in appends that fit the size limits, or in commit mode
    /// returns them as protocol messages for the activation's completion. If the completion ends
    /// the run, a Redis stream's close is kept until the eviction that says the server accepted it;
    /// a native stream is closed by the server.
    async fn workflow_activation_completed(
        &self,
        req: WorkflowActivationCompletedRequest,
    ) -> Result<Vec<ProtocolMessage>> {
        let stream = api::StreamRef {
            workflow_id: req.workflow_id,
            first_run_id: req.first_run_id,
        };
        let owner = WorkflowProducer {
            run_id: req.run_id,
            previous_started_event_id: req.previous_started_event_id,
            attempt: req.attempt,
        };
        let mut run = self.runs.for_task(stream, owner);
        if let Backend::Native(native) = &self.backend
            && native.commit_mode()
        {
            let messages = commit_mode_messages(&mut run, req.records);
            self.runs.put(run);
            return messages;
        }
        for records in limits::split(req.records)? {
            let appended = self
                .append(api::AppendRequest {
                    stream: Some(run.stream.clone()),
                    producer: Some(Producer {
                        variant: Some(Variant::Workflow(run.owner.clone())),
                    }),
                    seq: run.next_seq,
                    records,
                    finish: false,
                })
                .await;
            if let Err(e) = appended {
                self.runs.put(run);
                return Err(e);
            }
            run.next_seq += 1;
        }
        if let (Some(ending), Backend::Redis(_)) = (req.ending, &self.backend) {
            run.pending_close = close_for_ending(ending);
        }
        self.runs.put(run);
        Ok(vec![])
    }

    /// Forgets an evicted run, first writing its pending close if the eviction says the server
    /// accepted the run's terminal completion.
    async fn workflow_evicted(&self, req: WorkflowEvictedRequest) -> Result<()> {
        let Some(run) = self.runs.remove(&req.run_id) else {
            return Ok(());
        };
        let (true, Some(close)) = (req.execution_ending, run.pending_close) else {
            return Ok(());
        };
        self.close(api::CloseRequest {
            stream: Some(run.stream),
            owner: Some(run.owner),
            seq: run.next_seq,
            close: Some(close),
        })
        .await
    }
}
