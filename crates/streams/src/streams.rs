use crate::error::{Error, Result};
use crate::limits;
use crate::native::NativeBackend;
use crate::proto::api::{
    self, AppendResponse, CloseResponse, Producer, WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse, WorkflowEvictedRequest, WorkflowEvictedResponse,
    WorkflowProducer, WorkflowStreamWrites, producer::Variant,
};
use crate::proto::service;
use crate::redis::RedisBackend;
use crate::workflow::{Run, Runs, close_for_ending, commit_mode_messages};
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

    /// Runs the named call on its encoded request and returns its encoded response. The calls and
    /// their messages are in `protos/temporal/sdk/streams/v1/streams.proto`.
    pub async fn call(&self, name: &str, request: &[u8]) -> Result<Vec<u8>> {
        Ok(match name {
            "Create" => service::CreateResponse {
                created: self.create(api::CreateRequest::decode(request)?).await?,
            }
            .encode_to_vec(),
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

    async fn create(&self, req: api::CreateRequest) -> Result<bool> {
        match &self.backend {
            Backend::Native(b) => b.create(req).await,
            Backend::Redis(b) => b.create(req).await,
        }
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

    /// Appends an activation's records to each of the workflow's streams that it wrote to, in
    /// appends that fit the size limits, or in commit mode returns them as protocol messages for the
    /// activation's completion, and then writes each stream's close that the workflow made.
    /// Otherwise, if the completion ends the run, a Redis run's close is kept until the eviction
    /// that says the server accepted it; native streams are closed by the server.
    ///
    /// If one of the appends or closes fails, the earlier ones stay stored and the call fails, which
    /// fails the activation; the workflow task's next attempt appends the records again under its
    /// own epoch.
    async fn workflow_activation_completed(
        &self,
        req: WorkflowActivationCompletedRequest,
    ) -> Result<Vec<ProtocolMessage>> {
        let stream = api::StreamRef {
            workflow_id: req.workflow_id,
            first_run_id: req.first_run_id,
            ..Default::default()
        };
        let owner = WorkflowProducer {
            run_id: req.run_id,
            previous_started_event_id: req.previous_started_event_id,
            attempt: req.attempt,
        };
        if req.commit_mode {
            if !matches!(self.backend, Backend::Native(_)) {
                return Err(Error::InvalidArgument(
                    "commit mode needs the native backend".into(),
                ));
            }
            if req.streams.iter().any(|w| w.close.is_some()) {
                return Err(Error::InvalidArgument(
                    "a workflow in commit mode cannot close its own streams".into(),
                ));
            }
        }
        let mut run = self.runs.for_task(stream, owner);
        let mut messages = vec![];
        for writes in req.streams {
            let written = if req.commit_mode && writes.stream_id.is_empty() {
                commit_mode_messages(&mut run, &writes.name, writes.records)
                    .map(|m| messages.extend(m))
            } else {
                self.write_stream(&mut run, writes).await
            };
            if let Err(e) = written {
                self.runs.put(run);
                return Err(e);
            }
        }
        if let (Some(ending), Backend::Redis(_)) = (req.ending, &self.backend) {
            run.pending_close = close_for_ending(ending);
        }
        self.runs.put(run);
        Ok(messages)
    }

    /// Appends one stream's records, as the run's workflow task, and then its close, if any.
    async fn write_stream(&self, run: &mut Run, writes: WorkflowStreamWrites) -> Result<()> {
        let stream = if writes.stream_id.is_empty() {
            run.stream(&writes.name)
        } else {
            if writes.close.is_some() {
                return Err(Error::InvalidArgument(
                    "a workflow cannot close a standalone stream".into(),
                ));
            }
            api::StreamRef {
                stream_id: writes.stream_id.clone(),
                ..Default::default()
            }
        };
        let key = (writes.name.clone(), writes.stream_id.clone());
        for records in limits::split(writes.records)? {
            let seq = run.next_seq.get(&key).copied().unwrap_or(0);
            self.append(api::AppendRequest {
                stream: Some(stream.clone()),
                producer: Some(Producer {
                    variant: Some(Variant::Workflow(run.owner.clone())),
                }),
                seq,
                records,
                finish: false,
            })
            .await?;
            run.next_seq.insert(key.clone(), seq + 1);
        }
        if let Some(close) = writes.close {
            let seq = run.next_seq.get(&key).copied().unwrap_or(0);
            self.close(api::CloseRequest {
                stream: Some(stream),
                owner: Some(run.owner.clone()),
                seq,
                close: Some(close),
                activity_owner: None,
            })
            .await?;
            run.next_seq.insert(key, seq + 1);
        }
        Ok(())
    }

    /// Forgets an evicted run, first writing its pending close if the eviction says the server
    /// accepted the run's terminal completion: on Redis, every stream of the run that is open is
    /// closed, and later appends to the run's streams that do not exist yet fail as closed. The run
    /// is forgotten before the close is written, so a close that fails after the retries is not
    /// written later, and the streams stay open until their retention passes.
    async fn workflow_evicted(&self, req: WorkflowEvictedRequest) -> Result<()> {
        let Some(run) = self.runs.remove(&req.run_id) else {
            return Ok(());
        };
        match (req.execution_ending, run.pending_close, &self.backend) {
            (true, Some(close), Backend::Redis(b)) => b.end_owner(&run.stream, close).await,
            _ => Ok(()),
        }
    }
}
