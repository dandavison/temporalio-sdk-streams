use crate::error::{Error, Result};
use crate::limits;
use crate::native::NativeBackend;
use crate::proto::api::{
    self, AppendResponse, CloseResponse, Producer, WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse, WorkflowEvictedRequest, WorkflowEvictedResponse,
    WorkflowProducer, WorkflowReadRequest, WorkflowReadResponse, producer::Variant,
};
use crate::proto::service::{self, Record};
use crate::redis::RedisBackend;
use crate::workflow::{Runs, close_for_ending, commit_mode_messages};
use prost::Message;
use sha2::{Digest, Sha256};
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

    /// A Redis backend. `connection` is the Temporal server's, for the owners' notifiers; without
    /// it producers send no Notify and workflow reads cannot wait.
    pub async fn redis(options: api::RedisOptions, connection: Option<Connection>) -> Result<Self> {
        Ok(Self {
            backend: Backend::Redis(RedisBackend::connect(options, connection).await?),
            runs: Runs::default(),
        })
    }

    /// Runs the named call on its encoded request and returns its encoded response. The calls and
    /// their messages are in `protos/temporal/sdk/streams/v1/streams.proto`.
    pub async fn call(&self, name: &str, request: &[u8]) -> Result<Vec<u8>> {
        Ok(match name {
            "Append" => {
                let req = api::AppendRequest::decode(request)?;
                let stream = req.stream.clone();
                let cursors = self.append(req).await?;
                self.writer_appended(&stream);
                AppendResponse { cursors }.encode_to_vec()
            }
            "Close" => {
                let req = api::CloseRequest::decode(request)?;
                let stream = req.stream.clone();
                self.close(req).await?;
                self.writer_appended(&stream);
                CloseResponse {}.encode_to_vec()
            }
            "Read" => self
                .read(api::ReadRequest::decode(request)?)
                .await?
                .encode_to_vec(),
            "WorkflowRead" => self
                .workflow_read(WorkflowReadRequest::decode(request)?)
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

    /// A workflow's read, which does not wait, with the digest of its records.
    async fn workflow_read(&self, req: WorkflowReadRequest) -> Result<WorkflowReadResponse> {
        let read = api::ReadRequest {
            wait: None,
            ..req.read.unwrap_or_default()
        };
        match &self.backend {
            Backend::Native(b) => {
                let read = b.read(read).await?;
                Ok(WorkflowReadResponse {
                    digest: records_digest(&read.records),
                    read: Some(read),
                    ..Default::default()
                })
            }
            Backend::Redis(b) => b.workflow_read(read, req.notifier).await,
        }
    }

    /// Notifies the owner of a Redis stream after an activity's or a client's write.
    fn writer_appended(&self, stream: &Option<api::StreamRef>) {
        if let Backend::Redis(b) = &self.backend {
            b.writer_appended(stream);
        }
    }

    /// Notifies the owner of a Redis stream after a workflow's own writes.
    fn workflow_appended(&self, stream: &api::StreamRef) {
        if let Backend::Redis(b) = &self.backend {
            b.workflow_appended(stream);
        }
    }

    /// Appends an activation's records, in appends that fit the size limits, or in commit mode
    /// returns them as protocol messages for the activation's completion, and then writes the
    /// workflow's own close if it closed its stream. Otherwise, if the completion ends the run, a
    /// Redis stream's close is kept until the eviction that says the server accepted it; a native
    /// stream is closed by the server.
    ///
    /// If one of the appends or the close fails, the earlier appends stay stored and the call
    /// fails, which fails the activation; the workflow task's next attempt appends the records
    /// again under its own epoch.
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
            if req.close.is_some() {
                return Err(Error::InvalidArgument(
                    "a workflow in commit mode cannot close its own stream".into(),
                ));
            }
        }
        let mut run = self.runs.for_task(stream, owner);
        if req.commit_mode {
            let messages = commit_mode_messages(&mut run, req.records, req.declare_mode);
            self.runs.put(run);
            return messages;
        }
        let mut wrote = false;
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
            wrote = true;
        }
        if let Some(close) = req.close {
            let closed = self
                .close(api::CloseRequest {
                    stream: Some(run.stream.clone()),
                    owner: Some(run.owner.clone()),
                    seq: run.next_seq,
                    close: Some(close),
                    activity_owner: None,
                })
                .await;
            if let Err(e) = closed {
                self.runs.put(run);
                return Err(e);
            }
            run.next_seq += 1;
            run.closed = true;
            wrote = true;
        }
        if wrote {
            self.workflow_appended(&run.stream);
        }
        if let (Some(ending), Backend::Redis(_), false) = (req.ending, &self.backend, run.closed) {
            run.pending_close = close_for_ending(ending);
        }
        self.runs.put(run);
        Ok(vec![])
    }

    /// Forgets an evicted run, first writing its pending close if the eviction says the server
    /// accepted the run's terminal completion. The run is forgotten before the close is written, so
    /// a close that fails after the retries is not written later, and the stream stays open until
    /// its retention passes.
    async fn workflow_evicted(&self, req: WorkflowEvictedRequest) -> Result<()> {
        let Some(run) = self.runs.remove(&req.run_id) else {
            return Ok(());
        };
        let (true, Some(close)) = (req.execution_ending, run.pending_close) else {
            return Ok(());
        };
        self.close(api::CloseRequest {
            stream: Some(run.stream.clone()),
            owner: Some(run.owner),
            seq: run.next_seq,
            close: Some(close),
            activity_owner: None,
        })
        .await?;
        self.workflow_appended(&run.stream);
        Ok(())
    }
}

/// SHA-256 over records as a read returned them: each record's cursor, producer identity, topic,
/// finish, markers, and payload (metadata in key order, then data). Payloads are as stored, after
/// the producer's codec.
pub fn records_digest(records: &[Record]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    let mut part = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    for r in records {
        part(&r.cursor);
        let p = r.producer.clone().unwrap_or_default();
        part(p.producer.as_bytes());
        part(p.workflow_run_id.as_bytes());
        for n in [
            p.workflow_task,
            p.workflow_task_attempt.into(),
            p.epoch,
            p.seq,
        ] {
            part(&n.to_be_bytes());
        }
        part(r.topic.as_bytes());
        part(&[r.finish.into()]);
        // The markers have no map fields, so their encoding is deterministic.
        for marker in [
            r.workflow_task_commit.as_ref().map(Message::encode_to_vec),
            r.stream_created.as_ref().map(Message::encode_to_vec),
            r.workflow_append_mode.as_ref().map(Message::encode_to_vec),
        ] {
            match marker {
                None => part(&[0]),
                Some(bytes) => {
                    part(&[1]);
                    part(&bytes);
                }
            }
        }
        match &r.payload {
            None => part(&[0]),
            Some(payload) => {
                part(&[1]);
                let mut metadata: Vec<_> = payload.metadata.iter().collect();
                metadata.sort();
                for (key, value) in metadata {
                    part(key.as_bytes());
                    part(value);
                }
                part(&payload.data);
            }
        }
    }
    hasher.finalize().to_vec()
}
