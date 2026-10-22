use crate::error::{Error, Result};
use crate::limits;
use crate::native::NativeBackend;
use crate::proto::api::{
    self, AppendResponse, CloseResponse, Producer, WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse, WorkflowEvictedRequest, WorkflowEvictedResponse,
    WorkflowProducer, WorkflowReadRequest, WorkflowReadResponse, WorkflowStreamWrites,
    producer::Variant,
};
use crate::proto::service::{self, Record};
use crate::redis::RedisBackend;
use crate::workflow::{Run, Runs, close_for_ending, commit_mode_messages, declaration};
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

    /// A Redis backend. `connection` is the Temporal server's, for the streams' notifiers; without
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
            "Create" => {
                let req = api::CreateRequest::decode(request)?;
                let stream = req.stream.clone();
                let created = self.create(req).await?;
                self.writer_appended(&stream);
                service::CreateResponse { created }.encode_to_vec()
            }
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

    /// Notifies a Redis stream after an activity's or a client's write.
    fn writer_appended(&self, stream: &Option<api::StreamRef>) {
        if let Backend::Redis(b) = &self.backend {
            b.writer_appended(stream);
        }
    }

    /// Notifies a Redis stream after a workflow's own writes.
    fn workflow_appended(&self, stream: &api::StreamRef) {
        if let Backend::Redis(b) = &self.backend {
            b.workflow_appended(stream);
        }
    }

    /// Appends an activation's records to each of the workflow's streams that it wrote to, in
    /// appends that fit the size limits, or in commit mode returns them as protocol messages for the
    /// activation's completion, after the run's mode declaration if the activation declares it, and
    /// then writes each stream's close that the workflow made, and on Redis notifies each stream it
    /// wrote. Otherwise, if the completion ends the run, a Redis run's close is kept until the
    /// eviction that says the server accepted it; native streams are closed by the server.
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
        let mut messages = declaration(&mut run, req.commit_mode && req.declare_mode);
        for writes in req.streams {
            let written = if req.commit_mode && writes.stream_id.is_empty() {
                commit_mode_messages(&mut run, &writes.name, writes.records, false)
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

    /// Appends one stream's records, as the run's workflow task, and then its close, if any, and
    /// notifies the stream if it wrote either.
    async fn write_stream(&self, run: &mut Run, writes: WorkflowStreamWrites) -> Result<()> {
        let stream = if writes.stream_id.is_empty() {
            run.written.insert(writes.name.clone());
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
        let wrote = !writes.records.is_empty() || writes.close.is_some();
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
                stream: Some(stream.clone()),
                owner: Some(run.owner.clone()),
                seq,
                close: Some(close),
                activity_owner: None,
            })
            .await?;
            run.next_seq.insert(key, seq + 1);
        }
        if wrote {
            self.workflow_appended(&stream);
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
        match (
            req.execution_ending,
            run.pending_close.clone(),
            &self.backend,
        ) {
            (true, Some(close), Backend::Redis(b)) => {
                b.end_owner(&run.stream, close).await?;
                for name in &run.written {
                    b.workflow_appended(&run.stream(name));
                }
                Ok(())
            }
            _ => Ok(()),
        }
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
