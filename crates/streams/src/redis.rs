//! A backend that stores streams in Redis, as a worker or client does. The server never talks to
//! Redis.
//!
//! A stream is keyed by its owner and name: a workflow run chain, by workflow ID and first run ID,
//! or a Standalone Activity execution, by activity ID and run ID; or, for a standalone stream, by
//! its stream ID alone. The owner's kind is part of the key, so a workflow, an activity and a
//! standalone stream with the same ID have different streams. A stream is four keys sharing a hash
//! tag with the owner's kind and ID: a Redis stream holding the log, a hash holding each producer's
//! dedupe and fencing state, a metadata hash holding existence, settings and close, and a tombstone.
//! An owner run also has a set of its streams' names and a key set when it ends, and an owner a key
//! holding the run of the owner ID whose first stream was created most recently, for readers that
//! do not know the run ID. Every create, append and close goes through one Lua script, which checks
//! and writes in one step; producers that write to the Redis stream directly are not supported.
//!
//! Every write refreshes the keys' time-to-live, the retention, so a stream that is no longer
//! written to is removed, closed or not; reads do not refresh it. Producer state is removed only
//! with the stream. The tombstone lives twice as long, so for that long a reader can tell a removed
//! stream from one that never existed.
//!
//! Properties weaker than native, all because Redis cannot see the server's state:
//! - A stale producer is fenced only once its successor has appended.
//! - Nothing writes workflow task commit records, so a failed workflow task attempt's records,
//!   and its local activities', are not withdrawn.
//! - Appends from speculative workflow tasks are not rejected: neither the worker nor Core can
//!   tell that a task is speculative.
//! - Producers are not verified, and anything that can write to Redis can write any key, so the
//!   owner-only close guards against mistakes, not against other writers.
//! - The owner's worker writes the close, of every stream of the run. A stream is left open, until
//!   its retention passes, when its workflow is terminated or times out, when a failed run with a
//!   retry policy is not retried (native closes it, because the server knows its retries are
//!   exhausted), or when the worker stops before writing the close. Until then producers can keep
//!   appending.
//! - Nothing closes a Standalone Activity's stream when the activity ends; the activity must close
//!   it itself. The worker cannot learn whether the server accepted the activity's completion,
//!   because Core discards the result of the Respond call, or whether a failed attempt will be
//!   retried, which only the server knows.
//! - A topic filter is applied after reading: the reader receives every record.
//! - A standalone stream's idle timeout is applied by the next create, append or close after it,
//!   and by readers before they report whether the stream is closed, because nothing in Redis runs
//!   when it passes. So a reader waiting at the stream's end learns of the close only when its wait
//!   ends.
//! - Any caller may close a standalone stream, on every backend; the creator's right to close is not
//!   enforced.
//! - Redis can acknowledge a write that it then loses: on a restart, unless the append-only file is
//!   fsynced on every write, or on a failover to a replica that had not received it. The deployment
//!   check does not check persistence.
//!
//! The keys share a hash tag so that one script's keys are in one slot, but the backend connects
//! with a single-node client, so it does not support Redis Cluster.
//!
//! Workflow readers wait on the stream's notifier in the Temporal server, which producers wake with
//! Notify after writing (see [`Coalescer`] for writers; the worker sends a workflow's own). A reader
//! loses a wake-up if a producer stops between a write and its Notify, or if a writer's Notify fails
//! after its retries, until its next write. Without a connection to the server, the backend sends
//! no Notify and workflow reads cannot wait.

use crate::error::{Error, Result};
use crate::identity::{Identity, identity};
use crate::limits;
use crate::native::NativeBackend;
use crate::notify::Coalescer;
use crate::proto::api::{
    AppendRequest, CloseRequest, CreateRequest, NativeOptions, Producer, ReadRequest, RedisOptions,
    StreamRef, WorkflowReadResponse, producer::Variant,
};
use crate::proto::service::{
    self, ProducerIdentity, Record, StreamClose, StreamFailureType, StreamMode,
};
use crate::proto::{Failure, Payload};
use crate::retry::with_retries;
use crate::stream_ref::{self, idle_timeout};
use crate::streams::records_digest;
use prost::Message;
use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client, ErrorKind, RedisError, Script, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::Connection;
use tokio::time::{Instant, sleep};

const DEFAULT_MAX_RECORDS: usize = 1000;
const MIN_VERSION: (u32, u32) = (6, 2);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const NOTIFY_INTERVAL: Duration = Duration::from_millis(100);

pub struct RedisBackend {
    client: Client,
    conn: ConnectionManager,
    namespace: String,
    retention: Duration,
    script: Script,
    /// The Temporal server, for the streams' notifiers.
    server: Option<NativeBackend>,
    writers: Option<Arc<Coalescer>>,
}

impl RedisBackend {
    /// Connects, and checks that the deployment will not evict stream data. Without a connection to
    /// the Temporal server, producers send no Notify and workflow reads cannot wait.
    pub async fn connect(options: RedisOptions, connection: Option<Connection>) -> Result<Self> {
        let retention = options
            .retention
            .and_then(|d| Duration::try_from(d).ok())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| Error::InvalidArgument("retention is required".into()))?;
        let client = Client::open(options.url).map_err(invalid)?;
        let mut conn = with_retries(|| {
            let client = client.clone();
            async move { ConnectionManager::new(client).await.map_err(redis_error) }
        })
        .await?;
        check_deployment(&mut conn).await?;
        let server = connection.map(|c| {
            NativeBackend::new(
                c,
                NativeOptions {
                    namespace: options.namespace.clone(),
                },
            )
        });
        let writers = server.clone().map(|server| {
            Coalescer::new(
                NOTIFY_INTERVAL,
                Arc::new(move |stream| {
                    let server = server.clone();
                    Box::pin(async move { server.notify(&stream, true).await })
                }),
            )
        });
        Ok(Self {
            client,
            conn,
            namespace: options.namespace,
            retention,
            script: Script::new(include_str!("stream.lua")),
            server,
            writers,
        })
    }

    /// Notifies the stream after an activity's or a client's write, coalesced per stream.
    pub fn writer_appended(&self, stream: &Option<StreamRef>) {
        if let (Some(writers), Some(stream)) = (&self.writers, stream) {
            writers.appended(stream);
        }
    }

    /// Notifies the stream after a workflow's own writes, once, without retries, in the background,
    /// so that the Notify adds nothing to the workflow task's latency. A failure is logged; the
    /// run's next write notifies again.
    pub fn workflow_appended(&self, stream: &StreamRef) {
        let Some(server) = self.server.clone() else {
            return;
        };
        let stream = stream.clone();
        tokio::spawn(async move {
            if let Err(e) = server.notify(&stream, false).await {
                tracing::warn!(error = %e, ?stream, "Notify after a workflow's stream writes failed");
            }
        });
    }

    /// Reads; if the read returns no records and no close and `notifier` is set, reads the stream's
    /// notifier state and then reads again, so that a Wait at that position misses no Notify: a
    /// write either is stored before the second read, which returns it, or its Notify moves the
    /// position after it was read. A standalone stream that does not exist, or an owner that does
    /// not, is reported as not found.
    pub async fn workflow_read(
        &self,
        req: ReadRequest,
        notifier: bool,
    ) -> Result<WorkflowReadResponse> {
        let first = self.read(req.clone()).await?;
        if !notifier || !first.records.is_empty() || first.close.is_some() {
            return Ok(read_response(first));
        }
        let mut stream = stream_ref(&req.stream)?.clone();
        if !stream.stream_id.is_empty() && !first.exists {
            return Ok(WorkflowReadResponse {
                owner_not_found: true,
                ..read_response(first)
            });
        }
        let server = self.server.as_ref().ok_or_else(|| {
            Error::InvalidArgument(
                "a workflow read of a Redis stream needs a connection to the Temporal server"
                    .into(),
            )
        })?;
        let cursor = if first.cursor.is_empty() {
            req.after.clone()
        } else {
            first.cursor.clone()
        };
        let mut run = String::new();
        if stream.stream_id.is_empty() {
            let Some(owner_run) = self.owner_run(server, &stream, &cursor).await? else {
                return Ok(WorkflowReadResponse {
                    owner_not_found: true,
                    ..read_response(first)
                });
            };
            if stream.activity_id.is_empty() {
                stream.first_run_id = owner_run.clone();
            } else {
                stream.activity_run_id = owner_run.clone();
            }
            run = owner_run;
        }
        let state = server.notifier_state(&stream).await?;
        let second = self
            .read(ReadRequest {
                stream: Some(stream),
                after: cursor,
                ..req
            })
            .await?;
        if !second.records.is_empty() || second.close.is_some() {
            return Ok(read_response(second));
        }
        Ok(WorkflowReadResponse {
            notifier: Some(state),
            owner_run_id: run,
            ..read_response(second)
        })
    }

    /// The run that names the stream's owner: the reference's, the cursor's, the owner ID's latest
    /// stream's, or the owner's current one, which the server is asked for. None if the owner does
    /// not exist.
    async fn owner_run(
        &self,
        server: &NativeBackend,
        stream: &StreamRef,
        cursor: &[u8],
    ) -> Result<Option<String>> {
        if !owner_run(stream).is_empty() {
            return Ok(Some(owner_run(stream).to_string()));
        }
        if !cursor.is_empty() {
            return Ok(Some(self.parse_cursor(stream, cursor)?.0));
        }
        let mut conn = self.conn.clone();
        let current: Option<String> = conn
            .get(self.current_key(&owner_key(stream)))
            .await
            .map_err(redis_error)?;
        match current {
            Some(run) => Ok(Some(run)),
            None => server.owner_run(stream).await,
        }
    }

    pub async fn append(&self, req: AppendRequest) -> Result<Vec<Vec<u8>>> {
        limits::check(&req.records)?;
        let id = identity(req.producer.as_ref().unwrap_or(&Default::default()))?;
        let stream = stream_ref(&req.stream)?;
        let mut entries: Vec<Vec<(&str, Vec<u8>)>> = req
            .records
            .iter()
            .map(|r| {
                vec![
                    ("kind", b"data".to_vec()),
                    ("topic", r.topic.clone().into_bytes()),
                    (
                        "payload",
                        r.payload.clone().unwrap_or_default().encode_to_vec(),
                    ),
                ]
            })
            .collect();
        if req.finish {
            entries.push(vec![("kind", b"finish".to_vec())]);
        }
        let keys = self.write_keys(stream)?;
        let ids = self
            .run_append(&keys, &id, req.seq, &entries, false)
            .await?;
        Ok(ids
            .into_iter()
            .take(req.records.len())
            .map(|entry_id| keys.cursor(&entry_id))
            .collect())
    }

    /// Creates the stream, or answers a create of the stream if it exists. Returns whether this
    /// create, by its request ID, created it.
    pub async fn create(&self, req: CreateRequest) -> Result<bool> {
        let stream = stream_ref(&req.stream)?;
        let idle = idle_timeout(stream, req.settings.as_ref())?;
        let keys = self.write_keys(stream)?;
        let mut invocation = self.script.prepare_invoke();
        self.stream_invocation(&mut invocation, &keys, "create", idle);
        invocation.arg(&req.request_id);
        let created: i64 = self.invoke(&invocation).await?;
        Ok(created == 1)
    }

    pub async fn close(&self, req: CloseRequest) -> Result<()> {
        let stream = stream_ref(&req.stream)?;
        let close = req.close.clone().unwrap_or_default();
        let failure = close.failure.map(|f| f.encode_to_vec()).unwrap_or_default();
        if !stream.stream_id.is_empty() {
            let keys = self.write_keys(stream)?;
            let mut invocation = self.script.prepare_invoke();
            invocation
                .key(&keys.meta)
                .key(&keys.tombstone)
                .arg("close")
                .arg(self.retention_ms())
                .arg(close.state)
                .arg(failure)
                .arg(&close.reason);
            let _: Value = self.invoke(&invocation).await?;
            return Ok(());
        }
        let owner = match (req.owner, req.activity_owner) {
            (Some(w), None) if stream.activity_id.is_empty() => Variant::Workflow(w),
            (None, Some(a))
                if a.workflow_run_id.is_empty()
                    && a.activity_id == stream.activity_id
                    && a.activity_run_id == stream.activity_run_id =>
            {
                Variant::Activity(a)
            }
            _ => {
                return Err(Error::stream(
                    StreamFailureType::NotOwner,
                    "only the stream's owner may close it",
                ));
            }
        };
        let id = identity(&Producer {
            variant: Some(owner),
        })?;
        let entry = vec![
            ("kind", b"close".to_vec()),
            ("state", close.state.to_string().into_bytes()),
            ("failure", failure),
            ("reason", close.reason.into_bytes()),
        ];
        let keys = self.write_keys(stream)?;
        self.run_append(&keys, &id, req.seq, &[entry], true).await?;
        Ok(())
    }

    /// Closes every stream of a workflow run that is not closed, and makes later creates and appends
    /// of the run's streams that do not exist yet fail as closed. Called once the server has
    /// accepted the run's terminal completion.
    pub async fn end_owner(&self, stream: &StreamRef, close: StreamClose) -> Result<()> {
        let keys = self.write_keys(stream)?;
        let mut invocation = self.script.prepare_invoke();
        invocation
            .key(&keys.ended)
            .key(&keys.streams)
            .arg("end_owner")
            .arg(&keys.run_prefix)
            .arg(self.retention_ms())
            .arg(close.state)
            .arg(close.failure.map(|f| f.encode_to_vec()).unwrap_or_default())
            .arg(&close.reason);
        let _: i64 = self.invoke(&invocation).await?;
        Ok(())
    }

    pub async fn read(&self, req: ReadRequest) -> Result<service::ReadResponse> {
        let stream = stream_ref(&req.stream)?;
        let wait = req
            .wait
            .and_then(|d| Duration::try_from(d).ok())
            .unwrap_or_default();
        let deadline = Instant::now() + wait;
        let mut conn = self.conn.clone();
        let (keys, after) = if req.after.is_empty() {
            match self.resolve_chain(&mut conn, stream, deadline).await? {
                Some(keys) => (keys, "0-0".to_string()),
                None => return Ok(self.response()),
            }
        } else {
            let (first_run, entry_id) = self.parse_cursor(stream, &req.after)?;
            let run = owner_run(stream);
            if !run.is_empty() && run != first_run {
                return Err(Error::stream(
                    StreamFailureType::CursorMismatch,
                    "the cursor belongs to another run chain",
                ));
            }
            (self.keys(stream, &first_run), entry_id)
        };
        let until = if req.until.is_empty() {
            None
        } else {
            let (run, entry_id) = self.parse_cursor(stream, &req.until)?;
            if run != keys.run {
                return Err(Error::stream(
                    StreamFailureType::CursorMismatch,
                    "the cursors belong to different run chains",
                ));
            }
            Some(entry_id)
        };
        if !exists(&mut conn, &keys.meta).await? {
            if exists(&mut conn, &keys.tombstone).await? {
                return Err(Error::stream(
                    StreamFailureType::Expired,
                    "the stream has been removed",
                ));
            }
            if !req.after.is_empty() {
                return Err(Error::stream(
                    StreamFailureType::NotFound,
                    "the stream does not exist",
                ));
            }
            return self
                .wait_for_chain_records(conn, &keys, &req, deadline)
                .await;
        }
        let mut resp = if req.last > 0 {
            self.read_last(&mut conn, &keys, &req).await?
        } else {
            self.read_after(conn, &keys, &req, after, until, deadline)
                .await?
        };
        resp.exists = true;
        Ok(resp)
    }

    /// An empty response: a Redis stream is always sink-mode, and its data is kept for the
    /// retention after its last write.
    fn response(&self) -> service::ReadResponse {
        service::ReadResponse {
            mode: StreamMode::Sink.into(),
            retention: prost_types::Duration::try_from(self.retention).ok(),
            ..Default::default()
        }
    }

    /// Resolves the chain a read from the beginning reads: the pinned one, or the run of the owner
    /// ID that most recently created a stream, waiting until the deadline for one to exist. A newer
    /// run that has not appended yet is not seen.
    async fn resolve_chain(
        &self,
        conn: &mut ConnectionManager,
        stream: &StreamRef,
        deadline: Instant,
    ) -> Result<Option<StreamKeys>> {
        if !owner_run(stream).is_empty() || !stream.stream_id.is_empty() {
            return Ok(Some(self.keys(stream, owner_run(stream))));
        }
        let current = self.current_key(&owner_key(stream));
        loop {
            let first_run: Option<String> = conn.get(&current).await.map_err(redis_error)?;
            if let Some(first_run) = first_run {
                return Ok(Some(self.keys(stream, &first_run)));
            }
            if Instant::now() + CHAIN_POLL_INTERVAL > deadline {
                return Ok(None);
            }
            sleep(CHAIN_POLL_INTERVAL).await;
        }
    }

    /// A read from the beginning of a chain with no records yet waits for its first.
    async fn wait_for_chain_records(
        &self,
        mut conn: ConnectionManager,
        keys: &StreamKeys,
        req: &ReadRequest,
        deadline: Instant,
    ) -> Result<service::ReadResponse> {
        loop {
            if exists(&mut conn, &keys.meta).await? {
                let mut resp = self
                    .read_after(conn, keys, req, "0-0".into(), None, deadline)
                    .await?;
                resp.exists = true;
                return Ok(resp);
            }
            if Instant::now() + CHAIN_POLL_INTERVAL > deadline {
                return Ok(self.response());
            }
            sleep(CHAIN_POLL_INTERVAL).await;
        }
    }

    /// Reads what is there after `last_id`, up to `until` if set; if there is nothing, and no
    /// `until`, blocks for the rest of the wait. Entries that the topic filter drops do not end the
    /// wait. The read stops before the record that would take its payloads past `max_bytes`.
    async fn read_after(
        &self,
        mut conn: ConnectionManager,
        keys: &StreamKeys,
        req: &ReadRequest,
        mut last_id: String,
        until: Option<String>,
        deadline: Instant,
    ) -> Result<service::ReadResponse> {
        let max_records = usize::try_from(req.max_records)
            .ok()
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_MAX_RECORDS);
        let end = until.clone().unwrap_or_else(|| "+".into());
        let mut resp = service::ReadResponse {
            cursor: req.after.clone(),
            ..self.response()
        };
        let mut bytes = 0;
        'read: loop {
            let reply: StreamRangeReply = conn
                .xrange_count(&keys.log, format!("({last_id}"), &end, max_records)
                .await
                .map_err(redis_error)?;
            let mut entries = reply.ids;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if entries.is_empty() && !remaining.is_zero() && until.is_none() {
                entries = self.block(&keys.log, &last_id, remaining).await?;
            }
            for entry in entries {
                let r = record(keys.cursor(&entry.id), &entry.map, &req.topics)?;
                if req.max_bytes > 0 {
                    let n = r
                        .as_ref()
                        .and_then(|r| r.payload.as_ref())
                        .map_or(0, |p| p.encoded_len() as i64);
                    if bytes + n > req.max_bytes {
                        if bytes == 0 {
                            return Err(Error::stream(
                                StreamFailureType::TooLarge,
                                format!(
                                    "the next record exceeds the read's limit of {} bytes",
                                    req.max_bytes
                                ),
                            ));
                        }
                        break 'read;
                    }
                    bytes += n;
                }
                last_id = entry.id.clone();
                resp.cursor = keys.cursor(&entry.id);
                resp.records.extend(r);
            }
            if !resp.records.is_empty()
                || Instant::now() >= deadline
                || until.is_some()
                || self.closed_at(&mut conn, keys, &last_id).await?
            {
                break;
            }
        }
        if self.closed_at(&mut conn, keys, &last_id).await? {
            resp.close = Some(close_from_meta(&mut conn, keys).await?);
        }
        Ok(resp)
    }

    /// Blocks for entries after `last_id`, on a connection of its own, since a blocking command
    /// would hold up every other command on a shared one. Each call opens a new connection.
    async fn block(
        &self,
        log: &str,
        last_id: &str,
        wait: Duration,
    ) -> Result<Vec<redis::streams::StreamId>> {
        let mut conn: MultiplexedConnection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        let block_ms = usize::try_from(wait.as_millis())
            .unwrap_or(usize::MAX)
            .max(1);
        let reply: Option<StreamReadReply> = conn
            .xread_options(
                &[log],
                &[last_id],
                &StreamReadOptions::default().block(block_ms),
            )
            .await
            .map_err(redis_error)?;
        Ok(reply
            .and_then(|r| r.keys.into_iter().next())
            .map(|k| k.ids)
            .unwrap_or_default())
    }

    /// Reads the last `req.last` matching records, scanning the log backwards, and the close if the
    /// stream is closed. It fetches the whole log in one reply, however few records it returns.
    async fn read_last(
        &self,
        conn: &mut ConnectionManager,
        keys: &StreamKeys,
        req: &ReadRequest,
    ) -> Result<service::ReadResponse> {
        let reply: StreamRangeReply = conn.xrevrange_all(&keys.log).await.map_err(redis_error)?;
        let mut resp = self.response();
        if let Some(newest) = reply.ids.first() {
            resp.cursor = keys.cursor(&newest.id);
        }
        let last = usize::try_from(req.last).unwrap_or_default();
        for entry in &reply.ids {
            if resp.records.len() == last {
                break;
            }
            if let Some(r) = record(keys.cursor(&entry.id), &entry.map, &req.topics)? {
                resp.records.insert(0, r);
            }
        }
        if self.closed(conn, keys).await? {
            resp.close = Some(close_from_meta(conn, keys).await?);
        }
        Ok(resp)
    }

    /// Whether the stream is closed and the reader has read every entry up to `last_id`, which
    /// includes the close entry, the stream's last.
    async fn closed_at(
        &self,
        conn: &mut ConnectionManager,
        keys: &StreamKeys,
        last_id: &str,
    ) -> Result<bool> {
        if !self.closed(conn, keys).await? {
            return Ok(false);
        }
        let after: StreamRangeReply = conn
            .xrange_count(&keys.log, format!("({last_id}"), "+", 1)
            .await
            .map_err(redis_error)?;
        Ok(after.ids.is_empty())
    }

    /// Runs the append script. A retry after the script ran but its reply was lost takes the dedupe
    /// path and returns the stored entry IDs.
    async fn run_append(
        &self,
        keys: &StreamKeys,
        id: &Identity,
        seq: i64,
        entries: &[Vec<(&str, Vec<u8>)>],
        closing: bool,
    ) -> Result<Vec<String>> {
        let idle = if keys.standalone {
            stream_ref::DEFAULT_IDLE_TIMEOUT
        } else {
            Duration::ZERO
        };
        let mut invocation = self.script.prepare_invoke();
        self.stream_invocation(&mut invocation, keys, "append", idle);
        invocation
            .arg(&id.producer)
            .arg(&id.run_id)
            .arg(id.workflow_task)
            .arg(id.workflow_task_attempt)
            .arg(id.epoch)
            .arg(seq)
            .arg(digest(entries))
            .arg(if closing { "1" } else { "0" })
            .arg(entries.len());
        for entry in entries {
            invocation.arg(entry.len());
            for (field, value) in entry {
                invocation.arg(*field).arg(value.as_slice());
            }
        }
        self.invoke(&invocation).await
    }

    /// Adds the keys and leading arguments of a create or append.
    fn stream_invocation(
        &self,
        invocation: &mut redis::ScriptInvocation<'_>,
        keys: &StreamKeys,
        op: &str,
        idle: Duration,
    ) {
        invocation
            .key(&keys.meta)
            .key(&keys.log)
            .key(&keys.producers)
            .key(&keys.tombstone)
            .key(&keys.current)
            .key(&keys.streams)
            .key(&keys.ended)
            .arg(op)
            .arg(&keys.run)
            .arg(&keys.name)
            .arg(if keys.standalone { "1" } else { "0" })
            .arg(self.retention_ms())
            .arg(u64::try_from(idle.as_millis()).unwrap_or(u64::MAX));
    }

    /// Runs a script invocation, retrying transient failures.
    async fn invoke<T: redis::FromRedisValue>(
        &self,
        invocation: &redis::ScriptInvocation<'_>,
    ) -> Result<T> {
        with_retries(|| {
            let mut conn = self.conn.clone();
            async move {
                invocation
                    .invoke_async(&mut conn)
                    .await
                    .map_err(redis_error)
            }
        })
        .await
    }

    /// Whether the stream is closed, closing a standalone stream that has reached its idle timeout
    /// first.
    async fn closed(&self, conn: &mut ConnectionManager, keys: &StreamKeys) -> Result<bool> {
        if keys.standalone {
            let mut invocation = self.script.prepare_invoke();
            invocation.key(&keys.meta).arg("close_if_idle");
            let closed: i64 = self.invoke(&invocation).await?;
            return Ok(closed == 1);
        }
        let closed: Option<String> = conn.hget(&keys.meta, "closed").await.map_err(redis_error)?;
        Ok(closed.as_deref() == Some("1"))
    }

    fn retention_ms(&self) -> u64 {
        u64::try_from(self.retention.as_millis()).unwrap_or(u64::MAX)
    }

    /// The keys a create, append or close writes, which for an owner's stream needs the owner's run
    /// ID.
    fn write_keys(&self, stream: &StreamRef) -> Result<StreamKeys> {
        if stream.stream_id.is_empty() {
            required_run(stream)?;
        }
        Ok(self.keys(stream, owner_run(stream)))
    }

    fn keys(&self, stream: &StreamRef, run: &str) -> StreamKeys {
        let owner = owner_key(stream);
        let run_prefix = format!("{}:{run}:", self.owner_prefix(&owner));
        let prefix = format!("{run_prefix}{}", stream.name);
        StreamKeys {
            meta: format!("{prefix}:meta"),
            log: format!("{prefix}:log"),
            producers: format!("{prefix}:producers"),
            tombstone: format!("{prefix}:tombstone"),
            current: self.current_key(&owner),
            streams: format!("{run_prefix}streams"),
            ended: format!("{run_prefix}ended"),
            run: run.to_string(),
            name: stream.name.clone(),
            standalone: !stream.stream_id.is_empty(),
            run_prefix,
            prefix,
        }
    }

    fn owner_prefix(&self, owner: &str) -> String {
        format!("stream:{{{}/{owner}}}", self.namespace)
    }

    fn current_key(&self, owner: &str) -> String {
        format!("{}:current", self.owner_prefix(owner))
    }

    /// Splits a cursor, `<owner prefix>:<run ID>:<name>#<entry ID>`, into its run and entry. The
    /// prefix and name bind the cursor to the namespace, owner and stream. Run IDs contain no ':'
    /// and entry IDs no '#'; names may contain either.
    fn parse_cursor(&self, stream: &StreamRef, cursor: &[u8]) -> Result<(String, String)> {
        let mismatch = || {
            Error::stream(
                StreamFailureType::CursorMismatch,
                "the cursor belongs to another stream",
            )
        };
        let cursor = std::str::from_utf8(cursor).map_err(|_| mismatch())?;
        let (chain, entry_id) = cursor.rsplit_once('#').ok_or_else(mismatch)?;
        let (first_run, name) = chain
            .strip_prefix(&format!("{}:", self.owner_prefix(&owner_key(stream))))
            .and_then(|rest| rest.split_once(':'))
            .ok_or_else(mismatch)?;
        if name != stream.name {
            return Err(mismatch());
        }
        Ok((first_run.to_string(), entry_id.to_string()))
    }
}

/// The keys of one stream, of its owner run (streams, ended), and the key holding the owner ID's
/// latest run. run_prefix is the prefix of the owner run's stream keys.
struct StreamKeys {
    prefix: String,
    meta: String,
    log: String,
    producers: String,
    tombstone: String,
    current: String,
    streams: String,
    ended: String,
    run_prefix: String,
    run: String,
    name: String,
    standalone: bool,
}

impl StreamKeys {
    fn cursor(&self, entry_id: &str) -> Vec<u8> {
        format!("{}#{entry_id}", self.prefix).into_bytes()
    }
}

/// Refuses a deployment that is too old for the commands used, or whose eviction policy could
/// drop stream data. It checks once, at connect, so a later change of the policy is not detected.
async fn check_deployment(conn: &mut ConnectionManager) -> Result<()> {
    let server: String = redis::cmd("INFO")
        .arg("server")
        .query_async(conn)
        .await
        .map_err(redis_error)?;
    let version = info_field(&server, "redis_version").unwrap_or_default();
    let mut parts = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor) = (parts.next().unwrap_or(0), parts.next().unwrap_or(0));
    if (major, minor) < MIN_VERSION {
        return Err(Error::InvalidArgument(format!(
            "Redis {version} is older than {}.{}, the oldest that supports exclusive stream ranges",
            MIN_VERSION.0, MIN_VERSION.1
        )));
    }
    let memory: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(conn)
        .await
        .map_err(redis_error)?;
    let policy = info_field(&memory, "maxmemory_policy").unwrap_or_default();
    if policy != "noeviction" {
        return Err(Error::InvalidArgument(format!(
            "Redis maxmemory-policy is {policy}; streams need noeviction, so that Redis never drops stream data"
        )));
    }
    Ok(())
}

fn info_field<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    info.lines()
        .find_map(|line| line.trim().strip_prefix(field)?.strip_prefix(':'))
}

/// The record of a log entry, or none for the close entry, which readers learn of from the
/// response's close, or for an entry the topic filter drops. Finish entries have no topic, so a
/// topic filter drops them, as on native.
fn record(
    cursor: Vec<u8>,
    fields: &HashMap<String, Value>,
    topics: &[String],
) -> Result<Option<Record>> {
    let kind = string_field(fields, "kind")?;
    let topic = string_field(fields, "topic").unwrap_or_default();
    if kind == "close" || (!topics.is_empty() && !topics.contains(&topic)) {
        return Ok(None);
    }
    let payload = if kind == "data" {
        Some(Payload::decode(bytes_field(fields, "payload")?.as_slice())?)
    } else {
        None
    };
    Ok(Some(Record {
        cursor,
        producer: Some(ProducerIdentity {
            producer: string_field(fields, "producer")?,
            workflow_run_id: string_field(fields, "run")?,
            workflow_task: int_field(fields, "wt")?,
            workflow_task_attempt: i32::try_from(int_field(fields, "wta")?).map_err(internal)?,
            epoch: int_field(fields, "epoch")?,
            seq: int_field(fields, "seq")?,
        }),
        topic,
        payload,
        finish: kind == "finish",
        ..Default::default()
    }))
}

async fn close_from_meta(conn: &mut ConnectionManager, keys: &StreamKeys) -> Result<StreamClose> {
    let meta: HashMap<String, Vec<u8>> = conn.hgetall(&keys.meta).await.map_err(redis_error)?;
    let field = |name: &str| meta.get(name).cloned().unwrap_or_default();
    let failure = field("close_failure");
    Ok(StreamClose {
        state: String::from_utf8_lossy(&field("close_state"))
            .parse()
            .map_err(internal)?,
        failure: if failure.is_empty() {
            None
        } else {
            Some(Failure::decode(failure.as_slice())?)
        },
        reason: String::from_utf8_lossy(&field("close_reason")).into_owned(),
    })
}

async fn exists(conn: &mut ConnectionManager, key: &str) -> Result<bool> {
    conn.exists(key).await.map_err(redis_error)
}

fn bytes_field(fields: &HashMap<String, Value>, name: &str) -> Result<Vec<u8>> {
    match fields.get(name) {
        Some(value) => redis::from_redis_value(value).map_err(internal),
        None => Err(Error::Internal(format!("stream entry has no {name}"))),
    }
}

fn string_field(fields: &HashMap<String, Value>, name: &str) -> Result<String> {
    String::from_utf8(bytes_field(fields, name)?).map_err(internal)
}

fn int_field(fields: &HashMap<String, Value>, name: &str) -> Result<i64> {
    string_field(fields, name)?.parse().map_err(internal)
}

/// Identifies an append's content, so that a retry with different content is detected.
fn digest(entries: &[Vec<(&str, Vec<u8>)>]) -> String {
    let mut hasher = Sha256::new();
    for entry in entries {
        for (field, value) in entry {
            for part in [field.as_bytes(), value.as_slice()] {
                hasher.update((part.len() as u64).to_be_bytes());
                hasher.update(part);
            }
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn read_response(read: service::ReadResponse) -> WorkflowReadResponse {
    WorkflowReadResponse {
        digest: records_digest(&read.records),
        read: Some(read),
        ..Default::default()
    }
}

fn stream_ref(stream: &Option<StreamRef>) -> Result<&StreamRef> {
    let stream = stream
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("stream is required".into()))?;
    stream_ref::validate(stream)?;
    Ok(stream)
}

/// Names a stream's owner, so that a workflow, an activity and a standalone stream with the same ID
/// have different streams.
fn owner_key(stream: &StreamRef) -> String {
    if !stream.activity_id.is_empty() {
        format!("activity/{}", stream.activity_id)
    } else if !stream.stream_id.is_empty() {
        format!("stream/{}", stream.stream_id)
    } else {
        format!("workflow/{}", stream.workflow_id)
    }
}

/// The run ID that pins a stream: a workflow's first run ID, or the activity's run ID. A standalone
/// stream has none.
fn owner_run(stream: &StreamRef) -> &str {
    if stream.activity_id.is_empty() {
        &stream.first_run_id
    } else {
        &stream.activity_run_id
    }
}

fn required_run(stream: &StreamRef) -> Result<&str> {
    let run = owner_run(stream);
    if run.is_empty() {
        return Err(Error::InvalidArgument(
            "appends to a Redis stream need the run ID of its owner: a workflow's first run ID, or \
             the activity's run ID"
                .into(),
        ));
    }
    Ok(run)
}

/// Classifies a Redis error: a failure the script reports, as an error whose code is a
/// StreamFailureType name without its prefix, or INVALID_ARGUMENT; a transient failure; or another.
fn redis_error(e: RedisError) -> Error {
    if e.code() == Some("INVALID_ARGUMENT") {
        return Error::InvalidArgument(e.detail().unwrap_or_default().to_string());
    }
    if let Some(code) = e.code()
        && let Some(failure_type) =
            StreamFailureType::from_str_name(&format!("STREAM_FAILURE_TYPE_{code}"))
    {
        return Error::stream(failure_type, e.detail().unwrap_or_default());
    }
    let transient = e.is_io_error()
        || e.is_connection_dropped()
        || e.is_connection_refusal()
        || e.is_timeout()
        || matches!(
            e.kind(),
            ErrorKind::BusyLoadingError
                | ErrorKind::TryAgain
                | ErrorKind::ClusterDown
                | ErrorKind::MasterDown
        );
    if transient {
        Error::Transient(e.to_string())
    } else {
        Error::Internal(e.to_string())
    }
}

fn invalid(e: impl std::fmt::Display) -> Error {
    Error::InvalidArgument(e.to_string())
}

fn internal(e: impl std::fmt::Display) -> Error {
    Error::Internal(e.to_string())
}
