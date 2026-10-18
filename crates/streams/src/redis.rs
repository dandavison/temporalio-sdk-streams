//! A backend that stores streams in Redis, as a worker or client does. The server never talks to
//! Redis.
//!
//! A run chain's stream is four keys sharing a hash tag with its workflow ID: a Redis stream
//! holding the log, a hash holding each producer's dedupe and fencing state, a metadata hash
//! holding existence and close, and a tombstone. A fifth key names the workflow ID's latest chain,
//! for readers that do not know the first run ID. Every append and close goes through one Lua
//! script, which checks and writes in one step.
//!
//! Every write refreshes the keys' time-to-live, the retention, so a stream that is no longer
//! written to is removed, closed or not. The tombstone lives twice as long, so for that long a
//! reader can tell a removed stream from one that never existed.
//!
//! Properties weaker than native, all because Redis cannot see the server's state:
//! - A stale producer is fenced only once its successor has appended.
//! - Nothing writes workflow task commit records, so a failed workflow task attempt's records,
//!   and its local activities', are not withdrawn.
//! - Producers are not verified, and anything that can write to Redis can write any key, so the
//!   owner-only close guards against mistakes, not against other writers.
//! - The owner's worker writes the close. A stream is left open, until its retention passes, when
//!   its workflow is terminated or times out, when a failed run with a retry policy is not
//!   retried, or when the worker stops before writing the close. Until then producers can keep
//!   appending.
//! - A topic filter is applied after reading: the reader receives every record.

use crate::error::{Error, Result};
use crate::identity::{Identity, identity};
use crate::limits;
use crate::proto::api::{AppendRequest, CloseRequest, ReadRequest, RedisOptions, StreamRef};
use crate::proto::service::{self, ProducerIdentity, Record, StreamClose, StreamFailureType};
use crate::proto::{Failure, Payload};
use crate::retry::with_retries;
use prost::Message;
use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client, ErrorKind, RedisError, Script, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_MAX_RECORDS: usize = 1000;
const MIN_VERSION: (u32, u32) = (6, 2);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub struct RedisBackend {
    client: Client,
    conn: ConnectionManager,
    namespace: String,
    retention: Duration,
    script: Script,
}

impl RedisBackend {
    /// Connects, and checks that the deployment cannot lose stream data silently.
    pub async fn connect(options: RedisOptions) -> Result<Self> {
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
        Ok(Self {
            client,
            conn,
            namespace: options.namespace,
            retention,
            script: Script::new(include_str!("append.lua")),
        })
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
        let keys = self.chain_keys(&stream.workflow_id, required_first_run(stream)?);
        let ids = self
            .run_append(&keys, &id, req.seq, &entries, false)
            .await?;
        Ok(ids
            .into_iter()
            .take(req.records.len())
            .map(|entry_id| keys.cursor(&entry_id))
            .collect())
    }

    pub async fn close(&self, req: CloseRequest) -> Result<()> {
        let stream = stream_ref(&req.stream)?;
        let owner = req.owner.ok_or_else(|| {
            Error::stream(
                StreamFailureType::NotOwner,
                "only the stream's owner may close it",
            )
        })?;
        let close = req.close.unwrap_or_default();
        let id = identity(&crate::proto::api::Producer {
            variant: Some(crate::proto::api::producer::Variant::Workflow(owner)),
        })?;
        let entry = vec![
            ("kind", b"close".to_vec()),
            ("state", close.state.to_string().into_bytes()),
            (
                "failure",
                close.failure.map(|f| f.encode_to_vec()).unwrap_or_default(),
            ),
            ("reason", close.reason.into_bytes()),
        ];
        let keys = self.chain_keys(&stream.workflow_id, required_first_run(stream)?);
        self.run_append(&keys, &id, req.seq, &[entry], true).await?;
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
                None => return Ok(service::ReadResponse::default()),
            }
        } else {
            let (first_run, entry_id) = self.parse_cursor(&stream.workflow_id, &req.after)?;
            if !stream.first_run_id.is_empty() && stream.first_run_id != first_run {
                return Err(Error::stream(
                    StreamFailureType::CursorMismatch,
                    "the cursor belongs to another run chain",
                ));
            }
            (self.chain_keys(&stream.workflow_id, &first_run), entry_id)
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
        if req.last > 0 {
            return self.read_last(&mut conn, &keys, &req).await;
        }
        self.read_after(conn, &keys, &req, after, deadline).await
    }

    /// Resolves the chain a read from the beginning reads: the pinned one, or the workflow ID's
    /// latest, waiting until the deadline for one to exist.
    async fn resolve_chain(
        &self,
        conn: &mut ConnectionManager,
        stream: &StreamRef,
        deadline: Instant,
    ) -> Result<Option<ChainKeys>> {
        if !stream.first_run_id.is_empty() {
            return Ok(Some(
                self.chain_keys(&stream.workflow_id, &stream.first_run_id),
            ));
        }
        let current = self.current_key(&stream.workflow_id);
        loop {
            let first_run: Option<String> = conn.get(&current).await.map_err(redis_error)?;
            if let Some(first_run) = first_run {
                return Ok(Some(self.chain_keys(&stream.workflow_id, &first_run)));
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
        keys: &ChainKeys,
        req: &ReadRequest,
        deadline: Instant,
    ) -> Result<service::ReadResponse> {
        loop {
            if exists(&mut conn, &keys.meta).await? {
                return self
                    .read_after(conn, keys, req, "0-0".into(), deadline)
                    .await;
            }
            if Instant::now() + CHAIN_POLL_INTERVAL > deadline {
                return Ok(service::ReadResponse::default());
            }
            sleep(CHAIN_POLL_INTERVAL).await;
        }
    }

    async fn read_after(
        &self,
        mut conn: ConnectionManager,
        keys: &ChainKeys,
        req: &ReadRequest,
        mut last_id: String,
        deadline: Instant,
    ) -> Result<service::ReadResponse> {
        let max_records = usize::try_from(req.max_records)
            .ok()
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_MAX_RECORDS);
        let mut resp = service::ReadResponse {
            cursor: req.after.clone(),
            ..Default::default()
        };
        loop {
            let reply: StreamRangeReply = conn
                .xrange_count(&keys.log, format!("({last_id}"), "+", max_records)
                .await
                .map_err(redis_error)?;
            let mut entries = reply.ids;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if entries.is_empty() && !remaining.is_zero() {
                entries = self.block(&keys.log, &last_id, remaining).await?;
            }
            for entry in entries {
                last_id = entry.id.clone();
                resp.cursor = keys.cursor(&entry.id);
                if let Some(r) = record(keys.cursor(&entry.id), &entry.map, &req.topics)? {
                    resp.records.push(r);
                }
            }
            if !resp.records.is_empty()
                || Instant::now() >= deadline
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
    /// would hold up every other command on a shared one.
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

    /// Reads the last `req.last` matching records, scanning the log backwards.
    async fn read_last(
        &self,
        conn: &mut ConnectionManager,
        keys: &ChainKeys,
        req: &ReadRequest,
    ) -> Result<service::ReadResponse> {
        let reply: StreamRangeReply = conn.xrevrange_all(&keys.log).await.map_err(redis_error)?;
        let mut resp = service::ReadResponse::default();
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
        Ok(resp)
    }

    /// Whether the stream is closed and the reader has read every entry up to `last_id`, which
    /// includes the close entry, the stream's last.
    async fn closed_at(
        &self,
        conn: &mut ConnectionManager,
        keys: &ChainKeys,
        last_id: &str,
    ) -> Result<bool> {
        let closed: Option<String> = conn.hget(&keys.meta, "closed").await.map_err(redis_error)?;
        if closed.as_deref() != Some("1") {
            return Ok(false);
        }
        let after: StreamRangeReply = conn
            .xrange_count(&keys.log, format!("({last_id}"), "+", 1)
            .await
            .map_err(redis_error)?;
        Ok(after.ids.is_empty())
    }

    async fn run_append(
        &self,
        keys: &ChainKeys,
        id: &Identity,
        seq: i64,
        entries: &[Vec<(&str, Vec<u8>)>],
        closing: bool,
    ) -> Result<Vec<String>> {
        let retention_ms = u64::try_from(self.retention.as_millis()).unwrap_or(u64::MAX);
        with_retries(|| {
            let mut invocation = self.script.prepare_invoke();
            invocation
                .key(&keys.meta)
                .key(&keys.log)
                .key(&keys.producers)
                .key(&keys.tombstone)
                .key(&keys.current)
                .arg(&keys.first_run)
                .arg(&id.producer)
                .arg(&id.run_id)
                .arg(id.workflow_task)
                .arg(id.workflow_task_attempt)
                .arg(id.epoch)
                .arg(seq)
                .arg(digest(entries))
                .arg(if closing { "1" } else { "0" })
                .arg(retention_ms)
                .arg(entries.len());
            for entry in entries {
                invocation.arg(entry.len());
                for (field, value) in entry {
                    invocation.arg(*field).arg(value.as_slice());
                }
            }
            let mut conn = self.conn.clone();
            async move {
                let ids: Vec<String> = invocation
                    .invoke_async(&mut conn)
                    .await
                    .map_err(redis_error)?;
                Ok(ids)
            }
        })
        .await
    }

    fn chain_keys(&self, workflow_id: &str, first_run: &str) -> ChainKeys {
        let prefix = format!("{}:{first_run}", self.workflow_prefix(workflow_id));
        ChainKeys {
            meta: format!("{prefix}:meta"),
            log: format!("{prefix}:log"),
            producers: format!("{prefix}:producers"),
            tombstone: format!("{prefix}:tombstone"),
            current: self.current_key(workflow_id),
            first_run: first_run.to_string(),
            prefix,
        }
    }

    fn workflow_prefix(&self, workflow_id: &str) -> String {
        format!("stream:{{{}/{workflow_id}}}", self.namespace)
    }

    fn current_key(&self, workflow_id: &str) -> String {
        format!("{}:current", self.workflow_prefix(workflow_id))
    }

    /// Splits a cursor, `<workflow prefix>:<first run ID>#<entry ID>`, into its chain and entry.
    fn parse_cursor(&self, workflow_id: &str, cursor: &[u8]) -> Result<(String, String)> {
        let mismatch = || {
            Error::stream(
                StreamFailureType::CursorMismatch,
                "the cursor belongs to another stream",
            )
        };
        let cursor = std::str::from_utf8(cursor).map_err(|_| mismatch())?;
        let (chain, entry_id) = cursor.rsplit_once('#').ok_or_else(mismatch)?;
        let first_run = chain
            .strip_prefix(&format!("{}:", self.workflow_prefix(workflow_id)))
            .ok_or_else(mismatch)?;
        Ok((first_run.to_string(), entry_id.to_string()))
    }
}

struct ChainKeys {
    prefix: String,
    meta: String,
    log: String,
    producers: String,
    tombstone: String,
    current: String,
    first_run: String,
}

impl ChainKeys {
    fn cursor(&self, entry_id: &str) -> Vec<u8> {
        format!("{}#{entry_id}", self.prefix).into_bytes()
    }
}

/// Refuses a deployment that is too old for the commands used, or whose eviction policy could
/// drop stream data.
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

async fn close_from_meta(conn: &mut ConnectionManager, keys: &ChainKeys) -> Result<StreamClose> {
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

fn stream_ref(stream: &Option<StreamRef>) -> Result<&StreamRef> {
    stream
        .as_ref()
        .filter(|s| !s.workflow_id.is_empty())
        .ok_or_else(|| Error::InvalidArgument("stream workflow ID is required".into()))
}

fn required_first_run(stream: &StreamRef) -> Result<&str> {
    if stream.first_run_id.is_empty() {
        return Err(Error::InvalidArgument(
            "appends to a Redis stream need the first run ID of the stream's run chain".into(),
        ));
    }
    Ok(&stream.first_run_id)
}

/// Classifies a Redis error: a failure the script reports, a transient failure, or another.
fn redis_error(e: RedisError) -> Error {
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
