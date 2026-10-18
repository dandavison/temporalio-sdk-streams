//! The Redis backend against a local `redis-server`, which must be on PATH. Each test starts its
//! own server on a free port.

use prost::Message;
use prost_types::Duration as ProtoDuration;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use temporalio_streams::proto::Payload;
use temporalio_streams::proto::api::{
    ActivityProducer, AppendRequest, AppendResponse, ClientProducer, CloseRequest, CloseResponse,
    Producer, ReadRequest, RedisOptions, StreamRef, WorkflowActivationCompletedRequest,
    WorkflowActivationCompletedResponse, WorkflowEnding, WorkflowEvictedRequest,
    WorkflowEvictedResponse, WorkflowProducer, producer::Variant,
};
use temporalio_streams::proto::service::{
    AppendRecord, CloseState, ReadResponse, StreamClose, StreamFailureType,
};
use temporalio_streams::{Error, Streams};
use tokio::time::sleep;

const RETENTION: Duration = Duration::from_millis(500);

struct RedisServer {
    port: u16,
    child: Child,
}

impl RedisServer {
    async fn start(extra: &[&str]) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Self::start_on(port, extra).await
    }

    async fn start_on(port: u16, extra: &[&str]) -> Self {
        let mut child = Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .args(extra)
            .stdout(Stdio::null())
            .spawn()
            .expect("redis-server must be on PATH");
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Self { port, child };
            }
            sleep(Duration::from_millis(20)).await;
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("redis-server did not start on port {port}");
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }

    fn stop(mut self) -> u16 {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.port
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn connect(server: &RedisServer) -> Result<Streams, Error> {
    Streams::redis(RedisOptions {
        url: server.url(),
        namespace: "default".into(),
        retention: Some(proto_duration(RETENTION)),
    })
    .await
}

async fn call<Resp: Message + Default>(
    streams: &Streams,
    name: &str,
    req: impl Message,
) -> Result<Resp, Error> {
    let bytes = streams.call(name, &req.encode_to_vec()).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

fn stream(first_run_id: &str) -> StreamRef {
    StreamRef {
        workflow_id: "wf".into(),
        first_run_id: first_run_id.into(),
        ..Default::default()
    }
}

fn activity_stream(activity_id: &str, run_id: &str) -> StreamRef {
    StreamRef {
        activity_id: activity_id.into(),
        activity_run_id: run_id.into(),
        ..Default::default()
    }
}

fn activity_producer(activity_id: &str, run_id: &str, attempt_stamp: i32) -> ActivityProducer {
    ActivityProducer {
        activity_id: activity_id.into(),
        activity_run_id: run_id.into(),
        attempt_stamp,
        ..Default::default()
    }
}

fn client(epoch: i64) -> Producer {
    Producer {
        variant: Some(Variant::Client(ClientProducer {
            id: "c".into(),
            epoch,
        })),
    }
}

fn owner(previous_started_event_id: i64, attempt: i32) -> WorkflowProducer {
    WorkflowProducer {
        run_id: "run-1".into(),
        previous_started_event_id,
        attempt,
    }
}

fn records(values: &[&str]) -> Vec<AppendRecord> {
    values
        .iter()
        .map(|v| AppendRecord {
            topic: String::new(),
            payload: Some(Payload {
                data: v.as_bytes().to_vec(),
                ..Default::default()
            }),
        })
        .collect()
}

async fn append(
    streams: &Streams,
    first_run_id: &str,
    producer: Producer,
    seq: i64,
    values: &[&str],
) -> Result<Vec<Vec<u8>>, Error> {
    let resp: AppendResponse = call(
        streams,
        "Append",
        AppendRequest {
            stream: Some(stream(first_run_id)),
            producer: Some(producer),
            seq,
            records: records(values),
            finish: false,
        },
    )
    .await?;
    Ok(resp.cursors)
}

async fn read(streams: &Streams, stream: StreamRef, after: Vec<u8>) -> Result<ReadResponse, Error> {
    call(
        streams,
        "Read",
        ReadRequest {
            stream: Some(stream),
            after,
            max_records: 10_000,
            wait: Some(proto_duration(Duration::from_millis(50))),
            ..Default::default()
        },
    )
    .await
}

fn values(resp: &ReadResponse) -> Vec<String> {
    resp.records
        .iter()
        .filter_map(|r| r.payload.as_ref())
        .map(|p| String::from_utf8(p.data.clone()).unwrap())
        .collect()
}

fn failure_type<T: std::fmt::Debug>(result: Result<T, Error>) -> StreamFailureType {
    match result {
        Err(Error::Stream { failure_type, .. }) => failure_type,
        other => panic!("expected a stream failure, got {other:?}"),
    }
}

fn proto_duration(d: Duration) -> ProtoDuration {
    ProtoDuration::try_from(d).unwrap()
}

async fn activation_completed(
    streams: &Streams,
    owner: WorkflowProducer,
    values: &[&str],
    ending: Option<WorkflowEnding>,
) -> Result<(), Error> {
    let _: WorkflowActivationCompletedResponse = call(
        streams,
        "WorkflowActivationCompleted",
        WorkflowActivationCompletedRequest {
            workflow_id: "wf".into(),
            run_id: owner.run_id,
            first_run_id: "run-1".into(),
            previous_started_event_id: owner.previous_started_event_id,
            attempt: owner.attempt,
            records: records(values),
            ending,
        },
    )
    .await?;
    Ok(())
}

async fn evicted(streams: &Streams, execution_ending: bool) {
    let _: WorkflowEvictedResponse = call(
        streams,
        "WorkflowEvicted",
        WorkflowEvictedRequest {
            run_id: "run-1".into(),
            execution_ending,
        },
    )
    .await
    .unwrap();
}

fn ending(state: CloseState, cron: bool, retry_policy: bool) -> Option<WorkflowEnding> {
    Some(WorkflowEnding {
        close: Some(StreamClose {
            state: state.into(),
            ..Default::default()
        }),
        cron,
        retry_policy,
    })
}

#[tokio::test]
async fn deployment_with_an_evicting_memory_policy_is_refused() {
    let server = RedisServer::start(&["--maxmemory-policy", "allkeys-lru"]).await;
    assert!(matches!(
        connect(&server).await,
        Err(Error::InvalidArgument(m)) if m.contains("noeviction")
    ));
}

#[tokio::test]
async fn seq_must_follow_the_latest_in_its_epoch() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 1, &["a"]).await),
        StreamFailureType::SeqGap
    );
    let first = append(&s, "run-1", client(1), 0, &["a"]).await.unwrap();
    assert_eq!(
        append(&s, "run-1", client(1), 0, &["a"]).await.unwrap(),
        first
    );
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 0, &["b"]).await),
        StreamFailureType::SeqDivergent
    );
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 2, &["b"]).await),
        StreamFailureType::SeqGap
    );
    append(&s, "run-1", client(1), 1, &["b"]).await.unwrap();
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 0, &["a"]).await),
        StreamFailureType::SeqStale
    );
    assert_eq!(
        failure_type(append(&s, "run-1", client(2), 1, &["c"]).await),
        StreamFailureType::SeqGap
    );
    append(&s, "run-1", client(2), 0, &["c"]).await.unwrap();
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 2, &["d"]).await),
        StreamFailureType::EpochFenced
    );
    let resp = read(&s, stream("run-1"), vec![]).await.unwrap();
    assert_eq!(values(&resp), ["a", "b", "c"]);
}

#[tokio::test]
async fn epochs_and_seqs_outside_exact_integers_are_rejected() {
    const MAX: i64 = (1 << 53) - 1;
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    append(&s, "run-1", client(MAX), 0, &["a"]).await.unwrap();
    append(&s, "run-1", client(MAX), 1, &["b"]).await.unwrap();
    let resp = read(&s, stream("run-1"), vec![]).await.unwrap();
    assert_eq!(values(&resp), ["a", "b"]);
    assert_eq!(resp.records[1].producer.as_ref().unwrap().epoch, MAX);
    for (epoch, seq) in [(MAX + 1, 0), (-1, 0), (1, MAX + 1)] {
        assert!(
            matches!(
                append(&s, "run-2", client(epoch), seq, &["c"]).await,
                Err(Error::InvalidArgument(_))
            ),
            "epoch {epoch} seq {seq}"
        );
    }
}

#[tokio::test]
async fn tail_read_reports_close() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    append(&s, "run-1", client(1), 0, &["a"]).await.unwrap();
    let _: CloseResponse = call(
        &s,
        "Close",
        CloseRequest {
            stream: Some(stream("run-1")),
            owner: Some(owner(3, 1)),
            activity_owner: None,
            seq: 0,
            close: Some(StreamClose {
                state: CloseState::Completed.into(),
                ..Default::default()
            }),
        },
    )
    .await
    .unwrap();
    let resp: ReadResponse = call(
        &s,
        "Read",
        ReadRequest {
            stream: Some(stream("run-1")),
            last: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(values(&resp), ["a"]);
    assert_eq!(resp.close.unwrap().state(), CloseState::Completed);
}

#[tokio::test]
async fn streams_are_keyed_by_run_chain() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    assert!(matches!(
        append(&s, "", client(1), 0, &["a"]).await,
        Err(Error::InvalidArgument(_))
    ));
    let first_chain = append(&s, "run-1", client(1), 0, &["a"]).await.unwrap();
    let _: CloseResponse = call(
        &s,
        "Close",
        CloseRequest {
            stream: Some(stream("run-1")),
            owner: Some(owner(3, 1)),
            activity_owner: None,
            seq: 0,
            close: Some(StreamClose {
                state: CloseState::Completed.into(),
                ..Default::default()
            }),
        },
    )
    .await
    .unwrap();
    append(&s, "run-2", client(1), 0, &["b"]).await.unwrap();

    assert_eq!(
        values(&read(&s, stream("run-1"), vec![]).await.unwrap()),
        ["a"]
    );
    assert_eq!(
        values(&read(&s, stream("run-2"), vec![]).await.unwrap()),
        ["b"]
    );
    assert_eq!(values(&read(&s, stream(""), vec![]).await.unwrap()), ["b"]);
    assert_eq!(
        failure_type(read(&s, stream("run-2"), first_chain[0].clone()).await),
        StreamFailureType::CursorMismatch
    );
}

#[tokio::test]
async fn removed_stream_reads_as_expired_until_its_tombstone_expires() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    let cursors = append(&s, "run-1", client(1), 0, &["a"]).await.unwrap();
    sleep(RETENTION + Duration::from_millis(200)).await;
    assert_eq!(
        failure_type(read(&s, stream("run-1"), vec![]).await),
        StreamFailureType::Expired
    );
    assert_eq!(
        failure_type(read(&s, stream("run-1"), cursors[0].clone()).await),
        StreamFailureType::Expired
    );
    assert_eq!(
        failure_type(append(&s, "run-1", client(1), 1, &["b"]).await),
        StreamFailureType::Expired
    );
    sleep(RETENTION).await;
    assert_eq!(
        failure_type(read(&s, stream("run-1"), cursors[0].clone()).await),
        StreamFailureType::NotFound
    );
    assert!(
        read(&s, stream("run-1"), vec![])
            .await
            .unwrap()
            .records
            .is_empty()
    );
}

#[tokio::test]
async fn writes_refresh_retention() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    for seq in 0..4 {
        append(&s, "run-1", client(1), seq, &["a"]).await.unwrap();
        sleep(RETENTION / 2).await;
    }
    assert_eq!(
        read(&s, stream("run-1"), vec![])
            .await
            .unwrap()
            .records
            .len(),
        4
    );
}

#[tokio::test]
async fn transient_failure_is_retried_until_redis_returns() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    let port = server.stop();
    let pending = tokio::spawn(async move {
        let result = append(&s, "run-1", client(1), 0, &["a"]).await;
        (s, result)
    });
    sleep(Duration::from_secs(1)).await;
    let _server = RedisServer::start_on(port, &[]).await;
    let (s, result) = pending.await.unwrap();
    result.unwrap();
    assert_eq!(
        values(&read(&s, stream("run-1"), vec![]).await.unwrap()),
        ["a"]
    );
}

#[tokio::test]
async fn activation_records_are_split_into_appends_that_fit() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    let many: Vec<String> = (0..2500).map(|i| i.to_string()).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    activation_completed(&s, owner(3, 1), &many, None)
        .await
        .unwrap();
    let resp = read(&s, stream("run-1"), vec![]).await.unwrap();
    assert_eq!(values(&resp), many);
    let seqs: Vec<i64> = resp
        .records
        .iter()
        .map(|r| r.producer.as_ref().unwrap().seq)
        .collect();
    assert_eq!((seqs[0], seqs[999], seqs[1000], seqs[2499]), (0, 0, 1, 2));
}

#[tokio::test]
async fn seq_continues_within_a_task_attempt_and_restarts_in_the_next() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    activation_completed(&s, owner(3, 1), &["a"], None)
        .await
        .unwrap();
    activation_completed(&s, owner(3, 1), &["b"], None)
        .await
        .unwrap();
    activation_completed(&s, owner(3, 2), &["c"], None)
        .await
        .unwrap();
    activation_completed(&s, owner(9, 1), &["d"], None)
        .await
        .unwrap();
    let resp = read(&s, stream("run-1"), vec![]).await.unwrap();
    let ids: Vec<(i64, i32, i64)> = resp
        .records
        .iter()
        .map(|r| {
            let p = r.producer.as_ref().unwrap();
            (p.workflow_task, p.workflow_task_attempt, p.seq)
        })
        .collect();
    assert_eq!(ids, [(3, 1, 0), (3, 1, 1), (3, 2, 0), (9, 1, 0)]);
}

#[tokio::test]
async fn close_is_written_only_after_an_ending_eviction() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    activation_completed(
        &s,
        owner(3, 1),
        &["a"],
        ending(CloseState::Completed, false, false),
    )
    .await
    .unwrap();
    assert!(
        read(&s, stream("run-1"), vec![])
            .await
            .unwrap()
            .close
            .is_none()
    );
    evicted(&s, false).await;
    assert!(
        read(&s, stream("run-1"), vec![])
            .await
            .unwrap()
            .close
            .is_none()
    );

    activation_completed(
        &s,
        owner(9, 1),
        &["b"],
        ending(CloseState::Completed, false, false),
    )
    .await
    .unwrap();
    evicted(&s, true).await;
    let resp = read(&s, stream("run-1"), vec![]).await.unwrap();
    assert_eq!(values(&resp), ["a", "b"]);
    assert_eq!(resp.close.unwrap().state(), CloseState::Completed);
}

#[tokio::test]
async fn continuing_chain_is_not_closed() {
    for (state, cron, retry_policy) in [
        (CloseState::Completed, true, false),
        (CloseState::Failed, true, false),
        (CloseState::Failed, false, true),
    ] {
        let server = RedisServer::start(&[]).await;
        let s = connect(&server).await.unwrap();
        activation_completed(&s, owner(3, 1), &["a"], ending(state, cron, retry_policy))
            .await
            .unwrap();
        evicted(&s, true).await;
        assert!(
            read(&s, stream("run-1"), vec![])
                .await
                .unwrap()
                .close
                .is_none(),
            "{state:?} cron={cron} retry_policy={retry_policy}"
        );
    }
}

#[tokio::test]
async fn activity_owned_stream_is_keyed_apart_from_a_workflow_with_the_same_id() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    append(&s, "run-1", client(1), 0, &["workflow"])
        .await
        .unwrap();
    let resp: AppendResponse = call(
        &s,
        "Append",
        AppendRequest {
            stream: Some(activity_stream("wf", "act-run")),
            producer: Some(Producer {
                variant: Some(Variant::Activity(activity_producer("wf", "act-run", 1))),
            }),
            seq: 0,
            records: records(&["activity"]),
            finish: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.cursors.len(), 1);
    assert_eq!(
        values(&read(&s, activity_stream("wf", ""), vec![]).await.unwrap()),
        ["activity"]
    );
    assert_eq!(
        values(&read(&s, stream(""), vec![]).await.unwrap()),
        ["workflow"]
    );
    assert_eq!(
        failure_type(read(&s, stream("run-1"), resp.cursors[0].clone()).await),
        StreamFailureType::CursorMismatch
    );
}

#[tokio::test]
async fn only_the_owning_activity_closes_its_stream() {
    let server = RedisServer::start(&[]).await;
    let s = connect(&server).await.unwrap();
    let close =
        |owner: Option<WorkflowProducer>, activity_owner: Option<ActivityProducer>| CloseRequest {
            stream: Some(activity_stream("act", "act-run")),
            owner,
            activity_owner,
            seq: 0,
            close: Some(StreamClose {
                state: CloseState::Completed.into(),
                ..Default::default()
            }),
        };
    for (owner, activity_owner) in [
        (Some(owner(3, 1)), None),
        (None, Some(activity_producer("other", "act-run", 1))),
    ] {
        assert_eq!(
            failure_type(call::<CloseResponse>(&s, "Close", close(owner, activity_owner)).await),
            StreamFailureType::NotOwner
        );
    }
    let _: CloseResponse = call(
        &s,
        "Close",
        close(None, Some(activity_producer("act", "act-run", 1))),
    )
    .await
    .unwrap();
    let resp = read(&s, activity_stream("act", "act-run"), vec![])
        .await
        .unwrap();
    assert_eq!(resp.close.unwrap().state(), CloseState::Completed);
}
