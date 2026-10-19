//! A backend that stores streams in Temporal, embedded in their owner: a workflow run, or a
//! Standalone Activity execution. Every operation is a direct Nexus call to the stream service on
//! the System Nexus Endpoint, which the server runs in a transaction on the owner, so the server
//! needs CHASM and direct Nexus calls enabled. The server checks the owner's own activity and
//! workflow task producers, records which attempt completed each of an owning workflow's tasks,
//! and closes the stream when the owner ends. It does not verify clients, checks a local activity
//! only by its run, and on an activity-owned stream does not verify any producer but the activity.
//!
//! A workflow's stream is not moved to the next run of its chain at continue-as-new, retry or
//! cron, so the next run starts with a new, empty stream.

use crate::error::{Error, Result};
use crate::limits;
use crate::proto::api::{self, AppendRequest, CloseRequest, ReadRequest, producer::Variant};
use crate::proto::service::{self, StreamFailureType};
use crate::proto::{Failure, Payload};
use crate::retry::with_retries;
use prost::{Message, Name};
use std::collections::HashMap;
use std::time::Duration;
use temporalio_client::Connection;
use temporalio_client::grpc::WorkflowService;
use temporalio_client::tonic::metadata::{
    AsciiMetadataKey, AsciiMetadataValue, MetadataKey, MetadataValue,
};
use temporalio_client::tonic::{self, Code};
use temporalio_protos::temporal::api::workflowservice::v1::{
    StartNexusOperationRequest, start_nexus_operation_response,
};

const SYSTEM_ENDPOINT: &str = "__temporal_system";
const SERVICE: &str = "temporal.api.nexusservices.streamservice.v1.StreamService";

pub struct NativeBackend {
    connection: Connection,
    namespace: String,
}

/// gRPC metadata and deadline for each call of an operation.
#[derive(Default)]
struct CallOptions {
    metadata: HashMap<String, Vec<u8>>,
    timeout: Option<Duration>,
}

impl NativeBackend {
    pub fn new(connection: Connection, options: api::NativeOptions) -> Self {
        Self {
            connection,
            namespace: options.namespace,
        }
    }

    pub async fn append(&self, req: AppendRequest) -> Result<Vec<Vec<u8>>> {
        limits::check(&req.records)?;
        let resp: service::AppendResponse = self
            .call(
                "Append",
                &service::AppendRequest {
                    stream: service_ref(req.stream),
                    producer: Some(service_producer(req.producer)?),
                    seq: req.seq,
                    records: req.records,
                    finish: req.finish,
                },
                CallOptions::default(),
            )
            .await?;
        Ok(resp.cursors)
    }

    pub async fn close(&self, req: CloseRequest) -> Result<()> {
        let (owner, activity_task_token) = match (req.owner, req.activity_owner) {
            (Some(w), None) => (
                Some(service::WorkflowTaskProducer {
                    run_id: w.run_id,
                    previous_started_event_id: w.previous_started_event_id,
                    attempt: w.attempt,
                }),
                vec![],
            ),
            (None, Some(a)) => (None, a.task_token),
            _ => {
                return Err(Error::stream(
                    StreamFailureType::NotOwner,
                    "only the stream's owner may close it",
                ));
            }
        };
        let _: service::CloseResponse = self
            .call(
                "Close",
                &service::CloseRequest {
                    stream: service_ref(req.stream),
                    owner,
                    seq: req.seq,
                    close: req.close,
                    activity_task_token,
                },
                CallOptions::default(),
            )
            .await?;
        Ok(())
    }

    pub async fn read(&self, req: ReadRequest) -> Result<service::ReadResponse> {
        let options = CallOptions {
            metadata: req.rpc_metadata,
            timeout: req
                .rpc_timeout
                .map(Duration::try_from)
                .transpose()
                .map_err(|e| Error::InvalidArgument(e.to_string()))?,
        };
        self.call(
            "Read",
            &service::ReadRequest {
                stream: service_ref(req.stream),
                after: req.after,
                topics: req.topics,
                max_records: req.max_records,
                wait: req.wait,
                last: req.last,
                ..Default::default()
            },
            options,
        )
        .await
    }

    /// Makes a direct Nexus call to the stream service, retrying transient failures. Retrying an
    /// append or close is safe because the stream dedupes a producer's latest seq; the call sets no
    /// request ID, and the server does not dedupe calls.
    async fn call<Req: Message + Name, Resp: Message + Default>(
        &self,
        operation: &str,
        req: &Req,
        options: CallOptions,
    ) -> Result<Resp> {
        let request = StartNexusOperationRequest {
            namespace: self.namespace.clone(),
            endpoint: SYSTEM_ENDPOINT.to_string(),
            service: SERVICE.to_string(),
            operation: operation.to_string(),
            input: Some(Payload {
                metadata: HashMap::from([
                    ("encoding".to_string(), b"binary/protobuf".to_vec()),
                    ("messageType".to_string(), Req::full_name().into_bytes()),
                ]),
                data: req.encode_to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = with_retries(|| {
            let mut connection = self.connection.clone();
            let request = grpc_request(request.clone(), &options);
            async move {
                WorkflowService::start_nexus_operation(&mut connection, request?)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(status_error)
            }
        })
        .await?;
        match response.variant {
            Some(start_nexus_operation_response::Variant::SyncSuccess(success)) => Ok(
                Resp::decode(success.payload.unwrap_or_default().data.as_slice())?,
            ),
            Some(start_nexus_operation_response::Variant::OperationFailure(failure)) => {
                Err(operation_error(failure.failure.unwrap_or_default()))
            }
            other => Err(Error::Internal(format!(
                "unexpected direct call response {other:?}"
            ))),
        }
    }
}

fn grpc_request<T>(message: T, options: &CallOptions) -> Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    for (key, value) in &options.metadata {
        let invalid = |e: &dyn std::fmt::Display| {
            Error::InvalidArgument(format!("invalid RPC metadata {key}: {e}"))
        };
        if key.ends_with("-bin") {
            let key = MetadataKey::from_bytes(key.as_bytes()).map_err(|e| invalid(&e))?;
            request
                .metadata_mut()
                .insert_bin(key, MetadataValue::from_bytes(value));
        } else {
            let key = AsciiMetadataKey::from_bytes(key.as_bytes()).map_err(|e| invalid(&e))?;
            let value = AsciiMetadataValue::try_from(value.as_slice()).map_err(|e| invalid(&e))?;
            request.metadata_mut().insert(key, value);
        }
    }
    if let Some(timeout) = options.timeout {
        request.set_timeout(timeout);
    }
    Ok(request)
}

fn service_ref(stream: Option<api::StreamRef>) -> Option<service::StreamRef> {
    stream.map(|s| service::StreamRef {
        workflow_id: s.workflow_id,
        first_run_id: s.first_run_id,
        activity_id: s.activity_id,
        activity_run_id: s.activity_run_id,
        namespace: String::new(),
    })
}

fn service_producer(producer: Option<api::Producer>) -> Result<service::Producer> {
    let variant = producer
        .and_then(|p| p.variant)
        .ok_or_else(|| Error::InvalidArgument("producer is required".into()))?;
    Ok(service::Producer {
        variant: Some(match variant {
            Variant::Client(c) => service::producer::Variant::Client(service::ClientProducer {
                id: c.id,
                epoch: c.epoch,
            }),
            Variant::Activity(a) => service::producer::Variant::ActivityTaskToken(a.task_token),
            Variant::Workflow(w) => {
                service::producer::Variant::WorkflowTask(service::WorkflowTaskProducer {
                    run_id: w.run_id,
                    previous_started_event_id: w.previous_started_event_id,
                    attempt: w.attempt,
                })
            }
            Variant::LocalActivity(la) => {
                service::producer::Variant::LocalActivity(service::LocalActivityProducer {
                    run_id: la.run_id,
                    previous_started_event_id: la.previous_started_event_id,
                    workflow_task_attempt: la.workflow_task_attempt,
                    activity_id: la.activity_id,
                    attempt: la.attempt,
                })
            }
        }),
    })
}

/// The stream error named by an ApplicationFailureInfo type in the failure's cause chain.
fn operation_error(failure: Failure) -> Error {
    let mut current = Some(&failure);
    while let Some(f) = current {
        if let Some(temporalio_protos::temporal::api::failure::v1::failure::FailureInfo::ApplicationFailureInfo(info)) =
            &f.failure_info
            && let Some(failure_type) = StreamFailureType::from_str_name(&info.r#type)
        {
            return Error::stream(failure_type, f.message.clone());
        }
        current = f.cause.as_deref();
    }
    Error::Internal(format!("stream operation failed: {}", failure.message))
}

/// Classifies a gRPC error. A missing stream or owner arrives as an operation failure, not as
/// gRPC NotFound, which comes from the frontend, for example for an unknown namespace; it is still
/// reported as NOT_FOUND.
fn status_error(status: tonic::Status) -> Error {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            Error::Transient(status.to_string())
        }
        Code::InvalidArgument => Error::InvalidArgument(status.message().to_string()),
        Code::NotFound => Error::stream(StreamFailureType::NotFound, status.message()),
        _ => Error::Internal(status.to_string()),
    }
}
