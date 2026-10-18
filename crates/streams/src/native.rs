//! A backend that stores streams in Temporal, embedded in their owning workflows. Every operation is
//! a direct Nexus call to the stream service on the System Nexus Endpoint, which the server runs in
//! a transaction on the owning workflow. The server checks activity and workflow producers, records
//! which attempt completed each workflow task, and closes the stream when the workflow ends.

use crate::error::{Error, Result};
use crate::limits;
use crate::proto::api::{self, AppendRequest, CloseRequest, ReadRequest, producer::Variant};
use crate::proto::service::{self, StreamFailureType};
use crate::proto::{Failure, Payload};
use crate::retry::with_retries;
use prost::{Message, Name};
use std::collections::HashMap;
use temporalio_client::Connection;
use temporalio_client::grpc::WorkflowService;
use temporalio_client::tonic::{self, Code};
use temporalio_protos::temporal::api::workflowservice::v1::{
    StartNexusOperationRequest, start_nexus_operation_response,
};

const SYSTEM_ENDPOINT: &str = "__temporal_system";
const SERVICE: &str = "temporal.api.nexusservices.streamservice.v1.StreamService";

pub struct NativeBackend {
    connection: Connection,
    namespace: String,
    commit_mode: bool,
}

impl NativeBackend {
    pub fn new(connection: Connection, options: api::NativeOptions) -> Self {
        Self {
            connection,
            namespace: options.namespace,
            commit_mode: options.commit_mode,
        }
    }

    /// Whether workflows' own records travel in the workflow task completion.
    pub fn commit_mode(&self) -> bool {
        self.commit_mode
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
            )
            .await?;
        Ok(resp.cursors)
    }

    pub async fn close(&self, req: CloseRequest) -> Result<()> {
        let owner = req.owner.ok_or_else(|| {
            Error::stream(
                StreamFailureType::NotOwner,
                "only the stream's owner may close it",
            )
        })?;
        let _: service::CloseResponse = self
            .call(
                "Close",
                &service::CloseRequest {
                    stream: service_ref(req.stream),
                    owner: Some(service::WorkflowTaskProducer {
                        run_id: owner.run_id,
                        previous_started_event_id: owner.previous_started_event_id,
                        attempt: owner.attempt,
                    }),
                    seq: req.seq,
                    close: req.close,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn read(&self, req: ReadRequest) -> Result<service::ReadResponse> {
        self.call(
            "Read",
            &service::ReadRequest {
                stream: service_ref(req.stream),
                after: req.after,
                topics: req.topics,
                max_records: req.max_records,
                wait: req.wait,
                last: req.last,
            },
        )
        .await
    }

    /// Makes a direct Nexus call to the stream service, retrying transient failures.
    async fn call<Req: Message + Name, Resp: Message + Default>(
        &self,
        operation: &str,
        req: &Req,
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
            let request = request.clone();
            async move {
                WorkflowService::start_nexus_operation(
                    &mut connection,
                    tonic::Request::new(request),
                )
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

fn service_ref(stream: Option<api::StreamRef>) -> Option<service::StreamRef> {
    stream.map(|s| service::StreamRef {
        workflow_id: s.workflow_id,
        first_run_id: s.first_run_id,
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
