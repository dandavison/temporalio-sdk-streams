//! Generated protos: the crate's API, and the stream service's.

/// The stream service's protos, `temporal.api.nexusservices.streamservice.v1`.
pub mod service {
    include!(concat!(
        env!("OUT_DIR"),
        "/temporal.api.nexusservices.streamservice.v1.rs"
    ));
}

/// The crate's API, `temporal.sdk.streams.v1`.
pub mod api {
    include!(concat!(env!("OUT_DIR"), "/temporal.sdk.streams.v1.rs"));
}

pub use temporalio_protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
