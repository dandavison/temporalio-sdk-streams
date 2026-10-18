//! Stream backends shared by Temporal SDKs. Prototype.
//!
//! An SDK holds a [`Streams`] for one backend, native or Redis, and makes every call through
//! [`Streams::call`], with requests and responses encoded as the protos in `protos/`. The SDK keeps
//! its public API and its payload codec; payloads reach this crate already encoded.

mod error;
mod identity;
mod limits;
mod native;
pub mod proto;
mod redis;
mod retry;
mod streams;
mod workflow;

pub use error::Error;
pub use streams::Streams;
