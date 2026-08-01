//! Domain-specific clients

#[cfg(feature = "legacy-blocking")]
pub mod kv;
#[cfg(feature = "legacy-blocking")]
pub mod lease;
#[cfg(feature = "legacy-blocking")]
pub mod notice;
#[cfg(feature = "legacy-blocking")]
pub mod queue;
pub mod routes;
#[cfg(feature = "legacy-blocking")]
pub mod rpc;
#[cfg(feature = "legacy-blocking")]
pub mod schedule;
pub mod stream;
