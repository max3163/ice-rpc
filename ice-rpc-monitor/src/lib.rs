//! Out-of-band observer for `ice-rpc`.
//!
//! The observer attaches **in read-only mode** to the iceoryx2 services an
//! ice-rpc process already exposes (`{channel}_req`, `{channel}_resp` and their
//! `_notify` event services), reads the zero-copy `RpcHeader` and derives:
//!
//! - **Prometheus metrics**: request/response counts, payload-size and latency
//!   histograms, in-flight gauge, sample-loss and orphan counters, node liveness;
//! - **a trace stream** correlated by `correlation_id`, emitted as NDJSON.
//!
//! In the default **stats** mode the rkyv payload is never read, so the observer
//! stays ignorant of the service types and does not touch the hot path. In
//! **detail** mode the payloads are decoded through the [`Decoders`] registry —
//! built from the shared service contract, e.g. `common::decoders()` — and
//! rendered with each type's [`Display`](std::fmt::Display) implementation, or
//! with its [`Debug`](std::fmt::Debug) implementation when it has none.
//!
//! Because the transport disables safe overflow, a saturated observer is skipped
//! by the publisher instead of blocking it; the lost samples are counted through
//! the `seq` field of the header.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic

pub mod config;
pub mod console;
pub mod health;
pub mod metrics;
pub mod prometheus;
pub mod traces;

mod collector;
mod correlate;
mod loss;

pub use collector::Monitor;
// Decoding lives in `ice-rpc` so the macro-generated `{Service}Decoder`s can
// implement it; re-exported here for the observer's convenience.
pub use ice_rpc::monitor::{ClosureDecoder, Decoders, ServiceDecoder};
