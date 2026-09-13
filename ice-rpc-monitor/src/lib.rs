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
//! It never decodes the rkyv payload, so it stays ignorant of the service types
//! and does not touch the hot path. Because the transport disables safe overflow,
//! a saturated observer is skipped by the publisher instead of blocking it; the
//! lost samples are counted through the `seq` field of the header.

#![cfg_attr(test, allow(clippy::unwrap_used))] // test code may panic

pub mod config;
pub mod metrics;
pub mod prometheus;

mod collector;
mod correlate;
mod loss;
mod traces;

pub use collector::Monitor;
