//! Benchmark workload service: representative request payloads.
//!
//! The existing demo services carry **tiny** requests (`get_user_age` takes one
//! short name, `get_person` two). That is the case where the cost of the request
//! path matters the least. This service carries the shapes that actually decide
//! it — measured in `benches/payload_shapes.rs`:
//!
//! - **one large text field** — the copy cost scales with the bytes, and the
//!   deserialization adds the UTF-8 validation;
//! - **K variable string fields** — the decode pays **one allocation per
//!   `String`**, so the cost scales with the *field count*, not the byte count;
//! - **one large binary body** — a single `Vec<u8>`, where the copy dominates;
//! - **a nested structure** — several strings and lists, the realistic shape.
//!
//! Every method takes what a real caller has: **owned** values. A request that
//! carries structures is materialized on both sides — the caller builds it, the
//! provider rebuilds it from the archive — and that is exactly the cost this
//! service exists to price. The A/B harness and the conclusions drawn from it
//! live in `plans/zero-copie-structs-options.md`.
//!
//! Every method answers a small `u32` (a length-derived checksum) so the
//! **response** cannot be the cause, except `fetch`, whose response is a
//! structure on purpose: it is the case that prices the response side.

use ice_rpc::{service, Observable};
use rkyv::{Archive, Deserialize, Serialize};

/// One filter of a workload query.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct WorkloadFilter {
    /// Field name the filter applies to.
    pub field: String,
    /// Value to match.
    pub value: String,
}

/// A representative nested request: several variable fields and two lists.
///
/// What matters to the wire is the shape, not the type: `search` sends the same
/// request declared **flat** (`&str` plus three `&[&str]`), so nothing new has to
/// be declared, documented or maintained on this side.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct WorkloadQuery {
    /// Primary key of the query.
    pub key: String,
    /// Free-form tags.
    pub tags: Vec<String>,
    /// Structured filters.
    pub filters: Vec<WorkloadFilter>,
}

/// A scalar enum, to exercise enum args end to end.
///
/// Named `WorkloadProfile` (not `WorkloadKind`) so the benchmark's own
/// case-selection enum keeps its name.
#[derive(
    Debug, Clone, Copy, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize,
)]
pub enum WorkloadProfile {
    /// No preference.
    Fast,
    /// Balanced.
    Balanced,
    /// Maximum precision.
    Precise,
}

/// Every scalar family, plus `Option` and an enum, in one struct.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct WorkloadScalars {
    /// A boolean flag.
    pub flag: bool,
    /// An unsigned count.
    pub count: u32,
    /// A signed total.
    pub total: i64,
    /// A floating-point ratio.
    pub ratio: f64,
    /// An enum.
    pub profile: WorkloadProfile,
    /// An optional value.
    pub optional: Option<u32>,
}

/// Benchmark workload over representative request shapes.
///
/// No `max_slice_len` is declared: with `publisher-allocation-strategy =
/// "power-of-two"` in the iceoryx2 configuration, the publisher grows the
/// sample past the channel's initial 256 B instead of refusing it, so the
/// representative payloads (up to 32 KiB) travel without pinning a 64 MiB
/// segment per channel.
#[service("WorkloadService")]
pub trait WorkloadService {
    /// Echoes the length of one text field.
    ///
    /// A whole `String` travels: one allocation to build the request, one to
    /// rebuild it on the provider side.
    async fn echo_text(&self, text: String) -> Observable<u32, String>;

    /// Indexes K string fields, echoing their total length.
    ///
    /// The shape that decides the cost of a structure-shaped request: K variable
    /// fields, so the decode pays **one `String` per field** — the cost scales
    /// with the field count, not with the byte count.
    async fn index_fields(&self, fields: Vec<String>) -> Observable<u32, String>;

    /// Accepts one binary body, echoing its length.
    ///
    /// A single `Vec<u8>`: here the byte count dominates, not the field count.
    async fn upload(&self, body: Vec<u8>) -> Observable<u32, String>;

    /// Searches with a nested request, echoing a checksum of its size.
    ///
    /// Declared **flat** rather than as one `WorkloadQuery`: the same fields
    /// travel as four arguments, so the case prices the variable fields without
    /// also pricing the structural nesting. Measured against a bespoke view type,
    /// the flat form is within 3 % and costs the library nothing at all
    /// (`plans/c2c-nested-views.md`).
    async fn search(
        &self,
        key: String,
        tags: Vec<String>,
        filter_fields: Vec<String>,
        filter_values: Vec<String>,
    ) -> Observable<u32, String>;

    /// Answers one scalar with one scalar: the **control** of [`fetch`](Self::fetch).
    ///
    /// Same request shape, response of four bytes. Comparing the two isolates
    /// what a big **response** costs, where the request cannot be the cause.
    async fn ping(&self, count: u32) -> Observable<u32, String>;

    /// Returns a nested structure, echoing nothing of the request.
    ///
    /// The response is the very `WorkloadQuery` the flat `search` sends as a
    /// request: a **struct** crosses the wire here as a *return* value, which is
    /// the direction the caller cannot read in place — a stream hands out owned
    /// values, so it materializes the whole structure. `count` drives the number
    /// of variable fields, so one parameter prices the response cost
    /// (`plans/zero-copie-structs-options.md` §9).
    async fn fetch(&self, count: u32) -> Observable<WorkloadQuery, String>;

    /// Covers every remaining type family in one call: scalars (`bool`, `u32`,
    /// `i64`, `f64`), an enum, an `Option`, a `Vec` of POD (`u32`) and a `Vec`
    /// of structs (`WorkloadFilter`).
    async fn mixed(
        &self,
        flag: bool,
        count: u32,
        total: i64,
        scalars: WorkloadScalars,
        codes: Vec<u32>,
        filters: Vec<WorkloadFilter>,
    ) -> Observable<u32, String>;
}
