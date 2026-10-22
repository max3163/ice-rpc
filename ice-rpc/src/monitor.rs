//! Read-only observation surface used by the `ice-rpc-monitor` observer.
//!
//! Lets an external process attach to the channels an ice-rpc process exposes
//! without touching the hot path: only the zero-copy `RpcHeader` (sequence,
//! timestamp) and the payload length are read, never the rkyv payload. The
//! identity of the emitter (PID, node id, publisher id) is read from the
//! **native** iceoryx2 sample [`Emitter`], so the wire header carries none.
//!
//! The observer is linked against the **same service definitions** as the
//! providers and consumers, so it can also decode the payloads. Each `#[service]`
//! trait generates a `{Trait}Decoder` implementing [`ServiceDecoder`] and, with
//! the `monitoring` feature, submits it into the `DECODERS` slice at link time.
//! `Decoders::linked()` reads that slice back, so an observer lists no service:
//! it renders every message with the [`Display`](std::fmt::Display)
//! implementation of the service types, or with their [`Debug`](std::fmt::Debug)
//! implementation when they have none (see [`render_value!`]).
//!
//! # Two concerns, two modules
//!
//! - `inventory` answers *what is running*: the iceoryx2 nodes and their
//!   liveness, the services and their role in a channel, and the file layout
//!   iceoryx2 uses for its segments;
//! - `decode` answers *what was said*: the rendering of a payload and the
//!   registry of the decoders.
//!
//! The split is what keeps a statistics-only observer free of the rendering
//! code: a monitor that never decodes does not carry the `Display` machinery.

mod decode;
mod inventory;

pub use crate::transport::{
    Direction, DirectionView, Emitter, decode_aligned, discover_channels, is_provider_connected,
};

pub use decode::{
    ClosureDecoder, Decoders, ServiceDecoder, decode_request, decode_response, decode_response_unit,
};

// The link-time registration surface of `monitoring`: `DECODERS` and
// `Decoders::linked()`.
#[cfg(feature = "monitoring")]
pub use decode::{DECODERS, DecoderRegistration};
pub use inventory::{
    Iceoryx2Layout, NodeHealth, NodeInfo, ServiceInfo, ServiceRole, iceoryx2_layout, list_nodes,
    list_services,
};

// Plumbing of the generated `monitoring` code: `render_value!` resolves these names.
#[doc(hidden)]
pub use decode::{RenderValue, RenderViaDebug, RenderViaDisplay};

#[doc(hidden)]
pub use crate::render_value;
