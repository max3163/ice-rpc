//! Read-only observation surface used by the `ice-rpc-monitor` observer.
//!
//! Lets an external process attach to the channels an ice-rpc process exposes
//! without touching the hot path: only the zero-copy `RpcHeader` (emitter pid,
//! sequence, timestamp) and the payload length are read, never the rkyv payload.
//!
//! The observer is linked against the **same service definitions** as the
//! providers and consumers, so it can also decode the payloads. Each `#[service]`
//! trait generates a `{Trait}Decoder` implementing [`ServiceDecoder`]; register
//! them in a [`Decoders`] registry, then the monitor renders every message with
//! the [`Display`](std::fmt::Display) implementation of the service types.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub use crate::transport::{decode_aligned, discover_channels, Direction, DirectionView};

/// Decodes a rkyv **request** payload into its [`Display`] form.
///
/// Returns `None` when the bytes are not a valid encoding of `R` — typically
/// because the observer was not built with the service types.
///
/// [`Display`]: std::fmt::Display
pub fn decode_request<R>(payload: &[u8]) -> Option<String>
where
    R: rkyv::Archive + std::fmt::Display,
    <R as rkyv::Archive>::Archived:
        rkyv::Deserialize<R, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <R as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    crate::transport::decode_aligned::<R>(payload)
        .ok()
        .map(|value| value.to_string())
}

/// Decodes a rkyv **response** payload into its [`Display`] form.
///
/// A response is a [`WireEvent<T, E>`](crate::types::WireEvent): the human
/// rendering is that of the carried value, or the terminal kind name.
///
/// [`Display`]: std::fmt::Display
pub fn decode_response<T, E>(payload: &[u8]) -> Option<String>
where
    T: rkyv::Archive + std::fmt::Display,
    E: rkyv::Archive + std::fmt::Display,
    crate::types::WireEvent<T, E>: rkyv::Archive,
    <crate::types::WireEvent<T, E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        crate::types::WireEvent<T, E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <crate::types::WireEvent<T, E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    use crate::types::WireEvent;
    crate::transport::decode_aligned::<WireEvent<T, E>>(payload)
        .ok()
        .map(|event| match event {
            WireEvent::Next(value) | WireEvent::CompleteWith(value) => value.to_string(),
            WireEvent::Complete => "complete".to_owned(),
            WireEvent::Error(error) => format!("error: {error}"),
            WireEvent::RpcError(error) => format!("rpc error: {error}"),
        })
}

/// Like [`decode_response`], for a service whose success type is `()`.
///
/// `()` does not implement [`Display`](std::fmt::Display), so the unit case needs
/// its own decoder — many health-check methods return `Observable<(), E>`.
pub fn decode_response_unit<E>(payload: &[u8]) -> Option<String>
where
    E: rkyv::Archive + std::fmt::Display,
    crate::types::WireEvent<(), E>: rkyv::Archive,
    <crate::types::WireEvent<(), E> as rkyv::Archive>::Archived: rkyv::Deserialize<
        crate::types::WireEvent<(), E>,
        rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
    >,
    for<'a> <crate::types::WireEvent<(), E> as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    use crate::types::WireEvent;
    crate::transport::decode_aligned::<WireEvent<(), E>>(payload)
        .ok()
        .map(|event| match event {
            WireEvent::Next(()) | WireEvent::CompleteWith(()) => "()".to_owned(),
            WireEvent::Complete => "complete".to_owned(),
            WireEvent::Error(error) => format!("error: {error}"),
            WireEvent::RpcError(error) => format!("rpc error: {error}"),
        })
}

/// Turns the raw rkyv payload of one service into human-readable text.
///
/// The two directions are separate because a request and a response carry
/// different types (`{Service}Request` and `WireEvent<T, E>`).
pub trait ServiceDecoder: Send + Sync {
    /// Renders a request payload, given the method name carried by the header.
    fn request(&self, method: &str, payload: &[u8]) -> Option<String>;

    /// Renders a response payload, given the method of the matched request.
    fn response(&self, method: &str, payload: &[u8]) -> Option<String>;
}

/// Boxed renderer used by [`ClosureDecoder`].
type Render = Box<dyn Fn(&str, &[u8]) -> Option<String> + Send + Sync>;

/// A [`ServiceDecoder`] assembled from two closures.
pub struct ClosureDecoder {
    request: Render,
    response: Render,
}

impl ClosureDecoder {
    /// Builds a decoder from a request renderer and a response renderer.
    pub fn new(
        request: impl Fn(&str, &[u8]) -> Option<String> + Send + Sync + 'static,
        response: impl Fn(&str, &[u8]) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            request: Box::new(request),
            response: Box::new(response),
        }
    }
}

impl ServiceDecoder for ClosureDecoder {
    fn request(&self, method: &str, payload: &[u8]) -> Option<String> {
        (self.request)(method, payload)
    }

    fn response(&self, method: &str, payload: &[u8]) -> Option<String> {
        (self.response)(method, payload)
    }
}

/// Registry of the decoders known to an observer, keyed by service id.
///
/// The generated `{Trait}Decoder::register` populates it; an observer binary
/// builds one from the service definitions it links against.
#[derive(Default)]
pub struct Decoders {
    services: HashMap<u32, Arc<dyn ServiceDecoder>>,
}

impl Decoders {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) the decoder of `service_id`.
    pub fn register(&mut self, service_id: u32, decoder: Arc<dyn ServiceDecoder>) -> &mut Self {
        self.services.insert(service_id, decoder);
        self
    }

    /// Number of services with a registered decoder.
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Returns `true` when no decoder is registered.
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Renders a request payload; `None` without a decoder or on a decode failure.
    pub fn request(&self, service_id: u32, method: &str, payload: &[u8]) -> Option<String> {
        self.services.get(&service_id)?.request(method, payload)
    }

    /// Renders a response payload; `None` without a decoder or on a decode failure.
    pub fn response(&self, service_id: u32, method: &str, payload: &[u8]) -> Option<String> {
        self.services.get(&service_id)?.response(method, payload)
    }
}

impl fmt::Debug for Decoders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoders")
            .field("services", &self.services.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
    struct Reply {
        value: i32,
    }

    impl fmt::Display for Reply {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "value={}", self.value)
        }
    }

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
    enum Request {
        Add { a: i32, b: i32 },
    }

    impl fmt::Display for Request {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Request::Add { a, b } => write!(f, "add(a={a}, b={b})"),
            }
        }
    }

    fn encode<T>(value: &T) -> Vec<u8>
    where
        T: for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        rkyv::to_bytes::<rkyv::rancor::Error>(value)
            .expect("encode")
            .to_vec()
    }

    #[test]
    fn requests_and_responses_decode_through_display() {
        let request = encode(&Request::Add { a: 1, b: 2 });
        assert_eq!(
            decode_request::<Request>(&request).as_deref(),
            Some("add(a=1, b=2)")
        );

        let response = encode(&crate::types::WireEvent::<Reply, String>::Next(Reply {
            value: 7,
        }));
        assert_eq!(
            decode_response::<Reply, String>(&response).as_deref(),
            Some("value=7")
        );

        let unit = encode(&crate::types::WireEvent::<(), String>::Complete);
        assert_eq!(
            decode_response_unit::<String>(&unit).as_deref(),
            Some("complete")
        );
    }

    #[test]
    fn garbage_does_not_decode() {
        assert_eq!(decode_request::<Request>(&[0xff, 0x00]), None);
        assert_eq!(decode_response::<Reply, String>(&[0xff, 0x00]), None);
    }

    #[test]
    fn a_registered_decoder_renders_both_directions() {
        let mut decoders = Decoders::new();
        decoders.register(
            7,
            Arc::new(ClosureDecoder::new(
                |_method, payload| Some(String::from_utf8_lossy(payload).into_owned()),
                |_method, payload| Some(format!("{} bytes", payload.len())),
            )),
        );

        assert_eq!(decoders.request(7, "echo", b"hi").as_deref(), Some("hi"));
        assert_eq!(
            decoders.response(7, "echo", b"abcd").as_deref(),
            Some("4 bytes")
        );
        assert_eq!(decoders.request(9, "echo", b"hi"), None);
    }
}
