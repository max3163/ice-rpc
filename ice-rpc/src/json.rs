//! The JSON contract of a service, in both directions.
//!
//! A service exposed to a JSON transport is described by **one** view, and that
//! view has two halves: the caller side, implemented by the `#[service]` macro on
//! the proxy, and the host side, implemented by whatever answers the calls.
//!
//! | Half | Types | Implemented by |
//! |---|---|---|
//! | caller | [`JsonInvoker`](crate::json::JsonInvoker), [`JsonOutcome`](crate::json::JsonOutcome), [`JsonCallError`](crate::json::JsonCallError) | `#[service]`, once per service |
//! | host | [`JsonDispatcher`](crate::json::JsonDispatcher), [`JsonCallStream`](crate::json::JsonCallStream), [`JsonCallEvent`](crate::json::JsonCallEvent) | the JSON host, registered by [`set_json_dispatcher`](crate::json::set_json_dispatcher) |
//!
//! The caller side has **one** entry point, [`JsonInvoker::invoke_json_stream`].
//! [`first_json`](crate::json::first_json) turns it into the unary answer the
//! transports also expose: it takes the first event and drops the call, which
//! cancels the rest of the stream remotely.
//!
//! **Nothing here names a transport.** The same expansion serves the Node.js
//! gateway (`gateway_nodejs`), the HTTP gateway — which consumes the very same
//! [`JsonInvoker`](crate::json::JsonInvoker) — a Rust test double, or a future
//! WASM host: they all register or consume the types below.

#[cfg(feature = "json")]
use rkyv::util::AlignedVec;

use crate::global::Global;
#[cfg(feature = "json")]
use crate::transport::PAYLOAD_ALIGNMENT;
use crate::types::CORRELATION_ID_LEN;
#[cfg(feature = "json")]
use crate::types::{EventKind, WireEvent};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

// ── Caller side: what a JSON transport calls a service through ───────────────

/// What a JSON call produced.
///
/// Like [`JsonCallError`], this is a closed vocabulary on the 0.4 line: the three
/// outcomes are the contract every JSON transport maps to its own answer (a body,
/// an empty response, an accepted notification), so a new outcome would be a
/// breaking change rather than an additive one.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonOutcome {
    /// The first value of the call, as a JSON value.
    Value(serde_json::Value),
    /// The service completed **without emitting any value**. A legitimate result
    /// that a bare `null` could not be told apart from a real `null` payload —
    /// and the very difference the two JSON transports used to disagree on.
    Nothing,
    /// The method is **one-way**: the call was published and *no answer will
    /// ever exist*. Not to be confused with [`JsonOutcome::Nothing`], which is
    /// an answered call that carried no value.
    ///
    /// `delivered` reports whether at least one subscriber took the sample. It
    /// is a **local** observation of the emitter (no provider, or a full buffer,
    /// answers `false`), never an acknowledgement that the service processed the
    /// value — the JSON transports map it to `202 Accepted`, which is exactly
    /// that promise.
    Accepted {
        /// Whether at least one subscriber received the published sample.
        delivered: bool,
    },
}

/// Why a JSON-mediated call failed.
///
/// The three variants are the **stable vocabulary** every JSON transport maps to
/// its own codes — an HTTP status, a Node.js `E_*` code — so the enum is
/// deliberately closed on the 0.4 line: adding a variant would break a
/// downstream `match`, and a new failure class needs a transport code of its own
/// anyway. `#[non_exhaustive]` belongs to a release that may break, never to a
/// 0.4.x patch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonCallError {
    /// A parameter could not be decoded into the declared Rust type.
    InvalidArgs(String),
    /// The service answered with a business error.
    Business(String),
    /// The transport failed, or the host never answered.
    Transport(String),
}

impl std::fmt::Display for JsonCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The payload already carries a complete sentence, and this mirrors
            // `ObservableError`'s rendering: the category belongs to the variant,
            // which a transport maps to its own code, not to the message.
            Self::InvalidArgs(message) | Self::Business(message) | Self::Transport(message) => {
                f.write_str(message)
            }
        }
    }
}

/// What [`first_json`] returns.
pub type JsonResult = Result<JsonOutcome, JsonCallError>;

/// What a streamed call helper returns before its terminal event is reported.
pub type JsonStreamResult = Result<(), JsonCallError>;

/// Factory creating the JSON view of a service.
///
/// The generated `consume()` constructor answers without a provider, so a
/// gateway exposes the service from startup and reports a missing provider per
/// call instead of making the route disappear.
pub type JsonInvokerFactory = fn() -> std::sync::Arc<dyn JsonInvoker>;

/// One event of a streamed JSON call.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum JsonStreamEvent {
    /// One intermediate value.
    Next(serde_json::Value),
    /// The call completed successfully without a final value.
    Complete,
    /// The method is one-way: the sample was published, nothing will follow.
    Accepted {
        /// Whether at least one subscriber received the published sample.
        delivered: bool,
    },
    /// The call failed; this is terminal.
    Failed(JsonCallError),
}

/// Bounded event sink of one streamed call.
///
/// Dropping the receiver is the cancellation signal: the next [`Self::send`]
/// returns `false`, and the producer must drop the underlying `Observable`.
#[derive(Clone)]
pub struct JsonEventSink {
    sender: async_channel::Sender<JsonStreamEvent>,
}

impl JsonEventSink {
    /// Builds a sink with a bounded capacity and the receiver the transport reads.
    #[must_use]
    pub fn bounded(capacity: usize) -> (Self, async_channel::Receiver<JsonStreamEvent>) {
        // `async_channel::bounded(0)` is invalid; a sink always buffers one event.
        let (sender, receiver) = async_channel::bounded(capacity.max(1));
        (Self { sender }, receiver)
    }

    /// Pushes one event. Returns `false` when the receiver is gone.
    #[must_use]
    pub async fn send(&self, event: JsonStreamEvent) -> bool {
        self.sender.send(event).await.is_ok()
    }

    /// Pushes a terminal failure. Returns `false` when the receiver is gone.
    #[must_use]
    pub async fn fail(&self, error: JsonCallError) -> bool {
        self.send(JsonStreamEvent::Failed(error)).await
    }
}

/// The JSON view of a service, from the caller's side.
///
/// Implemented automatically, **once**, by the `#[service]` macro, whatever the
/// JSON transport: the Node.js gateway and the HTTP gateway both consume this
/// trait, so neither adds generated code of its own. A Rust-to-Rust call does
/// not go through it at all — it calls the typed method.
///
/// # Example
///
/// ```rust,ignore
/// let invoker: Arc<dyn JsonInvoker> = ...;
/// match first_json(&*invoker, "get_user_age", json!({"name": "Alice"})).await {
///     Some(Ok(JsonOutcome::Value(age))) => println!("{age}"),
///     Some(Ok(JsonOutcome::Nothing)) => println!("completed without a value"),
///     Some(Err(error)) => eprintln!("{error:?}"),
///     None => eprintln!("no such method"),
/// }
/// ```
#[async_trait::async_trait]
pub trait JsonInvoker: Send + Sync {
    /// Returns the logical name of the service.
    fn service_name(&self) -> &'static str;

    /// Streams the events of one call into `sink`.
    ///
    /// The implementation generated by `#[service]` emits one event per
    /// `Observable` event. A transport that wants a unary answer calls
    /// [`first_json`] instead of reading this stream by hand.
    ///
    /// Returns `None` when the service has no such method. A failure is reported
    /// through the sink as [`JsonStreamEvent::Failed`], not as a return value.
    async fn invoke_json_stream(
        &self,
        method: &str,
        args: serde_json::Value,
        sink: JsonEventSink,
    ) -> Option<()>;
}
// ── Host side: what a JSON host implements to answer ─────────────────────────

/// How long the **first** event of a JSON call may take to arrive.
///
/// Guards the case that has no other bound: a host that never answers. Once
/// events flow, the call belongs to the service, which is why the deadline is
/// not re-armed per event — a long-lived stream is not a timeout.
pub const JSON_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// One step of a call to a JSON host, as read by the generated dispatcher.
#[derive(Debug)]
pub enum JsonCallEvent {
    /// An event pushed by the host.
    Event(Value),
    /// The host never answered. Reported on the bus as an internal error rather
    /// than silently ending the stream, which would look like a `Complete`.
    Failed(String),
}

/// The events of one call to a JSON host.
///
/// Built by the host ([`JsonCallStream::channel`]) and consumed by the generated
/// dispatcher, which emits one wire sample per event.
pub struct JsonCallStream {
    receiver: async_channel::Receiver<Value>,
    answered: bool,
}

impl JsonCallStream {
    /// Builds a stream together with the sender the host pushes events into.
    #[must_use]
    pub fn channel() -> (Self, async_channel::Sender<Value>) {
        let (sender, receiver) = async_channel::unbounded();
        (
            Self {
                receiver,
                answered: false,
            },
            sender,
        )
    }

    /// Returns the next event, or `None` once the host closed the call.
    pub async fn next(&mut self) -> Option<JsonCallEvent> {
        if self.answered {
            return self.receiver.recv().await.ok().map(JsonCallEvent::Event);
        }
        match crate::rt::timeout(JSON_CALL_TIMEOUT, self.receiver.recv()).await {
            Ok(Ok(value)) => {
                self.answered = true;
                Some(JsonCallEvent::Event(value))
            }
            // The sender was dropped without pushing anything: the call is over.
            Ok(Err(_)) => None,
            Err(_) => Some(JsonCallEvent::Failed(format!(
                "the JSON host did not answer within {}s",
                JSON_CALL_TIMEOUT.as_secs()
            ))),
        }
    }
}

/// The JSON view of a service, from the host's side.
///
/// Implemented by whatever hosts a JavaScript-like implementation of the
/// service; registered once, at startup, with [`set_json_dispatcher`].
pub trait JsonDispatcher: Send + Sync + 'static {
    /// Hands one call to the host and returns the stream of its events.
    ///
    /// Synchronous on purpose: [`JsonCallStream`] carries the asynchronous
    /// events, so a host that answers later returns the stream immediately and
    /// pushes into it. This keeps the provider handler free of a boxed future
    /// per call, which is what `async_trait` used to add.
    ///
    /// # Errors
    /// `Err` when the call could not even be started (no callback, host gone):
    /// the generated dispatcher reports it as an internal error.
    fn dispatch_json(
        &self,
        cid: [u8; CORRELATION_ID_LEN],
        service: &str,
        method: &str,
        args: Value,
    ) -> Result<JsonCallStream, String>;
}

static DISPATCHER: Global<Arc<dyn JsonDispatcher>> = Global::new();

/// Registers the JSON host. Called once, at startup.
///
/// Subsequent calls are ignored, so a second `init` cannot swap the host under
/// running calls.
pub fn set_json_dispatcher(dispatcher: Arc<dyn JsonDispatcher>) {
    DISPATCHER.get_or_init(|| dispatcher);
}

/// The registered host, if any.
pub fn json_dispatcher() -> Option<&'static Arc<dyn JsonDispatcher>> {
    DISPATCHER.get()
}

/// Hands a call to the registered host.
///
/// # Errors
/// Returns `Err` when no host was registered; the generated dispatcher logs and
/// reports it rather than dropping the call.
pub fn dispatch_json(
    cid: [u8; CORRELATION_ID_LEN],
    service: &str,
    method: &str,
    args: Value,
) -> Result<JsonCallStream, String> {
    match DISPATCHER.get() {
        Some(dispatcher) => dispatcher.dispatch_json(cid, service, method, args),
        None => Err("no JSON dispatcher registered — call \
                     ice_rpc::json::set_json_dispatcher() first"
            .to_string()),
    }
}

/// Reads the **first** event of a call, then cancels the rest.
///
/// This is the unary view every JSON transport exposes, built on the single
/// stream entry point: the call writes into a sink of capacity one, and the
/// first event — a value, a completion, an accepted notification or a failure —
/// is returned. The call future is then dropped, which drops the service stream
/// and publishes the remote cancellation; a multi-value or endless stream
/// therefore stops after its first event.
///
/// Returns `None` when the service has no such method.
pub async fn first_json(
    invoker: &dyn JsonInvoker,
    method: &str,
    args: Value,
) -> Option<JsonResult> {
    use std::future::Future as _;
    use std::task::Poll;

    let (sink, receiver) = JsonEventSink::bounded(1);
    let mut call = invoker.invoke_json_stream(method, args, sink);
    // Polling the call first lets a synchronous answer queue its event before
    // the receiver is examined; taking that event then drops the call.
    let mut recv = std::pin::pin!(receiver.recv());
    let first = futures_lite::future::poll_fn(|cx| {
        let call_ready = call.as_mut().poll(cx).is_ready();
        match recv.as_mut().poll(cx) {
            Poll::Ready(Ok(event)) => Poll::Ready(Some(event)),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            // The producer finished without leaving an event: no such method.
            Poll::Pending if call_ready => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    })
    .await;

    // Dropping the call drops the underlying `Observable`, which publishes the
    // cancellation of the rest of the stream.
    drop(call);

    match first? {
        JsonStreamEvent::Next(value) => Some(Ok(JsonOutcome::Value(value))),
        JsonStreamEvent::Complete => Some(Ok(JsonOutcome::Nothing)),
        JsonStreamEvent::Accepted { delivered } => Some(Ok(JsonOutcome::Accepted { delivered })),
        JsonStreamEvent::Failed(error) => Some(Err(error)),
    }
}

/// Reads an `Observable` as a stream of JSON events.
///
/// A `false` returned by the sink means the client is gone: the stream is
/// dropped, which publishes the remote cancellation. A failure is returned to
/// the caller, which reports it as [`JsonStreamEvent::Failed`].
pub async fn stream_json<T, E>(
    mut stream: crate::Observable<T, E>,
    sink: JsonEventSink,
) -> Result<(), JsonCallError>
where
    T: serde::Serialize,
    E: std::fmt::Display,
{
    loop {
        match stream.recv().await {
            Ok(crate::Event::Next(value)) => {
                let value = serde_json::to_value(value).map_err(|e| {
                    JsonCallError::Transport(format!("failed to serialize the response: {e}"))
                })?;
                if !sink.send(JsonStreamEvent::Next(value)).await {
                    // The receiver is gone: dropping the stream publishes Cancel.
                    return Ok(());
                }
            }
            Ok(crate::Event::Complete) => {
                let _ = sink.send(JsonStreamEvent::Complete).await;
                return Ok(());
            }
            Ok(crate::Event::Error(crate::ObservableError::Business(e))) => {
                return Err(JsonCallError::Business(e.to_string()));
            }
            Ok(crate::Event::Error(crate::ObservableError::Technical(e))) => {
                return Err(JsonCallError::Transport(e.to_string()));
            }
            Ok(crate::Event::Error(crate::ObservableError::Empty)) => {
                // A pull-side artefact, never authoritative: treat it as a close.
                let _ = sink.send(JsonStreamEvent::Complete).await;
                return Ok(());
            }
            Err(_) => {
                return Err(JsonCallError::Transport(
                    "stream closed before completion".to_string(),
                ));
            }
        }
    }
}

// ── Reading one argument, shared by every generated helper ──────────────────

/// Decodes one argument of a call.
///
/// Generic over the argument type — the one thing the expansion knows that this
/// function does not — so a generated helper carries one line per parameter and
/// the failure message is formatted in a single place.
#[cfg(feature = "json")]
pub fn arg_from_json<T>(method: &str, name: &str, value: Value) -> Result<T, JsonCallError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value).map_err(|e| {
        JsonCallError::InvalidArgs(format!("invalid parameter '{name}' of '{method}': {e}"))
    })
}

/// Same, for a `Vec<u8>` argument, which travels as base64.
///
/// Takes the `Option` the caller already has: a missing field and a field of the
/// wrong type are the same failure for the caller of a JSON method.
#[cfg(feature = "json")]
pub fn arg_from_base64(
    method: &str,
    name: &str,
    value: Option<&Value>,
) -> Result<Vec<u8>, JsonCallError> {
    use base64::Engine;
    let text = value.and_then(Value::as_str).ok_or_else(|| {
        JsonCallError::InvalidArgs(format!(
            "parameter '{name}' of '{method}' must be a base64 string"
        ))
    })?;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|e| {
            JsonCallError::InvalidArgs(format!(
                "parameter '{name}' of '{method}' is not valid base64: {e}"
            ))
        })
}

/// Moves one field out of the argument object.
///
/// A missing field becomes `Null`, which the decoding reports as an invalid
/// parameter: one error path for "absent" and "wrong shape", which a JSON caller
/// cannot tell apart anyway. Moving, not cloning, keeps a large subtree from being
/// copied just to be read.
#[cfg(feature = "json")]
pub fn take_field(args: &mut Value, name: &str) -> Value {
    args.get_mut(name)
        .map(std::mem::take)
        .unwrap_or(Value::Null)
}

/// Builds and serializes the wire event a JSON host asked for.
///
/// The host answers with `{ "type": "next"|"complete"|"error", "data": … }`; this
/// is the whole conversion, generic over the service's two result types. That is
/// what keeps the generated `serialize_response_from_value` to one line per
/// method instead of repeating the envelope dispatch in every arm.
///
/// `None` means the envelope or its `data` does not match the declaration; the
/// handler then reports `SerializationError` to the caller rather than leaving the
/// call unanswered.
///
/// # Bounds
/// `T` and `E` must be JSON-deserializable — the same requirement the generated
/// converters already impose through `serde` — and their `WireEvent` must be
/// rkyv-serializable, which `#[payload]` guarantees.
#[cfg(feature = "json")]
pub fn event_from_json<T, E>(value: Value) -> Option<(EventKind, AlignedVec<PAYLOAD_ALIGNMENT>)>
where
    T: serde::de::DeserializeOwned,
    E: serde::de::DeserializeOwned,
    // The exact strategy `rkyv::to_bytes` uses, as in `observable_to_responses`.
    for<'a> WireEvent<T, E>: rkyv::Serialize<
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
    // `object` is owned: the payload is moved into `from_value`, never cloned.
    let Value::Object(mut object) = value else {
        return None;
    };
    // A `&'static str` keeps the borrow from crossing the `remove` calls below.
    let event_type: &'static str = match object.get("type").and_then(|v| v.as_str()) {
        Some("complete") => "complete",
        Some("error") => "error",
        Some("next") | None => "next",
        Some(_) => return None,
    };
    let event: WireEvent<T, E> = match event_type {
        "next" => WireEvent::Next(serde_json::from_value(object.remove("data")?).ok()?),
        "complete" => match object.remove("data") {
            Some(data) => WireEvent::CompleteWith(serde_json::from_value(data).ok()?),
            None => WireEvent::Complete,
        },
        "error" => WireEvent::Error(serde_json::from_value(object.remove("data")?).ok()?),
        _ => return None,
    };
    let kind = event.kind();
    // Return the `AlignedVec` itself: the emitter accepts it via `&[u8]`, no copy.
    rkyv::to_bytes::<rkyv::rancor::Error>(&event)
        .ok()
        .map(|aligned| (kind, aligned))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_error_renders_its_message() {
        // `Display` is the transport-neutral rendering: the category stays on
        // the variant, the text is what the decoding or the service produced.
        for (error, message) in [
            (
                JsonCallError::InvalidArgs(String::from("bad argument")),
                "bad argument",
            ),
            (JsonCallError::Business(String::from("denied")), "denied"),
            (
                JsonCallError::Transport(String::from("link down")),
                "link down",
            ),
        ] {
            assert_eq!(error.to_string(), message);
        }
    }

    /// A minimal host: echoes the arguments as one event.
    struct EchoHost;

    impl JsonDispatcher for EchoHost {
        fn dispatch_json(
            &self,
            _cid: [u8; CORRELATION_ID_LEN],
            service: &str,
            method: &str,
            args: Value,
        ) -> Result<JsonCallStream, String> {
            // Answered before returning: a spawned answer would need an executor
            // outliving the call, which is not what this test covers.
            let (stream, sender) = JsonCallStream::channel();
            let _ = sender.try_send(serde_json::json!({
                "service": service,
                "method": method,
                "args": args,
            }));
            // Dropping the sender is what closes the call.
            drop(sender);
            Ok(stream)
        }
    }

    #[test]
    fn a_registered_host_receives_the_call_and_its_events_are_read() {
        set_json_dispatcher(Arc::new(EchoHost));

        // `test_block_on`: the armed deadline needs a runtime for the whole poll.
        let events = crate::rt::test_block_on(async {
            let mut stream = dispatch_json(
                [7u8; CORRELATION_ID_LEN],
                "DatabaseService",
                "get_user_age",
                serde_json::json!({ "name": "Alice" }),
            )
            .expect("a registered host accepts the call");

            let mut collected = Vec::new();
            while let Some(event) = stream.next().await {
                match event {
                    JsonCallEvent::Event(value) => collected.push(value),
                    JsonCallEvent::Failed(message) => panic!("unexpected failure: {message}"),
                }
            }
            collected
        });

        assert_eq!(
            events,
            vec![serde_json::json!({
                "service": "DatabaseService",
                "method": "get_user_age",
                "args": { "name": "Alice" },
            })]
        );

        // The registry is process-wide: only this test asserts which host it holds.
        let registered = json_dispatcher().expect("a registered host stays reachable");
        assert_eq!(
            Arc::strong_count(registered),
            1,
            "the framework holds the single reference to the host"
        );

        // A second registration is ignored, so `init` cannot swap the host.
        set_json_dispatcher(Arc::new(EchoHost));
        assert_eq!(
            Arc::strong_count(json_dispatcher().expect("the first host is still there")),
            1,
            "the second host is dropped, the first one is kept"
        );
    }

    #[test]
    fn a_stream_whose_sender_is_dropped_ends_without_an_event() {
        let (mut stream, sender) = JsonCallStream::channel();
        drop(sender);
        assert!(
            crate::rt::test_block_on(stream.next()).is_none(),
            "a dropped sender ends the stream"
        );
    }

    /// A stream of `i32` values, ending as the given events say.
    fn values(
        events: impl IntoIterator<Item = crate::Event<i32, String>>,
    ) -> crate::Observable<i32, String> {
        crate::Observable::from_events(events)
    }

    /// Drains every event a sink receiver holds, then stops at the close.
    fn drain(receiver: async_channel::Receiver<JsonStreamEvent>) -> Vec<JsonStreamEvent> {
        crate::rt::test_block_on(async move {
            let mut events = Vec::new();
            while let Ok(event) = receiver.recv().await {
                events.push(event);
            }
            events
        })
    }

    /// An invoker whose stream view replays a fixed script.
    struct ScriptedInvoker {
        events: Vec<JsonStreamEvent>,
    }

    #[async_trait::async_trait]
    impl JsonInvoker for ScriptedInvoker {
        fn service_name(&self) -> &'static str {
            "ScriptedService"
        }

        async fn invoke_json_stream(
            &self,
            _method: &str,
            _args: Value,
            sink: JsonEventSink,
        ) -> Option<()> {
            for event in &self.events {
                if !sink.send(event.clone()).await {
                    return Some(());
                }
            }
            Some(())
        }
    }

    /// An invoker whose stream never completes: it keeps sending after the first
    /// value, so a caller that returns would leave its future parked forever.
    struct EndlessInvoker;

    #[async_trait::async_trait]
    impl JsonInvoker for EndlessInvoker {
        fn service_name(&self) -> &'static str {
            "EndlessService"
        }

        async fn invoke_json_stream(
            &self,
            _method: &str,
            _args: Value,
            sink: JsonEventSink,
        ) -> Option<()> {
            if !sink.send(JsonStreamEvent::Next(serde_json::json!(1))).await {
                return Some(());
            }
            loop {
                if !sink.send(JsonStreamEvent::Next(serde_json::json!(2))).await {
                    return Some(());
                }
            }
        }
    }

    /// An invoker with no such method.
    struct NoMethod;

    #[async_trait::async_trait]
    impl JsonInvoker for NoMethod {
        fn service_name(&self) -> &'static str {
            "NoMethodService"
        }

        async fn invoke_json_stream(
            &self,
            _method: &str,
            _args: Value,
            _sink: JsonEventSink,
        ) -> Option<()> {
            None
        }
    }

    #[test]
    fn first_json_takes_the_first_value_and_stops_the_stream() {
        // The second value is never consumed: the call is dropped after the first
        // event, which is what cancels the rest of the stream.
        let invoker = ScriptedInvoker {
            events: vec![
                JsonStreamEvent::Next(serde_json::json!(1)),
                JsonStreamEvent::Next(serde_json::json!(2)),
                JsonStreamEvent::Complete,
            ],
        };

        assert_eq!(
            crate::rt::test_block_on(first_json(&invoker, "watch", Value::Null)),
            Some(Ok(JsonOutcome::Value(serde_json::json!(1))))
        );
    }

    #[test]
    fn first_json_returns_from_an_endless_stream() {
        assert_eq!(
            crate::rt::test_block_on(first_json(&EndlessInvoker, "watch", Value::Null)),
            Some(Ok(JsonOutcome::Value(serde_json::json!(1)))),
            "the first event must answer without waiting for the stream to end"
        );
    }

    #[test]
    fn first_json_reports_a_completion_without_a_value() {
        let invoker = ScriptedInvoker {
            events: vec![JsonStreamEvent::Complete],
        };

        assert_eq!(
            crate::rt::test_block_on(first_json(&invoker, "empty", Value::Null)),
            Some(Ok(JsonOutcome::Nothing))
        );
    }

    #[test]
    fn first_json_reports_an_accepted_notification() {
        let invoker = ScriptedInvoker {
            events: vec![JsonStreamEvent::Accepted { delivered: true }],
        };

        assert_eq!(
            crate::rt::test_block_on(first_json(&invoker, "notify", Value::Null)),
            Some(Ok(JsonOutcome::Accepted { delivered: true }))
        );
    }

    #[test]
    fn first_json_reports_a_failure() {
        let invoker = ScriptedInvoker {
            events: vec![JsonStreamEvent::Failed(JsonCallError::Business(
                String::from("denied"),
            ))],
        };

        assert_eq!(
            crate::rt::test_block_on(first_json(&invoker, "fail", Value::Null)),
            Some(Err(JsonCallError::Business(String::from("denied"))))
        );
    }

    #[test]
    fn first_json_returns_none_for_an_unknown_method() {
        assert_eq!(
            crate::rt::test_block_on(first_json(&NoMethod, "nope", Value::Null)),
            None
        );
    }

    #[test]
    fn stream_json_emits_each_value_then_complete() {
        let (sink, receiver) = JsonEventSink::bounded(4);
        let stream = values([
            crate::Event::Next(1),
            crate::Event::Next(2),
            crate::Event::Complete,
        ]);

        crate::rt::test_block_on(stream_json(stream, sink)).expect("the stream completes");

        assert_eq!(
            drain(receiver),
            vec![
                JsonStreamEvent::Next(serde_json::json!(1)),
                JsonStreamEvent::Next(serde_json::json!(2)),
                JsonStreamEvent::Complete,
            ]
        );
    }

    #[test]
    fn stream_json_maps_business_and_technical_errors() {
        let (business_sink, _business_receiver) = JsonEventSink::bounded(1);
        let business = values([crate::Event::Error(crate::ObservableError::Business(
            String::from("denied"),
        ))]);
        assert_eq!(
            crate::rt::test_block_on(stream_json(business, business_sink)),
            Err(JsonCallError::Business(String::from("denied")))
        );

        let (technical_sink, _technical_receiver) = JsonEventSink::bounded(1);
        let technical = values([crate::Event::Error(crate::ObservableError::Technical(
            crate::RpcError::TransportError(String::from("link down")),
        ))]);
        assert!(
            matches!(
                crate::rt::test_block_on(stream_json(technical, technical_sink)),
                Err(JsonCallError::Transport(message)) if message.contains("link down")
            ),
            "a technical error is a transport failure"
        );
    }

    #[test]
    fn stream_json_stops_when_the_receiver_is_dropped() {
        let (sink, receiver) = JsonEventSink::bounded(1);
        drop(receiver);

        let stream = values([
            crate::Event::Next(1),
            crate::Event::Next(2),
            crate::Event::Complete,
        ]);

        assert_eq!(
            crate::rt::test_block_on(stream_json(stream, sink)),
            Ok(()),
            "a dropped receiver ends the stream without an error"
        );
    }

    #[test]
    fn stream_json_reports_an_unserializable_value() {
        use std::collections::HashMap;

        let (sink, _receiver) = JsonEventSink::bounded(1);
        let failing = crate::Observable::<HashMap<Vec<u8>, i32>, String>::from_events([
            crate::Event::Next(HashMap::from([(vec![1, 2, 3], 2)])),
            crate::Event::Complete,
        ]);

        assert!(
            matches!(
                crate::rt::test_block_on(stream_json(failing, sink)),
                Err(JsonCallError::Transport(message))
                    if message.contains("failed to serialize the response")
            ),
            "a value JSON cannot represent is a transport failure"
        );
    }
}
