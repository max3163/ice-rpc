//! The generated rkyv ↔ `serde_json::Value` converters, exercised without a
//! Node.js host.
//!
//! These two functions are the whole contract between a service and the
//! `gateway_nodejs` bridge: the bridge calls them with the payload it received
//! and hands the result to JavaScript, then does the reverse with what JavaScript
//! returns. Nothing in this workspace executed them until now — they were only
//! type-checked, because the gateway itself is excluded from CI (it needs the
//! N-API toolchain).
//!
//! No JavaScript is needed to test them: they are pure functions over rkyv bytes
//! and `serde_json::Value`. The gateway's half of the contract — that it calls
//! them with the right method name — is covered by its own tests.

#![allow(missing_docs)] // test/example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use ice_rpc::gen::{
    decode_aligned, serde_json, EventKind, JsonCallError, JsonInvoker, ReadMode, WireEvent,
};
use ice_rpc::{Observable, ServiceInit};
use ice_rpc_macros::service;

/// A service with one argument of each shape the converter special-cases: an
/// owned `String` and a scalar.
#[service("ConverterDemo")]
#[async_trait::async_trait]
pub trait ConverterApi: Send + Sync + 'static {
    async fn echo(&self, text: String, count: i32) -> Observable<String, String>;
    async fn upload(&self, blob: Vec<u8>) -> Observable<(), String>;
}

/// A request built as the client would build it, then read back as JSON.
#[test]
fn a_request_decodes_to_a_json_object() {
    let request = ConverterApiRequest::Echo {
        text: "hello".to_owned(),
        count: 3,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request).unwrap();

    let value = ConverterApiProxy::deserialize_request_to_value("echo", &bytes)
        .expect("the request must convert");
    assert_eq!(value["text"], "hello");
    assert_eq!(value["count"], 3);
}

/// A `Vec<u8>` argument is transported as base64, not as a JSON array of
/// numbers: that is the one shape whose converter differs from the generic path.
///
/// It is also the shape of a **single-argument** method: the bridge receives the
/// value itself, not an object with one field. Two arguments or more become an
/// object, which is what the test above pins.
#[test]
fn a_vec_u8_argument_is_transported_as_base64() {
    let request = ConverterApiRequest::Upload {
        blob: vec![1, 2, 3, 4],
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request).unwrap();

    let value = ConverterApiProxy::deserialize_request_to_value("upload", &bytes)
        .expect("the request must convert");
    assert_eq!(
        value, "AQIDBA==",
        "4 bytes 1..4 are base64 'AQIDBA==', and a one-argument method sends the value itself"
    );
}

/// The unknown-method case must report `None` rather than panic: the bridge
/// ignores the call, it does not bring the process down.
#[test]
fn an_unknown_method_does_not_convert() {
    let request = ConverterApiRequest::Echo {
        text: "x".to_owned(),
        count: 0,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request).unwrap();

    assert!(ConverterApiProxy::deserialize_request_to_value("nope", &bytes).is_none());
    // Nor is a payload that is not a valid encoding of the service's request.
    assert!(ConverterApiProxy::deserialize_request_to_value("echo", &[0, 1, 2]).is_none());
}

/// Every terminal shape of a response, back to the wire variant the transport
/// expects — including the single-value `complete` form the bridge may send.
///
/// A **missing** `type` means `next`: the bridge may send a bare value, and the
/// converter reads it as an intermediate event rather than refusing it. That
/// default was in the generator but is pinned here for the first time.
#[test]
fn a_response_becomes_the_wire_event_it_announces() {
    let cases: [(&str, &str, WireEvent<String, String>); 5] = [
        (
            "next",
            r#"{"type":"next","data":"value"}"#,
            WireEvent::Next("value".to_owned()),
        ),
        (
            "complete",
            r#"{"type":"complete","data":"last"}"#,
            WireEvent::CompleteWith("last".to_owned()),
        ),
        // A `complete` without a payload closes the stream without a value.
        ("complete", r#"{"type":"complete"}"#, WireEvent::Complete),
        (
            "error",
            r#"{"type":"error","data":"boom"}"#,
            WireEvent::Error("boom".to_owned()),
        ),
        // No `type` at all: an intermediate value.
        (
            "untyped",
            r#"{"data":"bare"}"#,
            WireEvent::Next("bare".to_owned()),
        ),
    ];

    let expected_kinds = [
        EventKind::Next,
        EventKind::Complete,
        EventKind::Complete,
        EventKind::Error,
        EventKind::Next,
    ];

    for ((label, json, expected), expected_kind) in cases.into_iter().zip(expected_kinds) {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let (kind, bytes) = ConverterApiProxy::serialize_response_from_value("echo", value)
            .unwrap_or_else(|| panic!("{label} must convert"));

        assert_eq!(
            kind, expected_kind,
            "{json} must be labelled {expected_kind:?}"
        );
        let decoded = decode_aligned::<WireEvent<String, String>>(&bytes)
            .unwrap_or_else(|e| panic!("{label} must decode back: {e:?}"));
        assert_eq!(decoded, expected, "{json} must round-trip unchanged");
    }
}

/// A payload the bridge cannot interpret must be reported, not guessed: the
/// converter answers `None` and the caller decides what to do.
#[test]
fn an_unusable_response_does_not_convert() {
    for json in [
        r#"{"type":"unknown","data":1}"#,
        r#"{"type":"next"}"#,
        r#"{"type":"error"}"#,
        // No `type` and no `data`: nothing to read.
        r#"{}"#,
    ] {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert!(
            ConverterApiProxy::serialize_response_from_value("echo", value).is_none(),
            "{json} must not convert"
        );
    }

    // An unknown method, whatever the payload.
    let value: serde_json::Value = serde_json::from_str(r#"{"type":"next","data":"v"}"#).unwrap();
    assert!(ConverterApiProxy::serialize_response_from_value("nope", value).is_none());
}

/// The proxy of the JSON provider mode is the entry point the gateway registers;
/// it must exist and expose the service name, without a Node.js runtime.
#[test]
fn the_json_provider_mode_proxy_is_registrable() {
    let proxy = ConverterApiProxy::provide_json();
    assert_eq!(ConverterApiProxy::SERVICE_NAME, "ConverterDemo");
    let _: &dyn ServiceInit = &*proxy;
}

/// The single JSON view `gateway_nodejs` drives, exercised with no Node.js host
/// and no provider.
///
/// Built on a `ProviderJson` proxy on purpose: that mode answers without
/// touching the bus, so the contract can be checked in isolation.
///
/// What this pins is exactly what the hand-written table used to get wrong: the
/// served surface is the **declaration**, so a method that exists is served and a
/// name that is not declared is not. The reading mode travels as an argument, so
/// one table answers both entry points.
#[test]
fn the_generated_json_view_follows_the_declaration() {
    let proxy = ConverterApiProxy::provide_json();

    // A name that is not declared is reported as "no such method" (`None`), not
    // as a failure: the gateway turns that into `E_UNKNOWN_METHOD`.
    let unknown = ice_rpc::rt::test_block_on(proxy.invoke_json(
        "nope",
        serde_json::json!({}),
        ReadMode::First,
    ));
    assert!(unknown.is_none(), "an undeclared method must map to None");

    // Reading every value follows the same rule, from the same table.
    let unknown =
        ice_rpc::rt::test_block_on(proxy.invoke_json("nope", serde_json::json!({}), ReadMode::All));
    assert!(
        unknown.is_none(),
        "an undeclared method must map to None when every value is read too"
    );

    // Arguments that do not fit the declaration are refused as `InvalidArgs`,
    // and the message names the offending parameter. Decoding happens before the
    // call, so this never reaches the bus.
    let bad = ice_rpc::rt::test_block_on(proxy.invoke_json(
        "echo",
        serde_json::json!({"text": "hello", "count": "not-a-number"}),
        ReadMode::First,
    ));
    match bad.expect("a declared method must answer Some") {
        Err(JsonCallError::InvalidArgs(message)) => {
            assert!(
                message.contains("count"),
                "the message must name the field: {message}"
            );
        }
        other => panic!("invalid arguments must be reported as InvalidArgs: {other:?}"),
    }

    // A declared method is served even when the call itself cannot succeed here:
    // that is the difference the old table could not express, since an
    // undeclared-but-real method was indistinguishable from a typo.
    let declared = ice_rpc::rt::test_block_on(proxy.invoke_json(
        "upload",
        serde_json::json!("AQIDBA=="),
        ReadMode::First,
    ));
    assert!(
        declared.is_some(),
        "'upload' is declared and must be served"
    );
}
