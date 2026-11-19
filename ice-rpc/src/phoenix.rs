//! Codec for the Phoenix Channels wire protocol.
//!
//! This module owns the envelope only: the framing version selected by the
//! `vsn` query parameter, decoding of an inbound V1 object or V2 array, and
//! encoding of the outbound frames. Routing, joins, heartbeats and the RPC
//! semantics live in [`crate::channels_gateway`].
//!
//! It exists as a separate module because `trillium-channels` 0.4.0 cannot be
//! used for this job: it pins `trillium-websockets` 0.7, which drops the
//! upgrade query string, so it always negotiates V1 and silently ignores the
//! V2 array frames that the official Phoenix JavaScript client sends. Keeping
//! the envelope here, on top of `trillium-websockets` 0.9, is a small and
//! testable protocol layer rather than a replacement for the channel crate.

use serde_json::{Value, json};

/// Phoenix framing version negotiated through the `vsn` query parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhoenixVersion {
    /// Object framing, used by the first Phoenix clients.
    V1,
    /// Array framing `[join_ref, ref, topic, event, payload]`.
    V2,
}

/// One inbound Phoenix message, decoded from either framing.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PhoenixFrame {
    /// Reference of the join this message belongs to, if any.
    pub(crate) join_ref: Option<String>,
    /// Reference the client expects in the reply, if any.
    pub(crate) reference: Option<String>,
    /// Channel topic the message is addressed to.
    pub(crate) topic: String,
    /// Event name, such as `phx_join`, `call` or `cancel`.
    pub(crate) event: String,
    /// Event payload.
    pub(crate) payload: Value,
}

impl PhoenixFrame {
    /// Parses a V2 array or a V1 object.
    ///
    /// Returns `None` when the text is not valid JSON or does not carry the
    /// five fields of a Phoenix message with a string topic and event.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(text).ok()?;
        match value {
            Value::Array(mut items) if items.len() == 5 => {
                let payload = items.pop()?;
                let event = items.pop()?.as_str()?.to_owned();
                let topic = items.pop()?.as_str()?.to_owned();
                let reference = items.pop().and_then(reference_value);
                let join_ref = items.pop().and_then(reference_value);
                Some(Self {
                    join_ref,
                    reference,
                    topic,
                    event,
                    payload,
                })
            }
            Value::Object(mut object) => Some(Self {
                topic: object.remove("topic")?.as_str()?.to_owned(),
                event: object.remove("event")?.as_str()?.to_owned(),
                payload: object.remove("payload").unwrap_or(Value::Null),
                join_ref: object.remove("join_ref").and_then(reference_value),
                reference: object.remove("ref").and_then(reference_value),
            }),
            _ => None,
        }
    }

    /// Encodes the `phx_reply` answering this frame.
    pub(crate) fn reply(&self, version: PhoenixVersion, status: &str, response: &Value) -> String {
        encode(
            version,
            self.join_ref.as_deref(),
            self.reference.as_deref(),
            &self.topic,
            "phx_reply",
            &json!({ "status": status, "response": response }),
        )
    }
}

/// Reads a `ref` or `join_ref`.
///
/// The V2 JavaScript client sends them as strings, while some V1
/// implementations send numbers. Both are accepted so the gateway can use the
/// Phoenix reference as its correlation id without depending on the encoding.
fn reference_value(value: Value) -> Option<String> {
    match value {
        Value::String(reference) => Some(reference),
        Value::Number(reference) => Some(reference.to_string()),
        _ => None,
    }
}

/// Encodes one outbound frame in the negotiated framing.
pub(crate) fn encode(
    version: PhoenixVersion,
    join_ref: Option<&str>,
    reference: Option<&str>,
    topic: &str,
    event: &str,
    payload: &Value,
) -> String {
    let value = match version {
        PhoenixVersion::V2 => json!([join_ref, reference, topic, event, payload]),
        PhoenixVersion::V1 => json!({
            "topic": topic,
            "event": event,
            "payload": payload,
            "ref": reference,
            "join_ref": join_ref,
        }),
    };
    value.to_string()
}

/// Reads the framing version from the WebSocket upgrade query string.
///
/// The official Phoenix JavaScript client always sends `vsn=2.0.0`. A missing
/// `vsn` is treated as V2, because only a non-Phoenix client can omit it; any
/// other value keeps the historical V1 interpretation.
pub(crate) fn version_from_query(query: &str) -> PhoenixVersion {
    match query_param(query, "vsn") {
        Some(version) if version.starts_with('2') => PhoenixVersion::V2,
        Some(_) => PhoenixVersion::V1,
        None => PhoenixVersion::V2,
    }
}

/// Returns one query-string parameter, without percent-decoding.
pub(crate) fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then_some(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_v2_frame_is_an_array() {
        let frame =
            PhoenixFrame::parse(r#"["1","2","rpc","call",{"id":"a"}]"#).expect("a V2 frame parses");

        assert_eq!(frame.join_ref.as_deref(), Some("1"));
        assert_eq!(frame.reference.as_deref(), Some("2"));
        assert_eq!(frame.topic, "rpc");
        assert_eq!(frame.event, "call");
        assert_eq!(frame.payload["id"], "a");
    }

    #[test]
    fn a_v1_frame_is_an_object() {
        let frame = PhoenixFrame::parse(
            r#"{"topic":"rpc","event":"call","payload":{"id":"a"},"ref":"2","join_ref":"1"}"#,
        )
        .expect("a V1 frame parses");

        assert_eq!(frame.join_ref.as_deref(), Some("1"));
        assert_eq!(frame.reference.as_deref(), Some("2"));
        assert_eq!(frame.topic, "rpc");
        assert_eq!(frame.event, "call");
        assert_eq!(frame.payload["id"], "a");
    }

    #[test]
    fn an_incomplete_frame_is_refused() {
        assert!(PhoenixFrame::parse("not json").is_none());
        assert!(PhoenixFrame::parse(r#"["1","2","rpc"]"#).is_none());
        assert!(PhoenixFrame::parse(r#"{"topic":"rpc"}"#).is_none());
        assert!(PhoenixFrame::parse(r#"["1","2",3,"call",{}]"#).is_none());
    }

    #[test]
    fn numeric_references_are_accepted() {
        let v2 = PhoenixFrame::parse(r#"[1,2,"Database","call",{}]"#)
            .expect("a V2 frame with numeric references parses");
        assert_eq!(v2.join_ref.as_deref(), Some("1"));
        assert_eq!(v2.reference.as_deref(), Some("2"));

        let v1 = PhoenixFrame::parse(
            r#"{"topic":"Database","event":"call","payload":{},"ref":2,"join_ref":1}"#,
        )
        .expect("a V1 frame with numeric references parses");
        assert_eq!(v1.join_ref.as_deref(), Some("1"));
        assert_eq!(v1.reference.as_deref(), Some("2"));
    }

    #[test]
    fn a_v2_reply_is_an_array_frame() {
        let frame = PhoenixFrame {
            join_ref: Some("1".to_string()),
            reference: Some("2".to_string()),
            topic: "rpc".to_string(),
            event: "call".to_string(),
            payload: json!({}),
        };

        let value: Value = serde_json::from_str(&frame.reply(
            PhoenixVersion::V2,
            "ok",
            &json!({ "kind": "value", "data": 7 }),
        ))
        .expect("the encoded reply is valid JSON");

        assert_eq!(value[0].as_str(), Some("1"));
        assert_eq!(value[1].as_str(), Some("2"));
        assert_eq!(value[2].as_str(), Some("rpc"));
        assert_eq!(value[3].as_str(), Some("phx_reply"));
        assert_eq!(value[4]["status"].as_str(), Some("ok"));
        assert_eq!(value[4]["response"]["data"].as_u64(), Some(7));
    }

    #[test]
    fn a_v1_reply_is_an_object_frame() {
        let frame = PhoenixFrame {
            join_ref: Some("1".to_string()),
            reference: Some("2".to_string()),
            topic: "rpc".to_string(),
            event: "call".to_string(),
            payload: json!({}),
        };

        let value: Value = serde_json::from_str(&frame.reply(
            PhoenixVersion::V1,
            "error",
            &json!({ "kind": "E_LIMIT", "message": "too many" }),
        ))
        .expect("the encoded reply is valid JSON");

        assert_eq!(value["topic"].as_str(), Some("rpc"));
        assert_eq!(value["event"].as_str(), Some("phx_reply"));
        assert_eq!(value["ref"].as_str(), Some("2"));
        assert_eq!(value["join_ref"].as_str(), Some("1"));
        assert_eq!(value["payload"]["status"].as_str(), Some("error"));
        assert_eq!(
            value["payload"]["response"]["kind"].as_str(),
            Some("E_LIMIT")
        );
    }

    #[test]
    fn the_protocol_version_comes_from_the_query() {
        assert_eq!(version_from_query("vsn=2.0.0"), PhoenixVersion::V2);
        assert_eq!(version_from_query("vsn=1.0.0"), PhoenixVersion::V1);
        assert_eq!(version_from_query("foo=bar"), PhoenixVersion::V2);
        assert_eq!(version_from_query(""), PhoenixVersion::V2);
    }

    #[test]
    fn query_param_reads_one_parameter() {
        assert_eq!(query_param("vsn=2.0.0&foo=bar", "vsn"), Some("2.0.0"));
        assert_eq!(query_param("vsn=2.0.0&foo=bar", "foo"), Some("bar"));
        assert_eq!(query_param("foo=bar", "vsn"), None);
        assert_eq!(query_param("flag", "flag"), None);
    }
}
