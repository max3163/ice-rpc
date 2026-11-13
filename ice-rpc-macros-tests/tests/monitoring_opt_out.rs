#![allow(missing_docs)] // test target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests may panic
// Integration tests for `#[service(..., no_monitoring)]`.
//
// The option opts a service out of the **automatic** observation while the
// `monitoring` feature stays on: the decoder is not submitted to the link-time
// `DECODERS` registry, its channel is submitted to `HIDDEN_CHANNELS` so
// `discover_channels` skips it, and the decoder remains available for an explicit
// registration.

use ice_rpc::monitor::{DECODERS, Decoders, HIDDEN_CHANNELS, is_hidden_channel};
use ice_rpc::{Observable, service};

/// The control: a service declared without the option stays observable.
#[service("VisibleProbe")]
#[async_trait::async_trait]
pub trait VisibleProbe: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

/// Opted out, with the default channel (the logical name).
#[service("HiddenProbe", no_monitoring)]
#[async_trait::async_trait]
pub trait HiddenProbe: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

/// Opted out, sharing a named channel: the group is what gets hidden.
#[service("HiddenGrouped", group = "hidden-group", no_monitoring)]
#[async_trait::async_trait]
pub trait HiddenGrouped: Send + Sync + 'static {
    async fn ping(&self) -> Observable<(), String>;
}

#[test]
fn an_opted_out_service_is_not_in_the_linked_decoders() {
    let names: Vec<&str> = DECODERS
        .iter()
        .map(|registration| registration.service_name)
        .collect();

    assert!(
        names.contains(&"VisibleProbe"),
        "a default service must submit its decoder: {names:?}"
    );
    assert!(
        !names.contains(&"HiddenProbe"),
        "an opted-out service must not submit its decoder: {names:?}"
    );
    assert!(
        !names.contains(&"HiddenGrouped"),
        "an opted-out service must not submit its decoder: {names:?}"
    );
    assert_eq!(
        Decoders::linked().len(),
        1,
        "only the visible service must be in the linked registry"
    );
}

#[test]
fn an_opted_out_service_hides_its_channel() {
    assert!(
        HIDDEN_CHANNELS
            .iter()
            .any(|registration| registration.channel == "HiddenProbe"),
        "the default channel is the logical name"
    );
    assert!(
        HIDDEN_CHANNELS
            .iter()
            .any(|registration| registration.channel == "hidden-group"),
        "a declared group is the hidden channel"
    );
    assert!(is_hidden_channel("hidden-group"));
    assert!(
        !is_hidden_channel("VisibleProbe"),
        "a service without the option must stay discoverable"
    );
    assert!(!is_hidden_channel("not-declared"));
}

#[test]
fn the_decoder_remains_available_for_an_explicit_registration() {
    let mut decoders = Decoders::new();
    HiddenProbeDecoder::register(&mut decoders);
    assert_eq!(
        decoders.len(),
        1,
        "an explicitly registered decoder must reach the registry"
    );
}
