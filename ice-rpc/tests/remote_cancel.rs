//! Integration tests of the **remote** cancellation of a call.
//!
//! A response stream dropped while its call is still in flight tells the provider
//! to abandon it: the handler's future is dropped, so a query, a report or a scan
//! stops instead of running on for a caller that no longer listens.
//!
//! These tests run in their own binary (one process per test file), against the
//! real iceoryx2 transport: the provider and the caller are two threads of this
//! process, and the Cancel travels on the bus like any other sample — which is
//! exactly what must be verified, since nothing about it is visible from the
//! library API alone.
//!
//! The observer of [`DirectionView`] is what makes the test conclusive: it sees
//! the samples the caller publishes on the request channel, so "one Cancel, for
//! this very call" is asserted on the wire, not inferred.

#![allow(clippy::unwrap_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ice_rpc::gen::{rkyv, service_id_of, EventKind, ServiceDispatcher, ServiceRef, WireEvent};
use ice_rpc::transport::{native_call, spawn_native_service, Direction, DirectionView};
use ice_rpc::{CallContext, CancellationToken};

/// Time a channel needs to appear on the bus before a caller or an observer can
/// open it.
const CHANNEL_STARTUP: Duration = Duration::from_millis(300);

/// Longest wait for something this test cannot schedule itself.
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Encodes a wire event together with the [`EventKind`] the transport stamps in
/// the zero-copy header.
fn encode_i32(event: WireEvent<i32, String>) -> (EventKind, Vec<u8>) {
    let kind = event.kind();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
        .expect("encode event")
        .to_vec();
    (kind, bytes)
}

/// A future that never completes: the stand-in for a long query or a report being
/// generated. Only a cancellation can stop a handler awaiting it.
struct NeverReady;

impl Future for NeverReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

/// Sets a flag when it is dropped.
///
/// Held by a handler's future: the flag being set is the proof that the provider
/// **dropped** the work, which is what a cancellation does — not that it finished.
struct MarkDropped(Arc<AtomicBool>);

impl Drop for MarkDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Waits for `flag` to be set, and reports whether it was.
fn wait_for(flag: &AtomicBool) -> bool {
    let deadline = Instant::now() + EVENT_TIMEOUT;
    while Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    flag.load(Ordering::SeqCst)
}

/// Opens a read-only view of `channel`, retrying until the provider is there.
///
/// The provider creates four iceoryx2 services in turn, so a channel is briefly
/// half-registered: an observer opened a moment too early fails on the service
/// that is not created yet, for a reason that disappears by itself. A fixed sleep
/// cannot express "wait until it exists", and a slow machine turns it into a
/// failure that has nothing to do with the test.
fn open_view(channel: &str, direction: Direction) -> DirectionView {
    let deadline = Instant::now() + EVENT_TIMEOUT;
    loop {
        match DirectionView::open(channel, direction) {
            Ok(view) => return view,
            Err(e) if Instant::now() >= deadline => {
                panic!("'{channel}' ({direction:?}) never became observable: {e}")
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Drains an observer, returning the kind and the correlation id of every sample
/// it has seen.
fn drain(view: &DirectionView) -> Vec<(EventKind, [u8; 16])> {
    let mut seen = Vec::new();
    while let Ok(Some((header, _emitter, _len))) = view.try_receive() {
        seen.push((header.event_kind(), header.correlation_id));
    }
    seen
}

/// Dropping the stream of a call in flight cancels it on the provider: the
/// handler's future is dropped, and the Cancel that did it names that very call.
#[test]
fn dropping_a_stream_cancels_the_call_on_the_provider() {
    let channel = format!("IceRpcCancelReport{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    // The provider records the call it serves, so the Cancel observed on the wire
    // can be checked against the call it is supposed to name.
    let served: Arc<Mutex<Option<[u8; 16]>>> = Arc::new(Mutex::new(None));

    let handler_started = Arc::clone(&started);
    let handler_dropped = Arc::clone(&dropped);
    let handler_served = Arc::clone(&served);
    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("report", move |header, _payload, _emitter| {
        let started = Arc::clone(&handler_started);
        let dropped = Arc::clone(&handler_dropped);
        let served = Arc::clone(&handler_served);
        Box::pin(async move {
            // This dispatcher is hand-written, so no generated `call_scoped`
            // installs the call context here — the header is at hand instead. The
            // cancellation token, though, is installed by the transport itself: it
            // is the one ambient value a handler always finds, generated or not.
            assert!(
                CallContext::cancellation().is_some(),
                "a served call always has a cancellation token"
            );
            *served.lock().unwrap_or_else(|e| e.into_inner()) = Some(header.correlation_id);
            started.store(true, Ordering::SeqCst);

            let _dropped = MarkDropped(dropped);
            NeverReady.await;
        })
    });
    // A second method on the same channel: the proof that cancelling one call does
    // not damage the channel the others are served on.
    dispatcher.method("ping", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let (kind, payload) = encode_i32(WireEvent::CompleteWith(7));
            let _ = emitter.emit(kind, &payload);
        })
    });

    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    let observer = open_view(&channel, Direction::Request);

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "report", b"")
            .expect("native_call must open the native service");
    assert!(wait_for(&started), "the provider must serve the call");

    // The whole mechanism in one line: the caller stops listening.
    drop(stream);

    assert!(
        wait_for(&dropped),
        "the provider must abandon the work of the abandoned call"
    );

    let cancels: Vec<[u8; 16]> = drain(&observer)
        .into_iter()
        .filter(|(kind, _)| *kind == EventKind::Cancel)
        .map(|(_, cid)| cid)
        .collect();
    assert_eq!(
        cancels.len(),
        1,
        "exactly one Cancel, for the one abandoned call"
    );
    let served_cid = served
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .expect("a call was served");
    assert_eq!(
        cancels[0], served_cid,
        "the Cancel names the call it abandons"
    );

    // The channel still serves: the cancelled call took nothing else down.
    let stream = native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "ping", b"")
        .expect("the channel still works");
    assert_eq!(
        pollster::block_on(stream.collect()).expect("the next call is answered"),
        vec![7]
    );

    stop.cancel();
    let _ = server.join();
}

/// A call that was answered is not cancelled by the drop of its stream: there is
/// nothing left to abandon, and the request channel must not see a Cancel.
#[test]
fn a_completed_call_is_not_cancelled() {
    let channel = format!("IceRpcCancelDone{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("ping", |_header, _payload, mut emitter| {
        Box::pin(async move {
            let (kind, payload) = encode_i32(WireEvent::CompleteWith(7));
            let _ = emitter.emit(kind, &payload);
        })
    });
    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());

    let observer = open_view(&channel, Direction::Request);

    // `collect` consumes the stream, so its drop happens as the call completes —
    // the case that must stay silent.
    let stream = native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "ping", b"")
        .expect("native_call must open the native service");
    assert_eq!(
        pollster::block_on(stream.collect()).expect("the call is answered"),
        vec![7]
    );

    // Give a spurious Cancel all the time it needs to be observed.
    std::thread::sleep(Duration::from_millis(300));
    let cancels = drain(&observer)
        .into_iter()
        .filter(|(kind, _)| *kind == EventKind::Cancel)
        .count();
    assert_eq!(cancels, 0, "an answered call has nothing to cancel");

    stop.cancel();
    let _ = server.join();
}

/// Dropping a stream whose provider is gone must not block: the Cancel is a
/// single best-effort sample, never the provider wait a request is allowed.
#[test]
fn dropping_a_stream_without_a_provider_does_not_block() {
    let channel = format!("IceRpcCancelGone{}", std::process::id());
    let service_id = service_id_of(&channel);
    let stop = CancellationToken::new();

    let started = Arc::new(AtomicBool::new(false));
    let handler_started = Arc::clone(&started);
    let mut dispatcher = ServiceDispatcher::new(ServiceRef::new(service_id, 1));
    dispatcher.method("report", move |_header, _payload, _emitter| {
        let started = Arc::clone(&handler_started);
        Box::pin(async move {
            started.store(true, Ordering::SeqCst);
            NeverReady.await;
        })
    });

    let server = spawn_native_service(&channel, vec![dispatcher], stop.clone());
    std::thread::sleep(CHANNEL_STARTUP);

    let stream =
        native_call::<i32, String>(&channel, ServiceRef::new(service_id, 1), "report", b"")
            .expect("native_call must open the native service");
    assert!(wait_for(&started), "the provider must serve the call");

    // Nobody is left to receive the Cancel, and nobody may wait for one.
    stop.cancel();
    let _ = server.join();

    let started_at = Instant::now();
    drop(stream);
    let elapsed = started_at.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "the drop waited for the provider: {elapsed:?}"
    );
}
