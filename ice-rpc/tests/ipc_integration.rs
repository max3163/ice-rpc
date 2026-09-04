//! Integration tests exercising the iceoryx2-backed IPC paths.
//!
//! These tests run in their own test binary (separate process), so the global
//! iceoryx2 node and the global lock do not conflict with the unit tests.
//! The tests below still mutate process-global state (iceoryx2 config, the
//! global hub singleton), so they are serialized with a shared mutex.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ice_rpc::{NodeId, RpcHeader, ServiceLocator};

static INTEGRATION_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn blackboard_create_and_list_services_roundtrip() {
    let _guard = INTEGRATION_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Configure the global iceoryx2 config before creating the node.
    ice_rpc::setup_iceoryx2_global_config();

    let locator = ServiceLocator::global();
    let _node = locator
        .get_node_sync()
        .expect("failed to create the iceoryx2 node");

    // Use a node id derived from the PID so it does not collide with
    // concurrently running tests in other processes.
    let node_id = std::process::id() ^ 0x5EED_0001;

    let services = vec!["DatabaseService".to_string(), "ConfigService".to_string()];
    ice_rpc::gen::create_node_blackboard(node_id, &services);

    let mut listed = ice_rpc::gen::list_services(node_id);
    listed.sort();
    assert_eq!(
        listed,
        vec!["ConfigService".to_string(), "DatabaseService".to_string()]
    );
}

#[test]
fn hub_send_and_dispatch_loopback() {
    let _guard = INTEGRATION_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    ice_rpc::setup_iceoryx2_global_config();

    let locator = ServiceLocator::global();
    let _node = locator
        .get_node_sync()
        .expect("failed to create the iceoryx2 node");

    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();
    let handler = Arc::new(move |_hdr: RpcHeader, payload: &[u8]| {
        assert_eq!(payload, b"hello");
        received_clone.fetch_add(1, Ordering::SeqCst);
    });

    let hub = locator.hub();
    hub.register_request_handler("DatabaseService", handler);
    locator.start_dispatch_if_needed();

    // Send to our own node id (loopback): the dispatch loop listens on
    // `node_{pid}_default` and routes the request back to the handler.
    let local = NodeId(std::process::id());
    hub.ensure_publishers(local)
        .expect("failed to ensure publishers");

    let header = RpcHeader::new("DatabaseService", "get_user_age");
    hub.send_to_node(local, header, b"hello")
        .expect("failed to send request");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while received.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    assert_eq!(
        received.load(Ordering::SeqCst),
        1,
        "the request handler must be invoked by the dispatch loop"
    );
}
