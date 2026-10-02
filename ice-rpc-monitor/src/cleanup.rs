//! Opt-in reaping of the state left behind by processes that were killed.
//!
//! An observer is **read-only by default**: it never creates, and never removes,
//! anything on the bus. `--cleanup` turns the startup into a one-shot repair
//! instead, so a bus polluted by killed runs does not keep every registry scan
//! expensive — a stale service makes `Service::list` / `Node::list` slow and the
//! next node creation try to replay the cleanup.
//!
//! It is the same repair an `ice-rpc` provider performs on startup
//! (`run_provider_inner`), exposed here so a machine that only runs the observer
//! can heal itself too.

/// Reaps the dead-node resources and the orphan shared-memory markers.
///
/// Returns `(dead_nodes, orphan_markers)`. Both operations are best-effort: a
/// resource another process still holds is simply skipped.
pub fn reap_dead_state() -> (u64, usize) {
    let dead_nodes = ice_rpc::transport::cleanup_dead_nodes();
    let orphan_markers = ice_rpc::transport::sweep_orphan_shm_markers();
    (dead_nodes, orphan_markers)
}
