//! ice-rpc driven by the `pollster` executor (no async runtime).
//!
//! `pollster` only runs a top-level future; it does not spawn tasks or timers.
//! The ice-rpc core therefore provides its own runtime-agnostic primitives via
//! `ice_rpc::rt` (global executor for spawning, blocking pool for blocking
//! threads, async-io for timers).

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    ice_rpc::init_without_ctrl_c();

    pollster::block_on(async {
        log::info!("ice-rpc running on the pollster executor.");

        // Spawning goes to the agnostic global executor.
        ice_rpc::rt::spawn(async move {
            ice_rpc::rt::sleep(std::time::Duration::from_millis(20)).await;
            log::info!("agnostic task finished.");
        });

        ice_rpc::rt::sleep(std::time::Duration::from_millis(40)).await;

        // Clean release of the iceoryx2 resources.
        ice_rpc::shutdown_and_release().await;
    });

    log::info!("pollster example finished.");
}
