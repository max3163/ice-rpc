//! ice-rpc driven by the `smol` runtime (runtime-agnostic core).
//!
//! Demonstrates that the ice-rpc core has no hard dependency on Tokio:
//! the top-level future is executed by `smol::block_on`, tasks are spawned
//! through `smol::spawn`, and the ice-rpc primitives come from `ice_rpc::rt`.

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    ice_rpc::init_without_ctrl_c();

    smol::block_on(async {
        log::info!("ice-rpc running on the smol executor.");

        // A task spawned on the smol executor.
        let handle = smol::spawn(async move {
            ice_rpc::rt::sleep(std::time::Duration::from_millis(20)).await;
            log::info!("smol task finished.");
        });
        handle.await;

        // Clean release of the iceoryx2 resources.
        ice_rpc::shutdown_and_release().await;
    });

    log::info!("smol example finished.");
}
