//! Codegen: `{Trait}Server` struct and its `run()` method.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Visibility};

use super::helpers::gen_hub_config;

pub struct ServerGenInput<'a> {
    pub trait_name: &'a Ident,
    pub logical_name: &'a str,
    pub visibility: &'a Visibility,
    pub server_name: &'a Ident,
    pub req_enum_name: &'a Ident,
    pub topic_ready: &'a str,
    pub blackboard_key: u8,
    pub server_match_arms: &'a [TokenStream],
    pub allow_large_payload: bool,
    pub default_size_message_kb: Option<u64>,
}

/// Generates the `{Trait}Server` and its `run()` method.
///
/// The handler deserializes the request directly from the iceoryx2 shared
/// memory via `rkyv::from_bytes(raw)`, then forwards the native type into
/// the dispatch channel. The serialization buffer (`AlignedVec`) is
/// shared via `Arc<async_lock::Mutex<...>>` and reused between requests.
pub fn gen_server(input: &ServerGenInput<'_>) -> TokenStream {
    let ServerGenInput {
        trait_name,
        logical_name,
        visibility,
        server_name,
        req_enum_name,
        topic_ready,
        blackboard_key,
        server_match_arms,
        allow_large_payload,
        default_size_message_kb,
    } = input;

    let hub_config = gen_hub_config(*allow_large_payload, *default_size_message_kb);

    quote! {
        #[derive(Clone)]
        #visibility struct #server_name {
            service_impl: std::sync::Arc<dyn #trait_name>,
            scratch: std::sync::Arc<ice_rpc::async_lock::Mutex<ice_rpc::rkyv::util::AlignedVec<8>>>,
        }

        impl #server_name {
            fn new(service_impl: std::sync::Arc<dyn #trait_name>) -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self {
                    service_impl,
                    scratch: std::sync::Arc::new(ice_rpc::async_lock::Mutex::new(
                        ice_rpc::rkyv::util::AlignedVec::<8>::with_capacity(4096)
                    )),
                })
            }

            async fn run(
                &self,
                ready_tx: ice_rpc::rt::oneshot::Sender<Result<(), String>>,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                #hub_config

                use ice_rpc::futures::FutureExt;

                let svc_name: &'static str = #logical_name;
                let svc_impl = self.service_impl.clone();
                let scratch = self.scratch.clone();

                let (dispatch_tx, mut dispatch_rx) =
                    ice_rpc::async_channel::bounded::<([u8; 16], #req_enum_name, usize)>(1024);

                let dispatch_tx_clone = dispatch_tx.clone();
                std::thread::spawn(move || {
                    let node = match ice_rpc::ServiceLocator::global().get_node_sync() {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("get_node_sync: {}", e)));
                            return;
                        }
                    };

                    let handler: ice_rpc::RequestHandler = std::sync::Arc::new({
                        let tx = dispatch_tx_clone;
                        move |hdr: ice_rpc::RpcHeader, raw: &[u8]| {
                            let cid = hdr.correlation_id;
                            let client_pid = hdr.caller_pid;
                            let client_node = ice_rpc::NodeId(client_pid);

                            if !ice_rpc::ServiceLocator::global().hub().has_publishers(client_node) {
                                if let Err(e) = ice_rpc::ServiceLocator::global()
                                    .hub().ensure_publishers_blocking(client_node)
                                {
                                    ::log::error!("[{}Server] ensure_publishers: {}", svc_name, e);
                                    return;
                                }
                            }

                            let raw_len = raw.len();
                            use ice_rpc::rkyv::rancor::Error as RkyvError;
                            let native_req: #req_enum_name = match ice_rpc::rkyv::from_bytes::<
                                #req_enum_name, RkyvError
                            >(raw) {
                                Ok(v) => v,
                                Err(e) => {
                                    ::log::error!("[{}Server] from_bytes(raw) from shared memory: {:?}", svc_name, e);
                                    return;
                                }
                            };

                            if tx.try_send((cid, native_req, raw_len)).is_err() {
                                ::log::warn!("[{}Server] channel saturated — request rejected", svc_name);
                            }
                        }
                    });

                    ice_rpc::ServiceLocator::global().hub().register_request_handler(svc_name, handler);

                    use ice_rpc::iceoryx2::prelude::ServiceName;
                    let ready_name = match ServiceName::new(#topic_ready) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("ServiceName({}): {:?}", #topic_ready, e)));
                            return;
                        }
                    };
                    let ready_svc = match node.service_builder(&ready_name)
                        .blackboard_creator::<u8>()
                        .max_readers(ice_rpc::BLACKBOARD_MAX_READERS)
                        .add::<bool>(#blackboard_key, false)
                        .create()
                    {
                        Ok(s) => s,
                        Err(_) => match node.service_builder(&ready_name)
                            .blackboard_opener::<u8>().open()
                        {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = ready_tx.send(Err(format!("blackboard open: {:?}", e)));
                                return;
                            }
                        },
                    };
                    let ready_writer = match ready_svc.writer_builder().create() {
                        Ok(w) => w,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("ready_writer: {:?}", e)));
                            return;
                        }
                    };
                    if let Ok(entry) = ready_writer.entry::<bool>(&#blackboard_key) {
                        entry.update_with_copy(true);
                    }

                    {
                        use std::sync::OnceLock;
                        static READY_WRITER: OnceLock<
                            Box<dyn std::any::Any + Send + Sync>
                        > = OnceLock::new();
                        let _ = READY_WRITER.set(Box::new(ready_writer));
                    }

                    let _ = ready_tx.send(Ok(()));
                });

                let cancel = ice_rpc::global_cancel_token();
                loop {
                    ice_rpc::futures::select! {
                        _ = cancel.cancelled().fuse() => break,
                        msg = dispatch_rx.recv().fuse() => {
                            match msg {
                                Err(_) => break,
                                Ok((cid, req_val, size_hint)) => {
                                    let impl_ref = svc_impl.clone();
                                    let scratch_ref = scratch.clone();
                                    ice_rpc::rt::spawn(async move {
                                        match req_val { #(#server_match_arms),* };
                                    });
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

/// Generates a server match arm for an RPC method.
///
/// Receives the request in native form, calls the business implementation,
/// and for each stream event, serializes the response via the shared
/// `scratch_ref` buffer and sends it via `send_to_node`.
pub fn gen_server_match_arm(
    trait_name: &Ident,
    logical_name: &str,
    fn_name: &Ident,
    var_name: &Ident,
    arg_names: &[&Ident],
    req_enum_name: &Ident,
) -> TokenStream {
    quote! {
        #req_enum_name::#var_name { #(#arg_names),* } => {
            use ice_rpc::rkyv::{api::high::to_bytes_in, util::AlignedVec, rancor::Error as RkyvError};

            let mut guard = scratch_ref.lock().await;
            if guard.capacity() < size_hint + 4096 {
                *guard = AlignedVec::<8>::with_capacity(size_hint + 4096);
            }
            let client_pid = ice_rpc::caller_pid_from_cid(&cid);
            let client_node = ice_rpc::NodeId(client_pid);
            let hub = ice_rpc::ServiceLocator::global().hub();

            match impl_ref.#fn_name(#(#arg_names),*).await {
                Ok(mut stream) => {
                    while let Ok(event) = stream.recv().await {
                        let kind = match &event {
                            ice_rpc::Event::Next(_)  => ice_rpc::EventKind::Next,
                            ice_rpc::Event::Complete => ice_rpc::EventKind::Complete,
                            ice_rpc::Event::Error(_) => ice_rpc::EventKind::Error,
                        };
                        guard.clear();
                        if to_bytes_in::<_, RkyvError>(&event, &mut *guard).is_err() { continue; }

                        let resp_header = ice_rpc::RpcHeader {
                            correlation_id: cid,
                            sent_at_ns: ice_rpc::RpcHeader::now_ns(),
                            caller_pid: std::process::id(),
                            service_name: ice_rpc::StaticString::from_bytes_truncated(
                                #logical_name.as_bytes()
                            ).unwrap_or_default(),
                            method_name: ice_rpc::StaticString::from_bytes_truncated(
                                stringify!(#fn_name).as_bytes()
                            ).unwrap_or_default(),
                            event_kind: kind,
                        };
                        let _ = hub.send_to_node(client_node, resp_header, &*guard);
                        if kind.is_terminal() { break; }
                    }
                }
                Err(e) => {
                    ::log::error!("[{}] IPC error on '{}': {}", stringify!(#trait_name), stringify!(#fn_name), e);
                    guard.clear();
                    let complete_event: ice_rpc::Event<(), ()> = ice_rpc::Event::Complete;
                    if to_bytes_in::<_, RkyvError>(&complete_event, &mut *guard).is_ok() {
                        let resp_header = ice_rpc::RpcHeader {
                            correlation_id: cid,
                            sent_at_ns: ice_rpc::RpcHeader::now_ns(),
                            caller_pid: std::process::id(),
                            service_name: ice_rpc::StaticString::from_bytes_truncated(
                                #logical_name.as_bytes()
                            ).unwrap_or_default(),
                            method_name: ice_rpc::StaticString::from_bytes_truncated(
                                stringify!(#fn_name).as_bytes()
                            ).unwrap_or_default(),
                            event_kind: ice_rpc::EventKind::Complete,
                        };
                        let _ = hub.send_to_node(client_node, resp_header, &*guard);
                    }
                }
            }
            drop(guard);
        }
    }
}
