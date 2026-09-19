//! Codegen: `{Trait}Server` struct and its native request/response dispatcher.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Visibility};

/// Generation parameters of the server struct.
pub struct ServerGenInput<'a> {
    /// The user's `#[service]` trait.
    pub trait_name: &'a Ident,
    /// Visibility inherited from the trait.
    pub visibility: &'a Visibility,
    /// Name of the generated server type (`{Trait}Server`).
    pub server_name: &'a Ident,
    /// One `ServiceDispatcher::method(...)` registration per RPC method.
    pub server_native_methods: &'a [TokenStream],
    /// Expression of the shared [`ServiceRef`] of the service (id + version).
    pub service_ref: &'a TokenStream,
}

/// Generates the `{Trait}Server` struct and its native dispatcher.
///
/// The service is exposed through the iceoryx2 native request/response
/// transport: one [`ServiceDispatcher`](ice_rpc) entry per RPC method, each of
/// them decoding the rkyv request enum and streaming the resulting `Observable`
/// back as rkyv `WireEvent` samples.
pub fn gen_server(input: &ServerGenInput<'_>) -> TokenStream {
    let ServerGenInput {
        trait_name,
        visibility,
        server_name,
        server_native_methods,
        service_ref,
    } = input;

    quote! {
        #[derive(Clone)]
        #visibility struct #server_name {
            service_impl: std::sync::Arc<dyn #trait_name>,
        }

        impl #server_name {
            fn new(service_impl: std::sync::Arc<dyn #trait_name>) -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self { service_impl })
            }

            /// Builds the native `request_response` dispatcher of this service.
            ///
            /// Each RPC method is registered with its own handler: it decodes
            /// the rkyv request enum from the payload, invokes the local
            /// implementation, and streams the resulting `Observable` through
            /// `observable_to_responses`.
            ///
            /// Each handler returns a **task**, which the transport polls once on
            /// the channel's thread before detaching it: a handler that answers
            /// without yielding runs on that thread, one that `await`s runs as a
            /// task and cannot hold back the next request.
            fn native_dispatcher(self: std::sync::Arc<Self>) -> ice_rpc::gen::ServiceDispatcher {
                let mut dispatcher = ice_rpc::gen::ServiceDispatcher::new(#service_ref);
                #(#server_native_methods)*
                dispatcher
            }
        }
    }
}

/// Generates one `ServiceDispatcher::method(...)` registration for the native
/// request/response transport.
///
/// The handler returns a **task**: an owned request in, a boxed future out. The
/// transport polls it once on the channel's thread and detaches it only if it
/// yields, so a call that waits on a database cannot hold back the next request
/// of the same `group` — while a call that answers from memory pays no hop at all.
///
/// The handler installs the `CallContext` of the call it serves as an ambient
/// value for every poll of that task, so the implementation reads it with
/// `CallContext::current()` and its signature is untouched.
///
/// A payload that does not decode — or that decodes as another method's request
/// variant — is answered **immediately** with a `RpcError::SerializationError`,
/// never dropped: silence would leave the caller waiting for a transport timeout
/// that names nothing. Every request routed to a known service and method is
/// therefore answered, which is what makes the whole protocol fail-fast.
pub fn gen_native_method(
    proxy_name: &Ident,
    fn_name: &Ident,
    var_name: &Ident,
    arg_names: &[&Ident],
    req_enum_name: &Ident,
) -> TokenStream {
    let method_name_str = fn_name.to_string();

    quote! {
        {
            let service_impl = self.service_impl.clone();
            dispatcher.method(
                #method_name_str,
                move |header: ice_rpc::gen::RpcHeader,
                      payload: Vec<u8>,
                      emitter: ice_rpc::gen::OwnedEmitter|
                      -> ice_rpc::gen::BoxResponseFuture {
                    // Built before the coroutine: it is copied into the task, and
                    // the header itself is not needed past this point.
                    let ctx = ice_rpc::gen::CallContext::new(&header, #method_name_str);
                    // Clone per invocation: the closure is `Fn`, so it must not
                    // move the captured `Arc` into the coroutine.
                    let impl_ref = service_impl.clone();
                    // Installed around each poll rather than around the whole
                    // call: the tasks of one channel are polled interleaved, so a
                    // context held across an await would label the wrong call.
                    ice_rpc::gen::call_scoped(
                        ctx,
                        async move {
                            let mut emitter = emitter;
                            // The framed payload is not necessarily aligned for
                            // rkyv, so the decode goes through an aligned copy.
                            match ice_rpc::gen::decode_aligned::<#req_enum_name>(&payload) {
                                Ok(#req_enum_name::#var_name { #(#arg_names),* }) => {
                                    let stream = impl_ref.#fn_name(#(#arg_names),*).await;
                                    // Awaited, never blocked on: the other calls
                                    // of the channel run meanwhile.
                                    ice_rpc::gen::observable_to_responses(stream, &mut *emitter)
                                        .await;
                                }
                                // Fail-fast: a payload that does not decode is
                                // answered at once with a technical error.
                                Err(e) => {
                                    ice_rpc::gen::log::error!(
                                        "[{}::{}] request payload decoding failed: {:?}",
                                        <#proxy_name>::SERVICE_NAME,
                                        #method_name_str,
                                        e
                                    );
                                    let _ = ice_rpc::gen::emit_rpc_error(
                                        ice_rpc::gen::RpcError::SerializationError,
                                        &mut *emitter,
                                    );
                                }
                                // The payload decoded, but as the request variant
                                // of another method: a caller contract violation,
                                // answered like a decoding failure rather than
                                // silently. The log line tells the two cases apart.
                                Ok(_) => {
                                    ice_rpc::gen::log::error!(
                                        "[{}::{}] request payload is another method's variant",
                                        <#proxy_name>::SERVICE_NAME,
                                        #method_name_str
                                    );
                                    let _ = ice_rpc::gen::emit_rpc_error(
                                        ice_rpc::gen::RpcError::SerializationError,
                                        &mut *emitter,
                                    );
                                }
                            }
                        },
                    )
                },
            );
        }
    }
}
