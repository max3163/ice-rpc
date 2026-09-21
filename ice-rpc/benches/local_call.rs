//! Micro-benchmarks of a **direct** (in-process) service call.
//!
//! A provider that hosts two services and calls one from the other goes through
//! the generated proxy, whose `Provider` arm calls the local implementation
//! directly: no header, no `CallContext`, no span. Adding one is the subject of
//! `plans/spans-appels-internes-provider.md`; this benchmark prices it, so the
//! decision is taken on numbers rather than on intuition.
//!
//! Three questions are measured separately, because they have different answers:
//!
//! - `local_call` — what a direct call costs **today**, bare and drained;
//! - `local_call_wrapped` — the same call with each candidate envelope around
//!   it: an `async` block alone (the floor), the zero-allocation generic wrapper,
//!   `call_scoped` (two `Box::pin`), and, with the `tracing` feature, a span
//!   alone and the full wrapper;
//! - `local_call_primitives` — the ingredients priced one by one, so a total can
//!   be reconstructed and an outlier attributed.
//!
//! The envelopes are reproduced **here** from the public API rather than added to
//! the library: a measurement must not ship the code it measures. The wrapper
//! below is the shape the library would provide, including the trap it avoids —
//! the context is entered around each `poll`, never across an `await`.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p ice-rpc --bench local_call
//! cargo bench -p ice-rpc --bench local_call --features tracing
//! ```

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
#![allow(missing_docs)] // bench target: documented by the plan, not part of a published API

use std::future::Future;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use futures_lite::future::poll_fn;
use ice_rpc::gen::RpcHeader;
use ice_rpc::rt::block_on;
use ice_rpc::{service, CallContext, Observable};

/// The service every variant calls.
///
/// Declared here, with the real `#[service]` macro: the `Provider` arm being
/// measured is the generated one, not a hand-written imitation of it.
#[service("BenchLocalEcho")]
#[async_trait::async_trait]
pub trait LocalEcho: Send + Sync + 'static {
    /// Returns its argument.
    async fn echo(&self, value: i32) -> Observable<i32, String>;
}

/// Leaf implementation: builds a single-value stream.
struct EchoImpl;

#[async_trait::async_trait]
impl LocalEcho for EchoImpl {
    async fn echo(&self, value: i32) -> Observable<i32, String> {
        ice_rpc::of(value)
    }
}

/// Value every variant carries, and the value they must all return.
const VALUE: i32 = 7;

/// Method name the synthetic context names, as the code generator would.
const METHOD: &str = "echo";

/// Sink of the variant whose future must erase its output.
///
/// `call_scoped` returns `BoxResponseFuture`, i.e. `Output = ()`, so the value
/// cannot travel back through it. A static keeps the comparison honest without a
/// channel, which would add an allocation of its own to the measurement.
static SINK: AtomicUsize = AtomicUsize::new(0);

/// The context `CallContext::local` would build for a direct call.
///
/// `RpcHeader::request` mints a correlation id and reads the clock exactly like
/// the proposed constructor, and `CallContext::new` derives the span id — so this
/// helper prices the real thing, without the library having to grow a method
/// before the decision is taken.
fn local_context() -> CallContext {
    let header = RpcHeader::request(
        METHOD,
        ice_rpc::gen::service_id_of(LocalEchoProxy::SERVICE_NAME),
        1,
    );
    CallContext::new(&header, LocalEchoProxy::SERVICE_NAME, METHOD)
}

/// The zero-allocation envelope: the context is entered around each `poll`.
///
/// `pin!` on a local of an `async` block is what removes the second `Box::pin` of
/// [`call_scoped`](ice_rpc::gen::call_scoped); the `#[cfg]` lives in this function
/// rather than in a struct field, which is what the `pin_project_lite` limitation
/// recorded in `plans/lot2-allocations-par-appel.md` §2a.2 would otherwise forbid.
async fn local_scope<F>(ctx: CallContext, future: F) -> F::Output
where
    F: Future,
{
    #[cfg(feature = "tracing")]
    let span = ctx.span();

    let mut future = std::pin::pin!(future);
    poll_fn(move |cx| {
        let _ambient = ctx.enter_ambient();
        #[cfg(feature = "tracing")]
        let _span = span.enter();
        future.as_mut().poll(cx)
    })
    .await
}

/// The direct call as it is written today, drained to a value.
fn direct(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    let stream = block_on(proxy.echo(value));
    block_on(stream.first_value()).unwrap()
}

/// The direct call as it is written today, left undrained: the delegation alone.
fn direct_stream(proxy: &Arc<LocalEchoProxy>, value: i32) {
    let stream = block_on(proxy.echo(value));
    black_box(stream);
}

/// Floor of every envelope: the same body in an `async` block, nothing else.
fn async_block_only(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    block_on(async move {
        let stream = proxy.echo(value).await;
        stream.first_value().await.unwrap()
    })
}

/// Direct call wrapped by the zero-allocation envelope.
fn wrapped_generic(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    let ctx = local_context();
    block_on(local_scope(ctx, async move {
        let stream = proxy.echo(value).await;
        stream.first_value().await.unwrap()
    }))
}

/// Direct call wrapped by `call_scoped` — the envelope the transport uses.
///
/// It erases the future, so the value goes through [`SINK`]; the call itself is
/// otherwise identical.
fn wrapped_boxed(proxy: &Arc<LocalEchoProxy>, value: i32) {
    let ctx = local_context();
    let proxy = Arc::clone(proxy);
    let future = ice_rpc::gen::call_scoped(ctx, async move {
        let stream = proxy.echo(value).await;
        let value = stream.first_value().await.unwrap();
        SINK.store(value as usize, Ordering::Relaxed);
    });
    block_on(future);
}

/// Direct call wrapped by a **minimal** span: no context, no correlation id, no
/// clock read — the cheapest useful form of N1.
///
/// A real N1 would read `CallContext::current()` to parent on the caller's span;
/// that lookup is priced separately (`local_call_primitives/current_context`) and
/// is not a reason to build a whole context.
#[cfg(feature = "tracing")]
fn wrapped_span_only(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    use tracing::Instrument as _;

    let span = tracing::info_span!("rpc_local", method = METHOD);
    block_on(
        async move {
            let stream = proxy.echo(value).await;
            stream.first_value().await.unwrap()
        }
        .instrument(span),
    )
}

/// Full candidate: the context **and** its span are installed around each poll.
#[cfg(feature = "tracing")]
fn wrapped_generic_with_span(proxy: &Arc<LocalEchoProxy>, value: i32) -> i32 {
    let ctx = local_context();
    block_on(local_scope(ctx, async move {
        let stream = proxy.echo(value).await;
        stream.first_value().await.unwrap()
    }))
}

/// What a direct call costs today, and what each envelope adds to it.
fn bench_local_call(c: &mut Criterion) {
    let proxy = LocalEchoProxy::provide(EchoImpl);

    // Agreement first: an envelope that changed the result would be measuring a
    // different call.
    assert_eq!(direct(&proxy, VALUE), VALUE, "direct");
    assert_eq!(async_block_only(&proxy, VALUE), VALUE, "async_block_only");
    assert_eq!(wrapped_generic(&proxy, VALUE), VALUE, "wrapped_generic");
    wrapped_boxed(&proxy, VALUE);
    assert_eq!(
        SINK.load(Ordering::Relaxed) as i32,
        VALUE,
        "wrapped_boxed returned the wrong value"
    );
    #[cfg(feature = "tracing")]
    {
        assert_eq!(wrapped_span_only(&proxy, VALUE), VALUE, "wrapped_span_only");
        assert_eq!(
            wrapped_generic_with_span(&proxy, VALUE),
            VALUE,
            "wrapped_generic_with_span"
        );
    }

    let mut group = c.benchmark_group("local_call");
    group.bench_function("direct_stream", |b| {
        b.iter(|| {
            direct_stream(&proxy, black_box(VALUE));
            black_box(())
        })
    });
    group.bench_function("direct_drained", |b| {
        b.iter(|| black_box(direct(&proxy, black_box(VALUE))))
    });
    group.finish();

    let mut group = c.benchmark_group("local_call_wrapped");
    group.bench_function("async_block_only", |b| {
        b.iter(|| black_box(async_block_only(&proxy, black_box(VALUE))))
    });
    group.bench_function("generic_ctx_zero_alloc", |b| {
        b.iter(|| black_box(wrapped_generic(&proxy, black_box(VALUE))))
    });
    group.bench_function("boxed_ctx_call_scoped", |b| {
        b.iter(|| {
            wrapped_boxed(&proxy, black_box(VALUE));
            black_box(SINK.load(Ordering::Relaxed))
        })
    });
    #[cfg(feature = "tracing")]
    group.bench_function("span_only", |b| {
        b.iter(|| black_box(wrapped_span_only(&proxy, black_box(VALUE))))
    });
    #[cfg(feature = "tracing")]
    group.bench_function("generic_ctx_with_span", |b| {
        b.iter(|| black_box(wrapped_generic_with_span(&proxy, black_box(VALUE))))
    });
    group.finish();
}

/// The ingredients of the envelopes, priced one by one.
///
/// The end-to-end groups above give the answer to "how much"; this one gives the
/// answer to "where", which is what makes the answer auditable.
fn bench_primitives(c: &mut Criterion) {
    let ctx = local_context();

    let mut group = c.benchmark_group("local_call_primitives");

    group.bench_function("next_correlation_id", |b| {
        b.iter(|| black_box(ice_rpc::gen::next_correlation_id()))
    });

    group.bench_function("now_ns", |b| b.iter(|| black_box(ice_rpc::gen::now_ns())));

    group.bench_function("header_and_context", |b| {
        b.iter(|| {
            let header = RpcHeader::request(
                METHOD,
                ice_rpc::gen::service_id_of(LocalEchoProxy::SERVICE_NAME),
                1,
            );
            black_box(CallContext::new(
                &header,
                LocalEchoProxy::SERVICE_NAME,
                METHOD,
            ))
        })
    });

    group.bench_function("child_trace", |b| {
        b.iter(|| black_box(black_box(ctx).child_trace()))
    });

    group.bench_function("ambient_install_restore", |b| {
        b.iter(|| {
            let scope = black_box(ctx).enter_ambient();
            drop(scope);
        })
    });

    group.bench_function("call_scoped_empty", |b| {
        b.iter(|| {
            let future = ice_rpc::gen::call_scoped(black_box(ctx), async {});
            block_on(future);
        })
    });

    // What a real N1 pays to find the parent it must attach to, and nothing else.
    group.bench_function("current_context", |b| {
        b.iter(|| black_box(CallContext::current()))
    });

    #[cfg(feature = "tracing")]
    group.bench_function("span_create", |b| {
        b.iter(|| black_box(black_box(ctx).span()))
    });

    #[cfg(feature = "tracing")]
    group.bench_function("instrument_empty", |b| {
        use tracing::Instrument as _;
        b.iter(|| {
            let span = black_box(ctx).span();
            block_on(black_box(async {}).instrument(span));
        })
    });

    group.finish();
}

/// The same wrappers, with a `tracing` **subscriber** installed.
///
/// This is the case that decides: with no subscriber a span is never recorded
/// and costs a few nanoseconds (measured above), so what a deployment that
/// actually collects traces pays is this. `FmtSpan::CLOSE` is the demo's setting,
/// and the writer is a sink so the measurement is the collector's work, not the
/// terminal's.
#[cfg(feature = "tracing")]
fn bench_collected(c: &mut Criterion) {
    use tracing_subscriber::fmt::format::FmtSpan;

    let proxy = LocalEchoProxy::provide(EchoImpl);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::sink)
        .with_span_events(FmtSpan::CLOSE)
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);

    // Agreement under the collector too: a span that changed the result would be
    // measuring another call.
    assert_eq!(wrapped_span_only(&proxy, VALUE), VALUE, "span_only");
    assert_eq!(
        wrapped_generic_with_span(&proxy, VALUE),
        VALUE,
        "generic_ctx_with_span"
    );

    let mut group = c.benchmark_group("local_call_collected");
    tracing::dispatcher::with_default(&dispatch, || {
        group.bench_function("async_block_only", |b| {
            b.iter(|| black_box(async_block_only(&proxy, black_box(VALUE))))
        });
        group.bench_function("span_only", |b| {
            b.iter(|| black_box(wrapped_span_only(&proxy, black_box(VALUE))))
        });
        group.bench_function("generic_ctx_with_span", |b| {
            b.iter(|| black_box(wrapped_generic_with_span(&proxy, black_box(VALUE))))
        });
    });
    group.finish();
}

#[cfg(feature = "tracing")]
criterion_group!(benches, bench_local_call, bench_primitives, bench_collected);

#[cfg(not(feature = "tracing"))]
criterion_group!(benches, bench_local_call, bench_primitives);

criterion_main!(benches);
