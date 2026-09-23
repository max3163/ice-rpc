//! The ReactiveX operators, as **inherent methods** on [`crate::Observable`].
//!
//! There is no extension trait to import and no second stream type to name:
//! every operator is called directly on the `Observable` returned by a service
//! and returns another `Observable`, so a pipeline reads like RxJS:
//!
//! ```rust
//! use ice_rpc_rx::{of, rt::block_on, Observable};
//!
//! // In a provider, `stream` is the `Observable` returned by a call.
//! let stream: Observable<i32, String> = of(1);
//! let odds = stream.filter(|v| *v % 2 == 1).map(|v| v * 10).take(5);
//! assert_eq!(block_on(odds.collect()).expect("no error"), vec![10]);
//! ```
//!
//! # How to read this reference
//!
//! Each operator states the event it acts on, what it does to the terminal
//! events, and ships a runnable example. Two rules hold for **every** operator:
//!
//! - **Pull-based and lazy.** An operator only wraps its source in a boxed
//!   `futures_lite::Stream` through
//!   [`Observable::from_stream`](crate::Observable::from_stream): no
//!   intermediate channel, no spawned task, no `Arc`/lock. Nothing runs until a
//!   terminal consumes the pipeline.
//! - **Terminals pass through.** `Complete` and `Error` are never dropped,
//!   reordered or duplicated, unless the documentation below says otherwise
//!   ([`catch_error`](crate::Observable::catch_error),
//!   [`take`](crate::Observable::take),
//!   [`first`](crate::Observable::first),
//!   [`timeout`](crate::Observable::timeout),
//!   [`take_until`](crate::Observable::take_until)).
//!
//! # The two error channels
//!
//! [`ObservableError`](crate::ObservableError) distinguishes a **business** error,
//! authored by the service, from a **technical** one, raised by the framework
//! ([`RpcError`](crate::RpcError)). The distinction is load-bearing:
//! [`map_err`](crate::Observable::map_err) and
//! [`catch_error`](crate::Observable::catch_error) act on the business error
//! **only**. A technical error is fatal and travels untouched, because nothing in
//! a service implementation can meaningfully recover from a transport, discovery
//! or protocol failure — the retry or the reconnect belongs to the transport.
//!
//! # Operators by ReactiveX category
//!
//! | Category | Operators |
//! |---|---|
//! | Creating | [`of`](crate::of), [`from`](crate::from), [`throw_error`](crate::throw_error), [`channel`](crate::channel), [`Subject`](crate::Subject) |
//! | Transforming | [`map`](crate::Observable::map), [`map_err`](crate::Observable::map_err), [`scan`](crate::Observable::scan), [`switch_map`](crate::Observable::switch_map) |
//! | Filtering | [`filter`](crate::Observable::filter), [`take`](crate::Observable::take), [`skip`](crate::Observable::skip), [`first`](crate::Observable::first), [`first_with`](crate::Observable::first_with) |
//! | Combining | [`start_with`](crate::Observable::start_with) |
//! | Conditional / Boolean | [`take_until`](crate::Observable::take_until) |
//! | Error handling | [`catch_error`](crate::Observable::catch_error) |
//! | Utility | [`tap`](crate::Observable::tap), [`finalize`](crate::Observable::finalize), [`delay`](crate::Observable::delay), [`timeout`](crate::Observable::timeout) |
//! | Terminals (they end the chain) | [`collect`](crate::Observable::collect), [`first_value`](crate::Observable::first_value), [`for_each`](crate::Observable::for_each), [`subscribe`](crate::Observable::subscribe), [`subscribe_all`](crate::Observable::subscribe_all), [`next`](crate::Observable::next), [`recv`](crate::Observable::recv) |
//!
//! # Layout
//!
//! One file per ReactiveX category, named after it: `transforming.rs` holds
//! `map` / `map_err` / `scan` / `switch_map`, `filtering.rs` the filtering
//! methods, `utility.rs` the utility ones, and `terminals.rs` the consuming ones
//! (`for_each`, `subscribe`, `subscribe_all`). Each file carries both the
//! combinator struct and the inherent method that exposes it, so a new operator
//! is added in its category file — or in a new module declared here — never in
//! this one.

// One module per ReactiveX category. Each holds both the poll-based combinator
// and the inherent method that exposes it on `Observable`, so adding an
// operator touches one file and never this one. The operator *types* stay
// private: they never appear in a public signature.
mod combining;
mod conditional;
mod error_handling;
mod filtering;
mod terminals;
mod transforming;
mod utility;

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod tests;
