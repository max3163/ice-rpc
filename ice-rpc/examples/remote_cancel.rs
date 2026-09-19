//! Remote cancellation: the caller walks away, the provider stops working.
//!
//! A local drop already released the caller's handler. What this example adds is
//! the other half of the story: the drop is **published** as an `EventKind::Cancel`
//! carrying the correlation id of the call, and the provider fires the token of
//! that very call. The handler's future is dropped, so a long query, a report or a
//! filesystem scan stops instead of running on for a caller that stopped
//! listening.
//!
//! The signal belongs to the framework; **honouring it belongs to the
//! implementation**, which reads its own token with `CallContext::cancellation()`.
//! On the provider side the transport drops the *handler's* future, so a piece of
//! work that was offloaded to its own task — or to `spawn_blocking` — only stops
//! because it watches that token itself. The producer task below is exactly that
//! case, and its log line is the proof the cancellation arrived.
//!
//! Both halves in one process — the first call waits for the provider to appear, so
//! no sleep is needed anywhere:
//!
//! ```bash
//! cargo run -p ice-rpc --example remote_cancel --features tokio
//! ```
//!
//! Or as two processes, provider first:
//!
//! ```bash
//! cargo run -p ice-rpc --example remote_cancel --features tokio -- provider
//! cargo run -p ice-rpc --example remote_cancel --features tokio -- consumer
//! ```
//!
//! Expected output (interleaving aside: the provider logs while the caller reads):
//!
//! ```text
//! [provider] report 7: starting a 1000-line scan
//! [consumer] report 7: line 0
//! [provider] report 7: produced line 0
//! [consumer] report 7: line 1
//! [provider] report 7: produced line 1
//! [consumer] report 7: line 2
//! [provider] report 7: produced line 2
//! [consumer] walking away from report 7: dropping the stream
//! [provider] report 7: cancelled by the caller, 3 line(s) produced
//! [consumer] summary(7) = report 7: 1000 lines
//! ```

#![allow(missing_docs)] // example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic

use std::time::Duration;

use ice_rpc::{service, CallContext, Observable};

/// Delay between two report lines: long enough for the caller to walk away
/// mid-report, short enough to watch.
const LINE_INTERVAL: Duration = Duration::from_millis(200);

/// How many lines the report holds.
///
/// Far more than the caller reads, and that is the point: the provider has to be
/// **stopped**, not exhausted. A report of three lines could not tell a
/// cancellation from a normal end.
const REPORT_LINES: u32 = 1_000;

/// Lines the caller reads before it stops listening.
const LINES_READ: u32 = 3;

/// A service whose report is long enough to be worth abandoning.
#[service("ReportService")]
pub trait ReportService {
    /// Streams the lines of a report, one every [`LINE_INTERVAL`], and stops where
    /// the caller stopped listening.
    async fn generate_report(&self, id: u32) -> Observable<String, String>;

    /// Answers in one shot: called after the cancellation, it is the proof that
    /// cancelling one call did not damage the channel it was served on.
    async fn summary(&self, id: u32) -> Observable<String, String>;
}

struct ReportServiceImpl;

#[async_trait::async_trait]
impl ReportService for ReportServiceImpl {
    async fn generate_report(&self, id: u32) -> Observable<String, String> {
        // The token of **this** call. It is not a parameter and it is not stored
        // anywhere by the implementation: the transport installs it around every
        // poll of the handler, exactly like the call context, so it is read here
        // for free — and it is the only thing a remote cancellation hands over.
        let cancel = CallContext::cancellation().expect("a served call always carries a token");
        let call = CallContext::current().expect("the handler installs the context");
        println!(
            "[provider] report {id}: starting a {REPORT_LINES}-line scan (call {})",
            call.correlation()
        );

        // One slot per line: the producer never waits on a full channel, so the
        // *only* thing that can stop it is the token. A smaller channel would stop
        // it on the dropped reader instead — back-pressure, not cancellation — and
        // the demo would prove nothing.
        let capacity = REPORT_LINES as usize + 1;
        let (tx, rx) = ice_rpc::gen::channel::<String, String>(capacity);

        // The work runs as a task of its own, holding the token. This is the
        // shape of any real report: the handler returns a stream immediately and
        // the producing task keeps going. The transport drops the handler's
        // future — this task survives it — so watching the token is what makes
        // the cancellation effective, and the log line below is what makes it
        // visible.
        ice_rpc::rt::spawn(async move {
            for line in 0..REPORT_LINES {
                ice_rpc::rt::sleep(LINE_INTERVAL).await;

                // Checked **between** two steps, and that is the whole contract of
                // a cooperative cancellation: a step — a query, a scan, a report
                // builder — is never interrupted in the middle, it is abandoned
                // before the next one starts. The token is already fired by the
                // time the response stream is disposed, so this read is the
                // authoritative one.
                if cancel.is_cancelled() {
                    println!(
                        "[provider] report {id}: cancelled by the caller, {line} line(s) produced"
                    );
                    return;
                }

                if tx
                    .send_next(format!("report {id}: line {line}"))
                    .await
                    .is_err()
                {
                    // The stream was disposed without the token firing: the whole
                    // pipeline is going down, not one call.
                    println!("[provider] report {id}: the response stream was closed");
                    return;
                }
                println!("[provider] report {id}: produced line {line}");
            }

            // Reached only by a report nobody interrupted.
            println!("[provider] report {id}: the whole report was produced");
            let _ = tx.send_complete().await;
        });

        rx
    }

    async fn summary(&self, id: u32) -> Observable<String, String> {
        ice_rpc::of(format!("report {id}: {REPORT_LINES} lines"))
    }
}

/// Hosts the service and blocks until the process is asked to stop.
async fn run_provider() -> Result<(), Box<dyn std::error::Error>> {
    ice_rpc::run_provider!(ReportServiceProxy::provide(ReportServiceImpl)).await
}

/// Reads the beginning of a report, then walks away from it.
async fn run_consumer() {
    // `consume()` builds the caller side without the service locator: this
    // example owns its only service.
    let proxy = ReportServiceProxy::consume();

    let mut report = proxy.generate_report(7).await;
    for _ in 0..LINES_READ {
        match report.next().await {
            Some(Ok(line)) => println!("[consumer] {line}"),
            // A report that ends before the third line would make the rest of the
            // demo meaningless.
            Some(Err(error)) => panic!("the report failed early: {error:?}"),
            None => panic!("the report ended before the caller could walk away"),
        }
    }

    // The cancellation in one line: the caller stops reading. No method to call,
    // no handle to keep — dropping the stream *is* the cancellation. Every
    // operator that gives up on a stream ends up here too: `timeout`,
    // `take_until(my_token)`, a `switch_map` that moved on, or simply a caller
    // that returns.
    println!("[consumer] walking away from report 7: dropping the stream");
    drop(report);

    // The provider was told to stop; give it the instant it needs to say so.
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("[consumer] --- the provider should have logged the cancellation above ---");

    // A cancelled call must leave the channel untouched: the next call is served
    // as if nothing had happened.
    let summary = proxy
        .summary(7)
        .await
        .first_value()
        .await
        .expect("the channel still works");
    println!("[consumer] summary(7) = {summary}");
}

#[ice_rpc::main(tokio)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("provider") => run_provider().await,
        Some("consumer") => {
            run_consumer().await;
            Ok(())
        }
        // Provider and caller side by side in one process: the provider task
        // stops with the process, when `#[ice_rpc::main]` cancels the global
        // token on the way out.
        None | Some("demo") => {
            tokio::spawn(async {
                if let Err(error) = run_provider().await {
                    eprintln!("[provider] {error}");
                }
            });
            run_consumer().await;
            Ok(())
        }
        Some(other) => {
            eprintln!("usage: remote_cancel [demo|provider|consumer] (got {other})");
            Ok(())
        }
    }
}
