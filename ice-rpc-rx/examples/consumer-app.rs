//! ice-rpc Consumer (DatabaseService or ContextService).
//!
//! Starts an IPC consumer, runs demonstration queries, then
//! waits for [Enter] to replay the queries or Ctrl+C to quit.
//! The shutdown uses the RAII ShutdownGuard.
//!
//! ## Lazy pattern
//!
//! The consumer does not register the proxies manually.
//! `ServiceLocator::get()` instantiates the proxy on the first request from
//! its type — without `register()`, without a registry nor `initialize_all()`
//! on the consumer side.
//!
//! Usage:
//! - `cargo run --example consumer-app -- --service context`
//! - `cargo run --example consumer-app -- --service notifications`

use common::{
    ContextEntry, ContextError, ContextService, ContextServiceProxy, DatabaseError,
    DatabaseService, DatabaseServiceProxy, NotificationService, NotificationServiceProxy,
    PersonneInfo, PersonneQuery,
};
use ice_rpc::{ObservableError, StreamError};
use ice_rpc_rx::{from, of, throw_error, Observer, RxStreamExt};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};

fn fmt_latency(ms: f64) -> String {
    if ms < 5.0 {
        format!("\x1b[32m{:.3}ms\x1b[0m", ms)
    } else if ms < 20.0 {
        format!("\x1b[33m{:.3}ms\x1b[0m", ms)
    } else {
        format!("\x1b[31m{:.3}ms\x1b[0m", ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ServiceType {
    Database,
    Context,
    Notifications,
}

impl ServiceType {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|a| a == "--service" || a == "-s") {
            let mut found = false;
            for arg in &args {
                if found {
                    return match arg.as_str() {
                        "context" | "ContextService" => ServiceType::Context,
                        "notifications" | "Notifications" | "NotificationService" => {
                            ServiceType::Notifications
                        }
                        _ => {
                            log::warn!(
                                "Unknown service '{}', using DatabaseService by default.",
                                arg
                            );
                            ServiceType::Database
                        }
                    };
                }
                if arg == "--service" || arg == "-s" {
                    found = true;
                }
            }
        }
        ServiceType::Database
    }
}

async fn run_database_queries(db: &DatabaseServiceProxy) -> bool {
    let cancel = ice_rpc::global_cancel_token();

    macro_rules! run_query {
        ($label:expr, $query:expr, $handler:expr) => {{
            log::info!("-> {}", $label);
            let t_send = Instant::now();
            let stream = $query;
            let result = stream.take_until(cancel).first_value().await;
            match result {
                Err(StreamError::Technical(ice_rpc::RpcError::Cancelled)) => {
                    log::info!("   (cancelled by Ctrl+C)");
                    return false;
                }
                result => {
                    let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
                    $handler(result, elapsed_ms);
                }
            }
        }};
    }

    run_query!(
        "Requesting Alice's age...",
        db.get_user_age("Alice".into()).await,
        |r: Result<i32, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(age) => log::info!("<- Alice is {} years old  [{}]", age, fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "Requesting Bob's age...",
        db.get_user_age("Bob".into()).await,
        |r: Result<i32, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(age) => log::info!("<- Bob is {} years old  [{}]", age, fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "Requesting Max's age (unknown)...",
        db.get_user_age("Max".into()).await,
        |r: Result<i32, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(age) => log::info!("<- Max is {} years old  [{}]", age, fmt_latency(ms)),
            Err(StreamError::Business(DatabaseError::NotFound)) =>
                log::warn!("<- Max not found in database  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "Looking up Jean Dupont...",
        db.get_person(PersonneQuery {
            nom: "Dupont".into(),
            prenom: "Jean".into()
        })
        .await,
        |r: Result<PersonneInfo, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(info) => log::info!(
                "<- {} {} - {} years old, {}, {}, {}, {}  [{}]",
                info.nom,
                info.prenom,
                info.age,
                info.email,
                info.telephone,
                info.ville,
                info.profession,
                fmt_latency(ms)
            ),
            Err(StreamError::Business(DatabaseError::NotFound)) =>
                log::warn!("<- Person not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "Looking up Marie Martin...",
        db.get_person(PersonneQuery {
            nom: "Martin".into(),
            prenom: "Marie".into()
        })
        .await,
        |r: Result<PersonneInfo, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(info) => log::info!(
                "<- {} {} - {} years old, {}, {}, {}, {}  [{}]",
                info.nom,
                info.prenom,
                info.age,
                info.email,
                info.telephone,
                info.ville,
                info.profession,
                fmt_latency(ms)
            ),
            Err(StreamError::Business(DatabaseError::NotFound)) =>
                log::warn!("<- Person not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "Looking up Pierre Bernard...",
        db.get_person(PersonneQuery {
            nom: "Bernard".into(),
            prenom: "Pierre".into()
        })
        .await,
        |r: Result<PersonneInfo, StreamError<DatabaseError>>, ms: f64| match r {
            Ok(info) => log::info!(
                "<- {} {} - {} years old, {}, {}, {}, {}  [{}]",
                info.nom,
                info.prenom,
                info.age,
                info.email,
                info.telephone,
                info.ville,
                info.profession,
                fmt_latency(ms)
            ),
            Err(StreamError::Business(DatabaseError::NotFound)) =>
                log::warn!("<- Person not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {:?}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    true
}

async fn run_context_queries(ctx: &ContextServiceProxy) -> bool {
    let cancel = ice_rpc::global_cancel_token();

    macro_rules! run_query {
        ($label:expr, $query:expr, $handler:expr) => {{
            log::info!("-> {}", $label);
            let t_send = Instant::now();
            let stream = $query;
            let result = stream.take_until(cancel).first_value().await;
            match result {
                Err(StreamError::Technical(ice_rpc::RpcError::Cancelled)) => {
                    log::info!("   (cancelled by Ctrl+C)");
                    return false;
                }
                result => {
                    let elapsed_ms = t_send.elapsed().as_secs_f64() * 1000.0;
                    $handler(result, elapsed_ms);
                }
            }
        }};
    }

    run_query!(
        "GET app.name (should exist)...",
        ctx.get("app.name".into()).await,
        |r: Result<String, StreamError<ContextError>>, ms: f64| match r {
            Ok(value) => log::info!("<- app.name = \"{}\"  [{}]", value, fmt_latency(ms)),
            Err(StreamError::Business(ContextError::KeyNotFound)) =>
                log::warn!("<- Key 'app.name' not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "GET nonexistent.key (should fail)...",
        ctx.get("nonexistent.key".into()).await,
        |r: Result<String, StreamError<ContextError>>, ms: f64| match r {
            Ok(value) => log::info!("<- nonexistent.key = \"{}\"  [{}]", value, fmt_latency(ms)),
            Err(StreamError::Business(ContextError::KeyNotFound)) => log::warn!(
                "<- Key 'nonexistent.key' not found (OK)  [{}]",
                fmt_latency(ms)
            ),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "SET test.key = 'hello-world'...",
        ctx.set("test.key".into(), "hello-world".into()).await,
        |r: Result<bool, StreamError<ContextError>>, ms: f64| match r {
            Ok(true) => log::info!("<- test.key defined successfully  [{}]", fmt_latency(ms)),
            Ok(false) => log::warn!("<- test.key NOT defined  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "GET test.key (verification)...",
        ctx.get("test.key".into()).await,
        |r: Result<String, StreamError<ContextError>>, ms: f64| match r {
            Ok(value) => log::info!("<- test.key = \"{}\"  [{}]", value, fmt_latency(ms)),
            Err(StreamError::Business(ContextError::KeyNotFound)) =>
                log::warn!("<- Key 'test.key' not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "DELETE test.key...",
        ctx.delete("test.key".into()).await,
        |r: Result<bool, StreamError<ContextError>>, ms: f64| match r {
            Ok(true) => log::info!("<- test.key deleted  [{}]", fmt_latency(ms)),
            Ok(false) => log::warn!("<- test.key did not exist  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "GET test.key (after deletion)...",
        ctx.get("test.key".into()).await,
        |r: Result<String, StreamError<ContextError>>, ms: f64| match r {
            Ok(value) => log::info!("<- test.key = \"{}\"  [{}]", value, fmt_latency(ms)),
            Err(StreamError::Business(ContextError::KeyNotFound)) => log::warn!(
                "<- Key 'test.key' correctly deleted (OK)  [{}]",
                fmt_latency(ms)
            ),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "LIST all entries...",
        ctx.list().await,
        |r: Result<ContextEntry, StreamError<ContextError>>, ms: f64| match r {
            Ok(entry) => log::info!(
                "<- [LIST] {} = \"{}\"  [{}]",
                entry.key,
                entry.value,
                fmt_latency(ms)
            ),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) =>
                log::warn!("<- No entry (empty context)  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "SET 'session.user' = 'Jean Dupont'...",
        ctx.set("session.user".into(), "Jean Dupont".into()).await,
        |r: Result<bool, StreamError<ContextError>>, ms: f64| match r {
            Ok(true) => log::info!("<- session.user defined  [{}]", fmt_latency(ms)),
            Ok(false) => log::warn!("<- session.user NOT defined  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    let large_payload: String = {
        let mut items = Vec::with_capacity(50);
        for i in 0..50 {
            items.push(format!(
                r#"{{"id":{},"name":"item-{:03}","description":"Large payload to validate performance. Item {}","tags":["tag-a","tag-b","tag-c"],"metadata":{{"created":"2026-06-21T{:02}:00:00Z","score":{:.2},"active":{}}}}}"#,
                i, i, i, i % 24, (i as f64) * 1.5, i % 2 == 0
            ));
        }
        format!("[{}]", items.join(","))
    };
    let payload_size = large_payload.len();
    run_query!(
        format!("SET 'large.payload' ({} bytes)...", payload_size),
        ctx.set("large.payload".into(), large_payload).await,
        |r: Result<bool, StreamError<ContextError>>, ms: f64| match r {
            Ok(true) => log::info!(
                "<- large.payload ({:.1} KB) defined  [{}]",
                payload_size as f64 / 1024.0,
                fmt_latency(ms)
            ),
            Ok(false) => log::warn!("<- large.payload NOT defined  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "GET large.payload (verification)...",
        ctx.get("large.payload".into()).await,
        |r: Result<String, StreamError<ContextError>>, ms: f64| match r {
            Ok(value) => log::info!(
                "<- large.payload = {} bytes  [{}]",
                value.len(),
                fmt_latency(ms)
            ),
            Err(StreamError::Business(ContextError::KeyNotFound)) =>
                log::warn!("<- Key 'large.payload' not found  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    run_query!(
        "DELETE large.payload (cleanup)...",
        ctx.delete("large.payload".into()).await,
        |r: Result<bool, StreamError<ContextError>>, ms: f64| match r {
            Ok(true) => log::info!("<- large.payload deleted  [{}]", fmt_latency(ms)),
            Ok(false) => log::warn!("<- large.payload did not exist  [{}]", fmt_latency(ms)),
            Err(StreamError::Business(e)) =>
                log::warn!("<- Business error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Technical(e)) =>
                log::error!("<- IPC error: {}  [{}]", e, fmt_latency(ms)),
            Err(StreamError::Empty) => log::warn!("<- No value received  [{}]", fmt_latency(ms)),
        }
    );

    true
}

/// A hand-written observer: keeps its state and side effects off the callbacks.
struct NotificationPrinter {
    received: usize,
}

impl Observer<u32, String> for NotificationPrinter {
    fn next(&mut self, value: u32) {
        self.received += 1;
        log::info!("  next({}) — {} notification(s)", value, self.received);
    }

    fn error(&mut self, error: ObservableError<String>) {
        log::warn!("  error: {}", error);
    }

    fn complete(&mut self) {
        log::info!("  complete — {} notification(s) in total", self.received);
    }
}

/// Demonstrates the `subscribe` (push) mechanism: first on local sources (no IPC
/// involved), then on a real streaming RPC.
///
/// The two `subscribe` names are involved:
/// - `Subject::subscribe()` returns an `Observable` (multicast registration);
/// - `RxStreamExt::subscribe` is the terminal activation: one task pulls the
///   pipeline and pushes into an `Observer`.
async fn run_notification_demo(notif: &NotificationServiceProxy) -> bool {
    // ── Local sources: no provider needed, same Observer contract ─────
    log::info!("--- subscribe over local sources (no IPC) ---");

    // A plain closure is used as a value-only observer.
    let sub = from::<u32, String, _>([1, 2, 3]).subscribe(|v| log::info!("  [from] next {v}"));
    sub.closed().await; // resolves on Complete
    log::info!("   closed: {}", sub.is_closed());

    // The business error travels through the `error` callback, not as an `Err`.
    let sub = throw_error::<u32, String>("nothing to notify".to_string()).subscribe_with(
        |v| log::info!("  [error] next {v}"),
        |e| log::warn!("  [error] error: {e}"),
        || log::info!("  [error] complete"),
    );
    sub.closed().await;

    // A hand-written `Observer` implementation.
    let sub = of::<u32, String>(0).subscribe(NotificationPrinter { received: 0 });
    sub.closed().await;

    // ── Over ice-rpc ─────────────────────────────────────────────────
    log::info!("--- subscribe over ice-rpc ---");

    // Single-value call, read in pull mode (one single wire sample).
    match notif.ping().await.first_value().await {
        Ok(v) => log::info!("<- ping = {v}"),
        Err(e) => log::warn!("<- ping failed: {e}"),
    }

    // A whole stream, subscribed until completion.
    log::info!("-> watch(5) — subscribing until completion…");
    let sub = notif.watch(5).await.subscribe_with(
        |v| log::info!("<- notification {v}"),
        |e| log::error!("<- error: {e}"),
        || log::info!("<- complete"),
    );
    sub.closed().await;
    log::info!("   subscription closed: {}", sub.is_closed());

    // Unsubscribe in the middle of a stream: dropping the handle stops the local
    // task, and the provider's next `send_next` fails, so it stops producing.
    log::info!("-> watch(10) — unsubscribing after ~350 ms…");
    let sub = notif
        .watch(10)
        .await
        .subscribe(|v| log::info!("<- early notification {v}"));
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    log::info!("   dropping the Subscription (unsubscribe)");
    drop(sub);
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    true
}

/// Reads a line from stdin asynchronously.
/// Returns `None` if stdin is closed or the cancellation token is triggered.
async fn read_line_or_cancel(
    reader: &mut (impl AsyncBufReadExt + Unpin),
    cancel: &ice_rpc::CancellationToken,
) -> Option<String> {
    let mut line = String::new();
    tokio::select! {
        _ = cancel.cancelled() => None,
        result = reader.read_line(&mut line) => match result {
            Ok(0) => None, // EOF
            Ok(_) => Some(line),
            Err(_) => None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // RAII guard: cancels the cancellation tokens on Drop (even on panic).
    // shutdown() must be called explicitly for a clean stop with
    // waiting for the IPC threads and releasing the iceoryx2 node.
    let shutdown_guard = ice_rpc::ShutdownGuard::new();

    let service_type = ServiceType::from_args();
    match service_type {
        ServiceType::Database => log::info!("=== CONSUMER STARTUP (DatabaseService) ==="),
        ServiceType::Context => log::info!("=== CONSUMER STARTUP (ContextService) ==="),
        ServiceType::Notifications => {
            log::info!("=== CONSUMER STARTUP (NotificationService) ===")
        }
    }

    // This process consumes services via locator().get().
    ice_rpc::init();

    let cancel = ice_rpc::global_cancel_token();
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    match service_type {
        ServiceType::Database => {
            let db = ice_rpc::locator()
                .get::<DatabaseServiceProxy>()
                .await
                .expect("DatabaseService unknown in the registry");

            log::info!("--- Initial execution ---");
            if !run_database_queries(&db).await {
                return shutdown(&shutdown_guard).await;
            }
            log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");

            loop {
                match read_line_or_cancel(&mut reader, cancel).await {
                    None => break, // Ctrl+C or EOF
                    Some(_) => {
                        log::info!("\n--- Relaunching queries ---");
                        if !run_database_queries(&db).await {
                            break;
                        }
                        log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");
                    }
                }
            }
        }
        ServiceType::Context => {
            let ctx = ice_rpc::locator()
                .get::<ContextServiceProxy>()
                .await
                .expect("ContextService unknown in the registry");

            log::info!("--- Initial execution ---");
            if !run_context_queries(&ctx).await {
                return shutdown(&shutdown_guard).await;
            }
            log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");

            loop {
                match read_line_or_cancel(&mut reader, cancel).await {
                    None => break,
                    Some(_) => {
                        log::info!("\n--- Relaunching queries ---");
                        if !run_context_queries(&ctx).await {
                            break;
                        }
                        log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");
                    }
                }
            }
        }
        ServiceType::Notifications => {
            let notif = ice_rpc::locator()
                .get::<NotificationServiceProxy>()
                .await
                .expect("NotificationService unknown in the registry");

            log::info!("--- Initial execution ---");
            if !run_notification_demo(&notif).await {
                return shutdown(&shutdown_guard).await;
            }
            log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");

            loop {
                match read_line_or_cancel(&mut reader, cancel).await {
                    None => break,
                    Some(_) => {
                        log::info!("\n--- Relaunching notifications ---");
                        if !run_notification_demo(&notif).await {
                            break;
                        }
                        log::info!("--- End. [ENTER] to replay, [Ctrl+C] to quit. ---\n");
                    }
                }
            }
        }
    }

    shutdown(&shutdown_guard).await
}

/// Clean shutdown: ice-rpc shutdown via the RAII guard.
///
/// The guard guarantees that the tokens are cancelled even if this function
/// is not called (panic, early return…).
async fn shutdown(guard: &ice_rpc::ShutdownGuard) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Stopping consumer...");
    guard.shutdown().await;
    log::info!("Consumer stopped.");
    Ok(())
}
