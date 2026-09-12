//! ice-rpc performance benchmark — parallel workers with a configurable pipeline.
//!
//! # Usage
//! ```bash
//! cargo run --release --features="tokio" --example benchmark-app -- --workers 3 --pipeline 2 --requests 5000
//! ```
//!
//! # Getting trustworthy numbers
//!
//! Each measured phase is preceded by an **untimed warm-up phase**: the first
//! calls pay one-off costs — provider discovery, publisher creation,
//! shared-memory growth, OS page-in — which would otherwise land inside the
//! measured window.
//!
//! A single phase is still noisy, so use `--repeat` to get a **median** plus the
//! min→max spread, which is the noise floor of the run:
//!
//! ```bash
//! cargo run --release -p ice-rpc --example benchmark-app --features tokio -- \
//!     --workers 8 --requests 2000 --blast --repeat 5
//! ```
//!
//! Any difference smaller than the reported spread is **not** attributable to
//! the code.

#![allow(clippy::unwrap_used)] // tests/examples/benches may panic
use common::{ConfigServiceProxy, DatabaseService, DatabaseServiceProxy, PersonneQuery};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
struct BenchConfig {
    workers: usize,
    requests_per_worker: usize,
    warmup_per_worker: usize,
    service: String,
    pipeline_depth: usize,
    json: bool,
    min_success_rate: f64,
    min_rps: f64,
    /// Number of times the whole measured phase is repeated. With more than
    /// one repetition the **median** is reported: on a non-realtime OS a single
    /// phase is noisy enough to swing the throughput by 10-30%.
    repeat: usize,
}

impl BenchConfig {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut cfg = Self {
            workers: 8,
            requests_per_worker: 200,
            warmup_per_worker: 20,
            service: "db".into(),
            pipeline_depth: 1,
            json: false,
            min_success_rate: 0.95,
            min_rps: 0.0,
            repeat: 1,
        };
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--workers" => {
                    i += 1;
                    cfg.workers = args[i].parse().unwrap_or(cfg.workers);
                }
                "--requests" => {
                    i += 1;
                    cfg.requests_per_worker = args[i].parse().unwrap_or(cfg.requests_per_worker);
                }
                "--warmup" => {
                    i += 1;
                    cfg.warmup_per_worker = args[i].parse().unwrap_or(cfg.warmup_per_worker);
                }
                "--service" => {
                    i += 1;
                    cfg.service = args[i].clone();
                }
                "--pipeline" => {
                    i += 1;
                    cfg.pipeline_depth = args[i].parse().unwrap_or(cfg.pipeline_depth);
                }
                "--blast" => {
                    cfg.pipeline_depth = usize::MAX;
                }
                "--json" => {
                    cfg.json = true;
                }
                "--min-success-rate" => {
                    i += 1;
                    cfg.min_success_rate = args[i].parse().unwrap_or(cfg.min_success_rate);
                }
                "--min-rps" => {
                    i += 1;
                    cfg.min_rps = args[i].parse().unwrap_or(cfg.min_rps);
                }
                "--repeat" => {
                    i += 1;
                    cfg.repeat = args[i].parse().unwrap_or(cfg.repeat).max(1);
                }
                _ => {}
            }
            i += 1;
        }
        cfg
    }

    fn total_requests(&self) -> usize {
        self.workers * self.requests_per_worker
    }

    fn mode_label(&self) -> String {
        match self.pipeline_depth {
            1 => "sequential (pipeline=1)".into(),
            usize::MAX => "full blast (blast)".into(),
            n => format!("sliding window (pipeline={})", n),
        }
    }
}

#[derive(Debug)]
enum ReqOutcome {
    Ok(Duration),
    ErrIpc(Duration),
    ErrService(Duration),
    ErrEmpty(Duration),
}

impl ReqOutcome {
    fn duration(&self) -> Duration {
        match self {
            ReqOutcome::Ok(d) => *d,
            ReqOutcome::ErrIpc(d) => *d,
            ReqOutcome::ErrService(d) => *d,
            ReqOutcome::ErrEmpty(d) => *d,
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, ReqOutcome::Ok(_))
    }
}

const DB_NAMES: &[&str] = &[
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
];

const PERSON_QUERIES: &[(&str, &str)] = &[
    ("Dupont", "Jean"),
    ("Martin", "Marie"),
    ("Bernard", "Pierre"),
    ("Petit", "Sophie"),
    ("Thomas", "Luc"),
];

const REQ_TIMEOUT: Duration = Duration::from_secs(5);

async fn send_one_db(db: Arc<DatabaseServiceProxy>, name: String) -> ReqOutcome {
    let t0 = Instant::now();
    let mut rx = db.get_user_age(name).await;
    // `next()` is the concise view of the stream: `None` = the provider
    // completed without a value, `Some(Err(..))` = business or technical error.
    match tokio::time::timeout(REQ_TIMEOUT, rx.next()).await {
        Err(_) => ReqOutcome::ErrEmpty(t0.elapsed()),
        Ok(Some(Ok(_))) => ReqOutcome::Ok(t0.elapsed()),
        Ok(Some(Err(ice_rpc::ObservableError::Business(_)))) => {
            ReqOutcome::ErrService(t0.elapsed())
        }
        Ok(Some(Err(ice_rpc::ObservableError::Technical(_)))) => ReqOutcome::ErrIpc(t0.elapsed()),
        Ok(Some(Err(ice_rpc::ObservableError::Empty)) | None) => ReqOutcome::ErrEmpty(t0.elapsed()),
    }
}

async fn send_one_person(db: Arc<DatabaseServiceProxy>, query: PersonneQuery) -> ReqOutcome {
    let t0 = Instant::now();
    let mut rx = db.get_person(query).await;
    match tokio::time::timeout(REQ_TIMEOUT, rx.next()).await {
        Err(_) => ReqOutcome::ErrEmpty(t0.elapsed()),
        Ok(Some(Ok(_))) => ReqOutcome::Ok(t0.elapsed()),
        Ok(Some(Err(ice_rpc::ObservableError::Business(_)))) => {
            ReqOutcome::ErrService(t0.elapsed())
        }
        Ok(Some(Err(ice_rpc::ObservableError::Technical(_)))) => ReqOutcome::ErrIpc(t0.elapsed()),
        Ok(Some(Err(ice_rpc::ObservableError::Empty)) | None) => ReqOutcome::ErrEmpty(t0.elapsed()),
    }
}

async fn worker_db(
    db: Arc<DatabaseServiceProxy>,
    worker_id: usize,
    cfg: Arc<BenchConfig>,
) -> Vec<ReqOutcome> {
    let total = cfg.warmup_per_worker + cfg.requests_per_worker;

    for i in 0..cfg.warmup_per_worker {
        let name = DB_NAMES[(worker_id * 7 + i * 3) % DB_NAMES.len()].to_string();
        send_one_db(db.clone(), name).await;
    }

    let mut results = Vec::with_capacity(cfg.requests_per_worker);
    let depth = cfg.pipeline_depth.min(cfg.requests_per_worker);
    let sem = Arc::new(Semaphore::new(depth));
    let mut join_set = tokio::task::JoinSet::new();

    for i in cfg.warmup_per_worker..total {
        let name = DB_NAMES[(worker_id * 7 + i * 3) % DB_NAMES.len()].to_string();
        let db2 = db.clone();
        let sem2 = sem.clone();

        let permit = sem2.acquire_owned().await.unwrap();

        join_set.spawn(async move {
            let outcome = send_one_db(db2, name).await;
            drop(permit);
            outcome
        });

        while let Some(Ok(outcome)) = join_set.try_join_next() {
            results.push(outcome);
        }
    }

    while let Some(Ok(outcome)) = join_set.join_next().await {
        results.push(outcome);
    }
    results
}

async fn worker_person(
    db: Arc<DatabaseServiceProxy>,
    worker_id: usize,
    cfg: Arc<BenchConfig>,
) -> Vec<ReqOutcome> {
    let total = cfg.warmup_per_worker + cfg.requests_per_worker;

    for i in 0..cfg.warmup_per_worker {
        let (nom, prenom) = PERSON_QUERIES[(worker_id * 7 + i * 3) % PERSON_QUERIES.len()];
        let query = PersonneQuery {
            nom: nom.to_string(),
            prenom: prenom.to_string(),
        };
        send_one_person(db.clone(), query).await;
    }

    let mut results = Vec::with_capacity(cfg.requests_per_worker);
    let depth = cfg.pipeline_depth.min(cfg.requests_per_worker);
    let sem = Arc::new(Semaphore::new(depth));
    let mut join_set = tokio::task::JoinSet::new();

    for i in cfg.warmup_per_worker..total {
        let (nom, prenom) = PERSON_QUERIES[(worker_id * 7 + i * 3) % PERSON_QUERIES.len()];
        let query = PersonneQuery {
            nom: nom.to_string(),
            prenom: prenom.to_string(),
        };
        let db2 = db.clone();
        let sem2 = sem.clone();

        let permit = sem2.acquire_owned().await.unwrap();

        join_set.spawn(async move {
            let outcome = send_one_person(db2, query).await;
            drop(permit);
            outcome
        });

        while let Some(Ok(outcome)) = join_set.try_join_next() {
            results.push(outcome);
        }
    }

    while let Some(Ok(outcome)) = join_set.join_next().await {
        results.push(outcome);
    }
    results
}

struct Stats {
    count: usize,
    ok: usize,
    err_ipc: usize,
    err_svc: usize,
    err_empty: usize,
    min_us: u64,
    max_us: u64,
    mean_us: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    throughput: f64,
}

fn compute_stats(outcomes: &mut [ReqOutcome], wall: Duration) -> Stats {
    let count = outcomes.len();
    if count == 0 {
        return Stats {
            count: 0,
            ok: 0,
            err_ipc: 0,
            err_svc: 0,
            err_empty: 0,
            min_us: 0,
            max_us: 0,
            mean_us: 0.0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            throughput: 0.0,
        };
    }

    let ok = outcomes.iter().filter(|o| o.is_ok()).count();
    let err_ipc = outcomes
        .iter()
        .filter(|o| matches!(o, ReqOutcome::ErrIpc(_)))
        .count();
    let err_svc = outcomes
        .iter()
        .filter(|o| matches!(o, ReqOutcome::ErrService(_)))
        .count();
    let err_empty = outcomes
        .iter()
        .filter(|o| matches!(o, ReqOutcome::ErrEmpty(_)))
        .count();

    let mut durations_us: Vec<u64> = outcomes
        .iter()
        .map(|o| o.duration().as_micros() as u64)
        .collect();
    durations_us.sort_unstable();

    let min_us = *durations_us.first().unwrap();
    let max_us = *durations_us.last().unwrap();
    let mean_us = durations_us.iter().sum::<u64>() as f64 / count as f64;
    let p50_us = durations_us[count * 50 / 100];
    let p95_us = durations_us[count * 95 / 100];
    let p99_us = durations_us[(count * 99 / 100).min(count - 1)];

    let throughput = count as f64 / wall.as_secs_f64();

    Stats {
        count,
        ok,
        err_ipc,
        err_svc,
        err_empty,
        min_us,
        max_us,
        mean_us,
        p50_us,
        p95_us,
        p99_us,
        throughput,
    }
}

/// Median of a sample set (sorts in place; the caller keeps the sorted order to
/// read `first`/`last` as min/max).
fn median_f64(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("no NaN sample"));
    values[values.len() / 2]
}

/// Median of a sample set (sorts in place).
fn median_u64(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn print_stats(cfg: &BenchConfig, stats: &Stats, wall: Duration) {
    let sep = "─".repeat(60);
    println!("\n{sep}");
    println!("  ice-rpc BENCHMARK RESULTS");
    println!("{sep}");
    println!("  Service      : {}", cfg.service);
    println!("  Mode         : {}", cfg.mode_label());
    println!("  Workers      : {}", cfg.workers);
    println!("  Req/worker   : {}", cfg.requests_per_worker);
    println!("  Total req    : {}", stats.count);
    println!("  Total duration : {:.3}s", wall.as_secs_f64());
    println!("{sep}");
    println!("  Results");
    println!(
        "    ✓ Success     : {} ({:.1}%)",
        stats.ok,
        stats.ok as f64 / stats.count as f64 * 100.0
    );
    if stats.err_svc > 0 {
        println!(
            "    ~ Business err : {} ({:.1}%)",
            stats.err_svc,
            stats.err_svc as f64 / stats.count as f64 * 100.0
        );
    }
    if stats.err_ipc > 0 {
        println!(
            "    ✗ IPC err      : {} ({:.1}%)",
            stats.err_ipc,
            stats.err_ipc as f64 / stats.count as f64 * 100.0
        );
    }
    if stats.err_empty > 0 {
        println!(
            "    ✗ Empty err    : {} ({:.1}%)",
            stats.err_empty,
            stats.err_empty as f64 / stats.count as f64 * 100.0
        );
    }
    println!("{sep}");
    println!("  Latency (all requests)");
    println!("    min   : {:>8.3} ms", stats.min_us as f64 / 1000.0);
    println!("    p50   : {:>8.3} ms", stats.p50_us as f64 / 1000.0);
    println!("    mean  : {:>8.3} ms", stats.mean_us / 1000.0);
    println!("    p95   : {:>8.3} ms", stats.p95_us as f64 / 1000.0);
    println!("    p99   : {:>8.3} ms", stats.p99_us as f64 / 1000.0);
    println!("    max   : {:>8.3} ms", stats.max_us as f64 / 1000.0);
    println!("{sep}");
    println!("  Throughput    : {:.0} req/s", stats.throughput);
    println!("{sep}\n");
}

fn print_json(cfg: &BenchConfig, stats: &Stats, wall: Duration) {
    let mode_key = if cfg.pipeline_depth == 1 {
        "sequential"
    } else if cfg.pipeline_depth == usize::MAX {
        "blast"
    } else {
        "pipeline"
    };

    let out = serde_json::json!({
        "service": cfg.service,
        "mode": cfg.mode_label(),
        "mode_key": mode_key,
        "workers": cfg.workers,
        "requests_per_worker": cfg.requests_per_worker,
        "total_requests": cfg.total_requests(),
        "count": stats.count,
        "success": stats.ok,
        "success_rate": if stats.count > 0 { stats.ok as f64 / stats.count as f64 } else { 0.0 },
        "err_ipc": stats.err_ipc,
        "err_svc": stats.err_svc,
        "err_empty": stats.err_empty,
        "latency_us": {
            "min": stats.min_us,
            "p50": stats.p50_us,
            "mean": stats.mean_us,
            "p95": stats.p95_us,
            "p99": stats.p99_us,
            "max": stats.max_us,
        },
        "throughput_rps": stats.throughput,
        "duration_s": wall.as_secs_f64(),
    });
    println!("{}", serde_json::to_string(&out).unwrap_or_default());
}

/// Runs one full measured phase: spawns the workers, collects the outcomes and
/// computes the statistics.
///
/// The one-off connection costs (provider discovery, publisher creation,
/// shared-memory growth, OS page-in) are paid by the **first** phase, so it is
/// always used as an untimed warm-up before the measured repetitions.
async fn run_phase(proxy: Arc<DatabaseServiceProxy>, cfg: Arc<BenchConfig>) -> (Stats, Duration) {
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.workers);

    if cfg.service == "person" {
        for worker_id in 0..cfg.workers {
            let proxy = proxy.clone();
            let cfg_c = cfg.clone();
            handles.push(tokio::spawn(async move {
                worker_person(proxy, worker_id, cfg_c).await
            }));
        }
    } else {
        for worker_id in 0..cfg.workers {
            let proxy = proxy.clone();
            let cfg_c = cfg.clone();
            handles.push(tokio::spawn(async move {
                worker_db(proxy, worker_id, cfg_c).await
            }));
        }
    }

    let mut all_outcomes: Vec<ReqOutcome> = Vec::with_capacity(cfg.total_requests());
    for handle in handles {
        match handle.await {
            Ok(outcomes) => all_outcomes.extend(outcomes),
            Err(e) => log::error!("[benchmark] worker panicked: {}", e),
        }
    }

    let wall = wall_start.elapsed();
    (compute_stats(&mut all_outcomes, wall), wall)
}

#[ice_rpc::main(tokio)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cfg = Arc::new(BenchConfig::from_args());

    log::info!("=== ice-rpc BENCHMARK ===");
    log::info!("  Service      : {}", cfg.service);
    log::info!("  Mode         : {}", cfg.mode_label());
    log::info!("  Workers      : {}", cfg.workers);
    log::info!("  Req/worker   : {}", cfg.requests_per_worker);
    log::info!("  Warmup/wkr   : {}", cfg.warmup_per_worker);
    log::info!("  Total measured : {}", cfg.total_requests());
    log::info!("");

    // This process consumes services via locator().get().
    let db_proxy = if cfg.service == "db" || cfg.service == "person" || cfg.service == "all" {
        ice_rpc::locator().get::<DatabaseServiceProxy>().await
    } else {
        None
    };

    let _cfg_proxy = if cfg.service == "config" || cfg.service == "all" {
        ice_rpc::locator().get::<ConfigServiceProxy>().await
    } else {
        None
    };

    log::info!(
        "Launching {} workers ({})...",
        cfg.workers,
        cfg.mode_label()
    );
    log::info!("Warmup: {} req/worker (not counted)", cfg.warmup_per_worker);
    log::info!("");

    let proxy = db_proxy.expect("DatabaseServiceProxy not initialized");

    // ── Untimed warm-up phase ────────────────────────────────────────
    // Absorbs the one-off connection costs before the measured phase(s).
    log::info!("Warm-up phase (not measured)...");
    let (warm, _) = run_phase(proxy.clone(), cfg.clone()).await;
    log::info!(
        "  warm-up: {:.0} req/s · success {:.1}%",
        warm.throughput,
        if warm.count > 0 {
            warm.ok as f64 / warm.count as f64 * 100.0
        } else {
            0.0
        }
    );

    // ── Measured phase(s) ────────────────────────────────────────────
    let mut stats_reps: Vec<Stats> = Vec::with_capacity(cfg.repeat);
    for rep in 1..=cfg.repeat {
        let (stats, wall) = run_phase(proxy.clone(), cfg.clone()).await;

        if cfg.repeat == 1 {
            if cfg.json {
                print_json(&cfg, &stats, wall);
            } else {
                print_stats(&cfg, &stats, wall);
            }
        } else {
            let rate = if stats.count > 0 {
                stats.ok as f64 / stats.count as f64 * 100.0
            } else {
                0.0
            };
            log::info!(
                "  rep {}/{} : {:.0} req/s · p50 {:.3} ms · p95 {:.3} ms · success {:.1}%",
                rep,
                cfg.repeat,
                stats.throughput,
                stats.p50_us as f64 / 1000.0,
                stats.p95_us as f64 / 1000.0,
                rate,
            );
        }
        stats_reps.push(stats);
    }

    // ── Median summary (noise-resistant) ─────────────────────────────
    let mut throughputs: Vec<f64> = stats_reps.iter().map(|s| s.throughput).collect();
    let mut p50s: Vec<u64> = stats_reps.iter().map(|s| s.p50_us).collect();
    let mut p95s: Vec<u64> = stats_reps.iter().map(|s| s.p95_us).collect();
    let med_throughput = median_f64(&mut throughputs);
    let med_p50 = median_u64(&mut p50s);
    let med_p95 = median_u64(&mut p95s);

    if cfg.repeat > 1 {
        let sep = "─".repeat(60);
        log::info!("");
        log::info!("{sep}");
        log::info!("  MEDIAN over {} repetitions", cfg.repeat);
        log::info!(
            "  Throughput : {:.0} req/s   (min {:.0} / max {:.0})",
            med_throughput,
            throughputs[0],
            throughputs[throughputs.len() - 1]
        );
        log::info!(
            "  p50        : {:.3} ms     (min {:.3} / max {:.3})",
            med_p50 as f64 / 1000.0,
            p50s[0] as f64 / 1000.0,
            p50s[p50s.len() - 1] as f64 / 1000.0
        );
        log::info!("  p95        : {:.3} ms", med_p95 as f64 / 1000.0);
        log::info!("{sep}");
        log::info!("  Spread (min→max) is the machine noise floor of this run: any");
        log::info!("  difference smaller than it is not attributable to the code.");
    }

    let total_ok: usize = stats_reps.iter().map(|s| s.ok).sum();
    let total_count: usize = stats_reps.iter().map(|s| s.count).sum();

    let success_rate = if total_count > 0 {
        total_ok as f64 / total_count as f64
    } else {
        0.0
    };
    let mut failed = success_rate < cfg.min_success_rate;
    if failed {
        log::error!(
            "[benchmark] success rate {:.1}% below threshold {:.1}%",
            success_rate * 100.0,
            cfg.min_success_rate * 100.0
        );
    }
    if cfg.min_rps > 0.0 && med_throughput < cfg.min_rps {
        log::error!(
            "[benchmark] median throughput {:.0} req/s below threshold {:.0} req/s",
            med_throughput,
            cfg.min_rps
        );
        failed = true;
    }

    if !cfg.json {
        log::info!("");
        log::info!("=== get_person DEMO (3 calls) ===");
        let demo_queries: &[(&str, &str)] = &[
            ("Dupont", "Jean"),
            ("Martin", "Marie"),
            ("Bernard", "Pierre"),
        ];
        for (nom, prenom) in demo_queries {
            let query = PersonneQuery {
                nom: nom.to_string(),
                prenom: prenom.to_string(),
            };
            let mut rx = proxy.get_person(query).await;
            match rx.recv().await {
                Ok(ice_rpc::Event::Next(info)) => {
                    log::info!(
                        "  {} {} — {} years old, {}, {}",
                        info.nom,
                        info.prenom,
                        info.age,
                        info.ville,
                        info.profession
                    );
                }
                Ok(ice_rpc::Event::Error(e)) => {
                    log::warn!("  {} {} — Error: {}", nom, prenom, e);
                }
                _ => log::warn!("  {} {} — No response", nom, prenom),
            }
        }
    }

    // Clean shutdown (cancel + join of the IPC threads, node release) is
    // performed by the `#[ice_rpc::main]` guard once `main` returns.
    log::info!("Stopping benchmark...");

    if failed {
        return Err("benchmark thresholds not met".into());
    }

    Ok(())
}
