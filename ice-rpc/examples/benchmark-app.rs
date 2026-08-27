//! ice-rpc performance benchmark — parallel workers with a configurable pipeline.
//!
//! # Usage
//! ```bash
//! cargo run --release --example benchmark-app -- --workers 3 --pipeline 2 --requests 5000
//! ```

mod shared;

use shared::{ConfigServiceProxy, DatabaseService, DatabaseServiceProxy, PersonneQuery};
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
    let rx = match db.get_user_age(name).await {
        Err(_) => return ReqOutcome::ErrIpc(t0.elapsed()),
        Ok(rx) => rx,
    };
    match tokio::time::timeout(REQ_TIMEOUT, rx.recv()).await {
        Err(_) => ReqOutcome::ErrEmpty(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Next(_))) => ReqOutcome::Ok(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Error(_))) => ReqOutcome::ErrService(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Complete) | Err(_)) => ReqOutcome::ErrEmpty(t0.elapsed()),
    }
}

async fn send_one_person(db: Arc<DatabaseServiceProxy>, query: PersonneQuery) -> ReqOutcome {
    let t0 = Instant::now();
    let rx = match db.get_person(query).await {
        Err(_) => return ReqOutcome::ErrIpc(t0.elapsed()),
        Ok(rx) => rx,
    };
    match tokio::time::timeout(REQ_TIMEOUT, rx.recv()).await {
        Err(_) => ReqOutcome::ErrEmpty(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Next(_))) => ReqOutcome::Ok(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Error(_))) => ReqOutcome::ErrService(t0.elapsed()),
        Ok(Ok(ice_rpc::Event::Complete) | Err(_)) => ReqOutcome::ErrEmpty(t0.elapsed()),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cfg = Arc::new(BenchConfig::from_args());

    // RAII guard: cancels the cancellation tokens on Drop (even on panic).
    // shutdown() must be called explicitly for a clean stop with
    // waiting for the IPC threads and releasing the iceoryx2 node.
    let shutdown_guard = ice_rpc::ShutdownGuard::new();

    log::info!("=== ice-rpc BENCHMARK ===");
    log::info!("  Service      : {}", cfg.service);
    log::info!("  Mode         : {}", cfg.mode_label());
    log::info!("  Workers      : {}", cfg.workers);
    log::info!("  Req/worker   : {}", cfg.requests_per_worker);
    log::info!("  Warmup/wkr   : {}", cfg.warmup_per_worker);
    log::info!("  Total measured : {}", cfg.total_requests());
    log::info!("");

    // This process consumes services via locator().get().
    ice_rpc::init_consumer();

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

    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.workers);

    let proxy = db_proxy.expect("DatabaseServiceProxy not initialized");

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

    let stats = compute_stats(&mut all_outcomes, wall);
    print_stats(&cfg, &stats, wall);

    {
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
            match proxy.get_person(query).await {
                Ok(rx) => match rx.recv().await {
                    Ok(ice_rpc::Event::Next(info)) => {
                        log::info!(
                            "  {} {} — {} years old, {}, {}, {}, {}",
                            info.nom,
                            info.prenom,
                            info.age,
                            info.email,
                            info.telephone,
                            info.ville,
                            info.profession
                        );
                    }
                    Ok(ice_rpc::Event::Error(e)) => {
                        log::warn!("  {} {} — Error: {}", nom, prenom, e);
                    }
                    _ => log::warn!("  {} {} — No response", nom, prenom),
                },
                Err(e) => log::error!("  {} {} — IPC error: {}", nom, prenom, e),
            }
        }
    }

    // Clean shutdown: wait for the IPC threads, release the iceoryx2 node.
    // The guard guarantees the token cancellation even on panic before this line.
    log::info!("Stopping benchmark...");
    shutdown_guard.shutdown().await;

    Ok(())
}
