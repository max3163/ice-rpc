//! ice-rpc Provider hosting three services: ConfigService, DatabaseService, HttpService.
//!
//! Starts an ice-rpc node with three interconnected services:
//! - **ConfigService** : loads a TOML file.
//! - **DatabaseService** : depends on ConfigService for its configuration.
//! - **HttpService** : accepts payloads up to 100 MB.
//!
//! ## HTTP REST gateway
//!
//! To enable the HTTP gateway, set the `ICE_HTTP_PORT` environment variable
//! before launching:
//!
//! ```bash
//! # Requires the `tokio` feature: the service implementations use
//! # tokio::time::sleep / tokio::spawn, so the handlers run on Tokio.
//! cargo run --example provider-app --features tokio
//! cargo run --example provider-app --features tokio,http -- --http
//! # or: ICE_HTTP_PORT=8080 cargo run --example provider-app --features tokio,http
//! ```
//!
//! The services will then be accessible via:
//! - `GET  http://localhost:8080/ConfigService/get?key=database.url`
//! - `GET  http://localhost:8080/DatabaseService/get_user_age?name=Alice`
//! - `POST http://localhost:8080/DatabaseService/get_person` with a JSON body
//! - `POST http://localhost:8080/HttpService/send_request` with a JSON body

#![allow(missing_docs)] // test/example target: documented by Readme.md, not part of a published API
#![allow(clippy::unwrap_used)] // tests/examples/benches may panic; production libs keep the deny, see [workspace.lints]
use async_trait::async_trait;
use common::{
    ConfigError, ConfigService, ConfigServiceProxy, DatabaseError, DatabaseService,
    DatabaseServiceProxy, HttpError, HttpRequestParams, HttpResponseParams, HttpService,
    HttpServiceProxy, NotificationService, NotificationServiceProxy, PersonneInfo, PersonneQuery,
};
use ice_rpc::{from, of, throw_error, RxStreamExt};
use ice_rpc::{Observable, ServiceInit, StreamError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "http")]
/// Default port of the HTTP REST gateway (enabled via the
/// `ICE_HTTP_PORT` environment variable or the `--http` argument).
const DEFAULT_HTTP_PORT: u16 = 8080;

pub struct ConfigServiceImpl {
    config_path: String,
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl ConfigServiceImpl {
    pub fn new(config_path: impl Into<String>) -> Self {
        Self {
            config_path: config_path.into(),
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ConfigService for ConfigServiceImpl {
    async fn get(&self, key: String) -> Observable<String, ConfigError> {
        // Simulation of a long processing (file read, DB query…)
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let store = self.store.read().await;
        match store.get(&key).cloned() {
            // Single-response service: no channel, no task — just `of(value)`.
            Some(value) => of(value),
            None => throw_error(ConfigError::KeyNotFound),
        }
    }
}

#[async_trait]
impl ServiceInit for ConfigServiceImpl {
    async fn on_init(&self) -> bool {
        log::info!(
            "[ConfigService] Loading configuration: {}",
            self.config_path
        );

        log::info!("[ConfigService] Connectivity check ...");
        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

        let content = match std::fs::read_to_string(&self.config_path) {
            Ok(c) => c,
            Err(e) => {
                log::error!("[ConfigService] Failed to read {}: {}", self.config_path, e);
                return false;
            }
        };

        let toml_value: toml::Value = match toml::from_str(&content) {
            Ok(v) => v,
            Err(e) => {
                log::error!("[ConfigService] Invalid TOML file: {}", e);
                return false;
            }
        };

        let mut store = self.store.write().await;
        flatten_toml("", &toml_value, &mut store);

        log::info!("[ConfigService] {} key(s) loaded in memory.", store.len());
        for (k, v) in store.iter() {
            log::debug!("[ConfigService]   {} = {}", k, v);
        }
        true
    }
}

fn flatten_toml(prefix: &str, value: &toml::Value, out: &mut HashMap<String, String>) {
    match value {
        toml::Value::Table(table) => {
            for (k, v) in table {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                flatten_toml(&key, v, out);
            }
        }
        other => {
            out.insert(
                prefix.to_string(),
                other.to_string().trim_matches('"').to_string(),
            );
        }
    }
}

#[derive(Default)]
pub struct DatabaseServiceImpl {
    db_url: Arc<RwLock<Option<String>>>,
    config_proxy: Arc<RwLock<Option<Arc<ConfigServiceProxy>>>>,
}

impl DatabaseServiceImpl {
    pub fn new() -> Self {
        Self {
            db_url: Arc::new(RwLock::new(None)),
            config_proxy: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl DatabaseService for DatabaseServiceImpl {
    async fn get_user_age(&self, name: String) -> Observable<i32, DatabaseError> {
        match name.as_str() {
            "Alice" => of(30),
            "Bob" => of(42),
            "Charlie" => of(25),
            "Diana" => of(35),
            "Eve" => of(28),
            "Frank" => of(45),
            "Grace" => of(31),
            "Heidi" => of(27),
            "Ivan" => of(38),
            "Judy" => of(33),
            _ => throw_error(DatabaseError::NotFound),
        }
    }

    async fn get_person(&self, query: PersonneQuery) -> Observable<PersonneInfo, DatabaseError> {
        match (query.nom.as_str(), query.prenom.as_str()) {
            ("Dupont", "Jean") => of(PersonneInfo {
                nom: "Dupont".into(),
                prenom: "Jean".into(),
                age: 45,
                ville: "Paris".into(),
                profession: "Engineer".into(),
            }),
            ("Martin", "Marie") => of(PersonneInfo {
                nom: "Martin".into(),
                prenom: "Marie".into(),
                age: 32,
                ville: "Lyon".into(),
                profession: "Doctor".into(),
            }),
            ("Bernard", "Pierre") => of(PersonneInfo {
                nom: "Bernard".into(),
                prenom: "Pierre".into(),
                age: 28,
                ville: "Marseille".into(),
                profession: "Architect".into(),
            }),
            ("Petit", "Sophie") => of(PersonneInfo {
                nom: "Petit".into(),
                prenom: "Sophie".into(),
                age: 39,
                ville: "Bordeaux".into(),
                profession: "Lawyer".into(),
            }),
            ("Thomas", "Luc") => of(PersonneInfo {
                nom: "Thomas".into(),
                prenom: "Luc".into(),
                age: 51,
                ville: "Lille".into(),
                profession: "Teacher".into(),
            }),
            _ => throw_error(DatabaseError::NotFound),
        }
    }
}

#[async_trait]
impl ServiceInit for DatabaseServiceImpl {
    fn dependencies(&self) -> Vec<&'static str> {
        vec!["ConfigService"]
    }

    async fn on_init(&self) -> bool {
        log::info!("[DatabaseService] Initialization (ConfigService guaranteed ready)...");

        // get() retrieves ConfigServiceProxy from the ServiceLocator cache
        // (it was registered by the Provider before initialize_all()).
        let config = ice_rpc::locator().get::<ConfigServiceProxy>().await.expect(
            "[DatabaseService] BUG: ConfigServiceProxy missing despite the declared dependency",
        );

        *self.config_proxy.write().await = Some(config.clone());

        let rx = config.get("database.url".into()).await;

        let db_url = match rx.first_value().await {
            Ok(url) => url,
            Err(StreamError::Business(ConfigError::KeyNotFound)) => {
                log::error!("[DatabaseService] Key \"database.url\" missing from the config.");
                return false;
            }
            Err(_) => {
                log::error!("[DatabaseService] Unexpected response from ConfigService.");
                return false;
            }
        };

        log::info!("[DatabaseService] Default URL retrieved: {}", db_url);
        log::info!("[DatabaseService] Opening the connection pool...");
        for step in 1..=3 {
            log::info!("[DatabaseService]   connection {}/3...", step);
            tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
        }

        *self.db_url.write().await = Some(db_url);
        log::info!("[DatabaseService] Pool ready, IPC server starting.");
        true
    }
}

pub struct HttpServiceImpl {
    max_payload_bytes: u64,
}

impl HttpServiceImpl {
    pub fn new(max_payload_bytes: u64) -> Self {
        Self { max_payload_bytes }
    }
}

#[async_trait]
impl HttpService for HttpServiceImpl {
    async fn send_request(
        &self,
        request: HttpRequestParams,
    ) -> Observable<HttpResponseParams, HttpError> {
        let req_body_len = request.body.len() as u64;
        let max_payload = self.max_payload_bytes;
        log::info!(
            "[HttpService] Request received: {} {} ({} headers, body = {:.2} MB)",
            request.method,
            request.url,
            request.headers.len(),
            req_body_len as f64 / (1024.0 * 1024.0),
        );

        if req_body_len > max_payload {
            return throw_error(HttpError::PayloadTooLarge {
                max_bytes: max_payload,
                actual_bytes: req_body_len,
            });
        }

        of(HttpResponseParams {
            status_code: 200,
            status_text: "OK".to_string(),
            headers: vec![
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
                ("x-echo-size".to_string(), format!("{}", req_body_len)),
                ("x-echo-method".to_string(), request.method.clone()),
            ],
            body: request.body,
        })
    }
}

#[async_trait]
impl ice_rpc::ServiceInit for HttpServiceImpl {
    async fn on_init(&self) -> bool {
        log::info!(
            "[HttpService] Initialized, max payload size: {} MB",
            self.max_payload_bytes / (1024 * 1024)
        );
        true
    }
}

/// Streams notifications to its subscribers — the provider side of `subscribe`.
struct NotificationServiceImpl;

#[async_trait]
impl NotificationService for NotificationServiceImpl {
    async fn watch(&self, count: u32) -> Observable<u32, String> {
        // A pure pipeline — no channel, no task, no `spawn`: one value every
        // 100 ms, then `Complete`. `into_observable()` freezes it into the
        // concrete `Observable` required by the service signature.
        //
        // Trade-off: a pipeline cannot detect that the consumer unsubscribed, so
        // it runs to completion. A `channel` + `send_next` (which errors on a
        // closed receiver) is the way to stop early.
        from(1..=count)
            .delay(std::time::Duration::from_millis(100))
            .into_observable()
    }

    async fn ping(&self) -> Observable<u32, String> {
        // Single response: one wire sample (`CompleteWith`).
        of(0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    log::info!("=== PROVIDER STARTUP ===");

    // `run_provider!` performs the full bootstrap (init + shutdown): neither a
    // guard nor an explicit `init()` is needed here.
    //
    // DatabaseServiceImpl::on_init() calls locator().get::<ConfigServiceProxy>()
    // — this provider also consumes a service internally.

    // ── HTTP REST gateway (optional, requires the `http` feature) ──
    #[cfg(feature = "http")]
    {
        let http_port: Option<u16> = std::env::var("ICE_HTTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .or_else(|| {
                let args: Vec<String> = std::env::args().collect();
                if args.iter().any(|a| a == "--http") {
                    Some(DEFAULT_HTTP_PORT)
                } else {
                    None
                }
            });

        if let Some(port) = http_port {
            log::info!(
                "🌐 Starting the HTTP REST gateway on port {} (in the background)...",
                port
            );
            log::info!(
                "   Example: curl http://localhost:{}/DatabaseService/get_user_age?name=Alice",
                port
            );
            // Starts the HTTP server in the background via tokio::spawn.
            // The task will stop automatically when the global cancellation
            // token is triggered (Ctrl+C).
            tokio::spawn(async move {
                ice_rpc::start_http_gateway!(
                    port,
                    DatabaseServiceProxy,
                    ConfigServiceProxy,
                    HttpServiceProxy
                )
                .await;
            });
            // Give the server time to bind to the port.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    ice_rpc::run_provider!(
        DatabaseServiceProxy::provide_with_init(DatabaseServiceImpl::new()),
        ConfigServiceProxy::provide_with_init(ConfigServiceImpl::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/config.toml"
        ))),
        HttpServiceProxy::provide_with_init(HttpServiceImpl::new(100 * 1024 * 1024)),
        NotificationServiceProxy::provide(NotificationServiceImpl),
    )
    .await
}
