//! Observer configuration and command-line parsing.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ice_rpc::monitor::Decoders;

use crate::traces::TraceFormat;

/// Default address the Prometheus endpoint listens on.
const DEFAULT_PROMETHEUS: &str = "127.0.0.1:9898";

/// Level of detail captured by the observer.
///
/// The two modes exist because reading the payload is not free: capturing the
/// message content costs a copy per sample, which is only acceptable when the
/// traffic is moderate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Metadata only: the payload is **never** read. Built for high throughput,
    /// it still yields counts, throughput, error kinds and exact latencies.
    Stats,
    /// Also captures the raw payload bytes (hex-encoded) of every sample. Meant
    /// for debugging at moderate throughput.
    Detail,
}

/// Runtime configuration of the observer.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the Prometheus endpoint listens on; `None` disables it.
    pub prometheus_addr: Option<SocketAddr>,
    /// Channels to observe. Empty means "discover every channel automatically".
    pub channels: Vec<String>,
    /// Global capture mode.
    pub mode: Mode,
    /// Channels forced to [`Mode::Detail`] even when the global mode is `Stats`.
    pub detail_channels: Vec<String>,
    /// How often the channel list is refreshed when discovery is enabled.
    ///
    /// Paces the *listing* of the channels only, which costs a scan of the
    /// service registry: attaching to a channel already known is retried far more
    /// often, so a provider that starts after the observer is observed from its
    /// first call instead of the next discovery tick.
    pub discover_interval: Duration,
    /// How often correlation entries older than [`Config::call_ttl`] are swept.
    pub sweep_interval: Duration,
    /// A pending call older than this is considered lost.
    pub call_ttl: Duration,
    /// Maximum number of in-flight calls tracked before the oldest are evicted.
    pub max_inflight: usize,
    /// Emit one trace record every `trace_sample_rate` completed calls (0 = off).
    pub trace_sample_rate: u64,
    /// Trace sink: a file, or `None` for stdout.
    pub trace_file: Option<PathBuf>,
    /// Format of the emitted trace records (NDJSON or human-readable lines).
    pub trace_format: TraceFormat,
    /// Decoders that turn observed payloads into human-readable text.
    ///
    /// Only used in detail mode; an empty registry leaves the payloads opaque.
    pub decoders: Arc<Decoders>,
    /// Interval of the generic health inventory (nodes, services, shm).
    ///
    /// `Node::list` / `Service::list` are expensive, so the scan is throttled.
    /// Zero disables the inventory entirely.
    pub health_interval: Duration,
    /// Whether the health inventory also measures the on-disk shared-memory
    /// footprint (a filesystem walk).
    pub health_shm: bool,
    /// Whether to draw a `top`-like live view (redrawn in place, no scrolling).
    ///
    /// Ignored when stdout is not a terminal. In detail mode the messages are
    /// then kept in a bounded in-memory buffer and shown inside the frame.
    pub console_live: bool,
    /// Poll interval of the node-liveness check.
    pub liveness_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prometheus_addr: DEFAULT_PROMETHEUS.parse().ok(),
            channels: Vec::new(),
            mode: Mode::Stats,
            detail_channels: Vec::new(),
            discover_interval: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(5),
            call_ttl: Duration::from_secs(60),
            max_inflight: 100_000,
            trace_sample_rate: 0,
            trace_file: None,
            trace_format: TraceFormat::Json,
            decoders: Arc::new(Decoders::new()),
            health_interval: Duration::from_secs(2),
            health_shm: false,
            console_live: false,
            liveness_interval: Duration::from_secs(1),
        }
    }
}

impl Config {
    /// Returns `true` when `channel` must be observed in detail mode.
    pub fn detail_for(&self, channel: &str) -> bool {
        self.mode == Mode::Detail || self.detail_channels.iter().any(|name| name == channel)
    }

    /// Parses `--key value` arguments, falling back to the defaults.
    ///
    /// # Errors
    /// Returns a human-readable message on an unknown flag or a malformed value.
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        let mut config = Self::default();
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            // A flag that takes a value reads the next argument.
            let value = |i: &mut usize| -> Result<&str, String> {
                *i += 1;
                args.get(*i)
                    .map(String::as_str)
                    .ok_or_else(|| format!("missing value for {arg}"))
            };
            match arg {
                "--prometheus" => {
                    let v = value(&mut i)?;
                    config.prometheus_addr = if v == "off" {
                        None
                    } else {
                        Some(
                            v.parse()
                                .map_err(|e| format!("invalid --prometheus address: {e}"))?,
                        )
                    };
                }
                "--channel" => config.channels.push(value(&mut i)?.to_owned()),
                // Shorthand for the full capture mode: keeps the common case short.
                "--detail" => config.mode = Mode::Detail,
                "--mode" => {
                    config.mode = match value(&mut i)? {
                        "stats" => Mode::Stats,
                        "detail" => Mode::Detail,
                        other => {
                            return Err(format!(
                                "invalid --mode '{other}' (expected 'stats' or 'detail')"
                            ))
                        }
                    };
                }
                "--detail-channel" => config.detail_channels.push(value(&mut i)?.to_owned()),
                "--discover-interval-ms" => {
                    config.discover_interval = Duration::from_millis(parse_u64(value(&mut i)?)?);
                }
                "--call-ttl-ms" => {
                    config.call_ttl = Duration::from_millis(parse_u64(value(&mut i)?)?);
                }
                "--max-inflight" => {
                    config.max_inflight = parse_u64(value(&mut i)?)? as usize;
                }
                "--trace-sample-rate" => {
                    config.trace_sample_rate = parse_u64(value(&mut i)?)?;
                }
                "--trace-file" => config.trace_file = Some(PathBuf::from(value(&mut i)?)),
                "--trace-format" => {
                    config.trace_format = match value(&mut i)? {
                        "json" => TraceFormat::Json,
                        "human" => TraceFormat::Human,
                        other => {
                            return Err(format!(
                                "invalid --trace-format '{other}' (expected 'json' or 'human')"
                            ))
                        }
                    };
                }
                "--health-interval-ms" => {
                    let ms = parse_u64(value(&mut i)?)?;
                    // 0 disables the inventory; the collector treats it as `None`.
                    config.health_interval = if ms == 0 {
                        Duration::ZERO
                    } else {
                        Duration::from_millis(ms)
                    };
                }
                "--health-shm" => config.health_shm = true,
                "--live" => config.console_live = true,
                "--help" | "-h" => return Err(HELP.to_owned()),
                other => return Err(format!("unknown argument '{other}'\n{HELP}")),
            }
            i += 1;
        }
        Ok(config)
    }
}

/// Parses a decimal `u64`.
fn parse_u64(raw: &str) -> Result<u64, String> {
    raw.parse()
        .map_err(|e| format!("invalid number '{raw}': {e}"))
}

/// Usage text.
pub const HELP: &str = "\
ice-rpc-monitor — out-of-band observer for ice-rpc

USAGE:
    ice-rpc-monitor [OPTIONS]

OPTIONS:
    --prometheus <addr|off>        Prometheus endpoint (default 127.0.0.1:9898)
    --channel <name>               Observe only this channel (repeatable)
    --detail                       Shorthand for `--mode detail` (full capture)
    --mode <stats|detail>          Capture mode (default stats)
                                   stats  = metadata only, never reads the payload
                                   detail = also captures the payload (hex), costs a
                                            copy per sample: use it at lower throughput
    --detail-channel <name>        Force detail mode on this channel only (repeatable)
    --discover-interval-ms <ms>    Channel discovery interval (default 2000)
    --call-ttl-ms <ms>             Pending-call timeout (default 60000)
    --max-inflight <n>             Max tracked in-flight calls (default 100000)
    --trace-sample-rate <n>        Emit 1 trace every n calls (0 = off)
    --trace-file <path>            Trace sink (default: stdout)
    --trace-format <json|human>    Trace record format (default json)
    --health-interval-ms <ms>      Node/service inventory interval (default 2000, 0 = off)
    --health-shm                   Also measure the shared-memory footprint on disk
    --live                         Redraw the stats in place, like `top` (needs a terminal)
    -h, --help                     Show this help";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_usable() {
        let config = Config::default();
        assert_eq!(
            config.prometheus_addr,
            Some("127.0.0.1:9898".parse().unwrap())
        );
        assert!(config.channels.is_empty());
        assert_eq!(config.mode, Mode::Stats);
        assert_eq!(config.trace_sample_rate, 0);
    }

    #[test]
    fn parses_channels_and_disables_prometheus() {
        let args: Vec<String> = [
            "--channel",
            "DatabaseService",
            "--channel",
            "ConfigService",
            "--prometheus",
            "off",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        let config = Config::from_args(&args).expect("valid args");
        assert_eq!(config.channels, vec!["DatabaseService", "ConfigService"]);
        assert_eq!(config.prometheus_addr, None);
    }

    #[test]
    fn detail_mode_applies_globally_or_per_channel() {
        let global = Config::from_args(&["--mode".to_owned(), "detail".to_owned()]).unwrap();
        assert!(global.detail_for("Anything"));

        let per_channel: Vec<String> = ["--detail-channel", "DatabaseService"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let config = Config::from_args(&per_channel).unwrap();
        assert_eq!(config.mode, Mode::Stats);
        assert!(config.detail_for("DatabaseService"));
        assert!(!config.detail_for("ConfigService"));
    }

    #[test]
    fn rejects_a_missing_value_and_an_unknown_mode() {
        assert!(Config::from_args(&["--channel".to_owned()]).is_err());
        assert!(Config::from_args(&["--mode".to_owned(), "verbose".to_owned()]).is_err());
    }

    #[test]
    fn detail_flag_is_a_shorthand_for_the_detail_mode() {
        let config = Config::from_args(&["--detail".to_owned()]).expect("valid args");
        assert_eq!(config.mode, Mode::Detail);
        assert!(config.detail_for("Anything"));
        // Default stays stats-only.
        assert_eq!(Config::default().mode, Mode::Stats);
        assert_eq!(Config::default().trace_format, TraceFormat::Json);
    }

    #[test]
    fn trace_format_accepts_human() {
        let args: Vec<String> = ["--trace-format", "human"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let config = Config::from_args(&args).expect("valid args");
        assert_eq!(config.trace_format, TraceFormat::Human);
        assert!(Config::from_args(&["--trace-format".to_owned(), "xml".to_owned()]).is_err());
    }
}
