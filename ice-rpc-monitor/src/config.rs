//! Observer configuration and command-line parsing.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Default address the Prometheus endpoint listens on.
const DEFAULT_PROMETHEUS: &str = "127.0.0.1:9898";

/// Runtime configuration of the observer.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the Prometheus endpoint listens on; `None` disables it.
    pub prometheus_addr: Option<SocketAddr>,
    /// Channels to observe. Empty means "discover every channel automatically".
    pub channels: Vec<String>,
    /// How often the channel list is refreshed when discovery is enabled.
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
    /// Poll interval of the node-liveness check.
    pub liveness_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prometheus_addr: DEFAULT_PROMETHEUS.parse().ok(),
            channels: Vec::new(),
            discover_interval: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(5),
            call_ttl: Duration::from_secs(60),
            max_inflight: 100_000,
            trace_sample_rate: 0,
            trace_file: None,
            liveness_interval: Duration::from_secs(1),
        }
    }
}

impl Config {
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
    --discover-interval-ms <ms>    Channel discovery interval (default 2000)
    --call-ttl-ms <ms>             Pending-call timeout (default 60000)
    --max-inflight <n>             Max tracked in-flight calls (default 100000)
    --trace-sample-rate <n>        Emit 1 trace every n calls (0 = off)
    --trace-file <path>            Trace sink (default: stdout)
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
    fn rejects_a_missing_value() {
        let args = vec!["--channel".to_owned()];
        assert!(Config::from_args(&args).is_err());
    }
}
