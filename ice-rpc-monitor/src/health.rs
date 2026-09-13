//! Generic health of the observed system: nodes, services, channels, resources.
//!
//! Two very different cadences share this module. The bus metrics are updated on
//! every observed sample, while the **inventory** (`Node::list` / `Service::list`
//! and, optionally, a filesystem walk) is expensive. It is therefore throttled by
//! [`Scanner::refresh_if_due`] and **conservative on error**: a failed scan keeps
//! the previous snapshot and counts the failure, so a monitoring hiccup never
//! reports a node as dead.

use std::time::{Duration, Instant};

use ice_rpc::monitor::{self, Iceoryx2Layout, NodeInfo, ServiceInfo};

/// Health of one direction of one ice-rpc channel.
#[derive(Debug, Clone)]
pub struct ChannelHealth {
    /// ice-rpc channel (the `#[service(group = …)]` name).
    pub channel: String,
    /// `req` or `resp`.
    pub direction: &'static str,
    /// Whether the observer is attached to this direction.
    pub attached: bool,
    /// Active publishers on the service.
    pub publishers: usize,
    /// Active subscribers, excluding the observer's own subscriber.
    pub subscribers: usize,
    /// Publishers the service was created for.
    pub max_publishers: usize,
    /// Subscribers the service was created for.
    pub max_subscribers: usize,
    /// Largest subscriber buffer the service allows, in samples.
    pub subscriber_buffer: usize,
}

/// Measured on-disk footprint of the iceoryx2 shared-memory segments.
///
/// A **measurement**, not a computation: iceoryx2 exposes no public segment-size
/// getter, so the observer sums the segment files (`prefix + … + .data`) it finds
/// under the iceoryx2 root path and, on Unix, under `/dev/shm`.
///
/// Platform note: this only works where the segments are **file-backed**. On
/// Linux that is the POSIX shared memory in `/dev/shm`; on Windows the segments
/// are OS named mappings, so the walk finds nothing and the metrics report zero
/// (the observer then reports the footprint as unavailable).
#[derive(Debug, Clone, Copy, Default)]
pub struct ShmFootprint {
    /// Total size of the segment files, in bytes.
    pub bytes: u64,
    /// Number of segment files.
    pub segments: u64,
    /// Number of matching files seen (segments and other iceoryx2 files).
    pub files: u64,
}

/// Resources of one observed process (`process-metrics` feature).
#[derive(Debug, Clone)]
pub struct ProcessMetrics {
    /// Process id.
    pub pid: u32,
    /// Process name.
    pub name: String,
    /// CPU usage as a percentage of **one core** (may exceed 100 on a
    /// multi-threaded process).
    pub cpu_percent: f32,
    /// Same usage normalised to `0..=100` over every core, i.e. `cpu_percent`
    /// divided by the number of logical CPUs — comparable to a task manager.
    pub cpu_percent_total: f32,
    /// Resident memory in bytes.
    pub rss_bytes: u64,
    /// Virtual memory in bytes.
    pub virtual_bytes: u64,
    /// Seconds since the process started.
    pub run_time_secs: u64,
}

/// One inventory observation of the machine.
#[derive(Debug, Clone, Default)]
pub struct HealthSnapshot {
    /// Every iceoryx2 node of the machine.
    pub nodes: Vec<NodeInfo>,
    /// Every iceoryx2 service of the machine.
    pub services: Vec<ServiceInfo>,
    /// Filesystem layout of the iceoryx2 resources.
    pub layout: Option<Iceoryx2Layout>,
    /// Measured shared-memory footprint, when the shm scan is enabled.
    pub shm: Option<ShmFootprint>,
    /// Per-process resources, when the `process-metrics` feature is enabled.
    pub processes: Vec<ProcessMetrics>,
    /// Number of logical CPUs of the host, used to normalise the CPU usage.
    pub cpu_count: usize,
    /// Number of successful inventory scans so far.
    pub scans: u64,
    /// Number of failed partial scans so far.
    pub errors: u64,
}

/// Throttled inventory scanner.
pub struct Scanner {
    interval: Duration,
    shm_enabled: bool,
    /// Logical CPUs of the host, read once.
    cpu_count: usize,
    last: Option<Instant>,
    snapshot: HealthSnapshot,
    #[cfg(feature = "process-metrics")]
    system: sysinfo::System,
}

impl Scanner {
    /// Creates a scanner refreshing at most every `interval`, optionally walking
    /// the iceoryx2 directories to measure the shared-memory footprint.
    pub fn new(interval: Duration, shm_enabled: bool) -> Self {
        Self {
            interval,
            shm_enabled,
            cpu_count: std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1),
            last: None,
            snapshot: HealthSnapshot::default(),
            #[cfg(feature = "process-metrics")]
            system: sysinfo::System::new(),
        }
    }

    /// Refreshes the snapshot when the interval elapsed.
    ///
    /// Returns `true` when a scan happened, so the caller can push the new values
    /// to the metrics registry.
    pub fn refresh_if_due(&mut self) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last {
            if now.duration_since(last) < self.interval {
                return false;
            }
        }
        self.last = Some(now);
        self.refresh();
        true
    }

    /// Forces a scan regardless of the interval (first tick, tests).
    pub fn refresh(&mut self) {
        self.snapshot.scans += 1;

        match monitor::list_nodes() {
            Some(nodes) => self.snapshot.nodes = nodes,
            // Keep the previous nodes: an inconclusive scan is not "no node".
            None => self.snapshot.errors += 1,
        }

        match monitor::list_services() {
            Ok(services) => self.snapshot.services = services,
            Err(e) => {
                self.snapshot.errors += 1;
                log::debug!("[monitor] service inventory failed: {e}");
            }
        }

        let layout = monitor::iceoryx2_layout();
        if self.shm_enabled {
            self.snapshot.shm = Some(scan_shm(&layout));
        }
        self.snapshot.layout = Some(layout);
        self.snapshot.cpu_count = self.cpu_count;

        self.refresh_processes();
    }

    /// The latest snapshot.
    pub fn snapshot(&self) -> &HealthSnapshot {
        &self.snapshot
    }

    /// Number of scans performed so far.
    pub fn scans(&self) -> u64 {
        self.snapshot.scans
    }

    /// Number of failed partial scans so far.
    pub fn errors(&self) -> u64 {
        self.snapshot.errors
    }

    /// Refreshes the per-process resources of the known nodes.
    #[cfg(feature = "process-metrics")]
    fn refresh_processes(&mut self) {
        use sysinfo::{Pid, ProcessesToUpdate};

        let pids: Vec<Pid> = self
            .snapshot
            .nodes
            .iter()
            .map(|node| Pid::from_u32(node.pid))
            .collect();
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&pids), true);

        let cpu_count = self.cpu_count.max(1) as f32;
        let metrics: Vec<ProcessMetrics> = self
            .snapshot
            .nodes
            .iter()
            .filter_map(|node| {
                let process = self.system.process(Pid::from_u32(node.pid))?;
                let cpu_percent = process.cpu_usage();
                Some(ProcessMetrics {
                    pid: node.pid,
                    name: process.name().to_string_lossy().into_owned(),
                    cpu_percent,
                    cpu_percent_total: cpu_percent / cpu_count,
                    rss_bytes: process.memory(),
                    virtual_bytes: process.virtual_memory(),
                    run_time_secs: process.run_time(),
                })
            })
            .collect();
        self.snapshot.processes = metrics;
    }

    /// Without the feature there is nothing to refresh.
    #[cfg(not(feature = "process-metrics"))]
    fn refresh_processes(&mut self) {}
}

/// Maximum recursion depth of the shared-memory walk.
const MAX_WALK_DEPTH: usize = 8;

/// Measures the on-disk footprint of the iceoryx2 segments.
fn scan_shm(layout: &Iceoryx2Layout) -> ShmFootprint {
    let mut footprint = ShmFootprint::default();

    // On Unix the POSIX shared memory may live outside the root path.
    #[cfg(unix)]
    let roots = vec![
        std::path::PathBuf::from(&layout.root_path),
        std::path::PathBuf::from("/dev/shm"),
    ];
    #[cfg(not(unix))]
    let roots = vec![std::path::PathBuf::from(&layout.root_path)];

    for root in roots {
        walk(&root, layout, &mut footprint, 0);
    }
    footprint
}

/// Accumulates the size of every iceoryx2 file found under `dir`.
fn walk(dir: &std::path::Path, layout: &Iceoryx2Layout, out: &mut ShmFootprint, depth: usize) {
    if depth > MAX_WALK_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            walk(&entry.path(), layout, out, depth + 1);
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(&layout.prefix) {
            continue;
        }
        out.files += 1;
        if name.ends_with(&layout.data_segment_suffix) {
            out.segments += 1;
            out.bytes += metadata.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scan_reports_nodes_and_services() {
        ice_rpc::gen::setup_iceoryx2_global_config();
        let mut scanner = Scanner::new(Duration::from_millis(1), false);
        scanner.refresh();
        let snapshot = scanner.snapshot();
        // The scan may legitimately find nothing on a quiet machine, but it must
        // not have failed.
        assert_eq!(snapshot.errors, 0);
        assert_eq!(snapshot.scans, 1);
        assert!(snapshot.layout.is_some());
        assert!(snapshot.shm.is_none(), "shm scan is disabled");
        assert!(snapshot.cpu_count >= 1, "the host CPU count is available");
    }

    #[test]
    fn the_shm_walk_sums_only_the_segment_files() {
        let dir = std::env::temp_dir().join(format!("ice-rpc-shm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create dir");
        std::fs::write(dir.join("iox2_aaaa.data"), vec![0u8; 4096]).expect("write segment");
        std::fs::write(dir.join("iox2_bbbb.data"), vec![0u8; 2048]).expect("write segment");
        // A file that does not match the naming scheme must be ignored.
        std::fs::write(dir.join("unrelated.bin"), vec![0u8; 999]).expect("write other");

        let layout = Iceoryx2Layout {
            root_path: dir.to_string_lossy().into_owned(),
            service_dir: String::new(),
            node_dir: String::new(),
            prefix: "iox2_".to_owned(),
            data_segment_suffix: ".data".to_owned(),
        };
        let footprint = scan_shm(&layout);
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(footprint.segments, 2);
        assert_eq!(footprint.files, 2);
        assert_eq!(footprint.bytes, 4096 + 2048);
    }
}
