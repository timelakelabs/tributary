//! Host-metrics collector (#25) — the Telegraf `system` input family.
//!
//! tributary ships logs; this samples the machine on an interval and writes
//! the SAME measurements Telegraf does — `cpu`, `mem`, `disk`, `net`,
//! `system`, `swap` — with Telegraf's exact measurement, field and tag
//! names, so a dashboard built against InfluxDB + Telegraf keeps working
//! after the swap instead of going blank.
//!
//! It is one cross-platform code path: `sysinfo` reads `/proc` on Linux and
//! the equivalent APIs on Windows, and the mapping below turns whatever it
//! reports into the canonical names. The values are gathered per tick and
//! all rows in a tick share ONE timestamp — distinct series (a different
//! `cpu`/`device`/`interface` tag) are distinct primary keys, so nothing
//! collides and the log stamper's per-record disambiguation (which would
//! desync `cpu0` from `cpu-total`) is deliberately NOT used here.
//!
//! The records ride the same durable [`Queue`] -> [`Shipper`] path a source
//! uses: spooled, retried, watermarked like everything else.
//!
//! On Linux `cpu` carries the full Telegraf per-state breakdown
//! (`usage_user`/`usage_system`/`usage_iowait`/...) read from `/proc/stat`
//! (#28). `sysinfo` only reports one aggregate percentage per core, so on
//! every other platform `cpu` still carries `usage_idle`/`usage_active` only.
//!
//! Known gaps (documented, not bugs): disk inodes are unix-only (`statvfs`;
//! Windows has no inode concept); `load*` is zero on Windows, which has no
//! load-average concept.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sysinfo::{Disks, Networks, System};

use crate::config::{FieldValue, Metrics};
use crate::lp::{Record, Value};
use crate::queue::Queue;
use crate::ship::Shipper;

// ---- plain samples (what a collector reads; the unit of testability) ------

pub struct MemSample {
    pub total: u64,
    pub available: u64,
    pub used: u64,
    pub free: u64,
}

pub struct SwapSample {
    pub total: u64,
    pub used: u64,
    pub free: u64,
}

/// One core, or the aggregate. `name` is the `cpu` tag value: `cpu-total`
/// for the aggregate, `cpu0`, `cpu1`, ... per core — GENERATED, not taken
/// from `sysinfo`, whose per-core naming differs by platform. `usage` is the
/// active percentage (so `usage_idle` = 100 - usage); `states` is the Linux
/// `/proc/stat` per-state split (#28), `None` where only the aggregate is
/// available.
pub struct CpuSample {
    pub name: String,
    pub usage: f64,
    pub states: Option<CpuStates>,
}

/// The per-state CPU breakdown Telegraf's `cpu` carries, each a percentage of
/// the tick's total jiffies. The nine sum to `usage_active` (everything that
/// is not idle) — i.e. `100 - usage_idle`.
#[derive(Clone, Copy, Default)]
pub struct CpuStates {
    pub user: f64,
    pub system: f64,
    pub iowait: f64,
    pub nice: f64,
    pub irq: f64,
    pub softirq: f64,
    pub steal: f64,
    pub guest: f64,
    pub guest_nice: f64,
}

pub struct DiskSample {
    pub device: String,
    pub path: String,
    pub fstype: String,
    pub total: u64,
    pub free: u64,
    /// Inode counts from `statvfs`; `None` where the filesystem has no inode
    /// concept — Windows, and pseudo-filesystems that report zero.
    pub inodes: Option<InodeStats>,
}

/// Inode usage for a mount, the way Telegraf's `disk` reports it. A
/// filesystem can exhaust these long before it runs out of bytes, at which
/// point writes fail on a volume a byte-only dashboard swears is half empty.
#[derive(Clone, Copy)]
pub struct InodeStats {
    pub total: u64,
    pub free: u64,
    pub used: u64,
}

/// Build `InodeStats` from `statvfs` counts. A pseudo-filesystem (proc,
/// sysfs, some tmpfs) reports `f_files == 0` — there is nothing to report,
/// and an `inodes_used` computed from it would be a lie — so return `None`.
fn inode_stats_from(files: u64, ffree: u64) -> Option<InodeStats> {
    if files == 0 {
        return None;
    }
    Some(InodeStats {
        total: files,
        free: ffree,
        used: files.saturating_sub(ffree),
    })
}

pub struct NetSample {
    pub iface: String,
    pub bytes_recv: u64,
    pub bytes_sent: u64,
    pub packets_recv: u64,
    pub packets_sent: u64,
    pub err_in: u64,
    pub err_out: u64,
}

pub struct SystemSample {
    pub load1: f64,
    pub load5: f64,
    pub load15: f64,
    pub n_cpus: u64,
    pub uptime: u64,
}

// ---- the context every row carries: host tag + global tags + static fields

fn fieldvalue_to_lp(v: &FieldValue) -> Value {
    match v {
        FieldValue::Bool(b) => Value::Bool(*b),
        FieldValue::Integer(i) => Value::Int(*i),
        FieldValue::Float(f) => Value::Float(*f),
        FieldValue::Str(s) => Value::Str(s.clone()),
    }
}

/// The "additional fields" half of #25: constant tags and fields stamped on
/// every emitted point. `host` and the structural tags (`cpu`/`device`/...)
/// always win a name collision with a global tag; a metric's own field wins
/// a collision with a static field — so a stray `[metrics.global_tags] host`
/// or `[metrics.static_fields] used` cannot corrupt a series or emit a
/// duplicate field key.
pub struct Ctx {
    pub host: String,
    pub global_tags: BTreeMap<String, String>,
    pub static_fields: Vec<(String, Value)>,
}

impl Ctx {
    pub fn from_cfg(m: &Metrics) -> Self {
        let host = m
            .host
            .clone()
            .or_else(System::host_name)
            .unwrap_or_else(|| "unknown".to_string());
        let static_fields = m
            .static_fields
            .iter()
            .map(|(k, v)| (k.clone(), fieldvalue_to_lp(v)))
            .collect();
        Ctx {
            host,
            global_tags: m.global_tags.clone(),
            static_fields,
        }
    }

    /// Merge global tags, the `host` tag, and this measurement's structural
    /// tags into a canonical (key-sorted) set. Sorted because the tag set is
    /// the primary key: a stable order keeps a replayed point byte-identical.
    fn tags(&self, structural: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut m: BTreeMap<String, String> = self.global_tags.clone();
        m.insert("host".to_string(), self.host.clone());
        for (k, v) in structural {
            m.insert((*k).to_string(), (*v).to_string());
        }
        m.into_iter().collect()
    }

    /// The measurement's own fields, then any static fields whose name does
    /// not already exist (a duplicate field key is an unparseable line).
    fn fields(&self, base: Vec<(&str, Value)>) -> Vec<(String, Value)> {
        let taken: HashSet<&str> = base.iter().map(|(k, _)| *k).collect();
        let mut f: Vec<(String, Value)> =
            base.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        for (k, v) in &self.static_fields {
            if !taken.contains(k.as_str()) {
                f.push((k.clone(), v.clone()));
            }
        }
        f
    }
}

fn pct(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

// ---- pure mapping: sample -> Record with Telegraf names (golden-tested) ----

pub fn mem_record(s: &MemSample, ctx: &Ctx, ts_ns: i64) -> Record {
    Record {
        table: "mem".to_string(),
        tags: ctx.tags(&[]),
        fields: ctx.fields(vec![
            ("total", Value::Int(s.total as i64)),
            ("available", Value::Int(s.available as i64)),
            ("used", Value::Int(s.used as i64)),
            ("free", Value::Int(s.free as i64)),
            ("used_percent", Value::Float(pct(s.used, s.total))),
            ("available_percent", Value::Float(pct(s.available, s.total))),
        ]),
        ts_ns,
    }
}

pub fn swap_record(s: &SwapSample, ctx: &Ctx, ts_ns: i64) -> Record {
    Record {
        table: "swap".to_string(),
        tags: ctx.tags(&[]),
        fields: ctx.fields(vec![
            ("total", Value::Int(s.total as i64)),
            ("used", Value::Int(s.used as i64)),
            ("free", Value::Int(s.free as i64)),
            ("used_percent", Value::Float(pct(s.used, s.total))),
        ]),
        ts_ns,
    }
}

pub fn cpu_record(s: &CpuSample, ctx: &Ctx, ts_ns: i64) -> Record {
    let mut fields = vec![
        ("usage_idle", Value::Float((100.0 - s.usage).max(0.0))),
        ("usage_active", Value::Float(s.usage)),
    ];
    // On Linux the full /proc/stat split is present (#28). Elsewhere sysinfo
    // gives one aggregate only, so idle/active is all that is honest — do NOT
    // synthesise usage_user from the aggregate, it isn't user time.
    if let Some(st) = &s.states {
        fields.extend([
            ("usage_user", Value::Float(st.user)),
            ("usage_system", Value::Float(st.system)),
            ("usage_iowait", Value::Float(st.iowait)),
            ("usage_nice", Value::Float(st.nice)),
            ("usage_irq", Value::Float(st.irq)),
            ("usage_softirq", Value::Float(st.softirq)),
            ("usage_steal", Value::Float(st.steal)),
            ("usage_guest", Value::Float(st.guest)),
            ("usage_guest_nice", Value::Float(st.guest_nice)),
        ]);
    }
    Record {
        table: "cpu".to_string(),
        tags: ctx.tags(&[("cpu", &s.name)]),
        fields: ctx.fields(fields),
        ts_ns,
    }
}

pub fn disk_record(s: &DiskSample, ctx: &Ctx, ts_ns: i64) -> Record {
    let used = s.total.saturating_sub(s.free);
    let mut fields = vec![
        ("total", Value::Int(s.total as i64)),
        ("free", Value::Int(s.free as i64)),
        ("used", Value::Int(used as i64)),
        ("used_percent", Value::Float(pct(used, s.total))),
    ];
    // Inodes are present on unix filesystems that have them (#29); a volume
    // can be ENOSPC on inodes while bytes look fine, and this is the field
    // that shows it.
    if let Some(i) = &s.inodes {
        fields.extend([
            ("inodes_total", Value::Int(i.total as i64)),
            ("inodes_free", Value::Int(i.free as i64)),
            ("inodes_used", Value::Int(i.used as i64)),
        ]);
    }
    Record {
        table: "disk".to_string(),
        tags: ctx.tags(&[
            ("device", &s.device),
            ("path", &s.path),
            ("fstype", &s.fstype),
        ]),
        fields: ctx.fields(fields),
        ts_ns,
    }
}

pub fn net_record(s: &NetSample, ctx: &Ctx, ts_ns: i64) -> Record {
    // Trap 4: these are cumulative counters — emit as-is, dashboards take the
    // derivative themselves. (sysinfo counts from first observation, not
    // boot; the derivative is identical.)
    Record {
        table: "net".to_string(),
        tags: ctx.tags(&[("interface", &s.iface)]),
        fields: ctx.fields(vec![
            ("bytes_recv", Value::Int(s.bytes_recv as i64)),
            ("bytes_sent", Value::Int(s.bytes_sent as i64)),
            ("packets_recv", Value::Int(s.packets_recv as i64)),
            ("packets_sent", Value::Int(s.packets_sent as i64)),
            ("err_in", Value::Int(s.err_in as i64)),
            ("err_out", Value::Int(s.err_out as i64)),
        ]),
        ts_ns,
    }
}

pub fn system_record(s: &SystemSample, ctx: &Ctx, ts_ns: i64) -> Record {
    Record {
        table: "system".to_string(),
        tags: ctx.tags(&[]),
        fields: ctx.fields(vec![
            ("load1", Value::Float(s.load1)),
            ("load5", Value::Float(s.load5)),
            ("load15", Value::Float(s.load15)),
            ("n_cpus", Value::Int(s.n_cpus as i64)),
            ("uptime", Value::Int(s.uptime as i64)),
        ]),
        ts_ns,
    }
}

// ---- gather: sysinfo -> samples --------------------------------------------

fn gather_mem(sys: &System) -> MemSample {
    MemSample {
        total: sys.total_memory(),
        available: sys.available_memory(),
        used: sys.used_memory(),
        free: sys.free_memory(),
    }
}

fn gather_swap(sys: &System) -> SwapSample {
    SwapSample {
        total: sys.total_swap(),
        used: sys.used_swap(),
        free: sys.free_swap(),
    }
}

// What the collector holds between ticks to compute the CPU delta. On Linux
// it is the previous `/proc/stat` reading; elsewhere `sysinfo` needs no
// carried state, so it is the unit type.
#[cfg(target_os = "linux")]
type CpuBaseline = Vec<(String, CpuTimes)>;
#[cfg(not(target_os = "linux"))]
type CpuBaseline = ();

/// Cumulative CPU jiffies from one `/proc/stat` line. `guest`/`guest_nice`
/// are the last two columns and exist only on newer kernels — a short line
/// leaves them 0.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Default)]
struct CpuTimes {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
    guest: u64,
    guest_nice: u64,
}

/// Parse the `cpu`/`cpuN` lines of `/proc/stat` into `(name, times)`, where
/// `name` is the `cpu` TAG value: the aggregate `cpu` line becomes
/// `cpu-total`, `cpu0` stays `cpu0`. The cpu lines lead the file and are
/// contiguous, so we stop at the first non-cpu line (`intr`, `ctxt`, ...).
/// Missing trailing columns default to 0 (older kernels).
#[cfg(target_os = "linux")]
fn parse_proc_stat(text: &str) -> Vec<(String, CpuTimes)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let Some(label) = it.next() else { continue };
        if !label.starts_with("cpu") {
            break;
        }
        let name = if label == "cpu" {
            "cpu-total".to_string()
        } else {
            label.to_string()
        };
        let v: Vec<u64> = it.map(|t| t.parse().unwrap_or(0)).collect();
        let g = |i: usize| v.get(i).copied().unwrap_or(0);
        out.push((
            name,
            CpuTimes {
                user: g(0),
                nice: g(1),
                system: g(2),
                idle: g(3),
                iowait: g(4),
                irq: g(5),
                softirq: g(6),
                steal: g(7),
                guest: g(8),
                guest_nice: g(9),
            },
        ));
    }
    out
}

/// The per-state percentages between two readings of one cpu line, plus the
/// active percentage (`100 - idle`). `/proc/stat` counts `guest` inside
/// `user` and `guest_nice` inside `nice` (a kernel quirk), so those are
/// subtracted back out of user/nice to avoid double-counting — matching what
/// gopsutil, and therefore Telegraf, reports.
#[cfg(target_os = "linux")]
fn cpu_delta(prev: &CpuTimes, curr: &CpuTimes) -> (f64, CpuStates) {
    let d = |c: u64, p: u64| c.saturating_sub(p);
    let user = d(curr.user, prev.user);
    let nice = d(curr.nice, prev.nice);
    let system = d(curr.system, prev.system);
    let idle = d(curr.idle, prev.idle);
    let iowait = d(curr.iowait, prev.iowait);
    let irq = d(curr.irq, prev.irq);
    let softirq = d(curr.softirq, prev.softirq);
    let steal = d(curr.steal, prev.steal);
    let guest = d(curr.guest, prev.guest);
    let guest_nice = d(curr.guest_nice, prev.guest_nice);
    // user already includes guest and nice already includes guest_nice, so
    // the total is the raw sum — adding guest again would double-count it.
    let total = user + nice + system + idle + iowait + irq + softirq + steal;
    if total == 0 {
        return (0.0, CpuStates::default());
    }
    let p = |x: u64| x as f64 / total as f64 * 100.0;
    let states = CpuStates {
        user: p(user.saturating_sub(guest)),
        nice: p(nice.saturating_sub(guest_nice)),
        system: p(system),
        iowait: p(iowait),
        irq: p(irq),
        softirq: p(softirq),
        steal: p(steal),
        guest: p(guest),
        guest_nice: p(guest_nice),
    };
    (100.0 - p(idle), states)
}

/// cpu-total first, then cpu0.. On Linux the values are a `/proc/stat` delta
/// against `prev` (updated in place); the first call only seeds `prev` and
/// returns nothing, so the caller's cold-first-tick skip has real data by the
/// second tick. Elsewhere `sysinfo` gives the aggregate usage only.
#[cfg(target_os = "linux")]
fn gather_cpu(_sys: &System, prev: &mut CpuBaseline) -> Vec<CpuSample> {
    let text = match std::fs::read_to_string("/proc/stat") {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "could not read /proc/stat; skipping cpu this tick");
            return Vec::new();
        }
    };
    let curr = parse_proc_stat(&text);
    let mut out = Vec::new();
    if !prev.is_empty() {
        let prev_map: BTreeMap<&str, &CpuTimes> =
            prev.iter().map(|(n, t)| (n.as_str(), t)).collect();
        for (name, ct) in &curr {
            if let Some(pt) = prev_map.get(name.as_str()) {
                let (usage, states) = cpu_delta(pt, ct);
                out.push(CpuSample {
                    name: name.clone(),
                    usage,
                    states: Some(states),
                });
            }
        }
    }
    *prev = curr;
    out
}

/// cpu-total first, then cpu0.. — indices generated so the tag values match
/// Telegraf regardless of what `sysinfo` calls a core on this platform. Only
/// the aggregate usage is available here (no per-state split), so `states` is
/// `None`.
#[cfg(not(target_os = "linux"))]
fn gather_cpu(sys: &System, _prev: &mut CpuBaseline) -> Vec<CpuSample> {
    let mut out = vec![CpuSample {
        name: "cpu-total".to_string(),
        usage: sys.global_cpu_usage() as f64,
        states: None,
    }];
    for (i, c) in sys.cpus().iter().enumerate() {
        out.push(CpuSample {
            name: format!("cpu{i}"),
            usage: c.cpu_usage() as f64,
            states: None,
        });
    }
    out
}

/// Fresh disk list + space + inodes. Called in a bounded blocking task:
/// `statvfs` — sysinfo's own space read AND our inode read — blocks on an
/// unresponsive mount, so this must not run on the async runtime. A new
/// `Disks` each tick is fine — disk space is a gauge, nothing accumulates.
fn gather_disks() -> Vec<DiskSample> {
    let disks = Disks::new_with_refreshed_list();
    disks
        .iter()
        .map(|d| {
            let path = d.mount_point();
            DiskSample {
                device: d.name().to_string_lossy().into_owned(),
                path: path.to_string_lossy().into_owned(),
                fstype: d.file_system().to_string_lossy().into_owned(),
                total: d.total_space(),
                free: d.available_space(),
                inodes: inode_stats(path),
            }
        })
        .collect()
}

/// Inode counts for a mount via `statvfs`. Runs right after sysinfo read the
/// same mount for its space numbers, so a mount that answers sysinfo answers
/// this too.
#[cfg(unix)]
// f_files/f_ffree are fsfilcnt_t, which is u64 on 64-bit Linux (so the cast is
// redundant there) but u32 on some 32-bit targets (so it's needed for those).
#[allow(clippy::unnecessary_cast)]
fn inode_stats(mount: &std::path::Path) -> Option<InodeStats> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(mount.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c` is a valid NUL-terminated path and `s` is zeroed; statvfs
    // fills it and returns 0 on success, and we ignore `s` on any failure.
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    inode_stats_from(s.f_files as u64, s.f_ffree as u64)
}

/// Windows (and any non-unix) has no inode concept; Telegraf omits these
/// fields there too.
#[cfg(not(unix))]
fn inode_stats(_mount: &std::path::Path) -> Option<InodeStats> {
    None
}

fn gather_net(nets: &Networks) -> Vec<NetSample> {
    nets.iter()
        .map(|(iface, data)| NetSample {
            iface: iface.clone(),
            bytes_recv: data.total_received(),
            bytes_sent: data.total_transmitted(),
            packets_recv: data.total_packets_received(),
            packets_sent: data.total_packets_transmitted(),
            err_in: data.total_errors_on_received(),
            err_out: data.total_errors_on_transmitted(),
        })
        .collect()
}

fn gather_system(sys: &System) -> SystemSample {
    let load = System::load_average();
    SystemSample {
        load1: load.one,
        load5: load.five,
        load15: load.fifteen,
        n_cpus: sys.cpus().len() as u64,
        uptime: System::uptime(),
    }
}

// ---- the collector loop + its drain ----------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Collector {
    Cpu,
    Mem,
    Disk,
    Net,
    System,
    Swap,
}

fn resolve_collectors(names: &[String]) -> Vec<Collector> {
    names
        .iter()
        .filter_map(|n| match n.as_str() {
            "cpu" => Some(Collector::Cpu),
            "mem" => Some(Collector::Mem),
            "disk" => Some(Collector::Disk),
            "net" => Some(Collector::Net),
            "system" => Some(Collector::System),
            "swap" => Some(Collector::Swap),
            _ => None, // validated at config load; unreachable here
        })
        .collect()
}

/// Run the collector until shutdown: a durable queue, a drain task shipping
/// it, and a sampling loop. An independent pipeline — its own queue and
/// shipper — so a source's tail loop is untouched.
pub async fn run(
    cfg: Metrics,
    state_dir: PathBuf,
    queue_max_bytes: u64,
    shipper: Shipper,
) -> anyhow::Result<()> {
    let interval = cfg.interval_parsed()?;
    let collectors = resolve_collectors(&cfg.collectors);
    let ctx = Ctx::from_cfg(&cfg);
    let want = |c: Collector| collectors.contains(&c);

    let queue = Arc::new(Mutex::new(Queue::open(
        &state_dir.join("metrics-queue"),
        queue_max_bytes,
    )?));
    tokio::spawn(drain(Arc::clone(&queue), shipper));

    // Held across ticks: net counters are cumulative from first observation,
    // so the object must persist to accumulate (trap 4). Disk is gathered
    // fresh each tick in a bounded blocking task (below) — space is a gauge,
    // and statvfs must not block the async runtime.
    let mut sys = System::new();
    let mut nets = Networks::new_with_refreshed_list();
    // Trap 2: establish a CPU baseline so the first emitted tick has a delta.
    // (sysinfo's baseline, for the non-Linux path; the Linux /proc/stat
    // baseline is seeded by the first gather_cpu call below.)
    sys.refresh_cpu_all();
    let mut cpu_baseline: CpuBaseline = Default::default();
    // How long a disk gather may take before it is skipped for a tick: a dead
    // NFS mount blocks statvfs, and that must not wedge the other collectors.
    const DISK_GATHER_TIMEOUT_SECS: u64 = 5;

    tracing::info!(
        interval_secs = interval.as_secs(),
        host = ctx.host,
        collectors = cfg.collectors.join(","),
        "host-metrics collector started"
    );

    let mut ticker = tokio::time::interval(interval);
    // The interval's first tick fires immediately; that CPU reading is still
    // cold (no real window since the baseline above), so cpu is skipped on it.
    let mut first_tick = true;
    let mut last_ts: i64 = 0;

    loop {
        ticker.tick().await;

        sys.refresh_memory();
        sys.refresh_cpu_all();
        nets.refresh(true);

        // One timestamp for the whole tick, forced strictly monotonic so two
        // ticks can never share a primary key.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let ts_ns = now.max(last_ts + 1);
        last_ts = ts_ns;

        // Disk is gathered in a bounded blocking task BEFORE the line batch is
        // built (statvfs blocks on an unresponsive mount): a dead NFS skips
        // disk for this tick instead of wedging every collector.
        let disk_samples = if want(Collector::Disk) {
            match tokio::time::timeout(
                Duration::from_secs(DISK_GATHER_TIMEOUT_SECS),
                tokio::task::spawn_blocking(gather_disks),
            )
            .await
            {
                Ok(Ok(s)) => Some(s),
                Ok(Err(_)) => {
                    tracing::error!("disk gather task panicked; skipped this tick");
                    None
                }
                Err(_) => {
                    tracing::warn!(
                        "disk gather timed out (an unresponsive mount?); skipped this tick"
                    );
                    None
                }
            }
        } else {
            None
        };

        let mut lines = String::new();
        let mut push = |r: Record| {
            let _ = r.encode(&mut lines);
        };

        if want(Collector::Mem) {
            push(mem_record(&gather_mem(&sys), &ctx, ts_ns));
        }
        if want(Collector::Swap) {
            push(swap_record(&gather_swap(&sys), &ctx, ts_ns));
        }
        if want(Collector::System) {
            push(system_record(&gather_system(&sys), &ctx, ts_ns));
        }
        if let Some(samples) = disk_samples {
            for d in samples {
                push(disk_record(&d, &ctx, ts_ns));
            }
        }
        if want(Collector::Net) {
            for n in gather_net(&nets) {
                push(net_record(&n, &ctx, ts_ns));
            }
        }
        if want(Collector::Cpu) {
            // Gather every tick to keep the CPU baseline fresh; the first
            // (cold) tick's samples are suppressed. On Linux that first call
            // only seeds the /proc/stat baseline and returns nothing anyway.
            let samples = gather_cpu(&sys, &mut cpu_baseline);
            if !first_tick {
                for c in samples {
                    push(cpu_record(&c, &ctx, ts_ns));
                }
            }
        }
        first_tick = false;

        if !lines.is_empty() {
            match queue.lock().expect("metrics queue lock").push(&lines) {
                Ok(true) => {}
                Ok(false) => tracing::warn!("metrics queue full; sample dropped"),
                Err(e) => tracing::error!(error = %e, "could not queue metrics"),
            }
        }
    }
}

/// Ship queued batches FIFO, retrying transport failures. The queue is
/// durable, so a crash loses nothing already spooled. Same shape as the OTLP
/// receiver's drain.
async fn drain(queue: Arc<Mutex<Queue>>, shipper: Shipper) {
    loop {
        let front = queue.lock().expect("metrics queue lock").front();
        match front {
            None => tokio::time::sleep(Duration::from_millis(200)).await,
            Some(path) => {
                let body = match Queue::read(&path) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "metrics queue segment unreadable");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let lines: Vec<String> = body.split_inclusive('\n').map(str::to_string).collect();
                match shipper.send_lines(&lines).await {
                    Ok(_poison) => {
                        let _ = queue.lock().expect("metrics queue lock").pop(&path);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "metrics batch not shipped; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Ctx {
        Ctx {
            host: "h1".to_string(),
            global_tags: BTreeMap::from([("region".to_string(), "us-east".to_string())]),
            static_fields: vec![("deployment".to_string(), Value::Str("prod".to_string()))],
        }
    }

    fn enc(r: Record) -> String {
        let mut s = String::new();
        r.encode(&mut s).unwrap();
        s
    }

    // The golden strings ARE the schema contract (trap 3): if a field or tag
    // is renamed, one of these fails, before a dashboard silently blanks.

    #[test]
    fn mem_is_the_telegraf_shape() {
        // 8 GiB used of 16 GiB -> used_percent 50 exactly (power-of-two clean)
        let s = MemSample {
            total: 16_000,
            available: 8_000,
            used: 8_000,
            free: 8_000,
        };
        assert_eq!(
            enc(mem_record(&s, &ctx(), 1000)),
            "mem,host=h1,region=us-east total=16000i,available=8000i,used=8000i,free=8000i,\
             used_percent=50,available_percent=50,deployment=\"prod\" 1000\n"
        );
    }

    #[test]
    fn cpu_carries_cpu_tag_and_idle_active_only() {
        let s = CpuSample {
            name: "cpu-total".to_string(),
            usage: 25.0,
            states: None,
        };
        assert_eq!(
            enc(cpu_record(&s, &ctx(), 7)),
            "cpu,cpu=cpu-total,host=h1,region=us-east usage_idle=75,usage_active=25,\
             deployment=\"prod\" 7\n"
        );
    }

    #[test]
    fn disk_carries_device_path_fstype() {
        let s = DiskSample {
            device: "/dev/sda1".to_string(),
            path: "/".to_string(),
            fstype: "ext4".to_string(),
            total: 1000,
            free: 250,
            inodes: None,
        };
        // used = 750, used_percent = 75
        assert_eq!(
            enc(disk_record(&s, &ctx(), 2)),
            "disk,device=/dev/sda1,fstype=ext4,host=h1,path=/,region=us-east \
             total=1000i,free=250i,used=750i,used_percent=75,deployment=\"prod\" 2\n"
        );
    }

    #[test]
    fn disk_record_carries_inodes_when_present() {
        let s = DiskSample {
            device: "/dev/sda1".to_string(),
            path: "/".to_string(),
            fstype: "ext4".to_string(),
            total: 1000,
            free: 250,
            inodes: Some(InodeStats {
                total: 100,
                free: 40,
                used: 60,
            }),
        };
        assert_eq!(
            enc(disk_record(&s, &ctx(), 2)),
            "disk,device=/dev/sda1,fstype=ext4,host=h1,path=/,region=us-east \
             total=1000i,free=250i,used=750i,used_percent=75,\
             inodes_total=100i,inodes_free=40i,inodes_used=60i,deployment=\"prod\" 2\n"
        );
    }

    #[test]
    fn inode_stats_from_zero_files_is_none() {
        // A pseudo-filesystem reports 0 inodes — there is nothing to report.
        assert!(inode_stats_from(0, 0).is_none());
    }

    #[test]
    fn inode_stats_from_computes_used() {
        let i = inode_stats_from(100, 40).expect("100 files is a real fs");
        assert_eq!((i.total, i.free, i.used), (100, 40, 60));
    }

    #[cfg(unix)]
    #[test]
    fn inode_stats_reads_a_real_mount_without_crashing() {
        // Exercises the real statvfs FFI — linking and struct layout. A real
        // fs answers Some with a coherent count; a pseudo-fs answers None.
        // Both are fine; the point is the call works and the numbers agree.
        if let Some(i) = inode_stats(std::path::Path::new("/")) {
            assert!(i.total >= i.free);
            assert_eq!(i.used, i.total - i.free);
        }
    }

    #[test]
    fn net_counters_are_emitted_cumulative() {
        let s = NetSample {
            iface: "eth0".to_string(),
            bytes_recv: 100,
            bytes_sent: 200,
            packets_recv: 3,
            packets_sent: 4,
            err_in: 0,
            err_out: 0,
        };
        assert_eq!(
            enc(net_record(&s, &ctx(), 5)),
            "net,host=h1,interface=eth0,region=us-east bytes_recv=100i,bytes_sent=200i,\
             packets_recv=3i,packets_sent=4i,err_in=0i,err_out=0i,deployment=\"prod\" 5\n"
        );
    }

    #[test]
    fn system_and_swap_shapes() {
        let sys = SystemSample {
            load1: 0.5,
            load5: 0.25,
            load15: 0.0,
            n_cpus: 8,
            uptime: 3600,
        };
        assert_eq!(
            enc(system_record(&sys, &ctx(), 9)),
            "system,host=h1,region=us-east load1=0.5,load5=0.25,load15=0,n_cpus=8i,\
             uptime=3600i,deployment=\"prod\" 9\n"
        );
        let sw = SwapSample {
            total: 100,
            used: 20,
            free: 80,
        };
        assert_eq!(
            enc(swap_record(&sw, &ctx(), 11)),
            "swap,host=h1,region=us-east total=100i,used=20i,free=80i,used_percent=20,\
             deployment=\"prod\" 11\n"
        );
    }

    #[test]
    fn global_tags_and_static_fields_land_on_every_measurement() {
        let c = ctx();
        for line in [
            enc(mem_record(
                &MemSample {
                    total: 1,
                    available: 1,
                    used: 0,
                    free: 1,
                },
                &c,
                1,
            )),
            enc(cpu_record(
                &CpuSample {
                    name: "cpu0".to_string(),
                    usage: 1.0,
                    states: None,
                },
                &c,
                1,
            )),
            enc(net_record(
                &NetSample {
                    iface: "lo".to_string(),
                    bytes_recv: 0,
                    bytes_sent: 0,
                    packets_recv: 0,
                    packets_sent: 0,
                    err_in: 0,
                    err_out: 0,
                },
                &c,
                1,
            )),
        ] {
            assert!(
                line.contains("region=us-east"),
                "missing global tag: {line}"
            );
            assert!(
                line.contains("deployment=\"prod\""),
                "missing static field: {line}"
            );
            assert!(line.contains("host=h1"), "missing host tag: {line}");
        }
    }

    #[test]
    fn structural_tags_and_real_fields_win_collisions() {
        // A global tag named `host` and a static field named `used` must not
        // override the real ones (that would corrupt the series / dup a key).
        let c = Ctx {
            host: "real".to_string(),
            global_tags: BTreeMap::from([("host".to_string(), "spoofed".to_string())]),
            static_fields: vec![("used".to_string(), Value::Int(999))],
        };
        let line = enc(mem_record(
            &MemSample {
                total: 10,
                available: 4,
                used: 6,
                free: 4,
            },
            &c,
            1,
        ));
        assert!(
            line.contains("host=real"),
            "global tag overrode host: {line}"
        );
        assert!(!line.contains("spoofed"));
        assert!(
            line.contains("used=6i"),
            "static field overrode used: {line}"
        );
        assert!(!line.contains("used=999i"));
    }

    #[test]
    fn cpu_record_with_states_emits_the_full_breakdown() {
        let s = CpuSample {
            name: "cpu-total".to_string(),
            usage: 80.0,
            states: Some(CpuStates {
                user: 40.0,
                system: 20.0,
                iowait: 10.0,
                nice: 0.0,
                irq: 0.0,
                softirq: 0.0,
                steal: 0.0,
                guest: 10.0,
                guest_nice: 0.0,
            }),
        };
        assert_eq!(
            enc(cpu_record(&s, &ctx(), 3)),
            "cpu,cpu=cpu-total,host=h1,region=us-east usage_idle=20,usage_active=80,\
             usage_user=40,usage_system=20,usage_iowait=10,usage_nice=0,usage_irq=0,\
             usage_softirq=0,usage_steal=0,usage_guest=10,usage_guest_nice=0,\
             deployment=\"prod\" 3\n"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_maps_cpu_total_and_cores() {
        let text = "cpu  100 0 50 800 20 0 5 0 0 0\n\
                    cpu0 60 0 30 400 10 0 3 0 0 0\n\
                    cpu1 40 0 20 400 10 0 2 0 0 0\n\
                    intr 12345\nctxt 999\n";
        let p = parse_proc_stat(text);
        assert_eq!(p.len(), 3, "the aggregate + two cores, not intr/ctxt");
        assert_eq!(p[0].0, "cpu-total");
        assert_eq!(p[0].1.user, 100);
        assert_eq!(p[0].1.idle, 800);
        assert_eq!(p[1].0, "cpu0");
        assert_eq!(p[2].0, "cpu1");
        assert_eq!(p[2].1.softirq, 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_tolerates_missing_trailing_columns() {
        // A pre-guest kernel: seven columns, no steal/guest/guest_nice.
        let p = parse_proc_stat("cpu 10 0 5 80 0 0 0\n");
        assert_eq!(p[0].1.system, 5);
        assert_eq!(p[0].1.steal, 0);
        assert_eq!(p[0].1.guest, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpu_delta_is_telegraf_percentages_and_subtracts_guest() {
        // Total delta 100 jiffies, so each field's delta reads as its percent.
        // user INCLUDES guest (50 = 40 real + 10 guest), exactly as /proc/stat
        // reports it; the split must subtract guest back out of user.
        let prev = CpuTimes::default();
        let curr = CpuTimes {
            user: 50,
            nice: 0,
            system: 20,
            idle: 20,
            iowait: 10,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 10,
            guest_nice: 0,
        };
        let (active, s) = cpu_delta(&prev, &curr);
        assert_eq!(active, 80.0); // 100 - idle(20)
        assert_eq!(s.user, 40.0); // (user 50 - guest 10) / 100
        assert_eq!(s.guest, 10.0);
        assert_eq!(s.system, 20.0);
        assert_eq!(s.iowait, 10.0);
        // The nine states sum to usage_active (everything that is not idle).
        let sum = s.user
            + s.nice
            + s.system
            + s.iowait
            + s.irq
            + s.softirq
            + s.steal
            + s.guest
            + s.guest_nice;
        assert!(
            (sum - active).abs() < 1e-9,
            "states {sum} != active {active}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpu_delta_of_no_movement_is_zero_not_a_divide_by_zero() {
        let t = CpuTimes {
            user: 5,
            idle: 5,
            ..Default::default()
        };
        let (active, s) = cpu_delta(&t, &t); // total delta 0
        assert_eq!(active, 0.0);
        assert_eq!(s.user, 0.0);
    }
}
