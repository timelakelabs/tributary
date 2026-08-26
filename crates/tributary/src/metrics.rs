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
//! Known gaps (documented, not bugs): no CPU user/system/iowait split
//! (`sysinfo` reports one usage percentage per core, so `cpu` carries
//! `usage_idle`/`usage_active` only — the `/proc/stat` breakdown is a
//! follow-up); no disk inodes; `load*` is zero on Windows, which has no
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
/// from `sysinfo`, whose per-core naming differs by platform.
pub struct CpuSample {
    pub name: String,
    pub usage: f64,
}

pub struct DiskSample {
    pub device: String,
    pub path: String,
    pub fstype: String,
    pub total: u64,
    pub free: u64,
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
    Record {
        table: "cpu".to_string(),
        tags: ctx.tags(&[("cpu", &s.name)]),
        // Trap 1: sysinfo gives one usage %, NOT the user/system/iowait
        // split. usage_idle + usage_active is what that supports honestly;
        // do not synthesise usage_user.
        fields: ctx.fields(vec![
            ("usage_idle", Value::Float((100.0 - s.usage).max(0.0))),
            ("usage_active", Value::Float(s.usage)),
        ]),
        ts_ns,
    }
}

pub fn disk_record(s: &DiskSample, ctx: &Ctx, ts_ns: i64) -> Record {
    let used = s.total.saturating_sub(s.free);
    Record {
        table: "disk".to_string(),
        tags: ctx.tags(&[
            ("device", &s.device),
            ("path", &s.path),
            ("fstype", &s.fstype),
        ]),
        fields: ctx.fields(vec![
            ("total", Value::Int(s.total as i64)),
            ("free", Value::Int(s.free as i64)),
            ("used", Value::Int(used as i64)),
            ("used_percent", Value::Float(pct(used, s.total))),
        ]),
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

/// cpu-total first, then cpu0.. — indices generated so the tag values match
/// Telegraf regardless of what `sysinfo` calls a core on this platform.
fn gather_cpu(sys: &System) -> Vec<CpuSample> {
    let mut out = vec![CpuSample {
        name: "cpu-total".to_string(),
        usage: sys.global_cpu_usage() as f64,
    }];
    for (i, c) in sys.cpus().iter().enumerate() {
        out.push(CpuSample {
            name: format!("cpu{i}"),
            usage: c.cpu_usage() as f64,
        });
    }
    out
}

fn gather_disks(disks: &Disks) -> Vec<DiskSample> {
    disks
        .iter()
        .map(|d| DiskSample {
            device: d.name().to_string_lossy().into_owned(),
            path: d.mount_point().to_string_lossy().into_owned(),
            fstype: d.file_system().to_string_lossy().into_owned(),
            total: d.total_space(),
            free: d.available_space(),
        })
        .collect()
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

    // Held across ticks: net/disk counters are cumulative from first
    // observation, so the objects must persist to accumulate (trap 4).
    let mut sys = System::new();
    let mut disks = Disks::new_with_refreshed_list();
    let mut nets = Networks::new_with_refreshed_list();
    // Trap 2: establish a CPU baseline so the first emitted tick has a delta.
    sys.refresh_cpu_all();

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
        disks.refresh(true);
        nets.refresh(true);

        // One timestamp for the whole tick, forced strictly monotonic so two
        // ticks can never share a primary key.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let ts_ns = now.max(last_ts + 1);
        last_ts = ts_ns;

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
        if want(Collector::Disk) {
            for d in gather_disks(&disks) {
                push(disk_record(&d, &ctx, ts_ns));
            }
        }
        if want(Collector::Net) {
            for n in gather_net(&nets) {
                push(net_record(&n, &ctx, ts_ns));
            }
        }
        if want(Collector::Cpu) && !first_tick {
            for c in gather_cpu(&sys) {
                push(cpu_record(&c, &ctx, ts_ns));
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
        };
        // used = 750, used_percent = 75
        assert_eq!(
            enc(disk_record(&s, &ctx(), 2)),
            "disk,device=/dev/sda1,fstype=ext4,host=h1,path=/,region=us-east \
             total=1000i,free=250i,used=750i,used_percent=75,deployment=\"prod\" 2\n"
        );
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
}
