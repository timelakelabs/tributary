//! Tributary — a log-file agent for TimeLakeDB.
//!
//! L2 scope: everything L1 had, plus the disk queue and backpressure,
//! bisect-on-400, and watermarks. Multiline joins are the one L2 item
//! deliberately still outstanding — it is parser work, independent of
//! the durability story these gates test.

mod auth;
mod checkpoint;
mod config;
mod credential;
mod docker;
#[cfg(feature = "journald")]
mod journald;
mod logfile;
mod lp;
mod map;
mod metrics;
mod multiline;
mod otlp;
mod queue;
mod server;
mod ship;
mod stamp;
mod tail;
mod telemetry;
mod transform;
mod watermark;
// Ungated on purpose: the reader trait, mapping and loop are
// platform-independent and their tests run in the default build. Only the
// wevtapi implementation inside is #[cfg(all(feature = "winlog", windows))].
mod winlog;

/// The per-line framer: multiline joins for stack traces, or docker json-file
/// reassembly for the 16 KB splits. One per source; both emit complete "lines"
/// for the map path and both gate the checkpoint through `has_pending`, so the
/// tail loop treats them the same.
enum Framer {
    Multiline(multiline::Joiner),
    Docker(docker::Reassembler),
}

impl Framer {
    fn push(&mut self, line: &str) -> Result<Option<String>, docker::DockerError> {
        match self {
            Framer::Multiline(j) => Ok(j.push(line.to_string())),
            Framer::Docker(r) => r.push(line),
        }
    }
    fn expire(&mut self) -> Option<String> {
        match self {
            Framer::Multiline(j) => j.expire(),
            Framer::Docker(r) => r.expire(),
        }
    }
    fn drain(&mut self) -> Option<String> {
        match self {
            Framer::Multiline(j) => j.drain(),
            Framer::Docker(r) => r.drain(),
        }
    }
    fn has_pending(&self) -> bool {
        match self {
            Framer::Multiline(j) => j.has_pending(),
            Framer::Docker(r) => r.has_pending(),
        }
    }
    fn truncated(&self) -> u64 {
        match self {
            Framer::Multiline(j) => j.truncated,
            Framer::Docker(r) => r.truncated,
        }
    }
}

use std::path::PathBuf;
use std::time::{Duration, Instant};

use checkpoint::Checkpoint;
use config::Config;
use tracing_subscriber::EnvFilter;

struct Args {
    config: PathBuf,
    state_dir: PathBuf,
    once: bool,
}

fn parse_args() -> Args {
    let mut config = None;
    let mut state_dir = PathBuf::from("./state");
    let mut once = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => config = it.next().map(PathBuf::from),
            "--state-dir" => {
                if let Some(v) = it.next() {
                    state_dir = PathBuf::from(v);
                }
            }
            "--once" => once = true,
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }
    let Some(config) = config else {
        eprintln!("usage: tributary --config <file.toml> [--state-dir <dir>] [--once]");
        std::process::exit(2);
    };
    Args {
        config,
        state_dir,
        once,
    }
}

/// A finished ship task: the lines the server quarantined, or the error
/// plus the batch handed back so the caller can spool it.
type ShipOutcome = Result<Vec<String>, (String, Vec<String>)>;

/// Everything the flush path needs, gathered so the signature stays
/// readable as the agent grows.
struct Pipeline {
    shipper: ship::Shipper,
    /// Batches in flight. Bounded, because unbounded concurrency just
    /// moves the queue into memory and hides it.
    inflight: tokio::task::JoinSet<ShipOutcome>,
    max_inflight: usize,
    /// Nanoseconds spent reading, parsing and encoding — the other half
    /// of the breakdown that decides whether a faster wire is worth it.
    read_ns: u64,
    queue: queue::Queue,
    watermark: watermark::Watermark,
    cp_path: PathBuf,
    dead_letter: PathBuf,
    quarantined: u64,
    /// Records dropped by the transform filter (#42) — a decision, counted
    /// apart from loss so it never reads as data lost.
    dropped_filter: u64,
    /// Records dropped by the transform sampler (#43), same accounting.
    dropped_sample: u64,
}

impl Pipeline {
    fn count_drop(&mut self, stage: DropStage) {
        match stage {
            DropStage::Filter => self.dropped_filter += 1,
            DropStage::Sample => self.dropped_sample += 1,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The config is read BEFORE the subscriber is installed, because it is
    // what decides where the log goes. Nothing between here and `.init()`
    // logs; a config error surfaces on stderr through anyhow, which is
    // where someone running this by hand is already looking.
    // #11 diagnostic: `--winlog-dump` reads a channel through the real reader
    // and prints what it would ship, resuming from the checkpoint bookmark —
    // the drill uses it to prove crash-exact resume without standing up a
    // TimeLakeDB sink. Intercepted before the normal arg parse (its flags are
    // not the agent's) and before any config is required.
    let raw: Vec<String> = std::env::args().collect();
    if raw.iter().any(|a| a == "--winlog-dump") {
        return run_winlog_dump(&raw);
    }

    let args = parse_args();
    let cfg = Config::load(&args.config)?;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match &cfg.log {
        // Unchanged path: stdout, captured and rotated by systemd or Docker.
        None => tracing_subscriber::fmt().with_env_filter(filter).init(),
        Some(l) => {
            let (size, every, keep) = l.parsed()?;
            let sink = logfile::LogSink(logfile::RotatingLog::open(&l.file, size, every, keep)?);
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                // No ANSI in a file: escape codes make a log grep-hostile,
                // and it was exactly this that made the P1-7 drill's own
                // parsing silently find nothing.
                .with_ansi(false)
                .with_writer(sink)
                .init();
        }
    }

    std::fs::create_dir_all(&args.state_dir)?;

    // Sourceless agent: no file to tail — a push receiver, a metrics
    // collector, or both. (A config WITH a source runs these alongside the
    // tail, spawned below.)
    if cfg.sources.is_empty() {
        return run_sourceless(cfg, &args).await;
    }
    let source = cfg
        .sources
        .first()
        .expect("load() guarantees a source when there is no [otlp]");

    let shipper = build_shipper(&cfg.output)?;
    // A one-line, secret-free statement of posture: an operator can tell from
    // the log whether this agent is presenting a credential, without it ever
    // revealing the credential. The client-certificate CN is not a secret —
    // it is the identity the server will read out of the chain.
    tracing::info!(
        authenticated = shipper.is_authenticated(),
        client_identity = shipper.client_identity().unwrap_or_else(|| "none".into()),
        url = %cfg.output.url,
        "shipping to TimeLakeDB"
    );

    // T-1: self-telemetry. The shipper's counters are shared directly
    // (they are already behind an Arc for the in-flight batches); the rest
    // of the picture is published into this snapshot once per loop pass.
    let tel = telemetry::Telemetry::new(shipper.counters.clone());
    if let Some(t) = &cfg.telemetry {
        let addr: std::net::SocketAddr = t.addr.parse().map_err(|_| {
            anyhow::anyhow!("[telemetry].addr {:?} is not a host:port address", t.addr)
        })?;
        // A bind failure fails startup rather than logging and continuing:
        // an operator who configured telemetry and silently did not get it
        // finds out from an empty dashboard days later.
        server::serve(addr, std::sync::Arc::clone(&tel)).await?;
    }

    // #25: a metrics collector, if configured, runs alongside the source as
    // its own independent pipeline. Spawned before the journald/winlog early
    // returns so it runs regardless of which source path this config takes.
    spawn_metrics(&cfg, &args)?;

    // #23: a journald source reads the journal via the sd-journal cursor, not
    // a file — its own pull loop, reusing this shipper and the same
    // queue/stamper/checkpoint machinery. Returns instead of falling into the
    // file-tail setup below.
    if source.parser == config::Parser::Journald {
        return run_journald_source(source, shipper, &args, &cfg).await;
    }

    // #11: a winlog source reads a Windows Event Log channel via wevtapi and
    // its bookmark, not a file — its own pull loop, reusing this shipper and
    // the same queue/stamper/checkpoint machinery. Returns instead of falling
    // into the file-tail setup below.
    if source.parser == config::Parser::Winlog {
        return run_winlog_source(source, shipper, &args, &cfg).await;
    }

    // #12: run the OTLP receiver alongside the file tail when configured. Its
    // own queue/shipper/stamper make it an independent pipeline — the tail
    // loop below is untouched.
    if let Some(otlp) = &cfg.otlp {
        let otlp_shipper = build_shipper(&cfg.output)?;
        let run = otlp::run(
            otlp.clone(),
            args.state_dir.clone(),
            cfg.output.queue_max_bytes,
            std::sync::Arc::clone(&tel),
            otlp_shipper,
        );
        tokio::spawn(async move {
            if let Err(e) = run.await {
                tracing::error!(error = %e, "OTLP receiver stopped");
            }
        });
    }

    // #10 (T-5): SIGHUP requests a config reload. A dedicated listener sets a
    // flag the loop drains once per pass — the same shape as the L4 cert check
    // below, so a reload cannot race the shutdown or checkpoint path and a
    // burst of HUPs collapses to one reload. Unix only: there is no SIGHUP on
    // Windows (a winlog agent restarts via its service manager), and the flag
    // simply never sets there.
    let reload_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    #[cfg(unix)]
    {
        let flag = std::sync::Arc::clone(&reload_flag);
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut hup = match signal(SignalKind::hangup()) {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!(error = %e, "could not install SIGHUP handler — config reload disabled");
                    return;
                }
            };
            while hup.recv().await.is_some() {
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });
    }

    run_file_source(source, &cfg, &args, shipper, tel, reload_flag).await
}

/// One file-tail source's whole pipeline: per-source setup, the tail loop,
/// and the drain on shutdown. Phase 1 of #49 extracts it verbatim so phase 2
/// can spawn one per `[[source]]`; single-source behaviour is unchanged.
async fn run_file_source(
    source: &config::Source,
    cfg: &config::Config,
    args: &Args,
    shipper: ship::Shipper,
    tel: std::sync::Arc<telemetry::Telemetry>,
    reload_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    // #49: this source's slice of /metrics. The loop below writes it, and
    // Telemetry::aggregate sums it with the other sources at render time, so N
    // sources cannot clobber each other's numbers.
    let snap = tel.register_source();

    // The transform stage (#42/#43/#44) is the reloadable part of the source
    // (#10): held in its own locals, not read off `source`, so a SIGHUP can
    // swap the rules on the running tail while the source's identity and
    // schema stay frozen. Compiled once here — config validation already
    // checked the regexes parse, so this does not fail in practice.
    let mut filters = source.filter.clone();
    let mut samples = source.sample.clone();
    let mut redacts = transform::compile_redacts(&source.redact)?;

    let cp_path = Checkpoint::path_for(&args.state_dir, &source.name);
    let restored = Checkpoint::load(&cp_path)?;

    let mut stamper = stamp::Stamper::new(source.resolution());
    if let Some(cp) = &restored
        && let (Some(tick), seq) = (cp.last_tick_ns, cp.next_seq)
    {
        stamper.restore(tick, seq);
        tracing::info!(tick, next_seq = seq, "restored stamper state");
    }

    let mut wm = watermark::Watermark::new(
        cfg.output.watermark_floor_ms as i64 * 1_000_000,
        cfg.output.watermark_ceiling_ms as i64 * 1_000_000,
    );
    if let Some(cp) = &restored
        && let Some(l) = cp.lateness_ns
    {
        // Resume the converged estimate rather than re-learning it from
        // the ceiling on every restart.
        wm.restore(l);
    }

    let mut pipe = Pipeline {
        shipper,
        inflight: tokio::task::JoinSet::new(),
        max_inflight: cfg.output.max_inflight,
        read_ns: 0,
        queue: queue::Queue::open(&args.state_dir.join("queue"), cfg.output.queue_max_bytes)?,
        watermark: wm,
        cp_path,
        dead_letter: args.state_dir.join("dead-letter.lp"),
        quarantined: 0,
        dropped_filter: 0,
        dropped_sample: 0,
    };

    let mut framer = match source.parser {
        // A docker json-file line is >16 KB by design (that is the split it
        // reassembles), so allow a generous single message before the cap.
        config::Parser::DockerJson => Framer::Docker(docker::Reassembler::new(1 << 20, 1000)),
        _ => {
            let ml = source.multiline.as_ref();
            Framer::Multiline(multiline::Joiner::new(
                ml.map(|m| m.starts_with.as_str()),
                ml.map(|m| m.max_lines).unwrap_or(500),
                ml.map(|m| m.max_bytes).unwrap_or(64 * 1024),
                ml.map(|m| m.timeout_ms).unwrap_or(1000),
            )?)
        }
    };

    let mut tailer = tail::Tailer::resume(std::path::Path::new(&source.path), restored.as_ref())?;
    tracing::info!(
        stream = source.name,
        path = source.path,
        follow = !args.once,
        resumed = restored.is_some(),
        queued = pipe.queue.len(),
        "started"
    );

    let mut batch: Vec<String> = Vec::with_capacity(cfg.output.batch_lines);
    let mut batch_max_ts = i64::MIN;
    let mut read_total = 0u64;
    let mut last_flush = Instant::now();
    let mut last_wm_write = Instant::now();
    let flush_every = Duration::from_millis(500);
    // L4 certificate rotation, checked on the same loop as everything else
    // rather than on its own task: the check is a stat when nothing changed,
    // and keeping it here means rotation cannot race the shutdown path.
    let mut last_rpo_report = Instant::now();
    // 0 means off. Without this it would mean "every loop iteration", which
    // is a very effective way to make an operator turn the log off entirely.
    let mut rpo_every = match cfg.output.rpo_report_secs {
        0 => Duration::from_secs(u64::MAX),
        n => Duration::from_secs(n),
    };
    // #10: the reloadable output knobs, held as locals so a SIGHUP can retune
    // them on the running tail (the loop reads these, not cfg.output).
    let mut batch_lines = cfg.output.batch_lines;
    let mut watermark_every = cfg.output.watermark_every_secs;
    let mut last_cert_check = Instant::now();
    let cert_refresh_every = Duration::from_secs(
        cfg.output
            .tls
            .as_ref()
            .map(|t| t.refresh_secs)
            .unwrap_or(u64::MAX),
    );

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);

    loop {
        // Anything a previous run or a previous outage left queued ships
        // before new work, so recovery is FIFO and bounded.
        drain_queue(&mut pipe, source, &tailer, &stamper).await?;

        let mut idle = true;
        // While the queue is full the source is NOT read: the lines stay
        // in the file, which is the only place that can still hold them.
        while !pipe.queue.full
            && let Some(raw) = tailer.next_line()?
        {
            idle = false;
            let t_read = Instant::now();
            let decoded = map::decode_lossy(&raw);
            // Multiline: a record may span several source lines, so only
            // a COMPLETED record becomes a row.
            let line = match framer.push(&decoded) {
                Ok(Some(line)) => line,
                Ok(None) => continue,
                Err(e) => {
                    dead_letter(&mut pipe, &decoded, &e.to_string())?;
                    continue;
                }
            };
            read_total += 1;
            match build(
                source,
                &line,
                &mut stamper,
                &mut pipe.watermark,
                &filters,
                &samples,
                &redacts,
            ) {
                Ok(Built::Ready(record, source_ts)) => {
                    let mut encoded = String::new();
                    if record.encode(&mut encoded).is_ok() {
                        batch.push(encoded);
                        batch_max_ts = batch_max_ts.max(source_ts);
                    } else {
                        dead_letter(&mut pipe, &line, "unencodable")?;
                    }
                }
                Ok(Built::Empty) => read_total -= 1,
                Ok(Built::Dropped(stage)) => pipe.count_drop(stage),
                Err(e) => {
                    dead_letter(&mut pipe, &line, &e.to_string())?;
                }
            }
            pipe.read_ns += t_read.elapsed().as_nanos() as u64;
            // Never record progress past a half-assembled record: a
            // crash would resume after its lines and lose it.
            if batch.len() >= batch_lines && !framer.has_pending() {
                flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
                last_flush = Instant::now();
            }
        }

        // The last record in a quiet file has no successor to close it.
        if let Some(line) = framer.expire() {
            read_total += 1;
            match build(
                source,
                &line,
                &mut stamper,
                &mut pipe.watermark,
                &filters,
                &samples,
                &redacts,
            ) {
                Ok(Built::Ready(record, source_ts)) => {
                    let mut encoded = String::new();
                    if record.encode(&mut encoded).is_ok() {
                        batch.push(encoded);
                        batch_max_ts = batch_max_ts.max(source_ts);
                    } else {
                        dead_letter(&mut pipe, &line, "unencodable")?;
                    }
                }
                Ok(Built::Empty) => read_total -= 1,
                Ok(Built::Dropped(stage)) => pipe.count_drop(stage),
                Err(e) => dead_letter(&mut pipe, &line, &e.to_string())?,
            }
        }

        if last_flush.elapsed() >= flush_every && !framer.has_pending() {
            if !batch.is_empty() {
                flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
            }
            checkpoint_now(&mut pipe, &tailer, &stamper).await?;
            last_flush = Instant::now();
        }

        // Publish the completeness claim as ordinary rows, so a dashboard
        // can tell "this window is empty" from "not complete yet".
        if last_wm_write.elapsed() >= Duration::from_secs(watermark_every) {
            write_watermark(&mut pipe, cfg, &source.name).await?;
            last_wm_write = Instant::now();
        }

        // T-1: publish the snapshot a scrape reads. Once per pass, relaxed
        // stores only — the HTTP handler never touches the structures the
        // shipping path owns, so a scrape cannot stall a batch.
        {
            use std::sync::atomic::Ordering::Relaxed;
            tel.tick();
            snap.lines_read.store(read_total, Relaxed);
            snap.quarantined.store(pipe.quarantined, Relaxed);
            snap.records_dropped_filter
                .store(pipe.dropped_filter, Relaxed);
            snap.records_dropped_sample
                .store(pipe.dropped_sample, Relaxed);
            snap.queue_bytes.store(pipe.queue.bytes(), Relaxed);
            snap.queue_segments.store(pipe.queue.len() as u64, Relaxed);
            snap.queue_full.store(pipe.queue.full, Relaxed);
            snap.spilled_total.store(pipe.queue.spilled_total, Relaxed);
            snap.drained_total.store(pipe.queue.drained_total, Relaxed);
            snap.pending_lines.store(batch.len() as u64, Relaxed);
            snap.inflight_batches
                .store(pipe.inflight.len() as u64, Relaxed);
            snap.unread_bytes.store(tailer.unread_bytes(), Relaxed);
            snap.files_open.store(tailer.marks().len() as u64, Relaxed);
            snap.files_lost.store(tailer.files_lost, Relaxed);
            snap.rotations.store(tailer.rotations, Relaxed);
            snap.watermark_violations
                .store(pipe.watermark.violations, Relaxed);
            snap.out_of_window.store(stamper.out_of_window, Relaxed);
            snap.read_ns.store(pipe.read_ns, Relaxed);
            tel.cert_expires_in_secs.store(
                pipe.shipper.credential_expires_in_secs().unwrap_or(-1),
                Relaxed,
            );
            tel.cert_healthy
                .store(pipe.shipper.credential_healthy(), Relaxed);
            tel.cert_renewals_refused
                .store(pipe.shipper.credential_reloads_refused(), Relaxed);
        }

        // P1-7: what would be lost if this node vanished right now.
        //
        // Not a health check — a statement of exposure. Everything the
        // server has not acked lives only on this node: the batch being
        // assembled, the batches in flight, whatever the queue is holding,
        // and the source bytes not yet read. If the node COMES BACK these
        // are all recoverable (L1: the checkpoint resumes exactly, losing
        // and duplicating nothing). If the node is GONE — a spot eviction,
        // a terminated container with an emptyDir — they are gone with it,
        // including the log files themselves. That is the trade the queue
        // makes, and this line is how an operator sees the size of it
        // instead of deriving it from the config.
        if last_rpo_report.elapsed() >= rpo_every {
            tracing::info!(
                pending_lines = batch.len(),
                inflight_batches = pipe.inflight.len(),
                queue_segments = pipe.queue.len(),
                queue_bytes = pipe.queue.bytes(),
                unread_bytes = tailer.unread_bytes(),
                "at risk if this node is lost now"
            );
            last_rpo_report = Instant::now();
        }

        // L4: adopt a renewed client certificate without a restart. SEC-3
        // assumes ~24 h certificates, so a renewal lands while this loop is
        // running. A REFUSED renewal is logged loudly and the last-good pair
        // keeps shipping — it is never allowed to fail the agent, because a
        // bad file on disk must not take down a working shipper.
        if last_cert_check.elapsed() >= cert_refresh_every {
            match pipe.shipper.rotate_credentials() {
                Ok(true) => tracing::info!(
                    identity = pipe
                        .shipper
                        .client_identity()
                        .unwrap_or_else(|| "none".into()),
                    expires_in_secs = pipe.shipper.credential_expires_in_secs().unwrap_or(0),
                    "adopted a renewed client certificate"
                ),
                Ok(false) => {}
                Err(e) => tracing::error!(
                    error = %e,
                    refused_total = pipe.shipper.credential_reloads_refused(),
                    "client certificate renewal REJECTED — still presenting the last-good pair"
                ),
            }
            last_cert_check = Instant::now();
        }

        // #10 (T-5): a SIGHUP asked for a reload. Drain the flag and apply —
        // validate-before-swap: a new file that fails to load or validate is
        // refused and the last-good config keeps running, exactly like the
        // credential reload above. The checkpoint and queue are untouched; the
        // in-flight batch finishes on the old rules.
        if reload_flag.swap(false, std::sync::atomic::Ordering::Relaxed) {
            reload_config(
                &args.config,
                source,
                cfg,
                &mut Reloadable {
                    filters: &mut filters,
                    samples: &mut samples,
                    redacts: &mut redacts,
                    batch_lines: &mut batch_lines,
                    watermark_every: &mut watermark_every,
                    max_inflight: &mut pipe.max_inflight,
                    rpo_every: &mut rpo_every,
                },
                &tel,
            );
        }

        if idle {
            if args.once && pipe.queue.is_empty() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                _ = &mut shutdown => {
                    tracing::info!("shutting down");
                    break;
                }
            }
        }
    }

    while let Some(line) = framer.drain() {
        match build(
            source,
            &line,
            &mut stamper,
            &mut pipe.watermark,
            &filters,
            &samples,
            &redacts,
        ) {
            Ok(Built::Ready(record, source_ts)) => {
                let mut encoded = String::new();
                if record.encode(&mut encoded).is_ok() {
                    read_total += 1;
                    batch.push(encoded);
                    batch_max_ts = batch_max_ts.max(source_ts);
                }
            }
            Ok(Built::Dropped(stage)) => {
                read_total += 1;
                pipe.count_drop(stage);
            }
            _ => {}
        }
    }
    flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
    checkpoint_now(&mut pipe, &tailer, &stamper).await?;
    drain_queue(&mut pipe, source, &tailer, &stamper).await?;
    write_watermark(&mut pipe, cfg, &source.name).await.ok();

    if stamper.out_of_window > 0 {
        tracing::warn!(
            count = stamper.out_of_window,
            "lines whose source tick had left the dedup window — uniqueness not guaranteed"
        );
    }
    tracing::info!(
        read = read_total,
        shipped = pipe.shipper.shipped(),
        quarantined = pipe.quarantined,
        rotations = tailer.rotations,
        files_lost = tailer.files_lost,
        spilled = pipe.queue.spilled_total,
        drained = pipe.queue.drained_total,
        bisects = pipe.shipper.bisects(),
        unauthorized = pipe.shipper.unauthorized(),
        transport_rebuilds = pipe.shipper.transport_rebuilds(),
        queue_bytes = pipe.queue.bytes(),
        multiline_truncated = framer.truncated(),
        read_ms = pipe.read_ns / 1_000_000,
        ship_ms = pipe.shipper.ship_ns() / 1_000_000,
        requests = pipe.shipper.requests(),
        watermark_violations = pipe.watermark.violations,
        // L4: a refused renewal is not a shipping failure, so it would
        // otherwise leave no trace in the summary. `cert_healthy=false` says
        // the agent is running on a last-good certificate that stopped being
        // renewed — the state that ends in a handshake failure days later.
        cert_healthy = pipe.shipper.credential_healthy(),
        cert_renewals_refused = pipe.shipper.credential_reloads_refused(),
        cert_expires_in_secs = pipe.shipper.credential_expires_in_secs().unwrap_or(-1),
        // P1-7: the exposure at exit. On a clean shutdown these are zero; a
        // non-zero queue here is precisely what a node loss would have cost.
        at_risk_queue_bytes = pipe.queue.bytes(),
        at_risk_unread_bytes = tailer.unread_bytes(),
        "done"
    );
    println!(
        "{{\"read\":{},\"shipped\":{},\"quarantined\":{},\"dropped\":{},\"rotations\":{},\"files_lost\":{},\
          \"spilled\":{},\"drained\":{},\"bisects\":{},\"queued\":{},\"queue_bytes\":{},          \"watermark_violations\":{},\"read_ms\":{},\"ship_ms\":{},\"requests\":{}}}",
        read_total,
        pipe.shipper.shipped(),
        pipe.quarantined,
        pipe.dropped_filter + pipe.dropped_sample,
        tailer.rotations,
        tailer.files_lost,
        pipe.queue.spilled_total,
        pipe.queue.drained_total,
        pipe.shipper.bisects(),
        pipe.queue.len(),
        pipe.queue.bytes(),
        pipe.watermark.violations,
        pipe.read_ns / 1_000_000,
        pipe.shipper.ship_ns() / 1_000_000,
        pipe.shipper.requests(),
    );
    Ok(())
}

/// Build a shipper from the output config: resolve the data-plane token (SEC-4,
/// from `TRIBUTARY_TOKEN` or `token_file`, never the config body) and the
/// optional L4 TLS runtime, then hand both to `ship::Shipper`. Extracted so the
/// file-tail path and the OTLP receiver (#12) build identical shippers rather
/// than drifting into two credential-resolution paths.
/// The whole agent when there is no `[[source]]`: build the shipper and
/// telemetry, then run the OTLP receiver until shutdown. Durability is the
/// receiver's queue, replayed on the next start; there is no file checkpoint
/// because a push source has no file offset to resume from.
async fn run_sourceless(cfg: config::Config, args: &Args) -> anyhow::Result<()> {
    let shipper = build_shipper(&cfg.output)?;
    tracing::info!(
        authenticated = shipper.is_authenticated(),
        client_identity = shipper.client_identity().unwrap_or_else(|| "none".into()),
        url = %cfg.output.url,
        "sourceless agent shipping to TimeLakeDB"
    );
    let tel = telemetry::Telemetry::new(shipper.counters.clone());
    if let Some(t) = &cfg.telemetry {
        let addr: std::net::SocketAddr = t.addr.parse().map_err(|_| {
            anyhow::anyhow!("[telemetry].addr {:?} is not a host:port address", t.addr)
        })?;
        server::serve(addr, std::sync::Arc::clone(&tel)).await?;
    }

    // The collector, if configured, is its own pipeline with its own shipper.
    spawn_metrics(&cfg, args)?;

    match &cfg.otlp {
        Some(otlp) => {
            otlp::run(
                otlp.clone(),
                args.state_dir.clone(),
                cfg.output.queue_max_bytes,
                tel,
                shipper,
            )
            .await
        }
        // Metrics-only: nothing to serve or tail. The collector runs in its
        // spawned task; park here until a stop signal so the process stays up.
        None => {
            tracing::info!("metrics-only agent; collecting until shutdown");
            let _ = tokio::signal::ctrl_c().await;
            Ok(())
        }
    }
}

/// Spawn the host-metrics collector (#25) if `[metrics]` is configured. Its
/// own shipper and queue make it independent of whatever source pipeline the
/// caller is running; a collector failure logs and stops that task alone.
fn spawn_metrics(cfg: &config::Config, args: &Args) -> anyhow::Result<()> {
    if let Some(m) = &cfg.metrics {
        let shipper = build_shipper(&cfg.output)?;
        let run = metrics::run(
            m.clone(),
            args.state_dir.clone(),
            cfg.output.queue_max_bytes,
            shipper,
        );
        tokio::spawn(async move {
            if let Err(e) = run.await {
                tracing::error!(error = %e, "metrics collector stopped");
            }
        });
    }
    Ok(())
}

pub(crate) fn build_shipper(output: &config::Output) -> anyhow::Result<ship::Shipper> {
    let token = auth::resolve_token("TRIBUTARY_TOKEN", output.token_file.as_deref())?;
    let tls = match &output.tls {
        None => None,
        Some(t) => {
            let roots = match &t.ca_file {
                Some(p) => credential::load_ca_bundle(p)
                    .map_err(|e| anyhow::anyhow!("[output.tls].ca_file {}: {e}", p.display()))?,
                None => Vec::new(),
            };
            let identity = match (&t.cert_file, &t.key_file) {
                (Some(c), Some(k)) => Some(
                    credential::RotatingIdentity::load(Box::new(credential::FileCredentials::new(
                        c, k,
                    )))
                    .map_err(|e| anyhow::anyhow!("[output.tls] client certificate: {e}"))?,
                ),
                // `Tls::validate` already refused the half-configured cases.
                _ => None,
            };
            match identity {
                Some(identity) => Some(std::sync::Arc::new(ship::TlsRuntime { roots, identity })),
                None if roots.is_empty() => None,
                // CA-only: trust a private issuer without presenting an
                // identity, which is what Telegraf's TLS config does.
                None => Some(std::sync::Arc::new(ship::TlsRuntime {
                    roots,
                    identity: credential::RotatingIdentity::none(),
                })),
            }
        }
    };
    ship::Shipper::new(&output.url, &output.database, output.gzip, token, tls)
}

/// Run a journald source (#23): reader + the shared ship path, raced against
/// shutdown. Behind the feature so the default binary links no libsystemd; the
/// `not` arm is unreachable at runtime (Config::load refuses journald without
/// the feature) but is needed to compile.
#[allow(unused_variables)]
async fn run_journald_source(
    source: &config::Source,
    shipper: ship::Shipper,
    args: &Args,
    cfg: &Config,
) -> anyhow::Result<()> {
    #[cfg(feature = "journald")]
    {
        let reader = journald::RealJournal::open()?;
        tracing::info!(
            stream = source.name,
            follow = !args.once,
            "journald source started"
        );
        let run = journald::run_journald(
            reader,
            source,
            shipper,
            &args.state_dir,
            cfg.output.queue_max_bytes,
            cfg.output.batch_lines,
            args.once,
        );
        let shutdown = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        };
        tokio::select! {
            r = run => r,
            _ = shutdown => { tracing::info!("shutdown"); Ok(()) }
        }
    }
    #[cfg(not(feature = "journald"))]
    {
        anyhow::bail!(
            "journald source configured but this binary was built without the `journald` feature"
        )
    }
}

/// Run a winlog source (#11): reader + the shared ship path, raced against
/// shutdown. Behind the feature AND the windows target so a default (or
/// Linux) binary links no wevtapi; the `not` arm is unreachable at runtime
/// (Config::load refuses winlog without the feature) but is needed to
/// compile.
#[allow(unused_variables)]
async fn run_winlog_source(
    source: &config::Source,
    shipper: ship::Shipper,
    args: &Args,
    cfg: &Config,
) -> anyhow::Result<()> {
    #[cfg(all(feature = "winlog", windows))]
    {
        // `path` is the channel name (`System`, `Application`, …).
        let reader = winlog::RealEventLog::open(&source.path)?;
        tracing::info!(
            stream = source.name,
            channel = source.path,
            follow = !args.once,
            "winlog source started"
        );
        let run = winlog::run_winlog(
            reader,
            source,
            shipper,
            &args.state_dir,
            cfg.output.queue_max_bytes,
            cfg.output.batch_lines,
            args.once,
        );
        let shutdown = async {
            // No SIGTERM on Windows; Ctrl-C (and Ctrl-Break via ctrl_c) is the
            // stop signal a service manager sends.
            let _ = tokio::signal::ctrl_c().await;
        };
        tokio::select! {
            r = run => r,
            _ = shutdown => { tracing::info!("shutdown"); Ok(()) }
        }
    }
    #[cfg(not(all(feature = "winlog", windows)))]
    {
        anyhow::bail!(
            "winlog source configured but this binary was built without the `winlog` feature \
             (or not for a Windows target)"
        )
    }
}

/// `--winlog-dump --channel <name> --state-dir <dir> [--stream <s>] [--limit <n>]`
/// (#11): read up to `--limit` events (default 20) from the channel, resuming
/// from the checkpoint bookmark under `--state-dir`/`--stream`, print one line
/// per event (`EventRecordID<TAB>time_created_ns<TAB>mapped?`) and save the
/// advanced bookmark back to the checkpoint — the SAME `Checkpoint`+bookmark
/// path production uses, so a second run resumes exactly where the first
/// stopped. This is the drill's instrument; it also runs `map_event` on each
/// event so the mapping path is exercised, not just the read.
#[allow(unused_variables)]
fn run_winlog_dump(raw: &[String]) -> anyhow::Result<()> {
    #[cfg(all(feature = "winlog", windows))]
    {
        use winlog::WinlogReader as _;

        let mut channel = String::new();
        let mut state_dir = PathBuf::from("./state");
        let mut stream = "winsys".to_string();
        let mut limit: usize = 20;
        let mut it = raw.iter().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--winlog-dump" => {}
                "--channel" => channel = it.next().cloned().unwrap_or_default(),
                "--state-dir" => {
                    if let Some(v) = it.next() {
                        state_dir = PathBuf::from(v);
                    }
                }
                "--stream" => stream = it.next().cloned().unwrap_or(stream),
                "--limit" => {
                    if let Some(v) = it.next() {
                        limit = v.parse().unwrap_or(limit);
                    }
                }
                other => anyhow::bail!("unknown --winlog-dump argument {other:?}"),
            }
        }
        if channel.trim().is_empty() {
            anyhow::bail!("--winlog-dump needs --channel <name> (e.g. System)");
        }
        std::fs::create_dir_all(&state_dir)?;

        // A synthetic source so map_event runs the real allowlist path. Fields
        // present on essentially every event, so the mapping is exercised.
        let source = config::Source {
            name: stream.clone(),
            path: channel.clone(),
            table: "eventlog".into(),
            parser: config::Parser::Winlog,
            timestamp: config::Timestamp {
                field: None,
                format: "unix_ms".into(),
                resolution: "us".into(),
            },
            tags: vec!["Provider".into(), "Channel".into()],
            tags_static: Default::default(),
            fields: [
                ("EventID".to_string(), config::FieldType::String),
                ("Computer".to_string(), config::FieldType::String),
            ]
            .into(),
            visibility: None,
            multiline: None,
            filter: Vec::new(),
            sample: Vec::new(),
            redact: Vec::new(),
        };

        let cp_path = Checkpoint::path_for(&state_dir, &stream);
        let restored = Checkpoint::load(&cp_path)?;
        let bookmark = restored.as_ref().and_then(|c| c.cursor.clone());

        let mut stamper = stamp::Stamper::new(source.resolution());
        if let Some(c) = &restored
            && let Some(t) = c.last_tick_ns
        {
            stamper.restore(t, c.next_seq);
        }

        let mut reader = winlog::RealEventLog::open(&channel)?;
        reader.seek(bookmark.as_deref())?;
        eprintln!(
            "winlog-dump: channel={channel} stream={stream} limit={limit} resuming={}",
            bookmark.is_some()
        );

        let mut last_bookmark = bookmark.clone();
        let mut n = 0usize;
        while n < limit {
            match reader.next()? {
                Some(ev) => {
                    let rid = ev.fields.get("EventRecordID").cloned().unwrap_or_default();
                    let mapped = winlog::map_event(&source, &ev, &mut stamper)?.is_some();
                    println!("{rid}\t{}\t{mapped}", ev.time_created_ns);
                    last_bookmark = Some(reader.bookmark()?);
                    n += 1;
                }
                None => break,
            }
        }

        let (last_tick_ns, next_seq) = match stamper.checkpoint() {
            Some((t, s)) => (Some(t), s),
            None => (None, 0),
        };
        Checkpoint {
            files: Vec::new(),
            last_tick_ns,
            next_seq,
            lateness_ns: None,
            cursor: last_bookmark,
        }
        .save(&cp_path)?;
        eprintln!(
            "winlog-dump: read {n} events, checkpoint saved to {}",
            cp_path.display()
        );
        Ok(())
    }
    #[cfg(not(all(feature = "winlog", windows)))]
    {
        anyhow::bail!("--winlog-dump requires a build with --features winlog for a Windows target")
    }
}

/// Which transform stage dropped a record — for the per-stage drop counter.
#[derive(Clone, Copy)]
enum DropStage {
    Filter,
    Sample,
}

/// The outcome of turning a raw line into a shippable record.
enum Built {
    /// A record ready to encode and ship.
    Ready(lp::Record, i64),
    /// An empty line — nothing to do, and it does not count as read.
    Empty,
    /// Dropped by a transform stage (#42 filter, #43 sample): a decision, not a
    /// loss. Never observed by the watermark, never shipped, counted on its own.
    Dropped(DropStage),
}

fn build(
    source: &config::Source,
    line: &str,
    stamper: &mut stamp::Stamper,
    wm: &mut watermark::Watermark,
    // #10: the transform rules are passed in rather than read off `source`, so
    // a SIGHUP reload can swap filter/sample/redact live. `source` still owns
    // the frozen half — mapping, tags, schema — which a reload does not touch.
    filters: &[config::Filter],
    samples: &[config::Sample],
    redacts: &[transform::CompiledRedact],
) -> anyhow::Result<Built> {
    match map::map_line(source, line) {
        Ok((mut record, source_ts)) => {
            // Transform stage (#42): the filter runs BEFORE the watermark
            // observes this timestamp. Dropping after observe would make the
            // watermark claim arrival for data deliberately thrown away — the
            // completeness guarantee quietly lying (#7).
            if !transform::keeps(&record, filters) {
                return Ok(Built::Dropped(DropStage::Filter));
            }
            // Sample (#43) also runs before observe — a sampled-out record is
            // not arrived. Deterministic on the record's identity, so a resume
            // re-decides the same and LWW collapses the replay.
            if !transform::sample_keeps(&record, source_ts, samples) {
                return Ok(Built::Dropped(DropStage::Sample));
            }
            // Redact (#44): scrub matched values in string fields BEFORE the
            // record is encoded and queued, so a secret never reaches the queue,
            // the checkpoint, or anything durable — only the redacted form does.
            transform::apply_redacts(&mut record, redacts);
            wm.observe(source_ts);
            record.ts_ns = stamper
                .stamp(source_ts)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(Built::Ready(record, source_ts))
        }
        Err(map::MapError::Empty) => Ok(Built::Empty),
        Err(e) => Err(anyhow::anyhow!(e.to_string())),
    }
}

/// The reloadable slice of the running file-tail pipeline (#10, T-5).
/// Everything reachable through here can change on a SIGHUP without dropping
/// the tail; everything the reload does NOT touch — the source's identity and
/// schema, the output endpoint, TLS, the queue, bound listeners — needs a
/// restart, and a change to one is reported so it is visible, not silently
/// ignored.
struct Reloadable<'a> {
    filters: &'a mut Vec<config::Filter>,
    samples: &'a mut Vec<config::Sample>,
    redacts: &'a mut Vec<transform::CompiledRedact>,
    batch_lines: &'a mut usize,
    watermark_every: &'a mut u64,
    max_inflight: &'a mut usize,
    rpo_every: &'a mut Duration,
}

/// What a reload did: the knobs it applied, and the fields that changed but
/// need a restart to take effect.
#[derive(Default)]
struct ReloadReport {
    applied: Vec<String>,
    restart_required: Vec<String>,
}

/// Re-read the config and apply what can change live. Never fails the agent:
/// a file that will not load or validate is REFUSED, counted, and the running
/// config is left exactly as it was — the same contract as the L4 credential
/// reload.
fn reload_config(
    path: &std::path::Path,
    source: &config::Source,
    current: &config::Config,
    r: &mut Reloadable,
    tel: &telemetry::Telemetry,
) {
    use std::sync::atomic::Ordering::Relaxed;
    let new = match Config::load(path) {
        Ok(c) => c,
        Err(e) => {
            tel.config_reloads_refused.fetch_add(1, Relaxed);
            tel.config_last_reload_ok.store(false, Relaxed);
            tracing::error!(
                error = %e, path = %path.display(),
                "config reload REFUSED — the new file failed to load or validate; keeping the last-good config"
            );
            return;
        }
    };
    match apply_reload(source, current, new, r) {
        Ok(report) => {
            tel.config_reloads.fetch_add(1, Relaxed);
            tel.config_last_reload_ok.store(true, Relaxed);
            if report.restart_required.is_empty() {
                tracing::info!(applied = ?report.applied, "config reloaded");
            } else {
                tracing::warn!(
                    applied = ?report.applied,
                    restart_required = ?report.restart_required,
                    "config reloaded — the listed changes need a restart to take effect; still running the previous values for those"
                );
            }
        }
        Err(e) => {
            // Loaded and validated, but a hot-swap piece (a redaction regex)
            // would not compile. Refuse rather than half-apply.
            tel.config_reloads_refused.fetch_add(1, Relaxed);
            tel.config_last_reload_ok.store(false, Relaxed);
            tracing::error!(error = %e, "config reload REFUSED after load — keeping the last-good config");
        }
    }
}

/// The pure half of a reload: diff the loaded config against what is running
/// and mutate the reloadable state. Split from the I/O so it is unit-testable.
/// The transform rules swap only when the source's IDENTITY (name/path/parser)
/// is unchanged — a different source is a restart, not a live re-tail. That is
/// the #10 trap: never re-seek a tail the change did not actually touch.
fn apply_reload(
    source: &config::Source,
    current: &config::Config,
    new: Config,
    r: &mut Reloadable,
) -> anyhow::Result<ReloadReport> {
    let mut report = ReloadReport::default();

    match new.sources.into_iter().next() {
        None => report
            .restart_required
            .push("the source was removed".into()),
        Some(ns) => {
            let same_source =
                ns.name == source.name && ns.path == source.path && ns.parser == source.parser;
            if !same_source {
                report
                    .restart_required
                    .push("source identity (name/path/parser)".into());
            } else {
                // Validate-before-swap: compile the NEW redacts first. A bad
                // regex refuses the whole reload (propagated up), so nothing
                // is half-applied.
                let new_redacts = transform::compile_redacts(&ns.redact)?;
                // Schema is frozen at boot — the database fixes a field's type
                // on first write and tags are in the primary key. Report a
                // change rather than silently dropping it.
                if ns.table != source.table {
                    report.restart_required.push("source.table".into());
                }
                if ns.tags != source.tags {
                    report.restart_required.push("source.tags".into());
                }
                if ns.tags_static != source.tags_static {
                    report.restart_required.push("source.tags_static".into());
                }
                if ns.fields != source.fields {
                    report.restart_required.push("source.fields".into());
                }
                if ns.timestamp != source.timestamp {
                    report.restart_required.push("source.timestamp".into());
                }
                if ns.visibility != source.visibility {
                    report.restart_required.push("source.visibility".into());
                }
                if ns.multiline != source.multiline {
                    report.restart_required.push("source.multiline".into());
                }
                // The transforms hot-swap.
                *r.filters = ns.filter;
                *r.samples = ns.sample;
                *r.redacts = new_redacts;
                report
                    .applied
                    .push("transforms (filter/sample/redact)".into());
            }
        }
    }

    // Output knobs the loop just reads each pass — safe to retune live.
    if new.output.batch_lines != *r.batch_lines {
        *r.batch_lines = new.output.batch_lines;
        report
            .applied
            .push(format!("batch_lines={}", new.output.batch_lines));
    }
    if new.output.max_inflight != *r.max_inflight {
        *r.max_inflight = new.output.max_inflight;
        report
            .applied
            .push(format!("max_inflight={}", new.output.max_inflight));
    }
    if new.output.watermark_every_secs != *r.watermark_every {
        *r.watermark_every = new.output.watermark_every_secs;
        report.applied.push(format!(
            "watermark_every_secs={}",
            new.output.watermark_every_secs
        ));
    }
    let new_rpo = match new.output.rpo_report_secs {
        0 => Duration::from_secs(u64::MAX),
        n => Duration::from_secs(n),
    };
    if new_rpo != *r.rpo_every {
        *r.rpo_every = new_rpo;
        report
            .applied
            .push(format!("rpo_report_secs={}", new.output.rpo_report_secs));
    }

    // Output fields captured at boot — a bound socket, an opened queue, a
    // built shipper. A change needs a restart; report, do not apply. Comparing
    // against `current` (the boot config) is right: these are never hot-
    // swapped, so the running value is always the boot value.
    if new.output.url != current.output.url {
        report.restart_required.push("output.url".into());
    }
    if new.output.tls != current.output.tls {
        report.restart_required.push("output.tls".into());
    }
    if new.output.queue_max_bytes != current.output.queue_max_bytes {
        report
            .restart_required
            .push("output.queue_max_bytes".into());
    }
    if new.output.database != current.output.database {
        report.restart_required.push("output.database".into());
    }
    if new.output.watermark_floor_ms != current.output.watermark_floor_ms
        || new.output.watermark_ceiling_ms != current.output.watermark_ceiling_ms
    {
        report
            .restart_required
            .push("output.watermark_floor_ms/ceiling_ms".into());
    }

    Ok(report)
}

/// Submit a batch to the in-flight pipeline. Deliberately does NOT
/// checkpoint: draining the pipeline on every batch would serialise it
/// completely, which is exactly what the first L3 measurement showed
/// (four concurrent batches were no faster than one). Progress is
/// recorded by `checkpoint_now`, on an interval.
async fn flush(
    pipe: &mut Pipeline,
    batch: &mut Vec<String>,
    batch_max_ts: &mut i64,
) -> anyhow::Result<()> {
    if !batch.is_empty() {
        // Keep the pipe full but bounded: block only once `max_inflight`
        // batches are outstanding.
        while pipe.inflight.len() >= pipe.max_inflight {
            reap_one(pipe).await?;
        }
        let shipper = pipe.shipper.clone();
        let lines = std::mem::take(batch);
        pipe.inflight.spawn(async move {
            match shipper.send_lines(&lines).await {
                Ok(poison) => Ok(poison),
                // Hand the batch back so the caller can spool it — the
                // task has no queue of its own.
                Err(e) => Err((e.to_string(), lines)),
            }
        });
        if *batch_max_ts != i64::MIN {
            pipe.watermark.advance(*batch_max_ts);
        }
        *batch_max_ts = i64::MIN;
    }
    Ok(())
}

/// Record progress, once everything in flight has landed. Batch 3 can
/// finish before batch 1, so a checkpoint taken with work outstanding
/// could claim bytes that were never shipped.
async fn checkpoint_now(
    pipe: &mut Pipeline,
    tailer: &tail::Tailer,
    stamper: &stamp::Stamper,
) -> anyhow::Result<()> {
    drain_inflight(pipe).await?;
    save_checkpoint(pipe, tailer, stamper)
}

/// Collect one finished batch, spooling it if it could not be shipped.
async fn reap_one(pipe: &mut Pipeline) -> anyhow::Result<()> {
    let Some(joined) = pipe.inflight.join_next().await else {
        return Ok(());
    };
    match joined {
        Ok(Ok(poison)) => quarantine(pipe, &poison)?,
        Ok(Err((err, lines))) => {
            let body: String = lines.concat();
            if pipe.queue.push(&body)? {
                tracing::info!(error = %err, bytes = body.len(), "spooled to the queue");
            } else {
                // The queue is full AND the database is unreachable.
                // Refuse to drop: put it back in front of the queue by
                // failing loudly rather than silently losing it.
                tracing::error!(error = %err, "queue full and shipping failed; reads are paused");
            }
        }
        Err(join) => return Err(anyhow::anyhow!("ship task panicked: {join}")),
    }
    Ok(())
}

async fn drain_inflight(pipe: &mut Pipeline) -> anyhow::Result<()> {
    while !pipe.inflight.is_empty() {
        reap_one(pipe).await?;
    }
    Ok(())
}

/// Ship whatever is queued, oldest first, until it is empty or the
/// database refuses again.
async fn drain_queue(
    pipe: &mut Pipeline,
    _source: &config::Source,
    tailer: &tail::Tailer,
    stamper: &stamp::Stamper,
) -> anyhow::Result<()> {
    let mut drained = false;
    while let Some(path) = pipe.queue.front() {
        let body = queue::Queue::read(&path)?;
        let lines: Vec<String> = body.split_inclusive('\n').map(str::to_string).collect();
        match pipe.shipper.send_lines(&lines).await {
            Ok(poison) => {
                quarantine(pipe, &poison)?;
                pipe.queue.pop(&path)?;
                drained = true;
            }
            Err(ship::ShipError::Backpressure(d)) => {
                tokio::time::sleep(d).await;
                return Ok(());
            }
            Err(_) => return Ok(()), // still down; try again next tick
        }
    }
    if drained {
        save_checkpoint(pipe, tailer, stamper)?;
    }
    Ok(())
}

/// Record one dropped line with the reason it was dropped. A counter
/// alone tells an operator that something was lost; the file tells them
/// what, which is the difference between a metric and a diagnosis.
fn dead_letter(pipe: &mut Pipeline, line: &str, reason: &str) -> anyhow::Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&pipe.dead_letter)?;
    writeln!(f, "{{\"reason\":{:?},\"line\":{:?}}}", reason, line)?;
    pipe.quarantined += 1;
    tracing::warn!(reason, "quarantined");
    Ok(())
}

fn quarantine(pipe: &mut Pipeline, poison: &[String]) -> anyhow::Result<()> {
    if poison.is_empty() {
        return Ok(());
    }
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&pipe.dead_letter)?;
    for line in poison {
        f.write_all(line.as_bytes())?;
    }
    pipe.quarantined += poison.len() as u64;
    Ok(())
}

fn save_checkpoint(
    pipe: &Pipeline,
    tailer: &tail::Tailer,
    stamper: &stamp::Stamper,
) -> anyhow::Result<()> {
    let (last_tick_ns, next_seq) = match stamper.checkpoint() {
        Some((t, s)) => (Some(t), s),
        None => (None, 0),
    };
    Checkpoint {
        files: tailer.marks(),
        last_tick_ns,
        next_seq,
        lateness_ns: Some(pipe.watermark.lateness_ns()),
        cursor: None,
    }
    .save(&pipe.cp_path)
}

/// The watermark as ordinary rows. Best-effort: failing to publish a
/// completeness claim must never stop shipping the data it describes.
async fn write_watermark(pipe: &mut Pipeline, cfg: &Config, stream: &str) -> anyhow::Result<()> {
    let published = pipe.watermark.published_ns();
    if published == i64::MIN {
        return Ok(());
    }
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let line = format!(
        "{},stream={},host={} watermark_ns={}i,lateness_ns={}i,violations={}i {}\n",
        cfg.output.watermark_table,
        stream,
        host,
        published,
        pipe.watermark.lateness_ns(),
        pipe.watermark.violations,
        now_ns
    );
    if let Err(e) = pipe.shipper.send(&line).await {
        tracing::debug!(error = %e, "watermark publish deferred");
    }
    Ok(())
}

#[cfg(test)]
mod reload_tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    // A minimal valid config: one file source, no transforms.
    const V1: &str = "[output]\nurl = \"http://localhost:1963\"\n\n\
        [[source]]\nname = \"app\"\npath = \"/var/log/app.log\"\ntable = \"logs\"\n";
    // Same source identity; adds a redact rule and retunes batch_lines.
    const V2: &str = "[output]\nurl = \"http://localhost:1963\"\nbatch_lines = 123\n\n\
        [[source]]\nname = \"app\"\npath = \"/var/log/app.log\"\ntable = \"logs\"\n\n\
        [[source.redact]]\nfield = \"msg\"\npattern = \"secret\"\nreplacement = \"***\"\n";
    // A different source PATH — an identity change, not a live re-tail.
    const V3: &str = "[output]\nurl = \"http://localhost:1963\"\n\n\
        [[source]]\nname = \"app\"\npath = \"/var/log/other.log\"\ntable = \"logs\"\n\n\
        [[source.redact]]\nfield = \"msg\"\npattern = \"x\"\n";
    // Fails validation: a file source with no path to tail.
    const BAD: &str = "[output]\nurl = \"http://localhost:1963\"\n\n\
        [[source]]\nname = \"app\"\npath = \"\"\ntable = \"logs\"\n";

    fn write(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
        let p = dir.join("c.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    /// Held reloadable state, so the two tests build it the same way.
    struct Held {
        filters: Vec<config::Filter>,
        samples: Vec<config::Sample>,
        redacts: Vec<transform::CompiledRedact>,
        batch_lines: usize,
        watermark_every: u64,
        max_inflight: usize,
        rpo_every: Duration,
    }
    impl Held {
        fn from(cfg: &Config) -> Held {
            let s = cfg.sources.first().unwrap();
            Held {
                filters: s.filter.clone(),
                samples: s.sample.clone(),
                redacts: transform::compile_redacts(&s.redact).unwrap(),
                batch_lines: cfg.output.batch_lines,
                watermark_every: cfg.output.watermark_every_secs,
                max_inflight: cfg.output.max_inflight,
                rpo_every: Duration::from_secs(0),
            }
        }
        fn reloadable(&mut self) -> Reloadable<'_> {
            Reloadable {
                filters: &mut self.filters,
                samples: &mut self.samples,
                redacts: &mut self.redacts,
                batch_lines: &mut self.batch_lines,
                watermark_every: &mut self.watermark_every,
                max_inflight: &mut self.max_inflight,
                rpo_every: &mut self.rpo_every,
            }
        }
    }

    #[test]
    fn reload_hot_swaps_transforms_and_a_bad_config_is_refused_keeping_last_good() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(dir.path(), V1);
        let cfg = Config::load(&p).unwrap();
        let source = cfg.sources.first().unwrap();
        let mut held = Held::from(&cfg);
        let tel = telemetry::Telemetry::new(std::sync::Arc::new(ship::Counters::default()));
        assert_eq!(held.redacts.len(), 0, "v1 declares no redaction");

        // A valid reload of the SAME source: transforms and knobs go live.
        write(dir.path(), V2);
        reload_config(&p, source, &cfg, &mut held.reloadable(), &tel);
        assert_eq!(tel.config_reloads.load(Relaxed), 1);
        assert!(tel.config_last_reload_ok.load(Relaxed));
        assert_eq!(held.redacts.len(), 1, "the new redaction rule is live");
        assert_eq!(held.batch_lines, 123, "batch_lines retuned live");

        // A config that fails validation is refused; the last-good stays.
        write(dir.path(), BAD);
        reload_config(&p, source, &cfg, &mut held.reloadable(), &tel);
        assert_eq!(tel.config_reloads_refused.load(Relaxed), 1);
        assert!(!tel.config_last_reload_ok.load(Relaxed));
        assert_eq!(held.batch_lines, 123, "a refused reload changes nothing");
        assert_eq!(
            held.redacts.len(),
            1,
            "a refused reload keeps the last-good transforms"
        );
    }

    #[test]
    fn a_different_source_path_is_restart_required_not_a_live_retail() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(dir.path(), V1);
        let cfg = Config::load(&p).unwrap();
        let source = cfg.sources.first().unwrap();
        let mut held = Held::from(&cfg);

        write(dir.path(), V3);
        let new = Config::load(&p).unwrap();
        let report = apply_reload(source, &cfg, new, &mut held.reloadable()).unwrap();

        assert!(
            report
                .restart_required
                .iter()
                .any(|s| s.contains("source identity")),
            "a changed source path must be flagged restart-required: {:?}",
            report.restart_required
        );
        assert_eq!(
            held.redacts.len(),
            0,
            "a different source must NOT graft its transforms onto the running tail"
        );
    }
}
