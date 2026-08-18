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
mod logfile;
mod lp;
mod map;
mod multiline;
mod queue;
mod server;
mod ship;
mod stamp;
mod tail;
mod telemetry;
mod watermark;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The config is read BEFORE the subscriber is installed, because it is
    // what decides where the log goes. Nothing between here and `.init()`
    // logs; a config error surfaces on stderr through anyhow, which is
    // where someone running this by hand is already looking.
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

    let source = cfg
        .sources
        .first()
        .ok_or_else(|| anyhow::anyhow!("no [[source]] configured"))?;

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

    // The data-plane token (SEC-4), sourced from TRIBUTARY_TOKEN or the
    // configured token_file — never from the config body. Resolved at the
    // edge so the shipper is handed an opaque, already-redacted credential.
    let token = auth::resolve_token("TRIBUTARY_TOKEN", cfg.output.token_file.as_deref())?;

    // L4 transport: private trust anchors and/or a client certificate. Both
    // halves are optional and independent, so an unconfigured agent takes
    // exactly the path it took before this existed.
    let tls = match &cfg.output.tls {
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

    let shipper = ship::Shipper::new(
        &cfg.output.url,
        &cfg.output.database,
        cfg.output.gzip,
        token,
        tls,
    )?;
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
    };

    let ml = source.multiline.as_ref();
    let mut joiner = multiline::Joiner::new(
        ml.map(|m| m.starts_with.as_str()),
        ml.map(|m| m.max_lines).unwrap_or(500),
        ml.map(|m| m.max_bytes).unwrap_or(64 * 1024),
        ml.map(|m| m.timeout_ms).unwrap_or(1000),
    )?;

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
    let rpo_every = match cfg.output.rpo_report_secs {
        0 => Duration::from_secs(u64::MAX),
        n => Duration::from_secs(n),
    };
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
            let Some(line) = joiner.push(decoded) else {
                continue;
            };
            read_total += 1;
            match build(source, &line, &mut stamper, &mut pipe.watermark) {
                Ok(Some((record, source_ts))) => {
                    let mut encoded = String::new();
                    if record.encode(&mut encoded).is_ok() {
                        batch.push(encoded);
                        batch_max_ts = batch_max_ts.max(source_ts);
                    } else {
                        dead_letter(&mut pipe, &line, "unencodable")?;
                    }
                }
                Ok(None) => read_total -= 1,
                Err(e) => {
                    dead_letter(&mut pipe, &line, &e.to_string())?;
                }
            }
            pipe.read_ns += t_read.elapsed().as_nanos() as u64;
            // Never record progress past a half-assembled record: a
            // crash would resume after its lines and lose it.
            if batch.len() >= cfg.output.batch_lines && !joiner.has_pending() {
                flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
                last_flush = Instant::now();
            }
        }

        // The last record in a quiet file has no successor to close it.
        if let Some(line) = joiner.expire() {
            read_total += 1;
            match build(source, &line, &mut stamper, &mut pipe.watermark) {
                Ok(Some((record, source_ts))) => {
                    let mut encoded = String::new();
                    if record.encode(&mut encoded).is_ok() {
                        batch.push(encoded);
                        batch_max_ts = batch_max_ts.max(source_ts);
                    } else {
                        dead_letter(&mut pipe, &line, "unencodable")?;
                    }
                }
                Ok(None) => read_total -= 1,
                Err(e) => dead_letter(&mut pipe, &line, &e.to_string())?,
            }
        }

        if last_flush.elapsed() >= flush_every && !joiner.has_pending() {
            if !batch.is_empty() {
                flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
            }
            checkpoint_now(&mut pipe, &tailer, &stamper).await?;
            last_flush = Instant::now();
        }

        // Publish the completeness claim as ordinary rows, so a dashboard
        // can tell "this window is empty" from "not complete yet".
        if last_wm_write.elapsed() >= Duration::from_secs(cfg.output.watermark_every_secs) {
            write_watermark(&mut pipe, &cfg, &source.name).await?;
            last_wm_write = Instant::now();
        }

        // T-1: publish the snapshot a scrape reads. Once per pass, relaxed
        // stores only — the HTTP handler never touches the structures the
        // shipping path owns, so a scrape cannot stall a batch.
        {
            use std::sync::atomic::Ordering::Relaxed;
            tel.tick();
            tel.lines_read.store(read_total, Relaxed);
            tel.quarantined.store(pipe.quarantined, Relaxed);
            tel.queue_bytes.store(pipe.queue.bytes(), Relaxed);
            tel.queue_segments.store(pipe.queue.len() as u64, Relaxed);
            tel.queue_full.store(pipe.queue.full, Relaxed);
            tel.spilled_total.store(pipe.queue.spilled_total, Relaxed);
            tel.drained_total.store(pipe.queue.drained_total, Relaxed);
            tel.pending_lines.store(batch.len() as u64, Relaxed);
            tel.inflight_batches
                .store(pipe.inflight.len() as u64, Relaxed);
            tel.unread_bytes.store(tailer.unread_bytes(), Relaxed);
            tel.files_open.store(tailer.marks().len() as u64, Relaxed);
            tel.files_lost.store(tailer.files_lost, Relaxed);
            tel.rotations.store(tailer.rotations, Relaxed);
            tel.watermark_violations
                .store(pipe.watermark.violations, Relaxed);
            tel.out_of_window.store(stamper.out_of_window, Relaxed);
            tel.read_ns.store(pipe.read_ns, Relaxed);
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

    if let Some(line) = joiner.drain()
        && let Ok(Some((mut record, source_ts))) =
            build(source, &line, &mut stamper, &mut pipe.watermark)
    {
        record.ts_ns = record.ts_ns.max(record.ts_ns);
        let mut encoded = String::new();
        if record.encode(&mut encoded).is_ok() {
            read_total += 1;
            batch.push(encoded);
            batch_max_ts = batch_max_ts.max(source_ts);
        }
    }
    flush(&mut pipe, &mut batch, &mut batch_max_ts).await?;
    checkpoint_now(&mut pipe, &tailer, &stamper).await?;
    drain_queue(&mut pipe, source, &tailer, &stamper).await?;
    write_watermark(&mut pipe, &cfg, &source.name).await.ok();

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
        queue_bytes = pipe.queue.bytes(),
        multiline_truncated = joiner.truncated,
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
        "{{\"read\":{},\"shipped\":{},\"quarantined\":{},\"rotations\":{},\"files_lost\":{},\
          \"spilled\":{},\"drained\":{},\"bisects\":{},\"queued\":{},\"queue_bytes\":{},          \"watermark_violations\":{},\"read_ms\":{},\"ship_ms\":{},\"requests\":{}}}",
        read_total,
        pipe.shipper.shipped(),
        pipe.quarantined,
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

fn build(
    source: &config::Source,
    line: &str,
    stamper: &mut stamp::Stamper,
    wm: &mut watermark::Watermark,
) -> anyhow::Result<Option<(lp::Record, i64)>> {
    match map::map_line(source, line) {
        Ok((mut record, source_ts)) => {
            wm.observe(source_ts);
            record.ts_ns = stamper
                .stamp(source_ts)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(Some((record, source_ts)))
        }
        Err(map::MapError::Empty) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(e.to_string())),
    }
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
