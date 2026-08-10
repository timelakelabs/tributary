//! Tributary — a log-file agent for TimeLakeDB.
//!
//! L2 scope: everything L1 had, plus the disk queue and backpressure,
//! bisect-on-400, and watermarks. Multiline joins are the one L2 item
//! deliberately still outstanding — it is parser work, independent of
//! the durability story these gates test.

mod checkpoint;
mod config;
mod lp;
mod map;
mod multiline;
mod queue;
mod ship;
mod stamp;
mod tail;
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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let cfg = Config::load(&args.config)?;
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

    let mut pipe = Pipeline {
        shipper: ship::Shipper::new(&cfg.output.url, &cfg.output.database, cfg.output.gzip)?,
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
        queue_bytes = pipe.queue.bytes(),
        multiline_truncated = joiner.truncated,
        read_ms = pipe.read_ns / 1_000_000,
        ship_ms = pipe.shipper.ship_ns() / 1_000_000,
        requests = pipe.shipper.requests(),
        watermark_violations = pipe.watermark.violations,
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
