//! Tributary — a log-file agent for TimeLakeDB.
//!
//! L1 scope: follow files through rotation, checkpoint durably, resume
//! without losing or duplicating a line. The disk queue, bisect-on-400
//! and multiline are L2 (see ROADMAP.md).

mod checkpoint;
mod config;
mod lp;
mod map;
mod ship;
mod stamp;
mod tail;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use checkpoint::Checkpoint;
use config::Config;
use tracing_subscriber::EnvFilter;

struct Args {
    config: PathBuf,
    state_dir: PathBuf,
    /// Read to end of file and exit, rather than following.
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

    // One source per process keeps L1 honest: multiple streams need
    // independent checkpoints and shipping budgets, which is L2's queue.
    let source = cfg
        .sources
        .first()
        .ok_or_else(|| anyhow::anyhow!("no [[source]] configured"))?;

    let mut shipper = ship::Shipper::new(&cfg.output.url, &cfg.output.database, cfg.output.gzip)?;
    let cp_path = Checkpoint::path_for(&args.state_dir, &source.name);
    let restored = Checkpoint::load(&cp_path)?;

    let mut stamper = stamp::Stamper::new(source.resolution());
    if let Some(cp) = &restored
        && let (Some(tick), seq) = (cp.last_tick_ns, cp.next_seq)
    {
        // Without this the lines after the checkpoint restart the
        // sequence and overwrite the ones before it (DESIGN.md §3.2).
        stamper.restore(tick, seq);
        tracing::info!(tick, next_seq = seq, "restored stamper state");
    }

    let mut tailer = tail::Tailer::resume(std::path::Path::new(&source.path), restored.as_ref())?;
    tracing::info!(
        stream = source.name,
        path = source.path,
        follow = !args.once,
        resumed = restored.is_some(),
        "started"
    );

    let mut batch = String::with_capacity(1 << 20);
    let mut batch_lines = 0usize;
    let mut read_total = 0u64;
    let mut quarantined = 0u64;
    let mut last_flush = Instant::now();
    let flush_every = Duration::from_millis(500);

    // SIGTERM as well as SIGINT: systemd and Kubernetes both stop a
    // process with SIGTERM, and dying on it would drop the in-flight
    // batch on every ordinary restart.
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
        let mut idle = true;
        while let Some(raw) = tailer.next_line()? {
            idle = false;
            read_total += 1;
            // Lossy decode first: one invalid byte would otherwise have
            // the whole request refused (DESIGN.md §1.2).
            let line = map::decode_lossy(&raw);
            match build(source, &line, &mut stamper) {
                Ok(Some(record)) => {
                    if record.encode(&mut batch).is_ok() {
                        batch_lines += 1;
                    } else {
                        quarantined += 1;
                    }
                }
                Ok(None) => read_total -= 1, // blank line, not a record
                Err(e) => {
                    quarantined += 1;
                    tracing::warn!(stream = source.name, error = %e, "quarantined");
                }
            }
            if batch_lines >= cfg.output.batch_lines {
                flush(
                    &mut shipper,
                    &mut batch,
                    &mut batch_lines,
                    &tailer,
                    &stamper,
                    &cp_path,
                )
                .await?;
                last_flush = Instant::now();
            }
        }

        if batch_lines > 0 && last_flush.elapsed() >= flush_every {
            flush(
                &mut shipper,
                &mut batch,
                &mut batch_lines,
                &tailer,
                &stamper,
                &cp_path,
            )
            .await?;
            last_flush = Instant::now();
        }

        if idle {
            if args.once {
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

    flush(
        &mut shipper,
        &mut batch,
        &mut batch_lines,
        &tailer,
        &stamper,
        &cp_path,
    )
    .await?;

    if stamper.out_of_window > 0 {
        tracing::warn!(
            count = stamper.out_of_window,
            "lines whose source tick had left the dedup window — uniqueness not guaranteed"
        );
    }
    tracing::info!(
        read = read_total,
        shipped = shipper.shipped,
        quarantined,
        rotations = tailer.rotations,
        files_lost = tailer.files_lost,
        "done"
    );
    println!(
        "{{\"read\":{},\"shipped\":{},\"quarantined\":{},\"rotations\":{},\"files_lost\":{}}}",
        read_total, shipper.shipped, quarantined, tailer.rotations, tailer.files_lost
    );
    Ok(())
}

fn build(
    source: &config::Source,
    line: &str,
    stamper: &mut stamp::Stamper,
) -> anyhow::Result<Option<lp::Record>> {
    match map::map_line(source, line) {
        Ok((mut record, source_ts)) => {
            record.ts_ns = stamper
                .stamp(source_ts)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(Some(record))
        }
        Err(map::MapError::Empty) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(e.to_string())),
    }
}

/// Ship, then record progress — in that order, always. Checkpointing
/// first would claim lines that never landed; this way a crash in
/// between replays them, and the replay is idempotent because the
/// timestamps regenerate identically (DESIGN.md §3.2).
async fn flush(
    shipper: &mut ship::Shipper,
    batch: &mut String,
    lines: &mut usize,
    tailer: &tail::Tailer,
    stamper: &stamp::Stamper,
    cp_path: &std::path::Path,
) -> anyhow::Result<()> {
    if *lines > 0 {
        loop {
            match shipper.send(batch).await {
                Ok(()) => break,
                Err(ship::ShipError::Backpressure(d)) => {
                    tracing::info!(retry_in = ?d, "backpressure");
                    tokio::time::sleep(d).await;
                }
                Err(e) => return Err(anyhow::anyhow!(e.to_string())),
            }
        }
        batch.clear();
        *lines = 0;
    }
    let (last_tick_ns, next_seq) = match stamper.checkpoint() {
        Some((t, s)) => (Some(t), s),
        None => (None, 0),
    };
    Checkpoint {
        files: tailer.marks(),
        last_tick_ns,
        next_seq,
    }
    .save(cp_path)?;
    Ok(())
}
