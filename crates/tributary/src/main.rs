//! Tributary — a log-file agent for TimeLakeDB.
//!
//! L0 scope: read a file from the start, map it, ship it, and prove the
//! count is exact. Rotation, checkpoint persistence, the disk queue and
//! bisect-on-400 are L1/L2 (see ROADMAP.md); the seams they need are in
//! place but the behaviour is deliberately not claimed yet.

mod config;
mod lp;
mod map;
mod ship;
mod stamp;

use std::io::BufRead;

use config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let path = match (args.next().as_deref(), args.next()) {
        (Some("--config"), Some(p)) => p,
        _ => {
            eprintln!("usage: tributary --config <file.toml>");
            std::process::exit(2);
        }
    };

    let cfg = Config::load(std::path::Path::new(&path))?;
    let mut shipper = ship::Shipper::new(&cfg.output.url, &cfg.output.database, cfg.output.gzip)?;

    let mut read_total = 0u64;
    let mut quarantined = 0u64;

    for source in &cfg.sources {
        let file = std::fs::File::open(&source.path)?;
        let reader = std::io::BufReader::new(file);
        let mut stamper = stamp::Stamper::new(source.resolution());
        let mut batch = String::with_capacity(1 << 20);
        let mut batch_lines = 0usize;

        tracing::info!(
            stream = source.name,
            path = source.path,
            resolution = source.timestamp.resolution,
            "reading"
        );

        for raw in reader.split(b'\n') {
            let raw = raw?;
            if raw.is_empty() {
                continue;
            }
            read_total += 1;
            // Lossy decode BEFORE anything else: one invalid byte would
            // otherwise have the whole request refused (DESIGN.md §1.2).
            let line = map::decode_lossy(&raw);

            let (mut record, source_ts) = match map::map_line(source, &line) {
                Ok(v) => v,
                Err(map::MapError::Empty) => {
                    read_total -= 1;
                    continue;
                }
                Err(e) => {
                    quarantined += 1;
                    tracing::warn!(stream = source.name, error = %e, "quarantined");
                    continue;
                }
            };
            record.ts_ns = match stamper.stamp(source_ts) {
                Ok(ts) => ts,
                Err(e) => {
                    quarantined += 1;
                    tracing::warn!(stream = source.name, error = %e, "quarantined");
                    continue;
                }
            };
            if let Err(e) = record.encode(&mut batch) {
                quarantined += 1;
                tracing::warn!(stream = source.name, error = %e, "quarantined");
                continue;
            }
            batch_lines += 1;

            if batch_lines >= cfg.output.batch_lines {
                send(&mut shipper, &mut batch, &mut batch_lines).await?;
            }
        }
        send(&mut shipper, &mut batch, &mut batch_lines).await?;

        if stamper.out_of_window > 0 {
            tracing::warn!(
                stream = source.name,
                count = stamper.out_of_window,
                "lines whose source tick had left the dedup window — uniqueness not guaranteed"
            );
        }
    }

    tracing::info!(
        read = read_total,
        shipped = shipper.shipped,
        quarantined,
        rejected_batches = shipper.rejected,
        "done"
    );
    // The gate is arithmetic the operator can check from outside.
    println!(
        "{{\"read\":{},\"shipped\":{},\"quarantined\":{},\"rejected_batches\":{}}}",
        read_total, shipper.shipped, quarantined, shipper.rejected
    );
    Ok(())
}

async fn send(
    shipper: &mut ship::Shipper,
    batch: &mut String,
    lines: &mut usize,
) -> anyhow::Result<()> {
    if *lines == 0 {
        return Ok(());
    }
    loop {
        match shipper.send(batch).await {
            Ok(()) => break,
            // Explicit, named backpressure (RR-5) — wait, do not hammer.
            Err(ship::ShipError::Backpressure(d)) => {
                tracing::info!(retry_in = ?d, "backpressure");
                tokio::time::sleep(d).await;
            }
            Err(e) => return Err(anyhow::anyhow!(e.to_string())),
        }
    }
    batch.clear();
    *lines = 0;
    Ok(())
}
