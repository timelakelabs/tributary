//! Self-telemetry (T-1): `/metrics` and `/healthz`.
//!
//! An agent nobody can watch does not survive an ops review, and it cannot
//! be a DaemonSet — a pod with no readiness signal and no counters is a pod
//! that fails silently. This module is the shared state; `server.rs` serves
//! it.
//!
//! The shape is a **published snapshot**, not a live read. Most of what is
//! worth reporting — queue depth, files open, unread bytes, the batch being
//! assembled — lives in structures the main loop owns exclusively and does
//! not share. Rather than wrap all of them in locks that the shipping path
//! would then contend on, the loop writes a snapshot into these atomics once
//! per tick and the HTTP handler reads it. A scrape therefore sees state
//! that is at most one tick old, which is far better than a scrape that can
//! stall a batch.
//!
//! The shipper's own counters are the exception: they are already behind an
//! `Arc` because L3 put several batches in flight at once, so they are
//! shared directly and are exact rather than sampled.
//!
//! Metric names come from `DESIGN.md` §6.2, which specified them before any
//! of this existed. That section also states an invariant this module is
//! built to keep checkable from the outside:
//!
//! > `lines_read` minus `lines_shipped` minus `lines_quarantined` should be
//! > the queue depth. If it is not, something is being lost.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ship::Counters;

/// Everything `/metrics` and `/healthz` report.
pub struct Telemetry {
    /// Exact, shared with the shipping tasks.
    pub ship: Arc<Counters>,

    // -- published by the main loop, once per tick ---------------------
    pub lines_read: AtomicU64,
    pub quarantined: AtomicU64,
    pub queue_bytes: AtomicU64,
    pub queue_segments: AtomicU64,
    pub queue_full: AtomicBool,
    pub spilled_total: AtomicU64,
    pub drained_total: AtomicU64,
    pub pending_lines: AtomicU64,
    pub inflight_batches: AtomicU64,
    pub unread_bytes: AtomicU64,
    pub files_open: AtomicU64,
    pub files_lost: AtomicU64,
    pub rotations: AtomicU64,
    pub watermark_violations: AtomicU64,
    pub out_of_window: AtomicU64,
    pub read_ns: AtomicU64,

    /// L4 client certificate. `expiry` is seconds from now; `-1` means no
    /// certificate is configured, which is a different thing from expired
    /// and must not read as an alarm.
    pub cert_expires_in_secs: AtomicI64,
    pub cert_healthy: AtomicBool,
    pub cert_renewals_refused: AtomicU64,

    /// Epoch millis of the last main-loop tick. `/healthz` uses this and
    /// nothing else to decide liveness.
    pub last_tick_ms: AtomicU64,
    pub started_ms: u64,
}

impl Telemetry {
    pub fn new(ship: Arc<Counters>) -> Arc<Telemetry> {
        let now = epoch_ms();
        Arc::new(Telemetry {
            ship,
            lines_read: AtomicU64::new(0),
            quarantined: AtomicU64::new(0),
            queue_bytes: AtomicU64::new(0),
            queue_segments: AtomicU64::new(0),
            queue_full: AtomicBool::new(false),
            spilled_total: AtomicU64::new(0),
            drained_total: AtomicU64::new(0),
            pending_lines: AtomicU64::new(0),
            inflight_batches: AtomicU64::new(0),
            unread_bytes: AtomicU64::new(0),
            files_open: AtomicU64::new(0),
            files_lost: AtomicU64::new(0),
            rotations: AtomicU64::new(0),
            watermark_violations: AtomicU64::new(0),
            out_of_window: AtomicU64::new(0),
            read_ns: AtomicU64::new(0),
            cert_expires_in_secs: AtomicI64::new(-1),
            cert_healthy: AtomicBool::new(true),
            cert_renewals_refused: AtomicU64::new(0),
            last_tick_ms: AtomicU64::new(now),
            started_ms: now,
        })
    }

    /// Called by the main loop each pass. Cheap: a handful of relaxed
    /// stores, no allocation, no lock.
    pub fn tick(&self) {
        self.last_tick_ms.store(epoch_ms(), Ordering::Relaxed);
    }

    /// How long since the loop last turned. The only input to liveness.
    pub fn since_tick_ms(&self) -> u64 {
        epoch_ms().saturating_sub(self.last_tick_ms.load(Ordering::Relaxed))
    }

    pub fn uptime_secs(&self) -> u64 {
        epoch_ms().saturating_sub(self.started_ms) / 1000
    }

    /// The exposure that P1-7 measures: everything the server has not
    /// acked, which is what a vanished node takes with it.
    pub fn at_risk_lines(&self) -> u64 {
        self.pending_lines.load(Ordering::Relaxed)
    }

    /// Prometheus text exposition (v0.0.4). Hand-rolled on purpose — the
    /// format is a dozen lines of rules and this avoids a dependency in an
    /// agent whose whole argument is that it is small.
    pub fn render_prometheus(&self) -> String {
        let g = |v: &AtomicU64| v.load(Ordering::Relaxed);
        let mut s = String::with_capacity(4096);

        let mut m = |name: &str, kind: &str, help: &str, value: String| {
            s.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
            ));
        };

        // -- the pipeline, in the order data moves through it ----------
        m(
            "tributary_lines_read_total",
            "counter",
            "Lines read from the sources and turned into records.",
            g(&self.lines_read).to_string(),
        );
        m(
            "tributary_lines_shipped_total",
            "counter",
            "Lines acknowledged as durable by TimeLakeDB (HTTP 204).",
            self.ship.shipped.load(Ordering::Relaxed).to_string(),
        );
        m(
            "tributary_lines_quarantined_total",
            "counter",
            "Lines the server rejected, isolated by bisect and written to the dead-letter file.",
            g(&self.quarantined).to_string(),
        );
        m(
            "tributary_batches_rejected_total",
            "counter",
            "Batches refused with 400. The batch is atomic, so this triggers a bisect.",
            self.ship.rejected.load(Ordering::Relaxed).to_string(),
        );
        m(
            "tributary_bisects_total",
            "counter",
            "Bisect steps taken to isolate a poison line.",
            self.ship.bisects.load(Ordering::Relaxed).to_string(),
        );
        m(
            "tributary_requests_total",
            "counter",
            "HTTP requests made to TimeLakeDB.",
            self.ship.requests.load(Ordering::Relaxed).to_string(),
        );
        m(
            "tributary_unauthorized_total",
            "counter",
            "Ships refused with 401/403. Non-zero means the token is wrong, missing or unscoped.",
            self.ship.unauthorized.load(Ordering::Relaxed).to_string(),
        );
        m(
            "tributary_ship_seconds_total",
            "counter",
            "Seconds spent waiting on HTTP, cumulative.",
            format!(
                "{:.6}",
                self.ship.ship_ns.load(Ordering::Relaxed) as f64 / 1e9
            ),
        );
        m(
            "tributary_read_seconds_total",
            "counter",
            "Seconds spent reading, parsing and encoding, cumulative. Paired with ship_seconds this is the breakdown that decides whether a faster wire is worth building.",
            format!("{:.6}", g(&self.read_ns) as f64 / 1e9),
        );

        // -- the queue -------------------------------------------------
        m(
            "tributary_queue_bytes",
            "gauge",
            "Bytes spooled on disk awaiting delivery.",
            g(&self.queue_bytes).to_string(),
        );
        m(
            "tributary_queue_segments",
            "gauge",
            "Spool segments awaiting delivery.",
            g(&self.queue_segments).to_string(),
        );
        m(
            "tributary_queue_full",
            "gauge",
            "1 when the spool is at its cap and the agent has STOPPED READING rather than drop. The lines are still in the source files.",
            if self.queue_full.load(Ordering::Relaxed) {
                "1"
            } else {
                "0"
            }
            .into(),
        );
        m(
            "tributary_queue_spilled_total",
            "counter",
            "Batches written to the spool because a ship failed.",
            g(&self.spilled_total).to_string(),
        );
        m(
            "tributary_queue_drained_total",
            "counter",
            "Spool segments successfully re-shipped.",
            g(&self.drained_total).to_string(),
        );

        // -- P1-7 exposure ---------------------------------------------
        m(
            "tributary_at_risk_lines",
            "gauge",
            "Lines read but not yet acknowledged, held only in memory. Lost if this node vanishes.",
            g(&self.pending_lines).to_string(),
        );
        m(
            "tributary_inflight_batches",
            "gauge",
            "Batches in flight. Each holds up to batch_lines lines that are also at risk.",
            g(&self.inflight_batches).to_string(),
        );
        m(
            "tributary_unread_bytes",
            "gauge",
            "Bytes written to the sources but not yet read. On a node that vanishes these are lost too, because the log files go with it.",
            g(&self.unread_bytes).to_string(),
        );

        // -- the sources -----------------------------------------------
        m(
            "tributary_files_open",
            "gauge",
            "Source files currently open, including any still draining after a rotation.",
            g(&self.files_open).to_string(),
        );
        m(
            "tributary_files_lost_total",
            "counter",
            "Files that rotated away before they were fully read. Any non-zero value is data loss.",
            g(&self.files_lost).to_string(),
        );
        m(
            "tributary_rotations_total",
            "counter",
            "Source rotations observed and followed.",
            g(&self.rotations).to_string(),
        );
        m(
            "tributary_pk_disambiguated_total",
            "counter",
            "Records whose timestamp collided within a tick and were given a sub-tick sequence, so the primary key stays unique.",
            g(&self.out_of_window).to_string(),
        );
        m(
            "tributary_watermark_violations_total",
            "counter",
            "Records that arrived older than the published watermark.",
            g(&self.watermark_violations).to_string(),
        );

        // -- L4 credential ---------------------------------------------
        let expiry = self.cert_expires_in_secs.load(Ordering::Relaxed);
        m(
            "tributary_credential_expiry_seconds",
            "gauge",
            "Seconds until the client certificate expires. -1 when none is configured, which is not an alarm. This is the series to page on: a renewal that silently stops landing shows up here long before the handshake fails.",
            expiry.to_string(),
        );
        m(
            "tributary_credential_healthy",
            "gauge",
            "0 after a certificate renewal was REFUSED by the validate-before-swap gate. The last-good pair keeps shipping, so nothing is broken yet — but the certificate has stopped being renewed.",
            if self.cert_healthy.load(Ordering::Relaxed) {
                "1"
            } else {
                "0"
            }
            .into(),
        );
        m(
            "tributary_credential_renewals_refused_total",
            "counter",
            "Certificate renewals refused because they failed validation.",
            g(&self.cert_renewals_refused).to_string(),
        );

        // -- process ---------------------------------------------------
        m(
            "tributary_uptime_seconds",
            "gauge",
            "Seconds since the agent started.",
            self.uptime_secs().to_string(),
        );
        m(
            "tributary_last_tick_age_seconds",
            "gauge",
            "Seconds since the main loop last turned. This is what /healthz reads.",
            format!("{:.3}", self.since_tick_ms() as f64 / 1000.0),
        );
        s
    }
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tel() -> Arc<Telemetry> {
        Telemetry::new(Arc::new(Counters::default()))
    }

    #[test]
    fn renders_valid_prometheus_exposition() {
        let t = tel();
        let out = t.render_prometheus();
        // Every metric must carry HELP and TYPE, and every sample line must
        // be `name value` — a malformed exposition is silently dropped by
        // most scrapers, which is the worst way to have no monitoring.
        let mut names = Vec::new();
        for line in out.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut it = rest.split_whitespace();
                let name = it.next().expect("TYPE names a metric");
                let kind = it.next().expect("TYPE names a kind");
                assert!(
                    ["counter", "gauge", "histogram", "summary"].contains(&kind),
                    "unknown metric type {kind} for {name}"
                );
                names.push(name.to_string());
            } else if !line.starts_with('#') && !line.trim().is_empty() {
                let mut it = line.split_whitespace();
                let name = it.next().unwrap();
                let value = it.next().unwrap_or_else(|| panic!("{name} has no value"));
                value
                    .parse::<f64>()
                    .unwrap_or_else(|_| panic!("{name} value {value:?} is not a number"));
                assert!(it.next().is_none(), "{name} has trailing junk");
            }
        }
        assert!(
            names.len() >= 25,
            "expected the full set, got {}",
            names.len()
        );
        for required in [
            "tributary_lines_read_total",
            "tributary_lines_shipped_total",
            "tributary_lines_quarantined_total",
            "tributary_queue_bytes",
            "tributary_files_lost_total",
            "tributary_credential_expiry_seconds",
        ] {
            assert!(names.iter().any(|n| n == required), "missing {required}");
        }
    }

    #[test]
    fn every_metric_name_is_unique() {
        // A duplicated name makes a scrape ambiguous and some scrapers
        // reject the whole payload.
        let out = tel().render_prometheus();
        let mut seen = std::collections::BTreeSet::new();
        for line in out.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split_whitespace().next().unwrap().to_string();
                assert!(seen.insert(name.clone()), "duplicate metric {name}");
            }
        }
    }

    /// DESIGN.md §6.2: "`lines_read` minus `lines_shipped` minus
    /// `lines_quarantined` should be the queue depth. If it is not,
    /// something is being lost, and the metric set is designed so that
    /// arithmetic is checkable from the outside."
    ///
    /// This pins that the exported set actually permits the subtraction —
    /// all three terms present, same units, all monotonic counters.
    #[test]
    fn the_set_supports_the_accounting_invariant() {
        let t = tel();
        t.lines_read.store(1000, Ordering::Relaxed);
        t.ship.shipped.store(900, Ordering::Relaxed);
        t.quarantined.store(10, Ordering::Relaxed);
        t.pending_lines.store(90, Ordering::Relaxed);

        let out = t.render_prometheus();
        let val = |name: &str| -> f64 {
            out.lines()
                .find(|l| l.starts_with(&format!("{name} ")))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{name} not exported"))
        };
        let unaccounted = val("tributary_lines_read_total")
            - val("tributary_lines_shipped_total")
            - val("tributary_lines_quarantined_total");
        assert_eq!(
            unaccounted,
            val("tributary_at_risk_lines"),
            "read - shipped - quarantined must equal what is still held"
        );
    }

    #[test]
    fn no_certificate_reports_minus_one_not_expired() {
        // -1 and 0 mean very different things: "none configured" must not
        // page someone at 3am for an expiry that does not exist.
        let out = tel().render_prometheus();
        assert!(out.contains("tributary_credential_expiry_seconds -1"));
    }

    #[test]
    fn liveness_age_grows_and_a_tick_resets_it() {
        let t = tel();
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(t.since_tick_ms() >= 25, "age should grow between ticks");
        t.tick();
        assert!(t.since_tick_ms() < 25, "a tick resets the age");
    }
}
