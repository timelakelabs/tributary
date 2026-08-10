//! Watermarks — a completeness claim a reader cannot otherwise make:
//! *every line from this stream at or before `T` is durably stored.*
//!
//! The lateness allowance is **observed from the stream**, not
//! configured (ROADMAP §2.2), because a constant goes stale the moment
//! someone turns on stack traces. Four properties keep it honest:
//!
//! - a high quantile rather than the maximum, so one pathological line
//!   cannot pin the watermark behind it forever;
//! - a configured floor and ceiling, because a perfectly ordered stream
//!   would observe zero lateness and produce a watermark that any jitter
//!   violates;
//! - **a cold start at the ceiling, tightening as samples arrive** —
//!   assuming zero lateness on restart would over-claim completeness
//!   exactly when someone is checking after an incident;
//! - the published value never regresses, and a line arriving below it
//!   is counted rather than hidden.
//!
//! A violation is an inaccuracy in a *claim*, never lost data: the line
//! is still written normally. That distinction is what makes the number
//! worth publishing.

use std::collections::VecDeque;

/// How many lateness samples the estimate is drawn from.
const WINDOW: usize = 4096;
/// Below this many samples the estimate is not trusted and the ceiling
/// stands — the cold-start rule above.
const MIN_SAMPLES: usize = 256;
/// The quantile taken from the window.
const QUANTILE: f64 = 0.999;

#[derive(Debug)]
pub struct Watermark {
    floor_ns: i64,
    ceiling_ns: i64,
    samples: VecDeque<i64>,
    max_ts_seen: i64,
    lateness_ns: i64,
    published_ns: i64,
    pub violations: u64,
}

impl Watermark {
    pub fn new(floor_ns: i64, ceiling_ns: i64) -> Watermark {
        Watermark {
            floor_ns,
            ceiling_ns: ceiling_ns.max(floor_ns),
            samples: VecDeque::with_capacity(WINDOW),
            max_ts_seen: i64::MIN,
            // Conservative until proven otherwise.
            lateness_ns: ceiling_ns.max(floor_ns),
            published_ns: i64::MIN,
            violations: 0,
        }
    }

    /// Restore the estimate a previous run had converged on, so a
    /// restart resumes rather than re-learns.
    pub fn restore(&mut self, lateness_ns: i64) {
        self.lateness_ns = lateness_ns.clamp(self.floor_ns, self.ceiling_ns);
    }

    pub fn lateness_ns(&self) -> i64 {
        self.lateness_ns
    }

    pub fn published_ns(&self) -> i64 {
        self.published_ns
    }

    /// Record a line's source timestamp as it is read.
    pub fn observe(&mut self, source_ts_ns: i64) {
        if self.max_ts_seen != i64::MIN {
            let lateness = (self.max_ts_seen - source_ts_ns).max(0);
            if self.samples.len() == WINDOW {
                self.samples.pop_front();
            }
            self.samples.push_back(lateness);
        }
        self.max_ts_seen = self.max_ts_seen.max(source_ts_ns);

        // A line below the published watermark contradicts a claim
        // already made. The data is fine; the claim was optimistic.
        if self.published_ns != i64::MIN && source_ts_ns < self.published_ns {
            self.violations += 1;
        }
    }

    fn estimate(&self) -> i64 {
        if self.samples.len() < MIN_SAMPLES {
            return self.ceiling_ns;
        }
        let mut v: Vec<i64> = self.samples.iter().copied().collect();
        v.sort_unstable();
        let idx = ((v.len() as f64 - 1.0) * QUANTILE).round() as usize;
        v[idx].clamp(self.floor_ns, self.ceiling_ns)
    }

    /// Advance the watermark after a batch is durable. `acked_max_ts_ns`
    /// is the highest source timestamp in what just landed.
    ///
    /// Files are drained sequentially (rotated files finish before the
    /// live one), so a single high-water mark is sound here. Reading
    /// several files concurrently would need the `min` across them —
    /// that is a later concern, and this comment is the reminder.
    pub fn advance(&mut self, acked_max_ts_ns: i64) {
        self.lateness_ns = self.estimate();
        let candidate = acked_max_ts_ns.saturating_sub(self.lateness_ns);
        // Monotonic by construction: a watermark that went backwards
        // would un-claim data a reader has already trusted.
        if candidate > self.published_ns {
            self.published_ns = candidate;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: i64 = 1_000_000;

    fn wm() -> Watermark {
        Watermark::new(10 * MS, 5_000 * MS)
    }

    #[test]
    fn cold_start_is_conservative_not_optimistic() {
        // The trap: assuming zero lateness on a fresh start over-claims
        // completeness exactly when someone checks after an incident.
        let mut w = wm();
        assert_eq!(w.lateness_ns(), 5_000 * MS, "must begin at the ceiling");

        let t = 1_786_280_343_206 * MS;
        w.observe(t);
        w.advance(t);
        assert_eq!(
            w.published_ns(),
            t - 5_000 * MS,
            "the first claim must lag by the full ceiling"
        );
    }

    #[test]
    fn tightens_once_enough_samples_have_arrived() {
        let mut w = wm();
        let base = 1_786_280_343_206 * MS;
        // a well-ordered stream: no line is ever late
        for i in 0..(MIN_SAMPLES as i64 + 10) {
            w.observe(base + i * MS);
        }
        w.advance(base + 300 * MS);
        assert_eq!(
            w.lateness_ns(),
            10 * MS,
            "an ordered stream should settle at the floor, not zero"
        );
    }

    #[test]
    fn a_late_tail_widens_the_allowance() {
        let mut w = wm();
        let base = 1_786_280_343_206 * MS;
        for i in 0..500 {
            w.observe(base + i * MS);
        }
        // 1% of lines arrive 200 ms late — a multiline join completing
        for i in 0..500 {
            if i % 100 == 0 {
                w.observe(base + (500 + i) * MS - 200 * MS);
            }
            w.observe(base + (500 + i) * MS);
        }
        w.advance(base + 1000 * MS);
        assert!(
            w.lateness_ns() > 10 * MS,
            "observed lateness should exceed the floor, got {}",
            w.lateness_ns()
        );
        assert!(w.lateness_ns() <= 5_000 * MS);
    }

    #[test]
    fn one_pathological_line_does_not_pin_the_watermark() {
        let mut w = wm();
        let base = 1_786_280_343_206 * MS;
        for i in 0..1000 {
            w.observe(base + i * MS);
        }
        // a single line an hour old
        w.observe(base - 3_600_000 * MS);
        w.advance(base + 1000 * MS);
        assert!(
            w.lateness_ns() < 1000 * MS,
            "a quantile, not a maximum: got {}",
            w.lateness_ns()
        );
    }

    #[test]
    fn never_regresses_and_counts_what_it_missed() {
        let mut w = wm();
        let base = 1_786_280_343_206 * MS;
        for i in 0..(MIN_SAMPLES as i64 + 10) {
            w.observe(base + i * MS);
        }
        w.advance(base + 1000 * MS);
        let high = w.published_ns();

        // an older batch lands afterwards
        w.advance(base + 10 * MS);
        assert_eq!(w.published_ns(), high, "the claim must not be withdrawn");

        // and a line below the published mark is counted, not hidden
        let before = w.violations;
        w.observe(w.published_ns() - 1);
        assert_eq!(w.violations, before + 1);
    }

    #[test]
    fn restore_resumes_the_converged_estimate() {
        let mut w = wm();
        w.restore(250 * MS);
        assert_eq!(w.lateness_ns(), 250 * MS);
        // and a restored value is still bounded by the configured range
        w.restore(999_999 * MS);
        assert_eq!(w.lateness_ns(), 5_000 * MS);
    }
}
