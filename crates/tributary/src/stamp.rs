//! Timestamp assignment — the piece that stops TimeLakeDB silently
//! eating most of a busy service's logs (DESIGN.md §1.1, §3.1).
//!
//! TimeLakeDB's primary key is the tag set plus the timestamp, and a
//! duplicate is resolved last-write-wins. Ten lines sharing a
//! millisecond and a tag set are therefore ONE row, and the nine that
//! lost are gone with a 204 and no error anywhere.
//!
//! So when the source timestamp is coarser than nanoseconds, the unused
//! precision is filled with a per-stream sequence number:
//!
//! ```text
//! ts_ns = floor_to_tick(source_ts_ns) + seq        0 <= seq < tick_ns
//! ```
//!
//! At millisecond resolution that is a million slots per millisecond.
//! The assignment is deterministic given the stream's position, which is
//! what makes an at-least-once retry collapse into the original row
//! rather than duplicating it (§3.2).

use std::collections::HashMap;

/// How many nanoseconds one source tick covers. `Ns` means the source
/// already carries full precision and there is nothing to fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl Resolution {
    pub const fn tick_ns(self) -> i64 {
        match self {
            Resolution::Seconds => 1_000_000_000,
            Resolution::Millis => 1_000_000,
            Resolution::Micros => 1_000,
            Resolution::Nanos => 1,
        }
    }

    pub fn parse(s: &str) -> Option<Resolution> {
        match s {
            "s" | "sec" | "seconds" => Some(Resolution::Seconds),
            "ms" | "millis" | "milliseconds" => Some(Resolution::Millis),
            "us" | "micros" | "microseconds" => Some(Resolution::Micros),
            "ns" | "nanos" | "nanoseconds" => Some(Resolution::Nanos),
            _ => None,
        }
    }
}

/// How many recent ticks stay addressable. Logs inside one file are
/// near-ordered, but "near" is not "always" — a multiline join or a
/// source that stamps at write time can step back a few milliseconds.
/// Resetting the sequence on every tick change would then re-issue
/// timestamps already used, which is the exact silent-loss failure this
/// module exists to prevent, so recent ticks keep their own counters.
const WINDOW: usize = 64;

#[derive(Debug)]
pub enum StampError {
    /// More lines in one source tick than the tick has slots — over a
    /// million lines in a millisecond, per stream.
    Exhausted { tick_ns: i64, source_ts_ns: i64 },
}

impl std::fmt::Display for StampError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StampError::Exhausted {
                tick_ns,
                source_ts_ns,
            } => write!(
                f,
                "more than {tick_ns} lines share source timestamp {source_ts_ns}; \
                 the tick has no slots left"
            ),
        }
    }
}

/// Per-stream timestamp assignment. Cheap: one small map, no allocation
/// on the hot path once warm.
#[derive(Debug)]
pub struct Stamper {
    tick_ns: i64,
    /// tick -> next free sequence within it.
    next: HashMap<i64, i64>,
    /// Insertion order, so the window prunes oldest-first.
    order: std::collections::VecDeque<i64>,
    /// Lines whose tick had already fallen out of the window. Their
    /// uniqueness is not guaranteed, so they are counted rather than
    /// hidden — a non-zero value means the source is more out-of-order
    /// than WINDOW covers.
    pub out_of_window: u64,
}

impl Stamper {
    pub fn new(resolution: Resolution) -> Stamper {
        Stamper {
            tick_ns: resolution.tick_ns(),
            next: HashMap::new(),
            order: std::collections::VecDeque::new(),
            out_of_window: 0,
        }
    }

    /// Assign a unique nanosecond timestamp for a line whose source
    /// timestamp is `source_ts_ns`.
    pub fn stamp(&mut self, source_ts_ns: i64) -> Result<i64, StampError> {
        if self.tick_ns == 1 {
            // Nothing to fill: the source claims full precision, so its
            // uniqueness is its own problem (DESIGN.md §3.3).
            return Ok(source_ts_ns);
        }
        let tick = source_ts_ns - source_ts_ns.rem_euclid(self.tick_ns);

        let seq = match self.next.get_mut(&tick) {
            Some(seq) => {
                let s = *seq;
                if s >= self.tick_ns {
                    return Err(StampError::Exhausted {
                        tick_ns: self.tick_ns,
                        source_ts_ns,
                    });
                }
                *seq = s + 1;
                s
            }
            None => {
                // A tick older than everything we still remember cannot
                // be guaranteed unique — count it and carry on rather
                // than dropping the line.
                if let Some(&oldest) = self.order.front()
                    && tick < oldest
                {
                    self.out_of_window += 1;
                }
                self.next.insert(tick, 1);
                self.order.push_back(tick);
                while self.order.len() > WINDOW {
                    if let Some(evicted) = self.order.pop_front() {
                        self.next.remove(&evicted);
                    }
                }
                0
            }
        };
        Ok(tick + seq)
    }

    /// Restore state so a resumed stream continues the sequence instead
    /// of restarting it — without this, the lines after a checkpoint
    /// that landed mid-tick overwrite the ones before it (§3.2).
    ///
    /// Exercised by tests now and by the checkpoint at L1; kept here
    /// because the property it protects is proven in `stamp::tests`,
    /// not because the caller exists yet.
    #[allow(dead_code)]
    pub fn restore(&mut self, tick: i64, next_seq: i64) {
        if self.tick_ns == 1 {
            return;
        }
        self.next.insert(tick, next_seq);
        self.order.push_back(tick);
    }

    /// The state a checkpoint must carry: (tick, next sequence).
    #[allow(dead_code)] // consumed by the checkpoint writer at L1
    pub fn checkpoint(&self) -> Option<(i64, i64)> {
        let tick = *self.order.back()?;
        self.next.get(&tick).map(|&seq| (tick, seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: i64 = 1_000_000;

    #[test]
    fn fills_the_unused_precision_in_order() {
        let mut s = Stamper::new(Resolution::Millis);
        let t = 1_786_280_343_206 * MS;
        let stamped: Vec<i64> = (0..10).map(|_| s.stamp(t).unwrap()).collect();

        // ten lines in one millisecond become ten distinct timestamps...
        assert_eq!(stamped.len(), 10);
        let unique: std::collections::HashSet<_> = stamped.iter().collect();
        assert_eq!(unique.len(), 10, "this is the whole point: no collisions");
        // ...in file order...
        assert!(stamped.windows(2).all(|w| w[0] < w[1]));
        // ...and all still inside the millisecond they belong to.
        assert!(stamped.iter().all(|&x| x >= t && x < t + MS));
    }

    #[test]
    fn a_replay_reproduces_identical_timestamps() {
        // At-least-once delivery is only safe because a replayed line
        // lands on the same primary key and dedups away (§3.2).
        let t = 1_786_280_343_206 * MS;
        let run = |restore: Option<(i64, i64)>| -> Vec<i64> {
            let mut s = Stamper::new(Resolution::Millis);
            if let Some((tick, seq)) = restore {
                s.restore(tick, seq);
            }
            (0..5).map(|_| s.stamp(t).unwrap()).collect()
        };

        let first = run(None);
        // A crash after shipping lines 0..2 but before checkpointing:
        // resume from the checkpoint that HAD been written (tick, 2)
        // and the replayed lines must match the originals exactly.
        let resumed = run(Some((t, 2)));
        assert_eq!(&first[2..], &resumed[..3]);
    }

    #[test]
    fn out_of_order_within_the_window_does_not_collide() {
        let mut s = Stamper::new(Resolution::Millis);
        let base = 1_786_280_343_206 * MS;
        let mut seen = std::collections::HashSet::new();

        // forward, then a step back (a late multiline join), then forward
        for t in [base, base, base + MS, base + MS, base, base + 2 * MS, base] {
            assert!(seen.insert(s.stamp(t).unwrap()), "re-issued a timestamp");
        }
        assert_eq!(s.out_of_window, 0);
    }

    #[test]
    fn a_tick_beyond_the_window_is_counted_not_hidden() {
        let mut s = Stamper::new(Resolution::Millis);
        let base = 1_786_280_343_206 * MS;
        for i in 0..(WINDOW as i64 + 8) {
            s.stamp(base + i * MS).unwrap();
        }
        assert_eq!(s.out_of_window, 0);
        s.stamp(base).unwrap(); // long since evicted
        assert_eq!(
            s.out_of_window, 1,
            "unguaranteed uniqueness must be visible"
        );
    }

    #[test]
    fn exhausting_a_tick_is_an_error_not_a_silent_wrap() {
        // A wrap would re-issue timestamps and destroy rows, which is
        // precisely the failure being prevented — so it must be loud.
        let mut s = Stamper::new(Resolution::Micros); // only 1000 slots
        let t = 1_786_280_343_206_000_i64;
        for _ in 0..1000 {
            s.stamp(t).unwrap();
        }
        assert!(matches!(s.stamp(t), Err(StampError::Exhausted { .. })));
    }

    #[test]
    fn nanosecond_sources_pass_through_untouched() {
        let mut s = Stamper::new(Resolution::Nanos);
        let t = 1_786_280_343_206_123_456_i64;
        assert_eq!(s.stamp(t).unwrap(), t);
        assert_eq!(s.stamp(t).unwrap(), t, "no room to disambiguate; §3.3");
    }

    #[test]
    fn seconds_resolution_has_a_billion_slots() {
        let mut s = Stamper::new(Resolution::Seconds);
        let t = 1_786_280_343 * 1_000_000_000i64;
        let a = s.stamp(t).unwrap();
        let b = s.stamp(t).unwrap();
        assert_eq!(a, t);
        assert_eq!(b, t + 1);
    }
}
