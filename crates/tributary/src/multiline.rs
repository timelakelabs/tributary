//! Multiline joins — turning a stack trace back into one record.
//!
//! A line matching `starts_with` begins a record; anything else belongs
//! to the record above it. The joined text carries the timestamp of the
//! *first* line, because that is when the event happened, not when the
//! writer finished describing it.
//!
//! Three bounds, all of them there so an unterminated record cannot pin
//! memory: a line cap, a byte cap, and a timeout for the case that
//! matters most in practice — the last record in a quiet file, which has
//! no successor to close it and would otherwise never be emitted.
//!
//! **The checkpoint interaction is the subtle part.** A record spans
//! several source lines, and the tailer's offset advances as each is
//! read. Checkpointing while a record is half-assembled would record
//! progress *past* lines that have not been shipped, and a crash would
//! resume after them — losing the record. So progress is not recorded
//! while a record is pending (see `has_pending`), which costs a little
//! rework after a crash and never costs a line.

use std::time::{Duration, Instant};

pub struct Joiner {
    start: Option<regex::Regex>,
    max_lines: usize,
    max_bytes: usize,
    timeout: Duration,
    buf: Vec<String>,
    bytes: usize,
    since: Option<Instant>,
    pub truncated: u64,
}

impl Joiner {
    /// `None` for `starts_with` disables joining entirely: every line is
    /// its own record, which is what single-line formats want.
    pub fn new(
        starts_with: Option<&str>,
        max_lines: usize,
        max_bytes: usize,
        timeout_ms: u64,
    ) -> anyhow::Result<Joiner> {
        let start = match starts_with {
            Some(p) => Some(regex::Regex::new(p)?),
            None => None,
        };
        Ok(Joiner {
            start,
            max_lines: max_lines.max(1),
            max_bytes: max_bytes.max(1),
            timeout: Duration::from_millis(timeout_ms),
            buf: Vec::new(),
            bytes: 0,
            since: None,
            truncated: 0,
        })
    }

    /// True while a record is half-assembled. The caller must not record
    /// progress past these lines.
    pub fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// Offer a line. Returns a completed record, if this line closed one.
    pub fn push(&mut self, line: String) -> Option<String> {
        let Some(re) = &self.start else {
            return Some(line); // joining disabled
        };

        let starts_new = re.is_match(&line);
        let mut done = None;

        if starts_new && !self.buf.is_empty() {
            done = Some(self.take());
        }

        // A continuation arriving with nothing above it has lost its
        // start — a file opened mid-record, or a rotation that split one.
        // Keep it as its own record rather than discarding it.
        self.bytes += line.len() + 1;
        self.buf.push(line);
        if self.since.is_none() {
            self.since = Some(Instant::now());
        }

        // Bounds: emit what we have rather than growing without limit.
        if self.buf.len() >= self.max_lines || self.bytes >= self.max_bytes {
            self.truncated += 1;
            let forced = self.take();
            return Some(match done {
                // Two records completed at once is impossible to return,
                // so prefer the earlier and re-buffer the forced one.
                Some(d) => {
                    self.buf.push(forced);
                    self.bytes = self.buf[0].len() + 1;
                    self.since = Some(Instant::now());
                    d
                }
                None => forced,
            });
        }
        done
    }

    /// Emit a pending record whose timeout has elapsed. Without this the
    /// last record in a quiet file waits forever for a successor.
    pub fn expire(&mut self) -> Option<String> {
        let since = self.since?;
        if !self.buf.is_empty() && since.elapsed() >= self.timeout {
            return Some(self.take());
        }
        None
    }

    /// Emit whatever is buffered, regardless of the timeout — shutdown.
    pub fn drain(&mut self) -> Option<String> {
        (!self.buf.is_empty()).then(|| self.take())
    }

    fn take(&mut self) -> String {
        let joined = self.buf.join("\n");
        self.buf.clear();
        self.bytes = 0;
        self.since = None;
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn joiner() -> Joiner {
        Joiner::new(Some(r"^\d{4}-\d{2}-\d{2}"), 500, 65536, 50).unwrap()
    }

    #[test]
    fn a_stack_trace_becomes_one_record() {
        let mut j = joiner();
        assert_eq!(j.push("2026-08-10 first".into()), None);
        assert_eq!(j.push("  at foo()".into()), None);
        assert_eq!(j.push("  at bar()".into()), None);

        // the NEXT start line is what closes the previous record
        let done = j.push("2026-08-10 second".into()).unwrap();
        assert_eq!(done, "2026-08-10 first\n  at foo()\n  at bar()");
        assert!(j.has_pending(), "the second record is now open");
    }

    #[test]
    fn disabled_passes_every_line_straight_through() {
        let mut j = Joiner::new(None, 500, 65536, 50).unwrap();
        assert_eq!(j.push("anything".into()).unwrap(), "anything");
        assert!(!j.has_pending());
    }

    #[test]
    fn the_last_record_in_a_quiet_file_is_emitted_on_timeout() {
        // Nothing follows it, so without the timeout it would sit in
        // memory until the process exited.
        let mut j = joiner();
        j.push("2026-08-10 lonely".into());
        assert!(j.expire().is_none(), "not yet");
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(j.expire().unwrap(), "2026-08-10 lonely");
        assert!(!j.has_pending());
    }

    #[test]
    fn an_unbounded_record_is_cut_rather_than_growing_forever() {
        let mut j = Joiner::new(Some(r"^START"), 4, 65536, 1000).unwrap();
        j.push("START".into());
        j.push("a".into());
        j.push("b".into());
        let cut = j.push("c".into()).expect("hit the line cap");
        assert_eq!(cut, "START\na\nb\nc");
        assert_eq!(j.truncated, 1);
    }

    #[test]
    fn the_byte_cap_also_cuts() {
        let mut j = Joiner::new(Some(r"^START"), 500, 32, 1000).unwrap();
        j.push("START".into());
        let cut = j.push("x".repeat(40)).expect("hit the byte cap");
        assert!(cut.starts_with("START\n"));
        assert_eq!(j.truncated, 1);
    }

    #[test]
    fn a_continuation_with_no_start_is_kept_not_discarded() {
        // Opening a file mid-record, or a rotation splitting one: the
        // orphan is still a line somebody wrote.
        let mut j = joiner();
        assert_eq!(j.push("  at orphan()".into()), None);
        let done = j.push("2026-08-10 next".into()).unwrap();
        assert_eq!(done, "  at orphan()");
    }

    #[test]
    fn pending_state_tracks_the_checkpoint_hold() {
        let mut j = joiner();
        assert!(!j.has_pending(), "nothing buffered, safe to checkpoint");
        j.push("2026-08-10 open".into());
        assert!(j.has_pending(), "must not record progress past this");
        j.drain();
        assert!(!j.has_pending());
    }
}
