//! Docker `json-file` reassembly (#9).
//!
//! Docker's json-file log driver writes one JSON object per line —
//! `{"log":"<chunk>","stream":"stdout"|"stderr","time":"<RFC3339 nanos>"}` —
//! and SPLITS any log line longer than ~16 KB across several such objects.
//! Only the last chunk's `log` ends with a newline; the rest are silent
//! partials. A tail agent that treats each object as a record therefore
//! shreds every long line into pieces, and nothing tells you it happened.
//!
//! This reassembler joins the chunks back. `stdout` and `stderr` accumulate
//! SEPARATELY, because docker interleaves them in the one file and a partial
//! on one must not swallow a line on the other. When a chunk closes a message
//! (its `log` ends in `\n`), the whole message is re-emitted as one complete
//! envelope `{"log":<full>,"stream":…,"time":<first chunk's time>}` — a JSON
//! line the ordinary map path then handles exactly like any other, so the tag
//! allowlist, declared fields and RFC3339 timestamp all apply unchanged.
//!
//! It mirrors [`crate::multiline::Joiner`] — `push`/`expire`/`drain`/
//! `has_pending` — including the checkpoint rule: while a message is
//! half-assembled `has_pending` is true and the caller must not record
//! progress past it, or a crash would resume after chunks it never shipped.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A line that is not a decodable docker json-file object. Quarantined by the
/// caller through the same dead-letter path a map error takes — never dropped.
#[derive(Debug)]
pub struct DockerError(pub String);

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DockerError {}

struct Pending {
    log: String,
    /// The FIRST chunk's timestamp — when the write began, like multiline
    /// carries the first line's time.
    time: String,
    since: Instant,
}

pub struct Reassembler {
    /// Per-stream cap: a message that never terminates cannot pin memory.
    max_bytes: usize,
    timeout: Duration,
    streams: HashMap<String, Pending>,
    /// Messages force-emitted at the byte cap (exposed like `Joiner::truncated`).
    pub truncated: u64,
}

impl Reassembler {
    pub fn new(max_bytes: usize, timeout_ms: u64) -> Reassembler {
        Reassembler {
            max_bytes: max_bytes.max(1),
            timeout: Duration::from_millis(timeout_ms),
            streams: HashMap::new(),
            truncated: 0,
        }
    }

    /// True while any stream has a half-assembled message. The caller must not
    /// checkpoint past these chunks.
    pub fn has_pending(&self) -> bool {
        !self.streams.is_empty()
    }

    /// Offer one docker json-file line. Returns a complete envelope when this
    /// chunk closed a message, `Ok(None)` while still buffering, `Err` when the
    /// line is not a docker json-file object.
    pub fn push(&mut self, line: &str) -> Result<Option<String>, DockerError> {
        let v: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| DockerError(format!("not a docker json-file line: {e}")))?;
        let obj = v
            .as_object()
            .ok_or_else(|| DockerError("docker json-file line is not an object".into()))?;
        let field = |k: &str| {
            obj.get(k)
                .and_then(|x| x.as_str())
                .ok_or_else(|| DockerError(format!("docker json-file line has no string `{k}`")))
        };
        let log = field("log")?;
        let stream = field("stream")?;
        let time = field("time")?;

        let closes = log.ends_with('\n');
        let p = self
            .streams
            .entry(stream.to_string())
            .or_insert_with(|| Pending {
                log: String::new(),
                time: time.to_string(),
                since: Instant::now(),
            });
        p.log.push_str(log);
        let over = p.log.len() >= self.max_bytes;

        if closes || over {
            if over && !closes {
                self.truncated += 1;
            }
            let mut p = self.streams.remove(stream).expect("just inserted");
            // The terminating newline is the line delimiter, not content.
            if closes && p.log.ends_with('\n') {
                p.log.pop();
            }
            return Ok(Some(envelope(&p.log, stream, &p.time)));
        }
        Ok(None)
    }

    /// Emit one pending message whose partial has sat past the timeout — the
    /// trailing partial in a quiet file that will get no closing chunk.
    pub fn expire(&mut self) -> Option<String> {
        let key = self
            .streams
            .iter()
            .find(|(_, p)| p.since.elapsed() >= self.timeout)
            .map(|(k, _)| k.clone())?;
        let p = self.streams.remove(&key).expect("found above");
        Some(envelope(&p.log, &key, &p.time))
    }

    /// Emit one buffered message regardless of timeout — shutdown. Call in a
    /// loop to flush every stream.
    pub fn drain(&mut self) -> Option<String> {
        let key = self.streams.keys().next().cloned()?;
        let p = self.streams.remove(&key).expect("just read the key");
        Some(envelope(&p.log, &key, &p.time))
    }
}

/// Re-serialize a complete message as a docker envelope. `serde_json` does the
/// escaping, so a message full of quotes and backslashes survives the round
/// trip the map path then parses.
fn envelope(log: &str, stream: &str, time: &str) -> String {
    serde_json::json!({ "log": log, "stream": stream, "time": time }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn frame(log: &str, stream: &str, time: &str) -> String {
        serde_json::json!({ "log": log, "stream": stream, "time": time }).to_string()
    }

    fn parse(env: &str) -> (String, String, String) {
        let v: Value = serde_json::from_str(env).unwrap();
        (
            v["log"].as_str().unwrap().to_string(),
            v["stream"].as_str().unwrap().to_string(),
            v["time"].as_str().unwrap().to_string(),
        )
    }

    #[test]
    fn a_single_complete_frame_is_one_record_with_the_newline_stripped() {
        let mut r = Reassembler::new(1 << 20, 1000);
        let out = r
            .push(&frame("hello world\n", "stdout", "2024-01-01T00:00:00.5Z"))
            .unwrap()
            .expect("a terminated frame completes immediately");
        let (log, stream, time) = parse(&out);
        assert_eq!(log, "hello world");
        assert_eq!(stream, "stdout");
        assert_eq!(time, "2024-01-01T00:00:00.5Z");
        assert!(!r.has_pending());
    }

    #[test]
    fn a_split_message_reassembles_to_one_record() {
        // Three chunks, none but the last terminated: the >16 KB case.
        let big = "x".repeat(16 * 1024);
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(r.push(&frame(&big, "stdout", "t1")).unwrap().is_none());
        assert!(r.has_pending());
        assert!(r.push(&frame(&big, "stdout", "t2")).unwrap().is_none());
        let out = r
            .push(&frame("tail\n", "stdout", "t3"))
            .unwrap()
            .expect("the terminated chunk closes the message");
        let (log, _, time) = parse(&out);
        assert_eq!(log.len(), big.len() * 2 + "tail".len());
        assert!(log.starts_with(&big) && log.ends_with("tail"));
        // the FIRST chunk's time, not the last
        assert_eq!(time, "t1");
        assert!(!r.has_pending());
    }

    #[test]
    fn stdout_and_stderr_partials_do_not_cross_contaminate() {
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(
            r.push(&frame("out-part ", "stdout", "a"))
                .unwrap()
                .is_none()
        );
        assert!(
            r.push(&frame("err-part ", "stderr", "b"))
                .unwrap()
                .is_none()
        );
        let o = r.push(&frame("out-end\n", "stdout", "c")).unwrap().unwrap();
        let e = r.push(&frame("err-end\n", "stderr", "d")).unwrap().unwrap();
        assert_eq!(parse(&o).0, "out-part out-end");
        assert_eq!(parse(&e).0, "err-part err-end");
    }

    #[test]
    fn a_malformed_or_incomplete_line_is_an_error_not_a_silent_drop() {
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(r.push("this is not json").is_err());
        assert!(r.push(&frame("x\n", "stdout", "t")).unwrap().is_some()); // still works after
        // missing a required field
        assert!(
            r.push(r#"{"log":"x\n","time":"t"}"#).is_err(),
            "a line without `stream` is rejected"
        );
    }

    #[test]
    fn the_byte_cap_force_emits_rather_than_growing_without_bound() {
        let mut r = Reassembler::new(10, 1000);
        let out = r.push(&frame("0123456789abc", "stdout", "t")).unwrap();
        assert!(out.is_some(), "over the cap, emit what we have");
        assert_eq!(r.truncated, 1);
        assert!(!r.has_pending());
    }

    #[test]
    fn expire_flushes_a_trailing_partial() {
        let mut r = Reassembler::new(1 << 20, 0); // timeout 0 => immediately due
        assert!(r.push(&frame("partial", "stdout", "t")).unwrap().is_none());
        let out = r.expire().expect("a timed-out partial is emitted");
        assert_eq!(parse(&out).0, "partial");
        assert!(!r.has_pending());
    }
}
