//! CRI text-log reassembly (#71).
//!
//! containerd and CRI-O — the default Kubernetes runtimes — do NOT write JSON.
//! Each container-log line is the CRI text format:
//!
//! ```text
//! 2024-01-01T00:00:00.123456789Z stdout F the actual log message
//! ```
//!
//! four space-separated fields: an RFC3339-nanosecond timestamp, the stream
//! (`stdout`/`stderr`), a tag (`F` = full/final, `P` = partial), and the
//! message (which may itself contain spaces). A line longer than the kubelet's
//! read buffer (~16 KB) is split into several `P` entries followed by one `F`,
//! and only the `F` terminates the logical line.
//!
//! Shipping these with `parser = "plain"` buries the real timestamp, stream and
//! `F`/`P` marker inside `message` and stamps the record at ingestion time.
//! This reassembler pulls them out: it joins `P`…`F` back into one message
//! (per stream, so an interleaved stdout partial can't swallow a stderr line)
//! and re-emits the SAME `{"log","stream","time"}` envelope the docker
//! reassembler produces, so the ordinary map path handles it identically — the
//! `stream` tag, the declared fields and the RFC3339 timestamp all apply, and a
//! config written for `docker_json` works unchanged for `cri`.
//!
//! It deliberately mirrors [`crate::docker::Reassembler`] — same
//! `push`/`expire`/`drain`/`has_pending` shape and the same checkpoint rule
//! (`has_pending` true while a message is half-assembled). The differences from
//! docker are exactly two: the input is text, not JSON, and a CRI message
//! carries no trailing newline (the `F` tag is the terminator), so nothing is
//! stripped off the end.

use std::collections::HashMap;
use std::time::{Duration, Instant};

// The shared frame-parse error. Named for docker because it landed there first;
// a line that isn't a CRI line is quarantined through it, never dropped.
use crate::docker::DockerError;

struct Pending {
    log: String,
    /// The FIRST fragment's timestamp — when the write began, matching docker.
    time: String,
    since: Instant,
}

pub struct Reassembler {
    max_bytes: usize,
    timeout: Duration,
    streams: HashMap<String, Pending>,
    /// Messages force-emitted at the byte cap (like `Reassembler::truncated`).
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

    /// True while any stream has a half-assembled message; the caller must not
    /// checkpoint past these fragments.
    pub fn has_pending(&self) -> bool {
        !self.streams.is_empty()
    }

    /// Offer one CRI text line. Returns a complete envelope when an `F` (or the
    /// byte cap) closes the message, `Ok(None)` while buffering a `P`, and
    /// `Err` when the line is not the CRI format.
    pub fn push(&mut self, line: &str) -> Result<Option<String>, DockerError> {
        // `<time> <stream> <F|P> <message>` — the message keeps its spaces, so
        // split off only the first three fields.
        let mut parts = line.splitn(4, ' ');
        let time = parts
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DockerError("empty CRI line".into()))?;
        let stream = parts
            .next()
            .ok_or_else(|| DockerError(format!("CRI line has no stream field: {line:?}")))?;
        let tag = parts
            .next()
            .ok_or_else(|| DockerError(format!("CRI line has no F/P tag: {line:?}")))?;
        // A missing fourth field is an empty message, not an error — a bare
        // blank log line is legal.
        let msg = parts.next().unwrap_or("");

        if stream != "stdout" && stream != "stderr" {
            return Err(DockerError(format!(
                "CRI stream is {stream:?}, not stdout/stderr — not a CRI line"
            )));
        }
        let partial = match tag {
            "P" => true,
            "F" => false,
            other => {
                return Err(DockerError(format!(
                    "CRI tag is {other:?}, not P or F — not a CRI line"
                )));
            }
        };

        let p = self
            .streams
            .entry(stream.to_string())
            .or_insert_with(|| Pending {
                log: String::new(),
                time: time.to_string(),
                since: Instant::now(),
            });
        p.log.push_str(msg);
        let over = p.log.len() >= self.max_bytes;

        if !partial || over {
            if over && partial {
                self.truncated += 1;
            }
            let p = self.streams.remove(stream).expect("just inserted");
            // No trailing-newline strip: unlike docker's `log`, a CRI message
            // has none — the `F` tag is the terminator.
            return Ok(Some(envelope(&p.log, stream, &p.time)));
        }
        Ok(None)
    }

    /// Emit one pending message whose partial has sat past the timeout — the
    /// trailing `P` in a quiet file that will get no closing `F`.
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

/// Re-serialize a reassembled message as the docker-shaped envelope the map
/// path already understands. `serde_json` does the escaping, so a message full
/// of quotes survives the round trip.
fn envelope(log: &str, stream: &str, time: &str) -> String {
    serde_json::json!({ "log": log, "stream": stream, "time": time }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn parse(env: &str) -> (String, String, String) {
        let v: Value = serde_json::from_str(env).unwrap();
        (
            v["log"].as_str().unwrap().to_string(),
            v["stream"].as_str().unwrap().to_string(),
            v["time"].as_str().unwrap().to_string(),
        )
    }

    #[test]
    fn a_full_line_becomes_one_record_with_time_and_stream_pulled_out() {
        let mut r = Reassembler::new(1 << 20, 1000);
        let out = r
            .push("2024-01-01T00:00:00.123456789Z stdout F hello world with spaces")
            .unwrap()
            .expect("an F line completes immediately");
        let (log, stream, time) = parse(&out);
        assert_eq!(log, "hello world with spaces");
        assert_eq!(stream, "stdout");
        // Nanosecond precision survives — the map path parses this as the record
        // time instead of stamping ingestion time.
        assert_eq!(time, "2024-01-01T00:00:00.123456789Z");
        assert!(!r.has_pending());
    }

    #[test]
    fn partials_reassemble_to_one_line_carrying_the_first_timestamp() {
        let big = "x".repeat(16 * 1024);
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(r.push(&format!("t1 stdout P {big}")).unwrap().is_none());
        assert!(r.has_pending());
        assert!(r.push(&format!("t2 stdout P {big}")).unwrap().is_none());
        let out = r
            .push("t3 stdout F tail")
            .unwrap()
            .expect("the F closes the message");
        let (log, _, time) = parse(&out);
        assert_eq!(log.len(), big.len() * 2 + "tail".len());
        assert!(log.starts_with(&big) && log.ends_with("tail"));
        assert_eq!(time, "t1", "the first fragment's time, not the last");
        assert!(!r.has_pending());
    }

    #[test]
    fn stdout_and_stderr_partials_do_not_cross_contaminate() {
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(r.push("a stdout P out-").unwrap().is_none());
        assert!(r.push("b stderr P err-").unwrap().is_none());
        let o = r.push("c stdout F end").unwrap().unwrap();
        let e = r.push("d stderr F end").unwrap().unwrap();
        assert_eq!(parse(&o).0, "out-end");
        assert_eq!(parse(&e).0, "err-end");
    }

    #[test]
    fn an_empty_message_is_legal() {
        let mut r = Reassembler::new(1 << 20, 1000);
        let out = r
            .push("t stdout F ")
            .unwrap()
            .expect("blank line is a record");
        assert_eq!(parse(&out).0, "");
        // even with no trailing space at all
        let out = r
            .push("t stderr F")
            .unwrap()
            .expect("bare F, empty message");
        assert_eq!(parse(&out).0, "");
    }

    #[test]
    fn a_non_cri_line_is_an_error_not_a_silent_drop() {
        let mut r = Reassembler::new(1 << 20, 1000);
        assert!(
            r.push("this is just a plain log line").is_err(),
            "stream not stdout/stderr"
        );
        assert!(r.push("t stdout X msg").is_err(), "tag not F/P");
        assert!(r.push("").is_err(), "empty line");
        // still works after an error
        assert!(r.push("t stdout F ok").unwrap().is_some());
    }

    #[test]
    fn the_byte_cap_force_emits_rather_than_growing_without_bound() {
        let mut r = Reassembler::new(10, 1000);
        let out = r.push("t stdout P 0123456789abc").unwrap();
        assert!(out.is_some(), "over the cap, emit what we have");
        assert_eq!(r.truncated, 1);
        assert!(!r.has_pending());
    }

    #[test]
    fn expire_flushes_a_trailing_partial() {
        let mut r = Reassembler::new(1 << 20, 0); // timeout 0 => immediately due
        assert!(r.push("t stdout P partial").unwrap().is_none());
        let out = r.expire().expect("a timed-out partial is emitted");
        assert_eq!(parse(&out).0, "partial");
        assert!(!r.has_pending());
    }
}
