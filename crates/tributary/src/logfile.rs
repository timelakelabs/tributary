//! Rotating file sink for the agent's **own** diagnostic log.
//!
//! Not the log files it tails — those are `tail.rs`, and they are rotated
//! by whatever wrote them. This is the agent's own `tracing` output.
//!
//! Under systemd or Docker, stdout is captured and rotated for you and none
//! of this is needed; `[log]` absent leaves that path exactly as it was. It
//! exists for the bare-process deployment, where stdout redirected to a file
//! grows without bound until a disk fills and takes the agent down with it.
//!
//! Rotation fires on **either** trigger, whichever comes first:
//!
//! * `rotate_size` — bytes written to the current file;
//! * `rotate_every` — time since the current file was opened.
//!
//! Elapsed-since-open rather than wall-clock boundaries: `rotate_every =
//! "1d"` means "a day's worth of log per file", not "rolls at midnight".
//! That is simpler to reason about and to test, and for a diagnostic log
//! the boundary alignment buys nothing.
//!
//! `tracing-appender` was the obvious dependency and does not fit: it
//! rotates on time only, and size is the trigger that actually protects the
//! disk. Hand-rolling costs no new crates, which is the same argument T-1
//! made for serving `/metrics` on hyper directly.
//!
//! **This sink owns the file.** Do not also point logrotate at it: two
//! rotators racing on one path is how a log ends up in a deleted inode that
//! nothing can read.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How many rotated files to keep, beyond the live one. `None` = keep
/// everything, which is the safe default for anything anyone might need to
/// read after an incident.
pub type Keep = Option<usize>;

pub struct RotatingLog {
    path: PathBuf,
    size_limit: Option<u64>,
    interval: Option<Duration>,
    keep: Keep,
    inner: Mutex<Inner>,
}

struct Inner {
    file: File,
    written: u64,
    opened: SystemTime,
}

impl RotatingLog {
    pub fn open(
        path: impl Into<PathBuf>,
        size_limit: Option<u64>,
        interval: Option<Duration>,
        keep: Keep,
    ) -> io::Result<Arc<RotatingLog>> {
        let path = path.into();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        // Continue an existing file rather than truncating it: a restart
        // must not discard the log that explains why the process restarted.
        let written = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Arc::new(RotatingLog {
            path,
            size_limit,
            interval,
            keep,
            inner: Mutex::new(Inner {
                file,
                written,
                opened: SystemTime::now(),
            }),
        }))
    }

    /// Would either trigger fire for a write of `next` more bytes?
    fn should_rotate(&self, inner: &Inner, next: u64) -> bool {
        if let Some(limit) = self.size_limit
            && inner.written + next > limit
            // Never rotate an empty file: a single line longer than the
            // limit would otherwise rotate on every write, producing an
            // unbounded number of empty files — the opposite of the point.
            && inner.written > 0
        {
            return true;
        }
        if let Some(every) = self.interval
            && inner.opened.elapsed().unwrap_or_default() >= every
            && inner.written > 0
        {
            return true;
        }
        false
    }

    /// Rename the live file aside and open a fresh one.
    ///
    /// The suffix is a sortable UTC stamp plus a counter, because two
    /// rotations inside one second are entirely possible under a size
    /// trigger and must not collide — a collision would silently destroy
    /// the earlier file.
    fn rotate(&self, inner: &mut Inner) -> io::Result<()> {
        let stamp = stamp_utc(SystemTime::now());
        // APPEND the stamp; do not use `with_extension`, which REPLACES the
        // extension and turns `agent.log` into `agent.20250101-000000` —
        // dropping the `.log` that `rotated_files` scans for, so retention
        // then silently manages nothing.
        let dir = self
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let name = self
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("log")
            .to_string();

        let mut n = 0;
        let target = loop {
            let candidate = if n == 0 {
                dir.join(format!("{name}.{stamp}"))
            } else {
                dir.join(format!("{name}.{stamp}.{n}"))
            };
            if !candidate.exists() {
                break candidate;
            }
            n += 1;
            if n > 1000 {
                // Something is very wrong; keep logging rather than spin.
                return Ok(());
            }
        };

        inner.file.flush()?;
        std::fs::rename(&self.path, &target)?;
        inner.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        inner.written = 0;
        inner.opened = SystemTime::now();
        self.prune();
        Ok(())
    }

    /// Delete the oldest rotated files beyond `keep`. Never touches the
    /// live file, and does nothing at all when `keep` is `None`.
    fn prune(&self) {
        let Some(keep) = self.keep else { return };
        let mut rotated = self.rotated_files();
        if rotated.len() <= keep {
            return;
        }
        // Newest last, so the front is what goes.
        rotated.sort();
        let excess = rotated.len() - keep;
        for p in rotated.into_iter().take(excess) {
            let _ = std::fs::remove_file(p);
        }
    }

    /// Every rotated sibling of the live file, by name.
    pub fn rotated_files(&self) -> Vec<PathBuf> {
        let Some(dir) = self.path.parent() else {
            return Vec::new();
        };
        let Some(stem) = self.path.file_name().and_then(|s| s.to_str()) else {
            return Vec::new();
        };
        let prefix = format!("{stem}.");
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
            })
            .collect()
    }

    fn write_line(&self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.inner.lock().expect("log lock");
        if self.should_rotate(&inner, buf.len() as u64) {
            // A failed rotation must not lose the line or kill the agent:
            // log volume is diagnostic, the shipping path is the product.
            let _ = self.rotate(&mut inner);
        }
        let n = inner.file.write(buf)?;
        inner.written += n as u64;
        Ok(n)
    }
}

/// `tracing-subscriber` writes through this. Cloning it is cheap — it is an
/// `Arc` — and each write takes the lock only for as long as the write.
#[derive(Clone)]
pub struct LogSink(pub Arc<RotatingLog>);

impl Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write_line(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.inner.lock().expect("log lock").file.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `YYYYMMDD-HHMMSS`, UTC, sortable as a string.
fn stamp_utc(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}{m:02}{d:02}-{:02}{:02}{:02}",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `"100MiB"`, `"512KB"`, `"2GiB"`, or a bare byte count.
///
/// Both conventions are accepted because both are written in the wild, and
/// they are NOT the same number: `KiB` is 1024, `KB` is 1000. Silently
/// treating them as equal is the kind of quiet wrongness that shows up as a
/// disk filling 2.4% earlier than someone calculated.
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(v) = s.strip_suffix("GiB") {
        (v, 1024u64.pow(3))
    } else if let Some(v) = s.strip_suffix("MiB") {
        (v, 1024u64.pow(2))
    } else if let Some(v) = s.strip_suffix("KiB") {
        (v, 1024)
    } else if let Some(v) = s.strip_suffix("GB") {
        (v, 1_000_000_000)
    } else if let Some(v) = s.strip_suffix("MB") {
        (v, 1_000_000)
    } else if let Some(v) = s.strip_suffix("KB") {
        (v, 1_000)
    } else if let Some(v) = s.strip_suffix('B') {
        (v, 1)
    } else {
        (s, 1)
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .map(|n| n * mult)
}

/// `"1d"`, `"12h"`, `"30m"`, `"90s"`, or bare seconds.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    let (num, mult) = match s.chars().last()? {
        'd' => (&s[..s.len() - 1], 86_400),
        'h' => (&s[..s.len() - 1], 3_600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .map(|n| Duration::from_secs(n * mult))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(log: &Arc<RotatingLog>, s: &str) {
        LogSink(Arc::clone(log)).write_all(s.as_bytes()).unwrap();
    }

    #[test]
    fn rotates_when_the_size_trigger_fires() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, Some(64), None, None).unwrap();

        for _ in 0..10 {
            write(&log, "0123456789012345\n"); // 17 bytes
        }
        let rotated = log.rotated_files();
        assert!(!rotated.is_empty(), "size trigger should have rotated");
        // The live file exists and is under the limit.
        assert!(p.exists());
        assert!(std::fs::metadata(&p).unwrap().len() <= 64);
    }

    #[test]
    fn rotates_when_the_time_trigger_fires() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, None, Some(Duration::from_millis(50)), None).unwrap();

        write(&log, "before\n");
        std::thread::sleep(Duration::from_millis(80));
        write(&log, "after\n");

        assert_eq!(
            log.rotated_files().len(),
            1,
            "time trigger should rotate once"
        );
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "after\n");
    }

    #[test]
    fn nothing_is_lost_across_a_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, Some(40), None, None).unwrap();

        for i in 0..20 {
            write(&log, &format!("line-{i:03}\n"));
        }
        // Every line must be somewhere: live file plus rotated siblings.
        let mut all = std::fs::read_to_string(&p).unwrap();
        for r in log.rotated_files() {
            all.push_str(&std::fs::read_to_string(r).unwrap());
        }
        for i in 0..20 {
            assert!(all.contains(&format!("line-{i:03}")), "lost line {i}");
        }
    }

    #[test]
    fn retention_keeps_the_newest_and_prunes_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, Some(20), None, Some(2)).unwrap();

        for i in 0..15 {
            write(&log, &format!("aaaaaaaaaaaaaaa-{i}\n"));
        }
        assert!(
            log.rotated_files().len() <= 2,
            "keep=2 must bound the rotated files, got {}",
            log.rotated_files().len()
        );
    }

    #[test]
    fn keep_none_retains_everything() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, Some(20), None, None).unwrap();
        for i in 0..10 {
            write(&log, &format!("bbbbbbbbbbbbbbb-{i}\n"));
        }
        assert!(
            log.rotated_files().len() >= 5,
            "keep=None must not delete anything"
        );
    }

    #[test]
    fn a_restart_appends_rather_than_truncating() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        {
            let log = RotatingLog::open(&p, None, None, None).unwrap();
            write(&log, "before restart\n");
        }
        {
            let log = RotatingLog::open(&p, None, None, None).unwrap();
            write(&log, "after restart\n");
        }
        let body = std::fs::read_to_string(&p).unwrap();
        assert!(
            body.contains("before restart") && body.contains("after restart"),
            "a restart must not discard the log that explains it: {body:?}"
        );
    }

    /// A single line longer than the limit must not rotate on every write —
    /// that produces an unbounded number of near-empty files.
    #[test]
    fn an_oversized_line_does_not_rotate_forever() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("agent.log");
        let log = RotatingLog::open(&p, Some(10), None, None).unwrap();
        for _ in 0..5 {
            write(&log, "this line is far longer than ten bytes\n");
        }
        assert!(
            log.rotated_files().len() <= 5,
            "one rotation per oversized line at most, got {}",
            log.rotated_files().len()
        );
    }

    #[test]
    fn sizes_parse_and_the_two_conventions_differ() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1KiB"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1000));
        assert_eq!(parse_size("100MiB"), Some(100 * 1024 * 1024));
        assert_eq!(parse_size("2GiB"), Some(2 * 1024 * 1024 * 1024));
        assert_ne!(parse_size("1KiB"), parse_size("1KB"), "KiB is not KB");
        assert_eq!(parse_size("0"), None);
        assert_eq!(parse_size("banana"), None);
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86_400)));
        assert_eq!(parse_duration("12h"), Some(Duration::from_secs(43_200)));
        assert_eq!(parse_duration("30m"), Some(Duration::from_secs(1_800)));
        assert_eq!(parse_duration("90s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("90"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("0h"), None);
        assert_eq!(parse_duration("soon"), None);
    }

    #[test]
    fn rotated_names_sort_chronologically() {
        assert!(
            stamp_utc(UNIX_EPOCH + Duration::from_secs(1_735_689_600))
                < stamp_utc(UNIX_EPOCH + Duration::from_secs(1_767_225_600))
        );
        assert_eq!(
            stamp_utc(UNIX_EPOCH + Duration::from_secs(1_735_689_600)),
            "20250101-000000"
        );
    }
}
