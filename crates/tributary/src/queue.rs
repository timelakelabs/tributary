//! The disk queue — what lets a database outage be an inconvenience
//! rather than data loss.
//!
//! Tailing is not like receiving a network stream: there is nobody to
//! push back on, and the file keeps growing whether or not TimeLakeDB is
//! answering. So the queue buys time, and its exhaustion policy is a
//! decision rather than an accident: **spool to disk up to `max_bytes`,
//! then stop reading and alarm.** Never drop — silent loss is the one
//! thing this agent exists to prevent.
//!
//! Ordering with the checkpoint is what makes it safe. A batch is
//! durable on disk *before* the checkpoint advances past its bytes, so a
//! crash replays from the queue, not from a gap.

use std::io::Write as _;
use std::path::{Path, PathBuf};

/// The segment format's version. A segment is one batch of line protocol
/// exactly as it would have gone over the wire, in a file named by a
/// twelve-digit sequence with a `.lp` extension, adopted in name order by
/// whichever agent opens the directory next. That is the whole format, and
/// it has not changed since the queue existed. The number exists so a
/// change that WOULD change it — a header, a compression, a different
/// framing — has something to bump, which is what makes the compat gate
/// (`.github/persisted-formats.txt`) able to ask for a downgrade path.
pub const QUEUE_FORMAT_VERSION: u32 = 1;

/// Where each pipeline's spool lives under the state dir. These names are
/// a persisted format in their own right: an agent that looks for its
/// queue under a new name strands the old one, which is precisely the loss
/// the queue exists to prevent. They live here rather than at the call
/// sites so one watched file owns the on-disk layout (timelakedb#173).
impl Queue {
    /// `queue-<source name>`, one per source since #49.
    pub fn dir_for(state_dir: &Path, name: &str) -> PathBuf {
        state_dir.join(format!("queue-{name}"))
    }

    /// The single-source layout before #49: `queue`, no name. Migrated to
    /// [`Queue::dir_for`] on upgrade so spooled lines are not stranded.
    pub fn legacy_dir(state_dir: &Path) -> PathBuf {
        state_dir.join("queue")
    }

    /// The OTLP receiver's own spool (#12).
    pub fn otlp_dir(state_dir: &Path) -> PathBuf {
        state_dir.join("otlp-queue")
    }

    /// The host-metrics collector's own spool (#25).
    pub fn metrics_dir(state_dir: &Path) -> PathBuf {
        state_dir.join("metrics-queue")
    }
}

pub struct Queue {
    dir: PathBuf,
    max_bytes: u64,
    bytes: u64,
    next_seq: u64,
    segments: std::collections::VecDeque<PathBuf>,
    /// Set when the queue hit its cap. Reading must stop while this is
    /// true, and it is a named, visible limit (RR-5's posture).
    pub full: bool,
    pub spilled_total: u64,
    pub drained_total: u64,
}

impl Queue {
    /// Open a queue, adopting anything a previous run left behind —
    /// those segments are shipped before any new work.
    pub fn open(dir: &Path, max_bytes: u64) -> anyhow::Result<Queue> {
        std::fs::create_dir_all(dir)?;
        let mut segments: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "lp"))
            .collect();
        segments.sort();

        let bytes = segments
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        let next_seq = segments
            .last()
            .and_then(|p| p.file_stem()?.to_str()?.parse::<u64>().ok())
            .map(|n| n + 1)
            .unwrap_or(0);

        if !segments.is_empty() {
            tracing::info!(
                segments = segments.len(),
                bytes,
                format = QUEUE_FORMAT_VERSION,
                "adopted queued batches from a previous run"
            );
        }
        Ok(Queue {
            dir: dir.to_path_buf(),
            max_bytes,
            bytes,
            next_seq,
            segments: segments.into(),
            full: false,
            spilled_total: 0,
            drained_total: 0,
        })
    }

    /// Spool a batch not yet read off the source. `Ok(false)` means the queue
    /// is at its cap and the caller must stop reading — the batch is NOT
    /// accepted, and nothing is lost by refusing it because the source file
    /// still holds those bytes. This is the backpressure path; for an
    /// already-read batch that failed to ship, see [`Self::push_forced`].
    pub fn push(&mut self, body: &str) -> anyhow::Result<bool> {
        let len = body.len() as u64;
        if self.bytes + len > self.max_bytes {
            if !self.full {
                tracing::error!(
                    bytes = self.bytes,
                    max_bytes = self.max_bytes,
                    "QUEUE FULL — reading paused. The source keeps growing; if it \
                     rotates away before this drains, those bytes are gone."
                );
            }
            self.full = true;
            return Ok(false);
        }
        self.spool(body)
    }

    /// Spool a batch that MUST be kept even though the queue is over its soft
    /// cap: an in-flight batch that failed to ship *after* its lines were
    /// already read off the source. Refusing it here would be data loss, not
    /// backpressure — the read loop is already paused while `full`, so the
    /// overshoot is bounded to the in-flight set (`max_inflight` batches). The
    /// cap bounds READING; it must never license losing bytes already taken.
    pub fn push_forced(&mut self, body: &str) -> anyhow::Result<()> {
        self.spool(body)?;
        // Stay full: reads remain paused until this genuinely drains below the
        // resume mark (see `pop`'s hysteresis).
        self.full = true;
        Ok(())
    }

    fn spool(&mut self, body: &str) -> anyhow::Result<bool> {
        let len = body.len() as u64;
        let path = self.dir.join(format!("{:012}.lp", self.next_seq));
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            // Durable before the checkpoint may advance past these bytes.
            f.sync_data()?;
        }
        std::fs::rename(&tmp, &path)?;

        self.next_seq += 1;
        self.bytes += len;
        self.segments.push_back(path);
        self.spilled_total += 1;
        Ok(true)
    }

    /// The oldest queued batch, if any. FIFO: order is not required for
    /// correctness (every line already carries its own timestamp) but it
    /// keeps recovery legible.
    pub fn front(&self) -> Option<PathBuf> {
        self.segments.front().cloned()
    }

    pub fn read(path: &Path) -> anyhow::Result<String> {
        Ok(std::fs::read_to_string(path)?)
    }

    /// Retire a segment that shipped successfully.
    pub fn pop(&mut self, path: &Path) -> anyhow::Result<()> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        std::fs::remove_file(path)?;
        self.segments.pop_front();
        self.bytes = self.bytes.saturating_sub(len);
        self.drained_total += 1;
        // `* 2 <=` rather than `/ 2 <`: at exactly half the queue has
        // drained enough, and integer division would not say so.
        if self.full && self.bytes * 2 <= self.max_bytes {
            // Hysteresis: resume at half full, so a queue hovering at the
            // cap does not flap between paused and reading every batch.
            self.full = false;
            tracing::info!(
                bytes = self.bytes,
                "queue drained below the resume mark; reading again"
            );
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
    pub fn len(&self) -> usize {
        self.segments.len()
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The directory names ARE the layout an upgrade has to find. Pinned
    /// as strings, so a refactor that renames one shows up here and in the
    /// compat gate, not as a stranded spool on somebody's node.
    #[test]
    fn the_spool_directory_names_are_the_ones_shipped_agents_wrote() {
        let root = Path::new("/var/lib/tributary");
        assert_eq!(Queue::dir_for(root, "k8s"), root.join("queue-k8s"));
        assert_eq!(Queue::legacy_dir(root), root.join("queue"));
        assert_eq!(Queue::otlp_dir(root), root.join("otlp-queue"));
        assert_eq!(Queue::metrics_dir(root), root.join("metrics-queue"));
        assert_eq!(QUEUE_FORMAT_VERSION, 1);
    }

    #[test]
    fn fifo_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = Queue::open(dir.path(), 1 << 20).unwrap();
        assert!(q.is_empty());

        q.push("first\n").unwrap();
        q.push("second\n").unwrap();
        assert_eq!(q.len(), 2);

        let a = q.front().unwrap();
        assert_eq!(Queue::read(&a).unwrap(), "first\n");
        q.pop(&a).unwrap();
        let b = q.front().unwrap();
        assert_eq!(Queue::read(&b).unwrap(), "second\n");
        q.pop(&b).unwrap();
        assert!(q.is_empty());
        assert_eq!(q.bytes(), 0);
    }

    #[test]
    fn a_previous_runs_queue_is_adopted_not_orphaned() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut q = Queue::open(dir.path(), 1 << 20).unwrap();
            q.push("survives the restart\n").unwrap();
        }
        let q = Queue::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(
            Queue::read(&q.front().unwrap()).unwrap(),
            "survives the restart\n"
        );
    }

    #[test]
    fn refuses_rather_than_dropping_when_full() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = Queue::open(dir.path(), 20).unwrap();
        assert!(q.push("0123456789").unwrap());
        assert!(q.push("0123456789").unwrap());
        // The third would exceed the cap: refused, and NOT written.
        assert!(!q.push("0123456789").unwrap());
        assert!(q.full);
        assert_eq!(q.len(), 2, "a refused batch must not be spooled");
    }

    #[test]
    fn a_failed_in_flight_batch_is_force_spooled_not_dropped_when_full() {
        // Regression for #61 (backpressure chaos): the read-loop backpressure
        // refuses a NEW read at the cap (that batch is still in the file), but
        // an already-read batch that failed to ship must NOT be dropped — it is
        // forced past the cap, because refusing it is loss, not backpressure.
        let dir = tempfile::tempdir().unwrap();
        let mut q = Queue::open(dir.path(), 20).unwrap();
        assert!(q.push("0123456789").unwrap());
        assert!(q.push("0123456789").unwrap());
        assert!(!q.push("x").unwrap(), "a new read is refused at the cap");

        q.push_forced("already-read-bytes").unwrap();
        assert_eq!(q.len(), 3, "the failed in-flight batch is kept, not lost");
        assert!(q.full, "still full: reads stay paused until it drains");
    }

    #[test]
    fn resumes_with_hysteresis_not_on_the_first_byte_freed() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = Queue::open(dir.path(), 20).unwrap();
        q.push("0123456789").unwrap();
        q.push("0123456789").unwrap();
        assert!(!q.push("x").unwrap());
        assert!(q.full);

        // one segment freed leaves 10 of 20 bytes — at the resume mark
        let f = q.front().unwrap();
        q.pop(&f).unwrap();
        assert!(!q.full, "should resume once below half");
    }

    #[test]
    fn leaves_no_temp_files_behind() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = Queue::open(dir.path(), 1 << 20).unwrap();
        q.push("a\n").unwrap();
        q.push("b\n").unwrap();
        let stray: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains("tmp"))
            .collect();
        assert!(stray.is_empty(), "left {stray:?}");
    }
}
