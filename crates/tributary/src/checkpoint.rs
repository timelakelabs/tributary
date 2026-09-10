//! Durable progress, written the way the database writes its own
//! objects: temp file, fsync, rename — durable or absent, never torn.
//!
//! The offset alone is not enough. A checkpoint that lands *mid-tick*
//! and records only the byte position will, on resume, restart the
//! sequence at zero and re-issue timestamps the lines before it already
//! used — overwriting them (DESIGN.md §3.2). So `last_tick_ns` and
//! `next_seq` travel with the offset, and the stamper is restored from
//! them before a single line is read.

use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMark {
    /// Identity, not path: a rotation renames the file but the inode is
    /// what we were actually reading.
    pub dev: u64,
    pub ino: u64,
    pub offset: u64,
}

/// The checkpoint's format version. Every field since the first was added
/// with `#[serde(default)]`, which reads correctly forward and is silently
/// wrong backward — TimeLakeDB's manifest log was built the same way and
/// resurrected a dropped table on rollback (timelakedb#160). This number is
/// what lets an OLDER agent refuse a checkpoint it would mis-read instead
/// of guessing at it: bump it when a field carries an instruction an old
/// reader must not ignore (a position, a retirement), and the compat gate
/// (`.github/persisted-formats.txt`) will ask for a downgrade path.
///
/// 1 is the shape as of 0.5. A checkpoint with no `format` field predates
/// the number and reads as 1: the rule that a version is only bumped for a
/// change an older reader would MIS-apply means everything before it was,
/// by that rule, still 1.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

fn format_v1() -> u32 {
    CHECKPOINT_FORMAT_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    /// See [`CHECKPOINT_FORMAT_VERSION`]. Serialised first so a human
    /// reading the file sees it first.
    #[serde(default = "format_v1")]
    pub format: u32,
    /// Files still being drained, newest last. A rotation can leave the
    /// tail of the old file unread; forgetting it loses those bytes.
    pub files: Vec<FileMark>,
    /// Stamper state — the half that makes a replay reproduce identical
    /// timestamps instead of colliding with itself.
    pub last_tick_ns: Option<i64>,
    pub next_seq: i64,
    /// The watermark's converged lateness estimate, so a restart resumes
    /// it instead of falling back to the conservative ceiling.
    #[serde(default)]
    pub lateness_ns: Option<i64>,
    /// journald's opaque resume cursor (#23). The file tail leaves this
    /// None; a journald source sets it. It is NOT a byte offset.
    #[serde(default)]
    pub cursor: Option<String>,
}

impl Default for Checkpoint {
    /// By hand, not derived: a derived default would write `format: 0`,
    /// which is not a version anything has ever had.
    fn default() -> Self {
        Checkpoint {
            format: CHECKPOINT_FORMAT_VERSION,
            files: Vec::new(),
            last_tick_ns: None,
            next_seq: 0,
            lateness_ns: None,
            cursor: None,
        }
    }
}

impl Checkpoint {
    pub fn path_for(dir: &Path, stream: &str) -> PathBuf {
        dir.join(format!("{stream}.checkpoint"))
    }

    /// Load, and REFUSE a checkpoint from a newer agent rather than guess
    /// at it. The alternative is what timelakedb#160 did for three
    /// releases: read what you understand, drop what you do not, and
    /// resume from a position the newer format meant differently. For a
    /// checkpoint that is a replay or a gap, and both are silent. An
    /// operator rolling back gets one line saying what to do instead.
    pub fn load(path: &Path) -> anyhow::Result<Option<Checkpoint>> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let cp: Checkpoint = serde_json::from_slice(&bytes)?;
        if cp.format > CHECKPOINT_FORMAT_VERSION {
            anyhow::bail!(
                "{} was written by a newer tributary (checkpoint format {} > {}); \
                 refusing to guess at it. Run that version, or delete the file to \
                 re-read every source from the start (lines are then duplicated, \
                 not lost).",
                path.display(),
                cp.format,
                CHECKPOINT_FORMAT_VERSION
            );
        }
        Ok(Some(cp))
    }

    /// Atomic publish. A half-written checkpoint that survived a crash
    /// would be worse than none: it would claim progress that never
    /// happened and skip the lines in between.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("checkpoint.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&serde_json::to_vec(self)?)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    #[allow(dead_code)] // the tailer matches inodes itself; kept for tests and L2
    pub fn mark_for(&self, dev: u64, ino: u64) -> Option<&FileMark> {
        self.files.iter().find(|m| m.dev == dev && m.ino == ino)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_including_the_stamper_state() {
        let dir = tempfile::tempdir().unwrap();
        let p = Checkpoint::path_for(dir.path(), "app");

        assert_eq!(Checkpoint::load(&p).unwrap(), None);

        let cp = Checkpoint {
            format: CHECKPOINT_FORMAT_VERSION,
            files: vec![
                FileMark {
                    dev: 2049,
                    ino: 111,
                    offset: 4096,
                },
                FileMark {
                    dev: 2049,
                    ino: 222,
                    offset: 0,
                },
            ],
            last_tick_ns: Some(1_786_280_343_206_000_000),
            next_seq: 37,
            lateness_ns: Some(250_000_000),
            cursor: None,
        };
        cp.save(&p).unwrap();
        assert_eq!(Checkpoint::load(&p).unwrap().unwrap(), cp);
    }

    /// Every checkpoint written before 0.5 has no `format` field. It reads
    /// as 1, because 1 is defined as "the shape those files have".
    #[test]
    fn a_checkpoint_from_before_the_version_field_reads_as_format_1() {
        let dir = tempfile::tempdir().unwrap();
        let p = Checkpoint::path_for(dir.path(), "app");
        std::fs::write(
            &p,
            r#"{"files":[{"dev":1,"ino":2,"offset":3}],"last_tick_ns":null,"next_seq":0}"#,
        )
        .unwrap();
        let cp = Checkpoint::load(&p).unwrap().unwrap();
        assert_eq!(cp.format, 1);
        assert_eq!(cp.files[0].offset, 3);
        // And it is written back WITH the field, so the next reader sees it.
        cp.save(&p).unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.starts_with(r#"{"format":1,"#), "{text}");
    }

    /// The other direction is a refusal, not a guess: an operator rolling
    /// back must not resume from a position a newer format meant
    /// differently (timelakedb#160, in a checkpoint's clothes).
    #[test]
    fn a_checkpoint_from_a_newer_agent_is_refused_with_the_way_out() {
        let dir = tempfile::tempdir().unwrap();
        let p = Checkpoint::path_for(dir.path(), "app");
        std::fs::write(
            &p,
            format!(
                r#"{{"format":{},"files":[],"last_tick_ns":null,"next_seq":0}}"#,
                CHECKPOINT_FORMAT_VERSION + 1
            ),
        )
        .unwrap();
        let err = Checkpoint::load(&p).unwrap_err().to_string();
        assert!(err.contains("newer tributary"), "{err}");
        assert!(err.contains("delete the file"), "{err}");
        assert!(err.contains("duplicated, not lost"), "{err}");
    }

    #[test]
    fn publishing_leaves_no_temp_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        let p = Checkpoint::path_for(dir.path(), "app");
        Checkpoint::default().save(&p).unwrap();
        Checkpoint::default().save(&p).unwrap();

        let stray: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains("tmp"))
            .collect();
        assert!(stray.is_empty(), "left {stray:?}");
    }

    #[test]
    fn finds_a_files_mark_by_identity_not_position() {
        let cp = Checkpoint {
            files: vec![
                FileMark {
                    dev: 1,
                    ino: 10,
                    offset: 5,
                },
                FileMark {
                    dev: 1,
                    ino: 20,
                    offset: 15,
                },
            ],
            ..Default::default()
        };
        assert_eq!(cp.mark_for(1, 20).unwrap().offset, 15);
        assert!(cp.mark_for(1, 99).is_none());
    }
}
