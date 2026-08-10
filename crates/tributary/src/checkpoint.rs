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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
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
}

impl Checkpoint {
    pub fn path_for(dir: &Path, stream: &str) -> PathBuf {
        dir.join(format!("{stream}.checkpoint"))
    }

    pub fn load(path: &Path) -> anyhow::Result<Option<Checkpoint>> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
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
        };
        cp.save(&p).unwrap();
        assert_eq!(Checkpoint::load(&p).unwrap().unwrap(), cp);
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
