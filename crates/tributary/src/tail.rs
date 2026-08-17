//! Following a log file through rotation.
//!
//! Files are tracked by `(device, inode)`, never by path, because every
//! rotation style changes the relationship between the two:
//!
//! - **rename-and-recreate** — `app.log` becomes `app.log.1` and a new
//!   inode appears at `app.log`. The old inode still holds unread bytes.
//! - **create-new-with-suffix** — the writer moves to `app.log.2026-08-09`
//!   and `app.log` stops growing. Same problem, different names.
//! - **copy-and-truncate** — the inode is unchanged but its length drops
//!   below our offset. Seeking to the old offset would read garbage or
//!   nothing.
//!
//! The rule that matters in all three: **drain the old file before
//! following the new one.** The bytes written between the last read and
//! the rename are only in the old inode, and they are the ones nobody
//! notices are missing.

use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::checkpoint::{Checkpoint, FileMark};

pub struct Open {
    pub dev: u64,
    pub ino: u64,
    pub path: PathBuf,
    reader: BufReader<std::fs::File>,
    pub offset: u64,
}

impl Open {
    /// Current size of the file THIS handle refers to. Statted through the
    /// open descriptor rather than the path, so it stays correct for a file
    /// that has been rotated out from under its name and is still draining.
    fn size(&self) -> u64 {
        self.reader
            .get_ref()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(self.offset)
    }

    fn at(path: &Path, offset: u64) -> std::io::Result<Open> {
        let file = std::fs::File::open(path)?;
        let md = file.metadata()?;
        let mut reader = BufReader::new(file);
        let offset = offset.min(md.len());
        reader.seek(SeekFrom::Start(offset))?;
        Ok(Open {
            dev: md.dev(),
            ino: md.ino(),
            path: path.to_path_buf(),
            reader,
            offset,
        })
    }

    /// Read one line. `Ok(None)` means EOF *for now* — not end of file
    /// forever, since a live file grows.
    fn next_line(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let mut buf = Vec::new();
        let n = self.reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        // A partial final line means the writer is mid-write. Rewind and
        // wait: shipping half a line would quarantine it, and the other
        // half would arrive as a second broken line.
        if !buf.ends_with(b"\n") {
            self.reader.seek(SeekFrom::Start(self.offset))?;
            return Ok(None);
        }
        self.offset += n as u64;
        if buf.ends_with(b"\n") {
            buf.pop();
        }
        if buf.ends_with(b"\r") {
            buf.pop();
        }
        Ok(Some(buf))
    }

    fn len(&self) -> u64 {
        self.reader
            .get_ref()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(self.offset)
    }
}

pub struct Tailer {
    path: PathBuf,
    /// Rotated-away files with bytes still unread. Drained before the
    /// live file, oldest first.
    draining: Vec<Open>,
    current: Option<Open>,
    pub files_lost: u64,
    pub rotations: u64,
}

impl Tailer {
    /// Open a stream, resuming from `cp` where the recorded files still
    /// exist. A checkpointed inode that is gone entirely is counted, not
    /// silently skipped.
    pub fn resume(path: &Path, cp: Option<&Checkpoint>) -> anyhow::Result<Tailer> {
        let mut t = Tailer {
            path: path.to_path_buf(),
            draining: Vec::new(),
            current: None,
            files_lost: 0,
            rotations: 0,
        };

        let marks: &[FileMark] = cp.map(|c| c.files.as_slice()).unwrap_or(&[]);
        // A rotation while we were stopped leaves the old inode alive
        // under a sibling name, so look for it there before giving up.
        let siblings = sibling_paths(path);
        for mark in marks {
            match find_inode(&siblings, mark.dev, mark.ino) {
                Some(p) => {
                    let open = Open::at(&p, mark.offset)?;
                    if open.offset < open.len() || p != *path {
                        t.draining.push(open);
                    }
                }
                None => {
                    t.files_lost += 1;
                    tracing::warn!(
                        dev = mark.dev,
                        ino = mark.ino,
                        offset = mark.offset,
                        "checkpointed file is gone; its unread bytes are lost"
                    );
                }
            }
        }

        // The live file: continue it if it is one we already know.
        if path.exists() {
            let md = std::fs::metadata(path)?;
            let known = t
                .draining
                .iter()
                .position(|o| o.dev == md.dev() && o.ino == md.ino());
            t.current = match known {
                Some(i) => Some(t.draining.remove(i)),
                None => {
                    let start = marks
                        .iter()
                        .find(|m| m.dev == md.dev() && m.ino == md.ino())
                        .map(|m| m.offset)
                        .unwrap_or(0);
                    Some(Open::at(path, start)?)
                }
            };
        }
        Ok(t)
    }

    /// The next line from this stream, draining rotated files first so
    /// nothing is left behind. `Ok(None)` means "nothing available right
    /// now"; call again after a poll interval.
    pub fn next_line(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        // 1. finish anything already known to be rotated away
        if let Some(line) = self.drain_pending()? {
            return Ok(Some(line));
        }

        // 2. detect rotation, which may hand us a newly-rotated file
        self.check_rotation()?;

        // 3. drain AGAIN. The rotation just discovered above moved the
        //    old file here, and its unread tail was written before
        //    anything in the new file — returning the new file's first
        //    line now would emit them out of order, and the sequence
        //    numbers the stamper assigns follow emission order.
        if let Some(line) = self.drain_pending()? {
            return Ok(Some(line));
        }

        // 4. the live file
        if let Some(cur) = self.current.as_mut()
            && let Some(line) = cur.next_line()?
        {
            return Ok(Some(line));
        }
        Ok(None)
    }

    fn drain_pending(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        while let Some(front) = self.draining.first_mut() {
            if let Some(line) = front.next_line()? {
                return Ok(Some(line));
            }
            self.draining.remove(0);
        }
        Ok(None)
    }

    fn check_rotation(&mut self) -> anyhow::Result<()> {
        let Some(cur) = self.current.as_mut() else {
            // No live file yet — try to pick one up.
            self.current = Open::at(&self.path, 0).ok();
            return Ok(());
        };

        // copy-and-truncate: same inode, shorter than where we are.
        if cur.len() < cur.offset {
            tracing::info!(
                path = %cur.path.display(),
                len = cur.len(),
                offset = cur.offset,
                ino = cur.ino,
                "file truncated; restarting at 0"
            );
            self.rotations += 1;
            self.current = Open::at(&self.path, 0).ok();
            return Ok(());
        }

        let Ok(md) = std::fs::metadata(&self.path) else {
            return Ok(()); // renamed away and not yet recreated
        };
        if md.dev() == cur.dev && md.ino() == cur.ino {
            return Ok(()); // still the same file
        }

        // A new inode sits at the path. Keep draining the old one — its
        // last bytes exist nowhere else — and follow the new file after.
        tracing::info!(path = %self.path.display(), "rotation: following the new file");
        self.rotations += 1;
        let old = self.current.take().expect("checked above");
        if old.offset < old.len() {
            self.draining.push(old);
        }
        // Every rotation has a window where the path does not exist —
        // between the rename and the recreate. Failing here would kill
        // the agent partway through a routine logrotate run, which is
        // exactly when it must not die; `None` simply means "look again
        // next poll", and the drained file is already safe above.
        self.current = Open::at(&self.path, 0).ok();
        Ok(())
    }

    /// Everything a checkpoint needs to resume exactly here.
    pub fn marks(&self) -> Vec<FileMark> {
        let mut v: Vec<FileMark> = self
            .draining
            .iter()
            .map(|o| FileMark {
                dev: o.dev,
                ino: o.ino,
                offset: o.offset,
            })
            .collect();
        if let Some(c) = &self.current {
            v.push(FileMark {
                dev: c.dev,
                ino: c.ino,
                offset: c.offset,
            });
        }
        v
    }

    /// Bytes written to the sources but not yet read, across the current
    /// file and anything still draining after a rotation.
    ///
    /// Part of the P1-7 exposure picture: on a node that comes back, these
    /// are simply read later and nothing is lost. On a node that VANISHES,
    /// the log files go with it, so unread bytes are lost as surely as
    /// queued ones — which is the term people forget when they reason about
    /// the queue alone.
    pub fn unread_bytes(&self) -> u64 {
        self.draining
            .iter()
            .chain(self.current.iter())
            .map(|o| o.size().saturating_sub(o.offset))
            .sum()
    }
}

/// Files that a rotation could plausibly have moved our inode to: the
/// path itself and everything beside it sharing its name as a prefix.
fn sibling_paths(path: &Path) -> Vec<PathBuf> {
    let mut out = vec![path.to_path_buf()];
    let (Some(dir), Some(stem)) = (path.parent(), path.file_name()) else {
        return out;
    };
    let stem = stem.to_string_lossy().into_owned();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p != *path
                && p.file_name()
                    .map(|n| n.to_string_lossy().starts_with(&stem))
                    .unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
    out
}

fn find_inode(candidates: &[PathBuf], dev: u64, ino: u64) -> Option<PathBuf> {
    candidates.iter().find_map(|p| {
        let md = std::fs::metadata(p).ok()?;
        (md.dev() == dev && md.ino() == ino).then(|| p.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(path: &Path, s: &str) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        f.write_all(s.as_bytes()).unwrap();
    }

    fn drain(t: &mut Tailer) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(l) = t.next_line().unwrap() {
            out.push(String::from_utf8(l).unwrap());
        }
        out
    }

    #[test]
    fn follows_a_growing_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "a\nb\n");
        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(drain(&mut t), vec!["a", "b"]);
        write(&p, "c\n");
        assert_eq!(drain(&mut t), vec!["c"]);
    }

    #[test]
    fn a_partial_line_waits_for_its_newline() {
        // Shipping half a line quarantines it and then the other half
        // arrives as a second broken line — two losses from one race.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "complete\npar");
        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(drain(&mut t), vec!["complete"]);
        write(&p, "tial\n");
        assert_eq!(drain(&mut t), vec!["partial"]);
    }

    #[test]
    fn rename_and_recreate_drains_the_old_file_first() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "one\n");
        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(drain(&mut t), vec!["one"]);

        // bytes written just before the rename exist ONLY in the old inode
        write(&p, "last-before-rotate\n");
        std::fs::rename(&p, dir.path().join("app.log.1")).unwrap();
        write(&p, "first-after-rotate\n");

        assert_eq!(
            drain(&mut t),
            vec!["last-before-rotate", "first-after-rotate"],
            "the pre-rotation tail must not be skipped"
        );
        assert_eq!(t.rotations, 1);
    }

    #[test]
    fn copy_and_truncate_restarts_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "aaaa\nbbbb\n");
        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(drain(&mut t), vec!["aaaa", "bbbb"]);

        std::fs::copy(&p, dir.path().join("app.log.1")).unwrap();
        std::fs::write(&p, "").unwrap(); // truncate in place
        write(&p, "fresh\n");

        assert_eq!(drain(&mut t), vec!["fresh"]);
        assert_eq!(t.rotations, 1);
    }

    #[test]
    fn resume_continues_mid_file_without_replaying() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "1\n2\n3\n");

        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(t.next_line().unwrap().unwrap(), b"1");
        assert_eq!(t.next_line().unwrap().unwrap(), b"2");
        let cp = Checkpoint {
            files: t.marks(),
            ..Default::default()
        };

        // restart where we left off
        let mut t2 = Tailer::resume(&p, Some(&cp)).unwrap();
        assert_eq!(drain(&mut t2), vec!["3"], "must not replay 1 and 2");
    }

    #[test]
    fn resume_finds_a_file_that_rotated_while_we_were_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "before\n");
        let mut t = Tailer::resume(&p, None).unwrap();
        assert_eq!(drain(&mut t), vec!["before"]);
        let cp = Checkpoint {
            files: t.marks(),
            ..Default::default()
        };

        // rotation happens while the agent is down, and the old inode
        // keeps its unread tail under the sibling name
        write(&p, "written-while-down\n");
        std::fs::rename(&p, dir.path().join("app.log.1")).unwrap();
        write(&p, "after\n");

        let mut t2 = Tailer::resume(&p, Some(&cp)).unwrap();
        let got = drain(&mut t2);
        assert!(
            got.contains(&"written-while-down".to_string()),
            "got {got:?}"
        );
        assert!(got.contains(&"after".to_string()), "got {got:?}");
        assert_eq!(t2.files_lost, 0);
    }

    #[test]
    fn a_vanished_file_is_counted_not_silently_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        write(&p, "x\n");
        let t = Tailer::resume(&p, None).unwrap();
        let cp = Checkpoint {
            files: t.marks(),
            ..Default::default()
        };

        std::fs::remove_file(&p).unwrap();
        write(&p, "brand new\n"); // different inode

        let t2 = Tailer::resume(&p, Some(&cp)).unwrap();
        assert_eq!(t2.files_lost, 1, "losing bytes must be visible");
    }
}
