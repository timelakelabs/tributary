//! journald source (#23): read systemd's journal through the sd-journal
//! cursor, not by tailing the binary files.
//!
//! The journal is not a byte stream, so the file tail's byte offset is the
//! wrong resume token. The **cursor** is the right one — an opaque string the
//! journal hands back for the current entry — and it IS the checkpoint. On
//! restart we seek to the saved cursor and read AFTER it: no gap, no dupe.
//!
//! Each entry becomes a JSON object (`MESSAGE`, `_SYSTEMD_UNIT`, `PRIORITY`, …)
//! and goes through the ordinary [`crate::map::map_line`] (Parser::Journald ==
//! JSON), so a field becomes a tag only if the source's allowlist NAMES it —
//! the same FR-2 guard everything else has. The timestamp comes from the
//! entry's `__REALTIME_TIMESTAMP` (microseconds), not from a JSON field.
//!
//! Everything that links libsystemd is behind `#[cfg(feature = "journald")]`,
//! so the default build (and CI on a plain runner) grows no dependency; a
//! config naming journald without the feature is refused at load.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::config::Source;

/// One journal entry: its fields, plus the realtime timestamp the journal
/// stamped it with (microseconds since the epoch, an "address" field the data
/// enumeration does not return).
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub fields: BTreeMap<String, String>,
    pub realtime_usec: u64,
}

/// The seam to the journal, so the cursor/resume logic is testable with a fake
/// and the sd-journal implementation stays behind the feature gate.
pub trait JournalReader {
    /// Position for reading. `Some(cursor)` resumes AFTER that cursor's entry
    /// (the next `next()` returns the following entry); `None` starts at head.
    fn seek(&mut self, cursor: Option<&str>) -> anyhow::Result<()>;
    /// Advance to and return the next entry, or `None` at the tail.
    fn next(&mut self) -> anyhow::Result<Option<Entry>>;
    /// The cursor of the current entry — persist this, it is the resume token.
    fn cursor(&mut self) -> anyhow::Result<String>;
    /// Block until entries are appended past the current position, or the
    /// timeout elapses. Lets the follow loop sleep instead of spin.
    fn wait(&mut self, timeout: Duration) -> anyhow::Result<()>;
}

/// Render an entry as a JSON line for `map_line`. `MESSAGE` and the journal's
/// metadata fields become object keys; the allowlist and declared fields on
/// the source decide which survive.
pub fn entry_to_json(entry: &Entry) -> String {
    let map: serde_json::Map<String, serde_json::Value> = entry
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::Value::Object(map).to_string()
}

/// Map one entry to an encoded line-protocol record: JSON -> `map_line`
/// (allowlist + declared fields), with the journal's own realtime timestamp
/// overriding whatever the string-timestamp path would pick, then stamped.
///
/// Returns `Ok(None)` for an entry that maps to nothing (e.g. no declared
/// field present) so a metadata-only entry is skipped, not shipped empty.
pub fn map_entry(
    source: &Source,
    entry: &Entry,
    stamper: &mut crate::stamp::Stamper,
) -> anyhow::Result<Option<String>> {
    let line = entry_to_json(entry);
    let (mut record, _ignored_ts) = match crate::map::map_line(source, &line) {
        Ok(r) => r,
        Err(crate::map::MapError::Empty) => return Ok(None),
        Err(e) => return Err(anyhow::anyhow!(e.to_string())),
    };
    // A record with no declared field present is not shippable line protocol.
    if record.fields.is_empty() {
        return Ok(None);
    }
    // The journal's timestamp is authoritative — microseconds -> nanoseconds.
    let source_ts = (entry.realtime_usec as i64).saturating_mul(1_000);
    record.ts_ns = stamper
        .stamp(source_ts)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let mut out = String::new();
    record
        .encode(&mut out)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(Some(out))
}

/// Ship everything currently in the queue: front -> read -> send -> pop, until
/// empty or a send fails (the batch is left queued for the next attempt).
async fn drain(
    queue: &mut crate::queue::Queue,
    shipper: &crate::ship::Shipper,
) -> anyhow::Result<()> {
    while let Some(path) = queue.front() {
        let body = crate::queue::Queue::read(&path)?;
        let lines: Vec<String> = body.lines().map(String::from).collect();
        match shipper.send_lines(&lines).await {
            Ok(unshipped) if unshipped.is_empty() => queue.pop(&path)?,
            Ok(_) => break, // partial; leave it, retry next round
            Err(e) => {
                tracing::warn!(error = %e, "journald ship failed; leaving batch queued");
                break;
            }
        }
    }
    Ok(())
}

/// The journald agent loop: read entries via the cursor, queue durably, ship,
/// and checkpoint the cursor — the resume token. Reuses the same
/// Queue/Shipper/Stamper/Checkpoint the file tail uses; the file-tail loop is
/// untouched. The caller races this against its shutdown signal.
///
/// Durability order is the point: an entry is in the durable queue BEFORE the
/// cursor advances past it, so a crash re-drains the queue (LWW dedups) and
/// resumes reading after the last-queued entry — no gap.
#[allow(clippy::too_many_arguments)]
pub async fn run_journald<R: JournalReader>(
    mut reader: R,
    source: &Source,
    shipper: crate::ship::Shipper,
    state_dir: &std::path::Path,
    queue_max_bytes: u64,
    batch_lines: usize,
    once: bool,
) -> anyhow::Result<()> {
    let cp_path = crate::checkpoint::Checkpoint::path_for(state_dir, &source.name);
    let restored = crate::checkpoint::Checkpoint::load(&cp_path)?;
    let mut stamper = crate::stamp::Stamper::new(source.resolution());
    let mut cursor: Option<String> = None;
    if let Some(cp) = &restored {
        if let Some(t) = cp.last_tick_ns {
            stamper.restore(t, cp.next_seq);
        }
        cursor = cp.cursor.clone();
    }
    reader.seek(cursor.as_deref())?;

    let mut queue = crate::queue::Queue::open(&state_dir.join("journald-queue"), queue_max_bytes)?;
    // Anything a previous run left queued ships before new work.
    drain(&mut queue, &shipper).await?;

    loop {
        let mut batch: Vec<String> = Vec::new();
        let mut last_cursor: Option<String> = None;
        let mut at_tail = false;
        while batch.len() < batch_lines {
            match reader.next()? {
                Some(entry) => {
                    if let Some(lp) = map_entry(source, &entry, &mut stamper)? {
                        batch.push(lp);
                    }
                    last_cursor = Some(reader.cursor()?);
                }
                None => {
                    at_tail = true;
                    break;
                }
            }
        }

        if !batch.is_empty() {
            let body = batch.join("\n");
            queue.push(&body)?; // DURABLE before the cursor advances
            let (last_tick_ns, next_seq) = match stamper.checkpoint() {
                Some((t, s)) => (Some(t), s),
                None => (None, 0),
            };
            crate::checkpoint::Checkpoint {
                files: Vec::new(),
                last_tick_ns,
                next_seq,
                lateness_ns: None,
                cursor: last_cursor,
            }
            .save(&cp_path)?;
            drain(&mut queue, &shipper).await?;
        }

        if at_tail {
            if once {
                break;
            }
            // Block for appends on a blocking-allowed thread (the wait has its
            // own timeout, so the caller's shutdown select stays responsive).
            tokio::task::block_in_place(|| reader.wait(Duration::from_millis(500)))?;
        }
    }
    drain(&mut queue, &shipper).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The real journal, behind the feature so the default build links nothing.
// ---------------------------------------------------------------------------

#[cfg(feature = "journald")]
pub use real::RealJournal;

#[cfg(feature = "journald")]
mod real {
    use super::{Entry, JournalReader};
    use std::collections::BTreeMap;
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_int, c_void};
    use std::ptr;
    use std::time::Duration;

    #[repr(C)]
    pub struct sd_journal {
        _private: [u8; 0],
    }

    // The stable sd-journal C API. Declared here rather than via a -sys crate
    // so the only build requirement is libsystemd at link time, gated off by
    // default.
    #[link(name = "systemd")]
    unsafe extern "C" {
        fn sd_journal_open(ret: *mut *mut sd_journal, flags: c_int) -> c_int;
        fn sd_journal_close(j: *mut sd_journal);
        fn sd_journal_seek_head(j: *mut sd_journal) -> c_int;
        fn sd_journal_seek_cursor(j: *mut sd_journal, cursor: *const c_char) -> c_int;
        fn sd_journal_next(j: *mut sd_journal) -> c_int;
        fn sd_journal_get_cursor(j: *mut sd_journal, cursor: *mut *mut c_char) -> c_int;
        fn sd_journal_get_realtime_usec(j: *mut sd_journal, usec: *mut u64) -> c_int;
        fn sd_journal_restart_data(j: *mut sd_journal);
        fn sd_journal_enumerate_data(
            j: *mut sd_journal,
            data: *mut *const c_void,
            length: *mut usize,
        ) -> c_int;
        fn sd_journal_wait(j: *mut sd_journal, timeout_usec: u64) -> c_int;
    }

    /// Owns an open `sd_journal`.
    pub struct RealJournal {
        j: *mut sd_journal,
    }

    // sd_journal is used from a single task; we never share the pointer.
    unsafe impl Send for RealJournal {}

    fn ck(r: c_int, what: &str) -> anyhow::Result<()> {
        if r < 0 {
            Err(anyhow::anyhow!(
                "sd_journal {what} failed: {}",
                std::io::Error::from_raw_os_error(-r)
            ))
        } else {
            Ok(())
        }
    }

    impl RealJournal {
        /// Open the local system journal (flags 0 = default set of journals).
        pub fn open() -> anyhow::Result<RealJournal> {
            let mut j: *mut sd_journal = ptr::null_mut();
            unsafe { ck(sd_journal_open(&mut j, 0), "open")? };
            Ok(RealJournal { j })
        }

        /// Read every data field of the current entry.
        fn read_fields(&mut self) -> anyhow::Result<BTreeMap<String, String>> {
            let mut fields = BTreeMap::new();
            unsafe {
                sd_journal_restart_data(self.j);
                loop {
                    let mut data: *const c_void = ptr::null();
                    let mut len: usize = 0;
                    let r = sd_journal_enumerate_data(self.j, &mut data, &mut len);
                    if r == 0 {
                        break;
                    }
                    ck(r, "enumerate_data")?;
                    let bytes = std::slice::from_raw_parts(data as *const u8, len);
                    // Each item is `KEY=value`; value may be non-UTF8 (rare) —
                    // decode lossily, split on the first '='.
                    if let Some(eq) = bytes.iter().position(|&b| b == b'=') {
                        let key = String::from_utf8_lossy(&bytes[..eq]).into_owned();
                        let val = String::from_utf8_lossy(&bytes[eq + 1..]).into_owned();
                        fields.insert(key, val);
                    }
                }
            }
            Ok(fields)
        }
    }

    impl JournalReader for RealJournal {
        fn seek(&mut self, cursor: Option<&str>) -> anyhow::Result<()> {
            match cursor {
                None => unsafe { ck(sd_journal_seek_head(self.j), "seek_head") },
                Some(c) => {
                    let cs = CString::new(c)?;
                    unsafe {
                        ck(sd_journal_seek_cursor(self.j, cs.as_ptr()), "seek_cursor")?;
                        // Land ON the saved entry so the next `next()` returns
                        // the one after it — resume, not re-read.
                        sd_journal_next(self.j);
                    }
                    Ok(())
                }
            }
        }

        fn next(&mut self) -> anyhow::Result<Option<Entry>> {
            let r = unsafe { sd_journal_next(self.j) };
            ck(r, "next")?;
            if r == 0 {
                return Ok(None);
            }
            let fields = self.read_fields()?;
            let mut usec: u64 = 0;
            unsafe {
                ck(
                    sd_journal_get_realtime_usec(self.j, &mut usec),
                    "get_realtime_usec",
                )?
            };
            Ok(Some(Entry {
                fields,
                realtime_usec: usec,
            }))
        }

        fn cursor(&mut self) -> anyhow::Result<String> {
            let mut c: *mut c_char = ptr::null_mut();
            unsafe {
                ck(sd_journal_get_cursor(self.j, &mut c), "get_cursor")?;
                let s = CStr::from_ptr(c).to_string_lossy().into_owned();
                libc_free(c as *mut c_void);
                Ok(s)
            }
        }

        fn wait(&mut self, timeout: Duration) -> anyhow::Result<()> {
            let us = timeout.as_micros().min(u64::MAX as u128) as u64;
            let r = unsafe { sd_journal_wait(self.j, us) };
            ck(r, "wait")
        }
    }

    impl Drop for RealJournal {
        fn drop(&mut self) {
            unsafe { sd_journal_close(self.j) };
        }
    }

    // sd_journal_get_cursor allocates with malloc; free it with free().
    unsafe extern "C" {
        #[link_name = "free"]
        fn libc_free(p: *mut c_void);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldType, Parser, Timestamp};

    /// An in-memory journal for testing seek/next/cursor and resume without
    /// libsystemd. The cursor is just the entry index — opaque to the code
    /// under test, which is the whole point.
    struct FakeJournal {
        entries: Vec<Entry>,
        pos: Option<usize>, // index of the current entry, None before the first
    }

    impl FakeJournal {
        fn new(entries: Vec<Entry>) -> Self {
            FakeJournal { entries, pos: None }
        }
    }

    impl JournalReader for FakeJournal {
        fn seek(&mut self, cursor: Option<&str>) -> anyhow::Result<()> {
            self.pos = match cursor {
                None => None,
                Some(c) => Some(c.parse::<usize>()?), // land ON the saved entry
            };
            Ok(())
        }
        fn next(&mut self) -> anyhow::Result<Option<Entry>> {
            let i = match self.pos {
                None => 0,
                Some(p) => p + 1,
            };
            if i >= self.entries.len() {
                // Stay put so a later append could be read (follow semantics).
                return Ok(None);
            }
            self.pos = Some(i);
            Ok(Some(self.entries[i].clone()))
        }
        fn cursor(&mut self) -> anyhow::Result<String> {
            Ok(self.pos.expect("cursor before an entry").to_string())
        }
        fn wait(&mut self, _t: Duration) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn entry(msg: &str, unit: &str, usec: u64) -> Entry {
        let mut fields = BTreeMap::new();
        fields.insert("MESSAGE".into(), msg.into());
        fields.insert("_SYSTEMD_UNIT".into(), unit.into());
        fields.insert("PRIORITY".into(), "6".into());
        Entry {
            fields,
            realtime_usec: usec,
        }
    }

    fn journald_source() -> Source {
        Source {
            name: "journald".into(),
            path: String::new(),
            table: "syslog".into(),
            parser: Parser::Journald,
            timestamp: Timestamp {
                field: None,
                format: "unix_ms".into(),
                resolution: "ms".into(),
            },
            // Only _SYSTEMD_UNIT becomes a tag; MESSAGE is the field.
            tags: vec!["_SYSTEMD_UNIT".into()],
            tags_static: Default::default(),
            fields: [("MESSAGE".to_string(), FieldType::String)].into(),
            visibility: None,
            multiline: None,
        }
    }

    #[test]
    fn an_entry_maps_message_to_a_field_and_allowlisted_metadata_to_a_tag() {
        let src = journald_source();
        let mut stamper = crate::stamp::Stamper::new(crate::stamp::Resolution::Millis);
        let e = entry("disk full", "cron.service", 1_700_000_000_000_000); // us
        let lp = map_entry(&src, &e, &mut stamper).unwrap().unwrap();
        // _SYSTEMD_UNIT is a tag; PRIORITY is not allowlisted so it is absent.
        assert!(lp.contains("_SYSTEMD_UNIT=cron.service"));
        assert!(!lp.contains("PRIORITY"));
        assert!(lp.contains("MESSAGE=\"disk full\""));
        // us -> ns is in the encoded timestamp (the trailing integer).
        let ts: i64 = lp.rsplit(' ').next().unwrap().trim().parse().unwrap();
        // stamped, but within the same millisecond as the source micros->ns.
        assert!((ts - 1_700_000_000_000_000_000).abs() < 1_000_000);
    }

    #[test]
    fn resume_from_a_saved_cursor_reads_after_it_no_gap_no_dupe() {
        let entries: Vec<Entry> = (0..10)
            .map(|i| {
                entry(
                    &format!("line-{i}"),
                    "app.service",
                    1_700_000_000_000_000 + i,
                )
            })
            .collect();

        // First pass: read the first 4, remember the cursor of the last one.
        let mut j = FakeJournal::new(entries.clone());
        j.seek(None).unwrap();
        let mut read1 = Vec::new();
        for _ in 0..4 {
            let e = j.next().unwrap().unwrap();
            read1.push(e.fields["MESSAGE"].clone());
        }
        let saved = j.cursor().unwrap();

        // "Crash", then resume from the saved cursor: must read 5..=10 exactly.
        let mut j2 = FakeJournal::new(entries.clone());
        j2.seek(Some(&saved)).unwrap();
        let mut read2 = Vec::new();
        while let Some(e) = j2.next().unwrap() {
            read2.push(e.fields["MESSAGE"].clone());
        }

        let all: Vec<String> = read1.into_iter().chain(read2).collect();
        let expected: Vec<String> = (0..10).map(|i| format!("line-{i}")).collect();
        assert_eq!(
            all, expected,
            "every entry once, in order, across the resume"
        );
    }

    #[test]
    fn a_metadata_only_entry_with_no_declared_field_is_skipped() {
        let src = journald_source();
        let mut stamper = crate::stamp::Stamper::new(crate::stamp::Resolution::Millis);
        let mut fields = BTreeMap::new();
        fields.insert("_SYSTEMD_UNIT".into(), "app.service".into()); // no MESSAGE
        let e = Entry {
            fields,
            realtime_usec: 1_700_000_000_000_000,
        };
        assert!(map_entry(&src, &e, &mut stamper).unwrap().is_none());
    }
}
