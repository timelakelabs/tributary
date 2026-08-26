//! Windows Event Log source (#11): read a channel through the modern
//! Windows Event Log API (`wevtapi`), not by tailing a file — there is no
//! file to tail, the log is a structured store behind `EvtQuery`.
//!
//! The resume token is a **bookmark**, not a record number. Windows hands
//! back an opaque bookmark XML for the current event; that bookmark IS the
//! checkpoint. On restart we re-open the query and `EvtSeek` to just after
//! the saved bookmark, so we read the next event and no earlier one: no
//! gap, no dupe. (A raw `EventRecordID` offset is the wrong token — records
//! are purged and the channel can wrap, so an offset can point at nothing;
//! the bookmark survives that, which is the whole reason it exists.)
//!
//! Each event is rendered to XML, the fields we keep are pulled out into a
//! JSON object, and that object goes through the ordinary
//! [`crate::map::map_line`] (Parser::Winlog == JSON) — so a field becomes a
//! tag only if the source's allowlist NAMES it, the same FR-2 guard
//! everything else has. The timestamp is the event's own `TimeCreated`
//! (100 ns FILETIME precision), not a JSON field.
//!
//! Everything that links `wevtapi` is behind `#[cfg(feature = "winlog")]`
//! AND `#[cfg(windows)]`, so the default build (and CI on a plain Linux
//! runner) grows no dependency and links nothing; a config naming winlog
//! without the feature is refused at load.

// On any build without the real reader (the default, and every Linux build),
// the pull loop `run_winlog`/`drain` has no caller — it exists for the
// winlog+windows build and for the tests below. In a *binary* crate `pub`
// does not count as reachable, so silence dead_code in exactly that
// configuration rather than gate the whole module out: the reader trait,
// mapping and resume logic (and their tests) then still compile and run in
// the default build. When the real reader IS present, everything is used and
// dead code is still caught.
#![cfg_attr(not(all(feature = "winlog", windows)), allow(dead_code))]

use std::collections::BTreeMap;
use std::time::Duration;

use crate::config::Source;

/// One event: the fields we rendered out of it, plus the nanosecond
/// timestamp the channel stamped it with. `time_created_ns` is an
/// "address" of the event, not one of its data fields, so it rides
/// alongside the map rather than inside it (mirrors journald's realtime).
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub fields: BTreeMap<String, String>,
    pub time_created_ns: i64,
}

/// The seam to the event log, so the bookmark/resume logic is testable with
/// a fake and the `wevtapi` implementation stays behind the feature gate.
///
/// The shape mirrors [`crate::journald::JournalReader`] deliberately: the
/// two sources differ only in what an opaque resume token is made of.
pub trait WinlogReader {
    /// Position for reading. `Some(bookmark)` resumes AFTER that bookmark's
    /// event (the next `next()` returns the following one); `None` starts at
    /// the oldest event in the channel.
    fn seek(&mut self, bookmark: Option<&str>) -> anyhow::Result<()>;
    /// Advance to and return the next event, or `None` at the tail.
    fn next(&mut self) -> anyhow::Result<Option<Event>>;
    /// The bookmark for the current event — persist this, it is the resume
    /// token. Opaque XML; treat it as bytes, never parse it for an offset.
    fn bookmark(&mut self) -> anyhow::Result<String>;
    /// Block until events are appended past the current position, or the
    /// timeout elapses. Lets the follow loop sleep instead of spin.
    fn wait(&mut self, timeout: Duration) -> anyhow::Result<()>;
}

/// Render an event's fields as a JSON line for `map_line`. The allowlist and
/// declared fields on the source decide which of them survive.
pub fn event_to_json(event: &Event) -> String {
    let map: serde_json::Map<String, serde_json::Value> = event
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::Value::Object(map).to_string()
}

/// Map one event to an encoded line-protocol record: JSON -> `map_line`
/// (allowlist + declared fields), with the channel's own `TimeCreated`
/// overriding whatever the string-timestamp path would pick, then stamped.
///
/// Returns `Ok(None)` for an event that maps to nothing (e.g. no declared
/// field present) so a bookkeeping event is skipped, not shipped empty.
pub fn map_event(
    source: &Source,
    event: &Event,
    stamper: &mut crate::stamp::Stamper,
) -> anyhow::Result<Option<String>> {
    let line = event_to_json(event);
    let (mut record, _ignored_ts) = match crate::map::map_line(source, &line) {
        Ok(r) => r,
        Err(crate::map::MapError::Empty) => return Ok(None),
        Err(e) => return Err(anyhow::anyhow!(e.to_string())),
    };
    // A record with no declared field present is not shippable line protocol.
    if record.fields.is_empty() {
        return Ok(None);
    }
    // The channel's timestamp is authoritative — already nanoseconds.
    record.ts_ns = stamper
        .stamp(event.time_created_ns)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let mut out = String::new();
    record
        .encode(&mut out)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(Some(out))
}

/// Ship everything currently in the queue: front -> read -> send -> pop,
/// until empty or a send fails (the batch is left queued for next time).
/// Identical in shape to the journald drain — the queue does not care what
/// filled it.
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
                tracing::warn!(error = %e, "winlog ship failed; leaving batch queued");
                break;
            }
        }
    }
    Ok(())
}

/// The winlog agent loop: read events via the bookmark, queue durably, ship,
/// and checkpoint the bookmark — the resume token. Reuses the same
/// Queue/Shipper/Stamper/Checkpoint the file tail uses; the file-tail loop is
/// untouched. The caller races this against its shutdown signal.
///
/// Durability order is the point, exactly as in journald: an event is in the
/// durable queue BEFORE the bookmark advances past it, so a crash re-drains
/// the queue (LWW dedups) and resumes reading after the last-queued event —
/// no gap.
#[allow(clippy::too_many_arguments)]
pub async fn run_winlog<R: WinlogReader>(
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
    let mut bookmark: Option<String> = None;
    if let Some(cp) = &restored {
        if let Some(t) = cp.last_tick_ns {
            stamper.restore(t, cp.next_seq);
        }
        bookmark = cp.cursor.clone();
    }
    reader.seek(bookmark.as_deref())?;

    let mut queue = crate::queue::Queue::open(&state_dir.join("winlog-queue"), queue_max_bytes)?;
    // Anything a previous run left queued ships before new work.
    drain(&mut queue, &shipper).await?;

    loop {
        let mut batch: Vec<String> = Vec::new();
        let mut last_bookmark: Option<String> = None;
        let mut at_tail = false;
        while batch.len() < batch_lines {
            match reader.next()? {
                Some(event) => {
                    if let Some(lp) = map_event(source, &event, &mut stamper)? {
                        batch.push(lp);
                    }
                    last_bookmark = Some(reader.bookmark()?);
                }
                None => {
                    at_tail = true;
                    break;
                }
            }
        }

        if !batch.is_empty() {
            let body = batch.join("\n");
            queue.push(&body)?; // DURABLE before the bookmark advances
            let (last_tick_ns, next_seq) = match stamper.checkpoint() {
                Some((t, s)) => (Some(t), s),
                None => (None, 0),
            };
            crate::checkpoint::Checkpoint {
                files: Vec::new(),
                last_tick_ns,
                next_seq,
                lateness_ns: None,
                cursor: last_bookmark,
            }
            .save(&cp_path)?;
            drain(&mut queue, &shipper).await?;
        } else if at_tail {
            // Even with nothing shippable, persist a bookmark we advanced
            // over (a run of undeclared-field events) so we do not re-scan
            // them after a restart. `last_bookmark` is None here only when
            // literally nothing was read this pass.
            if let Some(bm) = last_bookmark {
                let (last_tick_ns, next_seq) = match stamper.checkpoint() {
                    Some((t, s)) => (Some(t), s),
                    None => (None, 0),
                };
                crate::checkpoint::Checkpoint {
                    files: Vec::new(),
                    last_tick_ns,
                    next_seq,
                    lateness_ns: None,
                    cursor: Some(bm),
                }
                .save(&cp_path)?;
            }
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
// The real event log, behind the feature AND the windows target so the
// default build links nothing.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "winlog", windows))]
pub use real::RealEventLog;

#[cfg(all(feature = "winlog", windows))]
mod real {
    use super::{Event, WinlogReader};
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::time::Duration;
    use windows::Win32::Foundation::ERROR_NO_MORE_ITEMS;
    use windows::Win32::System::EventLog::{
        EVT_HANDLE, EvtClose, EvtCreateBookmark, EvtNext, EvtQuery, EvtQueryChannelPath,
        EvtQueryForwardDirection, EvtRender, EvtRenderBookmark, EvtRenderEventXml, EvtSeek,
        EvtSeekRelativeToBookmark, EvtUpdateBookmark,
    };
    use windows::core::{HRESULT, PCWSTR};

    /// A wide (UTF-16, NUL-terminated) copy of a Rust string, for the `PCWSTR`
    /// arguments the API takes.
    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    /// Owns the query result set and the bookmark. Neither handle is shared
    /// across threads.
    pub struct RealEventLog {
        channel: Vec<u16>,
        query: Option<EVT_HANDLE>,
        bookmark: EVT_HANDLE,
        /// Whether we have advanced the bookmark over at least one event —
        /// only then is an `EvtSeek` relative to it meaningful.
        read_any: bool,
    }

    // The handles are used from a single task; we never share them.
    unsafe impl Send for RealEventLog {}

    /// Render a handle (an event, or the bookmark) to its XML string. Safe to
    /// call: the handle is a valid `EVT_HANDLE` owned by the caller, which is
    /// the only invariant the FFI needs.
    fn render(handle: EVT_HANDLE, flags: u32) -> anyhow::Result<String> {
        let mut used: u32 = 0;
        let mut props: u32 = 0;
        // First call sizes the buffer (returns ERROR_INSUFFICIENT_BUFFER,
        // which we ignore — `used` is what we came for).
        let _ = unsafe { EvtRender(None, handle, flags, 0, None, &mut used, &mut props) };
        if used == 0 {
            return Ok(String::new());
        }
        // `used` is a byte count of a UTF-16 buffer including the NUL.
        let wchars = used.div_ceil(2) as usize;
        let mut buf = vec![0u16; wchars];
        unsafe {
            EvtRender(
                None,
                handle,
                flags,
                used,
                Some(buf.as_mut_ptr() as *mut c_void),
                &mut used,
                &mut props,
            )?
        };
        let s = String::from_utf16_lossy(&buf);
        Ok(s.trim_end_matches('\0').to_string())
    }

    impl RealEventLog {
        /// Prepare to read `channel` (e.g. `"System"`, `"Application"`, or a
        /// custom channel path). Nothing is opened until the first `seek`.
        pub fn open(channel: &str) -> anyhow::Result<RealEventLog> {
            Ok(RealEventLog {
                channel: wide(channel),
                query: None,
                // An empty bookmark; a saved one replaces it in `seek`.
                bookmark: unsafe { EvtCreateBookmark(PCWSTR::null())? },
                read_any: false,
            })
        }

        /// (Re)open the query result set. If we have advanced the bookmark,
        /// seek to just after it so we resume rather than re-read.
        fn open_query(&mut self) -> anyhow::Result<()> {
            let flags = EvtQueryChannelPath.0 | EvtQueryForwardDirection.0;
            let q = unsafe {
                EvtQuery(
                    None,
                    PCWSTR(self.channel.as_ptr()),
                    PCWSTR::null(), // "*" — the whole channel, oldest first
                    flags,
                )?
            };
            if self.read_any {
                // Position 1 relative to the bookmark = the event AFTER it.
                unsafe {
                    EvtSeek(
                        q,
                        1,
                        Some(self.bookmark),
                        Some(0),
                        EvtSeekRelativeToBookmark.0,
                    )?
                };
            }
            self.query = Some(q);
            Ok(())
        }

        fn close_query(&mut self) {
            if let Some(q) = self.query.take() {
                let _ = unsafe { EvtClose(q) };
            }
        }
    }

    impl WinlogReader for RealEventLog {
        fn seek(&mut self, bookmark: Option<&str>) -> anyhow::Result<()> {
            self.close_query();
            // Replace the bookmark handle with one restored from the saved
            // XML, or a fresh empty one for a from-head read.
            let old = std::mem::replace(&mut self.bookmark, unsafe {
                match bookmark {
                    Some(xml) => {
                        let w = wide(xml);
                        EvtCreateBookmark(PCWSTR(w.as_ptr()))?
                    }
                    None => EvtCreateBookmark(PCWSTR::null())?,
                }
            });
            let _ = unsafe { EvtClose(old) };
            self.read_any = bookmark.is_some();
            Ok(())
        }

        fn next(&mut self) -> anyhow::Result<Option<Event>> {
            if self.query.is_none() {
                self.open_query()?;
            }
            let q = self.query.expect("opened above");
            // EvtNext fills an array of raw handles (`isize`); we wrap the one
            // we asked for back into an EVT_HANDLE.
            let mut evt = [0isize; 1];
            let mut returned: u32 = 0;
            let r = unsafe { EvtNext(q, &mut evt, 5_000, 0, &mut returned) };
            if let Err(e) = r {
                if e.code() == HRESULT::from_win32(ERROR_NO_MORE_ITEMS.0) {
                    // Exhausted: drop the result set so a later wait+next
                    // re-opens it seeked to the (now advanced) bookmark.
                    self.close_query();
                    return Ok(None);
                }
                return Err(anyhow::anyhow!("EvtNext failed: {e}"));
            }
            if returned == 0 {
                self.close_query();
                return Ok(None);
            }
            let handle = EVT_HANDLE(evt[0]);
            let xml = render(handle, EvtRenderEventXml.0);
            // Advance the bookmark to this event BEFORE closing it.
            let up = unsafe { EvtUpdateBookmark(self.bookmark, handle) };
            let _ = unsafe { EvtClose(handle) };
            let xml = xml?;
            up?;
            self.read_any = true;
            Ok(Some(parse_event_xml(&xml)))
        }

        fn bookmark(&mut self) -> anyhow::Result<String> {
            render(self.bookmark, EvtRenderBookmark.0)
        }

        fn wait(&mut self, timeout: Duration) -> anyhow::Result<()> {
            // A query result set is a snapshot, so "wait for appends" is a
            // sleep followed by a re-open (done lazily in `next`). Not a spin:
            // the caller only calls this at the tail.
            std::thread::sleep(timeout);
            self.close_query();
            Ok(())
        }
    }

    impl Drop for RealEventLog {
        fn drop(&mut self) {
            self.close_query();
            let _ = unsafe { EvtClose(self.bookmark) };
        }
    }

    /// Pull the fields we keep out of an event's rendered XML. Deliberately
    /// small: the `<System>` header fields by name, every `<EventData>`
    /// `<Data>` element, and the whole XML under `Xml` for anyone who
    /// declares it. Windows renders attributes single-quoted.
    fn parse_event_xml(xml: &str) -> Event {
        let mut fields = BTreeMap::new();

        if let Some(v) = attr(xml, "<Provider ", "Name") {
            fields.insert("Provider".into(), v);
        }
        for tag in [
            "EventID",
            "Level",
            "Task",
            "Opcode",
            "Keywords",
            "EventRecordID",
            "Channel",
            "Computer",
            "Version",
        ] {
            if let Some(v) = element(xml, tag) {
                fields.insert(tag.into(), v);
            }
        }

        // EventData: <Data Name='foo'>bar</Data>, or unnamed <Data>bar</Data>.
        let mut unnamed = 0;
        let mut rest = xml;
        while let Some(start) = rest.find("<Data") {
            let after = &rest[start..];
            let Some(gt) = after.find('>') else { break };
            let open_tag = &after[..gt];
            let body_start = start + gt + 1;
            let Some(end) = rest[body_start..].find("</Data>") else {
                break;
            };
            let val = unescape(&rest[body_start..body_start + end]);
            if let Some(name) = attr_in(open_tag, "Name") {
                fields.insert(name, val);
            } else if !val.is_empty() {
                fields.insert(format!("Data{unnamed}"), val);
                unnamed += 1;
            }
            rest = &rest[body_start + end + "</Data>".len()..];
        }

        let time_created_ns = attr(xml, "<TimeCreated ", "SystemTime")
            .and_then(|s| parse_systemtime_ns(&s))
            .unwrap_or_else(now_ns);

        fields.insert("Xml".into(), xml.to_string());
        Event {
            fields,
            time_created_ns,
        }
    }

    /// The text content of `<tag>...</tag>` (first occurrence), unescaped.
    fn element(xml: &str, tag: &str) -> Option<String> {
        let open = format!("<{tag}");
        let start = xml.find(&open)?;
        let after = &xml[start..];
        let gt = after.find('>')?;
        // A self-closing element (`<Tag .../>`) has no text content.
        if after.as_bytes().get(gt.wrapping_sub(1)) == Some(&b'/') {
            return None;
        }
        let body = &after[gt + 1..];
        let close = format!("</{tag}>");
        let end = body.find(&close)?;
        Some(unescape(&body[..end]))
    }

    /// The value of attribute `name` on the element that begins with `open`
    /// (e.g. `open = "<Provider "`, `name = "Name"`).
    fn attr(xml: &str, open: &str, name: &str) -> Option<String> {
        let start = xml.find(open)?;
        let after = &xml[start..];
        let gt = after.find('>')?;
        attr_in(&after[..gt], name)
    }

    /// The value of attribute `name` within a single opening-tag slice.
    fn attr_in(tag: &str, name: &str) -> Option<String> {
        let key = format!("{name}=");
        let at = tag.find(&key)?;
        let after = &tag[at + key.len()..];
        let quote = after.chars().next()?;
        if quote != '\'' && quote != '"' {
            return None;
        }
        let rest = &after[1..];
        let end = rest.find(quote)?;
        Some(unescape(&rest[..end]))
    }

    /// The five predefined XML entities, enough for event text.
    fn unescape(s: &str) -> String {
        s.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&")
    }

    /// `2024-01-01T12:34:56.7891234Z` -> unix nanoseconds. Windows uses up to
    /// seven fractional digits (100 ns); RFC 3339 parsing takes them.
    fn parse_systemtime_ns(s: &str) -> Option<i64> {
        use time::format_description::well_known::Rfc3339;
        time::OffsetDateTime::parse(s.trim(), &Rfc3339)
            .ok()
            .map(|dt| dt.unix_timestamp_nanos() as i64)
    }

    fn now_ns() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldType, Parser, Timestamp};

    /// An in-memory event log for testing seek/next/bookmark and resume
    /// without `wevtapi`. The bookmark is just the event index — opaque to
    /// the code under test, which is the whole point.
    struct FakeEventLog {
        events: Vec<Event>,
        pos: Option<usize>, // index of the current event, None before the first
    }

    impl FakeEventLog {
        fn new(events: Vec<Event>) -> Self {
            FakeEventLog { events, pos: None }
        }
    }

    impl WinlogReader for FakeEventLog {
        fn seek(&mut self, bookmark: Option<&str>) -> anyhow::Result<()> {
            self.pos = match bookmark {
                None => None,
                Some(b) => Some(b.parse::<usize>()?), // land ON the saved event
            };
            Ok(())
        }
        fn next(&mut self) -> anyhow::Result<Option<Event>> {
            let i = match self.pos {
                None => 0,
                Some(p) => p + 1,
            };
            if i >= self.events.len() {
                return Ok(None); // stay put so a later append could be read
            }
            self.pos = Some(i);
            Ok(Some(self.events[i].clone()))
        }
        fn bookmark(&mut self) -> anyhow::Result<String> {
            Ok(self.pos.expect("bookmark before an event").to_string())
        }
        fn wait(&mut self, _t: Duration) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn event(msg: &str, provider: &str, rid: u64, ns: i64) -> Event {
        let mut fields = BTreeMap::new();
        fields.insert("Message".into(), msg.into());
        fields.insert("Provider".into(), provider.into());
        fields.insert("EventID".into(), "7040".into());
        fields.insert("Level".into(), "4".into());
        fields.insert("EventRecordID".into(), rid.to_string());
        Event {
            fields,
            time_created_ns: ns,
        }
    }

    fn winlog_source() -> Source {
        Source {
            name: "winsys".into(),
            path: "System".into(),
            table: "eventlog".into(),
            parser: Parser::Winlog,
            timestamp: Timestamp {
                field: None,
                format: "unix_ms".into(),
                resolution: "us".into(),
            },
            // Only Provider becomes a tag; Message is the field.
            tags: vec!["Provider".into()],
            tags_static: Default::default(),
            fields: [("Message".to_string(), FieldType::String)].into(),
            visibility: None,
            multiline: None,
            filter: Vec::new(),
            sample: Vec::new(),
        }
    }

    #[test]
    fn an_event_maps_message_to_a_field_and_allowlisted_metadata_to_a_tag() {
        let src = winlog_source();
        let mut stamper = crate::stamp::Stamper::new(crate::stamp::Resolution::Micros);
        let e = event(
            "service stopped",
            "Service Control Manager",
            42,
            1_700_000_000_123_456_700,
        );
        let lp = map_event(&src, &e, &mut stamper).unwrap().unwrap();
        // Provider is a tag; EventID/Level are not allowlisted, so absent.
        assert!(
            lp.contains("Provider=Service\\ Control\\ Manager"),
            "got: {lp}"
        );
        assert!(!lp.contains("EventID"));
        assert!(!lp.contains("EventRecordID"));
        assert!(lp.contains("Message=\"service stopped\""));
        // The event's own ns timestamp survives to the encoded line (floored
        // to the microsecond tick, then sequence-filled — same millisecond).
        let ts: i64 = lp.rsplit(' ').next().unwrap().trim().parse().unwrap();
        assert!(
            (ts - 1_700_000_000_123_456_700).abs() < 1_000_000,
            "ts={ts}"
        );
    }

    #[test]
    fn resume_from_a_saved_bookmark_reads_after_it_no_gap_no_dupe() {
        let events: Vec<Event> = (0..10)
            .map(|i| {
                event(
                    &format!("line-{i}"),
                    "app",
                    i as u64,
                    1_700_000_000_000_000_000 + i as i64,
                )
            })
            .collect();

        // First pass: read the first 4, remember the bookmark of the last one.
        let mut r = FakeEventLog::new(events.clone());
        r.seek(None).unwrap();
        let mut read1 = Vec::new();
        for _ in 0..4 {
            let e = r.next().unwrap().unwrap();
            read1.push(e.fields["Message"].clone());
        }
        let saved = r.bookmark().unwrap();

        // "Crash", then resume from the saved bookmark: reads 5..=10 exactly.
        let mut r2 = FakeEventLog::new(events.clone());
        r2.seek(Some(&saved)).unwrap();
        let mut read2 = Vec::new();
        while let Some(e) = r2.next().unwrap() {
            read2.push(e.fields["Message"].clone());
        }

        let all: Vec<String> = read1.into_iter().chain(read2).collect();
        let expected: Vec<String> = (0..10).map(|i| format!("line-{i}")).collect();
        assert_eq!(
            all, expected,
            "every event once, in order, across the resume"
        );
    }

    #[test]
    fn an_event_with_no_declared_field_is_skipped() {
        let src = winlog_source();
        let mut stamper = crate::stamp::Stamper::new(crate::stamp::Resolution::Micros);
        let mut fields = BTreeMap::new();
        fields.insert("Provider".into(), "app".into()); // no Message
        fields.insert("EventID".into(), "1".into());
        let e = Event {
            fields,
            time_created_ns: 1_700_000_000_000_000_000,
        };
        assert!(map_event(&src, &e, &mut stamper).unwrap().is_none());
    }

    #[test]
    fn json_round_trip_preserves_the_kept_fields() {
        let e = event("hi", "prov", 7, 1);
        let j = event_to_json(&e);
        let back: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(back["Message"], "hi");
        assert_eq!(back["Provider"], "prov");
        assert_eq!(back["EventRecordID"], "7");
    }
}
