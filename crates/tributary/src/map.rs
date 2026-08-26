//! Parse a line and map it onto a record: the tag allowlist, the
//! declared field types, and the timestamp.
//!
//! Anything that cannot be represented faithfully is **quarantined**
//! rather than shipped, because the batch is atomic — one bad line
//! rejects five thousand good ones (DESIGN.md §1.2).

use crate::config::{FieldType, Parser, Source};
use crate::lp::{Record, Value};
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum MapError {
    Empty,
    Unparseable(String),
    BadTimestamp(String),
    UncoercibleField {
        key: String,
        want: FieldType,
        got: String,
    },
}

impl std::fmt::Display for MapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapError::Empty => write!(f, "empty line"),
            MapError::Unparseable(e) => write!(f, "unparseable: {e}"),
            MapError::BadTimestamp(e) => write!(f, "bad timestamp: {e}"),
            MapError::UncoercibleField { key, want, got } => {
                write!(f, "field '{key}' will not coerce to {want:?}: {got}")
            }
        }
    }
}

/// Decode bytes as UTF-8 **lossily**. Not a nicety: one invalid byte
/// would otherwise have the whole request refused before the parser runs,
/// and line protocol has no byte escape (DESIGN.md §1.2).
pub fn decode_lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn parse_timestamp(raw: &str, format: &str) -> Result<i64, MapError> {
    let bad = |e: String| MapError::BadTimestamp(e);
    match format {
        "unix_s" => raw
            .trim()
            .parse::<i64>()
            .map(|v| v * 1_000_000_000)
            .map_err(|e| bad(e.to_string())),
        "unix_ms" => raw
            .trim()
            .parse::<i64>()
            .map(|v| v * 1_000_000)
            .map_err(|e| bad(e.to_string())),
        "unix_us" => raw
            .trim()
            .parse::<i64>()
            .map(|v| v * 1_000)
            .map_err(|e| bad(e.to_string())),
        "unix_ns" => raw.trim().parse::<i64>().map_err(|e| bad(e.to_string())),
        "rfc3339" => {
            use time::format_description::well_known::Rfc3339;
            time::OffsetDateTime::parse(raw.trim(), &Rfc3339)
                .map(|dt| dt.unix_timestamp_nanos() as i64)
                .map_err(|e| bad(e.to_string()))
        }
        other => Err(bad(format!("unknown timestamp format {other:?}"))),
    }
}

fn coerce(key: &str, want: FieldType, raw: &serde_json::Value) -> Result<Value, MapError> {
    use serde_json::Value as J;
    let uncoercible = || MapError::UncoercibleField {
        key: key.to_string(),
        want,
        got: raw.to_string(),
    };
    Ok(match (want, raw) {
        (FieldType::String, J::String(s)) => Value::Str(s.clone()),
        // Anything renders as a string; that is the point of declaring it.
        (FieldType::String, other) => Value::Str(other.to_string()),
        (FieldType::Integer, J::Number(n)) => Value::Int(n.as_i64().ok_or_else(uncoercible)?),
        (FieldType::Integer, J::String(s)) => {
            Value::Int(s.trim().parse().map_err(|_| uncoercible())?)
        }
        (FieldType::Float, J::Number(n)) => Value::Float(n.as_f64().ok_or_else(uncoercible)?),
        (FieldType::Float, J::String(s)) => {
            Value::Float(s.trim().parse().map_err(|_| uncoercible())?)
        }
        (FieldType::Boolean, J::Bool(b)) => Value::Bool(*b),
        (FieldType::Boolean, J::String(s)) => match s.trim() {
            "true" | "True" | "TRUE" | "t" | "T" => Value::Bool(true),
            "false" | "False" | "FALSE" | "f" | "F" => Value::Bool(false),
            _ => return Err(uncoercible()),
        },
        _ => return Err(uncoercible()),
    })
}

/// Turn one raw line into a record, minus its final timestamp — the
/// caller applies the [`crate::stamp::Stamper`], because sequence
/// assignment is per stream and not per line.
pub fn map_line(src: &Source, line: &str) -> Result<(Record, i64), MapError> {
    let line = line.trim_end_matches(['\n', '\r']);
    if line.trim().is_empty() {
        return Err(MapError::Empty);
    }

    let parsed: BTreeMap<String, serde_json::Value> = match src.parser {
        // DockerJson, Journald and Winlog are JSON here: an upstream step
        // (the docker reassembler / the journald reader / the winlog reader)
        // has already turned the wire form into a JSON object whose keys are
        // the parsed map.
        Parser::Json | Parser::DockerJson | Parser::Journald | Parser::Winlog => {
            serde_json::from_str(line).map_err(|e| MapError::Unparseable(e.to_string()))?
        }
        Parser::Plain => {
            let mut m = BTreeMap::new();
            m.insert(
                "message".to_string(),
                serde_json::Value::String(line.to_string()),
            );
            m
        }
    };

    let source_ts_ns = match &src.timestamp.field {
        Some(key) => {
            let raw = parsed
                .get(key)
                .ok_or_else(|| MapError::BadTimestamp(format!("no '{key}' in line")))?;
            let text = match raw {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            parse_timestamp(&text, &src.timestamp.format)?
        }
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    };

    build_record(src, &parsed, source_ts_ns)
}

/// Build a record from an already-parsed key→value map and a source
/// timestamp: the tag allowlist, static tags, stream identity and
/// visibility, then the declared fields with their coercion. Shared by
/// [`map_line`] (file tails) and the OTLP receiver (#12), so a pushed
/// OpenTelemetry log inherits the SAME allowlist — the FR-2 cardinality
/// guard — and the same "declared types only" field rule as a tailed line.
pub(crate) fn build_record(
    src: &Source,
    parsed: &BTreeMap<String, serde_json::Value>,
    source_ts_ns: i64,
) -> Result<(Record, i64), MapError> {
    // Tags: the allowlist, plus statics, plus the stream identity.
    let mut tags: Vec<(String, String)> = vec![("stream".into(), src.name.clone())];
    for (k, v) in &src.tags_static {
        tags.push((k.clone(), v.clone()));
    }
    for key in &src.tags {
        if let Some(v) = parsed.get(key) {
            let text = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            // An empty tag value is stored as an empty string here (it
            // differs from InfluxDB, which drops the tag) — so dropping
            // it is the agent's job, and keeps the PK stable.
            if !text.is_empty() {
                tags.push((key.clone(), text));
            }
        }
    }
    if let Some(vis) = &src.visibility {
        tags.push(("_visibility".into(), vis.clone()));
    }
    tags.sort_by(|a, b| a.0.cmp(&b.0));

    // Fields: declared types only. An undeclared key is dropped rather
    // than guessed, because a guess becomes permanent on first write.
    let mut fields: Vec<(String, Value)> = Vec::new();
    for (key, want) in &src.fields {
        if let Some(raw) = parsed.get(key) {
            fields.push((key.clone(), coerce(key, *want, raw)?));
        }
    }
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok((
        Record {
            table: src.table.clone(),
            tags,
            fields,
            ts_ns: 0, // assigned by the stamper
        },
        source_ts_ns,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Timestamp;

    fn src() -> Source {
        Source {
            name: "app".into(),
            path: "/dev/null".into(),
            table: "app_logs".into(),
            parser: Parser::Json,
            timestamp: Timestamp {
                field: Some("ts".into()),
                format: "unix_ms".into(),
                resolution: "ms".into(),
            },
            tags: vec!["level".into(), "service".into()],
            tags_static: [("host".to_string(), "node1".to_string())].into(),
            fields: [
                ("message".to_string(), FieldType::String),
                ("idx".to_string(), FieldType::Integer),
            ]
            .into(),
            visibility: None,
            multiline: None,
            filter: Vec::new(),
        }
    }

    #[test]
    fn a_docker_json_envelope_maps_through_the_full_path() {
        use crate::config::{FieldType, Timestamp};
        // A reassembled docker envelope (docker.rs strips the terminating
        // newline and tests that; here we prove the map half: log -> field,
        // stream -> tag, RFC3339 time -> ns).
        let envelope = serde_json::json!(
            {"log": "disk full", "stream": "stderr", "time": "2024-01-01T00:00:00Z"}
        )
        .to_string();
        let src = Source {
            name: "web".into(),
            path: "/x".into(),
            table: "container_logs".into(),
            parser: Parser::DockerJson,
            timestamp: Timestamp {
                field: Some("time".into()),
                format: "rfc3339".into(),
                resolution: "ns".into(),
            },
            tags: vec!["stream".into()],
            tags_static: Default::default(),
            fields: [("log".to_string(), FieldType::String)].into(),
            visibility: None,
            multiline: None,
            filter: Vec::new(),
        };
        let (rec, ts) = map_line(&src, &envelope).unwrap();
        assert_eq!(rec.table, "container_logs");
        assert_eq!(
            rec.fields,
            vec![("log".into(), Value::Str("disk full".into()))]
        );
        // stdout/stderr rides the `stream` tag (last-wins over the source name).
        assert!(rec.tags.iter().any(|(k, v)| k == "stream" && v == "stderr"));
        assert_eq!(ts, 1_704_067_200_000_000_000); // 2024-01-01T00:00:00Z in ns
    }

    #[test]
    fn maps_declared_tags_and_fields_only() {
        let (r, ts) = map_line(
            &src(),
            r#"{"ts":1786280343206,"level":"warn","service":"api","idx":7,
                "message":"hi","secret":"not declared"}"#,
        )
        .unwrap();

        assert_eq!(ts, 1_786_280_343_206 * 1_000_000);
        let tags: Vec<&str> = r.tags.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(tags, vec!["host", "level", "service", "stream"]);
        let fields: Vec<&str> = r.fields.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            fields,
            vec!["idx", "message"],
            "undeclared keys are dropped"
        );
    }

    #[test]
    fn declared_types_beat_what_the_line_happens_to_contain() {
        // The database fixes a field's type on first write forever, so
        // a string that should be a float must coerce, not redefine.
        let mut s = src();
        s.fields.insert("dur".into(), FieldType::Float);
        let (r, _) = map_line(&s, r#"{"ts":1,"dur":"1.5","message":"x","idx":1}"#).unwrap();
        let dur = r.fields.iter().find(|(k, _)| k == "dur").unwrap();
        assert_eq!(dur.1, Value::Float(1.5));

        // and what cannot coerce is quarantined, not shipped
        let e = map_line(&s, r#"{"ts":1,"dur":"n/a","message":"x","idx":1}"#);
        assert!(matches!(e, Err(MapError::UncoercibleField { .. })));
    }

    #[test]
    fn visibility_rides_as_an_ordinary_tag() {
        let mut s = src();
        s.visibility = Some("(ops&audit)|admin".into());
        let (r, _) = map_line(&s, r#"{"ts":1,"message":"x","idx":1}"#).unwrap();
        assert!(
            r.tags
                .contains(&("_visibility".into(), "(ops&audit)|admin".into()))
        );
    }

    #[test]
    fn empty_tag_values_are_dropped_to_keep_the_key_stable() {
        let (r, _) = map_line(&src(), r#"{"ts":1,"level":"","message":"x","idx":1}"#).unwrap();
        assert!(!r.tags.iter().any(|(k, _)| k == "level"));
    }

    #[test]
    fn lossy_decode_survives_binary_garbage() {
        let s = decode_lossy(b"ok \xff\xfe bytes");
        assert!(s.contains('\u{FFFD}'));
        assert!(s.starts_with("ok "));
    }
}
