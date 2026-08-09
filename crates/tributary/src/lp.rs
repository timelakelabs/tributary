//! Line-protocol encoding.
//!
//! Two rules from the reference matter more than the grammar:
//!
//! - **The batch is atomic** — if any line fails to parse the whole
//!   request is rejected and nothing in it is written. So this encoder
//!   never emits a line it is not sure of; anything questionable is
//!   refused here, where it costs one quarantined line, rather than at
//!   the server, where it costs the batch.
//! - **A non-UTF-8 body is refused whole**, before the parser, and line
//!   protocol has no byte escape. Input is decoded lossily upstream; this
//!   encoder additionally strips the control characters that would
//!   produce an unparseable line.

use std::fmt::Write as _;

/// A value bound for a field column. Tags are always strings.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone)]
pub struct Record {
    pub table: String,
    /// Ordered so encoding is deterministic — a replayed line must
    /// produce byte-identical output, and the tag set is the primary key.
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, Value)>,
    pub ts_ns: i64,
}

#[derive(Debug, PartialEq)]
pub enum EncodeError {
    NoFields,
    EmptyTable,
    NonFinite(String),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::NoFields => {
                write!(f, "a measurement with no field set is not a record")
            }
            EncodeError::EmptyTable => write!(f, "empty measurement name"),
            EncodeError::NonFinite(k) => {
                write!(
                    f,
                    "field '{k}' is NaN or infinite and has no line-protocol form"
                )
            }
        }
    }
}

/// Escape a measurement name, tag key, tag value or field key.
fn esc_key(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            ',' | '=' | ' ' => {
                out.push('\\');
                out.push(c);
            }
            // A newline inside a key would split the record into two
            // unparseable ones and reject the whole batch.
            '\n' | '\r' => out.push(' '),
            c if (c as u32) < 0x20 => out.push(' '),
            c => out.push(c),
        }
    }
}

/// Escape a quoted string field value: only `"` and `\` are special.
fn esc_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => {}
            c if (c as u32) < 0x20 => out.push(' '),
            c => out.push(c),
        }
    }
    out.push('"');
}

impl Record {
    pub fn encode(&self, out: &mut String) -> Result<(), EncodeError> {
        if self.table.is_empty() {
            return Err(EncodeError::EmptyTable);
        }
        if self.fields.is_empty() {
            return Err(EncodeError::NoFields);
        }
        // Refuse before emitting: a partially written line would corrupt
        // the batch that follows it.
        for (k, v) in &self.fields {
            if let Value::Float(f) = v
                && !f.is_finite()
            {
                return Err(EncodeError::NonFinite(k.clone()));
            }
        }

        esc_key(out, &self.table);
        for (k, v) in &self.tags {
            out.push(',');
            esc_key(out, k);
            out.push('=');
            esc_key(out, v);
        }
        out.push(' ');
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            esc_key(out, k);
            out.push('=');
            match v {
                Value::Str(s) => esc_str(out, s),
                Value::Int(n) => {
                    let _ = write!(out, "{n}i");
                }
                Value::Float(f) => {
                    let _ = write!(out, "{f}");
                }
                Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            }
        }
        let _ = write!(out, " {}", self.ts_ns);
        out.push('\n');
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> Record {
        Record {
            table: "app_logs".into(),
            tags: vec![
                ("service".into(), "api".into()),
                ("level".into(), "warn".into()),
            ],
            fields: vec![
                ("message".into(), Value::Str("disk 91% full".into())),
                ("idx".into(), Value::Int(42)),
            ],
            ts_ns: 1_786_280_343_206_000_007,
        }
    }

    #[test]
    fn encodes_the_documented_shape() {
        let mut s = String::new();
        rec().encode(&mut s).unwrap();
        assert_eq!(
            s,
            "app_logs,service=api,level=warn message=\"disk 91% full\",idx=42i \
             1786280343206000007\n"
        );
    }

    #[test]
    fn escapes_what_would_break_the_batch() {
        let mut r = rec();
        r.tags = vec![("path".into(), "a,b c=d".into())];
        r.fields = vec![(
            "message".into(),
            Value::Str("said \"hi\"\\ and\nnewline".into()),
        )];
        let mut s = String::new();
        r.encode(&mut s).unwrap();

        // exactly one record: a raw newline in a value would have made two
        assert_eq!(s.matches('\n').count(), 1);
        assert!(s.contains(r"path=a\,b\ c\=d"));
        assert!(s.contains(r#"said \"hi\"\\ and\nnewline"#));
    }

    #[test]
    fn control_characters_are_neutralised() {
        let mut r = rec();
        // what a truncated write or a mis-decoded byte leaves behind
        r.fields = vec![("message".into(), Value::Str("bad\u{0}\u{7}byte".into()))];
        let mut s = String::new();
        r.encode(&mut s).unwrap();
        assert!(!s.contains('\u{0}'));
        assert!(s.contains("bad  byte"));
    }

    #[test]
    fn refuses_rather_than_emitting_something_the_server_will_reject() {
        let mut r = rec();
        r.fields.clear();
        assert_eq!(r.encode(&mut String::new()), Err(EncodeError::NoFields));

        let mut r = rec();
        r.fields = vec![("ratio".into(), Value::Float(f64::NAN))];
        assert!(matches!(
            r.encode(&mut String::new()),
            Err(EncodeError::NonFinite(_))
        ));
    }

    #[test]
    fn encoding_is_deterministic() {
        // A replayed line must produce byte-identical output, or it
        // lands on a different primary key and duplicates instead of
        // deduplicating.
        let mut a = String::new();
        let mut b = String::new();
        rec().encode(&mut a).unwrap();
        rec().encode(&mut b).unwrap();
        assert_eq!(a, b);
    }
}
