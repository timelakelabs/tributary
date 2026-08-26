//! The transform stage (T-2, #7): what happens to a mapped record between
//! `map_line` and the queue. Today: filter (#42) — drop a record by a declared
//! predicate. Sample (#43) and redact (#44) join here.
//!
//! Everything runs on the MAPPED record — typed fields and canonical tags, not
//! raw text — and BEFORE the watermark counts the record, so a dropped record
//! is never claimed as arrived and the completeness guarantee stays honest.

use crate::config::{Filter, Sample};
use crate::lp::{Record, Value};

/// Whether a record survives the filter rules (#42). `drop = true` rules form a
/// deny-list (a match removes the record); `drop = false` rules form an
/// allow-list (only records matching some allow rule survive). A record is kept
/// iff no deny rule matches AND (there are no allow rules, or one matches).
pub fn keeps(record: &Record, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    // Deny: any matching drop=true rule removes the record.
    if filters.iter().any(|f| f.drop && rule_matches(record, f)) {
        return false;
    }
    // Allow-list: if any allow rules exist, keep only records that match one.
    let mut has_allow = false;
    let mut allowed = false;
    for f in filters.iter().filter(|f| !f.drop) {
        has_allow = true;
        if rule_matches(record, f) {
            allowed = true;
            break;
        }
    }
    !has_allow || allowed
}

fn rule_matches(record: &Record, f: &Filter) -> bool {
    field_equals(record, &f.field, &f.equals)
}

/// A record has a tag OR field named `field` whose value equals `equals`. Field
/// values compare as their string form, so a numeric field `code=500` matches
/// `"500"`. Shared by filter (#42) and sample (#43).
fn field_equals(record: &Record, field: &str, equals: &str) -> bool {
    record.tags.iter().any(|(k, v)| k == field && v == equals)
        || record
            .fields
            .iter()
            .any(|(k, v)| k == field && value_eq(v, equals))
}

fn value_eq(v: &Value, s: &str) -> bool {
    match v {
        Value::Str(x) => x == s,
        Value::Int(n) => n.to_string() == s,
        Value::Float(x) => x.to_string() == s,
        Value::Bool(b) => (if *b { "true" } else { "false" }) == s,
    }
}

/// Whether a record survives the sample rules (#43). A rule with a predicate
/// (`field`/`equals`) samples only the records it matches — so a subset can be
/// sampled while the rest passes untouched; a rule with no predicate samples
/// the whole source. The keep decision is DETERMINISTIC on the record's
/// identity — a fixed-seed hash of its content and source timestamp,
/// `% rate == 0` — so a crash-resumed tail re-decides identically and
/// TimeLakeDB's last-write-wins collapses the replay (DESIGN §3.2). It is NOT a
/// running counter (which would re-decide on resume) and NOT std's
/// `DefaultHasher` (SipHash, randomly seeded per process — it would re-roll the
/// decision on every restart).
pub fn sample_keeps(record: &Record, source_ts: i64, rules: &[Sample]) -> bool {
    for r in rules {
        let applies = match (&r.field, &r.equals) {
            (Some(f), Some(e)) => field_equals(record, f, e),
            _ => true, // no predicate = the whole source
        };
        if applies && r.rate > 1 && !record_hash(record, source_ts).is_multiple_of(r.rate) {
            return false; // sampled out of this rule's 1-in-rate
        }
    }
    true
}

/// FNV-1a over the record's identity (tags, fields, source timestamp). A FIXED
/// seed on purpose: the sample decision must survive a process restart, which
/// std's per-process-seeded hasher would not.
fn record_hash(record: &Record, source_ts: i64) -> u64 {
    let mut h = Fnv::new();
    for (k, v) in &record.tags {
        h.feed(k);
        h.feed(v);
    }
    for (k, v) in &record.fields {
        h.feed(k);
        match v {
            Value::Str(s) => h.feed(s),
            Value::Int(n) => h.feed(&n.to_string()),
            Value::Float(f) => h.feed(&f.to_string()),
            Value::Bool(b) => h.feed(if *b { "true" } else { "false" }),
        }
    }
    h.feed_bytes(&source_ts.to_le_bytes());
    h.0
}

struct Fnv(u64);
impl Fnv {
    fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
    /// Feed a string, then a separator so ("ab","c") hashes unlike ("a","bc").
    fn feed(&mut self, s: &str) {
        self.feed_bytes(s.as_bytes());
        self.byte(0);
    }
    fn feed_bytes(&mut self, bs: &[u8]) {
        for &b in bs {
            self.byte(b);
        }
    }
    fn byte(&mut self, b: u8) {
        self.0 ^= b as u64;
        self.0 = self.0.wrapping_mul(0x100000001b3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(field: &str, equals: &str, drop: bool) -> Filter {
        Filter {
            field: field.to_string(),
            equals: equals.to_string(),
            drop,
        }
    }

    fn rec(tags: &[(&str, &str)], fields: &[(&str, Value)]) -> Record {
        Record {
            table: "t".to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            ts_ns: 0,
        }
    }

    #[test]
    fn no_filters_keeps_everything() {
        assert!(keeps(&rec(&[("level", "info")], &[]), &[]));
    }

    #[test]
    fn a_deny_rule_drops_a_matching_tag_but_not_a_non_match() {
        let deny = [filter("level", "debug", true)];
        assert!(!keeps(&rec(&[("level", "debug")], &[]), &deny));
        assert!(keeps(&rec(&[("level", "error")], &[]), &deny));
    }

    #[test]
    fn a_deny_rule_matches_a_field_value_stringified() {
        let r = rec(&[], &[("code", Value::Int(500))]);
        assert!(!keeps(&r, &[filter("code", "500", true)]));
    }

    #[test]
    fn an_allow_list_keeps_only_matches() {
        let allow = [filter("level", "error", false)];
        assert!(keeps(&rec(&[("level", "error")], &[]), &allow));
        assert!(!keeps(&rec(&[("level", "info")], &[]), &allow));
    }

    #[test]
    fn deny_wins_over_allow() {
        // Allow env=prod, but deny level=debug: a prod debug line is dropped
        // (deny is checked first); a prod error survives.
        let rules = [filter("level", "debug", true), filter("env", "prod", false)];
        assert!(!keeps(
            &rec(&[("env", "prod"), ("level", "debug")], &[]),
            &rules
        ));
        assert!(keeps(
            &rec(&[("env", "prod"), ("level", "error")], &[]),
            &rules
        ));
    }

    fn sample(field: Option<&str>, equals: Option<&str>, rate: u64) -> Sample {
        Sample {
            field: field.map(str::to_string),
            equals: equals.map(str::to_string),
            rate,
        }
    }

    #[test]
    fn a_sample_decision_is_deterministic() {
        // The same record + source_ts always decides the same way — the whole
        // point, so a crash-resumed tail can't re-decide and double-count.
        let r = rec(&[("level", "debug")], &[("msg", Value::Str("x".into()))]);
        let rules = [sample(None, None, 3)];
        assert_eq!(sample_keeps(&r, 42, &rules), sample_keeps(&r, 42, &rules));
    }

    #[test]
    fn record_hash_is_fixed_seed_not_randomized() {
        // A hardcoded value proves the seed is FIXED (FNV-1a) rather than std's
        // per-process-random SipHash — which would make this change run to run
        // and silently break crash-resume determinism.
        let r = rec(&[("a", "b")], &[]);
        assert_eq!(record_hash(&r, 0), 18_082_658_368_423_242_550);
    }

    #[test]
    fn rate_1_keeps_everything() {
        let r = rec(&[("level", "debug")], &[]);
        assert!(sample_keeps(&r, 1, &[sample(None, None, 1)]));
    }

    #[test]
    fn sample_keeps_roughly_one_in_rate() {
        let rate = 4u64;
        let rules = [sample(None, None, rate)];
        let kept = (0..4000)
            .filter(|i| sample_keeps(&rec(&[("id", &i.to_string())], &[]), 0, &rules))
            .count();
        // 4000 / 4 = 1000; a generous band for hash distribution.
        assert!((700..=1300).contains(&kept), "kept {kept}, want ~1000");
    }

    #[test]
    fn a_predicate_samples_only_matching_records() {
        // Sample debug at 1-in-a-million (nearly all dropped); errors pass.
        let rules = [sample(Some("level"), Some("debug"), 1_000_000)];
        let debug_kept = (0..200)
            .filter(|i| {
                sample_keeps(
                    &rec(&[("level", "debug"), ("id", &i.to_string())], &[]),
                    0,
                    &rules,
                )
            })
            .count();
        assert!(
            debug_kept < 5,
            "most debug should be sampled out, kept {debug_kept}"
        );
        // Every error passes untouched — the rule doesn't apply to it.
        for i in 0..200 {
            assert!(sample_keeps(
                &rec(&[("level", "error"), ("id", &i.to_string())], &[]),
                0,
                &rules
            ));
        }
    }
}
