//! The transform stage (T-2, #7): what happens to a mapped record between
//! `map_line` and the queue. Today: filter (#42) — drop a record by a declared
//! predicate. Sample (#43) and redact (#44) join here.
//!
//! Everything runs on the MAPPED record — typed fields and canonical tags, not
//! raw text — and BEFORE the watermark counts the record, so a dropped record
//! is never claimed as arrived and the completeness guarantee stays honest.

use crate::config::Filter;
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

/// A rule matches if a tag OR a field named `field` has value `equals`. Field
/// values are compared as their string form, so a numeric field `code=500`
/// matches `equals = "500"`.
fn rule_matches(record: &Record, f: &Filter) -> bool {
    record
        .tags
        .iter()
        .any(|(k, v)| k == &f.field && v == &f.equals)
        || record
            .fields
            .iter()
            .any(|(k, v)| k == &f.field && value_eq(v, &f.equals))
}

fn value_eq(v: &Value, s: &str) -> bool {
    match v {
        Value::Str(x) => x == s,
        Value::Int(n) => n.to_string() == s,
        Value::Float(x) => x.to_string() == s,
        Value::Bool(b) => (if *b { "true" } else { "false" }) == s,
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
}
