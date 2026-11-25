//! Default redaction of failing-row output (zero-retention safe by default).
//!
//! Without `--explicit`, a report never carries raw cell or key values from the
//! bound inputs. Rule ids, severities, counts, binding names, column names and the
//! shape of every entry are kept; each key value and cell value is replaced by an
//! opaque, deterministic token.
//!
//! What this is NOT: the tokens are unsalted SHA-256 prefixes, so a low-entropy
//! value (a status code, a small integer) can be recovered by guessing candidates.
//! Column names, binding names, rule ids, the rule SQL/predicate text and file
//! paths are not redacted: they come from the constraints and the command line,
//! not from the bound data.

use serde_json::Value;
use sha2::{Digest, Sha256};
use verify_core::{refusal::RefusalCode, report::VerifyReport};

pub const REDACTED_PREFIX: &str = "redacted:sha256:";
const REDACTED_TEXT: &str =
    "details withheld by default redaction; pass --explicit to include them";

/// Opaque deterministic token for one value: the same value always yields the same token.
pub fn token(value: &Value) -> Value {
    let canonical = serde_json::to_string(value).expect("a JSON value serializes");
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(16);
    for byte in &digest[..8] {
        hex.push_str(&format!("{byte:02x}"));
    }
    Value::String(format!("{REDACTED_PREFIX}{hex}"))
}

pub fn redact_report(report: &mut VerifyReport) {
    for result in &mut report.results {
        for entry in &mut result.affected {
            if let Some(key) = entry.key.as_mut() {
                for value in key.values_mut() {
                    *value = token(value);
                }
            }
            if let Some(value) = entry.value.as_mut() {
                *value = token(value);
            }
        }
    }
    if let Some(refusal) = report.refusal.as_mut() {
        redact_key_objects(&mut refusal.detail);
        // DuckDB's own error text can quote data (a failed conversion names the
        // offending string). The one SQL_ERROR that carries a `column` field names
        // only a type, so it stays readable. Key tuples in details were hashed above.
        let names_column_only = refusal.detail.get("column").is_some_and(Value::is_string);
        if refusal.code == RefusalCode::SqlError && !names_column_only {
            refusal.message = format!("{:?}: {REDACTED_TEXT}", refusal.code);
            if let Value::Object(fields) = &mut refusal.detail {
                for name in ["message", "error"] {
                    if fields.contains_key(name) {
                        fields.insert(name.to_owned(), Value::String(REDACTED_TEXT.to_owned()));
                    }
                }
            }
        }
    }
}

/// Replace every value inside any object stored under a field named `key`.
fn redact_key_objects(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (name, child) in fields.iter_mut() {
                if name == "key" {
                    match child {
                        Value::Object(inner) => {
                            for leaf in inner.values_mut() {
                                *leaf = token(leaf);
                            }
                        }
                        Value::Null => {}
                        other => *other = token(other),
                    }
                } else {
                    redact_key_objects(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_key_objects),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use verify_core::{
        constraint::Severity,
        report::{AffectedEntry, ExecutionMode, ResultStatus, RuleResult, VerifyReport},
    };

    use super::*;

    fn failing_report() -> VerifyReport {
        let mut report = VerifyReport::new(ExecutionMode::Batch, "set", "sha256:x");
        report.results.push(RuleResult {
            rule_id: "R1".to_owned(),
            severity: Severity::Error,
            status: ResultStatus::Fail,
            violation_count: 1,
            affected: vec![AffectedEntry {
                binding: "loans".to_owned(),
                key: Some(BTreeMap::from([(
                    "loan_id".to_owned(),
                    json!("CANARY-LOAN"),
                )])),
                field: Some("balance".to_owned()),
                value: Some(json!("CANARY-VALUE")),
            }],
        });
        report
    }

    #[test]
    fn values_become_opaque_deterministic_tokens_and_shape_is_kept() {
        let mut report = failing_report();
        redact_report(&mut report);
        let text = serde_json::to_string(&report).unwrap();
        assert!(!text.contains("CANARY"), "{text}");
        let entry = &report.results[0].affected[0];
        assert_eq!(entry.binding, "loans");
        assert_eq!(entry.field.as_deref(), Some("balance"));
        assert!(entry.key.as_ref().unwrap().contains_key("loan_id"));
        let mut again = failing_report();
        redact_report(&mut again);
        assert_eq!(report, again, "tokens must be deterministic");
        // Equal values share a token, different values do not.
        assert_eq!(token(&json!("a")), token(&json!("a")));
        assert_ne!(token(&json!("a")), token(&json!("b")));
    }

    #[test]
    fn refusals_that_can_quote_data_are_scrubbed() {
        let mut report = VerifyReport::refusal(
            ExecutionMode::Batch,
            "set",
            "sha256:x",
            RefusalCode::SqlError,
            "SQL error: Could not convert string 'CANARY-CELL' to INT",
            json!({"rule_id": "R1", "message": "Could not convert string 'CANARY-CELL'",
                   "key": {"id": "CANARY-KEY"}}),
        );
        redact_report(&mut report);
        let text = serde_json::to_string(&report).unwrap();
        assert!(!text.contains("CANARY"), "{text}");
        assert!(text.contains("R1"));
    }
}
