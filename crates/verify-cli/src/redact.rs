//! Default redaction of failing-row output (zero-retention safe by default).
//!
//! Without `--explicit`, a report never carries raw cell or key values from the bound
//! inputs. Rule ids, severities, counts, binding names, column names and the shape of
//! every entry are kept; each key value and cell value is replaced by a marker.
//!
//! Two marker modes:
//!
//! - **Ordinal refs (default).** `redacted:ref:k1`, `k2`, ... for key values and
//!   `redacted:ref:v1`, `v2`, ... for cell values, numbered in order of first appearance
//!   over the entries that are actually emitted. Keys and cells are separate namespaces, so a
//!   key and a cell holding equal bytes do not share a ref. No hash of any value is
//!   computed, so nothing here can be guessed from a dictionary. A ref reveals only the
//!   equality pattern inside this one output and the number of distinct values, never the
//!   content. Refs are not comparable across runs on different inputs or different sampling.
//!   Output is byte-identical when the same input is run again with the same flags.
//! - **Keyed markers (opt-in).** When `VERIFY_REDACTION_KEY` holds a key of at least 16
//!   bytes, each value becomes `redacted:hmac-sha256:<32 hex>`: HMAC-SHA256 over the
//!   canonical JSON of the value, truncated to 128 bits. Stable across runs and inputs for the
//!   key holder, unguessable without the key. An empty or short key is refused
//!   (`E_REDACTION_KEY`), never silently downgraded to ordinal refs.
//!
//! What this is NOT: column names, binding names, rule ids, the rule's own SQL or
//! predicate text, file paths and counts are not redacted. Equality of values inside one
//! output is visible in both modes, and across outputs in keyed mode.

use std::collections::BTreeMap;
use std::ffi::OsString;

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use verify_core::{refusal::RefusalCode, report::VerifyReport};

pub const ENV_KEY: &str = "VERIFY_REDACTION_KEY";
pub const MIN_KEY_BYTES: usize = 16;
pub const REF_PREFIX: &str = "redacted:ref:";
pub const HMAC_PREFIX: &str = "redacted:hmac-sha256:";
const REDACTED_TEXT: &str =
    "details withheld by default redaction; pass --explicit to include them";

/// Why `VERIFY_REDACTION_KEY` was refused. Never carries the key or its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    Empty,
    TooShort,
}

impl KeyError {
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::TooShort => "too_short",
        }
    }
}

/// An operator-held redaction key. `Debug` never prints the bytes.
#[derive(Clone)]
pub struct RedactionKey(Vec<u8>);

impl std::fmt::Debug for RedactionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedactionKey(<withheld>)")
    }
}

impl RedactionKey {
    /// `None` when the variable is unset (ordinal refs). Set-but-empty and too-short values are
    /// errors, so a misconfigured key cannot quietly fall back.
    pub fn from_env_value(value: Option<OsString>) -> Result<Option<Self>, KeyError> {
        let Some(value) = value else {
            return Ok(None);
        };
        #[cfg(unix)]
        let bytes = std::os::unix::ffi::OsStringExt::into_vec(value);
        #[cfg(not(unix))]
        let bytes = value.to_string_lossy().into_owned().into_bytes();
        if bytes.is_empty() {
            return Err(KeyError::Empty);
        }
        if bytes.len() < MIN_KEY_BYTES {
            return Err(KeyError::TooShort);
        }
        Ok(Some(Self(bytes)))
    }

    pub fn from_env() -> Result<Option<Self>, KeyError> {
        Self::from_env_value(std::env::var_os(ENV_KEY))
    }

    /// `redacted:hmac-sha256:<32 hex>` over the canonical JSON of `value`.
    pub fn marker(&self, value: &Value) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts a key of any length");
        mac.update(canonical(value).as_bytes());
        let digest = mac.finalize().into_bytes();
        let mut hex = String::with_capacity(32);
        for byte in &digest[..16] {
            hex.push_str(&format!("{byte:02x}"));
        }
        format!("{HMAC_PREFIX}{hex}")
    }
}

fn canonical(value: &Value) -> String {
    serde_json::to_string(value).expect("a JSON value serializes")
}

#[derive(Clone, Copy)]
enum Namespace {
    Key,
    Cell,
}

struct Marker<'a> {
    key: Option<&'a RedactionKey>,
    keys: BTreeMap<String, usize>,
    cells: BTreeMap<String, usize>,
}

impl<'a> Marker<'a> {
    fn new(key: Option<&'a RedactionKey>) -> Self {
        Self {
            key,
            keys: BTreeMap::new(),
            cells: BTreeMap::new(),
        }
    }

    fn mark(&mut self, namespace: Namespace, value: &Value) -> Value {
        if let Some(key) = self.key {
            return Value::String(key.marker(value));
        }
        let (table, letter) = match namespace {
            Namespace::Key => (&mut self.keys, 'k'),
            Namespace::Cell => (&mut self.cells, 'v'),
        };
        let next = table.len() + 1;
        let ordinal = *table.entry(canonical(value)).or_insert(next);
        Value::String(format!("{REF_PREFIX}{letter}{ordinal}"))
    }
}

/// Redact a report in place.
///
/// `emitted_per_rule` is how many affected entries per rule the output will actually print
/// (`Some(n)` for human output under `--sample-affected n`, `None` when every entry is
/// printed). Ordinals are numbered over the emitted entries first, so an unprinted row can
/// never shift or reveal anything about a printed ref; entries that are never printed are
/// numbered afterwards.
pub fn redact_report(
    report: &mut VerifyReport,
    key: Option<&RedactionKey>,
    emitted_per_rule: Option<usize>,
) {
    let mut marker = Marker::new(key);
    for pass_emitted in [true, false] {
        for result in &mut report.results {
            let split =
                emitted_per_rule.map_or(result.affected.len(), |n| n.min(result.affected.len()));
            let range = if pass_emitted {
                0..split
            } else {
                split..result.affected.len()
            };
            for entry in &mut result.affected[range] {
                if let Some(entry_key) = entry.key.as_mut() {
                    for value in entry_key.values_mut() {
                        *value = marker.mark(Namespace::Key, value);
                    }
                }
                if let Some(value) = entry.value.as_mut() {
                    *value = marker.mark(Namespace::Cell, value);
                }
            }
        }
    }
    if let Some(refusal) = report.refusal.as_mut() {
        redact_key_objects(&mut refusal.detail, &mut marker);
        // DuckDB's own error text can quote data (a failed conversion names the
        // offending string). The one SQL_ERROR that carries a `column` field names
        // only a type, so it stays readable. Key tuples in details were marked above.
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
fn redact_key_objects(value: &mut Value, marker: &mut Marker<'_>) {
    match value {
        Value::Object(fields) => {
            for (name, child) in fields.iter_mut() {
                if name == "key" {
                    match child {
                        Value::Object(inner) => {
                            for leaf in inner.values_mut() {
                                *leaf = marker.mark(Namespace::Key, leaf);
                            }
                        }
                        Value::Null => {}
                        other => *other = marker.mark(Namespace::Key, other),
                    }
                } else {
                    redact_key_objects(child, marker);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_key_objects(item, marker);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use sha2::{Digest, Sha256};
    use verify_core::{
        constraint::Severity,
        report::{AffectedEntry, ExecutionMode, ResultStatus, RuleResult, VerifyReport},
    };

    use super::*;

    fn entry(key: &str, value: &str) -> AffectedEntry {
        AffectedEntry {
            binding: "loans".to_owned(),
            key: Some(BTreeMap::from([("loan_id".to_owned(), json!(key))])),
            field: Some("balance".to_owned()),
            value: Some(json!(value)),
        }
    }

    fn rule(id: &str, entries: Vec<AffectedEntry>) -> RuleResult {
        RuleResult {
            rule_id: id.to_owned(),
            severity: Severity::Error,
            status: ResultStatus::Fail,
            violation_count: entries.len(),
            affected: entries,
        }
    }

    fn report_with(results: Vec<RuleResult>) -> VerifyReport {
        // A neutral constraint hash, so "no sha256 text anywhere" checks only the redacted values.
        let mut report = VerifyReport::new(ExecutionMode::Batch, "set", "constraint-hash");
        report.results = results;
        report
    }

    fn refs(report: &VerifyReport) -> Vec<(String, String)> {
        report
            .results
            .iter()
            .flat_map(|result| &result.affected)
            .map(|entry| {
                (
                    entry.key.as_ref().unwrap()["loan_id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    entry.value.as_ref().unwrap().as_str().unwrap().to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn ordinals_follow_first_appearance_and_equal_values_share_a_ref() {
        let mut report = report_with(vec![rule(
            "R",
            vec![entry("A", "x"), entry("B", "x"), entry("A", "y")],
        )]);
        redact_report(&mut report, None, None);
        assert_eq!(
            refs(&report),
            vec![
                ("redacted:ref:k1".into(), "redacted:ref:v1".into()),
                ("redacted:ref:k2".into(), "redacted:ref:v1".into()),
                ("redacted:ref:k1".into(), "redacted:ref:v2".into()),
            ]
        );
        let text = serde_json::to_string(&report).unwrap();
        assert!(!text.contains("\"A\"") && !text.contains("\"x\""), "{text}");
    }

    #[test]
    fn keys_and_cells_with_equal_bytes_use_separate_namespaces() {
        let mut report = report_with(vec![rule("R", vec![entry("same", "same")])]);
        redact_report(&mut report, None, None);
        assert_eq!(
            refs(&report),
            vec![("redacted:ref:k1".into(), "redacted:ref:v1".into())]
        );
    }

    #[test]
    fn unemitted_entries_cannot_shift_the_numbering_of_emitted_ones() {
        // Rule 1 prints only its first entry; its other entries hold three more distinct values.
        let build = || {
            report_with(vec![
                rule(
                    "R1",
                    vec![
                        entry("a", "1"),
                        entry("b", "2"),
                        entry("c", "3"),
                        entry("d", "4"),
                    ],
                ),
                rule("R2", vec![entry("z", "9")]),
            ])
        };
        let mut sampled = build();
        redact_report(&mut sampled, None, Some(1));
        let mut only_emitted = report_with(vec![
            rule("R1", vec![entry("a", "1")]),
            rule("R2", vec![entry("z", "9")]),
        ]);
        redact_report(&mut only_emitted, None, None);
        // The emitted entries (R1[0] and R2[0]) carry exactly the refs they would have if the
        // unprinted rows did not exist.
        assert_eq!(
            sampled.results[0].affected[0],
            only_emitted.results[0].affected[0]
        );
        assert_eq!(
            sampled.results[1].affected[0],
            only_emitted.results[1].affected[0]
        );
        assert_eq!(
            sampled.results[1].affected[0].key.as_ref().unwrap()["loan_id"],
            json!("redacted:ref:k2")
        );
    }

    #[test]
    fn default_output_contains_no_value_derived_hash() {
        let mut report = report_with(vec![rule("R", vec![entry("CANARY-KEY", "CANARY-CELL")])]);
        redact_report(&mut report, None, None);
        let text = serde_json::to_string(&report).unwrap();
        for canary in ["CANARY-KEY", "CANARY-CELL"] {
            let old = format!("{:x}", Sha256::digest(canonical(&json!(canary)).as_bytes()));
            assert!(!text.contains(&old[..16]) && !text.contains(&old), "{text}");
        }
        assert!(!text.contains("sha256"), "{text}");
        assert!(!text.contains("hmac"), "{text}");
    }

    #[test]
    fn key_validation_refuses_empty_and_short_and_accepts_16() {
        assert!(RedactionKey::from_env_value(None).unwrap().is_none());
        assert_eq!(
            RedactionKey::from_env_value(Some("".into())).unwrap_err(),
            KeyError::Empty
        );
        assert_eq!(
            RedactionKey::from_env_value(Some("0123456789abcde".into())).unwrap_err(),
            KeyError::TooShort
        );
        assert!(
            RedactionKey::from_env_value(Some("0123456789abcdef".into()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn keyed_markers_match_the_independent_known_answer_and_hide_the_key() {
        // Cross-checked with: printf '%s' '"CANARY-CELL-77c2d4"' | openssl dgst -sha256 -hmac 0123456789abcdef
        //   f90d3ee3380b0597dd4632a72c95605640d12893452bc855d916768ecd90b575
        let key = RedactionKey::from_env_value(Some("0123456789abcdef".into()))
            .unwrap()
            .unwrap();
        assert_eq!(
            key.marker(&json!("CANARY-CELL-77c2d4")),
            "redacted:hmac-sha256:f90d3ee3380b0597dd4632a72c956056"
        );
        let other = RedactionKey::from_env_value(Some("0123456789abcdeg".into()))
            .unwrap()
            .unwrap();
        assert_ne!(key.marker(&json!("v")), other.marker(&json!("v")));
        assert_eq!(key.marker(&json!("v")), key.marker(&json!("v")));
        assert!(!format!("{key:?}").contains("0123456789abcdef"));
    }

    #[test]
    fn keyed_mode_replaces_ordinals_and_is_stable_across_reports() {
        let key = RedactionKey::from_env_value(Some("0123456789abcdef".into()))
            .unwrap()
            .unwrap();
        let mut first = report_with(vec![rule("R", vec![entry("A", "x")])]);
        let mut second = report_with(vec![rule("R", vec![entry("B", "q"), entry("A", "x")])]);
        redact_report(&mut first, Some(&key), None);
        redact_report(&mut second, Some(&key), None);
        assert_eq!(first.results[0].affected[0], second.results[0].affected[1]);
        let text = serde_json::to_string(&first).unwrap();
        assert!(
            text.contains(HMAC_PREFIX) && !text.contains(REF_PREFIX),
            "{text}"
        );
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
        redact_report(&mut report, None, None);
        let text = serde_json::to_string(&report).unwrap();
        assert!(!text.contains("CANARY"), "{text}");
        assert!(
            text.contains("R1") && text.contains("redacted:ref:k1"),
            "{text}"
        );
    }
}
