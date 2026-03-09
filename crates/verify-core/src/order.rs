use std::cmp::Ordering;

use serde::Serialize;
use serde_json::Value;

use crate::{
    constraint::{Binding, Rule},
    report::{AffectedEntry, RuleResult, VerifyReport},
};

pub fn cmp_option_str(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub fn sort_strings(values: &mut [String]) {
    values.sort();
}

pub fn sort_bindings(bindings: &mut [Binding]) {
    bindings.sort_by(|left, right| left.name.cmp(&right.name));
}

pub fn sort_rules(rules: &mut [Rule]) {
    rules.sort_by(|left, right| left.id.cmp(&right.id));
}

pub fn sort_affected_entries(entries: &mut [AffectedEntry]) {
    entries.sort_by(cmp_affected_entries);
}

pub fn sort_rule_results(results: &mut [RuleResult]) {
    for result in results.iter_mut() {
        sort_affected_entries(&mut result.affected);
    }
    results.sort_by(|left, right| left.rule_id.cmp(&right.rule_id));
}

pub fn sort_report(report: &mut VerifyReport) {
    sort_rule_results(&mut report.results);
}

pub fn canonical_json_string<T>(value: &T) -> serde_json::Result<String>
where
    T: Serialize,
{
    serde_json::to_string(value)
}

pub fn canonical_json_bytes<T>(value: &T) -> serde_json::Result<Vec<u8>>
where
    T: Serialize,
{
    serde_json::to_vec(value)
}

fn cmp_affected_entries(left: &AffectedEntry, right: &AffectedEntry) -> Ordering {
    left.binding
        .cmp(&right.binding)
        .then_with(|| cmp_key_maps(left.key.as_ref(), right.key.as_ref()))
        .then_with(|| cmp_option_str(left.field.as_deref(), right.field.as_deref()))
        .then_with(|| cmp_json_values(left.value.as_ref(), right.value.as_ref()))
}

fn cmp_key_maps(
    left: Option<&std::collections::BTreeMap<String, Value>>,
    right: Option<&std::collections::BTreeMap<String, Value>>,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => {
            for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right.iter())
            {
                let ordering = left_key
                    .cmp(right_key)
                    .then_with(|| cmp_json_values(Some(left_value), Some(right_value)));
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn cmp_json_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.to_string().cmp(&right.to_string()),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{canonical_json_string, sort_bindings, sort_report, sort_rules};
    use crate::{
        REPORT_VERSION, TOOL_NAME,
        constraint::{Binding, BindingKind, Check, ConstraintSet, Portability, Rule, Severity},
        report::{
            AffectedEntry, ExecutionMode, Outcome, PolicySignals, ResultStatus, RuleResult,
            SeverityBand, SeveritySummary, Summary, VerifyReport,
        },
    };

    #[test]
    fn sorts_constraint_artifacts_by_declared_identity() {
        let mut constraint_set = ConstraintSet::new("loan_tape.monthly.v1");
        constraint_set.bindings = vec![
            Binding {
                name: "zeta".to_owned(),
                kind: BindingKind::Relation,
                key_fields: Vec::new(),
            },
            Binding {
                name: "alpha".to_owned(),
                kind: BindingKind::Relation,
                key_fields: vec!["loan_id".to_owned()],
            },
        ];
        constraint_set.rules = vec![
            Rule {
                id: "Z_RULE".to_owned(),
                severity: Severity::Warn,
                portability: Portability::Portable,
                check: Check::RowCount {
                    binding: "zeta".to_owned(),
                    compare: crate::constraint::Comparison {
                        gte: Some(json!(1)),
                        ..Default::default()
                    },
                },
            },
            Rule {
                id: "A_RULE".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::NotNull {
                    binding: "alpha".to_owned(),
                    columns: vec!["loan_id".to_owned()],
                },
            },
        ];

        sort_bindings(&mut constraint_set.bindings);
        sort_rules(&mut constraint_set.rules);

        assert_eq!(constraint_set.bindings[0].name, "alpha");
        assert_eq!(constraint_set.rules[0].id, "A_RULE");
    }

    #[test]
    fn sorts_rule_results_and_localized_failures_deterministically() {
        let mut report = VerifyReport {
            tool: TOOL_NAME.to_owned(),
            version: REPORT_VERSION.to_owned(),
            execution_mode: ExecutionMode::Batch,
            outcome: Outcome::Fail,
            constraint_set_id: "loan_tape.monthly.v1".to_owned(),
            constraint_hash: "sha256:constraint".to_owned(),
            bindings: BTreeMap::new(),
            summary: Summary {
                total_rules: 2,
                passed_rules: 0,
                failed_rules: 2,
                by_severity: SeveritySummary { error: 2, warn: 0 },
            },
            policy_signals: PolicySignals {
                severity_band: SeverityBand::ErrorPresent,
            },
            results: vec![
                RuleResult {
                    rule_id: "Z_RULE".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 2,
                    affected: vec![
                        AffectedEntry {
                            binding: "input".to_owned(),
                            key: Some(BTreeMap::from([("loan_id".to_owned(), json!("LN-200"))])),
                            field: Some("balance".to_owned()),
                            value: Some(json!(2)),
                        },
                        AffectedEntry {
                            binding: "input".to_owned(),
                            key: Some(BTreeMap::from([("loan_id".to_owned(), json!("LN-100"))])),
                            field: Some("balance".to_owned()),
                            value: Some(json!(1)),
                        },
                    ],
                },
                RuleResult {
                    rule_id: "A_RULE".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 1,
                    affected: vec![AffectedEntry {
                        binding: "input".to_owned(),
                        key: None,
                        field: Some("loan_id".to_owned()),
                        value: Some(json!(null)),
                    }],
                },
            ],
            refusal: None,
        };

        sort_report(&mut report);

        assert_eq!(report.results[0].rule_id, "A_RULE");
        assert_eq!(
            report.results[1].affected[0]
                .key
                .as_ref()
                .expect("key present")
                .get("loan_id"),
            Some(&json!("LN-100"))
        );
    }

    #[test]
    fn canonical_json_helpers_are_stable() {
        let report = VerifyReport::new(ExecutionMode::Batch, "loan_tape.monthly.v1", "sha256:abc");
        let first = canonical_json_string(&report).expect("first serialization");
        let second = canonical_json_string(&report).expect("second serialization");

        assert_eq!(first, second);
    }
}
