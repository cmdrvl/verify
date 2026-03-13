use std::fmt::Write;

use serde_json::Value;
use verify_core::{
    refusal::RefusalCode,
    report::{Outcome, ResultStatus, VerifyReport},
};

pub fn scaffold_message(surface: &str) -> String {
    format!("verify scaffold only: {surface} is not implemented yet")
}

/// Render a `VerifyReport` as compact human-readable text.
///
/// Format matches plan.md §Output (human):
///
/// ```text
/// VERIFY FAIL
/// constraint_set: loan_tape.monthly.v1
/// binding: input=tape.csv
/// passed_rules: 2
/// failed_rules: 1
/// severity_band: ERROR_PRESENT
///
/// FAIL POSITIVE_BALANCE binding=input key.loan_id=LN-00421 field=balance value=-500.0
/// ```
pub fn render_report(report: &VerifyReport, sample_affected: Option<usize>) -> String {
    match report.outcome {
        Outcome::Refusal => render_refusal(report),
        _ => render_pass_or_fail(report, sample_affected),
    }
}

fn render_pass_or_fail(report: &VerifyReport, sample_affected: Option<usize>) -> String {
    let mut out = String::new();

    // Header line
    let outcome = outcome_label(report.outcome);
    writeln!(out, "VERIFY {outcome}").unwrap();

    // Summary section
    writeln!(out, "constraint_set: {}", report.constraint_set_id).unwrap();

    for (name, binding) in &report.bindings {
        writeln!(out, "binding: {name}={}", binding.source).unwrap();
    }

    writeln!(out, "passed_rules: {}", report.summary.passed_rules).unwrap();
    writeln!(out, "failed_rules: {}", report.summary.failed_rules).unwrap();
    writeln!(
        out,
        "severity_band: {}",
        severity_band_label(&report.policy_signals.severity_band)
    )
    .unwrap();

    // Failed rule detail lines
    let failed_results: Vec<_> = report
        .results
        .iter()
        .filter(|r| matches!(r.status, ResultStatus::Fail))
        .collect();

    if !failed_results.is_empty() {
        out.push('\n');
        for result in failed_results {
            if result.affected.is_empty() {
                writeln!(out, "FAIL {}", result.rule_id).unwrap();
            } else {
                for affected in affected_preview(&result.affected, sample_affected) {
                    let mut detail =
                        format!("FAIL {} binding={}", result.rule_id, affected.binding);

                    if let Some(key) = &affected.key {
                        for (field, value) in key {
                            write!(detail, " key.{field}={}", display_value(value)).unwrap();
                        }
                    }

                    if let Some(field) = &affected.field {
                        write!(detail, " field={field}").unwrap();
                    }

                    if let Some(value) = &affected.value {
                        write!(detail, " value={}", display_value(value)).unwrap();
                    }

                    writeln!(out, "{detail}").unwrap();
                }

                if let Some(limit) = sample_affected {
                    let remaining = result.affected.len().saturating_sub(limit);
                    if remaining > 0 {
                        let noun = if remaining == 1 {
                            "affected entry"
                        } else {
                            "affected entries"
                        };
                        writeln!(
                            out,
                            "preview: {remaining} more {noun} not shown for {}",
                            result.rule_id
                        )
                        .unwrap();
                    }
                }
            }
        }
    }

    // Trim trailing newline for clean output
    while out.ends_with('\n') {
        out.pop();
    }

    out
}

fn affected_preview(
    affected: &[verify_core::report::AffectedEntry],
    sample_affected: Option<usize>,
) -> &[verify_core::report::AffectedEntry] {
    match sample_affected {
        Some(limit) => {
            let end = affected.len().min(limit);
            &affected[..end]
        }
        None => affected,
    }
}

fn render_refusal(report: &VerifyReport) -> String {
    let mut out = String::new();

    writeln!(out, "VERIFY REFUSAL").unwrap();
    writeln!(out, "constraint_set: {}", report.constraint_set_id).unwrap();

    if let Some(refusal) = &report.refusal {
        writeln!(
            out,
            "{}: {}",
            refusal_code_label(&refusal.code),
            refusal.message
        )
        .unwrap();
        writeln!(out, "next_step: {}", refusal.next_step).unwrap();
    }

    while out.ends_with('\n') {
        out.pop();
    }

    out
}

fn outcome_label(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Pass => "PASS",
        Outcome::Fail => "FAIL",
        Outcome::Refusal => "REFUSAL",
    }
}

fn severity_band_label(band: &verify_core::report::SeverityBand) -> &'static str {
    match band {
        verify_core::report::SeverityBand::Clean => "CLEAN",
        verify_core::report::SeverityBand::WarnOnly => "WARN_ONLY",
        verify_core::report::SeverityBand::ErrorPresent => "ERROR_PRESENT",
    }
}

fn refusal_code_label(code: &RefusalCode) -> String {
    serde_json::to_string(code)
        .expect("refusal code should serialize")
        .trim_matches('"')
        .to_owned()
}

/// Format a JSON value for compact human display.
fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use verify_core::{
        constraint::{BindingKind, Severity},
        refusal::RefusalCode,
        report::{
            AffectedEntry, BindingReport, ExecutionMode, Outcome, PolicySignals, ResultStatus,
            RuleResult, SeverityBand, SeveritySummary, Summary, VerifyReport,
        },
    };

    use super::{render_report, scaffold_message};

    #[test]
    fn scaffold_message_uses_refusal_text() {
        let message = scaffold_message("validate compiled constraints");
        assert!(message.contains("verify scaffold only"));
        assert!(message.contains("validate compiled constraints"));
    }

    fn pass_report() -> VerifyReport {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "input".to_owned(),
            BindingReport {
                kind: BindingKind::Relation,
                source: "tape.csv".to_owned(),
                content_hash: "sha256:input".to_owned(),
                input_verification: None,
            },
        );

        VerifyReport {
            outcome: Outcome::Pass,
            constraint_set_id: "loan_tape.monthly.v1".to_owned(),
            bindings,
            summary: Summary {
                total_rules: 1,
                passed_rules: 1,
                failed_rules: 0,
                by_severity: SeveritySummary::default(),
            },
            policy_signals: PolicySignals {
                severity_band: SeverityBand::Clean,
            },
            results: vec![RuleResult {
                rule_id: "LOAN_ID_PRESENT".to_owned(),
                severity: Severity::Error,
                status: ResultStatus::Pass,
                violation_count: 0,
                affected: Vec::new(),
            }],
            ..VerifyReport::new(ExecutionMode::Batch, "loan_tape.monthly.v1", "sha256:c")
        }
    }

    fn fail_report() -> VerifyReport {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "input".to_owned(),
            BindingReport {
                kind: BindingKind::Relation,
                source: "tape.csv".to_owned(),
                content_hash: "sha256:input".to_owned(),
                input_verification: None,
            },
        );

        VerifyReport {
            outcome: Outcome::Fail,
            constraint_set_id: "loan_tape.monthly.v1".to_owned(),
            bindings,
            summary: Summary {
                total_rules: 3,
                passed_rules: 2,
                failed_rules: 1,
                by_severity: SeveritySummary { error: 1, warn: 0 },
            },
            policy_signals: PolicySignals {
                severity_band: SeverityBand::ErrorPresent,
            },
            results: vec![RuleResult {
                rule_id: "POSITIVE_BALANCE".to_owned(),
                severity: Severity::Error,
                status: ResultStatus::Fail,
                violation_count: 1,
                affected: vec![AffectedEntry {
                    binding: "input".to_owned(),
                    key: Some(BTreeMap::from([("loan_id".to_owned(), json!("LN-00421"))])),
                    field: Some("balance".to_owned()),
                    value: Some(json!(-500.0)),
                }],
            }],
            ..VerifyReport::new(ExecutionMode::Batch, "loan_tape.monthly.v1", "sha256:c")
        }
    }

    #[test]
    fn pass_report_renders_clean_summary() {
        let rendered = render_report(&pass_report(), None);

        assert!(rendered.starts_with("VERIFY PASS\n"));
        assert!(rendered.contains("constraint_set: loan_tape.monthly.v1"));
        assert!(rendered.contains("binding: input=tape.csv"));
        assert!(rendered.contains("passed_rules: 1"));
        assert!(rendered.contains("failed_rules: 0"));
        assert!(rendered.contains("severity_band: CLEAN"));
        // No failure detail lines for passing reports
        assert!(!rendered.contains("FAIL "));
    }

    #[test]
    fn fail_report_renders_localized_violations() {
        let rendered = render_report(&fail_report(), None);

        assert!(rendered.starts_with("VERIFY FAIL\n"));
        assert!(rendered.contains("constraint_set: loan_tape.monthly.v1"));
        assert!(rendered.contains("passed_rules: 2"));
        assert!(rendered.contains("failed_rules: 1"));
        assert!(rendered.contains("severity_band: ERROR_PRESENT"));

        // Failure detail line with full localization
        assert!(rendered.contains(
            "FAIL POSITIVE_BALANCE binding=input key.loan_id=LN-00421 field=balance value=-500"
        ));
    }

    #[test]
    fn fail_report_matches_plan_format() {
        let rendered = render_report(&fail_report(), None);
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines[0], "VERIFY FAIL");
        assert_eq!(lines[1], "constraint_set: loan_tape.monthly.v1");
        assert_eq!(lines[2], "binding: input=tape.csv");
        assert_eq!(lines[3], "passed_rules: 2");
        assert_eq!(lines[4], "failed_rules: 1");
        assert_eq!(lines[5], "severity_band: ERROR_PRESENT");
        assert_eq!(lines[6], ""); // blank separator
        assert!(lines[7].starts_with("FAIL POSITIVE_BALANCE binding=input"));
    }

    #[test]
    fn refusal_report_renders_code_and_message() {
        let report = VerifyReport::refusal(
            ExecutionMode::Batch,
            "fixtures.refusals.missing_binding",
            "sha256:d",
            RefusalCode::MissingBinding,
            "Constraint set expects binding property, but no input was supplied for it.",
            json!({"binding": "property"}),
        );

        let rendered = render_report(&report, None);

        assert!(rendered.starts_with("VERIFY REFUSAL\n"));
        assert!(rendered.contains("E_MISSING_BINDING:"));
        assert!(rendered.contains("Constraint set expects binding property"));
        assert!(rendered.contains("next_step:"));
    }

    #[test]
    fn multiple_bindings_each_appear() {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "property".to_owned(),
            BindingReport {
                kind: BindingKind::Relation,
                source: "property.csv".to_owned(),
                content_hash: "sha256:p".to_owned(),
                input_verification: None,
            },
        );
        bindings.insert(
            "tenants".to_owned(),
            BindingReport {
                kind: BindingKind::Relation,
                source: "tenants.csv".to_owned(),
                content_hash: "sha256:t".to_owned(),
                input_verification: None,
            },
        );

        let report = VerifyReport {
            bindings,
            ..pass_report()
        };

        let rendered = render_report(&report, None);
        assert!(rendered.contains("binding: property=property.csv"));
        assert!(rendered.contains("binding: tenants=tenants.csv"));
    }

    #[test]
    fn multiple_affected_entries_each_get_a_line() {
        let report = VerifyReport {
            outcome: Outcome::Fail,
            summary: Summary {
                total_rules: 1,
                passed_rules: 0,
                failed_rules: 1,
                by_severity: SeveritySummary { error: 1, warn: 0 },
            },
            policy_signals: PolicySignals {
                severity_band: SeverityBand::ErrorPresent,
            },
            results: vec![RuleResult {
                rule_id: "LOAN_ID_PRESENT".to_owned(),
                severity: Severity::Error,
                status: ResultStatus::Fail,
                violation_count: 2,
                affected: vec![
                    AffectedEntry {
                        binding: "input".to_owned(),
                        key: Some(BTreeMap::from([("row".to_owned(), json!(1))])),
                        field: Some("loan_id".to_owned()),
                        value: Some(json!(null)),
                    },
                    AffectedEntry {
                        binding: "input".to_owned(),
                        key: Some(BTreeMap::from([("row".to_owned(), json!(3))])),
                        field: Some("loan_id".to_owned()),
                        value: Some(json!("")),
                    },
                ],
            }],
            ..pass_report()
        };

        let rendered = render_report(&report, None);
        let fail_lines: Vec<&str> = rendered
            .lines()
            .filter(|l| l.starts_with("FAIL "))
            .collect();
        assert_eq!(fail_lines.len(), 2);
    }

    #[test]
    fn sample_affected_limits_preview_and_preserves_localization_shapes() {
        let report = VerifyReport {
            outcome: Outcome::Fail,
            summary: Summary {
                total_rules: 4,
                passed_rules: 0,
                failed_rules: 4,
                by_severity: SeveritySummary { error: 4, warn: 0 },
            },
            policy_signals: PolicySignals {
                severity_band: SeverityBand::ErrorPresent,
            },
            results: vec![
                RuleResult {
                    rule_id: "UNIQUE_LOAN_ID".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 2,
                    affected: vec![
                        AffectedEntry {
                            binding: "input".to_owned(),
                            key: Some(BTreeMap::from([("row".to_owned(), json!(1))])),
                            field: Some("loan_id".to_owned()),
                            value: Some(json!("LN-100")),
                        },
                        AffectedEntry {
                            binding: "input".to_owned(),
                            key: Some(BTreeMap::from([("row".to_owned(), json!(3))])),
                            field: Some("loan_id".to_owned()),
                            value: Some(json!("LN-100")),
                        },
                    ],
                },
                RuleResult {
                    rule_id: "INPUT_LOAN_ID_PRESENT".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 1,
                    affected: vec![AffectedEntry {
                        binding: "input".to_owned(),
                        key: Some(BTreeMap::from([("loan_id".to_owned(), json!("LN-42"))])),
                        field: Some("loan_id".to_owned()),
                        value: Some(json!(null)),
                    }],
                },
                RuleResult {
                    rule_id: "POSITIVE_BALANCE".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 1,
                    affected: vec![AffectedEntry {
                        binding: "input".to_owned(),
                        key: Some(BTreeMap::from([("loan_id".to_owned(), json!("LN-55"))])),
                        field: Some("balance".to_owned()),
                        value: Some(json!(-500.0)),
                    }],
                },
                RuleResult {
                    rule_id: "NO_ORPHAN_TENANTS".to_owned(),
                    severity: Severity::Error,
                    status: ResultStatus::Fail,
                    violation_count: 1,
                    affected: vec![AffectedEntry {
                        binding: "property".to_owned(),
                        key: Some(BTreeMap::from([("property_id".to_owned(), json!("P-1"))])),
                        field: None,
                        value: Some(json!("T-999")),
                    }],
                },
            ],
            ..pass_report()
        };

        let rendered = render_report(&report, Some(1));
        let lines: Vec<&str> = rendered.lines().collect();

        assert!(lines.iter().any(|line| {
            *line == "FAIL UNIQUE_LOAN_ID binding=input key.row=1 field=loan_id value=LN-100"
        }));
        assert!(lines.iter().any(|line| {
            *line == "preview: 1 more affected entry not shown for UNIQUE_LOAN_ID"
        }));
        assert!(lines.iter().any(|line| {
            *line
                == "FAIL INPUT_LOAN_ID_PRESENT binding=input key.loan_id=LN-42 field=loan_id value=null"
        }));
        assert!(lines.iter().any(|line| {
            *line
                == "FAIL POSITIVE_BALANCE binding=input key.loan_id=LN-55 field=balance value=-500.0"
        }));
        assert!(lines.iter().any(|line| {
            *line == "FAIL NO_ORPHAN_TENANTS binding=property key.property_id=P-1 value=T-999"
        }));
    }

    #[test]
    fn display_value_formats_types_cleanly() {
        use super::display_value;

        assert_eq!(display_value(&json!(null)), "null");
        assert_eq!(display_value(&json!(true)), "true");
        assert_eq!(display_value(&json!(42)), "42");
        assert_eq!(display_value(&json!(-500.0)), "-500.0");
        assert_eq!(display_value(&json!("hello")), "hello");
    }
}
