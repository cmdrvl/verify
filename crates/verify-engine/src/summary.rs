use verify_core::{
    constraint::Severity,
    report::{
        PolicySignals, ResultStatus, RuleResult, SeverityBand, SeveritySummary, Summary,
        VerifyReport,
    },
};

pub const SEVERITY_BANDS: &[SeverityBand] = &[
    SeverityBand::Clean,
    SeverityBand::WarnOnly,
    SeverityBand::ErrorPresent,
];

#[derive(Debug, Clone, Default)]
pub struct SummaryEngine;

impl SummaryEngine {
    pub fn summarize(results: &[RuleResult]) -> Summary {
        summarize_results(results)
    }

    pub fn derive_policy_signals(results: &[RuleResult]) -> PolicySignals {
        derive_policy_signals(results)
    }

    pub fn apply(report: &mut VerifyReport) {
        report.summary = summarize_results(&report.results);
        report.policy_signals = derive_policy_signals(&report.results);
    }
}

pub fn summarize_results(results: &[RuleResult]) -> Summary {
    let mut summary = Summary {
        total_rules: results.len(),
        passed_rules: 0,
        failed_rules: 0,
        by_severity: SeveritySummary::default(),
    };

    for result in results {
        match result.status {
            ResultStatus::Pass => {
                summary.passed_rules += 1;
            }
            ResultStatus::Fail => {
                summary.failed_rules += 1;
                match result.severity {
                    Severity::Error => summary.by_severity.error += 1,
                    Severity::Warn => summary.by_severity.warn += 1,
                }
            }
        }
    }

    summary
}

pub fn derive_policy_signals(results: &[RuleResult]) -> PolicySignals {
    let severity_band = if results
        .iter()
        .any(|result| matches!(result.status, ResultStatus::Fail))
    {
        if results.iter().any(|result| {
            matches!(result.status, ResultStatus::Fail)
                && matches!(result.severity, Severity::Error)
        }) {
            SeverityBand::ErrorPresent
        } else {
            SeverityBand::WarnOnly
        }
    } else {
        SeverityBand::Clean
    };

    PolicySignals { severity_band }
}

#[cfg(test)]
mod tests {
    use verify_core::{
        constraint::Severity,
        report::{
            ExecutionMode, PolicySignals, ResultStatus, RuleResult, SeverityBand, Summary,
            VerifyReport,
        },
    };

    use super::{SEVERITY_BANDS, SummaryEngine, derive_policy_signals, summarize_results};

    fn rule_result(rule_id: &str, severity: Severity, status: ResultStatus) -> RuleResult {
        RuleResult {
            rule_id: rule_id.to_owned(),
            severity,
            status,
            violation_count: usize::from(matches!(status, ResultStatus::Fail)),
            affected: Vec::new(),
        }
    }

    #[test]
    fn summary_counts_passes_and_failures() {
        let summary = summarize_results(&[
            rule_result("A_RULE", Severity::Error, ResultStatus::Pass),
            rule_result("B_RULE", Severity::Warn, ResultStatus::Fail),
            rule_result("C_RULE", Severity::Error, ResultStatus::Fail),
        ]);

        assert_eq!(
            summary,
            Summary {
                total_rules: 3,
                passed_rules: 1,
                failed_rules: 2,
                by_severity: verify_core::report::SeveritySummary { error: 1, warn: 1 },
            }
        );
    }

    #[test]
    fn summary_ignores_passed_rule_severity_counts() {
        let summary = summarize_results(&[
            rule_result("A_RULE", Severity::Error, ResultStatus::Pass),
            rule_result("B_RULE", Severity::Warn, ResultStatus::Pass),
        ]);

        assert_eq!(summary.by_severity.error, 0);
        assert_eq!(summary.by_severity.warn, 0);
    }

    #[test]
    fn severity_band_is_clean_when_nothing_fails() {
        assert_eq!(
            derive_policy_signals(&[
                rule_result("A_RULE", Severity::Error, ResultStatus::Pass),
                rule_result("B_RULE", Severity::Warn, ResultStatus::Pass),
            ]),
            PolicySignals {
                severity_band: SeverityBand::Clean,
            }
        );
    }

    #[test]
    fn severity_band_is_warn_only_when_all_failures_are_warn() {
        assert_eq!(
            derive_policy_signals(&[
                rule_result("A_RULE", Severity::Warn, ResultStatus::Fail),
                rule_result("B_RULE", Severity::Warn, ResultStatus::Pass),
            ]),
            PolicySignals {
                severity_band: SeverityBand::WarnOnly,
            }
        );
    }

    #[test]
    fn severity_band_is_error_present_when_any_error_fails() {
        assert_eq!(
            derive_policy_signals(&[
                rule_result("A_RULE", Severity::Warn, ResultStatus::Fail),
                rule_result("B_RULE", Severity::Error, ResultStatus::Fail),
            ]),
            PolicySignals {
                severity_band: SeverityBand::ErrorPresent,
            }
        );
    }

    #[test]
    fn engine_applies_summary_and_policy_signals_to_report() {
        let mut report = VerifyReport::new(
            ExecutionMode::Batch,
            "fixtures.arity1.not_null_loans",
            "sha256:constraint",
        );
        report.results = vec![
            rule_result("A_RULE", Severity::Error, ResultStatus::Pass),
            rule_result("B_RULE", Severity::Warn, ResultStatus::Fail),
        ];

        SummaryEngine::apply(&mut report);

        assert_eq!(report.summary.total_rules, 2);
        assert_eq!(report.summary.failed_rules, 1);
        assert!(matches!(
            report.policy_signals.severity_band,
            SeverityBand::WarnOnly
        ));
    }

    #[test]
    fn exported_severity_bands_match_protocol_order() {
        assert_eq!(
            SEVERITY_BANDS,
            &[
                SeverityBand::Clean,
                SeverityBand::WarnOnly,
                SeverityBand::ErrorPresent,
            ]
        );
    }
}
