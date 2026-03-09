use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    REPORT_VERSION, TOOL_NAME,
    constraint::{BindingKind, Severity},
    refusal::{Refusal, RefusalCode},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyReport {
    pub tool: String,
    pub version: String,
    pub execution_mode: ExecutionMode,
    pub outcome: Outcome,
    pub constraint_set_id: String,
    pub constraint_hash: String,
    #[serde(default)]
    pub bindings: BTreeMap<String, BindingReport>,
    pub summary: Summary,
    pub policy_signals: PolicySignals,
    #[serde(default)]
    pub results: Vec<RuleResult>,
    #[serde(default)]
    pub refusal: Option<Refusal>,
}

impl VerifyReport {
    pub fn new(
        execution_mode: ExecutionMode,
        constraint_set_id: impl Into<String>,
        constraint_hash: impl Into<String>,
    ) -> Self {
        Self {
            tool: TOOL_NAME.to_owned(),
            version: REPORT_VERSION.to_owned(),
            execution_mode,
            outcome: Outcome::Pass,
            constraint_set_id: constraint_set_id.into(),
            constraint_hash: constraint_hash.into(),
            bindings: BTreeMap::new(),
            summary: Summary::default(),
            policy_signals: PolicySignals::default(),
            results: Vec::new(),
            refusal: None,
        }
    }

    pub fn refusal(
        execution_mode: ExecutionMode,
        constraint_set_id: impl Into<String>,
        constraint_hash: impl Into<String>,
        code: RefusalCode,
        message: impl Into<String>,
        detail: Value,
    ) -> Self {
        Self {
            outcome: Outcome::Refusal,
            refusal: Some(Refusal::new(code, message, detail)),
            ..Self::new(execution_mode, constraint_set_id, constraint_hash)
        }
    }
}

impl Default for VerifyReport {
    fn default() -> Self {
        Self::new(ExecutionMode::Batch, String::new(), String::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Batch,
    Embedded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Outcome {
    Pass,
    Fail,
    Refusal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingReport {
    pub kind: BindingKind,
    pub source: String,
    pub content_hash: String,
    #[serde(default)]
    pub input_verification: Option<InputVerification>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputVerification {
    pub status: InputVerificationStatus,
    #[serde(default)]
    pub locks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InputVerificationStatus {
    Verified,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleResult {
    pub rule_id: String,
    pub severity: Severity,
    pub status: ResultStatus,
    pub violation_count: usize,
    #[serde(default)]
    pub affected: Vec<AffectedEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AffectedEntry {
    pub binding: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub total_rules: usize,
    pub passed_rules: usize,
    pub failed_rules: usize,
    pub by_severity: SeveritySummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeveritySummary {
    pub error: usize,
    pub warn: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySignals {
    pub severity_band: SeverityBand,
}

impl Default for PolicySignals {
    fn default() -> Self {
        Self {
            severity_band: SeverityBand::Clean,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SeverityBand {
    Clean,
    WarnOnly,
    ErrorPresent,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        AffectedEntry, BindingReport, ExecutionMode, InputVerification, InputVerificationStatus,
        Outcome, PolicySignals, ResultStatus, RuleResult, SeverityBand, Summary, VerifyReport,
    };
    use crate::{
        REPORT_VERSION, TOOL_NAME,
        constraint::{BindingKind, Severity},
        refusal::RefusalCode,
    };

    #[test]
    fn report_round_trips() {
        let report = json!({
            "tool": TOOL_NAME,
            "version": REPORT_VERSION,
            "execution_mode": "batch",
            "outcome": "FAIL",
            "constraint_set_id": "loan_tape.monthly.v1",
            "constraint_hash": "sha256:constraint",
            "bindings": {
                "input": {
                    "kind": "relation",
                    "source": "tape.csv",
                    "content_hash": "sha256:input",
                    "input_verification": {
                        "status": "VERIFIED",
                        "locks": ["dec.lock.json"]
                    }
                }
            },
            "summary": {
                "total_rules": 3,
                "passed_rules": 2,
                "failed_rules": 1,
                "by_severity": {
                    "error": 1,
                    "warn": 0
                }
            },
            "policy_signals": {
                "severity_band": "ERROR_PRESENT"
            },
            "results": [
                {
                    "rule_id": "POSITIVE_BALANCE",
                    "severity": "error",
                    "status": "fail",
                    "violation_count": 1,
                    "affected": [
                        {
                            "binding": "input",
                            "key": {
                                "loan_id": "LN-00421"
                            },
                            "field": "balance",
                            "value": -500.0
                        }
                    ]
                }
            ],
            "refusal": null
        });

        let parsed: VerifyReport =
            serde_json::from_value(report.clone()).expect("report parses successfully");
        let round_tripped = serde_json::to_value(parsed).expect("report serializes again");

        assert_eq!(round_tripped, report);
    }

    #[test]
    fn refusal_builder_produces_spec_shape() {
        let refusal = VerifyReport::refusal(
            ExecutionMode::Embedded,
            "embedded.constraint.v1",
            "sha256:embedded",
            RefusalCode::BatchOnlyRule,
            "Embedded execution cannot evaluate batch-only rules",
            json!({
                "rule_id": "QUERY_ASSERTION",
                "binding": "input",
            }),
        );

        assert_eq!(refusal.tool, TOOL_NAME);
        assert_eq!(refusal.version, REPORT_VERSION);
        assert!(matches!(refusal.outcome, Outcome::Refusal));
        assert!(matches!(
            refusal.policy_signals.severity_band,
            SeverityBand::Clean
        ));
        assert_eq!(refusal.summary.failed_rules, 0);
        assert_eq!(
            refusal.refusal.expect("refusal payload").next_step,
            "Lower the rule or run in batch mode."
        );
    }

    #[test]
    fn report_structs_capture_binding_and_result_details() {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "input".to_owned(),
            BindingReport {
                kind: BindingKind::Relation,
                source: "tape.csv".to_owned(),
                content_hash: "sha256:input".to_owned(),
                input_verification: Some(InputVerification {
                    status: InputVerificationStatus::Verified,
                    locks: vec!["dec.lock.json".to_owned()],
                }),
            },
        );

        let report = VerifyReport {
            tool: TOOL_NAME.to_owned(),
            version: REPORT_VERSION.to_owned(),
            execution_mode: ExecutionMode::Batch,
            outcome: Outcome::Fail,
            constraint_set_id: "loan_tape.monthly.v1".to_owned(),
            constraint_hash: "sha256:constraint".to_owned(),
            bindings,
            summary: Summary {
                total_rules: 1,
                passed_rules: 0,
                failed_rules: 1,
                by_severity: crate::report::SeveritySummary { error: 1, warn: 0 },
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
            refusal: None,
        };

        assert!(matches!(report.outcome, Outcome::Fail));
        assert_eq!(report.bindings.len(), 1);
        assert_eq!(report.results[0].violation_count, 1);
        assert!(matches!(report.results[0].status, ResultStatus::Fail));
    }

    #[test]
    fn seeded_report_fixtures_round_trip() {
        const PASS_REPORT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/reports/pass/arity1_not_null.pass.json"
        ));
        const FAIL_REPORT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/reports/fail/arity1_not_null.fail.json"
        ));
        const QUERY_REPORT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/reports/query_localization/orphan_rows.fail.json"
        ));
        const REFUSAL_REPORT: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/reports/refusal/bad_authoring.refusal.json"
        ));

        for fixture in [PASS_REPORT, FAIL_REPORT, QUERY_REPORT, REFUSAL_REPORT] {
            let parsed: VerifyReport =
                serde_json::from_str(fixture).expect("fixture report parses");
            let expected: serde_json::Value =
                serde_json::from_str(fixture).expect("fixture value parses");
            let round_tripped =
                serde_json::to_value(parsed).expect("fixture report serializes again");

            assert_eq!(round_tripped, expected);
        }
    }
}
