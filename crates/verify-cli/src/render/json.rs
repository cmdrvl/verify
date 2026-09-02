use verify_core::report::VerifyReport;

/// Serialize a `VerifyReport` to pretty-printed JSON matching verify.report.v1.
pub fn render_report(report: &VerifyReport) -> String {
    serde_json::to_string_pretty(report).expect("VerifyReport must serialize to valid JSON")
}

#[cfg(test)]
mod tests {
    use verify_core::report::{ExecutionMode, Outcome, VerifyReport};

    use super::render_report;

    #[test]
    fn render_report_produces_valid_json() {
        let report = VerifyReport::new(
            ExecutionMode::Batch,
            "loan_tape.monthly.v1",
            "sha256:constraint",
        );

        let rendered = render_report(&report);
        let parsed: VerifyReport =
            serde_json::from_str(&rendered).expect("rendered JSON should parse back");

        assert_eq!(parsed.constraint_set_id, "loan_tape.monthly.v1");
        assert!(matches!(parsed.outcome, Outcome::Pass));
    }

    #[test]
    fn render_report_round_trips_fixture() {
        const FAIL_FIXTURE: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/reports/fail/arity1_not_null.fail.json"
        ));

        let report: VerifyReport =
            serde_json::from_str(FAIL_FIXTURE).expect("fixture should parse");
        let rendered = render_report(&report);
        let re_parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("rendered should parse");
        let expected: serde_json::Value =
            serde_json::from_str(FAIL_FIXTURE).expect("fixture should parse as value");

        assert_eq!(re_parsed, expected);
    }
}
