use verify_core::report::VerifyReport;

pub fn scaffold_message(surface: &str) -> String {
    serde_json::json!({
        "tool": "verify",
        "status": "scaffold_only",
        "surface": surface,
        "message": format!("verify scaffold only: {surface} is not implemented yet"),
        "detail": {
            "surface": surface,
            "status": "scaffold_only",
        }
    })
    .to_string()
}

/// Serialize a `VerifyReport` to pretty-printed JSON matching verify.report.v1.
pub fn render_report(report: &VerifyReport) -> String {
    serde_json::to_string_pretty(report).expect("VerifyReport must serialize to valid JSON")
}

#[cfg(test)]
mod tests {
    use verify_core::report::{ExecutionMode, Outcome, VerifyReport};

    use super::{render_report, scaffold_message};

    #[test]
    fn scaffold_message_serializes_json_scaffold_marker() {
        let value: serde_json::Value =
            serde_json::from_str(&scaffold_message("compile portable authoring"))
                .expect("scaffold refusal should be valid json");

        assert_eq!(value["tool"], "verify");
        assert_eq!(value["status"], "scaffold_only");
        assert_eq!(value["detail"]["surface"], "compile portable authoring");
    }

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
