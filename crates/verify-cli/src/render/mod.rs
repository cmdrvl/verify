use verify_core::report::VerifyReport;

pub mod human;
pub mod json;

const CONSTRAINT_SCHEMA: &str =
    include_str!("../../../../schemas/verify.constraint.v1.schema.json");
const OPERATOR_JSON: &str = include_str!("../../../../operator.json");
const REPORT_SCHEMA: &str = include_str!("../../../../schemas/verify.report.v1.schema.json");

/// Render a `VerifyReport` as either JSON or compact human text.
pub fn render_report(
    report: &VerifyReport,
    json_output: bool,
    sample_affected: Option<usize>,
) -> String {
    if json_output {
        json::render_report(report)
    } else {
        human::render_report(report, sample_affected)
    }
}

pub fn constraint_schema() -> &'static str {
    CONSTRAINT_SCHEMA
}

pub fn operator_contract() -> &'static str {
    OPERATOR_JSON
}

pub fn report_schema() -> &'static str {
    REPORT_SCHEMA
}

#[cfg(test)]
mod tests {
    use verify_core::report::{ExecutionMode, VerifyReport};

    use super::{constraint_schema, operator_contract, render_report, report_schema};

    #[test]
    fn constraint_schema_is_embedded() {
        assert!(constraint_schema().contains("\"title\": \"verify.constraint.v1\""));
    }

    #[test]
    fn report_schema_is_embedded() {
        assert!(report_schema().contains("\"title\": \"verify.report.v1\""));
    }

    #[test]
    fn operator_contract_is_embedded() {
        let value: serde_json::Value =
            serde_json::from_str(operator_contract()).expect("operator contract must parse");
        assert_eq!(value["schema_version"], "operator.v0");
        assert_eq!(value["name"], "verify");
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn render_report_json_mode_produces_json() {
        let report = VerifyReport::new(ExecutionMode::Batch, "test.constraint", "sha256:test");
        let rendered = render_report(&report, true, None);
        assert!(rendered.starts_with('{'));
        assert!(rendered.contains("\"outcome\": \"PASS\""));
    }

    #[test]
    fn render_report_human_mode_produces_text() {
        let report = VerifyReport::new(ExecutionMode::Batch, "test.constraint", "sha256:test");
        let rendered = render_report(&report, false, None);
        assert!(rendered.starts_with("VERIFY PASS"));
        assert!(rendered.contains("constraint_set: test.constraint"));
    }
}
