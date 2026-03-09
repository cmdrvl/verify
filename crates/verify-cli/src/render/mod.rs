use verify_core::report::VerifyReport;

pub mod human;
pub mod json;

const CONSTRAINT_SCHEMA: &str =
    include_str!("../../../../schemas/verify.constraint.v1.schema.json");
const REPORT_SCHEMA: &str = include_str!("../../../../schemas/verify.report.v1.schema.json");

pub fn scaffold_message(surface: &str, json_output: bool) -> String {
    if json_output {
        json::scaffold_message(surface)
    } else {
        human::scaffold_message(surface)
    }
}

/// Render a `VerifyReport` as either JSON or compact human text.
pub fn render_report(report: &VerifyReport, json_output: bool) -> String {
    if json_output {
        json::render_report(report)
    } else {
        human::render_report(report)
    }
}

pub fn constraint_schema() -> &'static str {
    CONSTRAINT_SCHEMA
}

pub fn report_schema() -> &'static str {
    REPORT_SCHEMA
}

#[cfg(test)]
mod tests {
    use verify_core::report::{ExecutionMode, VerifyReport};

    use super::{constraint_schema, render_report, report_schema, scaffold_message};

    #[test]
    fn constraint_schema_is_embedded() {
        assert!(constraint_schema().contains("\"title\": \"verify.constraint.v1\""));
    }

    #[test]
    fn report_schema_is_embedded() {
        assert!(report_schema().contains("\"title\": \"verify.report.v1\""));
    }

    #[test]
    fn scaffold_message_switches_by_output_mode() {
        assert!(scaffold_message("compile portable authoring", true).contains("\"surface\""));
        assert!(
            scaffold_message("compile portable authoring", false).contains("verify scaffold only")
        );
    }

    #[test]
    fn render_report_json_mode_produces_json() {
        let report = VerifyReport::new(ExecutionMode::Batch, "test.constraint", "sha256:test");
        let rendered = render_report(&report, true);
        assert!(rendered.starts_with('{'));
        assert!(rendered.contains("\"outcome\": \"PASS\""));
    }

    #[test]
    fn render_report_human_mode_produces_text() {
        let report = VerifyReport::new(ExecutionMode::Batch, "test.constraint", "sha256:test");
        let rendered = render_report(&report, false);
        assert!(rendered.starts_with("VERIFY PASS"));
        assert!(rendered.contains("constraint_set: test.constraint"));
    }
}
