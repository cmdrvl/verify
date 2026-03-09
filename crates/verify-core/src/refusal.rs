use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{REPORT_VERSION, TOOL_NAME, report::Outcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RefusalCode {
    ScaffoldOnly,
    NotImplemented,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefusalEnvelope {
    pub tool: String,
    pub version: String,
    pub outcome: Outcome,
    pub code: RefusalCode,
    pub message: String,
    pub detail: Value,
}

impl RefusalEnvelope {
    pub fn scaffold(surface: &str) -> Self {
        Self {
            tool: TOOL_NAME.to_owned(),
            version: REPORT_VERSION.to_owned(),
            outcome: Outcome::Refusal,
            code: RefusalCode::ScaffoldOnly,
            message: format!("verify scaffold only: {surface} is not implemented yet"),
            detail: serde_json::json!({
                "surface": surface,
                "status": "scaffold_only",
            }),
        }
    }
}
