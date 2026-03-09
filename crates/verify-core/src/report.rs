use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{REPORT_VERSION, TOOL_NAME};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyReport {
    pub tool: String,
    pub version: String,
    pub execution_mode: ExecutionMode,
    pub outcome: Outcome,
    pub constraint_set_id: String,
    #[serde(default)]
    pub results: Vec<RuleResult>,
    pub summary: Summary,
}

impl Default for VerifyReport {
    fn default() -> Self {
        Self {
            tool: TOOL_NAME.to_owned(),
            version: REPORT_VERSION.to_owned(),
            execution_mode: ExecutionMode::Batch,
            outcome: Outcome::Refusal,
            constraint_set_id: "scaffold".to_owned(),
            results: Vec::new(),
            summary: Summary::default(),
        }
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
pub struct RuleResult {
    pub rule_id: String,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected: Vec<AffectedEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AffectedEntry {
    pub binding: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub key: BTreeMap<String, String>,
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
}
