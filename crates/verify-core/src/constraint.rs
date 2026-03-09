use serde::{Deserialize, Serialize};

use crate::CONSTRAINT_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintSet {
    pub version: String,
    pub constraint_set_id: String,
    pub bindings: Vec<Binding>,
    pub rules: Vec<Rule>,
}

impl Default for ConstraintSet {
    fn default() -> Self {
        Self {
            version: CONSTRAINT_VERSION.to_owned(),
            constraint_set_id: "scaffold".to_owned(),
            bindings: Vec::new(),
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub name: String,
    pub kind: BindingKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingKind {
    Relation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub severity: Severity,
    pub portability: Portability,
    pub check: Check,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Portability {
    Portable,
    BatchOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    pub op: String,
}
