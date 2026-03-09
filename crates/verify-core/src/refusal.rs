use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefusalCode {
    #[serde(rename = "E_IO")]
    Io,
    #[serde(rename = "E_BAD_CONSTRAINTS")]
    BadConstraints,
    #[serde(rename = "E_BAD_AUTHORING")]
    BadAuthoring,
    #[serde(rename = "E_DUPLICATE_BINDING")]
    DuplicateBinding,
    #[serde(rename = "E_MISSING_BINDING")]
    MissingBinding,
    #[serde(rename = "E_UNDECLARED_BINDING")]
    UndeclaredBinding,
    #[serde(rename = "E_FORMAT_DETECT")]
    FormatDetect,
    #[serde(rename = "E_FIELD_NOT_FOUND")]
    FieldNotFound,
    #[serde(rename = "E_BAD_EXPR")]
    BadExpr,
    #[serde(rename = "E_SQL_ERROR")]
    SqlError,
    #[serde(rename = "E_BATCH_ONLY_RULE")]
    BatchOnlyRule,
    #[serde(rename = "E_KEY_CONFLICT")]
    KeyConflict,
    #[serde(rename = "E_INPUT_NOT_LOCKED")]
    InputNotLocked,
    #[serde(rename = "E_INPUT_DRIFT")]
    InputDrift,
    #[serde(rename = "E_TOO_LARGE")]
    TooLarge,
}

impl RefusalCode {
    pub const fn next_step(self) -> &'static str {
        match self {
            Self::Io => "Check paths and file permissions.",
            Self::BadConstraints => "Recompile or fix the constraint artifact.",
            Self::BadAuthoring => "Fix the authoring file, then re-run `verify compile`.",
            Self::DuplicateBinding => "Remove duplicate `--bind` inputs.",
            Self::MissingBinding => "Add the missing `--bind`.",
            Self::UndeclaredBinding => "Remove or rename the extra `--bind`.",
            Self::FormatDetect => "Use CSV, JSON, JSONL, or Parquet.",
            Self::FieldNotFound => {
                "Fix the constraint set or bind an input that exposes the required field."
            }
            Self::BadExpr => "Fix the rule expression.",
            Self::SqlError => "Fix the query-backed rule.",
            Self::BatchOnlyRule => "Lower the rule or run in batch mode.",
            Self::KeyConflict => "Remove the CLI override or fix the authored binding key.",
            Self::InputNotLocked => "Lock the input or provide the correct lock.",
            Self::InputDrift => "Use the locked artifact or regenerate the lock intentionally.",
            Self::TooLarge => "Increase the limit or split the input.",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Refusal {
    pub code: RefusalCode,
    pub message: String,
    pub detail: Value,
    pub next_step: String,
}

impl Refusal {
    pub fn new(code: RefusalCode, message: impl Into<String>, detail: Value) -> Self {
        Self {
            code,
            message: message.into(),
            detail,
            next_step: code.next_step().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Refusal, RefusalCode};

    #[test]
    fn refusal_codes_serialize_to_protocol_values() {
        let serialized =
            serde_json::to_string(&RefusalCode::FieldNotFound).expect("code serializes");
        assert_eq!(serialized, "\"E_FIELD_NOT_FOUND\"");
    }

    #[test]
    fn refusal_builder_attaches_next_step_guidance() {
        let refusal = Refusal::new(
            RefusalCode::KeyConflict,
            "Shortcut key override conflicts with authored key fields",
            json!({
                "binding": "input",
                "authored_key_fields": ["loan_identifier"],
                "cli_key_field": "loan_id",
            }),
        );

        assert_eq!(
            refusal.next_step,
            "Remove the CLI override or fix the authored binding key."
        );
    }
}
