#![forbid(unsafe_code)]

pub mod embedded;
pub mod portable_relation;
pub mod portable_row;
pub mod summary;

use std::collections::BTreeMap;

use serde_json::Value;
use verify_core::{constraint::ConstraintSet, report::VerifyReport};

pub use portable_row::{EngineError, PORTABLE_ROW_OPS};
pub use summary::{SEVERITY_BANDS, SummaryEngine, derive_policy_signals, summarize_results};

/// A materialized relation for portable rule evaluation.
///
/// Both batch (DuckDB) and embedded executors produce `Relation` values
/// that the portable engine consumes without knowing how data was loaded.
#[derive(Debug, Clone)]
pub struct Relation {
    /// Key fields used for failure localization in reports.
    pub key_fields: Vec<String>,
    /// Row data: each row maps column names to values.
    pub rows: Vec<BTreeMap<String, Value>>,
}

impl Relation {
    pub fn new(key_fields: Vec<String>, rows: Vec<BTreeMap<String, Value>>) -> Self {
        Self { key_fields, rows }
    }

    pub fn empty(key_fields: Vec<String>) -> Self {
        Self {
            key_fields,
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone, Default)]
pub struct EvaluationContext;

pub fn evaluate_scaffold(
    _constraints: &ConstraintSet,
    _context: &EvaluationContext,
) -> Result<VerifyReport, &'static str> {
    Err("verify-engine scaffold only")
}
