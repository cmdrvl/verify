#![forbid(unsafe_code)]

pub mod embedded;
pub mod portable_relation;
pub mod portable_row;
pub mod summary;

use verify_core::{constraint::ConstraintSet, report::VerifyReport};

#[derive(Debug, Clone, Default)]
pub struct EvaluationContext;

pub fn evaluate_scaffold(
    _constraints: &ConstraintSet,
    _context: &EvaluationContext,
) -> Result<VerifyReport, &'static str> {
    Err("verify-engine scaffold only")
}
