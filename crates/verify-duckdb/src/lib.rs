#![forbid(unsafe_code)]

pub mod bindings;
pub mod lock_check;
pub mod query_rules;

use verify_core::constraint::ConstraintSet;

#[derive(Debug, Clone, Default)]
pub struct BatchContext;

pub fn prepare_scaffold(_constraints: &ConstraintSet) -> BatchContext {
    BatchContext
}
