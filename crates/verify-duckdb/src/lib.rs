#![forbid(unsafe_code)]

pub mod bindings;
pub mod lock_check;
pub mod query_rules;

use duckdb::Connection;
use verify_core::constraint::ConstraintSet;

pub use bindings::{
    BatchBindingError, BatchBindingInput, BatchBindingLimits, BindingColumn, BindingFormat,
    BindingMetadata, BindingRegistry, LoadedBinding, SUPPORTED_EXTENSIONS,
};
pub use lock_check::{LockError, verify_locks};
pub use query_rules::{QueryRuleError, evaluate_query_rule, execute_query_rules};

pub struct BatchContext {
    connection: Connection,
    bindings: BindingRegistry,
}

impl std::fmt::Debug for BatchContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BatchContext")
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

impl BatchContext {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn bindings(&self) -> &BindingRegistry {
        &self.bindings
    }

    pub fn into_parts(self) -> (Connection, BindingRegistry) {
        (self.connection, self.bindings)
    }
}

pub fn prepare_batch_context(
    constraints: &ConstraintSet,
    inputs: Vec<BatchBindingInput>,
    limits: BatchBindingLimits,
) -> Result<BatchContext, BatchBindingError> {
    let connection = Connection::open_in_memory().map_err(BatchBindingError::open_connection)?;
    let bindings = bindings::load_binding_registry(&connection, constraints, inputs, limits)?;

    Ok(BatchContext {
        connection,
        bindings,
    })
}
