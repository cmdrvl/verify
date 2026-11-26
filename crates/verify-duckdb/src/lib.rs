#![forbid(unsafe_code)]

pub mod binding_predicates;
pub mod bindings;
pub mod lock_check;
pub mod query_rules;
pub mod scalar;
pub mod xlsx;

use duckdb::Connection;
use verify_core::constraint::ConstraintSet;

pub use binding_predicates::{
    BINDING_PREDICATE_OP, BindingPredicateError, BindingPredicateExecutor,
    evaluate_rule as evaluate_binding_predicate,
};
pub use bindings::{
    BatchBindingError, BatchBindingInput, BatchBindingLimits, BindingColumn, BindingFormat,
    BindingMetadata, BindingRegistry, LoadedBinding, SUPPORTED_EXTENSIONS, quote_identifier,
};
pub use lock_check::{LockError, verify_locks};
pub use query_rules::{QueryRuleError, evaluate_query_rule, execute_query_rules};
pub use scalar::{
    DuckValueError, DuckValueErrorKind, duckdb_type_category, duckdb_type_is_temporal,
    duckdb_value_to_protocol,
};

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

/// Statements that take away everything an untrusted rule's SQL could use to reach
/// outside the loaded bindings: extension install/autoload, file/http/s3 access
/// (`read_csv`, `COPY ... TO`, `ATTACH`, ...), and finally the ability to undo any
/// of that with `SET`.
pub const LOCKDOWN_SQL: &str = "SET autoinstall_known_extensions = false; \
     SET autoload_known_extensions = false; \
     SET enable_external_access = false; \
     SET lock_configuration = true;";

/// Lock a connection after its bindings are loaded. Call it once per connection;
/// a second call is expected to fail because configuration is already locked.
pub fn lock_down(connection: &Connection) -> Result<(), BatchBindingError> {
    connection
        .execute_batch(LOCKDOWN_SQL)
        .map_err(|source| BatchBindingError::Lockdown { source })
}

pub fn prepare_batch_context(
    constraints: &ConstraintSet,
    inputs: Vec<BatchBindingInput>,
    limits: BatchBindingLimits,
) -> Result<BatchContext, BatchBindingError> {
    let connection = Connection::open_in_memory().map_err(BatchBindingError::open_connection)?;
    let bindings = bindings::load_binding_registry(&connection, constraints, inputs, limits)?;
    // Every binding is now a materialized TEMP table, so nothing below needs the
    // filesystem or network. Lock before any rule SQL runs.
    lock_down(&connection)?;

    Ok(BatchContext {
        connection,
        bindings,
    })
}

#[cfg(test)]
mod lockdown_tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use super::lock_down;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let dir = std::env::temp_dir().join(format!(
                "verify-duckdb-lockdown-{name}-{}-{nanos}",
                std::process::id()
            ));
            fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }

        fn path(&self, file: &str) -> PathBuf {
            self.0.join(file)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn quoted(path: &Path) -> String {
        path.to_string_lossy().replace('\'', "''")
    }

    fn locked() -> Connection {
        let connection = Connection::open_in_memory().expect("in-memory DuckDB");
        lock_down(&connection).expect("lock down");
        connection
    }

    fn write_canary_csv(scratch: &Scratch) -> PathBuf {
        let path = scratch.path("canary.csv");
        fs::write(&path, "id,secret\n1,CANARY-7c1f\n").expect("write secret");
        path
    }

    #[test]
    fn host_files_are_readable_unlocked_and_unreachable_locked() {
        let scratch = Scratch::new("read");
        let canary_path = write_canary_csv(&scratch);
        let read = format!(
            "SELECT secret FROM read_csv_auto('{}')",
            quoted(&canary_path)
        );

        // Control: the same statement works on an unlocked connection, so a refusal
        // below is the lock's doing and not a typo in the test.
        let unlocked = Connection::open_in_memory().expect("in-memory DuckDB");
        let value: String = unlocked
            .query_row(&read, [], |row| row.get(0))
            .expect("unlocked read works");
        assert_eq!(value, "CANARY-7c1f");

        let locked = locked();
        for statement in [
            read.clone(),
            format!("SELECT * FROM read_csv('{}')", quoted(&canary_path)),
            format!("SELECT * FROM '{}'", quoted(&canary_path)),
            format!("SELECT * FROM read_text('{}')", quoted(&canary_path)),
            format!("SELECT * FROM glob('{}/*')", quoted(&scratch.0)),
        ] {
            let error = locked
                .execute_batch(&statement)
                .expect_err(&format!("locked connection must refuse: {statement}"));
            assert!(
                !error.to_string().contains("CANARY-7c1f"),
                "the refusal leaked file content: {error}"
            );
        }
    }

    #[test]
    fn copy_to_and_attach_create_nothing_when_locked() {
        let scratch = Scratch::new("write");

        // Control: COPY ... TO really writes on an unlocked connection.
        let control = scratch.path("control.csv");
        Connection::open_in_memory()
            .expect("in-memory DuckDB")
            .execute_batch(&format!("COPY (SELECT 1 AS a) TO '{}'", quoted(&control)))
            .expect("unlocked COPY works");
        assert!(control.exists(), "control COPY should have created a file");

        let locked = locked();
        let exfil = scratch.path("exfil.csv");
        let attached = scratch.path("attached.db");
        let parquet = scratch.path("exfil.parquet");
        assert!(
            locked
                .execute_batch(&format!("COPY (SELECT 1 AS a) TO '{}'", quoted(&exfil)))
                .is_err()
        );
        assert!(
            locked
                .execute_batch(&format!(
                    "COPY (SELECT 1 AS a) TO '{}' (FORMAT parquet)",
                    quoted(&parquet)
                ))
                .is_err()
        );
        assert!(
            locked
                .execute_batch(&format!("ATTACH '{}' AS other", quoted(&attached)))
                .is_err()
        );
        assert!(!exfil.exists(), "COPY wrote a file through the lock");
        assert!(!parquet.exists(), "COPY (FORMAT parquet) wrote a file");
        assert!(!attached.exists(), "ATTACH created a database file");
    }

    #[test]
    fn configuration_cannot_be_undone_after_the_lock() {
        let scratch = Scratch::new("config");
        let canary_path = write_canary_csv(&scratch);
        let locked = locked();

        assert!(
            locked
                .execute_batch("SET enable_external_access = true")
                .is_err()
        );
        assert!(
            locked
                .execute_batch("SET autoload_known_extensions = true")
                .is_err()
        );
        assert!(
            locked
                .execute_batch("SET lock_configuration = false")
                .is_err()
        );
        // A second lock_down also fails: configuration is locked.
        assert!(lock_down(&locked).is_err());
        // And the read is still refused after all those attempts.
        assert!(
            locked
                .execute_batch(&format!(
                    "CREATE TEMP TABLE leak AS SELECT * FROM read_csv_auto('{}')",
                    quoted(&canary_path)
                ))
                .is_err()
        );
    }

    #[test]
    fn http_and_s3_sources_are_refused_when_locked() {
        let locked = locked();
        for statement in [
            "SELECT * FROM 'https://127.0.0.1:9/x.csv'",
            "SELECT * FROM read_csv('http://127.0.0.1:9/x.csv')",
            "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
            "SELECT * FROM read_json('https://127.0.0.1:9/x.json')",
        ] {
            assert!(locked.execute_batch(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn bound_temp_tables_stay_usable_after_the_lock() {
        let connection = Connection::open_in_memory().expect("in-memory DuckDB");
        connection
            .execute_batch("CREATE TEMP TABLE loans AS SELECT 1 AS id UNION ALL SELECT 2 AS id")
            .expect("bind");
        lock_down(&connection).expect("lock down");
        let count: i64 = connection
            .query_row("SELECT count(*) FROM loans", [], |row| row.get(0))
            .expect("query after lock");
        assert_eq!(count, 2);
        connection
            .execute_batch("CREATE TEMP TABLE derived AS SELECT id * 2 AS id FROM loans")
            .expect("temp tables still work for rule evaluation");
    }
}
