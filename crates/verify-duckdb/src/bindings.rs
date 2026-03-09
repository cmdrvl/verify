use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

use duckdb::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};
use verify_core::{
    constraint::{Binding, ConstraintSet},
    refusal::{Refusal, RefusalCode},
    report::BindingReport,
};

pub const SUPPORTED_EXTENSIONS: &[&str] = &["csv", "json", "jsonl", "parquet"];
const CSV_NULL_SENTINEL: &str = "__VERIFY_NULL_SENTINEL__";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingFormat {
    Csv,
    Json,
    Jsonl,
    Parquet,
}

impl BindingFormat {
    pub fn detect(path: &Path) -> Result<Self, BatchBindingError> {
        let extension = path
            .extension()
            .and_then(OsStr::to_str)
            .map(|value| value.to_ascii_lowercase());

        match extension.as_deref() {
            Some("csv") => Ok(Self::Csv),
            Some("json") => Ok(Self::Json),
            Some("jsonl") => Ok(Self::Jsonl),
            Some("parquet") => Ok(Self::Parquet),
            _ => Err(BatchBindingError::FormatDetect {
                binding: String::new(),
                path: path.to_path_buf(),
                detail: format!(
                    "unsupported extension; expected one of {}",
                    SUPPORTED_EXTENSIONS.join(", ")
                ),
            }),
        }
    }

    fn create_sql(self, binding_name: &str, path: &Path) -> String {
        let relation_name = quote_identifier(binding_name);
        let escaped_path = sql_string_literal(path);

        match self {
            Self::Csv => format!(
                "CREATE TEMP TABLE {relation_name} AS SELECT * FROM read_csv_auto('{escaped_path}', nullstr='{CSV_NULL_SENTINEL}');"
            ),
            Self::Json => format!(
                "CREATE TEMP TABLE {relation_name} AS SELECT * FROM read_json_auto('{escaped_path}');"
            ),
            Self::Jsonl => format!(
                "CREATE TEMP TABLE {relation_name} AS SELECT * FROM read_ndjson_auto('{escaped_path}');"
            ),
            Self::Parquet => format!(
                "CREATE TEMP TABLE {relation_name} AS SELECT * FROM read_parquet('{escaped_path}');"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchBindingInput {
    pub name: String,
    pub path: PathBuf,
}

impl BatchBindingInput {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchBindingLimits {
    pub max_bytes: Option<u64>,
    pub max_rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingMetadata {
    pub source: String,
    pub content_hash: String,
    pub byte_len: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedBinding {
    declared: Binding,
    source_path: PathBuf,
    format: BindingFormat,
    relation_name: String,
    columns: Vec<BindingColumn>,
    metadata: BindingMetadata,
}

impl LoadedBinding {
    pub fn declared(&self) -> &Binding {
        &self.declared
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn format(&self) -> BindingFormat {
        self.format
    }

    pub fn relation_name(&self) -> &str {
        &self.relation_name
    }

    pub fn columns(&self) -> &[BindingColumn] {
        &self.columns
    }

    pub fn metadata(&self) -> &BindingMetadata {
        &self.metadata
    }

    pub fn binding_report(&self) -> BindingReport {
        BindingReport {
            kind: self.declared.kind,
            source: self.metadata.source.clone(),
            content_hash: self.metadata.content_hash.clone(),
            input_verification: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BindingRegistry {
    bindings: BTreeMap<String, LoadedBinding>,
}

impl BindingRegistry {
    pub fn get(&self, name: &str) -> Option<&LoadedBinding> {
        self.bindings.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &LoadedBinding)> {
        self.bindings
            .iter()
            .map(|(name, binding)| (name.as_str(), binding))
    }

    pub fn binding_reports(&self) -> BTreeMap<String, BindingReport> {
        self.bindings
            .iter()
            .map(|(name, binding)| (name.clone(), binding.binding_report()))
            .collect()
    }
}

#[derive(Debug)]
pub enum BatchBindingError {
    OpenConnection {
        source: duckdb::Error,
    },
    Io {
        binding: String,
        path: PathBuf,
        source: io::Error,
    },
    DuplicateBinding {
        binding: String,
    },
    MissingBinding {
        binding: String,
    },
    UndeclaredBinding {
        binding: String,
        path: PathBuf,
    },
    FormatDetect {
        binding: String,
        path: PathBuf,
        detail: String,
    },
    TooLarge {
        binding: String,
        path: PathBuf,
        limit_kind: &'static str,
        limit: u64,
        observed: u64,
    },
    Load {
        binding: String,
        path: PathBuf,
        source: duckdb::Error,
    },
}

impl BatchBindingError {
    pub fn open_connection(source: duckdb::Error) -> Self {
        Self::OpenConnection { source }
    }

    pub fn refusal_code(&self) -> RefusalCode {
        match self {
            Self::OpenConnection { .. } | Self::Io { .. } | Self::Load { .. } => RefusalCode::Io,
            Self::DuplicateBinding { .. } => RefusalCode::DuplicateBinding,
            Self::MissingBinding { .. } => RefusalCode::MissingBinding,
            Self::UndeclaredBinding { .. } => RefusalCode::UndeclaredBinding,
            Self::FormatDetect { .. } => RefusalCode::FormatDetect,
            Self::TooLarge { .. } => RefusalCode::TooLarge,
        }
    }

    pub fn to_refusal(&self) -> Refusal {
        Refusal::new(self.refusal_code(), self.to_string(), self.detail())
    }

    pub fn detail(&self) -> serde_json::Value {
        match self {
            Self::OpenConnection { .. } => json!({}),
            Self::Io { binding, path, .. } => json!({
                "binding": binding,
                "path": path.to_string_lossy(),
            }),
            Self::DuplicateBinding { binding } => json!({
                "binding": binding,
            }),
            Self::MissingBinding { binding } => json!({
                "binding": binding,
            }),
            Self::UndeclaredBinding { binding, .. } => json!({
                "binding": binding,
            }),
            Self::FormatDetect {
                binding,
                path,
                detail,
            } => json!({
                "binding": binding,
                "path": path.to_string_lossy(),
                "detail": detail,
            }),
            Self::TooLarge {
                binding,
                limit_kind,
                limit,
                observed,
                ..
            } => json!({
                "binding": binding,
                "limit_kind": limit_kind,
                "limit": limit,
                "observed": observed,
            }),
            Self::Load { binding, path, .. } => json!({
                "binding": binding,
                "path": path.to_string_lossy(),
            }),
        }
    }
}

impl std::fmt::Display for BatchBindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenConnection { .. } => {
                write!(formatter, "failed to open in-memory DuckDB connection")
            }
            Self::Io { binding, path, .. } => write!(
                formatter,
                "binding {binding} is unreadable: {}",
                path.to_string_lossy()
            ),
            Self::DuplicateBinding { binding } => {
                write!(formatter, "binding {binding} was supplied more than once")
            }
            Self::MissingBinding { binding } => {
                write!(
                    formatter,
                    "constraint set expects binding {binding}, but no input was supplied"
                )
            }
            Self::UndeclaredBinding { binding, .. } => {
                write!(
                    formatter,
                    "binding {binding} was supplied but not declared by the constraint set"
                )
            }
            Self::FormatDetect {
                binding,
                path,
                detail,
            } => write!(
                formatter,
                "binding {binding} cannot be loaded from {}: {detail}",
                path.to_string_lossy()
            ),
            Self::TooLarge {
                binding,
                limit_kind,
                limit,
                observed,
                ..
            } => write!(
                formatter,
                "binding {binding} exceeds {limit_kind} limit {limit} (observed {observed})"
            ),
            Self::Load { binding, path, .. } => write!(
                formatter,
                "failed to load binding {binding} from {} into DuckDB",
                path.to_string_lossy()
            ),
        }
    }
}

impl std::error::Error for BatchBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OpenConnection { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::Load { source, .. } => Some(source),
            Self::DuplicateBinding { .. }
            | Self::MissingBinding { .. }
            | Self::UndeclaredBinding { .. }
            | Self::FormatDetect { .. }
            | Self::TooLarge { .. } => None,
        }
    }
}

pub fn load_binding_registry(
    connection: &Connection,
    constraints: &ConstraintSet,
    inputs: Vec<BatchBindingInput>,
    limits: BatchBindingLimits,
) -> Result<BindingRegistry, BatchBindingError> {
    let declared = constraints
        .bindings
        .iter()
        .map(|binding| (binding.name.as_str(), binding))
        .collect::<BTreeMap<_, _>>();
    let mut supplied = BTreeMap::new();

    for input in inputs {
        if supplied.contains_key(input.name.as_str()) {
            return Err(BatchBindingError::DuplicateBinding {
                binding: input.name.clone(),
            });
        }

        if !declared.contains_key(input.name.as_str()) {
            return Err(BatchBindingError::UndeclaredBinding {
                binding: input.name,
                path: input.path,
            });
        }

        supplied.insert(input.name, input.path);
    }

    let mut bindings = BTreeMap::new();
    for declared_binding in &constraints.bindings {
        let path = supplied
            .remove(declared_binding.name.as_str())
            .ok_or_else(|| BatchBindingError::MissingBinding {
                binding: declared_binding.name.clone(),
            })?;
        let loaded = load_single_binding(connection, declared_binding.clone(), path, limits)?;
        bindings.insert(declared_binding.name.clone(), loaded);
    }

    Ok(BindingRegistry { bindings })
}

fn load_single_binding(
    connection: &Connection,
    declared: Binding,
    path: PathBuf,
    limits: BatchBindingLimits,
) -> Result<LoadedBinding, BatchBindingError> {
    let metadata = fs::metadata(&path).map_err(|source| BatchBindingError::Io {
        binding: declared.name.clone(),
        path: path.clone(),
        source,
    })?;

    if !metadata.is_file() {
        return Err(BatchBindingError::Io {
            binding: declared.name.clone(),
            path: path.clone(),
            source: io::Error::other("binding path is not a file"),
        });
    }

    let byte_len = metadata.len();
    if let Some(limit) = limits.max_bytes
        && byte_len > limit
    {
        return Err(BatchBindingError::TooLarge {
            binding: declared.name.clone(),
            path: path.clone(),
            limit_kind: "max_bytes",
            limit,
            observed: byte_len,
        });
    }

    let format = BindingFormat::detect(&path).map_err(|error| match error {
        BatchBindingError::FormatDetect { detail, .. } => BatchBindingError::FormatDetect {
            binding: declared.name.clone(),
            path: path.clone(),
            detail,
        },
        other => other,
    })?;

    let load_sql = format.create_sql(&declared.name, &path);
    connection
        .execute_batch(&load_sql)
        .map_err(|source| BatchBindingError::Load {
            binding: declared.name.clone(),
            path: path.clone(),
            source,
        })?;

    let columns = describe_relation(connection, &declared.name).map_err(|source| {
        BatchBindingError::Load {
            binding: declared.name.clone(),
            path: path.clone(),
            source,
        }
    })?;

    if matches!(format, BindingFormat::Json | BindingFormat::Jsonl) {
        enforce_row_oriented_json(&declared.name, &path, &columns)?;
    }

    let row_count =
        count_rows(connection, &declared.name).map_err(|source| BatchBindingError::Load {
            binding: declared.name.clone(),
            path: path.clone(),
            source,
        })?;

    if let Some(limit) = limits.max_rows
        && row_count > limit
    {
        return Err(BatchBindingError::TooLarge {
            binding: declared.name.clone(),
            path: path.clone(),
            limit_kind: "max_rows",
            limit,
            observed: row_count,
        });
    }

    let content_hash = file_sha256(&path, &declared.name)?;
    Ok(LoadedBinding {
        relation_name: declared.name.clone(),
        declared,
        source_path: path.clone(),
        format,
        columns,
        metadata: BindingMetadata {
            source: path.to_string_lossy().to_string(),
            content_hash,
            byte_len,
            row_count,
        },
    })
}

fn file_sha256(path: &Path, binding: &str) -> Result<String, BatchBindingError> {
    let bytes = fs::read(path).map_err(|source| BatchBindingError::Io {
        binding: binding.to_owned(),
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    Ok(format!("sha256:{digest:x}"))
}

fn describe_relation(
    connection: &Connection,
    relation_name: &str,
) -> duckdb::Result<Vec<BindingColumn>> {
    let mut statement = connection.prepare(&format!(
        "DESCRIBE SELECT * FROM {}",
        quote_identifier(relation_name)
    ))?;
    let rows = statement.query_map([], |row| {
        Ok(BindingColumn {
            name: row.get(0)?,
            data_type: row.get(1)?,
        })
    })?;

    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn count_rows(connection: &Connection, relation_name: &str) -> duckdb::Result<u64> {
    connection.query_row(
        &format!("SELECT COUNT(*) FROM {}", quote_identifier(relation_name)),
        [],
        |row| row.get::<_, u64>(0),
    )
}

fn enforce_row_oriented_json(
    binding: &str,
    path: &Path,
    columns: &[BindingColumn],
) -> Result<(), BatchBindingError> {
    let nested_columns = columns
        .iter()
        .filter(|column| is_nested_type(&column.data_type))
        .map(|column| format!("{} ({})", column.name, column.data_type))
        .collect::<Vec<_>>();

    if nested_columns.is_empty() {
        return Ok(());
    }

    Err(BatchBindingError::FormatDetect {
        binding: binding.to_owned(),
        path: path.to_path_buf(),
        detail: format!(
            "JSON binding must be row-oriented; nested columns: {}",
            nested_columns.join(", ")
        ),
    })
}

fn is_nested_type(data_type: &str) -> bool {
    let normalized = data_type.to_ascii_uppercase();
    normalized == "JSON"
        || normalized.contains("STRUCT(")
        || normalized.contains("MAP(")
        || normalized.contains("UNION(")
        || normalized.contains("LIST")
        || normalized.contains("[]")
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_string_literal(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fs, io,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;
    use verify_core::constraint::{Binding, BindingKind, ConstraintSet};

    use super::{
        BatchBindingError, BatchBindingInput, BatchBindingLimits, BindingFormat, BindingRegistry,
        count_rows, file_sha256, load_binding_registry, quote_identifier, sql_string_literal,
    };

    static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> io::Result<Self> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_nanos();

            for _ in 0..16 {
                let unique = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
                let path =
                    std::env::temp_dir().join(format!("verify-bindings-{timestamp}-{unique}"));

                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self { path }),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }

            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "failed to allocate unique binding test directory",
            ))
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_fixture(dir: &TestDir, name: &str, contents: &str) -> io::Result<PathBuf> {
        let path = dir.path().join(name);
        fs::write(&path, contents)?;
        Ok(path)
    }

    fn write_parquet_fixture(dir: &TestDir, name: &str) -> Result<PathBuf, Box<dyn Error>> {
        let path = dir.path().join(name);
        let connection = Connection::open_in_memory()?;
        let escaped_path = sql_string_literal(&path);
        connection.execute_batch(&format!(
            "COPY (
                SELECT 'LN-001' AS loan_id, 100.0 AS balance
                UNION ALL
                SELECT 'LN-002' AS loan_id, 250.5 AS balance
            ) TO '{escaped_path}' (FORMAT parquet);"
        ))?;
        Ok(path)
    }

    fn count_relation_rows(
        connection: &Connection,
        relation_name: &str,
    ) -> Result<u64, Box<dyn Error>> {
        Ok(count_rows(connection, relation_name)?)
    }

    fn constraint_with_bindings(binding_names: &[&str]) -> ConstraintSet {
        let mut constraints = ConstraintSet::new("fixtures.batch.bindings");
        constraints.bindings = binding_names
            .iter()
            .map(|name| Binding {
                name: (*name).to_owned(),
                kind: BindingKind::Relation,
                key_fields: Vec::new(),
            })
            .collect();
        constraints
    }

    fn load_registry_for_test(
        constraints: &ConstraintSet,
        inputs: Vec<BatchBindingInput>,
        limits: BatchBindingLimits,
    ) -> Result<(Connection, BindingRegistry), BatchBindingError> {
        let connection =
            Connection::open_in_memory().map_err(BatchBindingError::open_connection)?;
        let registry = load_binding_registry(&connection, constraints, inputs, limits)?;
        Ok((connection, registry))
    }

    #[test]
    fn detects_supported_binding_formats() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            BindingFormat::detect(Path::new("input.csv"))?,
            BindingFormat::Csv
        );
        assert_eq!(
            BindingFormat::detect(Path::new("input.json"))?,
            BindingFormat::Json
        );
        assert_eq!(
            BindingFormat::detect(Path::new("input.jsonl"))?,
            BindingFormat::Jsonl
        );
        assert_eq!(
            BindingFormat::detect(Path::new("input.parquet"))?,
            BindingFormat::Parquet
        );
        Ok(())
    }

    #[test]
    fn rejects_unsupported_binding_extension() -> Result<(), Box<dyn Error>> {
        let error =
            BindingFormat::detect(Path::new("input.txt")).expect_err("unsupported extension");
        assert!(matches!(error, BatchBindingError::FormatDetect { .. }));
        Ok(())
    }

    #[test]
    fn loads_csv_binding_and_preserves_blank_strings() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(
            &dir,
            "input.csv",
            "row_id,required_value\nROW-001,\nROW-002,\"   \"\nROW-003,present\n",
        )?;
        let constraints = constraint_with_bindings(&["input"]);
        let (connection, registry) = load_registry_for_test(
            &constraints,
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits::default(),
        )?;

        assert_eq!(count_relation_rows(&connection, "input")?, 3);
        let mut statement = connection.prepare(&format!(
            "SELECT required_value FROM {} ORDER BY row_id",
            quote_identifier("input")
        ))?;
        let rows = statement.query_map([], |row| row.get::<_, Option<String>>(0))?;
        let values = rows.collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            values,
            vec![
                Some(String::new()),
                Some("   ".to_owned()),
                Some("present".to_owned())
            ]
        );
        assert_eq!(
            registry
                .get("input")
                .expect("binding exists")
                .metadata()
                .row_count,
            3
        );
        Ok(())
    }

    #[test]
    fn loads_json_jsonl_and_parquet_bindings() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let json_path = write_fixture(
            &dir,
            "input.json",
            r#"[{"loan_id":"LN-001","balance":100.0},{"loan_id":"LN-002","balance":250.5}]"#,
        )?;
        let jsonl_path = write_fixture(
            &dir,
            "input.jsonl",
            "{\"loan_id\":\"LN-001\",\"balance\":100.0}\n{\"loan_id\":\"LN-002\",\"balance\":250.5}\n",
        )?;
        let parquet_path = write_parquet_fixture(&dir, "input.parquet")?;

        for path in [&json_path, &jsonl_path, &parquet_path] {
            let constraints = constraint_with_bindings(&["input"]);
            let (connection, registry) = load_registry_for_test(
                &constraints,
                vec![BatchBindingInput::new("input", path)],
                BatchBindingLimits::default(),
            )?;
            assert_eq!(count_relation_rows(&connection, "input")?, 2);
            assert_eq!(
                registry
                    .get("input")
                    .expect("binding exists")
                    .columns()
                    .len(),
                2
            );
        }

        Ok(())
    }

    #[test]
    fn rejects_nested_json_bindings() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(
            &dir,
            "input.json",
            r#"[{"loan_id":"LN-001","payload":{"nested":true}}]"#,
        )?;
        let constraints = constraint_with_bindings(&["input"]);
        let error = load_registry_for_test(
            &constraints,
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits::default(),
        )
        .expect_err("nested json should refuse");

        assert!(matches!(error, BatchBindingError::FormatDetect { .. }));
        Ok(())
    }

    #[test]
    fn enforces_max_bytes_before_loading() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(
            &dir,
            "input.csv",
            "loan_id,balance\nLN-001,100.0\nLN-002,250.5\n",
        )?;
        let constraints = constraint_with_bindings(&["input"]);
        let error = load_registry_for_test(
            &constraints,
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits {
                max_bytes: Some(8),
                max_rows: None,
            },
        )
        .expect_err("max bytes should refuse");

        assert!(matches!(
            error,
            BatchBindingError::TooLarge {
                limit_kind: "max_bytes",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn enforces_max_rows_after_loading() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(
            &dir,
            "input.csv",
            "loan_id,balance\nLN-001,100.0\nLN-002,250.5\n",
        )?;
        let constraints = constraint_with_bindings(&["input"]);
        let error = load_registry_for_test(
            &constraints,
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits {
                max_bytes: None,
                max_rows: Some(1),
            },
        )
        .expect_err("max rows should refuse");

        assert!(matches!(
            error,
            BatchBindingError::TooLarge {
                limit_kind: "max_rows",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn rejects_duplicate_missing_and_undeclared_bindings() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(&dir, "input.csv", "loan_id\nLN-001\n")?;

        let duplicate_error = load_registry_for_test(
            &constraint_with_bindings(&["input"]),
            vec![
                BatchBindingInput::new("input", &path),
                BatchBindingInput::new("input", &path),
            ],
            BatchBindingLimits::default(),
        )
        .expect_err("duplicate binding should refuse");
        assert!(matches!(
            duplicate_error,
            BatchBindingError::DuplicateBinding { .. }
        ));

        let missing_error = load_registry_for_test(
            &constraint_with_bindings(&["input", "reference"]),
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits::default(),
        )
        .expect_err("missing binding should refuse");
        assert!(matches!(
            missing_error,
            BatchBindingError::MissingBinding { .. }
        ));

        let undeclared_error = load_registry_for_test(
            &constraint_with_bindings(&["input"]),
            vec![BatchBindingInput::new("extra", &path)],
            BatchBindingLimits::default(),
        )
        .expect_err("undeclared binding should refuse");
        assert!(matches!(
            undeclared_error,
            BatchBindingError::UndeclaredBinding { .. }
        ));

        Ok(())
    }

    #[test]
    fn binding_reports_expose_stable_metadata() -> Result<(), Box<dyn Error>> {
        let dir = TestDir::new()?;
        let path = write_fixture(&dir, "input.csv", "loan_id\nLN-001\n")?;
        let constraints = constraint_with_bindings(&["input"]);
        let (_, registry) = load_registry_for_test(
            &constraints,
            vec![BatchBindingInput::new("input", &path)],
            BatchBindingLimits::default(),
        )?;

        let reports = registry.binding_reports();
        let report = reports.get("input").expect("report exists");
        let expected_hash = file_sha256(&path, "input")?;

        assert_eq!(report.kind, BindingKind::Relation);
        assert_eq!(report.source, path.to_string_lossy());
        assert_eq!(report.content_hash, expected_hash);
        assert!(report.input_verification.is_none());
        Ok(())
    }
}
