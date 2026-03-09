use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::{ArgAction, Args};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use verify_core::{
    constraint::{Check, ConstraintSet},
    order::sort_report,
    refusal::RefusalCode,
    report::{ExecutionMode, InputVerification, Outcome, VerifyReport},
};
use verify_duckdb::{
    BatchBindingInput, BatchBindingLimits, BindingRegistry, execute_query_rules,
    prepare_batch_context, verify_locks,
};
use verify_engine::{Relation, SummaryEngine, portable_relation, portable_row};

use crate::witness::{self, WitnessInput};

pub const SHORTCUT_SUBCOMMAND: &str = "__shortcut__";

#[derive(Debug, Clone, Args, Default)]
pub struct CommonRunArgs {
    #[arg(long = "lock", action = ArgAction::Append, value_name = "LOCKFILE")]
    pub locks: Vec<PathBuf>,
    #[arg(long, value_name = "N")]
    pub max_rows: Option<u64>,
    #[arg(long, value_name = "N")]
    pub max_bytes: Option<u64>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub no_witness: bool,
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[arg(value_name = "COMPILED_CONSTRAINTS")]
    pub compiled_constraints: PathBuf,
    #[arg(long = "bind", action = ArgAction::Append, value_name = "NAME=PATH", required = true)]
    pub binds: Vec<String>,
    #[command(flatten)]
    pub common: CommonRunArgs,
}

#[derive(Debug, Clone, Args)]
pub struct ShortcutArgs {
    #[arg(value_name = "DATASET")]
    pub dataset: PathBuf,
    #[arg(long, value_name = "SOURCE", required = true)]
    pub rules: PathBuf,
    #[arg(long, value_name = "COLUMN")]
    pub key: Option<String>,
    #[command(flatten)]
    pub common: CommonRunArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunExit {
    Pass,
    Fail,
    Refusal,
}

impl RunExit {
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Pass => 0,
            Self::Fail => 1,
            Self::Refusal => 2,
        }
    }

    pub const fn witness_outcome(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Refusal => "REFUSAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommandResult {
    pub exit: RunExit,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

impl RunCommandResult {
    pub fn refusal(report: VerifyReport, json_output: bool) -> Self {
        let rendered =
            serde_json::to_string(&report).expect("refusal reports must serialize to valid JSON");
        let message = report
            .refusal
            .as_ref()
            .map(|refusal| refusal.message.clone())
            .unwrap_or_else(|| "verify refused to evaluate safely".to_owned());

        if json_output {
            Self {
                exit: RunExit::Refusal,
                stdout: Some(rendered),
                stderr: None,
            }
        } else {
            Self {
                exit: RunExit::Refusal,
                stdout: None,
                stderr: Some(message),
            }
        }
    }

    fn append_warning(&mut self, warning: String) {
        match &mut self.stderr {
            Some(stderr) if !stderr.is_empty() => {
                stderr.push('\n');
                stderr.push_str(&warning);
            }
            Some(stderr) => stderr.push_str(&warning),
            None => self.stderr = Some(warning),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub fn execute(args: RunArgs) -> RunCommandResult {
    let report = run_batch(&args);
    let mut result = report_to_result(report, args.common.json);
    append_witness_for_run(&args, &mut result);
    result
}

pub fn execute_shortcut(args: ShortcutArgs) -> RunCommandResult {
    let report = run_shortcut(&args);
    let mut result = report_to_result(report, args.common.json);
    append_witness_for_shortcut(&args, &mut result);
    result
}

// ---------------------------------------------------------------------------
// Batch run path  (verify run <compiled> --bind name=path ...)
// ---------------------------------------------------------------------------

fn run_batch(args: &RunArgs) -> VerifyReport {
    match run_batch_inner(args) {
        Ok(report) | Err(report) => report,
    }
}

fn run_batch_inner(args: &RunArgs) -> Result<VerifyReport, VerifyReport> {
    // 1. Read compiled constraints from disk.
    let constraint_bytes = std::fs::read(&args.compiled_constraints).map_err(|error| {
        batch_refusal(
            RefusalCode::Io,
            format!(
                "failed to read constraint file {}: {error}",
                args.compiled_constraints.display()
            ),
            json!({ "path": args.compiled_constraints.to_string_lossy() }),
        )
    })?;

    // 2. Compute content hash of the constraint file.
    let constraint_hash = sha256_of_bytes(&constraint_bytes);

    // 3. Parse JSON into ConstraintSet.
    let constraints: ConstraintSet =
        serde_json::from_slice(&constraint_bytes).map_err(|error| {
            batch_refusal_ctx(
                "",
                &constraint_hash,
                RefusalCode::BadConstraints,
                format!("failed to parse compiled constraints: {error}"),
                json!({ "error": error.to_string() }),
            )
        })?;

    // 4. Parse --bind NAME=PATH arguments.
    let inputs = parse_bind_args(&args.binds).map_err(|message| {
        batch_refusal_ctx(
            &constraints.constraint_set_id,
            &constraint_hash,
            RefusalCode::BadConstraints,
            message.clone(),
            json!({ "error": message }),
        )
    })?;

    // 5. Prepare DuckDB context (loads all bindings as temp tables).
    let limits = BatchBindingLimits {
        max_bytes: args.common.max_bytes,
        max_rows: args.common.max_rows,
    };
    let context = prepare_batch_context(&constraints, inputs, limits).map_err(|error| {
        let r = error.to_refusal();
        VerifyReport::refusal(
            ExecutionMode::Batch,
            constraints.constraint_set_id.clone(),
            constraint_hash.clone(),
            r.code,
            r.message,
            error.detail(),
        )
    })?;

    // 6. Optional lock verification.
    let lock_results = verify_locks_if_present(
        &args.common.locks,
        context.bindings(),
        &constraints.constraint_set_id,
        &constraint_hash,
    )?;

    // 7. Evaluate all rules.
    evaluate_batch(&constraints, &constraint_hash, context, lock_results)
}

// ---------------------------------------------------------------------------
// Shortcut path  (verify <dataset> --rules <authoring> [--key col])
// ---------------------------------------------------------------------------

fn run_shortcut(args: &ShortcutArgs) -> VerifyReport {
    match run_shortcut_inner(args) {
        Ok(report) | Err(report) => report,
    }
}

fn run_shortcut_inner(args: &ShortcutArgs) -> Result<VerifyReport, VerifyReport> {
    // 1. Compile authoring YAML/SQL into a ConstraintSet.
    let mut constraints = compile_authoring(&args.rules)?;

    // 2. Override key_fields if --key is provided.
    if let Some(key) = &args.key
        && let Some(binding) = constraints.bindings.first_mut()
    {
        binding.key_fields = vec![key.clone()];
    }

    // 3. Hash the compiled constraints (serialized form).
    let constraint_json = serde_json::to_vec(&constraints).expect("ConstraintSet serializes");
    let constraint_hash = sha256_of_bytes(&constraint_json);

    // 4. Determine the single binding name.
    let binding_name = constraints
        .bindings
        .first()
        .map(|b| b.name.clone())
        .ok_or_else(|| {
            batch_refusal_ctx(
                &constraints.constraint_set_id,
                &constraint_hash,
                RefusalCode::BadAuthoring,
                "compiled constraints declare no bindings".to_owned(),
                json!({}),
            )
        })?;

    // 5. Create single binding input.
    let inputs = vec![BatchBindingInput::new(&binding_name, &args.dataset)];
    let limits = BatchBindingLimits {
        max_bytes: args.common.max_bytes,
        max_rows: args.common.max_rows,
    };

    // 6. Prepare DuckDB context.
    let context = prepare_batch_context(&constraints, inputs, limits).map_err(|error| {
        let r = error.to_refusal();
        VerifyReport::refusal(
            ExecutionMode::Batch,
            constraints.constraint_set_id.clone(),
            constraint_hash.clone(),
            r.code,
            r.message,
            error.detail(),
        )
    })?;

    // 7. Optional lock verification.
    let lock_results = verify_locks_if_present(
        &args.common.locks,
        context.bindings(),
        &constraints.constraint_set_id,
        &constraint_hash,
    )?;

    // 8. Evaluate all rules.
    evaluate_batch(&constraints, &constraint_hash, context, lock_results)
}

// ---------------------------------------------------------------------------
// Core evaluation — shared by both run and shortcut paths
// ---------------------------------------------------------------------------

fn evaluate_batch(
    constraints: &ConstraintSet,
    constraint_hash: &str,
    context: verify_duckdb::BatchContext,
    lock_results: Option<BTreeMap<String, InputVerification>>,
) -> Result<VerifyReport, VerifyReport> {
    let (connection, registry) = context.into_parts();

    // Build binding reports with optional lock verification.
    let mut bindings = registry.binding_reports();
    if let Some(locks) = &lock_results {
        for (name, verification) in locks {
            if let Some(binding_report) = bindings.get_mut(name) {
                binding_report.input_verification = Some(verification.clone());
            }
        }
    }

    // Materialize DuckDB tables into Relations for portable evaluation.
    let relations = materialize_all_bindings(&connection, &registry, constraints, constraint_hash)?;

    // Initialize report.
    let mut report = VerifyReport::new(
        ExecutionMode::Batch,
        constraints.constraint_set_id.clone(),
        constraint_hash,
    );
    report.bindings = bindings;

    // Evaluate portable rules (skip query_zero_rows — handled separately).
    for rule in &constraints.rules {
        let result = match &rule.check {
            Check::QueryZeroRows { .. } => continue,
            Check::Unique { .. } | Check::NotNull { .. } | Check::Predicate { .. } => {
                portable_row::evaluate_rule(rule, &relations).map_err(|error| {
                    VerifyReport::refusal(
                        ExecutionMode::Batch,
                        constraints.constraint_set_id.clone(),
                        constraint_hash,
                        RefusalCode::BadConstraints,
                        format!("portable rule evaluation error: {error}"),
                        json!({ "rule_id": rule.id, "error": error.to_string() }),
                    )
                })?
            }
            Check::RowCount { .. } | Check::AggregateCompare { .. } | Check::ForeignKey { .. } => {
                portable_relation::evaluate_rule(rule, &relations).map_err(|error| {
                    VerifyReport::refusal(
                        ExecutionMode::Batch,
                        constraints.constraint_set_id.clone(),
                        constraint_hash,
                        RefusalCode::BadConstraints,
                        format!("portable rule evaluation error: {error}"),
                        json!({ "rule_id": rule.id, "error": error.to_string() }),
                    )
                })?
            }
        };
        report.results.push(result);
    }

    // Execute query_zero_rows rules against DuckDB.
    let query_results = execute_query_rules(&connection, &constraints.rules).map_err(|error| {
        let r = error.to_refusal();
        VerifyReport::refusal(
            ExecutionMode::Batch,
            constraints.constraint_set_id.clone(),
            constraint_hash,
            r.code,
            r.message,
            error.detail(),
        )
    })?;
    report.results.extend(query_results);

    // Apply summary, determine outcome, sort.
    SummaryEngine::apply(&mut report);
    report.outcome = if report.summary.failed_rules > 0 {
        Outcome::Fail
    } else {
        Outcome::Pass
    };
    sort_report(&mut report);

    Ok(report)
}

// ---------------------------------------------------------------------------
// DuckDB → Relation materialization
// ---------------------------------------------------------------------------

fn materialize_all_bindings(
    connection: &duckdb::Connection,
    registry: &BindingRegistry,
    constraints: &ConstraintSet,
    constraint_hash: &str,
) -> Result<BTreeMap<String, Relation>, VerifyReport> {
    let mut relations = BTreeMap::new();
    for (name, loaded) in registry.iter() {
        let key_fields = constraints
            .bindings
            .iter()
            .find(|b| b.name == name)
            .map(|b| b.key_fields.clone())
            .unwrap_or_default();

        let relation = materialize_relation(connection, loaded.relation_name(), key_fields)
            .map_err(|error| {
                VerifyReport::refusal(
                    ExecutionMode::Batch,
                    constraints.constraint_set_id.clone(),
                    constraint_hash,
                    RefusalCode::SqlError,
                    format!("failed to materialize binding {name}: {error}"),
                    json!({ "binding": name, "error": error.to_string() }),
                )
            })?;
        relations.insert(name.to_owned(), relation);
    }
    Ok(relations)
}

fn materialize_relation(
    connection: &duckdb::Connection,
    relation_name: &str,
    key_fields: Vec<String>,
) -> Result<Relation, duckdb::Error> {
    let query = format!("SELECT * FROM {relation_name}");
    let mut statement = connection.prepare(&query)?;
    let mut duckdb_rows = statement.query([])?;

    let column_count = duckdb_rows.as_ref().expect("rows ref").column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|i| {
            duckdb_rows
                .as_ref()
                .expect("rows ref")
                .column_name(i)
                .map_or("?".to_owned(), |name| name.to_owned())
        })
        .collect();

    let mut rows = Vec::new();
    while let Some(row) = duckdb_rows.next()? {
        let mut row_map = BTreeMap::new();
        for (i, col_name) in column_names.iter().enumerate() {
            let duck_value: duckdb::types::Value = row.get(i).unwrap_or(duckdb::types::Value::Null);
            row_map.insert(col_name.clone(), duckdb_value_to_json(duck_value));
        }
        rows.push(row_map);
    }

    Ok(Relation::new(key_fields, rows))
}

/// Convert a DuckDB value to a serde_json Value for portable evaluation.
fn duckdb_value_to_json(value: duckdb::types::Value) -> Value {
    match value {
        duckdb::types::Value::Null => Value::Null,
        duckdb::types::Value::Boolean(b) => Value::Bool(b),
        duckdb::types::Value::TinyInt(n) => Value::Number(n.into()),
        duckdb::types::Value::SmallInt(n) => Value::Number(n.into()),
        duckdb::types::Value::Int(n) => Value::Number(n.into()),
        duckdb::types::Value::BigInt(n) => Value::Number(n.into()),
        duckdb::types::Value::Float(f) => {
            serde_json::Number::from_f64(f64::from(f)).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Double(f) => {
            serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Text(s) => Value::String(s),
        _ => Value::String(format!("{value:?}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn report_to_result(report: VerifyReport, json: bool) -> RunCommandResult {
    match report.outcome {
        Outcome::Refusal => RunCommandResult::refusal(report, json),
        _ => {
            let exit = if report.outcome == Outcome::Pass {
                RunExit::Pass
            } else {
                RunExit::Fail
            };
            let rendered = crate::render::render_report(&report, json);
            RunCommandResult {
                exit,
                stdout: Some(rendered),
                stderr: None,
            }
        }
    }
}

fn sha256_of_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn parse_bind_args(binds: &[String]) -> Result<Vec<BatchBindingInput>, String> {
    binds
        .iter()
        .map(|bind| {
            let (name, path) = bind
                .split_once('=')
                .ok_or_else(|| format!("invalid --bind: expected NAME=PATH, got: {bind}"))?;
            Ok(BatchBindingInput::new(name, path))
        })
        .collect()
}

fn verify_locks_if_present(
    locks: &[PathBuf],
    registry: &BindingRegistry,
    constraint_set_id: &str,
    constraint_hash: &str,
) -> Result<Option<BTreeMap<String, InputVerification>>, VerifyReport> {
    if locks.is_empty() {
        return Ok(None);
    }
    verify_locks(locks, registry).map(Some).map_err(|error| {
        let r = error.to_refusal();
        VerifyReport::refusal(
            ExecutionMode::Batch,
            constraint_set_id,
            constraint_hash,
            r.code,
            r.message,
            error.detail(),
        )
    })
}

fn batch_refusal(code: RefusalCode, message: String, detail: Value) -> VerifyReport {
    VerifyReport::refusal(ExecutionMode::Batch, "", "", code, message, detail)
}

fn batch_refusal_ctx(
    constraint_set_id: &str,
    constraint_hash: &str,
    code: RefusalCode,
    message: String,
    detail: Value,
) -> VerifyReport {
    VerifyReport::refusal(
        ExecutionMode::Batch,
        constraint_set_id,
        constraint_hash,
        code,
        message,
        detail,
    )
}

fn compile_authoring(path: &Path) -> Result<ConstraintSet, VerifyReport> {
    if crate::compile::query::is_query_authoring(path) {
        crate::compile::query::compile_from_path(path).map_err(|error| {
            batch_refusal(
                RefusalCode::BadAuthoring,
                error.render(path),
                json!({ "path": path.to_string_lossy() }),
            )
        })
    } else {
        crate::compile::portable::compile_from_path(path).map_err(|error| {
            batch_refusal(
                RefusalCode::BadAuthoring,
                error.render(path),
                json!({ "path": path.to_string_lossy() }),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// Witness helpers
// ---------------------------------------------------------------------------

fn append_witness_for_run(args: &RunArgs, result: &mut RunCommandResult) {
    if args.common.no_witness {
        return;
    }

    let mut params = Map::new();
    params.insert("surface".to_owned(), Value::String("run".to_owned()));
    params.insert("json".to_owned(), Value::Bool(args.common.json));
    params.insert("binds".to_owned(), Value::Array(bind_names(&args.binds)));

    if let Some(max_rows) = args.common.max_rows {
        params.insert("max_rows".to_owned(), Value::Number(max_rows.into()));
    }

    if let Some(max_bytes) = args.common.max_bytes {
        params.insert("max_bytes".to_owned(), Value::Number(max_bytes.into()));
    }

    let mut inputs = vec![WitnessInput::from_path(&args.compiled_constraints)];
    inputs.extend(bind_paths(&args.binds));
    inputs.extend(args.common.locks.iter().map(WitnessInput::from_path));

    if let Err(error) = witness::append_run_record(
        "run",
        result.exit.witness_outcome(),
        result.exit.exit_code(),
        inputs,
        params,
        rendered_output_bytes(result).as_slice(),
    ) {
        result.append_warning(format!(
            "verify: warning: failed to append witness record: {error}"
        ));
    }
}

fn append_witness_for_shortcut(args: &ShortcutArgs, result: &mut RunCommandResult) {
    if args.common.no_witness {
        return;
    }

    let mut params = Map::new();
    params.insert("surface".to_owned(), Value::String("shortcut".to_owned()));
    params.insert("json".to_owned(), Value::Bool(args.common.json));

    if let Some(key) = &args.key {
        params.insert("key".to_owned(), Value::String(key.clone()));
    }

    if let Some(max_rows) = args.common.max_rows {
        params.insert("max_rows".to_owned(), Value::Number(max_rows.into()));
    }

    if let Some(max_bytes) = args.common.max_bytes {
        params.insert("max_bytes".to_owned(), Value::Number(max_bytes.into()));
    }

    let mut inputs = vec![
        WitnessInput::from_path(&args.dataset),
        WitnessInput::from_path(&args.rules),
    ];
    inputs.extend(args.common.locks.iter().map(WitnessInput::from_path));

    if let Err(error) = witness::append_run_record(
        "shortcut",
        result.exit.witness_outcome(),
        result.exit.exit_code(),
        inputs,
        params,
        rendered_output_bytes(result).as_slice(),
    ) {
        result.append_warning(format!(
            "verify: warning: failed to append witness record: {error}"
        ));
    }
}

fn bind_names(binds: &[String]) -> Vec<Value> {
    binds
        .iter()
        .filter_map(|bind| {
            bind.split_once('=')
                .map(|(name, _)| Value::String(name.to_owned()))
        })
        .collect()
}

fn bind_paths(binds: &[String]) -> Vec<WitnessInput> {
    binds
        .iter()
        .map(|bind| bind.split_once('=').map_or(bind.as_str(), |(_, path)| path))
        .map(WitnessInput::from_path)
        .collect()
}

fn rendered_output_bytes(result: &RunCommandResult) -> Vec<u8> {
    let mut bytes = Vec::new();

    if let Some(stdout) = &result.stdout {
        bytes.extend_from_slice(stdout.as_bytes());
    }

    if let Some(stderr) = &result.stderr {
        bytes.extend_from_slice(stderr.as_bytes());
    }

    bytes
}
