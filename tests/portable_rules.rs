/// Portable rule integration tests for verify.
///
/// These exercise the full compile → bind → evaluate pipeline for portable
/// rule types through the CLI binary, covering not_null, foreign_key, and
/// the predicate grammar.
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use duckdb::Connection;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> String {
    format!("{WORKSPACE_ROOT}/{relative}")
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
}

fn typed_temporal_parquet() -> (TempDir, PathBuf) {
    let directory = tempdir().expect("temporary temporal fixture directory");
    let path = directory.path().join("typed_temporal.parquet");
    let connection = Connection::open_in_memory().expect("DuckDB connection");
    connection
        .execute_batch(
            "CREATE TABLE typed_temporal (
                loan_id VARCHAR,
                origination_date DATE,
                last_paid_at TIMESTAMP,
                cut_off_time TIME,
                seasoning INTERVAL
             );
             INSERT INTO typed_temporal VALUES
                ('LN-001', DATE '2024-01-31', TIMESTAMP '2026-01-31 10:00:00', TIME '10:00:00', INTERVAL '1 day 2 hours'),
                ('LN-002', NULL, NULL, NULL, NULL),
                ('LN-003', DATE '2024-03-31', TIMESTAMP '2026-03-31 09:15:00', TIME '09:15:00', INTERVAL '3 days');
             "
        )
        .expect("typed temporal parquet should write");
    connection
        .execute(
            "COPY typed_temporal TO ? (FORMAT PARQUET)",
            duckdb::params![path.to_string_lossy().as_ref()],
        )
        .expect("typed temporal parquet should export");
    (directory, path)
}

fn temporal_constraints(directory: &Path, id: &str, rules: Value) -> PathBuf {
    let path = directory.join(format!("{id}.verify.json"));
    let constraints = json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": format!("tests.{id}"),
        "bindings": [{
            "name": "input",
            "kind": "relation",
            "key_fields": ["loan_id"]
        }],
        "rules": rules,
    });
    fs::write(
        &path,
        serde_json::to_vec_pretty(&constraints).expect("constraints serialize"),
    )
    .expect("constraints should write");
    path
}

fn run_temporal_rules(
    input: &Path,
    constraints: &Path,
    explicit: bool,
) -> (std::process::Output, Value) {
    let bind = format!("input={}", input.display());
    let mut command = verify_command();
    command.args([
        "run",
        &constraints.to_string_lossy(),
        "--bind",
        &bind,
        "--json",
        "--no-witness",
    ]);
    if explicit {
        command.arg("--explicit");
    }
    let output = command.output().expect("temporal rules should run");
    let report = serde_json::from_slice(&output.stdout).expect("expected JSON report");
    (output, report)
}

fn result<'a>(report: &'a Value, rule_id: &str) -> &'a Value {
    report["results"]
        .as_array()
        .expect("report results")
        .iter()
        .find(|result| result["rule_id"] == rule_id)
        .expect("missing expected rule result")
}

// ---------------------------------------------------------------------------
// not_null portable rule
// ---------------------------------------------------------------------------

#[test]
fn not_null_pass_over_complete_data() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!("input={}", fixture("fixtures/inputs/arity1/loans.csv"));
    let output = verify_command()
        .args([
            "run",
            &constraints,
            "--bind",
            &bind,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("not_null pass should run");

    assert_eq!(output.status.code(), Some(0));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["results"][0]["rule_id"], "INPUT_LOAN_ID_PRESENT");
    assert_eq!(report["results"][0]["status"], "pass");
    assert_eq!(report["results"][0]["violation_count"], 0);
}

#[test]
fn not_null_fail_over_missing_ids() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!(
        "input={}",
        fixture("fixtures/inputs/arity1/loans_missing_id.csv")
    );
    let output = verify_command()
        .args([
            "run",
            &constraints,
            "--bind",
            &bind,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("not_null fail should run");

    assert_eq!(output.status.code(), Some(1));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["results"][0]["status"], "fail");
    assert!(report["results"][0]["violation_count"].as_u64().unwrap() >= 1);

    // Affected entries should have binding and field localization
    let affected = &report["results"][0]["affected"];
    assert!(!affected.as_array().unwrap().is_empty());
    assert_eq!(affected[0]["binding"], "input");
    assert_eq!(affected[0]["field"], "loan_id");
}

// ---------------------------------------------------------------------------
// foreign_key portable rule (arity-N)
// ---------------------------------------------------------------------------

#[test]
fn foreign_key_pass_when_all_references_exist() {
    let constraints =
        fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
    let bind_property = format!(
        "property={}",
        fixture("fixtures/inputs/arity_n/property_no_orphans.csv")
    );
    let bind_tenants = format!("tenants={}", fixture("fixtures/inputs/arity_n/tenants.csv"));
    let output = verify_command()
        .args([
            "run",
            &constraints,
            "--bind",
            &bind_property,
            "--bind",
            &bind_tenants,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("foreign_key pass should run");

    assert_eq!(output.status.code(), Some(0));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["results"][0]["rule_id"], "PROPERTY_TENANT_EXISTS");
    assert_eq!(report["results"][0]["violation_count"], 0);
}

#[test]
fn foreign_key_fail_when_orphan_reference_exists() {
    let constraints =
        fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
    let bind_property = format!(
        "property={}",
        fixture("fixtures/inputs/arity_n/property.csv")
    );
    let bind_tenants = format!("tenants={}", fixture("fixtures/inputs/arity_n/tenants.csv"));
    let output = verify_command()
        .args([
            "run",
            &constraints,
            "--bind",
            &bind_property,
            "--bind",
            &bind_tenants,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("foreign_key fail should run");

    assert_eq!(output.status.code(), Some(1));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["results"][0]["rule_id"], "PROPERTY_TENANT_EXISTS");

    let affected = &report["results"][0]["affected"];
    assert!(!affected.as_array().unwrap().is_empty());
    assert_eq!(affected[0]["binding"], "property");
}

// ---------------------------------------------------------------------------
// Shortcut path compiles and evaluates in one step
// ---------------------------------------------------------------------------

#[test]
fn shortcut_compiles_and_evaluates_portable_authoring() {
    let dataset = fixture("fixtures/inputs/arity1/loans.csv");
    let rules = fixture("fixtures/authoring/arity1/not_null_loans.yaml");
    let output = verify_command()
        .args([
            &dataset,
            "--rules",
            &rules,
            "--key",
            "loan_id",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("shortcut should run");

    assert_eq!(output.status.code(), Some(0));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "PASS");
    assert!(report["constraint_set_id"].as_str().is_some());
}

// ---------------------------------------------------------------------------
// Compile surface validates authoring
// ---------------------------------------------------------------------------

#[test]
fn compile_check_validates_portable_authoring() {
    let authoring = fixture("fixtures/authoring/arity1/not_null_loans.yaml");
    let output = verify_command()
        .args(["compile", &authoring, "--check"])
        .output()
        .expect("compile check should run");

    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn compile_check_validates_sql_authoring() {
    let authoring = fixture("fixtures/authoring/query_rules/orphan_rows.sql");
    let output = verify_command()
        .args(["compile", &authoring, "--check"])
        .output()
        .expect("compile check should run");

    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn compile_outputs_valid_constraint_json() {
    let authoring = fixture("fixtures/authoring/arity1/not_null_loans.yaml");
    let output = verify_command()
        .args(["compile", &authoring])
        .output()
        .expect("compile should run");

    assert_eq!(output.status.code(), Some(0));

    let compiled: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(compiled["version"], "verify.constraint.v1");
    assert!(compiled["rules"].as_array().is_some_and(|r| !r.is_empty()));
}

// ---------------------------------------------------------------------------
// Materialization projects only the columns rules actually read
// ---------------------------------------------------------------------------

/// Regression for bd-1ka: a `DATE`/`TIMESTAMP` column no rule references must
/// not abort the load. Before this, materialization converted every column of
/// the relation into a protocol scalar, so an unread temporal column killed
/// every single-binding constraint set over real tape data.
#[test]
fn unreferenced_temporal_columns_do_not_block_materialization() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!(
        "input={}",
        fixture("fixtures/inputs/arity1/loans_with_temporal_columns.csv")
    );
    let output = verify_command()
        .args([
            "run",
            &constraints,
            "--bind",
            &bind,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("unreferenced temporal columns should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["refusal"], Value::Null);
    assert_eq!(report["results"][0]["rule_id"], "INPUT_LOAN_ID_PRESENT");
    assert_eq!(report["results"][0]["status"], "pass");
}

/// A temporal column used as a value still refuses. Projection narrows what is
/// materialized; it must not restore the lossy `Debug` cast that silently
/// misrepresented dates before 0.4.0. Nullity-only rules are covered below and
/// are intentionally allowed to inspect presence without a protocol scalar.
#[test]
fn value_using_temporal_columns_still_refuse() {
    let authoring = concat!(
        "constraint_set_id: tests.referenced_temporal\n",
        "bindings:\n",
        "  input:\n",
        "    key_fields:\n",
        "      - loan_id\n",
        "rules:\n",
        "  - id: ORIGINATION_DATE_MATCHES\n",
        "    severity: error\n",
        "    binding: input\n",
        "    op: predicate\n",
        "    expr:\n",
        "      eq:\n",
        "        - column: origination_date\n",
        "        - 2024-01-31\n",
    );
    let authoring_path = std::env::temp_dir().join("verify_referenced_temporal.yaml");
    std::fs::write(&authoring_path, authoring).expect("authoring should write");

    let compiled_path = std::env::temp_dir().join("verify_referenced_temporal.verify.json");
    let compile = verify_command()
        .args([
            "compile",
            &authoring_path.to_string_lossy(),
            "--out",
            &compiled_path.to_string_lossy(),
        ])
        .output()
        .expect("compile should run");
    assert_eq!(compile.status.code(), Some(0));

    let bind = format!(
        "input={}",
        fixture("fixtures/inputs/arity1/loans_with_temporal_columns.csv")
    );
    let output = verify_command()
        .args([
            "run",
            &compiled_path.to_string_lossy(),
            "--bind",
            &bind,
            "--no-witness",
        ])
        .output()
        .expect("referenced temporal column should run");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("origination_date"),
        "refusal should name the referenced temporal column: {stderr}"
    );
}

#[test]
fn nullity_rules_evaluate_all_typed_temporals_by_presence() {
    let (directory, input) = typed_temporal_parquet();
    let constraints = temporal_constraints(
        directory.path(),
        "typed_temporal_presence",
        json!([
            {
                "id": "DATE_NOT_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "not_null",
                    "binding": "input",
                    "columns": ["origination_date"]
                }
            },
            {
                "id": "TIMESTAMP_NOT_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "not_null",
                    "binding": "input",
                    "columns": ["last_paid_at"]
                }
            },
            {
                "id": "TIME_NOT_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "not_null",
                    "binding": "input",
                    "columns": ["cut_off_time"]
                }
            },
            {
                "id": "INTERVAL_NOT_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "not_null",
                    "binding": "input",
                    "columns": ["seasoning"]
                }
            },
            {
                "id": "DATE_NOT_NULL_PREDICATE",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"not": {"is_null": {"column": "origination_date"}}}
                }
            },
            {
                "id": "TIMESTAMP_NOT_BLANK_PREDICATE",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"not": {"is_blank": {"column": "last_paid_at"}}}
                }
            }
        ]),
    );

    let (output, report) = run_temporal_rules(&input, &constraints, true);
    assert_eq!(output.status.code(), Some(1), "report: {report}");
    assert_eq!(report["outcome"], "FAIL");
    for rule_id in [
        "DATE_NOT_NULL",
        "TIMESTAMP_NOT_NULL",
        "TIME_NOT_NULL",
        "INTERVAL_NOT_NULL",
        "DATE_NOT_NULL_PREDICATE",
        "TIMESTAMP_NOT_BLANK_PREDICATE",
    ] {
        let rule = result(&report, rule_id);
        assert_eq!(rule["status"], "fail", "{rule_id}");
        assert_eq!(rule["violation_count"], 1, "{rule_id}");
        assert!(
            rule["affected"][0]
                .as_object()
                .expect("affected entry")
                .contains_key("value"),
            "{rule_id} should retain a localized null value"
        );
        assert_eq!(rule["affected"][0]["value"], Value::Null, "{rule_id}");
    }
}

#[test]
fn nullity_failures_localize_temporals_without_default_leakage() {
    let (directory, input) = typed_temporal_parquet();
    let constraints = temporal_constraints(
        directory.path(),
        "typed_temporal_localization",
        json!([
            {
                "id": "DATE_IS_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"is_null": {"column": "origination_date"}}
                }
            },
            {
                "id": "TIMESTAMP_IS_BLANK",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"is_blank": {"column": "last_paid_at"}}
                }
            },
            {
                "id": "TIME_IS_NULL",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"is_null": {"column": "cut_off_time"}}
                }
            },
            {
                "id": "INTERVAL_IS_BLANK",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"is_blank": {"column": "seasoning"}}
                }
            }
        ]),
    );

    let (default_output, default_report) = run_temporal_rules(&input, &constraints, false);
    assert_eq!(
        default_output.status.code(),
        Some(1),
        "report: {default_report}"
    );
    let default_text = String::from_utf8(default_output.stdout).expect("UTF-8 report");
    for raw in ["2024-01-31", "2026-01-31 10:00:00", "10:00:00", "1 day"] {
        assert!(
            !default_text.contains(raw),
            "default redaction leaked temporal value {raw}: {default_text}"
        );
    }

    let (explicit_output, explicit_report) = run_temporal_rules(&input, &constraints, true);
    assert_eq!(
        explicit_output.status.code(),
        Some(1),
        "report: {explicit_report}"
    );
    assert_eq!(
        result(&explicit_report, "DATE_IS_NULL")["affected"][0]["value"],
        "2024-01-31"
    );
    assert_eq!(
        result(&explicit_report, "TIMESTAMP_IS_BLANK")["affected"][0]["value"],
        "2026-01-31 10:00:00"
    );
    assert_eq!(
        result(&explicit_report, "TIME_IS_NULL")["affected"][0]["value"],
        "10:00:00"
    );
    let interval = result(&explicit_report, "INTERVAL_IS_BLANK")["affected"][0]["value"]
        .as_str()
        .expect("interval localization should be text");
    assert!(
        interval.contains("day"),
        "interval localization: {interval}"
    );
}
