/// Portable rule integration tests for verify.
///
/// These exercise the full compile → bind → evaluate pipeline for portable
/// rule types through the CLI binary, covering not_null, foreign_key, and
/// the predicate grammar.
use std::process::Command;

use serde_json::Value;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> String {
    format!("{WORKSPACE_ROOT}/{relative}")
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
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

/// The complement of the case above: a temporal column a rule genuinely reads
/// still refuses. Projection narrows what is materialized; it must not restore
/// the lossy `Debug` cast that silently misrepresented dates before 0.4.0.
#[test]
fn referenced_temporal_columns_still_refuse() {
    let authoring = concat!(
        "constraint_set_id: tests.referenced_temporal\n",
        "bindings:\n",
        "  input:\n",
        "    key_fields:\n",
        "      - loan_id\n",
        "rules:\n",
        "  - id: ORIGINATION_DATE_PRESENT\n",
        "    severity: error\n",
        "    binding: input\n",
        "    op: not_null\n",
        "    columns:\n",
        "      - origination_date\n",
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
