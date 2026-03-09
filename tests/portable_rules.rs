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
