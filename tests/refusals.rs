/// Refusal integration tests for verify.
///
/// Tests that the verify CLI produces correct REFUSAL outcomes with proper
/// error codes for various invalid input scenarios.
use std::io;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> String {
    format!("{WORKSPACE_ROOT}/{relative}")
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
}

fn reserve_temp_directory(stem: &str) -> io::Result<PathBuf> {
    for suffix in 0..1_024 {
        let candidate = std::env::temp_dir().join(format!("{stem}-{suffix}"));
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not reserve a temporary test directory",
    ))
}

// ---------------------------------------------------------------------------
// IO errors
// ---------------------------------------------------------------------------

#[test]
fn run_missing_constraint_file_produces_io_refusal() {
    let output = verify_command()
        .args([
            "run",
            "nonexistent.verify.json",
            "--bind",
            "input=data.csv",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");

    assert_eq!(output.status.code(), Some(2));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_IO");
}

#[test]
fn shortcut_missing_rules_file_produces_authoring_refusal() {
    let output = verify_command()
        .args([
            "dataset.csv",
            "--rules",
            "nonexistent.yaml",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");

    assert_eq!(output.status.code(), Some(2));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_BAD_AUTHORING");
}

// ---------------------------------------------------------------------------
// Bad constraints
// ---------------------------------------------------------------------------

#[test]
fn run_malformed_json_produces_bad_constraints_refusal() {
    let directory = reserve_temp_directory("verify-refusal-bad-json")
        .expect("temporary directory should be reserved");
    let tmp = directory.join("constraints.verify.json");
    std::fs::write(&tmp, "{ not valid json }").expect("write tmp");

    let output = verify_command()
        .args([
            "run",
            tmp.to_str().unwrap(),
            "--bind",
            "input=data.csv",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");

    std::fs::remove_file(&tmp).ok();
    std::fs::remove_dir(directory).ok();

    assert_eq!(output.status.code(), Some(2));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_BAD_CONSTRAINTS");
}

#[test]
fn run_unknown_constraint_field_produces_bad_constraints_refusal() {
    let directory = reserve_temp_directory("verify-refusal-unknown-field")
        .expect("temporary directory should be reserved");
    let tmp = directory.join("constraints.verify.json");
    std::fs::write(
        &tmp,
        r#"{
            "version": "verify.constraint.v1",
            "constraint_set_id": "invalid.unknown_field",
            "bindings": [{"name": "input", "kind": "relation"}],
            "rules": [{
                "id": "VALUE_PRESENT",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"column": "value", "unexpected": "other"}
                }
            }]
        }"#,
    )
    .expect("write tmp");

    let output = verify_command()
        .args([
            "run",
            tmp.to_str().unwrap(),
            "--bind",
            "input=data.csv",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");
    std::fs::remove_file(tmp).ok();
    std::fs::remove_dir(directory).ok();

    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_BAD_CONSTRAINTS");
}

#[test]
fn run_incomparable_predicate_operands_produces_bad_expr_refusal() {
    let directory = reserve_temp_directory("verify-refusal-incomparable")
        .expect("temporary directory should be reserved");
    let constraints = directory.join("constraints.verify.json");
    let input = directory.join("input.csv");
    std::fs::write(
        &constraints,
        r#"{
            "version": "verify.constraint.v1",
            "constraint_set_id": "invalid.incomparable",
            "bindings": [{"name": "input", "kind": "relation", "key_fields": ["id"]}],
            "rules": [{
                "id": "VALUE_IS_ZERO",
                "severity": "error",
                "portability": "portable",
                "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": {"eq": [{"column": "value"}, "0"]}
                }
            }]
        }"#,
    )
    .expect("write constraints");
    std::fs::write(&input, "id,value\nrow-1,0\n").expect("write input");
    let bind = format!("input={}", input.display());

    let output = verify_command()
        .args([
            "run",
            constraints.to_str().unwrap(),
            "--bind",
            &bind,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");
    std::fs::remove_file(constraints).ok();
    std::fs::remove_file(input).ok();
    std::fs::remove_dir(directory).ok();

    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_BAD_EXPR");
    assert_eq!(report["refusal"]["detail"]["rule_id"], "VALUE_IS_ZERO");
    assert_eq!(report["refusal"]["detail"]["operator"], "eq");
    assert_eq!(report["refusal"]["detail"]["left_type"], "number");
    assert_eq!(report["refusal"]["detail"]["right_type"], "string");
    assert_eq!(report["refusal"]["detail"]["binding"], "input");
    assert_eq!(report["refusal"]["detail"]["key"]["id"], "row-1");
    assert_eq!(report["refusal"]["detail"]["field"], "value");
}

// ---------------------------------------------------------------------------
// Missing binding
// ---------------------------------------------------------------------------

#[test]
fn run_missing_binding_produces_refusal() {
    let constraints =
        fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
    // Only supply one of two required bindings
    let bind = format!(
        "property={}",
        fixture("fixtures/inputs/arity_n/property.csv")
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
        .expect("command should run");

    assert_eq!(output.status.code(), Some(2));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_MISSING_BINDING");
}

// ---------------------------------------------------------------------------
// Human mode refusals use stderr
// ---------------------------------------------------------------------------

#[test]
fn run_missing_file_human_mode_uses_stderr() {
    let output = verify_command()
        .args([
            "run",
            "nonexistent.verify.json",
            "--bind",
            "input=data.csv",
            "--no-witness",
        ])
        .output()
        .expect("command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("nonexistent.verify.json"));
}

// ---------------------------------------------------------------------------
// Refusal reports include next_step guidance
// ---------------------------------------------------------------------------

#[test]
fn refusal_report_includes_next_step() {
    let output = verify_command()
        .args([
            "run",
            "nonexistent.verify.json",
            "--bind",
            "input=data.csv",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("command should run");

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert!(
        report["refusal"]["next_step"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "refusal should include non-empty next_step"
    );
}
