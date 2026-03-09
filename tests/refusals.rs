/// Refusal integration tests for verify.
///
/// Tests that the verify CLI produces correct REFUSAL outcomes with proper
/// error codes for various invalid input scenarios.
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
    let tmp = std::env::temp_dir().join(format!(
        "verify-refusal-bad-json-{}",
        std::process::id()
    ));
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

    assert_eq!(output.status.code(), Some(2));

    let report: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["refusal"]["code"], "E_BAD_CONSTRAINTS");
}

// ---------------------------------------------------------------------------
// Missing binding
// ---------------------------------------------------------------------------

#[test]
fn run_missing_binding_produces_refusal() {
    let constraints = fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
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
