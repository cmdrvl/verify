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
    // Default output redacts the key tuple the refusal would otherwise quote.
    let key_token = report["refusal"]["detail"]["key"]["id"]
        .as_str()
        .expect("key id token");
    assert_eq!(key_token, "redacted:ref:k1");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("row-1"));
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

// ---------------------------------------------------------------------------
// Non-finite numbers in authoring (bd-zw7)
// ---------------------------------------------------------------------------

/// A predicate literal of NaN or an infinity has no protocol number. Compiled as null it is
/// satisfied by a null row, so authoring must refuse it, one-step and `compile` alike.
#[test]
fn non_finite_predicate_literals_are_refused_not_compiled_as_null() {
    let directory = reserve_temp_directory("verify-refusal-non-finite")
        .expect("temporary directory should be reserved");
    let rules = directory.join("rules.yaml");
    let data = directory.join("data.jsonl");
    std::fs::write(&data, "{\"v\":null}\n{\"v\":5}\n").expect("write data");

    let mut wrong = Vec::new();
    for expr in [
        "{ in: [ { column: v }, [ .inf, 5 ] ] }",
        "{ in: [ { column: v }, [ 5, -.inf ] ] }",
        "{ in: [ { column: v }, [ .NaN, 5 ] ] }",
        "{ eq: [ { column: v }, .nan ] }",
        "{ or: [ { eq: [ { column: v }, 5 ] }, { eq: [ { column: v }, +.Inf ] } ] }",
        "{ not: { ne: [ { column: v }, !!float .nan ] } }",
    ] {
        std::fs::write(
            &rules,
            format!(
                "constraint_set_id: non_finite\nbindings:\n  input: {{}}\nrules:\n  - id: LITERAL\n    severity: error\n    binding: input\n    op: predicate\n    expr: {expr}\n"
            ),
        )
        .expect("write rules");

        let one_step = verify_command()
            .arg(&data)
            .arg("--rules")
            .arg(&rules)
            .args(["--json", "--no-witness"])
            .output()
            .expect("command should run");
        let report: Value = serde_json::from_slice(&one_step.stdout).expect("valid json");
        let compile = verify_command()
            .arg("compile")
            .arg(&rules)
            .output()
            .expect("command should run");

        if one_step.status.code() != Some(2) || report["refusal"]["code"] != "E_BAD_AUTHORING" {
            wrong.push(format!(
                "one-step {expr}: exit {:?}, outcome {}, failed_rules {}",
                one_step.status.code(),
                report["outcome"],
                report["summary"]["failed_rules"]
            ));
        }
        if compile.status.success() || !compile.stdout.is_empty() {
            wrong.push(format!(
                "compile {expr}: {}",
                String::from_utf8_lossy(&compile.stdout)
                    .split_whitespace()
                    .collect::<String>()
            ));
        }
    }

    std::fs::remove_file(rules).ok();
    std::fs::remove_file(data).ok();
    std::fs::remove_dir(directory).ok();

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The neighbours of the refused spelling still compile: a quoted ".inf" is a string, and an
/// authored null stays a null.
#[test]
fn quoted_non_finite_text_and_authored_null_still_compile() {
    let directory = reserve_temp_directory("verify-refusal-quoted-non-finite")
        .expect("temporary directory should be reserved");
    let rules = directory.join("rules.yaml");
    std::fs::write(
        &rules,
        "constraint_set_id: quoted\nbindings:\n  input: {}\nrules:\n  - id: LITERAL\n    severity: error\n    binding: input\n    op: predicate\n    expr: { in: [ { column: v }, [ \".inf\", \".nan\", null, 5 ] ] }\n",
    )
    .expect("write rules");

    let compile = verify_command()
        .arg("compile")
        .arg(&rules)
        .output()
        .expect("command should run");

    std::fs::remove_file(rules).ok();
    std::fs::remove_dir(directory).ok();

    assert!(compile.status.success(), "{compile:?}");
    let artifact: Value = serde_json::from_slice(&compile.stdout).expect("valid json");
    assert_eq!(
        artifact["rules"][0]["check"]["expr"]["in"][1],
        serde_json::json!([".inf", ".nan", null, 5])
    );
}

// ---------------------------------------------------------------------------
// One-step --key against authored key_fields (bd-6dv)
// ---------------------------------------------------------------------------

const KEY_DATA: &str = "id,v\n1,a\n2,b\n";

fn keyed_authoring(binding: &str) -> String {
    format!(
        "constraint_set_id: keys\nbindings:\n  input: {binding}\nrules:\n  - id: V_UNIQUE\n    severity: error\n    binding: input\n    op: unique\n    columns: [v]\n"
    )
}

const KEYED_SQL: &str = "-- verify.constraint_set_id: keys\n-- verify.rule_id: NO_ROWS\n-- verify.severity: error\n-- verify.bindings: input\nSELECT id AS key__id FROM input WHERE id < 0\n";

/// Runs the one-step path over `KEY_DATA` and returns the exit code and JSON report.
fn one_step(
    directory: &std::path::Path,
    file_name: &str,
    authoring: &str,
    key: Option<&str>,
) -> (Option<i32>, Value) {
    let rules = directory.join(file_name);
    let data = directory.join("data.csv");
    std::fs::write(&rules, authoring).expect("write rules");
    std::fs::write(&data, KEY_DATA).expect("write data");

    let mut command = verify_command();
    command.arg(&data).arg("--rules").arg(&rules);
    if let Some(key) = key {
        command.args(["--key", key]);
    }
    let output = command
        .args(["--json", "--no-witness"])
        .output()
        .expect("command should run");

    std::fs::remove_file(rules).ok();
    std::fs::remove_file(data).ok();

    (
        output.status.code(),
        serde_json::from_slice(&output.stdout).expect("valid json"),
    )
}

/// docs/plan.md: a `--key` that disagrees with authored `key_fields` refuses.
#[test]
fn cli_key_conflict() {
    let directory = reserve_temp_directory("verify-refusal-key-conflict")
        .expect("temporary directory should be reserved");
    let single = keyed_authoring("{ key_fields: [id] }");
    let composite = keyed_authoring("{ key_fields: [id, v] }");
    let cases: [(&str, &str, &str, &[&str]); 5] = [
        ("rules.yaml", &single, "v", &["id"]),
        ("rules.yaml", &single, "no_such_column", &["id"]),
        ("rules.yaml", &single, "ID", &["id"]),
        ("rules.yaml", &composite, "id", &["id", "v"]),
        ("rules.sql", KEYED_SQL, "v", &["id"]),
    ];

    let mut wrong = Vec::new();
    for (file_name, authoring, key, authored) in cases {
        let (_, unflagged) = one_step(&directory, file_name, authoring, None);
        let (code, report) = one_step(&directory, file_name, authoring, Some(key));
        if code != Some(2) {
            wrong.push(format!(
                "{authored:?} with --key {key}: exit {code:?}, outcome {}",
                report["outcome"]
            ));
            continue;
        }
        assert_eq!(report["outcome"], "REFUSAL");
        assert_eq!(report["refusal"]["code"], "E_KEY_CONFLICT");
        assert_eq!(
            report["refusal"]["detail"],
            serde_json::json!({
                "binding": "input",
                "authored_key_fields": authored,
                "cli_key_field": key,
            })
        );
        // The refusal names the authored contract, not an overridden one.
        assert_eq!(report["constraint_hash"], unflagged["constraint_hash"]);
    }

    std::fs::remove_dir(directory).ok();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The neighbours of a conflict keep working: a `--key` equal to the authored key, and a
/// `--key` supplying one where none was authored. Both evaluate the same contract.
#[test]
fn cli_key_that_agrees_or_supplies_a_key_still_evaluates() {
    let directory = reserve_temp_directory("verify-refusal-key-agrees")
        .expect("temporary directory should be reserved");
    let keyed = keyed_authoring("{ key_fields: [id] }");
    let unkeyed = keyed_authoring("{}");

    let (_, authored) = one_step(&directory, "rules.yaml", &keyed, None);
    for (file_name, authoring) in [("rules.yaml", &keyed), ("rules.yaml", &unkeyed)] {
        let (code, report) = one_step(&directory, file_name, authoring, Some("id"));
        assert_eq!(code, Some(0), "{report}");
        assert_eq!(report["outcome"], "PASS");
        assert_eq!(report["constraint_hash"], authored["constraint_hash"]);
    }
    let (code, report) = one_step(&directory, "rules.sql", KEYED_SQL, Some("id"));
    assert_eq!(code, Some(0), "{report}");

    std::fs::remove_dir(directory).ok();
}
