use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> String {
    format!("{WORKSPACE_ROOT}/{relative}")
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
}

fn verify_command_with_ledger(ledger: &TestLedger) -> Command {
    let mut command = verify_command();
    command.env("EPISTEMIC_WITNESS", &ledger.path);
    command
}

struct TestLedger {
    root: PathBuf,
    path: PathBuf,
}

impl TestLedger {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "verify-cli-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));

        fs::create_dir_all(&root).expect("test ledger directory should be created");
        let path = root.join("witness.jsonl");

        Self { root, path }
    }

    fn read_json_lines(&self) -> Vec<Value> {
        let content = fs::read_to_string(&self.path).expect("witness ledger should exist");

        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("ledger line should be valid json"))
            .collect()
    }
}

impl Drop for TestLedger {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn help_output_lists_primary_run_surface() {
    let output = verify_command()
        .arg("--help")
        .output()
        .expect("help command should run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: verify"));
    assert!(stdout.contains("Commands:"));
    assert!(stdout.contains("--robot-triage"));
    assert!(stdout.contains("capabilities"));
    assert!(stdout.contains("robot-docs"));
    assert!(stdout.contains("run"));
    assert!(output.stderr.is_empty());
}

#[test]
fn bare_verify_explains_discovery_and_run_commands() {
    let output = verify_command().output().expect("bare command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no command or dataset was provided"));
    assert!(stderr.contains("verify --robot-triage"));
    assert!(stderr.contains("verify capabilities --json"));
    assert!(stderr.contains("verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH>"));
}

#[test]
fn run_help_mentions_sample_affected() {
    let output = verify_command()
        .args(["run", "--help"])
        .output()
        .expect("run help command should run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--sample-affected"));
    assert!(output.stderr.is_empty());
}

#[test]
fn version_output_uses_package_version() {
    let output = verify_command()
        .arg("--version")
        .output()
        .expect("version command should run");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains(env!("CARGO_PKG_VERSION")));
    assert!(output.stderr.is_empty());
}

#[test]
fn run_io_error_uses_stderr_and_exit_code_two() {
    let output = verify_command()
        .args([
            "run",
            "compiled.verify.json",
            "--bind",
            "input=data.csv",
            "--no-witness",
        ])
        .output()
        .expect("run command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("compiled.verify.json"));
}

#[test]
fn run_json_io_error_uses_stdout_and_protocol_shape() {
    let output = verify_command()
        .args([
            "run",
            "compiled.verify.json",
            "--bind",
            "input=data.csv",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("json run command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"outcome\":\"REFUSAL\""));
    assert!(stdout.contains("\"code\":\"E_IO\""));
}

#[test]
fn shortcut_bad_authoring_uses_stderr_and_exit_code_two() {
    let output = verify_command()
        .args(["dataset.csv", "--rules", "rules.yaml", "--no-witness"])
        .output()
        .expect("shortcut command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("rules.yaml"));
}

#[test]
fn shortcut_json_bad_authoring_uses_stdout_and_code() {
    let output = verify_command()
        .args([
            "dataset.csv",
            "--rules",
            "rules.yaml",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("json shortcut command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"outcome\":\"REFUSAL\""));
    assert!(stdout.contains("\"code\":\"E_BAD_AUTHORING\""));
}

#[test]
fn run_appends_witness_record_by_default() {
    let ledger = TestLedger::new("run-appends");

    let output = verify_command_with_ledger(&ledger)
        .args(["run", "compiled.verify.json", "--bind", "input=data.csv"])
        .output()
        .expect("run command should run");

    assert_eq!(output.status.code(), Some(2));

    let records = ledger.read_json_lines();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["tool"], "verify");
    assert_eq!(records[0]["command"], "run");
    assert_eq!(records[0]["outcome"], "REFUSAL");
    assert_eq!(records[0]["exit_code"], 2);
    assert_eq!(records[0]["inputs"][0]["path"], "compiled.verify.json");
    assert_eq!(records[0]["inputs"][1]["path"], "data.csv");
}

#[test]
fn no_witness_suppresses_ledger_append() {
    let ledger = TestLedger::new("run-no-witness");

    let output = verify_command_with_ledger(&ledger)
        .args([
            "run",
            "compiled.verify.json",
            "--bind",
            "input=data.csv",
            "--no-witness",
        ])
        .output()
        .expect("run command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(
        !ledger.path.exists(),
        "no witness file should be created when --no-witness is set"
    );
}

#[test]
fn witness_query_last_and_count_read_appended_records() {
    let ledger = TestLedger::new("witness-query");

    let run_output = verify_command_with_ledger(&ledger)
        .args(["run", "compiled.verify.json", "--bind", "input=data.csv"])
        .output()
        .expect("run command should run");
    assert_eq!(run_output.status.code(), Some(2));

    let shortcut_output = verify_command_with_ledger(&ledger)
        .args(["dataset.csv", "--rules", "rules.yaml"])
        .output()
        .expect("shortcut command should run");
    assert_eq!(shortcut_output.status.code(), Some(2));

    let query_output = verify_command_with_ledger(&ledger)
        .args(["witness", "query", "--json"])
        .output()
        .expect("witness query should run");
    assert_eq!(query_output.status.code(), Some(0));
    assert!(query_output.stderr.is_empty());

    let query_records: Vec<Value> =
        serde_json::from_slice(&query_output.stdout).expect("query output should be valid json");
    assert_eq!(query_records.len(), 2);
    assert_eq!(query_records[0]["tool"], "verify");
    assert_eq!(query_records[1]["command"], "shortcut");

    let last_output = verify_command_with_ledger(&ledger)
        .args(["witness", "last", "--json"])
        .output()
        .expect("witness last should run");
    assert_eq!(last_output.status.code(), Some(0));
    assert!(last_output.stderr.is_empty());

    let last_record: Value =
        serde_json::from_slice(&last_output.stdout).expect("last output should be valid json");
    assert_eq!(last_record["command"], "shortcut");
    assert_eq!(last_record["outcome"], "REFUSAL");

    let count_output = verify_command_with_ledger(&ledger)
        .args(["witness", "count", "--json"])
        .output()
        .expect("witness count should run");
    assert_eq!(count_output.status.code(), Some(0));
    assert!(count_output.stderr.is_empty());

    let count_record: Value =
        serde_json::from_slice(&count_output.stdout).expect("count output should be valid json");
    assert_eq!(count_record["count"], 2);
}

// ---------------------------------------------------------------------------
// End-to-end PASS / FAIL / shortcut tests
// ---------------------------------------------------------------------------

#[test]
fn run_pass_with_fixture_exits_zero() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!("input={}", fixture("fixtures/inputs/arity1/loans.csv"));
    let output = verify_command()
        .args(["run", &constraints, "--bind", &bind, "--no-witness"])
        .output()
        .expect("run pass command should run");

    assert_eq!(output.status.code(), Some(0), "PASS should exit 0");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY PASS"));
    assert!(stdout.contains("constraint_set: fixtures.arity1.not_null_loans"));
    assert!(stdout.contains("passed_rules: 1"));
    assert!(stdout.contains("failed_rules: 0"));
}

#[test]
fn run_pass_json_produces_protocol_report() {
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
        .expect("run pass json command should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(
        report["constraint_set_id"],
        "fixtures.arity1.not_null_loans"
    );
    assert_eq!(report["summary"]["passed_rules"], 1);
    assert_eq!(report["summary"]["failed_rules"], 0);
    assert_eq!(report["policy_signals"]["severity_band"], "CLEAN");
    assert!(
        report["bindings"]["input"]["content_hash"]
            .as_str()
            .is_some_and(|h| h.starts_with("sha256:"))
    );
}

#[test]
fn run_fail_with_missing_ids_exits_one() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!(
        "input={}",
        fixture("fixtures/inputs/arity1/loans_missing_id.csv")
    );
    let output = verify_command()
        .args(["run", &constraints, "--bind", &bind, "--no-witness"])
        .output()
        .expect("run fail command should run");

    assert_eq!(output.status.code(), Some(1), "FAIL should exit 1");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains("failed_rules: 1"));
    assert!(stdout.contains("FAIL INPUT_LOAN_ID_PRESENT"));
}

#[test]
fn run_fail_with_sample_affected_renders_localized_preview() {
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
            "--sample-affected",
            "1",
            "--explicit",
            "--no-witness",
        ])
        .output()
        .expect("run fail command with sample preview should run");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(
        stdout
            .contains("FAIL INPUT_LOAN_ID_PRESENT binding=input key.loan_id= field=loan_id value=")
    );
}

#[test]
fn run_fail_json_includes_affected_entries() {
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
        .expect("run fail json command should run");

    assert_eq!(output.status.code(), Some(1));

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["summary"]["failed_rules"], 1);
    assert_eq!(report["policy_signals"]["severity_band"], "ERROR_PRESENT");
    assert_eq!(report["results"][0]["rule_id"], "INPUT_LOAN_ID_PRESENT");
    assert_eq!(report["results"][0]["status"], "fail");
    assert!(report["results"][0]["violation_count"].as_u64().unwrap() >= 1);
}

#[test]
fn shortcut_pass_with_fixture_exits_zero() {
    let dataset = fixture("fixtures/inputs/arity1/loans.csv");
    let rules = fixture("fixtures/authoring/arity1/not_null_loans.yaml");
    let output = verify_command()
        .args([
            &dataset,
            "--rules",
            &rules,
            "--key",
            "loan_id",
            "--no-witness",
        ])
        .output()
        .expect("shortcut pass command should run");

    assert_eq!(output.status.code(), Some(0), "PASS shortcut should exit 0");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY PASS"));
    assert!(stdout.contains("passed_rules: 1"));
}

#[test]
fn shortcut_fail_exits_one() {
    let dataset = fixture("fixtures/inputs/arity1/loans_missing_id.csv");
    let rules = fixture("fixtures/authoring/arity1/not_null_loans.yaml");
    let output = verify_command()
        .args([
            &dataset,
            "--rules",
            &rules,
            "--key",
            "loan_id",
            "--no-witness",
        ])
        .output()
        .expect("shortcut fail command should run");

    assert_eq!(output.status.code(), Some(1), "FAIL shortcut should exit 1");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains("FAIL INPUT_LOAN_ID_PRESENT"));
}

#[test]
fn run_pass_appends_pass_witness_record() {
    let ledger = TestLedger::new("run-pass-witness");
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bind = format!("input={}", fixture("fixtures/inputs/arity1/loans.csv"));

    let output = verify_command_with_ledger(&ledger)
        .args(["run", &constraints, "--bind", &bind])
        .output()
        .expect("run pass command should run");

    assert_eq!(output.status.code(), Some(0));

    let records = ledger.read_json_lines();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["outcome"], "PASS");
    assert_eq!(records[0]["exit_code"], 0);
    assert_eq!(records[0]["command"], "run");
}

#[test]
fn witness_empty_ledger_is_success_with_a_zero_count() {
    let ledger = TestLedger::new("witness-empty");

    let output = verify_command_with_ledger(&ledger)
        .args(["witness", "count", "--json"])
        .output()
        .expect("witness count should run");

    // Family contract: an empty read is exit 0 (it was exit 1 before 0.5.2).
    assert_eq!(output.status.code(), Some(0));
    let stdout: Value =
        serde_json::from_slice(&output.stdout).expect("count output should be valid json");
    assert_eq!(stdout["count"], 0);
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "verify: no witness records matched"
    );
}

// ---------------------------------------------------------------------------
// Hostile inputs (bd-16h): refused with a typed code and no side effect
// ---------------------------------------------------------------------------

fn write_query_constraints(ledger: &TestLedger, query: &str) -> PathBuf {
    let constraints = serde_json::json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": "hostile.query",
        "bindings": [{"name": "property", "kind": "relation", "key_fields": ["property_id"]}],
        "rules": [{
            "id": "HOSTILE",
            "severity": "error",
            "portability": "batch_only",
            "check": {"op": "query_zero_rows", "bindings": ["property"], "query": query}
        }]
    });
    let path = ledger.root.join("hostile.verify.json");
    fs::write(&path, constraints.to_string()).expect("write constraints");
    path
}

fn run_refusal_json(constraints: &std::path::Path, bind: &str) -> (std::process::Output, Value) {
    let output = verify_command()
        .args([
            "run",
            &constraints.to_string_lossy(),
            "--bind",
            bind,
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("verify run should execute");
    let report: Value = serde_json::from_slice(&output.stdout).unwrap_or(Value::Null);
    (output, report)
}

#[test]
fn hostile_query_rules_are_refused_by_the_binary_without_side_effects() {
    let ledger = TestLedger::new("hostile-rules");
    let exfil = ledger.root.join("exfil.csv");
    let attached = ledger.root.join("attached.db");
    let bind = format!(
        "property={}",
        fixture("fixtures/inputs/arity_n/property.csv")
    );

    for query in [
        format!("COPY (SELECT * FROM property) TO '{}'", exfil.display()),
        format!("ATTACH '{}' AS other", attached.display()),
        "SELECT * FROM read_csv_auto('/etc/hosts')".to_owned(),
        "SELECT * FROM 'https://127.0.0.1:9/x.csv'".to_owned(),
        "SET enable_external_access = true".to_owned(),
        "SELECT 1 WHERE false; DELETE FROM property".to_owned(),
    ] {
        let constraints = write_query_constraints(&ledger, &query);
        let (output, report) = run_refusal_json(&constraints, &bind);
        assert_eq!(output.status.code(), Some(2), "{query}");
        assert_eq!(report["outcome"], "REFUSAL", "{query}: {report}");
        assert_eq!(
            report["refusal"]["code"], "E_BAD_CONSTRAINTS",
            "{query}: {report}"
        );
    }
    assert!(!exfil.exists(), "a refused COPY must not write a file");
    assert!(
        !attached.exists(),
        "a refused ATTACH must not create a file"
    );
}

#[test]
fn url_and_glob_bind_paths_are_refused_before_any_io() {
    let ledger = TestLedger::new("hostile-binds");
    let constraints = write_query_constraints(&ledger, "SELECT 1 WHERE false");
    for bind in [
        "property=https://127.0.0.1:9/x.csv".to_owned(),
        "property=s3://bucket/x.parquet".to_owned(),
        "property=http://127.0.0.1:9/x.csv".to_owned(),
        format!("property={}", fixture("fixtures/inputs/arity_n/*.csv")),
    ] {
        let (output, report) = run_refusal_json(&constraints, &bind);
        assert_eq!(output.status.code(), Some(2), "{bind}");
        assert_eq!(report["refusal"]["code"], "E_IO", "{bind}: {report}");
    }
}

#[test]
fn a_legitimate_query_rule_still_runs_through_the_binary() {
    let ledger = TestLedger::new("legit-query");
    let constraints = write_query_constraints(
        &ledger,
        "SELECT property_id AS key__property_id FROM property WHERE tenant_id = 'T-999'",
    );
    let bind = format!(
        "property={}",
        fixture("fixtures/inputs/arity_n/property.csv")
    );
    let (output, report) = run_refusal_json(&constraints, &bind);
    assert_eq!(
        output.status.code(),
        Some(1),
        "FAIL, not a refusal: {report}"
    );
    assert_eq!(report["outcome"], "FAIL");
}

// ---------------------------------------------------------------------------
// Arity-N (multi-binding) end-to-end tests
// ---------------------------------------------------------------------------

#[test]
fn run_arity_n_foreign_key_fail_exits_one() {
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
            "--no-witness",
        ])
        .output()
        .expect("arity-N fail command should run");

    assert_eq!(output.status.code(), Some(1), "orphan FK should exit 1");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains("failed_rules: 1"));
    assert!(stdout.contains("FAIL PROPERTY_TENANT_EXISTS"));
}

#[test]
fn run_arity_n_foreign_key_pass_exits_zero() {
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
            "--no-witness",
        ])
        .output()
        .expect("arity-N pass command should run");

    assert_eq!(output.status.code(), Some(0), "clean FK should exit 0");
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY PASS"));
    assert!(stdout.contains("passed_rules: 1"));
    assert!(stdout.contains("failed_rules: 0"));
}

#[test]
fn run_arity_n_json_fail_includes_affected_entries() {
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
        .expect("arity-N json fail command should run");

    assert_eq!(output.status.code(), Some(1));

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(
        report["constraint_set_id"],
        "fixtures.arity_n.foreign_key_property_tenants"
    );
    assert_eq!(report["summary"]["failed_rules"], 1);
    assert_eq!(report["results"][0]["rule_id"], "PROPERTY_TENANT_EXISTS");
    assert!(report["results"][0]["violation_count"].as_u64().unwrap() >= 1);

    // Both bindings should appear in the report
    assert!(
        report["bindings"]["property"]["content_hash"]
            .as_str()
            .is_some_and(|h| h.starts_with("sha256:"))
    );
    assert!(
        report["bindings"]["tenants"]["content_hash"]
            .as_str()
            .is_some_and(|h| h.starts_with("sha256:"))
    );
}

// ---------------------------------------------------------------------------
// Query rules (batch_only) end-to-end tests
// ---------------------------------------------------------------------------

#[test]
fn run_query_rules_fail_exits_one() {
    let constraints = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
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
            "--no-witness",
        ])
        .output()
        .expect("query rules fail command should run");

    assert_eq!(
        output.status.code(),
        Some(1),
        "orphan query rule should exit 1"
    );
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains("FAIL ORPHAN_PROPERTY_TENANT"));
}

#[test]
fn run_query_rules_pass_exits_zero() {
    let constraints = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
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
            "--no-witness",
        ])
        .output()
        .expect("query rules pass command should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "clean query rule should exit 0"
    );
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY PASS"));
    assert!(stdout.contains("passed_rules: 1"));
}

#[test]
fn run_query_rules_json_fail_has_localization() {
    let constraints = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
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
            "--explicit",
            "--no-witness",
        ])
        .output()
        .expect("query rules json fail command should run");

    assert_eq!(output.status.code(), Some(1));

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["results"][0]["rule_id"], "ORPHAN_PROPERTY_TENANT");

    // The query localization contract should produce affected entries with binding, field, value, key
    let affected = &report["results"][0]["affected"][0];
    assert_eq!(affected["binding"], "property");
    assert_eq!(affected["field"], "tenant_id");
    assert_eq!(affected["value"], "T-999");
    assert!(affected["key"]["property_id"].as_str().is_some());
}

#[test]
fn run_query_rules_human_sample_preview_shows_binding_key_field_and_value() {
    let constraints = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
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
            "--sample-affected",
            "1",
            "--explicit",
            "--no-witness",
        ])
        .output()
        .expect("query rules human preview command should run");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains(
        "FAIL ORPHAN_PROPERTY_TENANT binding=property key.property_id=P-003 field=tenant_id value=T-999"
    ));
}

// ---------------------------------------------------------------------------
// Validate command end-to-end tests
// ---------------------------------------------------------------------------

#[test]
fn validate_accepts_valid_compiled_constraints() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let output = verify_command()
        .args(["validate", &constraints])
        .output()
        .expect("validate command should run");

    assert_eq!(
        output.status.code(),
        Some(0),
        "valid constraints should exit 0"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn validate_json_prints_summary() {
    let constraints =
        fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
    let output = verify_command()
        .args(["validate", &constraints, "--json"])
        .output()
        .expect("validate json command should run");

    assert_eq!(output.status.code(), Some(0));

    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(summary["valid"], true);
    assert_eq!(
        summary["constraint_set_id"],
        "fixtures.arity_n.foreign_key_property_tenants"
    );
    assert_eq!(summary["bindings"], 2);
    assert_eq!(summary["rules"], 1);
}

#[test]
fn validate_rejects_missing_file() {
    let output = verify_command()
        .args(["validate", "nonexistent.verify.json"])
        .output()
        .expect("validate command should run");

    assert_eq!(output.status.code(), Some(2));

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to read"));
}

// ---------------------------------------------------------------------------
// Schema output test
// ---------------------------------------------------------------------------

#[test]
fn schema_flag_prints_report_schema() {
    let output = verify_command()
        .arg("--schema")
        .output()
        .expect("schema flag should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"title\": \"verify.report.v1\""));
}

// ---------------------------------------------------------------------------
// Default redaction of failing-row output (bd-q4d)
// ---------------------------------------------------------------------------

const CANARY_KEY: &str = "CANARY-KEY-7f3a91";
const CANARY_CELL: &str = "CANARY-CELL-c2d8e4";

fn write_canary_input(ledger: &TestLedger) -> String {
    let path = ledger.root.join("canary.csv");
    fs::write(
        &path,
        format!("property_id,secret\n{CANARY_KEY},{CANARY_CELL}\n"),
    )
    .expect("write canary input");
    format!("property={}", path.display())
}

fn run_with(constraints: &std::path::Path, bind: &str, extra: &[&str]) -> std::process::Output {
    verify_command()
        .env_remove("VERIFY_REDACTION_KEY")
        .args(["run", &constraints.to_string_lossy(), "--bind", bind])
        .args(extra)
        .arg("--no-witness")
        .output()
        .expect("verify run should execute")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn failing_rows_are_redacted_by_default_and_raw_only_with_explicit() {
    let ledger = TestLedger::new("redact-rows");
    let bind = write_canary_input(&ledger);
    let constraints = write_query_constraints(
        &ledger,
        "SELECT property_id AS key__property_id, secret AS value FROM property",
    );

    for extra in [
        &["--json"][..],
        &[][..],
        &["--json", "--sample-affected", "1"][..],
    ] {
        let output = run_with(&constraints, &bind, extra);
        assert_eq!(output.status.code(), Some(1), "{extra:?}");
        let text = combined(&output);
        assert!(!text.contains(CANARY_KEY), "key leaked ({extra:?}): {text}");
        assert!(
            !text.contains(CANARY_CELL),
            "cell leaked ({extra:?}): {text}"
        );
        assert!(text.contains("redacted:ref:k1"), "{extra:?}: {text}");
        assert!(text.contains("redacted:ref:v1"), "{extra:?}: {text}");
        assert!(text.contains("HOSTILE"), "rule id must stay: {text}");
    }

    // JSON shape is kept: counts, binding and key column name survive redaction.
    let output = run_with(&constraints, &bind, &["--json"]);
    let report: Value = serde_json::from_slice(&output.stdout).expect("json report");
    let entry = &report["results"][0]["affected"][0];
    assert_eq!(report["results"][0]["violation_count"], 1);
    assert_eq!(entry["binding"], "property");
    assert!(
        entry["key"]["property_id"]
            .as_str()
            .is_some_and(|t| t == "redacted:ref:k1")
    );
    // Deterministic: a second run gives byte-identical output.
    let again = run_with(&constraints, &bind, &["--json"]);
    assert_eq!(output.stdout, again.stdout);

    // Control: --explicit restores the raw values, so the redaction above is
    // the flag's doing and not a test that can never see the data.
    let explicit = run_with(&constraints, &bind, &["--json", "--explicit"]);
    let text = combined(&explicit);
    assert!(
        text.contains(CANARY_KEY) && text.contains(CANARY_CELL),
        "{text}"
    );
    let human = run_with(&constraints, &bind, &["--explicit"]);
    assert!(combined(&human).contains(CANARY_CELL));
}

#[test]
fn sql_error_text_that_quotes_a_cell_is_redacted_by_default() {
    let ledger = TestLedger::new("redact-sql-error");
    let bind = write_canary_input(&ledger);
    // DuckDB's conversion error quotes the offending string.
    let constraints = write_query_constraints(
        &ledger,
        "SELECT property_id AS key__property_id FROM property WHERE CAST(secret AS INTEGER) > 0",
    );

    let explicit = run_with(&constraints, &bind, &["--json", "--explicit"]);
    assert_eq!(explicit.status.code(), Some(2));
    assert!(
        combined(&explicit).contains(CANARY_CELL),
        "control: DuckDB error text should quote the cell: {}",
        combined(&explicit)
    );

    for extra in [&["--json"][..], &[][..]] {
        let output = run_with(&constraints, &bind, extra);
        assert_eq!(output.status.code(), Some(2), "{extra:?}");
        let text = combined(&output);
        assert!(
            !text.contains(CANARY_CELL),
            "SQL error leaked ({extra:?}): {text}"
        );
        assert!(!text.contains(CANARY_KEY), "{text}");
    }
}

#[test]
fn run_lock_accepts_real_lock_v0_blake3_and_sha256_and_refuses_stale_input() {
    let constraints = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let loans = fixture("fixtures/locks/lock_v0/loans.csv");
    for lock_file in ["sha256.lock.json", "blake3.lock.json"] {
        let lock = fixture(&format!("fixtures/locks/lock_v0/{lock_file}"));
        let output = verify_command()
            .args([
                "run",
                &constraints,
                "--bind",
                &format!("input={loans}"),
                "--lock",
                &lock,
                "--json",
                "--no-witness",
            ])
            .output()
            .expect("verify run should execute");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{lock_file}: {}",
            combined(&output)
        );
        let report: Value = serde_json::from_slice(&output.stdout).expect("json report");
        assert_eq!(report["outcome"], "PASS", "{lock_file}");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(lock_file),
            "the report should name the vouching lock {lock_file}"
        );

        // Stale input: same name, different bytes.
        let ledger = TestLedger::new("stale-lock");
        let stale = ledger.root.join("loans.csv");
        fs::write(&stale, "loan_id,balance\nLN-1,10\nLN-2,21\n").expect("write stale csv");
        let output = verify_command()
            .args([
                "run",
                &constraints,
                "--bind",
                &format!("input={}", stale.display()),
                "--lock",
                &lock,
                "--json",
                "--no-witness",
            ])
            .output()
            .expect("verify run should execute");
        assert_eq!(output.status.code(), Some(2), "{lock_file}");
        let report: Value = serde_json::from_slice(&output.stdout).expect("json refusal");
        assert_eq!(report["refusal"]["code"], "E_INPUT_DRIFT", "{lock_file}");
    }
}
