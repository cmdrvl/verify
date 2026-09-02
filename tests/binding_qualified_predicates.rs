use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> PathBuf {
    Path::new(WORKSPACE_ROOT).join(relative)
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify")) // ubs:ignore — Cargo supplies this trusted test-binary path.
}

struct TempScenario {
    root: PathBuf,
}

impl TempScenario {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "verify-binding-predicate-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("scenario directory should be created");
        Self { root }
    }

    fn write_json(&self, name: &str, value: &Value) -> PathBuf {
        let path = self.root.join(name);
        fs::write(
            &path,
            serde_json::to_vec_pretty(value).expect("fixture should serialize"),
        )
        .expect("fixture should be written");
        path
    }

    fn write_text(&self, name: &str, value: &str) -> PathBuf {
        let path = self.root.join(name);
        fs::write(&path, value).expect("fixture should be written");
        path
    }
}

impl Drop for TempScenario {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_json(compiled: &Path, current: &Path, prior: &Path) -> Output {
    verify_command()
        .args([
            "run",
            compiled.to_str().expect("compiled path should be utf-8"),
            "--bind",
            &format!("current={}", current.display()),
            "--bind",
            &format!("prior={}", prior.display()),
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("verify run should execute")
}

fn report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should contain a JSON report")
}

fn result_by_rule<'a>(report: &'a Value, rule_id: &str) -> &'a Value {
    report["results"]
        .as_array()
        .expect("results should be an array")
        .iter()
        .find(|result| result["rule_id"] == rule_id)
        .expect("rule result should be present")
}

#[test]
fn compile_validate_and_run_binding_qualified_fixture() {
    let scenario = TempScenario::new("compile-validate");
    let authoring = fixture("fixtures/authoring/binding_qualified/maturity_date_immutable.yaml");
    let expected =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");
    let compiled = scenario.root.join("compiled.verify.json");

    let compile = verify_command()
        .args([
            "compile",
            authoring.to_str().expect("authoring path should be utf-8"),
            "--out",
            compiled.to_str().expect("output path should be utf-8"),
        ])
        .output()
        .expect("compile should execute");
    assert_eq!(
        compile.status.code(),
        Some(0),
        "compile stderr: {}",
        String::from_utf8_lossy(&compile.stderr)
    );

    let actual_value: Value =
        serde_json::from_slice(&fs::read(&compiled).expect("compiled fixture should exist"))
            .expect("compiled output should parse");
    let expected_value: Value = serde_json::from_slice(
        &fs::read(expected).expect("checked-in compiled fixture should exist"),
    )
    .expect("checked-in fixture should parse");
    assert_eq!(actual_value, expected_value);
    assert_eq!(actual_value["rules"][0]["portability"], "batch_only");

    let validate = verify_command()
        .args([
            "validate",
            compiled.to_str().expect("compiled path should be utf-8"),
            "--json",
        ])
        .output()
        .expect("validate should execute");
    assert_eq!(
        validate.status.code(),
        Some(0),
        "validate stderr: {}",
        String::from_utf8_lossy(&validate.stderr)
    );
    let validation: Value =
        serde_json::from_slice(&validate.stdout).expect("validate output should be JSON");
    assert_eq!(validation["valid"], true);

    let execution = run_json(
        &compiled,
        &fixture("fixtures/inputs/binding_qualified/current.csv"),
        &fixture("fixtures/inputs/binding_qualified/prior_matching.csv"),
    );
    assert_eq!(execution.status.code(), Some(0));
    let execution_report = report(&execution);
    assert_eq!(execution_report["outcome"], "PASS");
    assert_eq!(
        execution_report["results"][0]["rule_id"],
        "MATURITY_DATE_IMMUTABLE"
    );
    assert_eq!(execution_report["results"][0]["status"], "pass");
}

#[test]
fn pass_ignores_non_anchor_only_rows_and_fail_localizes_complete_anchor_key() {
    let compiled =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");
    let current = fixture("fixtures/inputs/binding_qualified/current.csv");

    let passing = run_json(
        &compiled,
        &current,
        &fixture("fixtures/inputs/binding_qualified/prior_matching.csv"),
    );
    assert_eq!(passing.status.code(), Some(0));
    assert_eq!(report(&passing)["outcome"], "PASS");

    let failing = run_json(
        &compiled,
        &current,
        &fixture("fixtures/inputs/binding_qualified/prior_changed.csv"),
    );
    assert_eq!(failing.status.code(), Some(1));
    assert!(failing.stderr.is_empty());
    let failing_report = report(&failing);
    assert_eq!(failing_report["outcome"], "FAIL");
    assert_eq!(failing_report["summary"]["failed_rules"], 1);
    assert_eq!(failing_report["results"][0]["violation_count"], 1);

    let affected = &failing_report["results"][0]["affected"][0];
    assert_eq!(affected["binding"], "current");
    assert_eq!(
        affected["key"],
        json!({ "loan_id": "LN-200", "tranche_id": "B" })
    );
    assert_eq!(affected["field"], "maturity_date");
    assert_eq!(affected["value"], "M2031-06-30");
}

#[test]
fn human_failure_preserves_exit_and_anchor_localization_contract() {
    let compiled =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");
    let current = fixture("fixtures/inputs/binding_qualified/current.csv");
    let prior = fixture("fixtures/inputs/binding_qualified/prior_changed.csv");
    let output = verify_command()
        .args([
            "run",
            compiled.to_str().expect("compiled path should be utf-8"),
            "--bind",
            &format!("current={}", current.display()),
            "--bind",
            &format!("prior={}", prior.display()),
            "--sample-affected",
            "1",
            "--no-witness",
        ])
        .output()
        .expect("human run should execute");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));
    assert!(stdout.contains("FAIL MATURITY_DATE_IMMUTABLE binding=current"));
    assert!(stdout.contains("key.loan_id=LN-200"));
    assert!(stdout.contains("key.tranche_id=B"));
}

#[test]
fn complete_predicate_grammar_executes_end_to_end_across_two_bindings() {
    let scenario = TempScenario::new("grammar");
    let column = |binding: &str, name: &str| json!({ "binding": binding, "column": name });
    let constraint = json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": "integration.binding_qualified.full_grammar",
        "bindings": [
            { "name": "current", "kind": "relation", "key_fields": ["loan_id", "tranche_id"] },
            { "name": "prior", "kind": "relation", "key_fields": ["asset_number", "class_code"] }
        ],
        "rules": [{
            "id": "FULL_GRAMMAR",
            "severity": "error",
            "portability": "batch_only",
            "check": {
                "op": "predicate",
                "binding": "current",
                "expr": { "and": [
                    { "eq": [column("current", "maturity_date"), column("prior", "maturity_date")] },
                    { "ne": [column("current", "amount"), column("prior", "amount")] },
                    { "gt": [column("current", "amount"), column("prior", "amount")] },
                    { "gte": [column("current", "amount"), column("prior", "amount")] },
                    { "lt": [column("prior", "amount"), column("current", "amount")] },
                    { "lte": [column("prior", "amount"), column("current", "amount")] },
                    { "in": [column("current", "status"), ["open", "closed"]] },
                    { "or": [
                        { "eq": [column("current", "status"), column("prior", "status")] },
                        { "ne": [column("current", "status"), column("prior", "status")] }
                    ] },
                    { "not": { "ne": [column("current", "maturity_date"), column("prior", "maturity_date")] } },
                    { "or": [
                        { "is_null": column("prior", "nullable_value") },
                        { "eq": [column("current", "status"), column("prior", "status")] }
                    ] },
                    { "is_blank": column("current", "note") },
                    column("current", "active")
                ] }
            }
        }]
    });
    let compiled = scenario.write_json("grammar.verify.json", &constraint);
    let output = run_json(
        &compiled,
        &fixture("fixtures/inputs/binding_qualified/current.csv"),
        &fixture("fixtures/inputs/binding_qualified/prior_matching.csv"),
    );

    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = report(&output);
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["results"][0]["rule_id"], "FULL_GRAMMAR");
    assert_eq!(report["results"][0]["violation_count"], 0);
}

#[test]
fn temporal_loan_date_rules_execute_end_to_end() {
    let scenario = TempScenario::new("temporal-loan-dates");
    let column = |binding: &str, name: &str| json!({ "binding": binding, "column": name });
    let constraint = json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": "integration.binding_qualified.temporal_loan_dates",
        "bindings": [
            { "name": "current", "kind": "relation", "key_fields": ["ASSETNUMBER"] },
            { "name": "prior", "kind": "relation", "key_fields": ["ASSETNUMBER"] }
        ],
        "rules": [
            {
                "id": "MATURITY_DATE_IMMUTABLE",
                "severity": "error",
                "portability": "batch_only",
                "check": {
                    "op": "predicate",
                    "binding": "current",
                    "expr": {
                        "eq": [column("current", "MATURITYDATE"), column("prior", "MATURITYDATE")]
                    }
                }
            },
            {
                "id": "ORIGINATION_DATE_IMMUTABLE",
                "severity": "error",
                "portability": "batch_only",
                "check": {
                    "op": "predicate",
                    "binding": "current",
                    "expr": {
                        "eq": [column("current", "ORIGINATIONDATE"), column("prior", "ORIGINATIONDATE")]
                    }
                }
            },
            {
                "id": "FIRST_PAYMENT_DUE_DATE_IMMUTABLE",
                "severity": "error",
                "portability": "batch_only",
                "check": {
                    "op": "predicate",
                    "binding": "current",
                    "expr": {
                        "eq": [
                            column("current", "FIRSTLOANPAYMENTDUEDATE"),
                            column("prior", "FIRSTLOANPAYMENTDUEDATE")
                        ]
                    }
                }
            },
            {
                "id": "POSTMODIFICATION_MATURITY_DATE_MONOTONIC",
                "severity": "error",
                "portability": "batch_only",
                "check": {
                    "op": "predicate",
                    "binding": "current",
                    "expr": {
                        "gte": [
                            column("current", "POSTMODIFICATIONMATURITYDATE"),
                            column("prior", "POSTMODIFICATIONMATURITYDATE")
                        ]
                    }
                }
            },
            {
                "id": "PAID_THROUGH_DATE_MONOTONIC",
                "severity": "error",
                "portability": "batch_only",
                "check": {
                    "op": "predicate",
                    "binding": "current",
                    "expr": {
                        "gte": [column("current", "PAIDTHROUGHDATE"), column("prior", "PAIDTHROUGHDATE")]
                    }
                }
            }
        ]
    });
    let compiled = scenario.write_json("temporal-loan-dates.verify.json", &constraint);
    let prior = scenario.write_text(
        "prior.csv",
        "ASSETNUMBER,MATURITYDATE,ORIGINATIONDATE,FIRSTLOANPAYMENTDUEDATE,POSTMODIFICATIONMATURITYDATE,PAIDTHROUGHDATE\n\
         100,2030-01-10,2020-02-15,2020-03-01,2030-01-10,2026-08-01\n\
         200,2032-12-31,2021-05-20,2021-06-01,2033-06-30,2026-08-15\n",
    );
    let current_pass = scenario.write_text(
        "current-pass.csv",
        "ASSETNUMBER,MATURITYDATE,ORIGINATIONDATE,FIRSTLOANPAYMENTDUEDATE,POSTMODIFICATIONMATURITYDATE,PAIDTHROUGHDATE\n\
         100,2030-01-10,2020-02-15,2020-03-01,2031-01-10,2026-09-01\n\
         200,2032-12-31,2021-05-20,2021-06-01,2033-12-31,2026-08-15\n",
    );
    let pass = run_json(&compiled, &current_pass, &prior);
    assert_eq!(
        pass.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&pass.stdout),
        String::from_utf8_lossy(&pass.stderr)
    );
    let pass_report = report(&pass);
    assert_eq!(pass_report["outcome"], "PASS");
    assert_eq!(pass_report["summary"]["total_rules"], 5);

    let current_origination_changed = scenario.write_text(
        "current-origination-changed.csv",
        "ASSETNUMBER,MATURITYDATE,ORIGINATIONDATE,FIRSTLOANPAYMENTDUEDATE,POSTMODIFICATIONMATURITYDATE,PAIDTHROUGHDATE\n\
         100,2030-01-10,2020-02-16,2020-03-01,2031-01-10,2026-09-01\n\
         200,2032-12-31,2021-05-20,2021-06-01,2033-12-31,2026-08-15\n",
    );
    let origination_fail = run_json(&compiled, &current_origination_changed, &prior);
    assert_eq!(origination_fail.status.code(), Some(1));
    let origination_report = report(&origination_fail);
    let origination = result_by_rule(&origination_report, "ORIGINATION_DATE_IMMUTABLE");
    assert_eq!(origination["status"], "fail");
    assert_eq!(origination["violation_count"], 1);
    assert_eq!(
        origination["affected"][0]["key"],
        json!({ "ASSETNUMBER": 100 })
    );
    assert_eq!(origination["affected"][0]["field"], "ORIGINATIONDATE");
    assert_eq!(origination["affected"][0]["value"], "2020-02-16");

    let current_paid_backward = scenario.write_text(
        "current-paid-backward.csv",
        "ASSETNUMBER,MATURITYDATE,ORIGINATIONDATE,FIRSTLOANPAYMENTDUEDATE,POSTMODIFICATIONMATURITYDATE,PAIDTHROUGHDATE\n\
         100,2030-01-10,2020-02-15,2020-03-01,2031-01-10,2026-07-31\n\
         200,2032-12-31,2021-05-20,2021-06-01,2033-12-31,2026-08-15\n",
    );
    let paid_fail = run_json(&compiled, &current_paid_backward, &prior);
    assert_eq!(paid_fail.status.code(), Some(1));
    let paid_report = report(&paid_fail);
    let paid = result_by_rule(&paid_report, "PAID_THROUGH_DATE_MONOTONIC");
    assert_eq!(paid["status"], "fail");
    assert_eq!(paid["violation_count"], 1);
    assert_eq!(paid["affected"][0]["key"], json!({ "ASSETNUMBER": 100 }));
    assert_eq!(paid["affected"][0]["field"], "PAIDTHROUGHDATE");
    assert_eq!(paid["affected"][0]["value"], "2026-07-31");
}
