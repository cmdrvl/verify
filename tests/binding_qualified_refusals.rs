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
            "verify-binding-refusal-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("scenario directory should be created");
        Self { root }
    }

    fn write_constraint(&self, name: &str, value: &Value) -> PathBuf {
        let path = self.root.join(name);
        fs::write(
            &path,
            serde_json::to_vec_pretty(value).expect("constraint should serialize"),
        )
        .expect("constraint should be written");
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

fn base_constraint() -> Value {
    serde_json::from_slice(
        &fs::read(fixture(
            "fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json",
        ))
        .expect("compiled fixture should exist"),
    )
    .expect("compiled fixture should parse")
}

fn run_json(compiled: &Path, bindings: &[(&str, PathBuf)]) -> Output {
    let mut command = verify_command();
    command.args([
        "run",
        compiled.to_str().expect("compiled path should be utf-8"),
    ]);
    let bind_args = bindings
        .iter()
        .map(|(name, path)| format!("{name}={}", path.display()))
        .collect::<Vec<_>>();
    for bind in &bind_args {
        command.args(["--bind", bind]);
    }
    command
        .args(["--json", "--no-witness"])
        .output()
        .expect("verify run should execute")
}

fn refusal(output: Output, expected_code: &str) -> Value {
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected refusal; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let report: Value = serde_json::from_slice(&output.stdout).expect("refusal should be JSON");
    assert_eq!(report["outcome"], "REFUSAL");
    assert_eq!(report["results"], json!([]));
    assert_eq!(report["refusal"]["code"], expected_code);
    report
}

fn normal_bindings(prior: &str) -> Vec<(&'static str, PathBuf)> {
    vec![
        (
            "current",
            fixture("fixtures/inputs/binding_qualified/current.csv"),
        ),
        ("prior", fixture(prior)),
    ]
}

#[test]
fn compiled_contract_defects_refuse_before_binding_evaluation() {
    let scenario = TempScenario::new("contract");

    let mut undeclared = base_constraint();
    undeclared["bindings"] = json!([
        { "name": "current", "kind": "relation", "key_fields": ["loan_id", "tranche_id"] }
    ]);
    let undeclared_path = scenario.write_constraint("undeclared.json", &undeclared);
    let report = refusal(
        run_json(
            &undeclared_path,
            &[(
                "current",
                fixture("fixtures/inputs/binding_qualified/current.csv"),
            )],
        ),
        "E_BAD_CONSTRAINTS",
    );
    assert_eq!(
        report["refusal"]["detail"]["reason"],
        "undeclared_reference"
    );
    assert_eq!(report["refusal"]["detail"]["binding"], "prior");

    let mut missing = base_constraint();
    missing["bindings"][1]
        .as_object_mut()
        .expect("binding should be an object")
        .remove("key_fields");
    let missing_path = scenario.write_constraint("missing-key-fields.json", &missing);
    let report = refusal(
        run_json(
            &missing_path,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_BAD_CONSTRAINTS",
    );
    assert_eq!(report["refusal"]["detail"]["reason"], "missing_key_fields");
    assert_eq!(report["refusal"]["detail"]["binding"], "prior");

    let mut empty = base_constraint();
    empty["bindings"][1]["key_fields"] = json!([]);
    let empty_path = scenario.write_constraint("empty-key-fields.json", &empty);
    let report = refusal(
        run_json(
            &empty_path,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_BAD_CONSTRAINTS",
    );
    assert!(
        report["refusal"]["detail"]["error"]
            .as_str()
            .is_some_and(|error| error.contains("empty_key_fields"))
    );

    let mut arity = base_constraint();
    arity["bindings"][1]["key_fields"] = json!(["asset_number"]);
    let arity_path = scenario.write_constraint("key-arity.json", &arity);
    let report = refusal(
        run_json(
            &arity_path,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_BAD_CONSTRAINTS",
    );
    assert_eq!(report["refusal"]["detail"]["reason"], "key_arity_mismatch");
    assert_eq!(
        report["refusal"]["next_step"],
        "Recompile or fix the constraint artifact."
    );
}

#[test]
fn missing_key_and_operand_fields_have_pinned_field_refusals() {
    let scenario = TempScenario::new("fields");

    let mut missing_key = base_constraint();
    missing_key["bindings"][1]["key_fields"] = json!(["asset_number", "missing_class"]);
    let missing_key_path = scenario.write_constraint("missing-key-field.json", &missing_key);
    let report = refusal(
        run_json(
            &missing_key_path,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_FIELD_NOT_FOUND",
    );
    assert_eq!(
        report["refusal"]["detail"],
        json!({
            "rule_id": "MATURITY_DATE_IMMUTABLE",
            "binding": "prior",
            "field": "missing_class"
        })
    );

    let mut missing_operand = base_constraint();
    missing_operand["rules"][0]["check"]["expr"]["eq"][1]["column"] = json!("missing_date");
    let missing_operand_path =
        scenario.write_constraint("missing-operand-field.json", &missing_operand);
    let report = refusal(
        run_json(
            &missing_operand_path,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_FIELD_NOT_FOUND",
    );
    assert_eq!(report["refusal"]["detail"]["binding"], "prior");
    assert_eq!(report["refusal"]["detail"]["field"], "missing_date");
    assert_eq!(
        report["refusal"]["next_step"],
        "Fix the constraint set or bind an input that exposes the required field."
    );
}

#[test]
fn invalid_duplicate_and_unmatched_keys_have_stable_runtime_refusals() {
    let compiled =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");

    let null_key = refusal(
        run_json(
            &compiled,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_null_key.json"),
        ),
        "E_KEY_INVALID",
    );
    assert_eq!(null_key["refusal"]["detail"]["reason"], "null_component");
    assert_eq!(null_key["refusal"]["detail"]["binding"], "prior");
    assert_eq!(null_key["refusal"]["detail"]["field"], "asset_number");

    let type_mismatch = refusal(
        run_json(
            &compiled,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_type_mismatch.csv"),
        ),
        "E_KEY_INVALID",
    );
    assert_eq!(
        type_mismatch["refusal"]["detail"]["reason"],
        "type_mismatch"
    );
    assert_eq!(type_mismatch["refusal"]["detail"]["left_type"], "string");
    assert_eq!(type_mismatch["refusal"]["detail"]["right_type"], "number");

    let duplicate = refusal(
        run_json(
            &compiled,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_duplicate.csv"),
        ),
        "E_KEY_AMBIGUOUS",
    );
    assert_eq!(duplicate["refusal"]["detail"]["binding"], "prior");
    assert_eq!(duplicate["refusal"]["detail"]["occurrences"], 2);
    assert_eq!(
        duplicate["refusal"]["detail"]["key"],
        json!({ "asset_number": "LN-999", "class_code": "Z" })
    );

    let unmatched = refusal(
        run_json(
            &compiled,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_unmatched.csv"),
        ),
        "E_KEY_UNMATCHED",
    );
    assert_eq!(unmatched["refusal"]["detail"]["binding"], "current");
    assert_eq!(
        unmatched["refusal"]["detail"]["key"],
        json!({ "loan_id": "LN-100", "tranche_id": "A" })
    );
    assert_eq!(unmatched["refusal"]["detail"]["missing_binding"], "prior");
}

#[test]
fn temporal_key_fields_are_refused_end_to_end() {
    let scenario = TempScenario::new("temporal-key");
    let constraint = json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": "integration.binding_qualified.temporal_key_refusal",
        "bindings": [
            { "name": "current", "kind": "relation", "key_fields": ["ASSETDATE"] },
            { "name": "prior", "kind": "relation", "key_fields": ["ASSETDATE"] }
        ],
        "rules": [{
            "id": "TEMPORAL_KEY_REFUSAL",
            "severity": "error",
            "portability": "batch_only",
            "check": {
                "op": "predicate",
                "binding": "current",
                "expr": {
                    "eq": [
                        { "binding": "current", "column": "amount" },
                        { "binding": "prior", "column": "amount" }
                    ]
                }
            }
        }]
    });
    let compiled = scenario.write_constraint("temporal-key.verify.json", &constraint);
    let current = scenario.write_text("current.csv", "ASSETDATE,amount\n2026-01-02,100\n");
    let prior = scenario.write_text("prior.csv", "ASSETDATE,amount\n2026-01-02,100\n");
    let report = refusal(
        run_json(&compiled, &[("current", current), ("prior", prior)]),
        "E_KEY_INVALID",
    );

    assert_eq!(
        report["refusal"]["detail"]["reason"],
        "unrepresentable_component"
    );
    assert_eq!(report["refusal"]["detail"]["value_type"], "date");
    assert_eq!(report["refusal"]["detail"]["field"], "ASSETDATE");
}

#[test]
fn incomparable_operands_refuse_without_partial_results_and_keep_binding_identity() {
    let scenario = TempScenario::new("bad-expression");
    let mut constraint = base_constraint();
    constraint["rules"][0]["check"]["expr"] = json!({
        "eq": [
            { "binding": "current", "column": "amount" },
            { "binding": "prior", "column": "maturity_date" }
        ]
    });
    let compiled = scenario.write_constraint("bad-expression.json", &constraint);
    let report = refusal(
        run_json(
            &compiled,
            &normal_bindings("fixtures/inputs/binding_qualified/prior_matching.csv"),
        ),
        "E_BAD_EXPR",
    );

    let detail = &report["refusal"]["detail"];
    assert_eq!(detail["rule_id"], "MATURITY_DATE_IMMUTABLE");
    assert_eq!(detail["operator"], "eq");
    assert_eq!(detail["left_type"], "number");
    assert_eq!(detail["right_type"], "string");
    assert_eq!(detail["binding"], "current");
    assert_eq!(
        detail["key"],
        json!({ "loan_id": "LN-100", "tranche_id": "A" })
    );
    assert_eq!(detail["left_binding"], "current");
    assert_eq!(detail["left_field"], "amount");
    assert_eq!(detail["right_binding"], "prior");
    assert_eq!(detail["right_field"], "maturity_date");
    assert!(report["bindings"]["current"]["content_hash"].is_string());
    assert!(report["bindings"]["prior"]["content_hash"].is_string());
    assert_eq!(report["refusal"]["next_step"], "Fix the rule expression.");
}
