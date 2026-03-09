use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use verify_core::constraint::{
    Binding, BindingKind, Check, ColumnReference, Comparison, ConstraintSet, Portability,
    PredicateExpression, PredicateOperand, Rule, Severity,
};

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn verify_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_verify"))
}

fn fixture(relative: &str) -> PathBuf {
    Path::new(WORKSPACE_ROOT).join(relative)
}

struct TempScenario {
    root: PathBuf,
}

impl TempScenario {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "verify-determinism-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("scenario directory should be created");
        Self { root }
    }

    fn write(&self, relative: &str, content: impl AsRef<[u8]>) -> PathBuf {
        let path = self.root.join(relative);
        fs::write(&path, content).expect("scenario file should be written");
        path
    }
}

impl Drop for TempScenario {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_verify_json(compiled: &Path, binds: &[(&str, &Path)]) -> Output {
    let mut command = Command::new(verify_binary());
    command.args([
        "run",
        compiled.to_str().expect("compiled path should be utf-8"),
    ]);

    let bind_args: Vec<String> = binds
        .iter()
        .map(|(name, path)| format!("{name}={}", path.display()))
        .collect();
    for bind in &bind_args {
        command.args(["--bind", bind]);
    }

    command
        .args(["--json", "--no-witness"])
        .output()
        .expect("verify command should run")
}

fn assert_fail_json(output: Output) -> Vec<u8> {
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected FAIL exit code; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "json-mode failures should not write diagnostics to stderr"
    );
    output.stdout
}

fn portable_determinism_constraint() -> ConstraintSet {
    let mut constraints = ConstraintSet::new("determinism.portable.fail");
    constraints.bindings = vec![Binding {
        name: "input".to_owned(),
        kind: BindingKind::Relation,
        key_fields: vec!["loan_id".to_owned()],
    }];
    constraints.rules = vec![
        Rule {
            id: "Z_REQUIRED_LOAN_ID".to_owned(),
            severity: Severity::Error,
            portability: Portability::Portable,
            check: Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["loan_id".to_owned()],
            },
        },
        Rule {
            id: "A_POSITIVE_BALANCE".to_owned(),
            severity: Severity::Error,
            portability: Portability::Portable,
            check: Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Gt {
                    gt: [
                        PredicateOperand::Column(ColumnReference {
                            column: "balance".to_owned(),
                        }),
                        PredicateOperand::Literal(json!(0)),
                    ],
                },
            },
        },
        Rule {
            id: "M_MIN_ROW_COUNT".to_owned(),
            severity: Severity::Warn,
            portability: Portability::Portable,
            check: Check::RowCount {
                binding: "input".to_owned(),
                compare: Comparison {
                    gte: Some(json!(1)),
                    ..Default::default()
                },
            },
        },
    ];
    constraints
}

#[test]
fn repeated_run_json_is_byte_identical_for_sorted_portable_failures() {
    let scenario = TempScenario::new("portable-fail");
    let compiled_path = scenario.write(
        "portable.verify.json",
        serde_json::to_vec_pretty(&portable_determinism_constraint())
            .expect("constraints should serialize"),
    );
    let dataset_path = scenario.write("input.csv", "loan_id,balance\nLN-200,-1\n,5\nLN-100,-2\n");

    let first = assert_fail_json(run_verify_json(&compiled_path, &[("input", &dataset_path)]));
    let second = assert_fail_json(run_verify_json(&compiled_path, &[("input", &dataset_path)]));

    assert_eq!(
        first, second,
        "repeated runs over the same compiled constraints and input should emit identical JSON bytes"
    );

    let report: Value = serde_json::from_slice(&first).expect("report should parse");
    let results = report["results"]
        .as_array()
        .expect("results should be an array");
    let rule_ids: Vec<&str> = results
        .iter()
        .map(|result| {
            result["rule_id"]
                .as_str()
                .expect("rule id should be a string")
        })
        .collect();

    assert_eq!(
        rule_ids,
        vec![
            "A_POSITIVE_BALANCE",
            "M_MIN_ROW_COUNT",
            "Z_REQUIRED_LOAN_ID"
        ]
    );

    let affected = results[0]["affected"]
        .as_array()
        .expect("predicate failures should be localized");
    assert_eq!(affected[0]["key"]["loan_id"], "LN-100");
    assert_eq!(affected[0]["field"], "balance");
    assert_eq!(affected[0]["value"], -2);
    assert_eq!(affected[1]["key"]["loan_id"], "LN-200");
    assert_eq!(affected[1]["field"], "balance");
    assert_eq!(affected[1]["value"], -1);
    assert_eq!(results[1]["status"], "pass");
    assert_eq!(results[2]["affected"][0]["field"], "loan_id");
}

#[test]
fn repeated_run_json_is_byte_identical_for_query_rule_failures() {
    let compiled = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
    let property = fixture("fixtures/inputs/arity_n/property.csv");
    let tenants = fixture("fixtures/inputs/arity_n/tenants.csv");

    let first = assert_fail_json(run_verify_json(
        &compiled,
        &[("property", &property), ("tenants", &tenants)],
    ));
    let second = assert_fail_json(run_verify_json(
        &compiled,
        &[("property", &property), ("tenants", &tenants)],
    ));

    assert_eq!(
        first, second,
        "query-rule runs should also produce identical JSON bytes across repeated executions"
    );

    let report: Value = serde_json::from_slice(&first).expect("report should parse");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["results"][0]["rule_id"], "ORPHAN_PROPERTY_TENANT");
    assert_eq!(report["results"][0]["affected"][0]["binding"], "property");
    assert_eq!(
        report["results"][0]["affected"][0]["key"]["property_id"],
        "P-003"
    );
    assert_eq!(report["results"][0]["affected"][0]["field"], "tenant_id");
    assert_eq!(report["results"][0]["affected"][0]["value"], "T-999");
}
