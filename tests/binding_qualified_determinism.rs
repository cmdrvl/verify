use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::Value;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> PathBuf {
    Path::new(WORKSPACE_ROOT).join(relative)
}

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify")) // ubs:ignore — Cargo supplies this trusted test-binary path.
}

fn run_json(current: &str, prior: &str) -> Output {
    let compiled =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");
    let current = fixture(current);
    let prior = fixture(prior);
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

fn parsed(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should contain a JSON report")
}

fn normalized_semantics(mut report: Value) -> Value {
    if let Some(bindings) = report["bindings"].as_object_mut() {
        for binding in bindings.values_mut() {
            if let Some(fields) = binding.as_object_mut() {
                fields.remove("source");
                fields.remove("content_hash");
            }
        }
    }
    report
}

#[test]
fn repeated_identical_failures_are_byte_identical() {
    let first = run_json(
        "fixtures/inputs/binding_qualified/current.csv",
        "fixtures/inputs/binding_qualified/prior_changed.csv",
    );
    let second = run_json(
        "fixtures/inputs/binding_qualified/current.csv",
        "fixtures/inputs/binding_qualified/prior_changed.csv",
    );

    assert_eq!(first.status.code(), Some(1));
    assert_eq!(second.status.code(), Some(1));
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());
    assert!(second.stderr.is_empty());
}

#[test]
fn shuffled_physical_rows_preserve_pass_and_fail_semantics() {
    let ordered_pass = run_json(
        "fixtures/inputs/binding_qualified/current.csv",
        "fixtures/inputs/binding_qualified/prior_matching.csv",
    );
    let shuffled_pass = run_json(
        "fixtures/inputs/binding_qualified/current_shuffled.csv",
        "fixtures/inputs/binding_qualified/prior_matching_shuffled.csv",
    );
    assert_eq!(ordered_pass.status.code(), Some(0));
    assert_eq!(shuffled_pass.status.code(), Some(0));
    assert_eq!(
        normalized_semantics(parsed(&ordered_pass)),
        normalized_semantics(parsed(&shuffled_pass))
    );

    let ordered_fail = run_json(
        "fixtures/inputs/binding_qualified/current.csv",
        "fixtures/inputs/binding_qualified/prior_changed.csv",
    );
    let shuffled_fail = run_json(
        "fixtures/inputs/binding_qualified/current_shuffled.csv",
        "fixtures/inputs/binding_qualified/prior_changed.csv",
    );
    assert_eq!(ordered_fail.status.code(), Some(1));
    assert_eq!(shuffled_fail.status.code(), Some(1));
    assert_eq!(
        normalized_semantics(parsed(&ordered_fail)),
        normalized_semantics(parsed(&shuffled_fail))
    );
}

#[test]
fn shuffled_anchor_rows_preserve_first_refusal_choice() {
    let ordered = run_json(
        "fixtures/inputs/binding_qualified/current.csv",
        "fixtures/inputs/binding_qualified/prior_duplicate.csv",
    );
    let shuffled = run_json(
        "fixtures/inputs/binding_qualified/current_shuffled.csv",
        "fixtures/inputs/binding_qualified/prior_duplicate.csv",
    );

    assert_eq!(ordered.status.code(), Some(2));
    assert_eq!(shuffled.status.code(), Some(2));
    let ordered = normalized_semantics(parsed(&ordered));
    let shuffled = normalized_semantics(parsed(&shuffled));
    assert_eq!(ordered, shuffled);
    assert_eq!(ordered["refusal"]["code"], "E_KEY_AMBIGUOUS");
    assert_eq!(ordered["refusal"]["detail"]["binding"], "prior");
    assert_eq!(ordered["refusal"]["detail"]["occurrences"], 2);
}
