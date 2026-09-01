/// Schema contract tests for verify.
///
/// Validates that all fixture files conform to the expected protocol shapes
/// and that schemas themselves are well-formed JSON.
use std::fs;

use serde_json::Value;
use verify_core::constraint::{Check, ConstraintSet, Portability};
use verify_core::report::VerifyReport;
use verify_core::validation::validate_constraint_predicates;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn fixture(relative: &str) -> String {
    format!("{WORKSPACE_ROOT}/{relative}")
}

// ---------------------------------------------------------------------------
// Constraint fixture round-trips
// ---------------------------------------------------------------------------

#[test]
fn arity1_not_null_fixture_parses_as_constraint_set() {
    let path = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let constraints: ConstraintSet =
        serde_json::from_slice(&bytes).expect("fixture should parse as ConstraintSet");

    assert_eq!(constraints.version, "verify.constraint.v1");
    assert_eq!(
        constraints.constraint_set_id,
        "fixtures.arity1.not_null_loans"
    );
    assert_eq!(constraints.bindings.len(), 1);
    assert_eq!(constraints.rules.len(), 1);
}

#[test]
fn arity_n_foreign_key_fixture_parses_as_constraint_set() {
    let path = fixture("fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let constraints: ConstraintSet =
        serde_json::from_slice(&bytes).expect("fixture should parse as ConstraintSet");

    assert_eq!(constraints.version, "verify.constraint.v1");
    assert_eq!(constraints.bindings.len(), 2);
    assert_eq!(constraints.rules.len(), 1);
}

#[test]
fn query_rules_fixture_parses_as_constraint_set() {
    let path = fixture("fixtures/constraints/query_rules/orphan_rows.verify.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let constraints: ConstraintSet =
        serde_json::from_slice(&bytes).expect("fixture should parse as ConstraintSet");

    assert_eq!(constraints.version, "verify.constraint.v1");
    assert_eq!(constraints.bindings.len(), 2);
    assert_eq!(constraints.rules.len(), 1);
}

#[test]
fn binding_qualified_fixture_round_trips_and_validates() {
    let path =
        fixture("fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let expected: Value = serde_json::from_slice(&bytes).expect("fixture should be valid JSON");
    let constraints: ConstraintSet =
        serde_json::from_slice(&bytes).expect("fixture should parse as ConstraintSet");

    assert_eq!(
        constraints.bindings[0].key_fields,
        ["loan_id", "tranche_id"]
    );
    assert_eq!(
        constraints.bindings[1].key_fields,
        ["asset_number", "class_code"]
    );
    assert_eq!(constraints.rules[0].portability, Portability::BatchOnly);
    let expr = constraints
        .rules
        .first()
        .and_then(|rule| match &rule.check {
            Check::Predicate { expr, .. } => Some(expr),
            _ => None,
        })
        .expect("fixture should contain a predicate rule");
    assert!(
        serde_json::to_value(expr).expect("expression should serialize")["eq"][1]["binding"]
            == "prior"
    );
    validate_constraint_predicates(&constraints)
        .expect("fixture predicate declarations should validate");
    assert_eq!(
        serde_json::to_value(constraints).expect("fixture should serialize"),
        expected
    );
}

// ---------------------------------------------------------------------------
// Report fixture round-trips
// ---------------------------------------------------------------------------

#[test]
fn pass_report_fixture_parses_as_verify_report() {
    let path = fixture("fixtures/reports/pass/arity1_not_null.pass.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let report: VerifyReport =
        serde_json::from_slice(&bytes).expect("fixture should parse as VerifyReport");

    assert_eq!(report.outcome, verify_core::report::Outcome::Pass);
    assert_eq!(report.constraint_set_id, "fixtures.arity1.not_null_loans");
    assert_eq!(report.summary.passed_rules, 1);
    assert_eq!(report.summary.failed_rules, 0);
}

#[test]
fn fail_report_fixture_parses_as_verify_report() {
    let path = fixture("fixtures/reports/fail/arity1_not_null.fail.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let report: VerifyReport =
        serde_json::from_slice(&bytes).expect("fixture should parse as VerifyReport");

    assert_eq!(report.outcome, verify_core::report::Outcome::Fail);
    assert!(report.summary.failed_rules >= 1);
}

#[test]
fn refusal_report_fixture_parses_as_verify_report() {
    let path = fixture("fixtures/reports/refusal/bad_authoring.refusal.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let report: VerifyReport =
        serde_json::from_slice(&bytes).expect("fixture should parse as VerifyReport");

    assert_eq!(report.outcome, verify_core::report::Outcome::Refusal);
    assert!(report.refusal.is_some());
}

// ---------------------------------------------------------------------------
// Constraint artifacts are valid JSON
// ---------------------------------------------------------------------------

#[test]
fn all_constraint_fixtures_are_valid_json() {
    for family in &[
        "constraints/arity1",
        "constraints/arity_n",
        "constraints/query_rules",
        "constraints/binding_qualified",
    ] {
        let dir = fixture(&format!("fixtures/{family}"));
        for entry in fs::read_dir(&dir).expect("fixture dir should exist") {
            let entry = entry.expect("entry should be readable");
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let bytes = fs::read(&path).expect("fixture should be readable");
                let parsed = serde_json::from_slice::<Value>(&bytes);
                assert!(
                    parsed.is_ok(),
                    "fixture {} should be valid JSON: {:?}",
                    path.display(),
                    parsed.err()
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schema strings are well-formed
// ---------------------------------------------------------------------------

#[test]
fn constraint_schema_is_valid_json() {
    let schema_path = fixture("schemas/verify.constraint.v1.schema.json");
    let bytes = fs::read(&schema_path).expect("schema should exist");
    let schema: Value = serde_json::from_slice(&bytes).expect("schema should be valid JSON");
    assert_eq!(schema["title"], "verify.constraint.v1");
}

#[test]
fn report_schema_is_valid_json() {
    let schema_path = fixture("schemas/verify.report.v1.schema.json");
    let bytes = fs::read(&schema_path).expect("schema should exist");
    let schema: Value = serde_json::from_slice(&bytes).expect("schema should be valid JSON");
    assert_eq!(schema["title"], "verify.report.v1");
}

#[test]
fn constraint_schema_admits_qualified_columns_and_rejects_empty_key_arrays() {
    let path = fixture("schemas/verify.constraint.v1.schema.json");
    let bytes = fs::read(path).expect("schema should exist");
    let schema: Value = serde_json::from_slice(&bytes).expect("schema should be valid JSON");

    assert_eq!(
        schema["$defs"]["column_ref"]["properties"]["binding"]["type"],
        "string"
    );
    assert_eq!(
        schema["$defs"]["column_ref"]["properties"]["binding"]["minLength"],
        1
    );
    assert_eq!(
        schema["$defs"]["binding"]["properties"]["key_fields"]["minItems"],
        1
    );
}

#[test]
fn report_schema_lists_binding_key_refusal_codes() {
    let path = fixture("schemas/verify.report.v1.schema.json");
    let bytes = fs::read(path).expect("schema should exist");
    let schema: Value = serde_json::from_slice(&bytes).expect("schema should be valid JSON");
    let codes = schema["$defs"]["refusal_code"]["enum"]
        .as_array()
        .expect("refusal codes should be an array");

    for code in ["E_KEY_INVALID", "E_KEY_AMBIGUOUS", "E_KEY_UNMATCHED"] {
        assert!(codes.iter().any(|candidate| candidate == code));
    }
}

// ---------------------------------------------------------------------------
// Constraint round-trip: serialize then deserialize produces identical struct
// ---------------------------------------------------------------------------

#[test]
fn constraint_round_trip_is_stable() {
    let path = fixture("fixtures/constraints/arity1/not_null_loans.verify.json");
    let bytes = fs::read(&path).expect("fixture should exist");
    let original: ConstraintSet = serde_json::from_slice(&bytes).expect("parses");

    let serialized = serde_json::to_string_pretty(&original).expect("serializes");
    let round_tripped: ConstraintSet =
        serde_json::from_str(&serialized).expect("round-trip parses");

    assert_eq!(original, round_tripped);
}
