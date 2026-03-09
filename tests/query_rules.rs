use std::{error::Error, fs, path::PathBuf};

use serde_json::json;
use verify_core::{
    constraint::{Binding, BindingKind, Check, ConstraintSet, Portability, Rule, Severity},
    refusal::RefusalCode,
    report::{ResultStatus, VerifyReport},
};
use verify_duckdb::{
    BatchBindingInput, BatchBindingLimits, prepare_batch_context,
    query_rules::{QueryRuleExecutor, evaluate_rule},
};

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("fixtures")
        .join(relative)
}

fn load_query_fixture_constraints() -> Result<ConstraintSet, Box<dyn Error>> {
    let path = fixture_path("constraints/query_rules/orphan_rows.verify.json");
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn load_query_fixture_report() -> Result<VerifyReport, Box<dyn Error>> {
    let path = fixture_path("reports/query_localization/orphan_rows.fail.json");
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn query_fixture_context(
    constraints: &ConstraintSet,
) -> Result<verify_duckdb::BatchContext, Box<dyn Error>> {
    Ok(prepare_batch_context(
        constraints,
        vec![
            BatchBindingInput::new("property", fixture_path("inputs/arity_n/property.csv")),
            BatchBindingInput::new("tenants", fixture_path("inputs/arity_n/tenants.csv")),
        ],
        BatchBindingLimits::default(),
    )?)
}

fn make_query_rule(id: &str, bindings: &[&str], query: &str) -> Rule {
    Rule {
        id: id.to_owned(),
        severity: Severity::Error,
        portability: Portability::BatchOnly,
        check: Check::QueryZeroRows {
            bindings: bindings
                .iter()
                .map(|binding| (*binding).to_owned())
                .collect(),
            query: query.to_owned(),
        },
    }
}

fn constraints_with_bindings(bindings: &[&str]) -> ConstraintSet {
    let mut constraints = ConstraintSet::new("fixtures.query_rules.test");
    constraints.bindings = bindings
        .iter()
        .map(|binding| Binding {
            name: (*binding).to_owned(),
            kind: BindingKind::Relation,
            key_fields: Vec::new(),
        })
        .collect();
    constraints
}

#[test]
fn query_zero_rows_maps_fixture_failures_into_localized_affected_entries()
-> Result<(), Box<dyn Error>> {
    let constraints = load_query_fixture_constraints()?;
    let expected = load_query_fixture_report()?;
    let context = query_fixture_context(&constraints)?;

    let result = QueryRuleExecutor::evaluate_rule(
        &constraints.rules[0],
        context.connection(),
        context.bindings(),
    )?;

    assert_eq!(result, expected.results[0]);
    Ok(())
}

#[test]
fn query_zero_rows_passes_when_query_returns_no_rows() -> Result<(), Box<dyn Error>> {
    let constraints = load_query_fixture_constraints()?;
    let context = query_fixture_context(&constraints)?;
    let rule = make_query_rule(
        "NO_ORPHANS",
        &["property", "tenants"],
        "SELECT property.property_id AS key__property_id FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE 1 = 0",
    );

    let result = evaluate_rule(&rule, context.connection(), context.bindings())?;

    assert!(matches!(result.status, ResultStatus::Pass));
    assert_eq!(result.violation_count, 0);
    assert!(result.affected.is_empty());
    Ok(())
}

#[test]
fn query_zero_rows_defaults_binding_to_first_declared_binding() -> Result<(), Box<dyn Error>> {
    let constraints = load_query_fixture_constraints()?;
    let context = query_fixture_context(&constraints)?;
    let rule = make_query_rule(
        "DEFAULT_BINDING",
        &["property", "tenants"],
        "SELECT property.tenant_id AS value, property.property_id AS key__property_id FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE tenants.tenant_id IS NULL",
    );

    let result = evaluate_rule(&rule, context.connection(), context.bindings())?;

    assert!(matches!(result.status, ResultStatus::Fail));
    assert_eq!(result.violation_count, 1);
    assert_eq!(result.affected[0].binding, "property");
    assert_eq!(
        result.affected[0]
            .key
            .as_ref()
            .and_then(|key| key.get("property_id")),
        Some(&json!("P-003"))
    );
    assert!(result.affected[0].field.is_none());
    assert_eq!(result.affected[0].value, Some(json!("T-999")));
    Ok(())
}

#[test]
fn query_zero_rows_sql_failures_map_to_refusal() -> Result<(), Box<dyn Error>> {
    let constraints = constraints_with_bindings(&["property"]);
    let context = prepare_batch_context(
        &constraints,
        vec![BatchBindingInput::new(
            "property",
            fixture_path("inputs/arity_n/property.csv"),
        )],
        BatchBindingLimits::default(),
    )?;
    let rule = make_query_rule(
        "BROKEN_QUERY",
        &["property"],
        "SELECT missing_column FROM property",
    );

    let error =
        evaluate_rule(&rule, context.connection(), context.bindings()).expect_err("should refuse");
    let refusal = error.to_refusal();

    assert_eq!(refusal.code, RefusalCode::SqlError);
    assert_eq!(refusal.detail["rule_id"], json!("BROKEN_QUERY"));
    assert_eq!(refusal.detail["bindings"], json!(["property"]));
    Ok(())
}
