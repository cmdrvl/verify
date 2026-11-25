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

// ---------------------------------------------------------------------------
// Hostile rule SQL (bd-16h): refused with a typed code, no side effect
// ---------------------------------------------------------------------------

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "verify-hostile-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }

    fn path(&self, file: &str) -> PathBuf {
        self.0.join(file)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn quoted(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn property_context() -> Result<verify_duckdb::BatchContext, Box<dyn Error>> {
    let constraints = constraints_with_bindings(&["property"]);
    Ok(prepare_batch_context(
        &constraints,
        vec![BatchBindingInput::new(
            "property",
            fixture_path("inputs/arity_n/property.csv"),
        )],
        BatchBindingLimits::default(),
    )?)
}

#[test]
fn hostile_rule_sql_is_refused_as_bad_constraints_and_touches_nothing() -> Result<(), Box<dyn Error>>
{
    let scratch = Scratch::new("rules");
    let exfil = scratch.path("exfil.csv");
    let attached = scratch.path("attached.db");
    let bound_file = fixture_path("inputs/arity_n/property.csv");
    let context = property_context()?;
    let rows_before: i64 =
        context
            .connection()
            .query_row("SELECT count(*) FROM property", [], |row| row.get(0))?;
    assert!(rows_before > 0);

    let hostile: Vec<(&str, String)> = vec![
        (
            "READ_FILE",
            format!("SELECT * FROM read_csv_auto('{}')", quoted(&bound_file)),
        ),
        (
            "READ_HOST",
            "SELECT * FROM read_csv('/etc/hosts')".to_owned(),
        ),
        (
            "READ_S3",
            "SELECT * FROM read_parquet('s3://bucket/x.parquet')".to_owned(),
        ),
        (
            "READ_HTTP",
            "SELECT * FROM 'https://127.0.0.1:9/x.csv'".to_owned(),
        ),
        (
            "COPY_OUT",
            format!("COPY (SELECT * FROM property) TO '{}'", quoted(&exfil)),
        ),
        (
            "ATTACH_DB",
            format!("ATTACH '{}' AS other", quoted(&attached)),
        ),
        ("INSTALL_EXT", "INSTALL httpfs".to_owned()),
        ("LOAD_EXT", "LOAD httpfs".to_owned()),
        ("SET_ACCESS", "SET enable_external_access = true".to_owned()),
        ("PRAGMA_LIST", "PRAGMA database_list".to_owned()),
        ("WRITE_STMT", "DELETE FROM property".to_owned()),
        (
            "MULTI",
            format!(
                "SELECT property_id FROM property WHERE 1 = 0; COPY property TO '{}'",
                quoted(&exfil)
            ),
        ),
        (
            "WITH_WRITE",
            "WITH x AS (SELECT 1) INSERT INTO property SELECT * FROM property".to_owned(),
        ),
    ];

    for (id, query) in &hostile {
        let rule = make_query_rule(id, &["property"], query);
        let error = evaluate_rule(&rule, context.connection(), context.bindings())
            .expect_err(&format!("{id} must be refused"));
        let refusal = error.to_refusal();
        assert_eq!(
            refusal.code,
            RefusalCode::BadConstraints,
            "{id}: {refusal:?}"
        );
        assert_eq!(refusal.detail["rule_id"], json!(id));
        assert!(
            refusal.detail["detail"]
                .as_str()
                .is_some_and(|text| text.starts_with("query is not read-only")),
            "{id}: {refusal:?}"
        );
    }

    assert!(!exfil.exists(), "a refused COPY must not create a file");
    assert!(
        !attached.exists(),
        "a refused ATTACH must not create a file"
    );
    // The bound data is still intact: nothing was deleted or inserted.
    let rows: i64 = context
        .connection()
        .query_row("SELECT count(*) FROM property", [], |row| row.get(0))?;
    assert_eq!(rows, rows_before);
    Ok(())
}

#[test]
fn the_loaded_connection_is_locked_even_if_the_validator_were_bypassed()
-> Result<(), Box<dyn Error>> {
    // Defense in depth: skip validate_read_only_query and hit the connection that
    // prepare_batch_context returns. Each statement must still fail, and the file
    // reads/writes must not happen.
    let scratch = Scratch::new("bypass");
    let exfil = scratch.path("exfil.csv");
    let attached = scratch.path("attached.db");
    let bound_file = fixture_path("inputs/arity_n/property.csv");
    let context = property_context()?;
    let connection = context.connection();

    for statement in [
        format!("SELECT * FROM read_csv_auto('{}')", quoted(&bound_file)),
        format!("COPY property TO '{}'", quoted(&exfil)),
        format!("ATTACH '{}' AS other", quoted(&attached)),
        "SET enable_external_access = true".to_owned(),
        "SET lock_configuration = false".to_owned(),
    ] {
        assert!(
            connection.execute_batch(&statement).is_err(),
            "locked connection accepted: {statement}"
        );
    }
    assert!(!exfil.exists());
    assert!(!attached.exists());
    Ok(())
}

#[test]
fn legitimate_query_rules_still_run_on_the_locked_connection() -> Result<(), Box<dyn Error>> {
    let context = property_context()?;
    let rule = make_query_rule(
        "STILL_WORKS",
        &["property"],
        "WITH bad AS (SELECT property_id FROM property WHERE tenant_id = 'T-999') \
         SELECT property_id AS key__property_id FROM bad -- trailing comment ; DROP TABLE x",
    );
    let result = evaluate_rule(&rule, context.connection(), context.bindings())?;
    assert!(matches!(result.status, ResultStatus::Fail));
    assert_eq!(result.violation_count, 1);
    Ok(())
}
