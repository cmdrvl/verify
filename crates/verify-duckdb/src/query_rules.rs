use std::collections::BTreeMap;

use duckdb::Connection;
use serde_json::{Value, json};
use verify_core::{
    constraint::{Check, Portability, Rule},
    order::sort_affected_entries,
    refusal::{Refusal, RefusalCode},
    report::{AffectedEntry, ResultStatus, RuleResult},
};

use crate::BindingRegistry;

pub const QUERY_RULE_OP: &str = "query_zero_rows";

#[derive(Debug, Clone, Default)]
pub struct QueryRuleExecutor;

impl QueryRuleExecutor {
    pub fn evaluate_rule(
        rule: &Rule,
        connection: &Connection,
        bindings: &BindingRegistry,
    ) -> Result<RuleResult, QueryRuleError> {
        evaluate_rule(rule, connection, bindings)
    }
}

#[derive(Debug)]
pub enum QueryRuleError {
    MissingBinding {
        rule_id: String,
        binding: String,
    },
    SqlError {
        rule_id: String,
        bindings: Vec<String>,
        query: String,
        message: String,
    },
    BadConstraints {
        rule_id: String,
        detail: String,
    },
}

impl QueryRuleError {
    pub fn refusal_code(&self) -> RefusalCode {
        match self {
            Self::MissingBinding { .. } => RefusalCode::MissingBinding,
            Self::SqlError { .. } => RefusalCode::SqlError,
            Self::BadConstraints { .. } => RefusalCode::BadConstraints,
        }
    }

    pub fn to_refusal(&self) -> Refusal {
        Refusal::new(self.refusal_code(), self.to_string(), self.detail())
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::MissingBinding { rule_id, binding } => json!({
                "rule_id": rule_id,
                "binding": binding,
            }),
            Self::SqlError {
                rule_id,
                bindings,
                query,
                message,
            } => json!({
                "rule_id": rule_id,
                "bindings": bindings,
                "query": query,
                "message": message,
            }),
            Self::BadConstraints { rule_id, detail } => json!({
                "rule_id": rule_id,
                "detail": detail,
            }),
        }
    }
}

impl std::fmt::Display for QueryRuleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBinding { rule_id, binding } => write!(
                formatter,
                "query_zero_rows rule {rule_id} references missing binding {binding}"
            ),
            Self::SqlError {
                rule_id, message, ..
            } => {
                write!(formatter, "SQL error in rule {rule_id}: {message}")
            }
            Self::BadConstraints { rule_id, detail } => {
                write!(
                    formatter,
                    "query_zero_rows rule {rule_id} is invalid: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for QueryRuleError {}

pub fn evaluate_rule(
    rule: &Rule,
    connection: &Connection,
    bindings: &BindingRegistry,
) -> Result<RuleResult, QueryRuleError> {
    let Check::QueryZeroRows {
        bindings: declared_bindings,
        ..
    } = &rule.check
    else {
        return Err(QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
        });
    };

    if !matches!(rule.portability, Portability::BatchOnly) {
        return Err(QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: "query_zero_rows rules must declare portability=batch_only".to_owned(),
        });
    }

    let default_binding =
        declared_bindings
            .first()
            .ok_or_else(|| QueryRuleError::BadConstraints {
                rule_id: rule.id.clone(),
                detail: "query_zero_rows rule must declare at least one binding".to_owned(),
            })?;

    for binding in declared_bindings {
        if bindings.get(binding).is_none() {
            return Err(QueryRuleError::MissingBinding {
                rule_id: rule.id.clone(),
                binding: binding.clone(),
            });
        }
    }

    evaluate_query_rule(rule, connection, default_binding)
}

pub fn evaluate_query_rule(
    rule: &Rule,
    connection: &Connection,
    default_binding: &str,
) -> Result<RuleResult, QueryRuleError> {
    let query = match &rule.check {
        Check::QueryZeroRows { query, .. } => query,
        _ => {
            return Err(QueryRuleError::BadConstraints {
                rule_id: rule.id.clone(),
                detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
            });
        }
    };

    let mut statement = connection
        .prepare(query)
        .map_err(|error| sql_error(rule, error.to_string()))?;
    let mut rows = statement
        .query([])
        .map_err(|error| sql_error(rule, error.to_string()))?;

    let column_count = rows.as_ref().expect("rows ref").column_count();
    let column_names: Vec<String> = (0..column_count)
        .map(|index| {
            rows.as_ref()
                .expect("rows ref")
                .column_name(index)
                .map_or_else(|_| "?".to_owned(), |name| name.to_owned())
        })
        .collect();

    let mut affected = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| sql_error(rule, error.to_string()))?
    {
        let mut values = BTreeMap::new();
        for (index, name) in column_names.iter().enumerate() {
            let duck_value: duckdb::types::Value = row
                .get(index)
                .map_err(|error| sql_error(rule, error.to_string()))?;
            values.insert(name.clone(), duckdb_to_json(duck_value));
        }
        affected.push(row_to_affected_entry(&values, default_binding));
    }

    sort_affected_entries(&mut affected);
    let status = if affected.is_empty() {
        ResultStatus::Pass
    } else {
        ResultStatus::Fail
    };

    Ok(RuleResult {
        rule_id: rule.id.clone(),
        severity: rule.severity,
        status,
        violation_count: affected.len(),
        affected,
    })
}

pub fn execute_query_rules(
    connection: &Connection,
    rules: &[Rule],
) -> Result<Vec<RuleResult>, QueryRuleError> {
    let mut results = Vec::new();
    for rule in rules {
        let Check::QueryZeroRows { bindings, .. } = &rule.check else {
            continue;
        };
        let default_binding = bindings.first().map(String::as_str).unwrap_or("unknown");
        results.push(evaluate_query_rule(rule, connection, default_binding)?);
    }
    Ok(results)
}

fn row_to_affected_entry(row: &BTreeMap<String, Value>, default_binding: &str) -> AffectedEntry {
    let binding = row
        .get("binding")
        .and_then(|value| value.as_str())
        .unwrap_or(default_binding)
        .to_owned();

    let field = row
        .get("field")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);

    let value = row.get("value").cloned();

    let mut key = BTreeMap::new();
    for (column_name, column_value) in row {
        if let Some(key_field) = column_name.strip_prefix("key__")
            && !key_field.is_empty()
        {
            key.insert(key_field.to_owned(), column_value.clone());
        }
    }

    AffectedEntry {
        binding,
        key: if key.is_empty() { None } else { Some(key) },
        field,
        value,
    }
}

fn duckdb_to_json(value: duckdb::types::Value) -> Value {
    match value {
        duckdb::types::Value::Null => Value::Null,
        duckdb::types::Value::Boolean(value) => Value::Bool(value),
        duckdb::types::Value::TinyInt(value) => Value::Number(value.into()),
        duckdb::types::Value::SmallInt(value) => Value::Number(value.into()),
        duckdb::types::Value::Int(value) => Value::Number(value.into()),
        duckdb::types::Value::BigInt(value) => Value::Number(value.into()),
        duckdb::types::Value::HugeInt(value) => Value::String(value.to_string()),
        duckdb::types::Value::UTinyInt(value) => Value::Number(value.into()),
        duckdb::types::Value::USmallInt(value) => Value::Number(value.into()),
        duckdb::types::Value::UInt(value) => Value::Number(value.into()),
        duckdb::types::Value::UBigInt(value) => Value::Number(value.into()),
        duckdb::types::Value::Float(value) => {
            serde_json::Number::from_f64(f64::from(value)).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Double(value) => {
            serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Decimal(value) => serde_json::from_str(&value.to_string())
            .unwrap_or_else(|_| Value::String(value.to_string())),
        duckdb::types::Value::Text(value) => Value::String(value),
        _ => Value::String(format!("{value:?}")),
    }
}

fn sql_error(rule: &Rule, message: String) -> QueryRuleError {
    match &rule.check {
        Check::QueryZeroRows { bindings, query } => QueryRuleError::SqlError {
            rule_id: rule.id.clone(),
            bindings: bindings.clone(),
            query: query.clone(),
            message,
        },
        _ => QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
        },
    }
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;
    use serde_json::json;
    use verify_core::constraint::{Check, Portability, Rule, Severity};
    use verify_core::report::ResultStatus;

    use super::{evaluate_query_rule, execute_query_rules};

    fn setup_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory DuckDB opens");
        conn.execute_batch(
            "CREATE TABLE property (property_id TEXT, tenant_id TEXT);
             INSERT INTO property VALUES ('P-001', 'T-001');
             INSERT INTO property VALUES ('P-002', 'T-002');
             INSERT INTO property VALUES ('P-003', 'T-999');
             CREATE TABLE tenants (tenant_id TEXT, name TEXT);
             INSERT INTO tenants VALUES ('T-001', 'Acme');
             INSERT INTO tenants VALUES ('T-002', 'Beta');",
        )
        .expect("test data inserts");
        conn
    }

    fn make_query_rule(id: &str, query: &str) -> Rule {
        Rule {
            id: id.to_owned(),
            severity: Severity::Error,
            portability: Portability::BatchOnly,
            check: Check::QueryZeroRows {
                bindings: vec!["property".to_owned(), "tenants".to_owned()],
                query: query.to_owned(),
            },
        }
    }

    #[test]
    fn zero_rows_means_pass() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "NO_VIOLATIONS",
            "SELECT 'property' AS binding FROM property WHERE 1 = 0",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
        assert!(result.affected.is_empty());
    }

    #[test]
    fn returned_rows_mean_fail_with_localization() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "ORPHAN_CHECK",
            "SELECT 'property' AS binding, 'tenant_id' AS field, property.tenant_id AS value, property.property_id AS key__property_id FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE tenants.tenant_id IS NULL",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);

        let affected = &result.affected[0];
        assert_eq!(affected.binding, "property");
        assert_eq!(affected.field.as_deref(), Some("tenant_id"));
        assert_eq!(affected.value, Some(json!("T-999")));

        let key = affected.key.as_ref().expect("key present");
        assert_eq!(key.get("property_id"), Some(&json!("P-003")));
    }

    #[test]
    fn missing_binding_column_uses_default() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "NO_BINDING_COL",
            "SELECT property_id AS key__property_id FROM property WHERE tenant_id = 'T-999'",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].binding, "property");
    }

    #[test]
    fn sql_error_returns_query_rule_error() {
        let conn = setup_connection();
        let rule = make_query_rule("BAD_SQL", "SELECT * FROM nonexistent_table");

        let error = evaluate_query_rule(&rule, &conn, "property").expect_err("should fail");
        assert!(error.to_string().contains("BAD_SQL"));
    }

    #[test]
    fn unsupported_check_type_returns_error() {
        let rule = Rule {
            id: "NOT_QUERY".to_owned(),
            severity: Severity::Error,
            portability: Portability::Portable,
            check: Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        };

        let conn = Connection::open_in_memory().expect("opens");
        let error = evaluate_query_rule(&rule, &conn, "input").expect_err("should fail");
        assert!(error.to_string().contains("query_zero_rows"));
    }

    #[test]
    fn multiple_violations_each_become_affected_entry() {
        let conn = Connection::open_in_memory().expect("opens");
        conn.execute_batch(
            "CREATE TABLE data (id TEXT, status TEXT);
             INSERT INTO data VALUES ('A', 'bad');
             INSERT INTO data VALUES ('B', 'bad');
             INSERT INTO data VALUES ('C', 'good');",
        )
        .expect("inserts");

        let rule = make_query_rule(
            "STATUS_CHECK",
            "SELECT 'data' AS binding, id AS key__id, 'status' AS field, status AS value FROM data WHERE status = 'bad'",
        );

        let result = evaluate_query_rule(&rule, &conn, "data").expect("executes");
        assert_eq!(result.violation_count, 2);
        assert_eq!(result.affected.len(), 2);

        let ids: Vec<_> = result
            .affected
            .iter()
            .map(|affected| {
                affected
                    .key
                    .as_ref()
                    .and_then(|key| key.get("id"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("?")
            })
            .collect();
        assert_eq!(ids, vec!["A", "B"]);
    }

    #[test]
    fn sql_error_maps_to_e_sql_error_refusal() {
        let conn = setup_connection();
        let rule = make_query_rule("BAD_SQL", "SELECT * FROM nonexistent_table");

        let error = evaluate_query_rule(&rule, &conn, "property").expect_err("should fail");
        assert_eq!(
            error.refusal_code(),
            verify_core::refusal::RefusalCode::SqlError
        );

        let refusal = error.to_refusal();
        assert_eq!(refusal.code, verify_core::refusal::RefusalCode::SqlError);
        assert_eq!(refusal.next_step, "Fix the query-backed rule.");

        let detail = error.detail();
        assert_eq!(detail["rule_id"], "BAD_SQL");
    }

    #[test]
    fn execute_query_rules_processes_only_query_rules() {
        let conn = setup_connection();
        let rules = vec![
            Rule {
                id: "NOT_QUERY".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::NotNull {
                    binding: "property".to_owned(),
                    columns: vec!["property_id".to_owned()],
                },
            },
            make_query_rule(
                "ORPHAN_CHECK",
                "SELECT 'property' AS binding FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE tenants.tenant_id IS NULL",
            ),
            make_query_rule(
                "ALL_PASS",
                "SELECT 'property' AS binding FROM property WHERE 1 = 0",
            ),
        ];

        let results = execute_query_rules(&conn, &rules).expect("executes");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].rule_id, "ORPHAN_CHECK");
        assert!(matches!(results[0].status, ResultStatus::Fail));
        assert_eq!(results[1].rule_id, "ALL_PASS");
        assert!(matches!(results[1].status, ResultStatus::Pass));
    }
}
