use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Number, Value};
use verify_core::{
    constraint::{Aggregate, Check, Comparison, Rule},
    report::{AffectedEntry, ResultStatus, RuleResult},
};

use crate::Relation;

pub const PORTABLE_RELATION_OPS: &[&str] = &["row_count", "aggregate_compare", "foreign_key"];

#[derive(Debug, Clone, Default)]
pub struct PortableRelationEngine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationEngineError {
    MissingBinding(String),
    MissingField { binding: String, field: String },
    UnsupportedOp(String),
    BadExpression(String),
}

impl std::fmt::Display for RelationEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBinding(name) => write!(f, "binding not found: {name}"),
            Self::MissingField { binding, field } => {
                write!(f, "field not found: {binding}.{field}")
            }
            Self::UnsupportedOp(op) => {
                write!(f, "unsupported op for portable relation engine: {op}")
            }
            Self::BadExpression(message) => {
                write!(f, "bad aggregate/compare expression: {message}")
            }
        }
    }
}

impl PortableRelationEngine {
    pub fn evaluate_rule(
        rule: &Rule,
        relations: &BTreeMap<String, Relation>,
    ) -> Result<RuleResult, RelationEngineError> {
        evaluate_rule(rule, relations)
    }
}

pub fn evaluate_rule(
    rule: &Rule,
    relations: &BTreeMap<String, Relation>,
) -> Result<RuleResult, RelationEngineError> {
    let affected = match &rule.check {
        Check::RowCount { binding, compare } => evaluate_row_count(binding, compare, relations)?,
        Check::AggregateCompare {
            binding,
            aggregate,
            compare,
        } => evaluate_aggregate_compare(binding, aggregate, compare, relations)?,
        Check::ForeignKey {
            binding,
            columns,
            ref_binding,
            ref_columns,
        } => evaluate_foreign_key(binding, columns, ref_binding, ref_columns, relations)?,
        other => return Err(RelationEngineError::UnsupportedOp(other.op().to_owned())),
    };

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

fn evaluate_row_count(
    binding: &str,
    compare: &Comparison,
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, RelationEngineError> {
    let relation = relation(binding, relations)?;
    let observed = numeric_value(relation.row_count() as f64)?;

    if evaluate_comparison(&observed, compare)? {
        Ok(Vec::new())
    } else {
        Ok(vec![AffectedEntry {
            binding: binding.to_owned(),
            key: None,
            field: None,
            value: Some(observed),
        }])
    }
}

fn evaluate_aggregate_compare(
    binding: &str,
    aggregate: &Aggregate,
    compare: &Comparison,
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, RelationEngineError> {
    let relation = relation(binding, relations)?;
    let observed = compute_aggregate(binding, relation, aggregate)?;

    if evaluate_comparison(&observed, compare)? {
        Ok(Vec::new())
    } else {
        let field = aggregate_field(aggregate)?.to_owned();
        Ok(vec![AffectedEntry {
            binding: binding.to_owned(),
            key: None,
            field: Some(field),
            value: Some(observed),
        }])
    }
}

fn evaluate_foreign_key(
    binding: &str,
    columns: &[String],
    ref_binding: &str,
    ref_columns: &[String],
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, RelationEngineError> {
    if columns.len() != ref_columns.len() {
        return Err(RelationEngineError::BadExpression(
            "foreign_key requires matching columns and ref_columns lengths".to_owned(),
        ));
    }

    let source_relation = relation(binding, relations)?;
    let referenced_relation = relation(ref_binding, relations)?;
    let ref_keys = build_reference_key_set(ref_binding, ref_columns, referenced_relation)?;
    let mut affected = Vec::new();

    for row in &source_relation.rows {
        let tuple = tuple_from_row(binding, columns, row)?;
        if !ref_keys.contains(&tuple_key(&tuple)) {
            affected.push(AffectedEntry {
                binding: binding.to_owned(),
                key: extract_key(row, &source_relation.key_fields),
                field: if columns.len() == 1 {
                    Some(columns[0].clone())
                } else {
                    None
                },
                value: Some(tuple_value(&tuple)),
            });
        }
    }

    Ok(affected)
}

fn compute_aggregate(
    binding: &str,
    relation: &Relation,
    aggregate: &Aggregate,
) -> Result<Value, RelationEngineError> {
    match aggregate_kind(aggregate)? {
        AggregateKind::Sum(field) => {
            let mut sum = 0.0_f64;
            for row in &relation.rows {
                let value = require_field(binding, row, field)?;
                sum += as_f64(binding, field, value)?;
            }
            numeric_value(sum)
        }
        AggregateKind::Avg(field) => {
            if relation.rows.is_empty() {
                return Err(RelationEngineError::BadExpression(
                    "aggregate_compare avg requires at least one row".to_owned(),
                ));
            }

            let mut sum = 0.0_f64;
            for row in &relation.rows {
                let value = require_field(binding, row, field)?;
                sum += as_f64(binding, field, value)?;
            }
            numeric_value(sum / relation.rows.len() as f64)
        }
        AggregateKind::Min(field) => {
            let mut values = aggregate_values(binding, relation, field)?;
            let first = values.pop_first().ok_or_else(|| {
                RelationEngineError::BadExpression(
                    "aggregate_compare min requires at least one row".to_owned(),
                )
            })?;
            Ok(first)
        }
        AggregateKind::Max(field) => {
            let mut values = aggregate_values(binding, relation, field)?;
            let last = values.pop_last().ok_or_else(|| {
                RelationEngineError::BadExpression(
                    "aggregate_compare max requires at least one row".to_owned(),
                )
            })?;
            Ok(last)
        }
    }
}

fn aggregate_values(
    binding: &str,
    relation: &Relation,
    field: &str,
) -> Result<SortableValueSet, RelationEngineError> {
    let mut values = SortableValueSet::default();
    for row in &relation.rows {
        let value = require_field(binding, row, field)?.clone();
        ensure_comparable(field, &value)?;
        values.insert(value);
    }
    Ok(values)
}

fn evaluate_comparison(
    observed: &Value,
    compare: &Comparison,
) -> Result<bool, RelationEngineError> {
    let predicates = [
        compare
            .eq
            .as_ref()
            .map(|expected| compare_eq(observed, expected, compare.tolerance)),
        compare
            .ne
            .as_ref()
            .map(|expected| Ok(!compare_eq(observed, expected, compare.tolerance)?)),
        compare
            .gt
            .as_ref()
            .map(|expected| compare_ordering(observed, expected, Ordering::Greater)),
        compare.gte.as_ref().map(|expected| {
            let ordering = compare_values(observed, expected).ok_or_else(|| {
                RelationEngineError::BadExpression("values are not comparable".to_owned())
            })?;
            Ok(matches!(ordering, Ordering::Greater | Ordering::Equal))
        }),
        compare
            .lt
            .as_ref()
            .map(|expected| compare_ordering(observed, expected, Ordering::Less)),
        compare.lte.as_ref().map(|expected| {
            let ordering = compare_values(observed, expected).ok_or_else(|| {
                RelationEngineError::BadExpression("values are not comparable".to_owned())
            })?;
            Ok(matches!(ordering, Ordering::Less | Ordering::Equal))
        }),
    ];

    let mut saw_predicate = false;
    for predicate in predicates.into_iter().flatten() {
        saw_predicate = true;
        if !predicate? {
            return Ok(false);
        }
    }

    if saw_predicate {
        Ok(true)
    } else {
        Err(RelationEngineError::BadExpression(
            "comparison requires at least one operator".to_owned(),
        ))
    }
}

fn compare_eq(
    observed: &Value,
    expected: &Value,
    tolerance: Option<f64>,
) -> Result<bool, RelationEngineError> {
    match (observed, expected, tolerance) {
        (Value::Number(left), Value::Number(right), Some(tolerance)) => {
            let left = left.as_f64().ok_or_else(|| {
                RelationEngineError::BadExpression("invalid numeric value".to_owned())
            })?;
            let right = right.as_f64().ok_or_else(|| {
                RelationEngineError::BadExpression("invalid numeric value".to_owned())
            })?;
            Ok((left - right).abs() <= tolerance)
        }
        _ => Ok(values_equal(observed, expected)),
    }
}

fn compare_ordering(
    observed: &Value,
    expected: &Value,
    target: Ordering,
) -> Result<bool, RelationEngineError> {
    let ordering = compare_values(observed, expected).ok_or_else(|| {
        RelationEngineError::BadExpression("values are not comparable".to_owned())
    })?;
    Ok(ordering == target)
}

fn relation<'a>(
    binding: &str,
    relations: &'a BTreeMap<String, Relation>,
) -> Result<&'a Relation, RelationEngineError> {
    relations
        .get(binding)
        .ok_or_else(|| RelationEngineError::MissingBinding(binding.to_owned()))
}

fn require_field<'a>(
    binding: &str,
    row: &'a BTreeMap<String, Value>,
    field: &str,
) -> Result<&'a Value, RelationEngineError> {
    row.get(field)
        .ok_or_else(|| RelationEngineError::MissingField {
            binding: binding.to_owned(),
            field: field.to_owned(),
        })
}

fn build_reference_key_set(
    binding: &str,
    columns: &[String],
    relation: &Relation,
) -> Result<BTreeSet<Vec<String>>, RelationEngineError> {
    let mut keys = BTreeSet::new();
    for row in &relation.rows {
        let tuple = tuple_from_row(binding, columns, row)?;
        keys.insert(tuple_key(&tuple));
    }
    Ok(keys)
}

fn tuple_from_row(
    binding: &str,
    columns: &[String],
    row: &BTreeMap<String, Value>,
) -> Result<Vec<Value>, RelationEngineError> {
    columns
        .iter()
        .map(|column| require_field(binding, row, column).cloned())
        .collect()
}

fn tuple_key(values: &[Value]) -> Vec<String> {
    values.iter().map(value_to_sort_key).collect()
}

fn tuple_value(values: &[Value]) -> Value {
    if values.len() == 1 {
        values[0].clone()
    } else {
        Value::Array(values.to_vec())
    }
}

fn aggregate_kind<'a>(aggregate: &'a Aggregate) -> Result<AggregateKind<'a>, RelationEngineError> {
    let mut selected = Vec::new();
    if let Some(field) = aggregate.sum.as_deref() {
        selected.push(AggregateKind::Sum(field));
    }
    if let Some(field) = aggregate.avg.as_deref() {
        selected.push(AggregateKind::Avg(field));
    }
    if let Some(field) = aggregate.min.as_deref() {
        selected.push(AggregateKind::Min(field));
    }
    if let Some(field) = aggregate.max.as_deref() {
        selected.push(AggregateKind::Max(field));
    }

    match selected.len() {
        1 => Ok(selected.remove(0)),
        0 => Err(RelationEngineError::BadExpression(
            "aggregate_compare requires exactly one aggregate operator".to_owned(),
        )),
        _ => Err(RelationEngineError::BadExpression(
            "aggregate_compare requires exactly one aggregate operator".to_owned(),
        )),
    }
}

fn aggregate_field(aggregate: &Aggregate) -> Result<&str, RelationEngineError> {
    match aggregate_kind(aggregate)? {
        AggregateKind::Sum(field)
        | AggregateKind::Avg(field)
        | AggregateKind::Min(field)
        | AggregateKind::Max(field) => Ok(field),
    }
}

fn ensure_comparable(field: &str, value: &Value) -> Result<(), RelationEngineError> {
    match value {
        Value::Null | Value::Number(_) | Value::String(_) | Value::Bool(_) => Ok(()),
        _ => Err(RelationEngineError::BadExpression(format!(
            "aggregate field {field} must contain comparable scalar values"
        ))),
    }
}

fn as_f64(binding: &str, field: &str, value: &Value) -> Result<f64, RelationEngineError> {
    value.as_f64().ok_or_else(|| {
        RelationEngineError::BadExpression(format!(
            "aggregate field {binding}.{field} must contain numeric values"
        ))
    })
}

fn numeric_value(value: f64) -> Result<Value, RelationEngineError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| RelationEngineError::BadExpression("invalid numeric aggregate".to_owned()))
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => match (l.as_f64(), r.as_f64()) {
            (Some(lf), Some(rf)) => lf == rf,
            _ => false,
        },
        _ => left == right,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => {
            l.as_f64().zip(r.as_f64()).map(|(lf, rf)| lf.total_cmp(&rf))
        }
        (Value::String(l), Value::String(r)) => Some(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

fn extract_key(
    row: &BTreeMap<String, Value>,
    key_fields: &[String],
) -> Option<BTreeMap<String, Value>> {
    if key_fields.is_empty() {
        return None;
    }

    let mut key = BTreeMap::new();
    for field in key_fields {
        key.insert(
            field.clone(),
            row.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    Some(key)
}

fn value_to_sort_key(value: &Value) -> String {
    match value {
        Value::Null => "\0NULL".to_owned(),
        Value::String(s) => format!("S:{s}"),
        Value::Number(n) => format!("N:{n}"),
        Value::Bool(b) => format!("B:{b}"),
        other => format!("O:{other}"),
    }
}

enum AggregateKind<'a> {
    Sum(&'a str),
    Avg(&'a str),
    Min(&'a str),
    Max(&'a str),
}

#[derive(Default)]
struct SortableValueSet(BTreeMap<String, Value>);

impl SortableValueSet {
    fn insert(&mut self, value: Value) {
        self.0.insert(value_to_sort_key(&value), value);
    }

    fn pop_first(&mut self) -> Option<Value> {
        let key = self.0.keys().next().cloned()?;
        self.0.remove(&key)
    }

    fn pop_last(&mut self) -> Option<Value> {
        let key = self.0.keys().next_back().cloned()?;
        self.0.remove(&key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Value, json};
    use verify_core::constraint::{Aggregate, Check, Comparison, Portability, Rule, Severity};
    use verify_core::report::ResultStatus;

    use super::{PORTABLE_RELATION_OPS, RelationEngineError, evaluate_rule};
    use crate::Relation;

    fn make_rule(id: &str, severity: Severity, check: Check) -> Rule {
        Rule {
            id: id.to_owned(),
            severity,
            portability: Portability::Portable,
            check,
        }
    }

    fn row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect()
    }

    fn relations_with(
        name: &str,
        key_fields: &[&str],
        rows: Vec<BTreeMap<String, Value>>,
    ) -> BTreeMap<String, Relation> {
        BTreeMap::from([(
            name.to_owned(),
            Relation::new(
                key_fields.iter().map(|field| (*field).to_owned()).collect(),
                rows,
            ),
        )])
    }

    #[test]
    fn row_count_passes_when_comparison_matches() {
        let rule = make_rule(
            "MIN_ROWS",
            Severity::Error,
            Check::RowCount {
                binding: "input".to_owned(),
                compare: Comparison {
                    gte: Some(json!(2)),
                    ..Default::default()
                },
            },
        );
        let relations = relations_with(
            "input",
            &[],
            vec![row(&[("id", json!("A"))]), row(&[("id", json!("B"))])],
        );

        let result = evaluate_rule(&rule, &relations).expect("row_count evaluates");

        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
    }

    #[test]
    fn row_count_failure_localizes_to_binding() {
        let rule = make_rule(
            "EXACT_ROWS",
            Severity::Warn,
            Check::RowCount {
                binding: "input".to_owned(),
                compare: Comparison {
                    eq: Some(json!(3)),
                    ..Default::default()
                },
            },
        );
        let relations = relations_with("input", &[], vec![row(&[("id", json!("A"))])]);

        let result = evaluate_rule(&rule, &relations).expect("row_count evaluates");

        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
        assert_eq!(result.affected[0].binding, "input");
        assert_eq!(result.affected[0].value, Some(json!(1.0)));
    }

    #[test]
    fn aggregate_compare_supports_sum_with_tolerance() {
        let rule = make_rule(
            "TOTAL_BALANCE",
            Severity::Error,
            Check::AggregateCompare {
                binding: "input".to_owned(),
                aggregate: Aggregate {
                    sum: Some("balance".to_owned()),
                    ..Default::default()
                },
                compare: Comparison {
                    eq: Some(json!(150.0)),
                    tolerance: Some(0.1),
                    ..Default::default()
                },
            },
        );
        let relations = relations_with(
            "input",
            &["loan_id"],
            vec![
                row(&[("loan_id", json!("LN-1")), ("balance", json!(100.0))]),
                row(&[("loan_id", json!("LN-2")), ("balance", json!(50.0))]),
            ],
        );

        let result = evaluate_rule(&rule, &relations).expect("aggregate compares");

        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
    }

    #[test]
    fn aggregate_compare_failure_reports_observed_value() {
        let rule = make_rule(
            "AVG_BALANCE",
            Severity::Warn,
            Check::AggregateCompare {
                binding: "input".to_owned(),
                aggregate: Aggregate {
                    avg: Some("balance".to_owned()),
                    ..Default::default()
                },
                compare: Comparison {
                    gt: Some(json!(100.0)),
                    ..Default::default()
                },
            },
        );
        let relations = relations_with(
            "input",
            &[],
            vec![
                row(&[("balance", json!(100.0))]),
                row(&[("balance", json!(60.0))]),
            ],
        );

        let result = evaluate_rule(&rule, &relations).expect("aggregate compares");

        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].field.as_deref(), Some("balance"));
        assert_eq!(result.affected[0].value, Some(json!(80.0)));
    }

    #[test]
    fn foreign_key_failures_localize_missing_reference_rows() {
        let rule = make_rule(
            "PROPERTY_TENANT_EXISTS",
            Severity::Error,
            Check::ForeignKey {
                binding: "property".to_owned(),
                columns: vec!["tenant_id".to_owned()],
                ref_binding: "tenants".to_owned(),
                ref_columns: vec!["tenant_id".to_owned()],
            },
        );
        let relations = BTreeMap::from([
            (
                "property".to_owned(),
                Relation::new(
                    vec!["property_id".to_owned()],
                    vec![
                        row(&[
                            ("property_id", json!("P-001")),
                            ("tenant_id", json!("T-001")),
                        ]),
                        row(&[
                            ("property_id", json!("P-003")),
                            ("tenant_id", json!("T-999")),
                        ]),
                    ],
                ),
            ),
            (
                "tenants".to_owned(),
                Relation::new(
                    vec!["tenant_id".to_owned()],
                    vec![row(&[
                        ("tenant_id", json!("T-001")),
                        ("tenant_name", json!("Alice Tenant")),
                    ])],
                ),
            ),
        ]);

        let result = evaluate_rule(&rule, &relations).expect("foreign key evaluates");

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
        assert_eq!(result.affected[0].field.as_deref(), Some("tenant_id"));
        assert_eq!(result.affected[0].value, Some(json!("T-999")));
    }

    #[test]
    fn missing_field_returns_error() {
        let rule = make_rule(
            "TOTAL_BALANCE",
            Severity::Error,
            Check::AggregateCompare {
                binding: "input".to_owned(),
                aggregate: Aggregate {
                    sum: Some("balance".to_owned()),
                    ..Default::default()
                },
                compare: Comparison {
                    eq: Some(json!(1.0)),
                    ..Default::default()
                },
            },
        );
        let relations = relations_with("input", &[], vec![row(&[("other", json!(1.0))])]);

        let error = evaluate_rule(&rule, &relations).expect_err("missing field should refuse");

        assert_eq!(
            error,
            RelationEngineError::MissingField {
                binding: "input".to_owned(),
                field: "balance".to_owned(),
            }
        );
    }

    #[test]
    fn exports_expected_relation_ops() {
        assert_eq!(
            PORTABLE_RELATION_OPS,
            &["row_count", "aggregate_compare", "foreign_key"]
        );
    }
}
