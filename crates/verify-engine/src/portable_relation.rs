use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Number, Value};
use verify_core::{
    constraint::{Aggregate, Check, Comparison, Rule},
    report::{AffectedEntry, ResultStatus, RuleResult},
};

use crate::{
    Relation,
    scalar::{value_category, value_type},
};

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
        AggregateKind::Min(field) => extremum(binding, relation, field, Extremum::Min),
        AggregateKind::Max(field) => extremum(binding, relation, field, Extremum::Max),
    }
}

#[derive(Clone, Copy)]
enum Extremum {
    Min,
    Max,
}

/// `min` and `max` as docs/plan.md defines them: every value non-null and of one scalar type.
/// The refusal chosen does not depend on row order.
fn extremum(
    binding: &str,
    relation: &Relation,
    field: &str,
    which: Extremum,
) -> Result<Value, RelationEngineError> {
    let (name, wanted) = match which {
        Extremum::Min => ("min", Ordering::Less),
        Extremum::Max => ("max", Ordering::Greater),
    };

    let mut values = Vec::with_capacity(relation.rows.len());
    for row in &relation.rows {
        let value = require_field(binding, row, field)?;
        ensure_comparable(field, value)?;
        values.push(value);
    }
    if values.iter().any(|value| value.is_null()) {
        return Err(RelationEngineError::BadExpression(format!(
            "aggregate_compare {name} requires non-null values in {binding}.{field}"
        )));
    }
    let Some((&first, rest)) = values.split_first() else {
        return Err(RelationEngineError::BadExpression(format!(
            "aggregate_compare {name} requires at least one row"
        )));
    };
    let mixed = || {
        RelationEngineError::BadExpression(format!(
            "aggregate field {binding}.{field} must contain values of one scalar type"
        ))
    };
    if rest
        .iter()
        .any(|value| value_category(value) != value_category(first))
    {
        return Err(mixed());
    }

    let mut selected = first;
    for &value in rest {
        let ordering = extremum_order(value, selected).ok_or_else(mixed)?;
        if ordering == wanted
            || (ordering == Ordering::Equal && tie_rank(value) < tie_rank(selected))
        {
            selected = value;
        }
    }
    Ok(selected.clone())
}

/// Order within one scalar type: numbers by exact value, strings by code point, `false` first.
fn extremum_order(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => exact_numeric_order(left, right),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

/// Compare the numbers as received, never rounding an integer to `f64`. `-0.0` equals `0.0`.
fn exact_numeric_order(left: &Number, right: &Number) -> Option<Ordering> {
    match (integer_value(left), integer_value(right)) {
        (Some(left), Some(right)) => Some(left.cmp(&right)),
        (Some(left), None) => integer_float_order(left, right.as_f64()?),
        (None, Some(right)) => integer_float_order(right, left.as_f64()?).map(Ordering::reverse),
        (None, None) => left.as_f64()?.partial_cmp(&right.as_f64()?),
    }
}

fn integer_value(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

/// `integer` against a finite `float`: whole parts first, then the float's fraction.
fn integer_float_order(integer: i128, float: f64) -> Option<Ordering> {
    let whole = float.trunc();
    let fraction_order = 0.0_f64.partial_cmp(&(float - whole))?;
    // The cast saturates beyond i128, which no i64 or u64 reaches, so the order holds.
    Some(integer.cmp(&(whole as i128)).then(fraction_order))
}

/// Which of two equal values is reported: an integer over a fraction-form number, and `0.0`
/// over `-0.0`, so that row order cannot change the report.
fn tie_rank(value: &Value) -> u8 {
    match value {
        Value::Number(number) if number.is_f64() => {
            let negative_zero = number
                .as_f64()
                .is_some_and(|float| float == 0.0 && float.is_sign_negative());
            if negative_zero { 2 } else { 1 }
        }
        _ => 0,
    }
}

fn evaluate_comparison(
    observed: &Value,
    compare: &Comparison,
) -> Result<bool, RelationEngineError> {
    check_threshold_types(observed, compare)?;

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

/// Every threshold must have the observed value's scalar type, and null matches none. All
/// operators are checked before any is decided, so a false one cannot hide a mistyped one.
fn check_threshold_types(
    observed: &Value,
    compare: &Comparison,
) -> Result<(), RelationEngineError> {
    let thresholds = [
        ("eq", &compare.eq),
        ("ne", &compare.ne),
        ("gt", &compare.gt),
        ("gte", &compare.gte),
        ("lt", &compare.lt),
        ("lte", &compare.lte),
    ];
    for (operator, threshold) in thresholds {
        let Some(threshold) = threshold else {
            continue;
        };
        if threshold.is_null() || value_category(threshold) != value_category(observed) {
            return Err(RelationEngineError::BadExpression(format!(
                "{operator} cannot compare an observed {} with a threshold of type {}",
                value_type(observed),
                value_type(threshold)
            )));
        }
    }
    Ok(())
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

    /// The value `min` or `max` selects over one column, read before any threshold comparison.
    fn extremum(function: &str, values: &[Value]) -> Result<Value, RelationEngineError> {
        let relation = Relation::new(
            Vec::new(),
            values
                .iter()
                .map(|value| row(&[("v", value.clone())]))
                .collect(),
        );
        let field = Some("v".to_owned());
        let aggregate = match function {
            "min" => Aggregate {
                min: field,
                ..Default::default()
            },
            _ => Aggregate {
                max: field,
                ..Default::default()
            },
        };
        super::compute_aggregate("input", &relation, &aggregate)
    }

    /// `expected` is compared as serialized text, which tells `10` from `10.0` and `0.0` from
    /// `-0.0`. Every rotation of the rows, forward and reversed, must select the same value.
    fn assert_extremum(function: &str, values: &[Value], expected: &Value) {
        let mut rows = values.to_vec();
        for _ in 0..2 {
            for _ in 0..rows.len() {
                rows.rotate_left(1);
                let selected = extremum(function, &rows).expect("column is orderable");
                assert_eq!(
                    selected.to_string(),
                    expected.to_string(),
                    "{function} over {rows:?}"
                );
            }
            rows.reverse();
        }
    }

    #[test]
    fn min_and_max_order_numbers_by_value() {
        let two_pow_53 = 9_007_199_254_740_992_u64;
        let cases: Vec<(Vec<Value>, Value, Value)> = vec![
            (vec![json!(9), json!(10)], json!(9), json!(10)),
            (vec![json!(100), json!(20), json!(3)], json!(3), json!(100)),
            (vec![json!(-9), json!(-10)], json!(-10), json!(-9)),
            (
                vec![json!(-1.5), json!(-3), json!(2), json!(-20.25)],
                json!(-20.25),
                json!(2),
            ),
            (
                vec![json!(2.0), json!(10), json!(9.5)],
                json!(2.0),
                json!(10),
            ),
            (
                vec![json!(0), json!(i64::MAX), json!(i64::MIN)],
                json!(i64::MIN),
                json!(i64::MAX),
            ),
            (
                vec![json!(i64::MAX), json!(u64::MAX), json!(-1)],
                json!(-1),
                json!(u64::MAX),
            ),
            // Neighbours that share one f64.
            (
                vec![json!(two_pow_53), json!(two_pow_53 + 1)],
                json!(two_pow_53),
                json!(two_pow_53 + 1),
            ),
            (
                vec![
                    json!(-9_007_199_254_740_993_i64),
                    json!(-9_007_199_254_740_992_i64),
                ],
                json!(-9_007_199_254_740_993_i64),
                json!(-9_007_199_254_740_992_i64),
            ),
            // An integer against a fraction-form number, without rounding the integer.
            (
                vec![json!(two_pow_53 + 1), json!(9_007_199_254_740_992.0)],
                json!(9_007_199_254_740_992.0),
                json!(two_pow_53 + 1),
            ),
            // u64::MAX is one below 2^64, which is the f64 it would round to.
            (
                vec![json!(u64::MAX), json!(18_446_744_073_709_551_616.0)],
                json!(u64::MAX),
                json!(18_446_744_073_709_551_616.0),
            ),
            (
                vec![json!(i64::MIN), json!(-1e300), json!(1e300)],
                json!(-1e300),
                json!(1e300),
            ),
            (
                vec![json!(-2), json!(-2.5), json!(-1.5)],
                json!(-2.5),
                json!(-1.5),
            ),
            // Long decimals as the engine receives them, and exponent spellings.
            (
                vec![
                    json!(0.3),
                    json!(0.30000000000000004),
                    json!(0.29999999999999993),
                ],
                json!(0.29999999999999993),
                json!(0.30000000000000004),
            ),
            (
                vec![json!(1e-7), json!(0.5), json!(5e-324), json!(1e308)],
                json!(5e-324),
                json!(1e308),
            ),
        ];

        for (values, minimum, maximum) in &cases {
            assert_extremum("min", values, minimum);
            assert_extremum("max", values, maximum);
        }
    }

    #[test]
    fn min_and_max_break_numeric_ties_independently_of_row_order() {
        for function in ["min", "max"] {
            assert_extremum(function, &[json!(10), json!(10.0)], &json!(10));
            assert_extremum(function, &[json!(0.0), json!(-0.0)], &json!(0.0));
            assert_extremum(function, &[json!(0), json!(-0.0), json!(0.0)], &json!(0));
            assert_extremum(function, &[json!(-0.0), json!(-0.0)], &json!(-0.0));
        }
    }

    #[test]
    fn min_and_max_keep_string_and_boolean_ordering() {
        let strings = [json!("9"), json!("10"), json!("a"), json!("A")];
        assert_extremum("min", &strings, &json!("10"));
        assert_extremum("max", &strings, &json!("a"));
        assert_extremum("min", &[json!(true), json!(false)], &json!(false));
        assert_extremum("max", &[json!(true), json!(false)], &json!(true));
    }

    #[test]
    fn min_and_max_refuse_nulls_mixed_types_and_empty_input() {
        let refused: [(&str, Vec<Value>); 10] = [
            ("non-null", vec![Value::Null, json!(5)]),
            ("non-null", vec![json!("a"), Value::Null]),
            ("non-null", vec![Value::Null]),
            ("non-null", vec![Value::Null, Value::Null]),
            // A null outranks a type mixture, whatever the row order.
            ("non-null", vec![json!(1), json!("1"), Value::Null]),
            ("one scalar type", vec![json!(1), json!("1")]),
            ("one scalar type", vec![json!(true), json!(1)]),
            ("one scalar type", vec![json!("a"), json!(false)]),
            ("comparable scalar", vec![json!(1), json!([1])]),
            ("at least one row", Vec::new()),
        ];

        for function in ["min", "max"] {
            for (reason, values) in &refused {
                let mut rows = values.clone();
                for _ in 0..2 {
                    let error = extremum(function, &rows)
                        .expect_err(&format!("{function} over {rows:?} must refuse"));
                    assert!(
                        matches!(&error, RelationEngineError::BadExpression(message)
                            if message.contains(reason)),
                        "{function} over {rows:?}: {error}"
                    );
                    rows.reverse();
                }
            }
        }
    }

    /// Parsed as an artifact's `compare` is, so an explicit null stays one.
    fn comparison(compare: Value) -> Comparison {
        serde_json::from_value(compare).expect("comparison parses")
    }

    fn with_threshold(operator: &str, threshold: &Value) -> serde_json::Map<String, Value> {
        let mut compare = serde_json::Map::new();
        compare.insert(operator.to_owned(), threshold.clone());
        compare
    }

    const OPERATORS: [&str; 6] = ["eq", "ne", "gt", "gte", "lt", "lte"];

    fn assert_threshold_refused(observed: &Value, compare: Value) {
        let outcome = super::evaluate_comparison(observed, &comparison(compare.clone()));
        assert!(
            matches!(&outcome, Err(RelationEngineError::BadExpression(message))
                if message.contains("cannot compare")),
            "observed {observed} with {compare}: {outcome:?}"
        );
    }

    #[test]
    fn wrong_typed_thresholds_refuse_for_every_operator() {
        let cases = [
            (
                json!(10),
                vec![
                    json!("10"),
                    json!("x"),
                    json!(true),
                    json!(false),
                    Value::Null,
                    json!([10]),
                    json!([]),
                    json!({}),
                ],
            ),
            (
                json!("10"),
                vec![
                    json!(10),
                    json!(true),
                    Value::Null,
                    json!(["10"]),
                    json!({}),
                ],
            ),
            (
                json!(false),
                vec![
                    json!(0),
                    json!("false"),
                    Value::Null,
                    json!([false]),
                    json!({}),
                ],
            ),
        ];

        for (observed, thresholds) in &cases {
            for threshold in thresholds {
                for operator in OPERATORS {
                    let compare = with_threshold(operator, threshold);
                    assert_threshold_refused(observed, Value::Object(compare.clone()));
                    let mut tolerated = compare;
                    tolerated.insert("tolerance".to_owned(), json!(100));
                    assert_threshold_refused(observed, Value::Object(tolerated));
                }
            }
        }
    }

    /// One mistyped operator refuses the comparison wherever it sits and whatever the others
    /// decide.
    #[test]
    fn every_operator_is_type_checked_before_a_verdict() {
        for wrong in OPERATORS {
            for right in OPERATORS {
                if wrong == right {
                    continue;
                }
                // Among these thresholds each well-typed operator is true once and false once.
                for threshold in [json!(5), json!(10), json!(15)] {
                    for mistyped in [json!("x"), Value::Null, json!([10])] {
                        let mut compare = with_threshold(wrong, &mistyped);
                        compare.insert(right.to_owned(), threshold.clone());
                        assert_threshold_refused(&json!(10), Value::Object(compare));
                    }
                }
            }
        }
    }

    #[test]
    fn same_type_comparisons_are_unchanged() {
        for (observed, below, equal, above) in [
            (json!(10), json!(9), json!(10.0), json!(11)),
            (json!("m"), json!("a"), json!("m"), json!("z")),
        ] {
            for (threshold, holds) in [
                (&below, [false, true, true, true, false, false]),
                (&equal, [true, false, false, true, false, true]),
                (&above, [false, true, false, false, true, true]),
            ] {
                for (operator, expected) in OPERATORS.into_iter().zip(holds) {
                    let compare = Value::Object(with_threshold(operator, threshold));
                    assert_eq!(
                        super::evaluate_comparison(&observed, &comparison(compare.clone())),
                        Ok(expected),
                        "observed {observed} with {compare}"
                    );
                }
            }
        }
        for (threshold, holds) in [
            (false, [false, true, true, true, false, false]),
            (true, [true, false, false, true, false, true]),
        ] {
            for (operator, expected) in OPERATORS.into_iter().zip(holds) {
                let compare = Value::Object(with_threshold(operator, &json!(threshold)));
                assert_eq!(
                    super::evaluate_comparison(&json!(true), &comparison(compare)),
                    Ok(expected),
                    "observed true with {operator} {threshold}"
                );
            }
        }
        for (observed, compare, expected) in [
            (json!(10), json!({ "gte": 5, "lte": 99 }), true),
            (json!(10), json!({ "gte": 5, "lte": 9 }), false),
            (json!(10), json!({ "eq": 12, "tolerance": 2 }), true),
            (json!(10), json!({ "ne": 12, "tolerance": 2 }), false),
            // A tolerance stays ignored by ordering operators and by strings.
            (json!(10), json!({ "gt": 10, "tolerance": 5 }), false),
            (json!("10"), json!({ "eq": "11", "tolerance": 5 }), false),
        ] {
            assert_eq!(
                super::evaluate_comparison(&observed, &comparison(compare.clone())),
                Ok(expected),
                "observed {observed} with {compare}"
            );
        }
        assert!(matches!(
            super::evaluate_comparison(&json!(10), &comparison(json!({}))),
            Err(RelationEngineError::BadExpression(message))
                if message.contains("at least one operator")
        ));
    }

    #[test]
    fn row_count_and_every_aggregate_refuse_a_wrong_typed_threshold() {
        let relations = relations_with(
            "input",
            &[],
            vec![row(&[("v", json!(9))]), row(&[("v", json!(10))])],
        );
        let mut checks = vec![json!({ "op": "row_count", "binding": "input" })];
        for function in ["min", "max", "sum", "avg"] {
            let mut aggregate = serde_json::Map::new();
            aggregate.insert(function.to_owned(), json!("v"));
            checks.push(
                json!({ "op": "aggregate_compare", "binding": "input", "aggregate": aggregate }),
            );
        }

        for check in checks {
            for operator in OPERATORS {
                let mut check = check.clone();
                check["compare"] = Value::Object(with_threshold(operator, &json!("x")));
                let rule = make_rule(
                    "THRESHOLD",
                    Severity::Error,
                    serde_json::from_value(check.clone()).expect("check parses"),
                );
                let outcome = evaluate_rule(&rule, &relations);
                assert!(
                    matches!(&outcome, Err(RelationEngineError::BadExpression(message))
                        if message.contains("cannot compare")),
                    "{check}: {outcome:?}"
                );
            }
        }
    }

    #[test]
    fn exports_expected_relation_ops() {
        assert_eq!(
            PORTABLE_RELATION_OPS,
            &["row_count", "aggregate_compare", "foreign_key"]
        );
    }
}
