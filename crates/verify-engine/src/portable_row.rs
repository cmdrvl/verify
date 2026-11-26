use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde_json::{Number, Value, json};
use verify_core::{
    constraint::{
        Check, MembershipOperand, Portability, PredicateExpression, PredicateOperand, Rule,
    },
    report::{AffectedEntry, ResultStatus, RuleResult},
    validation::analyze_predicate,
};

use crate::{
    Relation,
    scalar::{
        compare_values as compare_scalar_values, is_blank, value_type,
        values_equal as scalar_values_equal,
    },
};

pub const PORTABLE_ROW_OPS: &[&str] = &["unique", "not_null", "predicate"];

/// Localized context for an invalid scalar comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomparableOperandsDetail {
    pub operator: String,
    pub left_type: String,
    pub right_type: String,
    pub binding: String,
    pub key: Option<BTreeMap<String, Value>>,
    pub field: Option<String>,
}

/// Errors that can occur during portable rule evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// Binding name not found in the provided relations.
    MissingBinding(String),
    /// Rule op not supported by this engine surface.
    UnsupportedOp(String),
    /// Invalid predicate expression structure.
    BadExpression(String),
    /// Predicate operands do not belong to comparable scalar categories.
    IncomparableOperands(Box<IncomparableOperandsDetail>),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBinding(name) => write!(f, "binding not found: {name}"),
            Self::UnsupportedOp(op) => write!(f, "unsupported op for portable row engine: {op}"),
            Self::BadExpression(msg) => write!(f, "bad predicate expression: {msg}"),
            Self::IncomparableOperands(detail) => write!(
                f,
                "predicate operator {} cannot compare {} with {}",
                detail.operator, detail.left_type, detail.right_type
            ),
        }
    }
}

impl EngineError {
    pub const fn is_bad_expression(&self) -> bool {
        matches!(self, Self::BadExpression(_) | Self::IncomparableOperands(_))
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::MissingBinding(binding) => json!({ "binding": binding }),
            Self::UnsupportedOp(op) => json!({ "op": op }),
            Self::BadExpression(detail) => json!({ "detail": detail }),
            Self::IncomparableOperands(detail) => json!({
                "operator": detail.operator,
                "left_type": detail.left_type,
                "right_type": detail.right_type,
                "binding": detail.binding,
                "key": detail.key,
                "field": detail.field,
            }),
        }
    }
}

/// Evaluate a portable row/column rule against bound relations.
///
/// Supports `unique`, `not_null`, and `predicate` ops.
/// Returns a `RuleResult` with localized affected entries.
pub fn evaluate_rule(
    rule: &Rule,
    relations: &BTreeMap<String, Relation>,
) -> Result<RuleResult, EngineError> {
    if rule.portability != Portability::Portable {
        return Err(EngineError::UnsupportedOp(format!(
            "{} (batch_only)",
            rule.check.op()
        )));
    }
    if let Check::Predicate { binding, expr } = &rule.check
        && analyze_predicate(binding, expr).derived_portability == Portability::BatchOnly
    {
        return Err(EngineError::UnsupportedOp(
            "binding-qualified predicate".to_owned(),
        ));
    }

    let affected = match &rule.check {
        Check::Unique { binding, columns } => evaluate_unique(binding, columns, relations)?,
        Check::NotNull { binding, columns } => evaluate_not_null(binding, columns, relations)?,
        Check::Predicate { binding, expr } => evaluate_predicate(binding, expr, relations)?,
        other => return Err(EngineError::UnsupportedOp(other.op().to_owned())),
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

/// Evaluate `unique` — no two rows may share the same tuple across the named columns.
fn evaluate_unique(
    binding: &str,
    columns: &[String],
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, EngineError> {
    let relation = relations
        .get(binding)
        .ok_or_else(|| EngineError::MissingBinding(binding.to_owned()))?;

    if relation.rows.is_empty() {
        return Ok(Vec::new());
    }

    // Track seen key tuples. Value is index of first occurrence.
    let mut seen: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    // Track which first-occurrence indices had duplicates found.
    let mut first_reported: BTreeMap<usize, bool> = BTreeMap::new();
    let mut affected = Vec::new();

    for (idx, row) in relation.rows.iter().enumerate() {
        let tuple: Vec<String> = columns
            .iter()
            .map(|col| unique_identity(row.get(col).unwrap_or(&Value::Null)))
            .collect();

        if let Some(&first_idx) = seen.get(&tuple) {
            // Report the first occurrence once if not already reported.
            if !first_reported.get(&first_idx).copied().unwrap_or(false) {
                first_reported.insert(first_idx, true);
                affected.push(unique_affected_entry(
                    binding,
                    &relation.rows[first_idx],
                    &relation.key_fields,
                    columns,
                ));
            }
            // Report this duplicate occurrence.
            affected.push(unique_affected_entry(
                binding,
                row,
                &relation.key_fields,
                columns,
            ));
        } else {
            seen.insert(tuple, idx);
        }
    }

    Ok(affected)
}

fn unique_affected_entry(
    binding: &str,
    row: &BTreeMap<String, Value>,
    key_fields: &[String],
    columns: &[String],
) -> AffectedEntry {
    let (field, value) = if columns.len() == 1 {
        (
            Some(columns[0].clone()),
            Some(row.get(&columns[0]).cloned().unwrap_or(Value::Null)),
        )
    } else {
        (
            None,
            Some(Value::Array(
                columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                    .collect(),
            )),
        )
    };

    AffectedEntry {
        binding: binding.to_owned(),
        key: extract_key(row, key_fields),
        field,
        value,
    }
}

/// Evaluate `not_null` — named columns must be present and non-blank for every row.
///
/// V0 missingness semantics:
/// - null is missing
/// - empty string is blank
/// - whitespace-only string is blank
/// - `not_null` fails on null, empty string, and whitespace-only string
fn evaluate_not_null(
    binding: &str,
    columns: &[String],
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, EngineError> {
    let relation = relations
        .get(binding)
        .ok_or_else(|| EngineError::MissingBinding(binding.to_owned()))?;

    let mut affected = Vec::new();

    for row in &relation.rows {
        for col in columns {
            let value = row.get(col).unwrap_or(&Value::Null);
            if is_blank(value) {
                affected.push(AffectedEntry {
                    binding: binding.to_owned(),
                    key: extract_key(row, &relation.key_fields),
                    field: Some(col.clone()),
                    value: Some(value.clone()),
                });
            }
        }
    }

    Ok(affected)
}

/// Evaluate `predicate` — a row-level boolean expression must evaluate true for every row.
fn evaluate_predicate(
    binding: &str,
    expr: &PredicateExpression,
    relations: &BTreeMap<String, Relation>,
) -> Result<Vec<AffectedEntry>, EngineError> {
    let relation = relations
        .get(binding)
        .ok_or_else(|| EngineError::MissingBinding(binding.to_owned()))?;

    let mut affected = Vec::new();

    for row in &relation.rows {
        let context = ExpressionContext {
            binding,
            key_fields: &relation.key_fields,
            row,
        };
        if !evaluate_expr(expr, context)? {
            let (field, value) = extract_predicate_localization(expr, row);
            affected.push(AffectedEntry {
                binding: binding.to_owned(),
                key: extract_key(row, &relation.key_fields),
                field,
                value,
            });
        }
    }

    Ok(affected)
}

// --- Predicate expression evaluator ---

#[derive(Debug, Clone, Copy)]
struct ExpressionContext<'a> {
    binding: &'a str,
    key_fields: &'a [String],
    row: &'a BTreeMap<String, Value>,
}

fn evaluate_expr(
    expr: &PredicateExpression,
    context: ExpressionContext<'_>,
) -> Result<bool, EngineError> {
    match expr {
        PredicateExpression::Column(col_ref) => {
            let value = context.row.get(&col_ref.column).unwrap_or(&Value::Null);
            Ok(!is_blank(value))
        }
        PredicateExpression::Eq { eq } => {
            let left = resolve_operand(&eq[0], context.row);
            let right = resolve_operand(&eq[1], context.row);
            values_equal("eq", &left, &right, context, binary_field(eq).as_deref())
        }
        PredicateExpression::Ne { ne } => {
            let left = resolve_operand(&ne[0], context.row);
            let right = resolve_operand(&ne[1], context.row);
            values_equal("ne", &left, &right, context, binary_field(ne).as_deref())
                .map(|equal| !equal)
        }
        PredicateExpression::Gt { gt } => {
            let left = resolve_operand(&gt[0], context.row);
            let right = resolve_operand(&gt[1], context.row);
            compare_values("gt", &left, &right, context, binary_field(gt).as_deref())
                .map(|ordering| ordering == Ordering::Greater)
        }
        PredicateExpression::Gte { gte } => {
            let left = resolve_operand(&gte[0], context.row);
            let right = resolve_operand(&gte[1], context.row);
            compare_values("gte", &left, &right, context, binary_field(gte).as_deref())
                .map(|ordering| matches!(ordering, Ordering::Greater | Ordering::Equal))
        }
        PredicateExpression::Lt { lt } => {
            let left = resolve_operand(&lt[0], context.row);
            let right = resolve_operand(&lt[1], context.row);
            compare_values("lt", &left, &right, context, binary_field(lt).as_deref())
                .map(|ordering| ordering == Ordering::Less)
        }
        PredicateExpression::Lte { lte } => {
            let left = resolve_operand(&lte[0], context.row);
            let right = resolve_operand(&lte[1], context.row);
            compare_values("lte", &left, &right, context, binary_field(lte).as_deref())
                .map(|ordering| matches!(ordering, Ordering::Less | Ordering::Equal))
        }
        PredicateExpression::And { and } => {
            let mut result = true;
            for sub in and {
                result &= evaluate_expr(sub, context)?;
            }
            Ok(result)
        }
        PredicateExpression::Or { or } => {
            let mut result = false;
            for sub in or {
                result |= evaluate_expr(sub, context)?;
            }
            Ok(result)
        }
        PredicateExpression::Not { not } => Ok(!evaluate_expr(not, context)?),
        PredicateExpression::In { r#in } => {
            let value = resolve_membership_value(&r#in[0], context.row)?;
            let set = resolve_membership_set(&r#in[1])?;
            let field = membership_field(&r#in[0]);
            let mut matched = false;
            for member in set {
                if values_equal("in", &value, member, context, field.as_deref())? {
                    matched = true;
                }
            }
            Ok(matched)
        }
        PredicateExpression::IsNull { is_null: col_ref } => {
            let value = context.row.get(&col_ref.column).unwrap_or(&Value::Null);
            Ok(matches!(value, Value::Null))
        }
        PredicateExpression::IsBlank { is_blank: col_ref } => {
            let value = context.row.get(&col_ref.column).unwrap_or(&Value::Null);
            Ok(is_blank(value))
        }
    }
}

fn resolve_operand(operand: &PredicateOperand, row: &BTreeMap<String, Value>) -> Value {
    match operand {
        PredicateOperand::Column(col_ref) => {
            row.get(&col_ref.column).cloned().unwrap_or(Value::Null)
        }
        PredicateOperand::Literal(value) => value.clone(),
    }
}

fn resolve_membership_value(
    operand: &MembershipOperand,
    row: &BTreeMap<String, Value>,
) -> Result<Value, EngineError> {
    match operand {
        MembershipOperand::Operand(op) => Ok(resolve_operand(op, row)),
        MembershipOperand::Set(_) => Err(EngineError::BadExpression(
            "expected operand in first position of `in`, got set".to_owned(),
        )),
    }
}

fn resolve_membership_set(operand: &MembershipOperand) -> Result<&[Value], EngineError> {
    match operand {
        MembershipOperand::Set(set) => Ok(set),
        MembershipOperand::Operand(_) => Err(EngineError::BadExpression(
            "expected set in second position of `in`, got operand".to_owned(),
        )),
    }
}

// --- Value comparison ---

fn values_equal(
    operator: &str,
    left: &Value,
    right: &Value,
    context: ExpressionContext<'_>,
    field: Option<&str>,
) -> Result<bool, EngineError> {
    scalar_values_equal(left, right)
        .map_err(|_| incomparable_error(operator, left, right, context, field))
}

fn compare_values(
    operator: &str,
    left: &Value,
    right: &Value,
    context: ExpressionContext<'_>,
    field: Option<&str>,
) -> Result<std::cmp::Ordering, EngineError> {
    compare_scalar_values(left, right)
        .map_err(|_| incomparable_error(operator, left, right, context, field))
}

fn incomparable_error(
    operator: &str,
    left: &Value,
    right: &Value,
    context: ExpressionContext<'_>,
    field: Option<&str>,
) -> EngineError {
    EngineError::IncomparableOperands(Box::new(IncomparableOperandsDetail {
        operator: operator.to_owned(),
        left_type: value_type(left).to_owned(),
        right_type: value_type(right).to_owned(),
        binding: context.binding.to_owned(),
        key: extract_key(context.row, context.key_fields),
        field: field.map(ToOwned::to_owned),
    }))
}

fn binary_field(operands: &[PredicateOperand; 2]) -> Option<String> {
    operands.iter().find_map(|operand| match operand {
        PredicateOperand::Column(column) => Some(column.column.clone()),
        PredicateOperand::Literal(_) => None,
    })
}

fn membership_field(operand: &MembershipOperand) -> Option<String> {
    match operand {
        MembershipOperand::Operand(PredicateOperand::Column(column)) => Some(column.column.clone()),
        MembershipOperand::Set(_) | MembershipOperand::Operand(PredicateOperand::Literal(_)) => {
            None
        }
    }
}

// --- Helpers ---

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

/// For simple predicates, extract the primary column reference and its value
/// for failure localization. A negated simple predicate keeps that same
/// localization; more complex boolean compositions return (None, None).
fn extract_predicate_localization(
    expr: &PredicateExpression,
    row: &BTreeMap<String, Value>,
) -> (Option<String>, Option<Value>) {
    match expr {
        PredicateExpression::Eq { eq }
        | PredicateExpression::Ne { ne: eq }
        | PredicateExpression::Gt { gt: eq }
        | PredicateExpression::Gte { gte: eq }
        | PredicateExpression::Lt { lt: eq }
        | PredicateExpression::Lte { lte: eq } => extract_binary_localization(eq, row),
        PredicateExpression::In { r#in } => match &r#in[0] {
            MembershipOperand::Operand(PredicateOperand::Column(col_ref)) => {
                let value = row.get(&col_ref.column).cloned().unwrap_or(Value::Null);
                (Some(col_ref.column.clone()), Some(value))
            }
            _ => (None, None),
        },
        PredicateExpression::IsNull { is_null: col_ref }
        | PredicateExpression::IsBlank { is_blank: col_ref } => {
            let value = row.get(&col_ref.column).cloned().unwrap_or(Value::Null);
            (Some(col_ref.column.clone()), Some(value))
        }
        PredicateExpression::Column(col_ref) => {
            let value = row.get(&col_ref.column).cloned().unwrap_or(Value::Null);
            (Some(col_ref.column.clone()), Some(value))
        }
        PredicateExpression::Not { not } => extract_predicate_localization(not, row),
        PredicateExpression::And { .. } | PredicateExpression::Or { .. } => (None, None),
    }
}

fn extract_binary_localization(
    operands: &[PredicateOperand; 2],
    row: &BTreeMap<String, Value>,
) -> (Option<String>, Option<Value>) {
    match &operands[0] {
        PredicateOperand::Column(col_ref) => {
            let value = row.get(&col_ref.column).cloned().unwrap_or(Value::Null);
            (Some(col_ref.column.clone()), Some(value))
        }
        _ => match &operands[1] {
            PredicateOperand::Column(col_ref) => {
                let value = row.get(&col_ref.column).cloned().unwrap_or(Value::Null);
                (Some(col_ref.column.clone()), Some(value))
            }
            _ => (None, None),
        },
    }
}

/// Magnitude below which every integer has its own `f64`. From here up, a fraction-form number
/// may be a rounded neighbour of an integer, so the two are not taken for one value.
const EXACT_INTEGER_LIMIT: f64 = 9_007_199_254_740_992.0;

/// The identity `unique` gives one value: equal strings, equal booleans, null, and equal
/// numbers as docs/plan.md defines them. The type tag keeps types apart.
fn unique_identity(value: &Value) -> String {
    match value {
        Value::Null => "\0NULL".to_owned(),
        Value::String(s) => format!("S:{s}"),
        Value::Number(n) => unique_number_identity(n),
        Value::Bool(b) => format!("B:{b}"),
        other => format!("O:{other}"),
    }
}

/// Integers and fraction-form numbers are each told apart exactly as received. A fraction-form
/// number that is a whole number below the limit, `-0.0` included, takes its integer's identity.
/// Every other one keeps its own spelling, which always has a point or an exponent.
fn unique_number_identity(number: &Number) -> String {
    match number.as_f64() {
        Some(float)
            if number.is_f64() && float.fract() == 0.0 && float.abs() < EXACT_INTEGER_LIMIT =>
        {
            // Exact: a whole number of this magnitude fits in i64.
            format!("N:{}", float as i64)
        }
        _ => format!("N:{number}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Value, json};
    use verify_core::constraint::{
        Check, ColumnReference, Comparison, MembershipOperand, Portability, PredicateExpression,
        PredicateOperand, Rule, Severity,
    };
    use verify_core::report::ResultStatus;

    use super::{evaluate_rule, is_blank};
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
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn relations_with(
        name: &str,
        key_fields: &[&str],
        rows: Vec<BTreeMap<String, Value>>,
    ) -> BTreeMap<String, Relation> {
        let mut rels = BTreeMap::new();
        rels.insert(
            name.to_owned(),
            Relation::new(key_fields.iter().map(|s| (*s).to_owned()).collect(), rows),
        );
        rels
    }

    // --- is_blank tests ---

    #[test]
    fn blank_detects_null() {
        assert!(is_blank(&Value::Null));
    }

    #[test]
    fn blank_detects_empty_string() {
        assert!(is_blank(&json!("")));
    }

    #[test]
    fn blank_detects_whitespace_only() {
        assert!(is_blank(&json!("   ")));
        assert!(is_blank(&json!("\t\n")));
    }

    #[test]
    fn blank_rejects_present_string() {
        assert!(!is_blank(&json!("hello")));
    }

    #[test]
    fn blank_rejects_numbers_and_booleans() {
        assert!(!is_blank(&json!(0)));
        assert!(!is_blank(&json!(false)));
    }

    // --- unique tests ---

    /// Whether `unique` takes two tuples for one: it fails both rows, or passes both.
    fn one_tuple_for_unique(left: &[Value], right: &[Value]) -> bool {
        let columns: Vec<String> = (0..left.len()).map(|index| format!("c{index}")).collect();
        let tuple_row = |tuple: &[Value]| -> BTreeMap<String, Value> {
            columns.iter().cloned().zip(tuple.iter().cloned()).collect()
        };
        let rule = make_rule(
            "UNIQUE",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: columns.clone(),
            },
        );
        let relations = relations_with("input", &[], vec![tuple_row(left), tuple_row(right)]);
        let result = evaluate_rule(&rule, &relations).expect("unique evaluates");
        let violations = result.violation_count;
        assert!(
            violations == 0 || violations == 2,
            "{left:?} and {right:?}: {violations} violations"
        );
        violations == 2
    }

    /// Every spelling in a group is one value, and no two groups share one. An integer and a
    /// fraction-form number are one value only below 2^53, where every integer has its own f64.
    fn unique_value_classes() -> Vec<Vec<Value>> {
        let two_pow_53 = 9_007_199_254_740_992_i64;
        vec![
            vec![json!(0), json!(0.0), json!(-0.0)],
            vec![json!(1), json!(1.0)],
            vec![json!(-1), json!(-1.0)],
            vec![json!(1.5)],
            vec![json!(-1.5)],
            vec![json!(5e-324)],
            vec![json!(-5e-324)],
            // The largest magnitudes at which the two forms still meet.
            vec![json!(two_pow_53 - 1), json!(9_007_199_254_740_991.0)],
            vec![json!(1 - two_pow_53), json!(-9_007_199_254_740_991.0)],
            // From 2^53 on, each form is its own value.
            vec![json!(two_pow_53)],
            vec![json!(9_007_199_254_740_992.0)],
            vec![json!(two_pow_53 + 1)],
            vec![json!(9_007_199_254_740_994.0)],
            vec![json!(-two_pow_53)],
            vec![json!(-9_007_199_254_740_992.0)],
            vec![json!(i64::MIN)],
            vec![json!(-9_223_372_036_854_775_808.0)],
            vec![json!(u64::MAX)],
            vec![json!(18_446_744_073_709_551_616.0)],
            vec![json!(1e300)],
            // Other types never equal a number, and nulls equal each other.
            vec![json!("0")],
            vec![json!("1")],
            vec![json!("1.0")],
            vec![json!("")],
            vec![json!(false)],
            vec![json!(true)],
            vec![json!("true")],
            vec![Value::Null],
            vec![json!("null")],
        ]
    }

    #[test]
    fn unique_takes_equal_numbers_for_one_value_and_nothing_else() {
        let classes = unique_value_classes();
        for (left_class, left_values) in classes.iter().enumerate() {
            for (right_class, right_values) in classes.iter().enumerate() {
                for left in left_values {
                    for right in right_values {
                        assert_eq!(
                            one_tuple_for_unique(
                                std::slice::from_ref(left),
                                std::slice::from_ref(right)
                            ),
                            left_class == right_class,
                            "{left} and {right}"
                        );
                    }
                }
            }
        }
    }

    /// Composite tuples are the same only when every position is the same value.
    #[test]
    fn unique_compares_composite_tuples_position_by_position() {
        for (left, right, same) in [
            ([json!(1), json!(-0.0)], [json!(1.0), json!(0)], true),
            ([json!(1), Value::Null], [json!(1.0), Value::Null], true),
            ([Value::Null, Value::Null], [Value::Null, Value::Null], true),
            ([json!("a"), json!(2.0)], [json!("a"), json!(2)], true),
            ([json!(1), json!(2)], [json!(2), json!(1)], false),
            ([json!(1), json!(2)], [json!(1.0), json!(2.5)], false),
            ([json!(1), Value::Null], [json!(1), json!(0)], false),
            ([json!(1), json!("2")], [json!(1), json!(2)], false),
            (
                [json!(1), json!(9_007_199_254_740_992_i64)],
                [json!(1), json!(9_007_199_254_740_992.0)],
                false,
            ),
        ] {
            assert_eq!(
                one_tuple_for_unique(&left, &right),
                same,
                "{left:?} and {right:?}"
            );
        }
    }

    #[test]
    fn unique_passes_when_no_duplicates() {
        let rule = make_rule(
            "UNIQUE_ID",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["id"],
            vec![
                row(&[("id", json!("A"))]),
                row(&[("id", json!("B"))]),
                row(&[("id", json!("C"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
        assert!(result.affected.is_empty());
    }

    #[test]
    fn unique_fails_on_duplicate_rows() {
        let rule = make_rule(
            "UNIQUE_ID",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["id"],
            vec![
                row(&[("id", json!("A"))]),
                row(&[("id", json!("B"))]),
                row(&[("id", json!("A"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 2);
        assert_eq!(result.affected[0].field.as_deref(), Some("id"));
        assert_eq!(result.affected[0].value, Some(json!("A")));
    }

    #[test]
    fn unique_reports_all_duplicates_including_first_occurrence() {
        let rule = make_rule(
            "UNIQUE_ID",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["id"],
            vec![
                row(&[("id", json!("A")), ("name", json!("first"))]),
                row(&[("id", json!("A")), ("name", json!("second"))]),
                row(&[("id", json!("A")), ("name", json!("third"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert_eq!(result.violation_count, 3);
    }

    #[test]
    fn unique_on_empty_relation_passes() {
        let rule = make_rule(
            "UNIQUE_ID",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        );
        let rels = relations_with("input", &["id"], vec![]);

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Pass));
    }

    #[test]
    fn unique_multi_column_detects_duplicates() {
        let rule = make_rule(
            "UNIQUE_COMPOSITE",
            Severity::Error,
            Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["first".to_owned(), "last".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                row(&[("first", json!("John")), ("last", json!("Doe"))]),
                row(&[("first", json!("Jane")), ("last", json!("Doe"))]),
                row(&[("first", json!("John")), ("last", json!("Doe"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 2);
        // Multi-column unique: field is None, value is array
        assert!(result.affected[0].field.is_none());
        assert_eq!(result.affected[0].value, Some(json!(["John", "Doe"])));
    }

    // --- not_null tests ---

    #[test]
    fn not_null_passes_when_all_present() {
        let rule = make_rule(
            "REQUIRED_FIELDS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["loan_id".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["loan_id"],
            vec![
                row(&[("loan_id", json!("LN-001"))]),
                row(&[("loan_id", json!("LN-002"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
    }

    #[test]
    fn not_null_fails_on_null() {
        let rule = make_rule(
            "REQUIRED_FIELDS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["loan_id".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["loan_id"],
            vec![
                row(&[("loan_id", json!("LN-001"))]),
                row(&[("loan_id", Value::Null)]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
        assert_eq!(result.affected[0].field.as_deref(), Some("loan_id"));
        assert_eq!(result.affected[0].value, Some(Value::Null));
    }

    #[test]
    fn not_null_fails_on_empty_string() {
        let rule = make_rule(
            "REQUIRED_FIELDS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["required_value".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["row_id"],
            vec![row(&[
                ("row_id", json!("ROW-001")),
                ("required_value", json!("")),
            ])],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].value, Some(json!("")));
    }

    #[test]
    fn not_null_fails_on_whitespace_only() {
        let rule = make_rule(
            "REQUIRED_FIELDS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["required_value".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["row_id"],
            vec![row(&[
                ("row_id", json!("ROW-002")),
                ("required_value", json!("   ")),
            ])],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].value, Some(json!("   ")));
    }

    #[test]
    fn not_null_fails_on_missing_column() {
        let rule = make_rule(
            "REQUIRED_FIELDS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["missing_col".to_owned()],
            },
        );
        let rels = relations_with("input", &[], vec![row(&[("other_col", json!("value"))])]);

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].value, Some(Value::Null));
    }

    #[test]
    fn not_null_batch_missingness_all_three_fail() {
        let rule = make_rule(
            "BATCH_MISSINGNESS",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["required_value".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["row_id"],
            vec![
                row(&[
                    ("row_id", json!("ROW-001")),
                    ("required_value", Value::Null),
                ]),
                row(&[("row_id", json!("ROW-002")), ("required_value", json!(""))]),
                row(&[
                    ("row_id", json!("ROW-003")),
                    ("required_value", json!("   ")),
                ]),
                row(&[
                    ("row_id", json!("ROW-004")),
                    ("required_value", json!("present")),
                ]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 3);
    }

    // --- predicate tests ---

    #[test]
    fn predicate_gt_passes_when_satisfied() {
        let rule = make_rule(
            "POSITIVE_BALANCE",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Gt {
                    gt: [
                        PredicateOperand::Column(ColumnReference {
                            binding: None,
                            column: "balance".to_owned(),
                        }),
                        PredicateOperand::Literal(json!(0)),
                    ],
                },
            },
        );
        let rels = relations_with(
            "input",
            &["loan_id"],
            vec![
                row(&[("loan_id", json!("LN-001")), ("balance", json!(100.0))]),
                row(&[("loan_id", json!("LN-002")), ("balance", json!(250.5))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Pass));
    }

    #[test]
    fn predicate_gt_fails_with_localization() {
        let rule = make_rule(
            "POSITIVE_BALANCE",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Gt {
                    gt: [
                        PredicateOperand::Column(ColumnReference {
                            binding: None,
                            column: "balance".to_owned(),
                        }),
                        PredicateOperand::Literal(json!(0)),
                    ],
                },
            },
        );
        let rels = relations_with(
            "input",
            &["loan_id"],
            vec![
                row(&[("loan_id", json!("LN-001")), ("balance", json!(100.0))]),
                row(&[("loan_id", json!("LN-002")), ("balance", json!(-500.0))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
        assert_eq!(result.affected[0].field.as_deref(), Some("balance"));
        assert_eq!(result.affected[0].value, Some(json!(-500.0)));
        assert_eq!(
            result.affected[0].key.as_ref().unwrap().get("loan_id"),
            Some(&json!("LN-002"))
        );
    }

    #[test]
    fn predicate_eq_matches_strings() {
        let rule = make_rule(
            "STATUS_CHECK",
            Severity::Warn,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Eq {
                    eq: [
                        PredicateOperand::Column(ColumnReference {
                            binding: None,
                            column: "status".to_owned(),
                        }),
                        PredicateOperand::Literal(json!("active")),
                    ],
                },
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                row(&[("status", json!("active"))]),
                row(&[("status", json!("inactive"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
    }

    #[test]
    fn predicate_in_checks_set_membership() {
        let rule = make_rule(
            "STATUS_ALLOWED",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::In {
                    r#in: [
                        MembershipOperand::Operand(PredicateOperand::Column(ColumnReference {
                            binding: None,
                            column: "match_status".to_owned(),
                        })),
                        MembershipOperand::Set(vec![
                            json!("MATCHED"),
                            json!("UNMATCHED_GOLD"),
                            json!("UNMATCHED_CANDIDATE"),
                        ]),
                    ],
                },
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                row(&[("match_status", json!("MATCHED"))]),
                row(&[("match_status", json!("INVALID"))]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
        assert_eq!(result.affected[0].field.as_deref(), Some("match_status"));
        assert_eq!(result.affected[0].value, Some(json!("INVALID")));
    }

    #[test]
    fn predicate_boolean_composition() {
        // or(gt(balance, 0), is_blank(waiver_reason))
        let rule = make_rule(
            "BALANCE_OR_WAIVER",
            Severity::Warn,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Or {
                    or: vec![
                        PredicateExpression::Gt {
                            gt: [
                                PredicateOperand::Column(ColumnReference {
                                    binding: None,
                                    column: "balance".to_owned(),
                                }),
                                PredicateOperand::Literal(json!(0)),
                            ],
                        },
                        PredicateExpression::IsBlank {
                            is_blank: ColumnReference {
                                binding: None,
                                column: "waiver_reason".to_owned(),
                            },
                        },
                    ],
                },
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                // passes: balance > 0
                row(&[("balance", json!(100.0)), ("waiver_reason", json!("none"))]),
                // passes: waiver_reason is blank
                row(&[("balance", json!(-50.0)), ("waiver_reason", json!(""))]),
                // fails: balance <= 0 AND waiver_reason is not blank
                row(&[
                    ("balance", json!(-50.0)),
                    ("waiver_reason", json!("denied")),
                ]),
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
        // Boolean composition → no single field localization
        assert!(result.affected[0].field.is_none());
    }

    #[test]
    fn predicate_not_inverts() {
        let rule = make_rule(
            "NOT_ZERO",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Not {
                    not: Box::new(PredicateExpression::Eq {
                        eq: [
                            PredicateOperand::Column(ColumnReference {
                                binding: None,
                                column: "value".to_owned(),
                            }),
                            PredicateOperand::Literal(json!(0)),
                        ],
                    }),
                },
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                row(&[("value", json!(1))]),
                row(&[("value", json!(0))]), // fails: value == 0 → not(true) → false
            ],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);
    }

    #[test]
    fn predicate_is_null_and_is_blank_checks() {
        let rule_null = make_rule(
            "NULL_CHECK",
            Severity::Warn,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::IsNull {
                    is_null: ColumnReference {
                        binding: None,
                        column: "opt".to_owned(),
                    },
                },
            },
        );
        let rels = relations_with(
            "input",
            &[],
            vec![
                row(&[("opt", Value::Null)]),  // is_null → true → pass
                row(&[("opt", json!("val"))]), // is_null → false → fail
                row(&[("opt", json!(""))]),    // is_null → false → fail
            ],
        );

        let result = evaluate_rule(&rule_null, &rels).unwrap();
        // is_null predicate: only null passes, "" and "val" fail
        assert_eq!(result.violation_count, 2);
    }

    #[test]
    fn nullity_rules_use_presence_for_temporal_localization_text() {
        let rows = vec![
            row(&[
                ("date", json!("2024-01-31")),
                ("timestamp", json!("2026-01-31 10:00:00")),
                ("time", json!("10:00:00")),
                ("interval", json!("1 day 02:00:00")),
            ]),
            row(&[
                ("date", Value::Null),
                ("timestamp", Value::Null),
                ("time", Value::Null),
                ("interval", Value::Null),
            ]),
        ];
        let relations = relations_with("input", &[], rows);

        let not_null = make_rule(
            "TEMPORAL_NOT_NULL",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["date".to_owned()],
            },
        );
        let not_is_null = make_rule(
            "TEMPORAL_NOT_IS_NULL",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Not {
                    not: Box::new(PredicateExpression::IsNull {
                        is_null: ColumnReference {
                            binding: None,
                            column: "timestamp".to_owned(),
                        },
                    }),
                },
            },
        );
        let not_is_blank = make_rule(
            "TEMPORAL_NOT_IS_BLANK",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: PredicateExpression::Not {
                    not: Box::new(PredicateExpression::IsBlank {
                        is_blank: ColumnReference {
                            binding: None,
                            column: "interval".to_owned(),
                        },
                    }),
                },
            },
        );

        for rule in [not_null, not_is_null, not_is_blank] {
            let result = evaluate_rule(&rule, &relations).expect("nullity evaluates");
            assert!(matches!(result.status, ResultStatus::Fail));
            assert_eq!(result.violation_count, 1);
            assert_eq!(result.affected[0].value, Some(Value::Null));
        }
    }

    #[test]
    fn predicate_type_mismatches_refuse_for_every_comparison_operator() {
        let cases = [
            ("eq", json!({ "eq": [{ "column": "value" }, "0"] })),
            ("ne", json!({ "ne": [{ "column": "value" }, "0"] })),
            ("gt", json!({ "gt": [{ "column": "value" }, "0"] })),
            ("gte", json!({ "gte": [{ "column": "value" }, "0"] })),
            ("lt", json!({ "lt": [{ "column": "value" }, "0"] })),
            ("lte", json!({ "lte": [{ "column": "value" }, "0"] })),
            ("in", json!({ "in": [{ "column": "value" }, ["0", "1"]] })),
            (
                "eq",
                json!({ "not": { "eq": [{ "column": "value" }, "0"] } }),
            ),
            (
                "eq",
                json!({
                    "or": [
                        { "eq": [1, 1] },
                        { "eq": [{ "column": "value" }, "0"] }
                    ]
                }),
            ),
        ];
        let rels = relations_with(
            "input",
            &["id"],
            vec![row(&[("id", json!("row-1")), ("value", json!(0))])],
        );

        for (expected_operator, expression) in cases {
            let expr: PredicateExpression =
                serde_json::from_value(expression).expect("test expression should parse");
            let rule = make_rule(
                "TYPE_STRICT",
                Severity::Error,
                Check::Predicate {
                    binding: "input".to_owned(),
                    expr,
                },
            );

            let error = evaluate_rule(&rule, &rels).expect_err("type mismatch must refuse");
            assert!(matches!(error, super::EngineError::IncomparableOperands(_)));
            let super::EngineError::IncomparableOperands(detail) = error else {
                return;
            };
            assert_eq!(detail.operator, expected_operator);
            assert_eq!(detail.left_type, "number");
            assert_eq!(detail.right_type, "string");
            assert_eq!(detail.binding, "input");
            assert_eq!(
                detail.key.and_then(|key| key.get("id").cloned()),
                Some(json!("row-1"))
            );
            assert_eq!(detail.field.as_deref(), Some("value"));
        }
    }

    #[test]
    fn predicate_null_comparison_semantics_are_explicit() {
        let rels = relations_with(
            "input",
            &["id"],
            vec![row(&[("id", json!("row-1")), ("value", Value::Null)])],
        );
        let cases = [
            (json!({ "eq": [{ "column": "value" }, null] }), true),
            (json!({ "eq": [{ "column": "value" }, 0] }), false),
            (json!({ "ne": [{ "column": "value" }, 0] }), true),
            (json!({ "in": [{ "column": "value" }, [null, 0]] }), true),
        ];

        for (expression, should_pass) in cases {
            let expr = serde_json::from_value(expression).expect("test expression should parse");
            let rule = make_rule(
                "NULL_SEMANTICS",
                Severity::Error,
                Check::Predicate {
                    binding: "input".to_owned(),
                    expr,
                },
            );
            let result = evaluate_rule(&rule, &rels).expect("equality with null is defined");
            assert_eq!(matches!(result.status, ResultStatus::Pass), should_pass);
        }

        let ordered_null = make_rule(
            "NULL_ORDERING",
            Severity::Error,
            Check::Predicate {
                binding: "input".to_owned(),
                expr: serde_json::from_value(json!({
                    "gte": [{ "column": "value" }, null]
                }))
                .expect("test expression should parse"),
            },
        );
        let error = evaluate_rule(&ordered_null, &rels).expect_err("null has no ordering");
        assert!(matches!(
            error,
            super::EngineError::IncomparableOperands(detail)
                if detail.left_type == "null" && detail.right_type == "null"
        ));
    }

    // --- error handling tests ---

    #[test]
    fn missing_binding_returns_error() {
        let rule = make_rule(
            "UNIQUE_ID",
            Severity::Error,
            Check::Unique {
                binding: "nonexistent".to_owned(),
                columns: vec!["id".to_owned()],
            },
        );
        let rels = BTreeMap::new();

        let err = evaluate_rule(&rule, &rels).unwrap_err();
        assert!(matches!(err, super::EngineError::MissingBinding(_)));
    }

    #[test]
    fn unsupported_op_returns_error() {
        let rule = make_rule(
            "ROW_COUNT",
            Severity::Error,
            Check::RowCount {
                binding: "input".to_owned(),
                compare: Comparison::default(),
            },
        );
        let rels = relations_with("input", &[], vec![]);

        let err = evaluate_rule(&rule, &rels).unwrap_err();
        assert!(matches!(err, super::EngineError::UnsupportedOp(_)));
    }

    #[test]
    fn binding_qualified_predicates_fail_closed_in_the_portable_engine() {
        let rule = make_rule(
            "CROSS_BINDING",
            Severity::Error,
            Check::Predicate {
                binding: "current".to_owned(),
                expr: PredicateExpression::Eq {
                    eq: [
                        PredicateOperand::Column(ColumnReference {
                            binding: None,
                            column: "value".to_owned(),
                        }),
                        PredicateOperand::Column(ColumnReference {
                            binding: Some("prior".to_owned()),
                            column: "value".to_owned(),
                        }),
                    ],
                },
            },
        );

        let error = evaluate_rule(&rule, &BTreeMap::new())
            .expect_err("portable execution must not reinterpret cross-binding references");
        assert_eq!(
            error,
            super::EngineError::UnsupportedOp("binding-qualified predicate".to_owned())
        );
    }

    #[test]
    fn key_fields_appear_in_affected_entries() {
        let rule = make_rule(
            "NOT_NULL_ID",
            Severity::Error,
            Check::NotNull {
                binding: "input".to_owned(),
                columns: vec!["balance".to_owned()],
            },
        );
        let rels = relations_with(
            "input",
            &["loan_id"],
            vec![row(&[
                ("loan_id", json!("LN-042")),
                ("balance", Value::Null),
            ])],
        );

        let result = evaluate_rule(&rule, &rels).unwrap();
        let entry = &result.affected[0];
        assert_eq!(
            entry.key.as_ref().unwrap().get("loan_id"),
            Some(&json!("LN-042"))
        );
    }
}
