use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use duckdb::{Connection, types::Value as DuckValue};
use serde_json::{Map, Value, json};
use verify_core::{
    CONSTRAINT_VERSION,
    constraint::{
        Check, ConstraintSet, MembershipOperand, Portability, PredicateExpression,
        PredicateOperand, Rule,
    },
    order::sort_affected_entries,
    refusal::{Refusal, RefusalCode},
    report::{AffectedEntry, ResultStatus, RuleResult},
    validation::{PredicateAnalysis, analyze_predicate, validate_constraint_predicates},
};
use verify_engine::scalar::{
    ScalarCategory, compare_values, is_blank, value_category, values_equal,
};

use crate::{
    BindingRegistry,
    bindings::quote_identifier,
    scalar::{
        DuckValueError, duckdb_type_category, duckdb_value_sort_key, duckdb_value_to_protocol,
    },
};

pub const BINDING_PREDICATE_OP: &str = "predicate";

#[derive(Debug, Clone, Default)]
pub struct BindingPredicateExecutor;

impl BindingPredicateExecutor {
    pub fn evaluate_rule(
        rule: &Rule,
        connection: &Connection,
        bindings: &BindingRegistry,
    ) -> Result<RuleResult, BindingPredicateError> {
        evaluate_rule(rule, connection, bindings)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindingPredicateError {
    BadConstraints {
        rule_id: String,
        detail: Value,
    },
    MissingBinding {
        rule_id: String,
        binding: String,
    },
    FieldNotFound {
        rule_id: String,
        binding: String,
        field: String,
    },
    KeyInvalid {
        rule_id: String,
        detail: Value,
    },
    KeyAmbiguous {
        rule_id: String,
        detail: Value,
    },
    KeyUnmatched {
        rule_id: String,
        detail: Value,
    },
    BadExpression {
        rule_id: String,
        detail: Value,
    },
    SqlError {
        rule_id: String,
        message: String,
    },
}

impl BindingPredicateError {
    pub const fn refusal_code(&self) -> RefusalCode {
        match self {
            Self::BadConstraints { .. } => RefusalCode::BadConstraints,
            Self::MissingBinding { .. } => RefusalCode::MissingBinding,
            Self::FieldNotFound { .. } => RefusalCode::FieldNotFound,
            Self::KeyInvalid { .. } => RefusalCode::KeyInvalid,
            Self::KeyAmbiguous { .. } => RefusalCode::KeyAmbiguous,
            Self::KeyUnmatched { .. } => RefusalCode::KeyUnmatched,
            Self::BadExpression { .. } => RefusalCode::BadExpr,
            Self::SqlError { .. } => RefusalCode::SqlError,
        }
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::BadConstraints { detail, .. }
            | Self::KeyInvalid { detail, .. }
            | Self::KeyAmbiguous { detail, .. }
            | Self::KeyUnmatched { detail, .. }
            | Self::BadExpression { detail, .. } => detail.clone(),
            Self::MissingBinding { rule_id, binding } => json!({
                "rule_id": rule_id,
                "binding": binding,
            }),
            Self::FieldNotFound {
                rule_id,
                binding,
                field,
            } => json!({
                "rule_id": rule_id,
                "binding": binding,
                "field": field,
            }),
            Self::SqlError { rule_id, message } => json!({
                "rule_id": rule_id,
                "message": message,
            }),
        }
    }

    pub fn to_refusal(&self) -> Refusal {
        Refusal::new(self.refusal_code(), self.to_string(), self.detail())
    }
}

impl std::fmt::Display for BindingPredicateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadConstraints { rule_id, .. } => {
                write!(
                    formatter,
                    "binding-qualified predicate rule {rule_id} is invalid"
                )
            }
            Self::MissingBinding { rule_id, binding } => write!(
                formatter,
                "binding-qualified predicate rule {rule_id} is missing binding {binding}"
            ),
            Self::FieldNotFound {
                rule_id,
                binding,
                field,
            } => write!(
                formatter,
                "binding-qualified predicate rule {rule_id} references missing field {binding}.{field}"
            ),
            Self::KeyInvalid { rule_id, .. } => {
                write!(
                    formatter,
                    "binding-qualified predicate rule {rule_id} has an invalid key"
                )
            }
            Self::KeyAmbiguous { rule_id, .. } => write!(
                formatter,
                "binding-qualified predicate rule {rule_id} has an ambiguous key"
            ),
            Self::KeyUnmatched { rule_id, .. } => write!(
                formatter,
                "binding-qualified predicate rule {rule_id} has an unmatched anchor key"
            ),
            Self::BadExpression { rule_id, .. } => write!(
                formatter,
                "binding-qualified predicate rule {rule_id} has incomparable operands"
            ),
            Self::SqlError { rule_id, message } => {
                write!(
                    formatter,
                    "DuckDB error in predicate rule {rule_id}: {message}"
                )
            }
        }
    }
}

impl std::error::Error for BindingPredicateError {}

pub fn evaluate_rule(
    rule: &Rule,
    connection: &Connection,
    bindings: &BindingRegistry,
) -> Result<RuleResult, BindingPredicateError> {
    let (anchor_binding, expression) = match &rule.check {
        Check::Predicate { binding, expr } => (binding.as_str(), expr),
        other => {
            return Err(bad_constraints(
                rule,
                json!({
                    "reason": "wrong_rule_kind",
                    "expected": BINDING_PREDICATE_OP,
                    "actual": other.op(),
                }),
            ));
        }
    };

    let analysis = analyze_predicate(anchor_binding, expression);
    for binding in &analysis.participating_bindings {
        if bindings.get(binding).is_none() {
            return Err(BindingPredicateError::MissingBinding {
                rule_id: rule.id.clone(),
                binding: binding.clone(),
            });
        }
    }

    validate_rule_contract(rule, bindings)?;
    if analysis.derived_portability != Portability::BatchOnly {
        return Err(bad_constraints(
            rule,
            json!({
                "reason": "not_binding_qualified",
                "derived_portability": analysis.derived_portability,
            }),
        ));
    }
    let fields = preflight_fields(rule, &analysis, bindings)?;
    let keys = validate_keys(rule, &analysis, &fields, connection, bindings)?;
    validate_counterparts(rule, &analysis, &keys)?;

    let first_anchor_key = keys
        .get(anchor_binding)
        .and_then(|binding| binding.rows.values().next())
        .map(|row| &row.key);
    preflight_expression(rule, expression, anchor_binding, &fields, first_anchor_key)?;

    let plan = build_join_query(rule, &analysis, &fields, &keys, bindings)?;
    let rows = load_joined_rows(rule, &plan, connection)?;
    let mut affected = Vec::new();
    for row in rows {
        let values = row.protocol_values(rule, anchor_binding)?;
        if !evaluate_expression(rule, expression, anchor_binding, &row.key, &values)? {
            let localized = localized_anchor_column(expression, anchor_binding);
            let (field, value) = localized.map_or((None, None), |column| {
                (Some(column.field.clone()), values.get(&column).cloned())
            });
            affected.push(AffectedEntry {
                binding: anchor_binding.to_owned(),
                key: Some(row.key),
                field,
                value,
            });
        }
    }

    sort_affected_entries(&mut affected);
    Ok(RuleResult {
        rule_id: rule.id.clone(),
        severity: rule.severity,
        status: if affected.is_empty() {
            ResultStatus::Pass
        } else {
            ResultStatus::Fail
        },
        violation_count: affected.len(),
        affected,
    })
}

fn validate_rule_contract(
    rule: &Rule,
    bindings: &BindingRegistry,
) -> Result<(), BindingPredicateError> {
    let constraints = ConstraintSet {
        version: CONSTRAINT_VERSION.to_owned(),
        constraint_set_id: "verify.duckdb.binding_predicate".to_owned(),
        bindings: bindings.declarations(),
        rules: vec![rule.clone()],
    };
    validate_constraint_predicates(&constraints)
        .map(|_| ())
        .map_err(|error| bad_constraints(rule, error.detail))
}

fn bad_constraints(rule: &Rule, detail: Value) -> BindingPredicateError {
    BindingPredicateError::BadConstraints {
        rule_id: rule.id.clone(),
        detail,
    }
}

fn missing_binding(rule: &Rule, binding: &str) -> BindingPredicateError {
    BindingPredicateError::MissingBinding {
        rule_id: rule.id.clone(),
        binding: binding.to_owned(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnId {
    binding: String,
    field: String,
}

impl ColumnId {
    fn new(binding: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            binding: binding.into(),
            field: field.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct FieldInfo {
    category: Result<ScalarCategory, DuckValueError>,
}

type FieldCatalog = BTreeMap<ColumnId, FieldInfo>;

fn preflight_fields(
    rule: &Rule,
    analysis: &PredicateAnalysis,
    bindings: &BindingRegistry,
) -> Result<FieldCatalog, BindingPredicateError> {
    let mut referenced = analysis
        .references
        .iter()
        .map(|reference| ColumnId::new(&reference.binding, &reference.column))
        .collect::<BTreeSet<_>>();
    for binding_name in &analysis.participating_bindings {
        let loaded = bindings
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        for field in &loaded.declared().key_fields {
            referenced.insert(ColumnId::new(binding_name, field));
        }
    }

    let mut catalog = BTreeMap::new();
    for column in referenced {
        let loaded = bindings
            .get(&column.binding)
            .ok_or_else(|| missing_binding(rule, &column.binding))?;
        let described =
            loaded
                .column(&column.field)
                .ok_or_else(|| BindingPredicateError::FieldNotFound {
                    rule_id: rule.id.clone(),
                    binding: column.binding.clone(),
                    field: column.field.clone(),
                })?;
        catalog.insert(
            column,
            FieldInfo {
                category: duckdb_type_category(&described.data_type),
            },
        );
    }
    Ok(catalog)
}

#[derive(Debug, Clone)]
struct KeyRow {
    key: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
struct BindingKeys {
    fields: Vec<String>,
    categories: Vec<ScalarCategory>,
    rows: BTreeMap<KeyTuple, KeyRow>,
}

type KeyRegistry = BTreeMap<String, BindingKeys>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct KeyTuple(Vec<KeyPart>);

#[derive(Debug, Clone)]
enum KeyPart {
    Boolean(bool),
    Number(f64),
    String(String),
}

impl PartialEq for KeyPart {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Number(left), Self::Number(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for KeyPart {}

impl PartialOrd for KeyPart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyPart {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::Number(left), Self::Number(right)) => {
                normalized_number(*left).total_cmp(&normalized_number(*right))
            }
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::Boolean(_), _) => Ordering::Less,
            (Self::Number(_), Self::Boolean(_)) => Ordering::Greater,
            (Self::Number(_), Self::String(_)) => Ordering::Less,
            (Self::String(_), _) => Ordering::Greater,
        }
    }
}

fn normalized_number(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

#[derive(Debug)]
struct KeyIssue {
    binding: String,
    sort_key: String,
    rank: u8,
    error: BindingPredicateError,
}

fn validate_keys(
    rule: &Rule,
    analysis: &PredicateAnalysis,
    fields: &FieldCatalog,
    connection: &Connection,
    bindings: &BindingRegistry,
) -> Result<KeyRegistry, BindingPredicateError> {
    let mut registry = BTreeMap::new();
    let mut issues = Vec::new();

    for binding_name in &analysis.participating_bindings {
        let loaded = bindings
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        let key_fields = loaded.declared().key_fields.clone();
        let mut categories = Vec::with_capacity(key_fields.len());
        for (position, field) in key_fields.iter().enumerate() {
            let column = ColumnId::new(binding_name, field);
            let info = fields
                .get(&column)
                .ok_or_else(|| BindingPredicateError::FieldNotFound {
                    rule_id: rule.id.clone(),
                    binding: binding_name.clone(),
                    field: field.clone(),
                })?;
            match &info.category {
                Ok(category) => categories.push(*category),
                Err(error) => {
                    issues.push(KeyIssue {
                        binding: binding_name.clone(),
                        sort_key: format!("metadata:{position:08}:{field}"),
                        rank: 0,
                        error: key_invalid(
                            rule,
                            json!({
                                "rule_id": rule.id,
                                "binding": binding_name,
                                "key_fields": key_fields,
                                "field": field,
                                "value_type": error.value_type,
                                "reason": error.kind.key_reason(),
                            }),
                        ),
                    });
                    categories.push(ScalarCategory::Null);
                }
            }
        }

        let (rows, mut row_issues) = load_binding_keys(
            rule,
            connection,
            binding_name,
            loaded.relation_name(),
            &key_fields,
        )?;
        issues.append(&mut row_issues);
        registry.insert(
            binding_name.clone(),
            BindingKeys {
                fields: key_fields,
                categories,
                rows,
            },
        );
    }

    let anchor = registry
        .get(&analysis.anchor_binding)
        .ok_or_else(|| missing_binding(rule, &analysis.anchor_binding))?;
    for binding_name in analysis
        .participating_bindings
        .iter()
        .filter(|binding| *binding != &analysis.anchor_binding)
    {
        let participant = registry
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        for (position, (left, right)) in anchor
            .categories
            .iter()
            .zip(&participant.categories)
            .enumerate()
        {
            if left == right || *left == ScalarCategory::Null || *right == ScalarCategory::Null {
                continue;
            }
            issues.push(KeyIssue {
                binding: binding_name.clone(),
                sort_key: format!("metadata:{position:08}:type_mismatch"),
                rank: 1,
                error: key_invalid(
                    rule,
                    json!({
                        "rule_id": rule.id,
                        "binding": binding_name,
                        "reason": "type_mismatch",
                        "position": position,
                        "left_binding": analysis.anchor_binding,
                        "left_field": anchor.fields[position],
                        "left_type": left.as_str(),
                        "right_binding": binding_name,
                        "right_field": participant.fields[position],
                        "right_type": right.as_str(),
                    }),
                ),
            });
        }
    }

    issues.sort_by(|left, right| {
        left.binding
            .cmp(&right.binding)
            .then_with(|| left.sort_key.cmp(&right.sort_key))
            .then_with(|| left.rank.cmp(&right.rank))
    });
    if let Some(issue) = issues.into_iter().next() {
        return Err(issue.error);
    }

    Ok(registry)
}

fn load_binding_keys(
    rule: &Rule,
    connection: &Connection,
    binding_name: &str,
    relation_name: &str,
    key_fields: &[String],
) -> Result<(BTreeMap<KeyTuple, KeyRow>, Vec<KeyIssue>), BindingPredicateError> {
    let projection = key_fields
        .iter()
        .map(|field| quote_identifier(field))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {projection} FROM {}",
        quote_identifier(relation_name)
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| sql_error(rule, error))?;
    let mut query = statement
        .query([])
        .map_err(|error| sql_error(rule, error))?;
    let mut valid = BTreeMap::<KeyTuple, (BTreeMap<String, Value>, usize)>::new();
    let mut issues = Vec::new();

    while let Some(row) = query.next().map_err(|error| sql_error(rule, error))? {
        let mut raw_sort = Vec::with_capacity(key_fields.len());
        let mut tuple = Vec::with_capacity(key_fields.len());
        let mut key = BTreeMap::new();
        let mut row_error = None;
        for (index, field) in key_fields.iter().enumerate() {
            let raw: DuckValue = row.get(index).map_err(|error| sql_error(rule, error))?;
            raw_sort.push(duckdb_value_sort_key(&raw));
            match duckdb_value_to_protocol(raw) {
                Ok(Value::Null) => {
                    row_error.get_or_insert_with(|| {
                        key_invalid(
                            rule,
                            json!({
                                "rule_id": rule.id,
                                "binding": binding_name,
                                "key_fields": key_fields,
                                "field": field,
                                "value_type": "null",
                                "reason": "null_component",
                            }),
                        )
                    });
                }
                Ok(value) => match key_part(&value) {
                    Some(part) => {
                        tuple.push(part);
                        key.insert(field.clone(), value);
                    }
                    None => {
                        row_error.get_or_insert_with(|| {
                            key_invalid(
                                rule,
                                json!({
                                    "rule_id": rule.id,
                                    "binding": binding_name,
                                    "key_fields": key_fields,
                                    "field": field,
                                    "value_type": value_category(&value).as_str(),
                                    "reason": "non_scalar_component",
                                }),
                            )
                        });
                    }
                },
                Err(error) => {
                    row_error.get_or_insert_with(|| {
                        key_invalid(
                            rule,
                            json!({
                                "rule_id": rule.id,
                                "binding": binding_name,
                                "key_fields": key_fields,
                                "field": field,
                                "value_type": error.value_type,
                                "reason": error.kind.key_reason(),
                            }),
                        )
                    });
                }
            }
        }

        let sort_key = raw_sort.join("\u{1f}");
        if let Some(error) = row_error {
            issues.push(KeyIssue {
                binding: binding_name.to_owned(),
                sort_key,
                rank: 0,
                error,
            });
            continue;
        }

        let tuple = KeyTuple(tuple);
        let occurrence = valid.entry(tuple).or_insert((key, 0));
        occurrence.1 += 1;
    }

    let mut rows = BTreeMap::new();
    for (tuple, (key, occurrences)) in valid {
        if occurrences > 1 {
            issues.push(KeyIssue {
                binding: binding_name.to_owned(),
                sort_key: key_sort_key(&key),
                rank: 1,
                error: BindingPredicateError::KeyAmbiguous {
                    rule_id: rule.id.clone(),
                    detail: json!({
                        "rule_id": rule.id,
                        "binding": binding_name,
                        "key": key,
                        "occurrences": occurrences,
                    }),
                },
            });
        } else {
            rows.insert(tuple, KeyRow { key });
        }
    }
    Ok((rows, issues))
}

fn key_part(value: &Value) -> Option<KeyPart> {
    match value {
        Value::Bool(value) => Some(KeyPart::Boolean(*value)),
        Value::Number(value) => value.as_f64().map(KeyPart::Number),
        Value::String(value) => Some(KeyPart::String(value.clone())),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn key_sort_key(key: &BTreeMap<String, Value>) -> String {
    key.values()
        .map(|value| match value {
            Value::Null => "00:null".to_owned(),
            Value::Bool(value) => format!("01:boolean:{value}"),
            Value::Number(value) => format!("02:number:{value}"),
            Value::String(value) => format!("03:string:{value}"),
            other => format!("99:{other}"),
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

fn key_invalid(rule: &Rule, detail: Value) -> BindingPredicateError {
    BindingPredicateError::KeyInvalid {
        rule_id: rule.id.clone(),
        detail,
    }
}

fn validate_counterparts(
    rule: &Rule,
    analysis: &PredicateAnalysis,
    keys: &KeyRegistry,
) -> Result<(), BindingPredicateError> {
    let anchor = keys
        .get(&analysis.anchor_binding)
        .ok_or_else(|| missing_binding(rule, &analysis.anchor_binding))?;
    for (tuple, row) in &anchor.rows {
        for binding_name in analysis
            .participating_bindings
            .iter()
            .filter(|binding| *binding != &analysis.anchor_binding)
        {
            let participant = keys
                .get(binding_name)
                .ok_or_else(|| missing_binding(rule, binding_name))?;
            if !participant.rows.contains_key(tuple) {
                return Err(BindingPredicateError::KeyUnmatched {
                    rule_id: rule.id.clone(),
                    detail: json!({
                        "rule_id": rule.id,
                        "binding": analysis.anchor_binding,
                        "key": row.key,
                        "missing_binding": binding_name,
                        "missing_key_fields": participant.fields,
                    }),
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct QueryPlan {
    // DuckDB lowers the AST's resolved column surface into one keyed joined
    // projection. Predicate truth remains in verify-engine::scalar so DuckDB
    // casts, null logic, and collation never decide a verdict.
    sql: String,
    anchor_key_fields: Vec<String>,
    projections: Vec<ColumnId>,
}

fn build_join_query(
    rule: &Rule,
    analysis: &PredicateAnalysis,
    fields: &FieldCatalog,
    keys: &KeyRegistry,
    bindings: &BindingRegistry,
) -> Result<QueryPlan, BindingPredicateError> {
    let anchor_alias = "__verify_b0";
    let anchor = bindings
        .get(&analysis.anchor_binding)
        .ok_or_else(|| missing_binding(rule, &analysis.anchor_binding))?;
    let anchor_keys = keys
        .get(&analysis.anchor_binding)
        .ok_or_else(|| missing_binding(rule, &analysis.anchor_binding))?;
    let mut aliases = BTreeMap::from([(analysis.anchor_binding.clone(), anchor_alias.to_owned())]);
    for (index, binding) in analysis
        .participating_bindings
        .iter()
        .filter(|binding| *binding != &analysis.anchor_binding)
        .enumerate()
    {
        aliases.insert(binding.clone(), format!("__verify_b{}", index + 1));
    }

    let mut seen_projections = BTreeSet::new();
    let projections = analysis
        .references
        .iter()
        .map(|reference| ColumnId::new(&reference.binding, &reference.column))
        .filter(|column| seen_projections.insert(column.clone()))
        .collect::<Vec<_>>();
    let mut select = anchor_keys
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            format!(
                "{}.{} AS {}",
                quote_identifier(anchor_alias),
                quote_identifier(field),
                quote_identifier(&format!("__verify_key_{index}"))
            )
        })
        .collect::<Vec<_>>();
    for (index, column) in projections.iter().enumerate() {
        let alias = aliases
            .get(&column.binding)
            .ok_or_else(|| missing_binding(rule, &column.binding))?;
        select.push(format!(
            "{}.{} AS {}",
            quote_identifier(alias),
            quote_identifier(&column.field),
            quote_identifier(&format!("__verify_col_{index}"))
        ));
    }

    let mut sql = format!(
        "SELECT {} FROM {} AS {}",
        select.join(", "),
        quote_identifier(anchor.relation_name()),
        quote_identifier(anchor_alias)
    );
    for binding_name in analysis
        .participating_bindings
        .iter()
        .filter(|binding| *binding != &analysis.anchor_binding)
    {
        let loaded = bindings
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        let participant = keys
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        let alias = aliases
            .get(binding_name)
            .ok_or_else(|| missing_binding(rule, binding_name))?;
        let conditions = anchor_keys
            .fields
            .iter()
            .zip(&participant.fields)
            .enumerate()
            .map(|(position, (left, right))| {
                let left = qualified_identifier(anchor_alias, left);
                let right = qualified_identifier(alias, right);
                match anchor_keys.categories[position] {
                    ScalarCategory::Number => {
                        format!("CAST({left} AS DOUBLE) = CAST({right} AS DOUBLE)")
                    }
                    ScalarCategory::String => {
                        format!("encode({left}) = encode({right})")
                    }
                    _ => format!("{left} = {right}"),
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        sql.push_str(&format!(
            " LEFT JOIN {} AS {} ON {conditions}",
            quote_identifier(loaded.relation_name()),
            quote_identifier(alias)
        ));
    }

    debug_assert!(
        projections.iter().all(|column| fields.contains_key(column)),
        "every projected reference was preflighted"
    );
    Ok(QueryPlan {
        sql,
        anchor_key_fields: anchor_keys.fields.clone(),
        projections,
    })
}

fn qualified_identifier(alias: &str, field: &str) -> String {
    format!("{}.{}", quote_identifier(alias), quote_identifier(field))
}

#[derive(Debug)]
struct JoinedRow {
    tuple: KeyTuple,
    key: BTreeMap<String, Value>,
    values: Vec<(ColumnId, Result<Value, DuckValueError>)>,
}

impl JoinedRow {
    fn protocol_values(
        &self,
        rule: &Rule,
        anchor_binding: &str,
    ) -> Result<BTreeMap<ColumnId, Value>, BindingPredicateError> {
        let mut values = BTreeMap::new();
        for (column, value) in &self.values {
            match value {
                Ok(value) => {
                    values.insert(column.clone(), value.clone());
                }
                Err(error) => {
                    return Err(BindingPredicateError::BadExpression {
                        rule_id: rule.id.clone(),
                        detail: json!({
                            "rule_id": rule.id,
                            "reason": "unrepresentable_operand",
                            "binding": anchor_binding,
                            "key": self.key,
                            "operand_binding": column.binding,
                            "field": column.field,
                            "value_type": error.value_type,
                        }),
                    });
                }
            }
        }
        Ok(values)
    }
}

fn load_joined_rows(
    rule: &Rule,
    plan: &QueryPlan,
    connection: &Connection,
) -> Result<Vec<JoinedRow>, BindingPredicateError> {
    let mut statement = connection
        .prepare(&plan.sql)
        .map_err(|error| sql_error(rule, error))?;
    let mut query = statement
        .query([])
        .map_err(|error| sql_error(rule, error))?;
    let mut rows = Vec::new();
    while let Some(row) = query.next().map_err(|error| sql_error(rule, error))? {
        let mut tuple = Vec::with_capacity(plan.anchor_key_fields.len());
        let mut key = BTreeMap::new();
        for (index, field) in plan.anchor_key_fields.iter().enumerate() {
            let raw: DuckValue = row.get(index).map_err(|error| sql_error(rule, error))?;
            let value = duckdb_value_to_protocol(raw).map_err(|error| {
                key_invalid(
                    rule,
                    json!({
                        "rule_id": rule.id,
                        "binding": "anchor",
                        "field": field,
                        "value_type": error.value_type,
                        "reason": error.kind.key_reason(),
                    }),
                )
            })?;
            let part = key_part(&value).ok_or_else(|| {
                key_invalid(
                    rule,
                    json!({
                        "rule_id": rule.id,
                        "binding": "anchor",
                        "field": field,
                        "value_type": value_category(&value).as_str(),
                        "reason": "non_scalar_component",
                    }),
                )
            })?;
            tuple.push(part);
            key.insert(field.clone(), value);
        }

        let mut values = Vec::with_capacity(plan.projections.len());
        for (offset, column) in plan.projections.iter().enumerate() {
            let raw: DuckValue = row
                .get(plan.anchor_key_fields.len() + offset)
                .map_err(|error| sql_error(rule, error))?;
            values.push((column.clone(), duckdb_value_to_protocol(raw)));
        }
        rows.push(JoinedRow {
            tuple: KeyTuple(tuple),
            key,
            values,
        });
    }
    rows.sort_by(|left, right| left.tuple.cmp(&right.tuple));
    Ok(rows)
}

#[derive(Debug, Clone)]
struct OperandDescriptor {
    category: ScalarCategory,
    column: Option<ColumnId>,
}

fn preflight_expression(
    rule: &Rule,
    expression: &PredicateExpression,
    anchor_binding: &str,
    fields: &FieldCatalog,
    key: Option<&BTreeMap<String, Value>>,
) -> Result<(), BindingPredicateError> {
    match expression {
        PredicateExpression::Column(column) => {
            column_descriptor(rule, column, anchor_binding, fields, key).map(|_| ())
        }
        PredicateExpression::Eq { eq } | PredicateExpression::Ne { ne: eq } => {
            let operator = if matches!(expression, PredicateExpression::Eq { .. }) {
                "eq"
            } else {
                "ne"
            };
            let left = operand_descriptor(rule, &eq[0], anchor_binding, fields, key)?;
            let right = operand_descriptor(rule, &eq[1], anchor_binding, fields, key)?;
            if equality_categories_are_compatible(left.category, right.category) {
                Ok(())
            } else {
                Err(incomparable_categories(
                    rule,
                    operator,
                    anchor_binding,
                    key,
                    &left,
                    &right,
                ))
            }
        }
        PredicateExpression::Gt { gt } => {
            preflight_ordering(rule, "gt", gt, anchor_binding, fields, key)
        }
        PredicateExpression::Gte { gte } => {
            preflight_ordering(rule, "gte", gte, anchor_binding, fields, key)
        }
        PredicateExpression::Lt { lt } => {
            preflight_ordering(rule, "lt", lt, anchor_binding, fields, key)
        }
        PredicateExpression::Lte { lte } => {
            preflight_ordering(rule, "lte", lte, anchor_binding, fields, key)
        }
        PredicateExpression::And { and } | PredicateExpression::Or { or: and } => {
            for expression in and {
                preflight_expression(rule, expression, anchor_binding, fields, key)?;
            }
            Ok(())
        }
        PredicateExpression::Not { not } => {
            preflight_expression(rule, not, anchor_binding, fields, key)
        }
        PredicateExpression::In { r#in } => {
            let left = membership_operand_descriptor(rule, &r#in[0], anchor_binding, fields, key)?;
            let MembershipOperand::Set(set) = &r#in[1] else {
                return Err(bad_expression_structure(
                    rule,
                    "expected set in second position of `in`",
                ));
            };
            for member in set {
                let right = OperandDescriptor {
                    category: value_category(member),
                    column: None,
                };
                if !equality_categories_are_compatible(left.category, right.category) {
                    return Err(incomparable_categories(
                        rule,
                        "in",
                        anchor_binding,
                        key,
                        &left,
                        &right,
                    ));
                }
            }
            Ok(())
        }
        PredicateExpression::IsNull { is_null: column }
        | PredicateExpression::IsBlank { is_blank: column } => {
            column_descriptor(rule, column, anchor_binding, fields, key).map(|_| ())
        }
    }
}

fn preflight_ordering(
    rule: &Rule,
    operator: &str,
    operands: &[PredicateOperand; 2],
    anchor_binding: &str,
    fields: &FieldCatalog,
    key: Option<&BTreeMap<String, Value>>,
) -> Result<(), BindingPredicateError> {
    let left = operand_descriptor(rule, &operands[0], anchor_binding, fields, key)?;
    let right = operand_descriptor(rule, &operands[1], anchor_binding, fields, key)?;
    if ordering_categories_are_compatible(left.category, right.category) {
        Ok(())
    } else {
        Err(incomparable_categories(
            rule,
            operator,
            anchor_binding,
            key,
            &left,
            &right,
        ))
    }
}

fn operand_descriptor(
    rule: &Rule,
    operand: &PredicateOperand,
    anchor_binding: &str,
    fields: &FieldCatalog,
    key: Option<&BTreeMap<String, Value>>,
) -> Result<OperandDescriptor, BindingPredicateError> {
    match operand {
        PredicateOperand::Column(column) => {
            column_descriptor(rule, column, anchor_binding, fields, key)
        }
        PredicateOperand::Literal(value) => Ok(OperandDescriptor {
            category: value_category(value),
            column: None,
        }),
    }
}

fn membership_operand_descriptor(
    rule: &Rule,
    operand: &MembershipOperand,
    anchor_binding: &str,
    fields: &FieldCatalog,
    key: Option<&BTreeMap<String, Value>>,
) -> Result<OperandDescriptor, BindingPredicateError> {
    match operand {
        MembershipOperand::Operand(operand) => {
            operand_descriptor(rule, operand, anchor_binding, fields, key)
        }
        MembershipOperand::Set(_) => Err(bad_expression_structure(
            rule,
            "expected operand in first position of `in`",
        )),
    }
}

fn column_descriptor(
    rule: &Rule,
    column: &verify_core::constraint::ColumnReference,
    anchor_binding: &str,
    fields: &FieldCatalog,
    key: Option<&BTreeMap<String, Value>>,
) -> Result<OperandDescriptor, BindingPredicateError> {
    let column = ColumnId::new(
        column.binding.as_deref().unwrap_or(anchor_binding),
        &column.column,
    );
    let info = fields
        .get(&column)
        .ok_or_else(|| BindingPredicateError::FieldNotFound {
            rule_id: rule.id.clone(),
            binding: column.binding.clone(),
            field: column.field.clone(),
        })?;
    match &info.category {
        Ok(category) => Ok(OperandDescriptor {
            category: *category,
            column: Some(column),
        }),
        Err(error) => Err(BindingPredicateError::BadExpression {
            rule_id: rule.id.clone(),
            detail: json!({
                "rule_id": rule.id,
                "reason": "unrepresentable_operand",
                "binding": anchor_binding,
                "key": key,
                "operand_binding": column.binding,
                "field": column.field,
                "value_type": error.value_type,
            }),
        }),
    }
}

fn equality_categories_are_compatible(left: ScalarCategory, right: ScalarCategory) -> bool {
    left.is_protocol_scalar()
        && right.is_protocol_scalar()
        && (left == right || left == ScalarCategory::Null || right == ScalarCategory::Null)
}

fn ordering_categories_are_compatible(left: ScalarCategory, right: ScalarCategory) -> bool {
    left == right
        && matches!(
            left,
            ScalarCategory::Boolean | ScalarCategory::Number | ScalarCategory::String
        )
}

fn incomparable_categories(
    rule: &Rule,
    operator: &str,
    anchor_binding: &str,
    key: Option<&BTreeMap<String, Value>>,
    left: &OperandDescriptor,
    right: &OperandDescriptor,
) -> BindingPredicateError {
    let mut detail = Map::from_iter([
        ("rule_id".to_owned(), Value::String(rule.id.clone())),
        ("operator".to_owned(), Value::String(operator.to_owned())),
        (
            "left_type".to_owned(),
            Value::String(left.category.as_str().to_owned()),
        ),
        (
            "right_type".to_owned(),
            Value::String(right.category.as_str().to_owned()),
        ),
        (
            "binding".to_owned(),
            Value::String(anchor_binding.to_owned()),
        ),
        ("key".to_owned(), key.map_or(Value::Null, |key| json!(key))),
    ]);
    if let Some(column) = &left.column {
        detail.insert(
            "left_binding".to_owned(),
            Value::String(column.binding.clone()),
        );
        detail.insert("left_field".to_owned(), Value::String(column.field.clone()));
    }
    if let Some(column) = &right.column {
        detail.insert(
            "right_binding".to_owned(),
            Value::String(column.binding.clone()),
        );
        detail.insert(
            "right_field".to_owned(),
            Value::String(column.field.clone()),
        );
    }
    BindingPredicateError::BadExpression {
        rule_id: rule.id.clone(),
        detail: Value::Object(detail),
    }
}

fn evaluate_expression(
    rule: &Rule,
    expression: &PredicateExpression,
    anchor_binding: &str,
    key: &BTreeMap<String, Value>,
    values: &BTreeMap<ColumnId, Value>,
) -> Result<bool, BindingPredicateError> {
    match expression {
        PredicateExpression::Column(column) => Ok(!is_blank(require_column(
            rule,
            column,
            anchor_binding,
            values,
        )?)),
        PredicateExpression::Eq { eq } => {
            evaluate_equality(rule, "eq", &eq[0], &eq[1], anchor_binding, key, values)
        }
        PredicateExpression::Ne { ne } => {
            evaluate_equality(rule, "ne", &ne[0], &ne[1], anchor_binding, key, values)
                .map(|equal| !equal)
        }
        PredicateExpression::Gt { gt } => {
            evaluate_ordering(rule, "gt", &gt[0], &gt[1], anchor_binding, key, values)
                .map(|ordering| ordering == Ordering::Greater)
        }
        PredicateExpression::Gte { gte } => {
            evaluate_ordering(rule, "gte", &gte[0], &gte[1], anchor_binding, key, values)
                .map(|ordering| matches!(ordering, Ordering::Greater | Ordering::Equal))
        }
        PredicateExpression::Lt { lt } => {
            evaluate_ordering(rule, "lt", &lt[0], &lt[1], anchor_binding, key, values)
                .map(|ordering| ordering == Ordering::Less)
        }
        PredicateExpression::Lte { lte } => {
            evaluate_ordering(rule, "lte", &lte[0], &lte[1], anchor_binding, key, values)
                .map(|ordering| matches!(ordering, Ordering::Less | Ordering::Equal))
        }
        PredicateExpression::And { and } => {
            let mut result = true;
            for expression in and {
                result &= evaluate_expression(rule, expression, anchor_binding, key, values)?;
            }
            Ok(result)
        }
        PredicateExpression::Or { or } => {
            let mut result = false;
            for expression in or {
                result |= evaluate_expression(rule, expression, anchor_binding, key, values)?;
            }
            Ok(result)
        }
        PredicateExpression::Not { not } => Ok(!evaluate_expression(
            rule,
            not,
            anchor_binding,
            key,
            values,
        )?),
        PredicateExpression::In { r#in } => {
            let MembershipOperand::Operand(operand) = &r#in[0] else {
                return Err(bad_expression_structure(
                    rule,
                    "expected operand in first position of `in`",
                ));
            };
            let MembershipOperand::Set(set) = &r#in[1] else {
                return Err(bad_expression_structure(
                    rule,
                    "expected set in second position of `in`",
                ));
            };
            let (left, left_column) = resolve_operand(rule, operand, anchor_binding, values)?;
            let mut matched = false;
            for member in set {
                match values_equal(left, member) {
                    Ok(equal) => matched |= equal,
                    Err(_) => {
                        return Err(incomparable_values(
                            rule,
                            "in",
                            anchor_binding,
                            key,
                            left,
                            member,
                            left_column.as_ref(),
                            None,
                        ));
                    }
                }
            }
            Ok(matched)
        }
        PredicateExpression::IsNull { is_null: column } => {
            Ok(require_column(rule, column, anchor_binding, values)?.is_null())
        }
        PredicateExpression::IsBlank { is_blank: column } => Ok(is_blank(require_column(
            rule,
            column,
            anchor_binding,
            values,
        )?)),
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_equality(
    rule: &Rule,
    operator: &str,
    left: &PredicateOperand,
    right: &PredicateOperand,
    anchor_binding: &str,
    key: &BTreeMap<String, Value>,
    values: &BTreeMap<ColumnId, Value>,
) -> Result<bool, BindingPredicateError> {
    let (left_value, left_column) = resolve_operand(rule, left, anchor_binding, values)?;
    let (right_value, right_column) = resolve_operand(rule, right, anchor_binding, values)?;
    values_equal(left_value, right_value).map_err(|_| {
        incomparable_values(
            rule,
            operator,
            anchor_binding,
            key,
            left_value,
            right_value,
            left_column.as_ref(),
            right_column.as_ref(),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_ordering(
    rule: &Rule,
    operator: &str,
    left: &PredicateOperand,
    right: &PredicateOperand,
    anchor_binding: &str,
    key: &BTreeMap<String, Value>,
    values: &BTreeMap<ColumnId, Value>,
) -> Result<Ordering, BindingPredicateError> {
    let (left_value, left_column) = resolve_operand(rule, left, anchor_binding, values)?;
    let (right_value, right_column) = resolve_operand(rule, right, anchor_binding, values)?;
    compare_values(left_value, right_value).map_err(|_| {
        incomparable_values(
            rule,
            operator,
            anchor_binding,
            key,
            left_value,
            right_value,
            left_column.as_ref(),
            right_column.as_ref(),
        )
    })
}

fn resolve_operand<'a>(
    rule: &Rule,
    operand: &'a PredicateOperand,
    anchor_binding: &str,
    values: &'a BTreeMap<ColumnId, Value>,
) -> Result<(&'a Value, Option<ColumnId>), BindingPredicateError> {
    match operand {
        PredicateOperand::Column(column) => {
            let id = resolved_column_id(column, anchor_binding);
            let value = values
                .get(&id)
                .ok_or_else(|| missing_projected_column(rule, &id))?;
            Ok((value, Some(id)))
        }
        PredicateOperand::Literal(value) => Ok((value, None)),
    }
}

fn require_column<'a>(
    rule: &Rule,
    column: &verify_core::constraint::ColumnReference,
    anchor_binding: &str,
    values: &'a BTreeMap<ColumnId, Value>,
) -> Result<&'a Value, BindingPredicateError> {
    let id = resolved_column_id(column, anchor_binding);
    values
        .get(&id)
        .ok_or_else(|| missing_projected_column(rule, &id))
}

fn resolved_column_id(
    column: &verify_core::constraint::ColumnReference,
    anchor_binding: &str,
) -> ColumnId {
    ColumnId::new(
        column.binding.as_deref().unwrap_or(anchor_binding),
        &column.column,
    )
}

fn missing_projected_column(rule: &Rule, column: &ColumnId) -> BindingPredicateError {
    BindingPredicateError::BadExpression {
        rule_id: rule.id.clone(),
        detail: json!({
            "rule_id": rule.id,
            "reason": "missing_projected_operand",
            "binding": column.binding,
            "field": column.field,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn incomparable_values(
    rule: &Rule,
    operator: &str,
    anchor_binding: &str,
    key: &BTreeMap<String, Value>,
    left: &Value,
    right: &Value,
    left_column: Option<&ColumnId>,
    right_column: Option<&ColumnId>,
) -> BindingPredicateError {
    let left = OperandDescriptor {
        category: value_category(left),
        column: left_column.cloned(),
    };
    let right = OperandDescriptor {
        category: value_category(right),
        column: right_column.cloned(),
    };
    incomparable_categories(rule, operator, anchor_binding, Some(key), &left, &right)
}

fn bad_expression_structure(rule: &Rule, message: &str) -> BindingPredicateError {
    BindingPredicateError::BadExpression {
        rule_id: rule.id.clone(),
        detail: json!({
            "rule_id": rule.id,
            "reason": "bad_expression_structure",
            "detail": message,
        }),
    }
}

fn localized_anchor_column(
    expression: &PredicateExpression,
    anchor_binding: &str,
) -> Option<ColumnId> {
    let mut columns = BTreeSet::new();
    match expression {
        PredicateExpression::Column(column) => {
            columns.insert(ColumnId::new(
                column.binding.as_deref().unwrap_or(anchor_binding),
                &column.column,
            ));
        }
        PredicateExpression::Eq { eq }
        | PredicateExpression::Ne { ne: eq }
        | PredicateExpression::Gt { gt: eq }
        | PredicateExpression::Gte { gte: eq }
        | PredicateExpression::Lt { lt: eq }
        | PredicateExpression::Lte { lte: eq } => {
            for operand in eq {
                if let PredicateOperand::Column(column) = operand {
                    columns.insert(ColumnId::new(
                        column.binding.as_deref().unwrap_or(anchor_binding),
                        &column.column,
                    ));
                }
            }
        }
        PredicateExpression::In { r#in } => {
            if let MembershipOperand::Operand(PredicateOperand::Column(column)) = &r#in[0] {
                columns.insert(ColumnId::new(
                    column.binding.as_deref().unwrap_or(anchor_binding),
                    &column.column,
                ));
            }
        }
        PredicateExpression::IsNull { is_null: column }
        | PredicateExpression::IsBlank { is_blank: column } => {
            columns.insert(ColumnId::new(
                column.binding.as_deref().unwrap_or(anchor_binding),
                &column.column,
            ));
        }
        PredicateExpression::Not { .. }
        | PredicateExpression::And { .. }
        | PredicateExpression::Or { .. } => return None,
    }

    if columns.len() != 1 {
        return None;
    }
    columns
        .into_iter()
        .next()
        .filter(|column| column.binding == anchor_binding)
}

fn sql_error(rule: &Rule, error: duckdb::Error) -> BindingPredicateError {
    BindingPredicateError::SqlError {
        rule_id: rule.id.clone(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use duckdb::Connection;
    use serde_json::{Value, json};
    use verify_core::{
        constraint::{
            Binding, BindingKind, Check, Portability, PredicateExpression, Rule, Severity,
        },
        refusal::RefusalCode,
        report::ResultStatus,
        validation::analyze_predicate,
    };

    use super::{
        BindingPredicateError, build_join_query, evaluate_rule, preflight_fields, validate_keys,
    };
    use crate::BindingRegistry;

    fn binding(name: &str, key_fields: &[&str]) -> Binding {
        Binding {
            name: name.to_owned(),
            kind: BindingKind::Relation,
            key_fields: key_fields.iter().map(|field| (*field).to_owned()).collect(),
        }
    }

    fn setup(
        sql: &str,
        declarations: Vec<Binding>,
    ) -> Result<(Connection, BindingRegistry), Box<dyn Error>> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(sql)?;
        let registry = BindingRegistry::from_relations_for_test(&connection, declarations)?;
        Ok((connection, registry))
    }

    fn predicate_rule(id: &str, expression: Value) -> Rule {
        Rule {
            id: id.to_owned(),
            severity: Severity::Error,
            portability: Portability::BatchOnly,
            check: Check::Predicate {
                binding: "current".to_owned(),
                expr: serde_json::from_value(expression).expect("test expression should parse"),
            },
        }
    }

    fn immutable_value_rule() -> Rule {
        predicate_rule(
            "VALUE_IMMUTABLE",
            json!({
                "eq": [
                    {"column": "value"},
                    {"binding": "prior", "column": "value"}
                ]
            }),
        )
    }

    #[test]
    fn composite_keys_align_positionally_and_anchor_rows_localize_deterministically()
    -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (loan_id VARCHAR, tranche_id INTEGER, value VARCHAR);
             INSERT INTO current VALUES ('L-2', 2, 'changed'), ('L-1', 1, 'same');
             CREATE TABLE prior (asset_number VARCHAR, class_code INTEGER, value VARCHAR);
             INSERT INTO prior VALUES
                ('L-9', 9, 'non-anchor-only'),
                ('L-1', 1, 'same'),
                ('L-2', 2, 'old');",
            vec![
                binding("current", &["loan_id", "tranche_id"]),
                binding("prior", &["asset_number", "class_code"]),
            ],
        )?;

        let result = evaluate_rule(&immutable_value_rule(), &connection, &registry)?;

        assert_eq!(result.status, ResultStatus::Fail);
        assert_eq!(result.violation_count, 1);
        assert_eq!(result.affected[0].binding, "current");
        assert_eq!(
            result.affected[0]
                .key
                .as_ref()
                .and_then(|key| key.get("loan_id")),
            Some(&json!("L-2"))
        );
        assert!(result.affected[0].field.is_none());
        assert!(result.affected[0].value.is_none());
        Ok(())
    }

    #[test]
    fn complete_predicate_grammar_uses_each_non_anchor_join_once() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (
                id INTEGER, status VARCHAR, amount INTEGER, enabled BOOLEAN
             );
             INSERT INTO current VALUES (1, 'open', 10, true);
             CREATE TABLE prior (
                prior_id INTEGER, status VARCHAR, amount INTEGER, present VARCHAR
             );
             INSERT INTO prior VALUES (1, 'open', 10, 'yes');
             CREATE TABLE auxiliary (
                auxiliary_id INTEGER, optional_value VARCHAR, blank_value VARCHAR
             );
             INSERT INTO auxiliary VALUES (1, NULL, '   ');",
            vec![
                binding("current", &["id"]),
                binding("prior", &["prior_id"]),
                binding("auxiliary", &["auxiliary_id"]),
            ],
        )?;
        let rule = predicate_rule(
            "FULL_GRAMMAR",
            json!({
                "and": [
                    {"eq": [{"column": "status"}, {"binding": "prior", "column": "status"}]},
                    {"ne": [{"column": "status"}, "closed"]},
                    {"gt": [{"column": "amount"}, 0]},
                    {"gte": [{"binding": "prior", "column": "amount"}, {"column": "amount"}]},
                    {"lt": [{"column": "amount"}, 100]},
                    {"lte": [{"column": "amount"}, {"binding": "prior", "column": "amount"}]},
                    {"or": [
                        {"is_null": {"binding": "auxiliary", "column": "optional_value"}},
                        {"eq": [1, 0]}
                    ]},
                    {"not": {"eq": [{"column": "enabled"}, false]}},
                    {"in": [{"binding": "prior", "column": "status"}, ["open", null]]},
                    {"column": "enabled"},
                    {"is_blank": {"binding": "auxiliary", "column": "blank_value"}}
                ]
            }),
        );

        let result = evaluate_rule(&rule, &connection, &registry)?;
        assert_eq!(result.status, ResultStatus::Pass);

        let Check::Predicate { binding, expr } = &rule.check else {
            return Err("test rule should be a predicate".into());
        };
        let analysis = analyze_predicate(binding, expr);
        let fields = preflight_fields(&rule, &analysis, &registry)?;
        let keys = validate_keys(&rule, &analysis, &fields, &connection, &registry)?;
        let plan = build_join_query(&rule, &analysis, &fields, &keys, &registry)?;
        assert_eq!(plan.sql.matches("LEFT JOIN \"prior\"").count(), 1);
        assert_eq!(plan.sql.matches("LEFT JOIN \"auxiliary\"").count(), 1);
        assert!(!plan.sql.contains("CROSS JOIN"));
        Ok(())
    }

    #[test]
    fn every_predicate_operator_can_produce_a_cross_binding_failure() -> Result<(), Box<dyn Error>>
    {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, number_value INTEGER);
             INSERT INTO current VALUES (1, 10);
             CREATE TABLE prior (
                prior_id INTEGER,
                low_value INTEGER,
                equal_value INTEGER,
                high_value INTEGER,
                text_value VARCHAR,
                null_value VARCHAR
             );
             INSERT INTO prior VALUES (1, 5, 10, 20, 'closed', NULL);",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;
        let expressions = [
            json!({"eq": [{"column": "number_value"}, {"binding": "prior", "column": "low_value"}]}),
            json!({"ne": [{"column": "number_value"}, {"binding": "prior", "column": "equal_value"}]}),
            json!({"gt": [{"column": "number_value"}, {"binding": "prior", "column": "high_value"}]}),
            json!({"gte": [{"column": "number_value"}, {"binding": "prior", "column": "high_value"}]}),
            json!({"lt": [{"column": "number_value"}, {"binding": "prior", "column": "low_value"}]}),
            json!({"lte": [{"column": "number_value"}, {"binding": "prior", "column": "low_value"}]}),
            json!({"in": [{"binding": "prior", "column": "text_value"}, ["open"]]}),
            json!({"is_null": {"binding": "prior", "column": "text_value"}}),
            json!({"is_blank": {"binding": "prior", "column": "text_value"}}),
            json!({"column": "null_value", "binding": "prior"}),
            json!({"not": {"eq": [{"column": "number_value"}, {"binding": "prior", "column": "equal_value"}]}}),
            json!({"and": [{"eq": [1, 1]}, {"eq": [{"column": "number_value"}, {"binding": "prior", "column": "low_value"}]}]}),
            json!({"or": [{"eq": [1, 0]}, {"eq": [{"column": "number_value"}, {"binding": "prior", "column": "low_value"}]}]}),
        ];

        for (index, expression) in expressions.into_iter().enumerate() {
            let rule = predicate_rule(&format!("OPERATOR_{index}"), expression);
            let result = evaluate_rule(&rule, &connection, &registry)?;
            assert_eq!(result.status, ResultStatus::Fail, "operator case {index}");
            assert_eq!(result.violation_count, 1, "operator case {index}");
        }
        Ok(())
    }

    #[test]
    fn field_preflight_wins_over_key_and_expression_defects() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (1, 10);
             CREATE TABLE prior (prior_id INTEGER, value VARCHAR);
             INSERT INTO prior VALUES (1, '10'), (1, '10');",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;
        let rule = predicate_rule(
            "MISSING_FIELD_FIRST",
            json!({
                "eq": [
                    {"column": "missing"},
                    {"binding": "prior", "column": "value"}
                ]
            }),
        );

        let error = evaluate_rule(&rule, &connection, &registry)
            .expect_err("missing field should refuse before duplicate keys");
        assert_eq!(error.refusal_code(), RefusalCode::FieldNotFound);
        assert_eq!(error.detail()["binding"], "current");
        assert_eq!(error.detail()["field"], "missing");
        Ok(())
    }

    #[test]
    fn protocol_validation_rejects_missing_and_misaligned_key_metadata()
    -> Result<(), Box<dyn Error>> {
        let cases = [
            (
                vec![binding("current", &["id"]), binding("prior", &[])],
                "missing_key_fields",
            ),
            (
                vec![
                    binding("current", &["id", "part"]),
                    binding("prior", &["prior_id"]),
                ],
                "key_arity_mismatch",
            ),
        ];

        for (declarations, reason) in cases {
            let (connection, registry) = setup(
                "CREATE TABLE current (id INTEGER, part INTEGER, value INTEGER);
                 INSERT INTO current VALUES (1, 1, 10);
                 CREATE TABLE prior (prior_id INTEGER, value INTEGER);
                 INSERT INTO prior VALUES (1, 10);",
                declarations,
            )?;
            let error = evaluate_rule(&immutable_value_rule(), &connection, &registry)
                .expect_err("invalid key metadata should refuse before loading key values");
            assert_eq!(error.refusal_code(), RefusalCode::BadConstraints);
            assert_eq!(error.detail()["reason"], reason);
        }
        Ok(())
    }

    #[test]
    fn invalid_key_reasons_cover_null_non_scalar_and_type_mismatch() -> Result<(), Box<dyn Error>> {
        let cases = [
            (
                "CREATE TABLE current (id INTEGER, value INTEGER);
                 INSERT INTO current VALUES (1, 10);
                 CREATE TABLE prior (prior_id INTEGER, value INTEGER);
                 INSERT INTO prior VALUES (NULL, 10);",
                "null_component",
            ),
            (
                "CREATE TABLE current (id INTEGER, value INTEGER);
                 INSERT INTO current VALUES (1, 10);
                 CREATE TABLE prior (prior_id INTEGER[], value INTEGER);
                 INSERT INTO prior VALUES ([1], 10);",
                "non_scalar_component",
            ),
            (
                "CREATE TABLE current (id DATE, value INTEGER);
                 INSERT INTO current VALUES (DATE '2026-01-01', 10);
                 CREATE TABLE prior (prior_id DATE, value INTEGER);
                 INSERT INTO prior VALUES (DATE '2026-01-01', 10);",
                "unrepresentable_component",
            ),
            (
                "CREATE TABLE current (id INTEGER, value INTEGER);
                 INSERT INTO current VALUES (1, 10);
                 CREATE TABLE prior (prior_id VARCHAR, value INTEGER);
                 INSERT INTO prior VALUES ('1', 10);",
                "type_mismatch",
            ),
        ];

        for (sql, reason) in cases {
            let (connection, registry) = setup(
                sql,
                vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
            )?;
            let error = evaluate_rule(&immutable_value_rule(), &connection, &registry)
                .expect_err("invalid key should refuse");
            assert_eq!(error.refusal_code(), RefusalCode::KeyInvalid);
            assert_eq!(error.detail()["reason"], reason);
        }
        Ok(())
    }

    #[test]
    fn duplicate_non_anchor_only_keys_still_refuse() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (1, 10);
             CREATE TABLE prior (prior_id INTEGER, value INTEGER);
             INSERT INTO prior VALUES (1, 10), (9, 90), (9, 91);",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;

        let error = evaluate_rule(&immutable_value_rule(), &connection, &registry)
            .expect_err("duplicate non-anchor key should refuse");
        assert_eq!(error.refusal_code(), RefusalCode::KeyAmbiguous);
        assert_eq!(error.detail()["binding"], "prior");
        assert_eq!(error.detail()["key"]["prior_id"], 9);
        assert_eq!(error.detail()["occurrences"], 2);
        Ok(())
    }

    #[test]
    fn missing_counterpart_refuses_but_non_anchor_only_rows_do_not() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (2, 20), (1, 10);
             CREATE TABLE prior (prior_id INTEGER, value INTEGER);
             INSERT INTO prior VALUES (9, 90);",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;

        let error = evaluate_rule(&immutable_value_rule(), &connection, &registry)
            .expect_err("first canonical anchor key should be unmatched");
        assert_eq!(error.refusal_code(), RefusalCode::KeyUnmatched);
        assert_eq!(error.detail()["key"]["id"], 1);
        assert_eq!(error.detail()["missing_binding"], "prior");
        Ok(())
    }

    #[test]
    fn incomparable_operands_name_both_columns_and_never_become_a_verdict()
    -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (1, 10);
             CREATE TABLE prior (prior_id INTEGER, value VARCHAR);
             INSERT INTO prior VALUES (1, '10');",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;
        let rule = predicate_rule(
            "FULL_BRANCH_COMPARABILITY",
            json!({
                "or": [
                    {"eq": [1, 1]},
                    {"eq": [
                        {"column": "value"},
                        {"binding": "prior", "column": "value"}
                    ]}
                ]
            }),
        );

        let error = evaluate_rule(&rule, &connection, &registry)
            .expect_err("number/string comparison should refuse");
        assert_eq!(error.refusal_code(), RefusalCode::BadExpr);
        assert_eq!(error.detail()["operator"], "eq");
        assert_eq!(error.detail()["left_type"], "number");
        assert_eq!(error.detail()["right_type"], "string");
        assert_eq!(error.detail()["left_binding"], "current");
        assert_eq!(error.detail()["right_binding"], "prior");
        assert_eq!(error.detail()["key"]["id"], 1);
        Ok(())
    }

    #[test]
    fn null_equality_and_ordering_match_portable_scalar_semantics() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (1, NULL), (2, NULL);
             CREATE TABLE prior (prior_id INTEGER, value INTEGER);
             INSERT INTO prior VALUES (1, NULL), (2, 1);",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;

        let equality = evaluate_rule(&immutable_value_rule(), &connection, &registry)?;
        assert_eq!(equality.status, ResultStatus::Fail);
        assert_eq!(equality.violation_count, 1);
        assert_eq!(
            equality.affected[0].key.as_ref().map(|key| &key["id"]),
            Some(&json!(2))
        );

        let ordering = predicate_rule(
            "ORDERED_VALUE",
            json!({
                "gte": [
                    {"column": "value"},
                    {"binding": "prior", "column": "value"}
                ]
            }),
        );
        let error = evaluate_rule(&ordering, &connection, &registry)
            .expect_err("null values have no protocol ordering");
        assert_eq!(error.refusal_code(), RefusalCode::BadExpr);
        assert_eq!(error.detail()["left_type"], "null");
        assert_eq!(error.detail()["right_type"], "null");
        assert_eq!(error.detail()["key"]["id"], 1);
        Ok(())
    }

    #[test]
    fn row_order_permutations_preserve_results_and_first_refusal() -> Result<(), Box<dyn Error>> {
        let declarations = || vec![binding("current", &["id"]), binding("prior", &["prior_id"])];
        let (first_connection, first_registry) = setup(
            "CREATE TABLE current (id INTEGER, value VARCHAR);
             INSERT INTO current VALUES (2, 'changed'), (1, 'same');
             CREATE TABLE prior (prior_id INTEGER, value VARCHAR);
             INSERT INTO prior VALUES (2, 'old'), (1, 'same'), (9, 'extra');",
            declarations(),
        )?;
        let (second_connection, second_registry) = setup(
            "CREATE TABLE current (id INTEGER, value VARCHAR);
             INSERT INTO current VALUES (1, 'same'), (2, 'changed');
             CREATE TABLE prior (prior_id INTEGER, value VARCHAR);
             INSERT INTO prior VALUES (9, 'extra'), (1, 'same'), (2, 'old');",
            declarations(),
        )?;

        let first = evaluate_rule(&immutable_value_rule(), &first_connection, &first_registry)?;
        let second = evaluate_rule(
            &immutable_value_rule(),
            &second_connection,
            &second_registry,
        )?;
        assert_eq!(first, second);

        let (first_connection, first_registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (2, 20), (1, 10);
             CREATE TABLE prior (prior_id INTEGER, value INTEGER);",
            declarations(),
        )?;
        let (second_connection, second_registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);
             INSERT INTO current VALUES (1, 10), (2, 20);
             CREATE TABLE prior (prior_id INTEGER, value INTEGER);",
            declarations(),
        )?;
        let first = evaluate_rule(&immutable_value_rule(), &first_connection, &first_registry)
            .expect_err("counterparts are missing");
        let second = evaluate_rule(
            &immutable_value_rule(),
            &second_connection,
            &second_registry,
        )
        .expect_err("counterparts are missing");
        assert_eq!(first.refusal_code(), RefusalCode::KeyUnmatched);
        assert_eq!(first.detail(), second.detail());
        assert_eq!(first.detail()["key"]["id"], 1);
        Ok(())
    }

    #[test]
    fn set_oriented_executor_handles_ten_thousand_keyed_rows() -> Result<(), Box<dyn Error>> {
        let (connection, registry) = setup(
            "CREATE TABLE current AS
                SELECT i AS id, i AS value FROM range(10000) AS rows(i);
             CREATE TABLE prior AS
                SELECT i AS prior_id, i AS value
                FROM range(10000) AS rows(i)
                ORDER BY i DESC;",
            vec![binding("current", &["id"]), binding("prior", &["prior_id"])],
        )?;

        let result = evaluate_rule(&immutable_value_rule(), &connection, &registry)?;
        assert_eq!(result.status, ResultStatus::Pass);
        assert_eq!(result.violation_count, 0);
        Ok(())
    }

    #[test]
    fn typed_errors_map_to_protocol_refusals() {
        let error = BindingPredicateError::KeyUnmatched {
            rule_id: "RULE".to_owned(),
            detail: json!({"rule_id": "RULE"}),
        };
        let refusal = error.to_refusal();
        assert_eq!(refusal.code, RefusalCode::KeyUnmatched);
        assert_eq!(
            refusal.next_step,
            "Supply the counterpart row or correct key alignment."
        );
    }

    #[test]
    fn portable_or_non_predicate_rules_fail_closed_on_this_executor() -> Result<(), Box<dyn Error>>
    {
        let (connection, registry) = setup(
            "CREATE TABLE current (id INTEGER, value INTEGER);",
            vec![binding("current", &["id"])],
        )?;
        let portable = Rule {
            id: "PORTABLE".to_owned(),
            severity: Severity::Error,
            portability: Portability::Portable,
            check: Check::Predicate {
                binding: "current".to_owned(),
                expr: serde_json::from_value::<PredicateExpression>(json!({
                    "eq": [{"column": "value"}, 1]
                }))?,
            },
        };
        let non_predicate = Rule {
            id: "NOT_PREDICATE".to_owned(),
            severity: Severity::Error,
            portability: Portability::BatchOnly,
            check: Check::QueryZeroRows {
                bindings: vec!["current".to_owned()],
                query: "SELECT 1".to_owned(),
            },
        };

        assert_eq!(
            evaluate_rule(&portable, &connection, &registry)
                .expect_err("portable rule must fail closed")
                .refusal_code(),
            RefusalCode::BadConstraints
        );
        assert_eq!(
            evaluate_rule(&non_predicate, &connection, &registry)
                .expect_err("non-predicate rule must fail closed")
                .refusal_code(),
            RefusalCode::BadConstraints
        );
        Ok(())
    }
}
