use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::Value;

use crate::CONSTRAINT_VERSION;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConstraintSet {
    pub version: String,
    pub constraint_set_id: String,
    pub bindings: Vec<Binding>,
    pub rules: Vec<Rule>,
}

impl ConstraintSet {
    pub fn new(constraint_set_id: impl Into<String>) -> Self {
        Self {
            version: CONSTRAINT_VERSION.to_owned(),
            constraint_set_id: constraint_set_id.into(),
            bindings: Vec::new(),
            rules: Vec::new(),
        }
    }
}

impl Default for ConstraintSet {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub name: String,
    pub kind: BindingKind,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_key_fields"
    )]
    pub key_fields: Vec<String>,
}

fn deserialize_key_fields<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let key_fields = Vec::<String>::deserialize(deserializer)?;
    if key_fields.is_empty() {
        return Err(D::Error::custom(
            "empty_key_fields: key_fields must contain at least one field when present",
        ));
    }
    Ok(key_fields)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BindingKind {
    #[default]
    Relation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub id: String,
    pub severity: Severity,
    pub portability: Portability,
    pub check: Check,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Portability {
    Portable,
    BatchOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Check {
    Unique {
        binding: String,
        columns: Vec<String>,
    },
    NotNull {
        binding: String,
        columns: Vec<String>,
    },
    Predicate {
        binding: String,
        expr: PredicateExpression,
    },
    RowCount {
        binding: String,
        compare: Comparison,
    },
    AggregateCompare {
        binding: String,
        aggregate: Aggregate,
        compare: Comparison,
    },
    ForeignKey {
        binding: String,
        columns: Vec<String>,
        ref_binding: String,
        ref_columns: Vec<String>,
    },
    QueryZeroRows {
        bindings: Vec<String>,
        query: String,
    },
}

impl Check {
    pub const fn op(&self) -> &'static str {
        match self {
            Self::Unique { .. } => "unique",
            Self::NotNull { .. } => "not_null",
            Self::Predicate { .. } => "predicate",
            Self::RowCount { .. } => "row_count",
            Self::AggregateCompare { .. } => "aggregate_compare",
            Self::ForeignKey { .. } => "foreign_key",
            Self::QueryZeroRows { .. } => "query_zero_rows",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PredicateExpression {
    Column(ColumnReference),
    Eq { eq: [PredicateOperand; 2] },
    Ne { ne: [PredicateOperand; 2] },
    Gt { gt: [PredicateOperand; 2] },
    Gte { gte: [PredicateOperand; 2] },
    Lt { lt: [PredicateOperand; 2] },
    Lte { lte: [PredicateOperand; 2] },
    And { and: Vec<PredicateExpression> },
    Or { or: Vec<PredicateExpression> },
    Not { not: Box<PredicateExpression> },
    In { r#in: [MembershipOperand; 2] },
    IsNull { is_null: ColumnReference },
    IsBlank { is_blank: ColumnReference },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnReference {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_empty_string"
    )]
    pub binding: Option<String>,
    pub column: String,
}

fn deserialize_optional_non_empty_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(D::Error::custom("binding must be a non-empty string"));
    }
    Ok(Some(value))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PredicateOperand {
    Column(ColumnReference),
    Literal(Value),
}

impl<'de> Deserialize<'de> for PredicateOperand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Object(_) => ColumnReference::deserialize(value)
                .map(Self::Column)
                .map_err(D::Error::custom),
            Value::Array(_) => Err(D::Error::custom("predicate literals must be scalar values")),
            scalar => Ok(Self::Literal(scalar)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum MembershipOperand {
    Set(Vec<Value>),
    Operand(PredicateOperand),
}

impl<'de> Deserialize<'de> for MembershipOperand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Array(values) => {
                if values.iter().any(Value::is_array) || values.iter().any(Value::is_object) {
                    return Err(D::Error::custom(
                        "predicate membership sets may contain only scalar values",
                    ));
                }
                Ok(Self::Set(values))
            }
            operand => PredicateOperand::deserialize(operand)
                .map(Self::Operand)
                .map_err(D::Error::custom),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eq: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ne: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        Check, ConstraintSet, MembershipOperand, Portability, PredicateExpression, Severity,
    };
    use crate::CONSTRAINT_VERSION;

    #[test]
    fn constraint_artifact_round_trips() {
        let compiled = json!({
            "version": CONSTRAINT_VERSION,
            "constraint_set_id": "loan_tape.monthly.v1",
            "bindings": [
                {
                    "name": "input",
                    "kind": "relation",
                    "key_fields": ["loan_id"]
                },
                {
                    "name": "reference",
                    "kind": "relation"
                }
            ],
            "rules": [
                {
                    "id": "UNIQUE_LOAN_ID",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "unique",
                        "binding": "input",
                        "columns": ["loan_id"]
                    }
                },
                {
                    "id": "NOT_NULL_REQUIRED",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "not_null",
                        "binding": "input",
                        "columns": ["loan_id", "balance"]
                    }
                },
                {
                    "id": "POSITIVE_BALANCE",
                    "severity": "warn",
                    "portability": "portable",
                    "check": {
                        "op": "predicate",
                        "binding": "input",
                        "expr": {
                            "or": [
                                {
                                    "gt": [
                                        { "column": "balance" },
                                        0
                                    ]
                                },
                                {
                                    "is_blank": { "column": "waiver_reason" }
                                }
                            ]
                        }
                    }
                },
                {
                    "id": "MINIMUM_ROWS",
                    "severity": "warn",
                    "portability": "portable",
                    "check": {
                        "op": "row_count",
                        "binding": "input",
                        "compare": { "gte": 1 }
                    }
                },
                {
                    "id": "TOTAL_BALANCE",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "aggregate_compare",
                        "binding": "input",
                        "aggregate": { "sum": "balance" },
                        "compare": { "eq": 1500000000.0, "tolerance": 0.01 }
                    }
                },
                {
                    "id": "REFERENCE_EXISTS",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "foreign_key",
                        "binding": "input",
                        "columns": ["account_id"],
                        "ref_binding": "reference",
                        "ref_columns": ["id"]
                    }
                },
                {
                    "id": "QUERY_ASSERTION",
                    "severity": "error",
                    "portability": "batch_only",
                    "check": {
                        "op": "query_zero_rows",
                        "bindings": ["input", "reference"],
                        "query": "select 1 where false"
                    }
                }
            ]
        });

        let parsed: ConstraintSet =
            serde_json::from_value(compiled.clone()).expect("constraint artifact parses");
        let round_tripped =
            serde_json::to_value(parsed).expect("constraint artifact serializes again");

        assert_eq!(round_tripped, compiled);
    }

    #[test]
    fn predicate_and_query_variants_deserialize_to_expected_shapes() -> Result<(), String> {
        let parsed: ConstraintSet = serde_json::from_value(json!({
            "version": CONSTRAINT_VERSION,
            "constraint_set_id": "predicate.grammar.v1",
            "bindings": [
                { "name": "input", "kind": "relation" }
            ],
            "rules": [
                {
                    "id": "STATUS_ALLOWED",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "predicate",
                        "binding": "input",
                        "expr": {
                            "in": [
                                { "column": "match_status" },
                                ["MATCHED", "UNMATCHED_GOLD", "UNMATCHED_CANDIDATE"]
                            ]
                        }
                    }
                },
                {
                    "id": "QUERY_ASSERTION",
                    "severity": "error",
                    "portability": "batch_only",
                    "check": {
                        "op": "query_zero_rows",
                        "bindings": ["input"],
                        "query": "select binding, field, value from failed_rows"
                    }
                }
            ]
        }))
        .expect("predicate grammar parses");

        let expr = match &parsed.rules[0].check {
            Check::Predicate { expr, .. } => expr,
            other => return Err(format!("expected predicate check, got {other:?}")),
        };
        let PredicateExpression::In { r#in } = expr else {
            return Err(format!("expected membership expression, got {expr:?}"));
        };
        assert!(matches!(r#in[0], MembershipOperand::Operand(_)));
        assert!(matches!(r#in[1], MembershipOperand::Set(_)));

        assert!(matches!(parsed.rules[0].severity, Severity::Error));
        assert!(matches!(parsed.rules[0].portability, Portability::Portable));
        assert_eq!(parsed.rules[1].check.op(), "query_zero_rows");
        Ok(())
    }

    #[test]
    fn seeded_constraint_fixtures_round_trip() {
        const ARITY_ONE: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/arity1/not_null_loans.verify.json"
        ));
        const ARITY_N: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json"
        ));
        const QUERY_RULE: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/query_rules/orphan_rows.verify.json"
        ));

        for fixture in [ARITY_ONE, ARITY_N, QUERY_RULE] {
            let parsed: ConstraintSet =
                serde_json::from_str(fixture).expect("fixture constraint parses");
            let expected: serde_json::Value =
                serde_json::from_str(fixture).expect("fixture value parses");
            let round_tripped =
                serde_json::to_value(parsed).expect("fixture constraint serializes again");

            assert_eq!(round_tripped, expected);
        }
    }

    fn minimal_predicate_artifact() -> serde_json::Value {
        json!({
            "version": CONSTRAINT_VERSION,
            "constraint_set_id": "strict.predicate.v1",
            "bindings": [
                { "name": "input", "kind": "relation", "key_fields": ["id"] }
            ],
            "rules": [
                {
                    "id": "VALUE_PRESENT",
                    "severity": "error",
                    "portability": "portable",
                    "check": {
                        "op": "predicate",
                        "binding": "input",
                        "expr": { "column": "value" }
                    }
                }
            ]
        })
    }

    #[test]
    fn rejects_unknown_fields_at_every_compiled_constraint_boundary() {
        let mut cases = Vec::new();

        let mut top_level = minimal_predicate_artifact();
        top_level["unexpected"] = json!(true);
        cases.push(("top level", top_level));

        let mut binding = minimal_predicate_artifact();
        binding["bindings"][0]["unexpected"] = json!(true);
        cases.push(("binding", binding));

        let mut rule = minimal_predicate_artifact();
        rule["rules"][0]["unexpected"] = json!(true);
        cases.push(("rule", rule));

        let mut check = minimal_predicate_artifact();
        check["rules"][0]["check"]["unexpected"] = json!(true);
        cases.push(("check", check));

        let mut expression = minimal_predicate_artifact();
        expression["rules"][0]["check"]["expr"] = json!({
            "eq": [{ "column": "value" }, 1],
            "unexpected": true
        });
        cases.push(("predicate expression", expression));

        let mut column = minimal_predicate_artifact();
        column["rules"][0]["check"]["expr"]["unexpected"] = json!("other");
        cases.push(("column reference", column));

        for (boundary, artifact) in cases {
            let error = serde_json::from_value::<ConstraintSet>(artifact)
                .expect_err("unknown fields must be rejected");
            assert!(
                error.to_string().contains("unknown field")
                    || error.to_string().contains("did not match any variant"),
                "{boundary} produced an unexpected error: {error}"
            );
        }
    }

    #[test]
    fn predicate_objects_cannot_fall_back_to_literals() {
        for invalid in [
            json!({ "eq": [{ "column": "value" }, { "column": "value", "unexpected": true }] }),
            json!({ "eq": [{ "column": "value" }, [1, 2]] }),
            json!({ "in": [{ "column": "value" }, [1, { "nested": true }]] }),
        ] {
            serde_json::from_value::<PredicateExpression>(invalid)
                .expect_err("non-scalar predicate literals must be rejected");
        }
    }

    #[test]
    fn predicate_scalar_literals_and_membership_sets_remain_valid() {
        for scalar in [json!(null), json!(true), json!(1), json!("one")] {
            let expression = json!({ "eq": [{ "column": "value" }, scalar] });
            serde_json::from_value::<PredicateExpression>(expression)
                .expect("declared scalar predicate literal should parse");
        }

        serde_json::from_value::<PredicateExpression>(json!({
            "in": [{ "column": "value" }, [null, true, 1, "one"]]
        }))
        .expect("scalar membership set should parse");
    }

    #[test]
    fn optional_column_binding_round_trips_without_changing_legacy_shape() {
        let legacy_bytes = r#"{"eq":[{"column":"value"},1]}"#;
        let legacy: PredicateExpression =
            serde_json::from_str(legacy_bytes).expect("legacy predicate should parse");
        assert_eq!(
            serde_json::to_value(&legacy).expect("legacy predicate should serialize"),
            json!({ "eq": [{ "column": "value" }, 1] })
        );
        assert_eq!(
            serde_json::to_string(&legacy).expect("legacy predicate bytes should serialize"),
            legacy_bytes
        );

        let qualified = json!({
            "eq": [
                { "column": "value" },
                { "binding": "prior", "column": "value" }
            ]
        });
        let parsed: PredicateExpression =
            serde_json::from_value(qualified.clone()).expect("qualified predicate should parse");
        assert_eq!(
            serde_json::to_value(parsed).expect("qualified predicate should serialize"),
            qualified
        );
    }

    #[test]
    fn present_column_binding_must_be_a_non_empty_string() {
        for invalid_binding in [json!(""), json!(null)] {
            let expression = json!({
                "eq": [
                    { "binding": invalid_binding, "column": "value" },
                    1
                ]
            });
            serde_json::from_value::<PredicateExpression>(expression)
                .expect_err("present binding must be a non-empty string");
        }
    }

    #[test]
    fn explicitly_empty_key_fields_are_not_normalized_to_absence() {
        let mut artifact = minimal_predicate_artifact();
        artifact["bindings"][0]["key_fields"] = json!([]);

        let error = serde_json::from_value::<ConstraintSet>(artifact)
            .expect_err("explicitly empty key fields should fail closed");
        assert!(error.to_string().contains("empty_key_fields"));
    }
}
