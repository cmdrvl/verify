use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::CONSTRAINT_VERSION;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
pub struct Binding {
    pub name: String,
    pub kind: BindingKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BindingKind {
    #[default]
    Relation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[serde(tag = "op", rename_all = "snake_case")]
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
#[serde(untagged)]
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
pub struct ColumnReference {
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PredicateOperand {
    Column(ColumnReference),
    Literal(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MembershipOperand {
    Set(Vec<Value>),
    Operand(PredicateOperand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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
}
