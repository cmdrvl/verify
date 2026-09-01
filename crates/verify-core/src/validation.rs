use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;
use serde_json::{Value, json};

use crate::constraint::{
    Check, ColumnReference, ConstraintSet, MembershipOperand, Portability, PredicateExpression,
    PredicateOperand, Rule,
};

/// A column reference with an omitted binding resolved against its predicate anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumnReference {
    pub binding: String,
    pub column: String,
}

/// Deterministic, execution-independent analysis of one predicate expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateAnalysis {
    pub anchor_binding: String,
    pub references: Vec<ResolvedColumnReference>,
    pub participating_bindings: Vec<String>,
    pub derived_portability: Portability,
}

/// A validated predicate paired with its rule identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPredicate {
    pub rule_id: String,
    pub analysis: PredicateAnalysis,
}

/// Stable reasons shared by compiled-artifact and authoring validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintValidationReason {
    PortabilityMismatch,
    UndeclaredReference,
    MissingKeyFields,
    EmptyKeyFields,
    KeyArityMismatch,
}

impl ConstraintValidationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PortabilityMismatch => "portability_mismatch",
            Self::UndeclaredReference => "undeclared_reference",
            Self::MissingKeyFields => "missing_key_fields",
            Self::EmptyKeyFields => "empty_key_fields",
            Self::KeyArityMismatch => "key_arity_mismatch",
        }
    }
}

/// A structured validation failure whose detail is ready for a refusal envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct ConstraintValidationError {
    pub reason: ConstraintValidationReason,
    pub rule_id: String,
    pub detail: Value,
}

impl ConstraintValidationError {
    fn new(reason: ConstraintValidationReason, rule_id: &str, detail: Value) -> Self {
        Self {
            reason,
            rule_id: rule_id.to_owned(),
            detail,
        }
    }
}

impl fmt::Display for ConstraintValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} for predicate rule {}",
            self.reason.as_str(),
            self.rule_id
        )
    }
}

impl std::error::Error for ConstraintValidationError {}

/// Resolves every column reference, preserving AST traversal order and duplicates.
pub fn analyze_predicate(
    anchor_binding: &str,
    expression: &PredicateExpression,
) -> PredicateAnalysis {
    let mut references = Vec::new();
    collect_expression_references(expression, anchor_binding, &mut references);

    let mut participating = BTreeSet::from([anchor_binding.to_owned()]);
    participating.extend(references.iter().map(|reference| reference.binding.clone()));
    let derived_portability = if participating
        .iter()
        .all(|binding| binding == anchor_binding)
    {
        Portability::Portable
    } else {
        Portability::BatchOnly
    };

    PredicateAnalysis {
        anchor_binding: anchor_binding.to_owned(),
        references,
        participating_bindings: participating.into_iter().collect(),
        derived_portability,
    }
}

/// Validates predicate declarations and key shape without reading relation data.
///
/// Returned analyses are sorted by rule ID. Runtime key validity, uniqueness, and
/// counterpart matching deliberately remain outside this pure protocol layer.
pub fn validate_constraint_predicates(
    constraints: &ConstraintSet,
) -> Result<Vec<ValidatedPredicate>, ConstraintValidationError> {
    let declared_bindings = constraints
        .bindings
        .iter()
        .map(|binding| (binding.name.as_str(), binding))
        .collect::<BTreeMap<_, _>>();
    let mut predicate_rules = constraints
        .rules
        .iter()
        .filter_map(predicate_rule)
        .collect::<Vec<_>>();
    predicate_rules.sort_by(|left, right| left.0.id.cmp(&right.0.id));

    let mut validated = Vec::with_capacity(predicate_rules.len());
    for (rule, anchor_binding, expression) in predicate_rules {
        let analysis = analyze_predicate(anchor_binding, expression);
        if rule.portability != analysis.derived_portability {
            return Err(ConstraintValidationError::new(
                ConstraintValidationReason::PortabilityMismatch,
                &rule.id,
                json!({
                    "reason": ConstraintValidationReason::PortabilityMismatch,
                    "rule_id": rule.id,
                    "declared_portability": rule.portability,
                    "derived_portability": analysis.derived_portability,
                    "participating_bindings": analysis.participating_bindings,
                }),
            ));
        }

        if let Some(binding) = analysis
            .participating_bindings
            .iter()
            .find(|binding| !declared_bindings.contains_key(binding.as_str()))
        {
            return Err(ConstraintValidationError::new(
                ConstraintValidationReason::UndeclaredReference,
                &rule.id,
                json!({
                    "reason": ConstraintValidationReason::UndeclaredReference,
                    "rule_id": rule.id,
                    "binding": binding,
                    "participating_bindings": analysis.participating_bindings,
                }),
            ));
        }

        if analysis.derived_portability == Portability::BatchOnly {
            let declarations = analysis
                .participating_bindings
                .iter()
                .map(|name| {
                    let key_fields = declared_bindings.get(name.as_str()).and_then(|binding| {
                        (!binding.key_fields.is_empty()).then(|| binding.key_fields.clone())
                    });
                    (name.clone(), key_fields)
                })
                .collect();
            validate_predicate_key_fields(
                &rule.id,
                anchor_binding,
                &analysis.participating_bindings,
                &declarations,
            )?;
        }

        validated.push(ValidatedPredicate {
            rule_id: rule.id.clone(),
            analysis,
        });
    }

    Ok(validated)
}

/// Validates key declarations for a binding-qualified predicate.
///
/// `None` represents an omitted declaration, while `Some(vec![])` represents an
/// explicitly empty declaration. The authoring lane can therefore reuse this
/// function before compilation, while compiled artifacts use `None` for the
/// legacy omitted representation.
pub fn validate_predicate_key_fields(
    rule_id: &str,
    anchor_binding: &str,
    participating_bindings: &[String],
    key_fields_by_binding: &BTreeMap<String, Option<Vec<String>>>,
) -> Result<(), ConstraintValidationError> {
    for binding in participating_bindings {
        match key_fields_by_binding.get(binding) {
            None | Some(None) => {
                return Err(key_fields_error(
                    ConstraintValidationReason::MissingKeyFields,
                    rule_id,
                    binding,
                    participating_bindings,
                    key_fields_by_binding,
                ));
            }
            Some(Some(key_fields)) if key_fields.is_empty() => {
                return Err(key_fields_error(
                    ConstraintValidationReason::EmptyKeyFields,
                    rule_id,
                    binding,
                    participating_bindings,
                    key_fields_by_binding,
                ));
            }
            Some(Some(_)) => {}
        }
    }

    let expected_fields = match key_fields_by_binding.get(anchor_binding) {
        None | Some(None) => {
            return Err(key_fields_error(
                ConstraintValidationReason::MissingKeyFields,
                rule_id,
                anchor_binding,
                participating_bindings,
                key_fields_by_binding,
            ));
        }
        Some(Some(key_fields)) if key_fields.is_empty() => {
            return Err(key_fields_error(
                ConstraintValidationReason::EmptyKeyFields,
                rule_id,
                anchor_binding,
                participating_bindings,
                key_fields_by_binding,
            ));
        }
        Some(Some(key_fields)) => key_fields,
    };
    for binding in participating_bindings {
        let Some(Some(actual_fields)) = key_fields_by_binding.get(binding) else {
            return Err(key_fields_error(
                ConstraintValidationReason::MissingKeyFields,
                rule_id,
                binding,
                participating_bindings,
                key_fields_by_binding,
            ));
        };
        if actual_fields.len() != expected_fields.len() {
            return Err(ConstraintValidationError::new(
                ConstraintValidationReason::KeyArityMismatch,
                rule_id,
                json!({
                    "reason": ConstraintValidationReason::KeyArityMismatch,
                    "rule_id": rule_id,
                    "binding": binding,
                    "anchor_binding": anchor_binding,
                    "expected_arity": expected_fields.len(),
                    "actual_arity": actual_fields.len(),
                    "participating_bindings": participating_bindings,
                    "key_fields_by_binding": key_fields_by_binding,
                }),
            ));
        }
    }

    Ok(())
}

fn predicate_rule(rule: &Rule) -> Option<(&Rule, &str, &PredicateExpression)> {
    match &rule.check {
        Check::Predicate { binding, expr } => Some((rule, binding, expr)),
        _ => None,
    }
}

fn key_fields_error(
    reason: ConstraintValidationReason,
    rule_id: &str,
    binding: &str,
    participating_bindings: &[String],
    key_fields_by_binding: &BTreeMap<String, Option<Vec<String>>>,
) -> ConstraintValidationError {
    ConstraintValidationError::new(
        reason,
        rule_id,
        json!({
            "reason": reason,
            "rule_id": rule_id,
            "binding": binding,
            "participating_bindings": participating_bindings,
            "key_fields_by_binding": key_fields_by_binding,
        }),
    )
}

fn collect_expression_references(
    expression: &PredicateExpression,
    anchor_binding: &str,
    references: &mut Vec<ResolvedColumnReference>,
) {
    match expression {
        PredicateExpression::Column(reference) => {
            push_reference(reference, anchor_binding, references);
        }
        PredicateExpression::Eq { eq: operands }
        | PredicateExpression::Ne { ne: operands }
        | PredicateExpression::Gt { gt: operands }
        | PredicateExpression::Gte { gte: operands }
        | PredicateExpression::Lt { lt: operands }
        | PredicateExpression::Lte { lte: operands } => {
            for operand in operands {
                collect_operand_reference(operand, anchor_binding, references);
            }
        }
        PredicateExpression::And { and: expressions }
        | PredicateExpression::Or { or: expressions } => {
            for expression in expressions {
                collect_expression_references(expression, anchor_binding, references);
            }
        }
        PredicateExpression::Not { not: expression } => {
            collect_expression_references(expression, anchor_binding, references);
        }
        PredicateExpression::In { r#in: operands } => {
            for operand in operands {
                if let MembershipOperand::Operand(operand) = operand {
                    collect_operand_reference(operand, anchor_binding, references);
                }
            }
        }
        PredicateExpression::IsNull { is_null: reference }
        | PredicateExpression::IsBlank {
            is_blank: reference,
        } => push_reference(reference, anchor_binding, references),
    }
}

fn collect_operand_reference(
    operand: &PredicateOperand,
    anchor_binding: &str,
    references: &mut Vec<ResolvedColumnReference>,
) {
    if let PredicateOperand::Column(reference) = operand {
        push_reference(reference, anchor_binding, references);
    }
}

fn push_reference(
    reference: &ColumnReference,
    anchor_binding: &str,
    references: &mut Vec<ResolvedColumnReference>,
) {
    references.push(ResolvedColumnReference {
        binding: reference
            .binding
            .as_deref()
            .unwrap_or(anchor_binding)
            .to_owned(),
        column: reference.column.clone(),
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        ConstraintValidationReason, analyze_predicate, validate_constraint_predicates,
        validate_predicate_key_fields,
    };
    use crate::CONSTRAINT_VERSION;
    use crate::constraint::{
        Binding, BindingKind, Check, ConstraintSet, Portability, PredicateExpression, Rule,
        Severity,
    };

    fn expression(value: serde_json::Value) -> PredicateExpression {
        serde_json::from_value(value).expect("test predicate should parse")
    }

    fn binding(name: &str, key_fields: &[&str]) -> Binding {
        Binding {
            name: name.to_owned(),
            kind: BindingKind::Relation,
            key_fields: key_fields.iter().map(|field| (*field).to_owned()).collect(),
        }
    }

    fn constraint(
        bindings: Vec<Binding>,
        portability: Portability,
        expression: PredicateExpression,
    ) -> ConstraintSet {
        ConstraintSet {
            version: CONSTRAINT_VERSION.to_owned(),
            constraint_set_id: "binding.qualified.test".to_owned(),
            bindings,
            rules: vec![Rule {
                id: "CHECK_VALUE".to_owned(),
                severity: Severity::Error,
                portability,
                check: Check::Predicate {
                    binding: "current".to_owned(),
                    expr: expression,
                },
            }],
        }
    }

    #[test]
    fn recursive_analysis_visits_every_expression_form_and_resolves_anchor_references() {
        let predicate = expression(json!({
            "and": [
                {"eq": [{"column": "eq_current"}, {"binding": "prior", "column": "eq_prior"}]},
                {"ne": [{"column": "ne_current"}, 0]},
                {"gt": [{"binding": "history", "column": "gt_history"}, 0]},
                {"gte": [{"column": "gte_current"}, 0]},
                {"lt": [{"column": "lt_current"}, 1]},
                {"lte": [{"column": "lte_current"}, 1]},
                {"or": [
                    {"is_null": {"binding": "prior", "column": "nullable"}},
                    {"not": {"is_blank": {"column": "blankable"}}}
                ]},
                {"in": [{"binding": "history", "column": "status"}, ["A", "B"]]}
            ]
        }));

        let analysis = analyze_predicate("current", &predicate);

        assert_eq!(analysis.derived_portability, Portability::BatchOnly);
        assert_eq!(
            analysis.participating_bindings,
            vec!["current", "history", "prior"]
        );
        assert_eq!(analysis.references.len(), 10);
        assert_eq!(analysis.references[0].binding, "current");
        assert_eq!(analysis.references[1].binding, "prior");
        assert_eq!(analysis.references[9].column, "status");
    }

    #[test]
    fn explicit_anchor_reference_remains_portable_without_key_fields() {
        let constraints = constraint(
            vec![binding("current", &[])],
            Portability::Portable,
            expression(json!({
                "eq": [
                    {"binding": "current", "column": "value"},
                    {"column": "expected"}
                ]
            })),
        );

        let validated = validate_constraint_predicates(&constraints)
            .expect("same-anchor predicate should be portable");

        assert_eq!(validated.len(), 1);
        assert_eq!(
            validated[0].analysis.derived_portability,
            Portability::Portable
        );
    }

    #[test]
    fn valid_binding_qualified_predicate_allows_different_physical_key_names() {
        let constraints = constraint(
            vec![
                binding("current", &["loan_id", "tranche_id"]),
                binding("prior", &["asset_number", "class_code"]),
            ],
            Portability::BatchOnly,
            expression(json!({
                "eq": [
                    {"column": "maturity_date"},
                    {"binding": "prior", "column": "maturity_date"}
                ]
            })),
        );

        validate_constraint_predicates(&constraints)
            .expect("positionally aligned keys should validate");
    }

    #[test]
    fn validation_reasons_and_details_are_stable() {
        let qualified = || {
            expression(json!({
                "eq": [
                    {"column": "value"},
                    {"binding": "prior", "column": "value"}
                ]
            }))
        };

        let mismatch = validate_constraint_predicates(&constraint(
            vec![binding("current", &["id"]), binding("prior", &["id"])],
            Portability::Portable,
            qualified(),
        ))
        .expect_err("declared portability must match analysis");
        assert_eq!(
            mismatch.reason,
            ConstraintValidationReason::PortabilityMismatch
        );
        assert_eq!(mismatch.detail["reason"], "portability_mismatch");

        let undeclared = validate_constraint_predicates(&constraint(
            vec![binding("current", &["id"])],
            Portability::BatchOnly,
            qualified(),
        ))
        .expect_err("every resolved binding must be declared");
        assert_eq!(
            undeclared.reason,
            ConstraintValidationReason::UndeclaredReference
        );
        assert_eq!(undeclared.detail["binding"], "prior");

        let missing = validate_constraint_predicates(&constraint(
            vec![binding("current", &["id"]), binding("prior", &[])],
            Portability::BatchOnly,
            qualified(),
        ))
        .expect_err("every participant needs key fields");
        assert_eq!(missing.reason, ConstraintValidationReason::MissingKeyFields);
        assert_eq!(missing.detail["binding"], "prior");

        let arity = validate_constraint_predicates(&constraint(
            vec![
                binding("current", &["id", "part"]),
                binding("prior", &["id"]),
            ],
            Portability::BatchOnly,
            qualified(),
        ))
        .expect_err("participant key arity must match the anchor");
        assert_eq!(arity.reason, ConstraintValidationReason::KeyArityMismatch);
        assert_eq!(arity.detail["expected_arity"], 2);
        assert_eq!(arity.detail["actual_arity"], 1);
    }

    #[test]
    fn reusable_key_validator_distinguishes_missing_from_explicitly_empty() {
        let participants = vec!["current".to_owned(), "prior".to_owned()];
        let missing = BTreeMap::from([
            ("current".to_owned(), Some(vec!["id".to_owned()])),
            ("prior".to_owned(), None),
        ]);
        let error =
            validate_predicate_key_fields("CHECK_VALUE", "current", &participants, &missing)
                .expect_err("missing key declaration should fail");
        assert_eq!(error.reason, ConstraintValidationReason::MissingKeyFields);

        let empty = BTreeMap::from([
            ("current".to_owned(), Some(vec!["id".to_owned()])),
            ("prior".to_owned(), Some(Vec::new())),
        ]);
        let error = validate_predicate_key_fields("CHECK_VALUE", "current", &participants, &empty)
            .expect_err("empty key declaration should fail");
        assert_eq!(error.reason, ConstraintValidationReason::EmptyKeyFields);
        assert_eq!(error.detail["reason"], "empty_key_fields");

        let malformed_participants = vec!["prior".to_owned()];
        let missing_anchor = BTreeMap::from([("prior".to_owned(), Some(vec!["id".to_owned()]))]);
        let error = validate_predicate_key_fields(
            "CHECK_VALUE",
            "current",
            &malformed_participants,
            &missing_anchor,
        )
        .expect_err("a missing anchor declaration should return an error, not panic");
        assert_eq!(error.reason, ConstraintValidationReason::MissingKeyFields);
        assert_eq!(error.detail["binding"], "current");
    }
}
