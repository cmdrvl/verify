use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use verify_core::{
    constraint::{BindingKind, Check, ConstraintSet, Portability, Rule},
    order::sort_report,
    refusal::RefusalCode,
    report::{BindingReport, ExecutionMode, Outcome, VerifyReport},
};

use crate::{Relation, portable_relation, portable_row, summary::SummaryEngine};

pub type EmbeddedBindings = BTreeMap<String, EmbeddedBinding>;

#[derive(Debug, Clone)]
pub struct EmbeddedBinding {
    pub source: String,
    pub content_hash: String,
    pub relation: Relation,
}

impl EmbeddedBinding {
    pub fn new(
        source: impl Into<String>,
        content_hash: impl Into<String>,
        relation: Relation,
    ) -> Self {
        Self {
            source: source.into(),
            content_hash: content_hash.into(),
            relation,
        }
    }

    fn binding_report(&self, kind: BindingKind) -> BindingReport {
        BindingReport {
            kind,
            source: self.source.clone(),
            content_hash: self.content_hash.clone(),
            input_verification: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EmbeddedExecutor;

impl EmbeddedExecutor {
    pub fn evaluate(
        constraints: &ConstraintSet,
        constraint_hash: impl Into<String>,
        bindings: &EmbeddedBindings,
    ) -> VerifyReport {
        evaluate(constraints, constraint_hash, bindings)
    }
}

#[derive(Debug, Clone)]
enum EmbeddedEvaluationError {
    MissingBinding(String),
    UndeclaredBinding(String),
    FieldNotFound { binding: String, field: String },
    BadExpr(String),
    BadConstraints(String),
    BatchOnlyRule { rule_id: String, op: String },
}

pub fn evaluate(
    constraints: &ConstraintSet,
    constraint_hash: impl Into<String>,
    bindings: &EmbeddedBindings,
) -> VerifyReport {
    let constraint_hash = constraint_hash.into();

    if let Err(error) = validate_bindings(constraints, bindings) {
        return refusal_report(constraints, &constraint_hash, bindings, error);
    }

    if let Some(error) = batch_only_rule_error(constraints) {
        return refusal_report(constraints, &constraint_hash, bindings, error);
    }

    let relations = bindings
        .iter()
        .map(|(name, binding)| (name.clone(), binding.relation.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut report = VerifyReport::new(
        ExecutionMode::Embedded,
        constraints.constraint_set_id.clone(),
        constraint_hash,
    );
    report.bindings = binding_reports(constraints, bindings);

    for rule in &constraints.rules {
        let result = match evaluate_rule(rule, &relations) {
            Ok(result) => result,
            Err(error) => {
                return refusal_report(
                    constraints,
                    &report.constraint_hash,
                    bindings,
                    error.with_rule(rule),
                );
            }
        };
        report.results.push(result);
    }

    SummaryEngine::apply(&mut report);
    report.outcome = if report.summary.failed_rules > 0 {
        Outcome::Fail
    } else {
        Outcome::Pass
    };
    sort_report(&mut report);
    report
}

fn validate_bindings(
    constraints: &ConstraintSet,
    bindings: &EmbeddedBindings,
) -> Result<(), EmbeddedEvaluationError> {
    let declared_names = constraints
        .bindings
        .iter()
        .map(|binding| binding.name.as_str())
        .collect::<BTreeSet<_>>();

    for binding in &constraints.bindings {
        if !bindings.contains_key(binding.name.as_str()) {
            return Err(EmbeddedEvaluationError::MissingBinding(
                binding.name.clone(),
            ));
        }
    }

    for name in bindings.keys() {
        if !declared_names.contains(name.as_str()) {
            return Err(EmbeddedEvaluationError::UndeclaredBinding(name.clone()));
        }
    }

    Ok(())
}

fn batch_only_rule_error(constraints: &ConstraintSet) -> Option<EmbeddedEvaluationError> {
    constraints
        .rules
        .iter()
        .filter(|rule| {
            matches!(rule.portability, Portability::BatchOnly)
                || matches!(rule.check, Check::QueryZeroRows { .. })
        })
        .map(|rule| EmbeddedEvaluationError::BatchOnlyRule {
            rule_id: rule.id.clone(),
            op: rule.check.op().to_owned(),
        })
        .next()
}

fn binding_reports(
    constraints: &ConstraintSet,
    bindings: &EmbeddedBindings,
) -> BTreeMap<String, BindingReport> {
    let declared_kinds = constraints
        .bindings
        .iter()
        .map(|binding| (binding.name.as_str(), binding.kind))
        .collect::<BTreeMap<_, _>>();

    bindings
        .iter()
        .map(|(name, binding)| {
            (
                name.clone(),
                binding.binding_report(
                    declared_kinds
                        .get(name.as_str())
                        .copied()
                        .unwrap_or(BindingKind::Relation),
                ),
            )
        })
        .collect()
}

fn evaluate_rule(
    rule: &Rule,
    relations: &BTreeMap<String, Relation>,
) -> Result<verify_core::report::RuleResult, EmbeddedEvaluationError> {
    match &rule.check {
        Check::Unique { .. } | Check::NotNull { .. } | Check::Predicate { .. } => {
            portable_row::evaluate_rule(rule, relations).map_err(EmbeddedEvaluationError::from)
        }
        Check::RowCount { .. } | Check::AggregateCompare { .. } | Check::ForeignKey { .. } => {
            portable_relation::evaluate_rule(rule, relations).map_err(EmbeddedEvaluationError::from)
        }
        Check::QueryZeroRows { .. } => Err(EmbeddedEvaluationError::BatchOnlyRule {
            rule_id: rule.id.clone(),
            op: rule.check.op().to_owned(),
        }),
    }
}

fn refusal_report(
    constraints: &ConstraintSet,
    constraint_hash: &str,
    bindings: &EmbeddedBindings,
    error: EmbeddedEvaluationError,
) -> VerifyReport {
    let (code, message, detail) = error.protocol_parts();
    let mut report = VerifyReport::refusal(
        ExecutionMode::Embedded,
        constraints.constraint_set_id.clone(),
        constraint_hash.to_owned(),
        code,
        message,
        detail,
    );
    report.bindings = binding_reports(constraints, bindings);
    report
}

impl EmbeddedEvaluationError {
    fn with_rule(self, rule: &Rule) -> Self {
        match self {
            Self::BadExpr(message) => Self::BadExpr(format!("{message} [rule_id={}]", rule.id)),
            Self::BadConstraints(message) => {
                Self::BadConstraints(format!("{message} [rule_id={}]", rule.id))
            }
            other => other,
        }
    }

    fn protocol_parts(self) -> (RefusalCode, String, serde_json::Value) {
        match self {
            Self::MissingBinding(binding) => (
                RefusalCode::MissingBinding,
                format!("embedded execution missing declared binding: {binding}"),
                json!({ "binding": binding }),
            ),
            Self::UndeclaredBinding(binding) => (
                RefusalCode::UndeclaredBinding,
                format!("embedded execution received undeclared binding: {binding}"),
                json!({ "binding": binding }),
            ),
            Self::FieldNotFound { binding, field } => (
                RefusalCode::FieldNotFound,
                format!("field not found: {binding}.{field}"),
                json!({
                    "binding": binding,
                    "field": field,
                }),
            ),
            Self::BadExpr(detail) => (
                RefusalCode::BadExpr,
                format!("embedded execution could not evaluate rule expression: {detail}"),
                json!({ "detail": detail }),
            ),
            Self::BadConstraints(detail) => (
                RefusalCode::BadConstraints,
                format!("embedded execution received unsupported compiled constraints: {detail}"),
                json!({ "detail": detail }),
            ),
            Self::BatchOnlyRule { rule_id, op } => (
                RefusalCode::BatchOnlyRule,
                "Embedded execution cannot evaluate batch-only rules".to_owned(),
                json!({
                    "rule_id": rule_id,
                    "op": op,
                    "execution_mode": "embedded",
                }),
            ),
        }
    }
}

impl From<portable_row::EngineError> for EmbeddedEvaluationError {
    fn from(value: portable_row::EngineError) -> Self {
        match value {
            portable_row::EngineError::MissingBinding(binding) => Self::MissingBinding(binding),
            portable_row::EngineError::UnsupportedOp(op) => Self::BadConstraints(op),
            portable_row::EngineError::BadExpression(detail) => Self::BadExpr(detail),
        }
    }
}

impl From<portable_relation::RelationEngineError> for EmbeddedEvaluationError {
    fn from(value: portable_relation::RelationEngineError) -> Self {
        match value {
            portable_relation::RelationEngineError::MissingBinding(binding) => {
                Self::MissingBinding(binding)
            }
            portable_relation::RelationEngineError::MissingField { binding, field } => {
                Self::FieldNotFound { binding, field }
            }
            portable_relation::RelationEngineError::UnsupportedOp(op) => Self::BadConstraints(op),
            portable_relation::RelationEngineError::BadExpression(detail) => Self::BadExpr(detail),
        }
    }
}
