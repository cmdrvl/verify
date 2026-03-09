use std::collections::BTreeMap;

use serde_json::json;
use verify_core::{
    constraint::{
        Aggregate, Binding, BindingKind, Check, Comparison, ConstraintSet, Portability, Rule,
        Severity,
    },
    order::sort_report,
    refusal::RefusalCode,
    report::{ExecutionMode, Outcome, VerifyReport},
};
use verify_engine::{
    Relation, SummaryEngine,
    embedded::{EmbeddedBinding, EmbeddedBindings, EmbeddedExecutor},
    portable_relation, portable_row,
};

fn constraints() -> ConstraintSet {
    ConstraintSet {
        version: verify_core::CONSTRAINT_VERSION.to_owned(),
        constraint_set_id: "embedded.portable.parity.v1".to_owned(),
        bindings: vec![
            Binding {
                name: "loans".to_owned(),
                kind: BindingKind::Relation,
                key_fields: vec!["loan_id".to_owned()],
            },
            Binding {
                name: "properties".to_owned(),
                kind: BindingKind::Relation,
                key_fields: vec!["property_id".to_owned()],
            },
        ],
        rules: vec![
            Rule {
                id: "ROW_COUNT_AT_LEAST_FOUR".to_owned(),
                severity: Severity::Warn,
                portability: Portability::Portable,
                check: Check::RowCount {
                    binding: "loans".to_owned(),
                    compare: Comparison {
                        gte: Some(json!(4)),
                        ..Default::default()
                    },
                },
            },
            Rule {
                id: "UNIQUE_LOAN_ID".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::Unique {
                    binding: "loans".to_owned(),
                    columns: vec!["loan_id".to_owned()],
                },
            },
            Rule {
                id: "AMOUNT_PRESENT".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::NotNull {
                    binding: "loans".to_owned(),
                    columns: vec!["amount".to_owned()],
                },
            },
            Rule {
                id: "POSITIVE_OR_WAIVED".to_owned(),
                severity: Severity::Warn,
                portability: Portability::Portable,
                check: Check::Predicate {
                    binding: "loans".to_owned(),
                    expr: verify_core::constraint::PredicateExpression::Or {
                        or: vec![
                            verify_core::constraint::PredicateExpression::Gt {
                                gt: [
                                    verify_core::constraint::PredicateOperand::Column(
                                        verify_core::constraint::ColumnReference {
                                            column: "amount".to_owned(),
                                        },
                                    ),
                                    verify_core::constraint::PredicateOperand::Literal(json!(0)),
                                ],
                            },
                            verify_core::constraint::PredicateExpression::Not {
                                not: Box::new(
                                    verify_core::constraint::PredicateExpression::IsBlank {
                                        is_blank: verify_core::constraint::ColumnReference {
                                            column: "waiver_reason".to_owned(),
                                        },
                                    },
                                ),
                            },
                        ],
                    },
                },
            },
            Rule {
                id: "SUM_AMOUNT_AT_LEAST_TWENTY".to_owned(),
                severity: Severity::Warn,
                portability: Portability::Portable,
                check: Check::AggregateCompare {
                    binding: "loans".to_owned(),
                    aggregate: Aggregate {
                        sum: Some("amount".to_owned()),
                        ..Default::default()
                    },
                    compare: Comparison {
                        gte: Some(json!(20)),
                        ..Default::default()
                    },
                },
            },
            Rule {
                id: "PROPERTY_FOREIGN_KEY".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::ForeignKey {
                    binding: "loans".to_owned(),
                    columns: vec!["property_id".to_owned()],
                    ref_binding: "properties".to_owned(),
                    ref_columns: vec!["property_id".to_owned()],
                },
            },
        ],
    }
}

fn bindings() -> EmbeddedBindings {
    let loans_rows = vec![
        BTreeMap::from([
            ("loan_id".to_owned(), json!("LN-100")),
            ("amount".to_owned(), json!(10)),
            ("property_id".to_owned(), json!("P-1")),
            ("waiver_reason".to_owned(), json!("")),
        ]),
        BTreeMap::from([
            ("loan_id".to_owned(), json!("LN-200")),
            ("amount".to_owned(), json!(0)),
            ("property_id".to_owned(), json!("P-9")),
            ("waiver_reason".to_owned(), json!("manual_review")),
        ]),
        BTreeMap::from([
            ("loan_id".to_owned(), json!("LN-200")),
            ("amount".to_owned(), json!(5)),
            ("property_id".to_owned(), json!("P-2")),
            ("waiver_reason".to_owned(), json!("")),
        ]),
    ];
    let properties_rows = vec![
        BTreeMap::from([("property_id".to_owned(), json!("P-1"))]),
        BTreeMap::from([("property_id".to_owned(), json!("P-2"))]),
    ];

    BTreeMap::from([
        (
            "loans".to_owned(),
            EmbeddedBinding::new(
                "embedded://loans",
                "sha256:loans",
                Relation::new(vec!["loan_id".to_owned()], loans_rows),
            ),
        ),
        (
            "properties".to_owned(),
            EmbeddedBinding::new(
                "embedded://properties",
                "sha256:properties",
                Relation::new(vec!["property_id".to_owned()], properties_rows),
            ),
        ),
    ])
}

fn loans_only_bindings() -> EmbeddedBindings {
    BTreeMap::from([(
        "loans".to_owned(),
        EmbeddedBinding::new(
            "embedded://loans",
            "sha256:loans",
            Relation::new(
                vec!["loan_id".to_owned()],
                vec![BTreeMap::from([
                    ("loan_id".to_owned(), json!("LN-100")),
                    ("amount".to_owned(), json!(10)),
                    ("property_id".to_owned(), json!("P-1")),
                    ("waiver_reason".to_owned(), json!("")),
                ])],
            ),
        ),
    )])
}

fn batch_like_report(
    constraints: &ConstraintSet,
    bindings: &EmbeddedBindings,
    constraint_hash: &str,
) -> VerifyReport {
    let relations = bindings
        .iter()
        .map(|(name, binding)| (name.clone(), binding.relation.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut report = VerifyReport::new(
        ExecutionMode::Batch,
        constraints.constraint_set_id.clone(),
        constraint_hash.to_owned(),
    );
    report.bindings = bindings
        .iter()
        .map(|(name, binding)| {
            (
                name.clone(),
                verify_core::report::BindingReport {
                    kind: constraints
                        .bindings
                        .iter()
                        .find(|declared| declared.name == *name)
                        .map(|declared| declared.kind)
                        .unwrap_or(BindingKind::Relation),
                    source: binding.source.clone(),
                    content_hash: binding.content_hash.clone(),
                    input_verification: None,
                },
            )
        })
        .collect();

    for rule in &constraints.rules {
        assert!(
            !matches!(rule.check, Check::QueryZeroRows { .. }),
            "batch-like parity fixture should contain portable rules only"
        );
        let result = match &rule.check {
            Check::Unique { .. } | Check::NotNull { .. } | Check::Predicate { .. } => {
                portable_row::evaluate_rule(rule, &relations)
                    .expect("portable row rules should evaluate in parity fixture")
            }
            Check::RowCount { .. } | Check::AggregateCompare { .. } | Check::ForeignKey { .. } => {
                portable_relation::evaluate_rule(rule, &relations)
                    .expect("portable relation rules should evaluate in parity fixture")
            }
            Check::QueryZeroRows { .. } => continue,
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

#[test]
fn embedded_executor_matches_portable_batch_semantics() {
    let constraints = constraints();
    let bindings = bindings();

    let embedded = EmbeddedExecutor::evaluate(&constraints, "sha256:constraint", &bindings);
    let mut expected = batch_like_report(&constraints, &bindings, "sha256:constraint");
    expected.execution_mode = ExecutionMode::Embedded;

    assert_eq!(embedded, expected);
}

#[test]
fn embedded_executor_refuses_batch_only_rules() {
    let mut constraints = ConstraintSet::new("embedded.batch_only.refusal.v1");
    constraints.bindings = vec![Binding {
        name: "loans".to_owned(),
        kind: BindingKind::Relation,
        key_fields: vec!["loan_id".to_owned()],
    }];
    constraints.rules = vec![Rule {
        id: "SQL_ONLY_CHECK".to_owned(),
        severity: Severity::Error,
        portability: Portability::BatchOnly,
        check: Check::QueryZeroRows {
            bindings: vec!["loans".to_owned()],
            query: "select * from loans".to_owned(),
        },
    }];

    let bindings = loans_only_bindings();
    let report = EmbeddedExecutor::evaluate(&constraints, "sha256:constraint", &bindings);
    let refusal = report.refusal.expect("refusal payload");

    assert_eq!(report.execution_mode, ExecutionMode::Embedded);
    assert_eq!(report.outcome, Outcome::Refusal);
    assert_eq!(refusal.code, RefusalCode::BatchOnlyRule);
    assert_eq!(
        refusal.message,
        "Embedded execution cannot evaluate batch-only rules"
    );
    assert_eq!(
        refusal.detail,
        json!({
            "rule_id": "SQL_ONLY_CHECK",
            "op": "query_zero_rows",
            "execution_mode": "embedded",
        })
    );
}
