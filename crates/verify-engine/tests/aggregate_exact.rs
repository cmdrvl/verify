//! bd-vhs: `sum` and `avg` are exact over the values received, so row order cannot change a
//! verdict. A binary float means the shortest decimal a report prints for it. Synthetic data.

use std::collections::BTreeMap;

use serde_json::{Value, json};
use verify_core::{
    constraint::{Aggregate, Check, Comparison, Portability, Rule, Severity},
    report::ResultStatus,
};
use verify_engine::{Relation, portable_relation};

/// `PASS`, `FAIL <observed as printed>`, or `REFUSED <message>`.
fn run(function: &str, values: &[Value], compare: Value) -> String {
    let mut aggregate = Aggregate::default();
    match function {
        "sum" => aggregate.sum = Some("v".to_owned()),
        _ => aggregate.avg = Some("v".to_owned()),
    }
    let compare: Comparison = serde_json::from_value(compare).expect("comparison parses");
    let rule = Rule {
        id: "AGG".to_owned(),
        severity: Severity::Error,
        portability: Portability::Portable,
        check: Check::AggregateCompare {
            binding: "input".to_owned(),
            aggregate,
            compare,
        },
    };
    let rows = values
        .iter()
        .map(|value| BTreeMap::from([("v".to_owned(), value.clone())]))
        .collect();
    let relations = BTreeMap::from([("input".to_owned(), Relation::new(Vec::new(), rows))]);
    match portable_relation::evaluate_rule(&rule, &relations) {
        Err(error) => format!("REFUSED {error}"),
        Ok(result) if result.status == ResultStatus::Pass => "PASS".to_owned(),
        Ok(result) => format!(
            "FAIL {}",
            result.affected[0].value.as_ref().expect("observed value")
        ),
    }
}

fn permutations(values: &[Value]) -> Vec<Vec<Value>> {
    if values.len() <= 1 {
        return vec![values.to_vec()];
    }
    (0..values.len())
        .flat_map(|first| {
            let mut rest = values.to_vec();
            let head = rest.remove(first);
            permutations(&rest).into_iter().map(move |mut tail| {
                tail.insert(0, head.clone());
                tail
            })
        })
        .collect()
}

#[test]
fn row_order_never_changes_the_outcome() {
    let sets = [
        vec![json!(1e16), json!(1), json!(-1e16)],
        vec![json!(1e15), json!(1.5), json!(-1e15), json!(0.1)],
        vec![json!(0.1), json!(0.2), json!(0.3), json!(0.4), json!(0.7)],
        vec![json!(u64::MAX), json!(1), json!(-1), json!(i64::MIN)],
        vec![json!(5e-324), json!(1.0), json!(-1.0), json!(-5e-324)],
        vec![json!(1.0e15), json!(-0.0), json!(0.000001), json!(-1.0e15)],
    ];
    for values in sets {
        for function in ["sum", "avg"] {
            for threshold in [json!(0), json!(1), json!(0.3), json!(1.7), json!(0.000001)] {
                for operator in ["eq", "gt", "lt"] {
                    let compare = json!({ operator: threshold });
                    let outcomes: std::collections::BTreeSet<String> = permutations(&values)
                        .iter()
                        .map(|rows| run(function, rows, compare.clone()))
                        .collect();
                    assert_eq!(
                        outcomes.len(),
                        1,
                        "{function} {operator} {threshold} over {values:?}: {outcomes:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn decimals_add_as_written() {
    let pennies = [json!(0.1), json!(0.2)];
    assert_eq!(run("sum", &pennies, json!({"eq": 0.3})), "PASS");
    assert_eq!(run("sum", &pennies, json!({"gt": 0.3})), "FAIL 0.3");
    assert_eq!(run("sum", &pennies, json!({"lte": 0.3})), "PASS");
    assert_eq!(run("avg", &pennies, json!({"eq": 0.15})), "PASS");
    let money = [json!(100.10), json!(200.20), json!(300.30)];
    assert_eq!(run("sum", &money, json!({"eq": 600.6})), "PASS");
    assert_eq!(run("sum", &money, json!({"ne": 600.6})), "FAIL 600.6");
    // |3.3 - 3.0| is exactly the tolerance.
    let parts = [json!(1.1), json!(2.2)];
    assert_eq!(
        run("sum", &parts, json!({"eq": 3.0, "tolerance": 0.3})),
        "PASS"
    );
    assert_eq!(
        run("sum", &parts, json!({"eq": 3.0, "tolerance": 0.29999999})),
        "FAIL 3.3"
    );
    assert_eq!(
        run("avg", &parts, json!({"eq": 1.5, "tolerance": 0.15})),
        "PASS"
    );
}

#[test]
fn integer_sums_are_not_rounded() {
    let wide = [json!(9_007_199_254_740_992_u64), json!(1)];
    assert_eq!(
        run("sum", &wide, json!({"eq": 9_007_199_254_740_992_u64})),
        "FAIL 9007199254740993"
    );
    assert_eq!(
        run("sum", &wide, json!({"eq": 9_007_199_254_740_993_u64})),
        "PASS"
    );
    assert_eq!(
        run(
            "avg",
            &wide,
            json!({"gt": 4_503_599_627_370_496_u64, "lt": 4_503_599_627_370_497_u64})
        ),
        "PASS"
    );
    let beyond = [json!(u64::MAX), json!(1)];
    assert_eq!(run("sum", &beyond, json!({"gt": u64::MAX})), "PASS");
}

/// The observed value keeps the form reports already use wherever an `f64` holds it.
#[test]
fn reported_values_keep_their_form() {
    let integers = [json!(9), json!(10)];
    assert_eq!(run("sum", &integers, json!({"lt": 0})), "FAIL 19.0");
    assert_eq!(run("avg", &integers, json!({"lt": 0})), "FAIL 9.5");
    assert_eq!(run("sum", &[], json!({"lt": 0})), "FAIL 0.0");
    assert_eq!(run("sum", &[json!(-2.5)], json!({"gt": 0})), "FAIL -2.5");
    let thirds = [json!(1), json!(0), json!(0)];
    assert_eq!(
        run("avg", &thirds, json!({"lt": 0})),
        "FAIL 0.3333333333333333"
    );
}

/// Interim (bd-1t5): a float of magnitude 2^53 or more may already be a rounded integer, so
/// an exact sum of it could pass something false. It refuses, in every row order.
#[test]
fn wide_floats_refuse_until_loading_is_exact() {
    for values in [
        vec![json!(1e16), json!(1), json!(-1e16)],
        vec![json!(1), json!(9_007_199_254_740_992.0)],
    ] {
        for rows in permutations(&values) {
            let outcome = run("sum", &rows, json!({"eq": 1}));
            assert!(outcome.contains("interim"), "{outcome}");
        }
    }
    for compare in [json!({"lt": 1e16}), json!({"eq": 1, "tolerance": 1e16})] {
        let outcome = run("sum", &[json!(1)], compare);
        assert!(outcome.contains("interim"), "{outcome}");
    }
    // Just below the limit is exact.
    assert_eq!(
        run(
            "sum",
            &[json!(9_007_199_254_740_991.0), json!(1)],
            json!({"eq": 9_007_199_254_740_992_u64})
        ),
        "PASS"
    );
}

#[test]
fn the_refusal_chosen_does_not_depend_on_row_order() {
    let values = vec![json!("text"), json!(1e16), json!(null)];
    let outcomes: std::collections::BTreeSet<String> = permutations(&values)
        .iter()
        .map(|rows| run("sum", rows, json!({"eq": 1})))
        .collect();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.contains("numeric values")),
        "{outcomes:?}"
    );
}
