//! A number in a rule and the same text in the data are the same number (bd-u2j).
//!
//! The oracle is `str::parse::<f64>`, which is correctly rounded and does not go through
//! `serde_json`. Every case is seeded, and a failure prints the text that went wrong.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

use duckdb::Connection;
use serde_json::{Value, json};
use tempfile::tempdir;
use verify_core::constraint::ConstraintSet;
use verify_engine::{
    Relation,
    embedded::{EmbeddedBinding, EmbeddedBindings, EmbeddedExecutor},
};

/// SplitMix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// `count` decimal digits that neither start nor end with zero.
fn significand(rng: &mut Rng, count: u64) -> String {
    (0..count)
        .map(|index| {
            let digit = if index == 0 || index + 1 == count {
                1 + rng.below(9)
            } else {
                rng.below(10)
            };
            char::from(b'0' + u8::try_from(digit).expect("one digit"))
        })
        .collect()
}

/// A fraction, so no loader reads it as an integer: long ones, and short ones with exponents
/// far enough from zero that scaling the digits is not exact.
fn number_text(rng: &mut Rng) -> String {
    let sign = if rng.below(4) == 0 { "-" } else { "" };
    let (digits, exponent) = match rng.below(3) {
        0 => (16 + rng.below(4), None),
        1 => (20 + rng.below(12), None),
        _ => {
            let digits = 2 + rng.below(18);
            let magnitude = 23 + rng.below(260);
            let direction = if rng.below(2) == 0 { "-" } else { "" };
            (digits, Some(format!("e{direction}{magnitude}")))
        }
    };
    let digits = significand(rng, digits);
    let point = 1 + usize::try_from(rng.below(digits.len() as u64 - 1)).expect("fits");
    let (whole, fraction) = digits.split_at(point);
    format!("{sign}{whole}.{fraction}{}", exponent.unwrap_or_default())
}

fn seeded_texts(seed: u64, count: usize) -> Vec<String> {
    let mut rng = Rng(seed);
    (0..count).map(|_| number_text(&mut rng)).collect()
}

fn nearest(text: &str) -> f64 {
    text.parse().expect("generated number parses")
}

/// (operator, whether it holds between two equal numbers)
const OPERATORS: [(&str, bool); 6] = [
    ("eq", true),
    ("ne", false),
    ("gt", false),
    ("gte", true),
    ("lt", false),
    ("lte", true),
];

/// One rule per value and operator: the value's column against the identical text. Aggregate
/// thresholds are covered through `max` over the one-row column.
fn compiled_artifact(texts: &[String]) -> String {
    let mut rules = Vec::new();
    for (index, text) in texts.iter().enumerate() {
        for (operator, _) in OPERATORS {
            rules.push(format!(
                r#"{{"id":"P-{index}-{operator}","severity":"error","portability":"portable","check":{{"op":"predicate","binding":"input","expr":{{"{operator}":[{{"column":"c{index}"}},{text}]}}}}}}"#
            ));
            rules.push(format!(
                r#"{{"id":"T-{index}-{operator}","severity":"error","portability":"portable","check":{{"op":"aggregate_compare","binding":"input","aggregate":{{"max":"c{index}"}},"compare":{{"{operator}":{text}}}}}}}"#
            ));
        }
    }
    format!(
        r#"{{"version":"verify.constraint.v1","constraint_set_id":"tests.literals","bindings":[{{"name":"input","kind":"relation","key_fields":["id"]}}],"rules":[{}]}}"#,
        rules.join(",")
    )
}

fn yaml_authoring(texts: &[String]) -> String {
    let mut yaml =
        "constraint_set_id: tests.literals\nbindings:\n  input:\n    key_fields: [id]\nrules:\n"
            .to_owned();
    for (index, text) in texts.iter().enumerate() {
        for (operator, _) in OPERATORS {
            yaml.push_str(&format!(
                "  - {{id: P-{index}-{operator}, severity: error, binding: input, op: predicate, expr: {{{operator}: [{{column: c{index}}}, {text}]}}}}\n  - {{id: T-{index}-{operator}, severity: error, binding: input, op: aggregate_compare, aggregate: {{max: c{index}}}, compare: {{{operator}: {text}}}}}\n"
            ));
        }
    }
    yaml
}

fn write_data(directory: &Path, format: &str, texts: &[String]) -> std::path::PathBuf {
    let path = directory.join(format!("input.{format}"));
    let names: Vec<String> = (0..texts.len()).map(|index| format!("c{index}")).collect();
    let content = if format == "csv" {
        format!("id,{}\nr0,{}\n", names.join(","), texts.join(","))
    } else {
        let cells: Vec<String> = names
            .iter()
            .zip(texts)
            .map(|(name, text)| format!(r#""{name}":{text}"#))
            .collect();
        format!("{{\"id\":\"r0\",{}}}\n", cells.join(","))
    };
    fs::write(&path, content).expect("data written");
    path
}

fn verify(arguments: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verify")) // ubs:ignore — the binary under test.
        .args(arguments)
        .output()
        .expect("verify runs")
}

/// Every rule whose verdict is not the one equal numbers give, as `rule id: text`.
fn wrong_rules(report: &Value, texts: &[String]) -> Vec<String> {
    let results = report["results"].as_array();
    assert!(results.is_some(), "no results: {report}");
    let results = results.expect("asserted above");
    assert_eq!(results.len(), texts.len() * OPERATORS.len() * 2);
    results
        .iter()
        .filter_map(|result| {
            let id = result["rule_id"].as_str().expect("rule id");
            let mut parts = id.split('-').skip(1);
            let index: usize = parts.next().expect("index").parse().expect("index");
            let operator = parts.next().expect("operator");
            let holds = OPERATORS
                .iter()
                .any(|(name, holds)| *name == operator && *holds);
            let expected = if holds { "pass" } else { "fail" };
            (result["status"] != expected).then(|| format!("{id}: {}", texts[index]))
        })
        .collect()
}

fn assert_none_wrong(label: &str, wrong: &[String], total: usize) {
    assert!(
        wrong.is_empty(),
        "{label}: {} of {total} verdicts wrong, e.g. {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
}

const VALUES: usize = 150;

#[test]
fn compiled_json_literals_equal_the_identical_text_in_csv_and_jsonl() {
    for (format, seed) in [("csv", 0xC5_u64), ("jsonl", 0x10)] {
        let texts = seeded_texts(seed, VALUES);
        let directory = tempdir().expect("scratch");
        let data = write_data(directory.path(), format, &texts);
        let constraints = directory.path().join("constraints.verify.json");
        fs::write(&constraints, compiled_artifact(&texts)).expect("artifact written");
        let output = verify(&[
            "run".as_ref(),
            constraints.as_os_str(),
            "--bind".as_ref(),
            format!("input={}", data.display()).as_ref(),
            "--json".as_ref(),
            "--no-witness".as_ref(),
        ]);
        let report: Value = serde_json::from_slice(&output.stdout).expect("JSON report");
        assert_none_wrong(
            &format!("{format} seed {seed:#x}"),
            &wrong_rules(&report, &texts),
            VALUES * 12,
        );
    }
}

/// `verify <data> --rules` and `compile` then `run` read the same YAML to the same numbers.
#[test]
fn yaml_literals_equal_the_identical_text_in_one_step_and_compile_then_run() {
    let seed = 0x1A_u64;
    let texts = seeded_texts(seed, VALUES);
    let directory = tempdir().expect("scratch");
    let data = write_data(directory.path(), "csv", &texts);
    let rules = directory.path().join("rules.yaml");
    fs::write(&rules, yaml_authoring(&texts)).expect("rules written");

    let one_step = verify(&[
        data.as_os_str(),
        "--rules".as_ref(),
        rules.as_os_str(),
        "--json".as_ref(),
        "--no-witness".as_ref(),
    ]);
    let one_step: Value = serde_json::from_slice(&one_step.stdout).expect("JSON report");

    let compiled = directory.path().join("compiled.verify.json");
    let output = verify(&[
        "compile".as_ref(),
        rules.as_os_str(),
        "--out".as_ref(),
        compiled.as_os_str(),
    ]);
    assert!(output.status.success(), "compile failed: {output:?}");
    let then_run = verify(&[
        "run".as_ref(),
        compiled.as_os_str(),
        "--bind".as_ref(),
        format!("input={}", data.display()).as_ref(),
        "--json".as_ref(),
        "--no-witness".as_ref(),
    ]);
    let then_run: Value = serde_json::from_slice(&then_run.stdout).expect("JSON report");

    assert_none_wrong(
        &format!("one-step seed {seed:#x}"),
        &wrong_rules(&one_step, &texts),
        VALUES * 12,
    );
    assert_none_wrong(
        &format!("compile then run seed {seed:#x}"),
        &wrong_rules(&then_run, &texts),
        VALUES * 12,
    );
    assert_eq!(one_step["results"], then_run["results"]);
}

/// An embedded caller's artifact is parsed from JSON; its rows here hold the nearest `f64`.
#[test]
fn parsed_artifacts_equal_correctly_rounded_embedded_rows() {
    let seed = 0xE3_u64;
    let texts = seeded_texts(seed, VALUES);
    let constraints: ConstraintSet =
        serde_json::from_str(&compiled_artifact(&texts)).expect("artifact parses");
    let row: BTreeMap<String, Value> = std::iter::once(("id".to_owned(), json!("r0")))
        .chain(
            texts
                .iter()
                .enumerate()
                .map(|(index, text)| (format!("c{index}"), json!(nearest(text)))),
        )
        .collect();
    let bindings: EmbeddedBindings = std::iter::once((
        "input".to_owned(),
        EmbeddedBinding::new(
            "embedded://input",
            "sha256:input",
            Relation::new(vec!["id".to_owned()], vec![row]),
        ),
    ))
    .collect();
    let report = EmbeddedExecutor::evaluate(&constraints, "sha256:x", &bindings);
    let report = serde_json::to_value(report).expect("report");
    assert_none_wrong(
        &format!("embedded seed {seed:#x}"),
        &wrong_rules(&report, &texts),
        VALUES * 12,
    );
}

/// A DECIMAL reaches the engine as the `f64` nearest its text. Only values the DECIMAL
/// conversion can hold are generated: larger ones abort the process today (bd-c3y).
#[test]
fn decimal_cells_convert_to_the_nearest_f64() {
    let connection = Connection::open_in_memory().expect("DuckDB");
    let seed = 0xDE_u64;
    let mut rng = Rng(seed);
    let mut wrong = Vec::new();
    let total = 600;
    for _ in 0..total {
        let digits = 16 + rng.below(12);
        let text = significand(&mut rng, digits);
        let scale = 1 + rng.below(digits - 1);
        let (whole, fraction) = text.split_at(text.len() - usize::try_from(scale).expect("small"));
        let text = format!("{whole}.{fraction}");
        let value: duckdb::types::Value = connection
            .query_row(
                &format!("SELECT CAST('{text}' AS DECIMAL(38,{scale}))"),
                [],
                |row| row.get(0),
            )
            .expect("decimal value");
        let converted = verify_duckdb::duckdb_value_to_protocol(value).expect("converts");
        if converted.as_f64() != Some(nearest(&text)) {
            wrong.push(format!("DECIMAL(38,{scale}) {text} -> {converted}"));
        }
    }
    assert_none_wrong(&format!("decimal seed {seed:#x}"), &wrong, total);
}

/// The reported minimal case.
#[test]
fn a_value_is_equal_to_itself() {
    let directory = tempdir().expect("scratch");
    let data = directory.path().join("input.csv");
    fs::write(&data, "id,v\nr0,985085759598861.9\n").expect("data written");
    for (operator, outcome) in [("eq", "PASS"), ("ne", "FAIL")] {
        let constraints = directory.path().join("constraints.verify.json");
        fs::write(
            &constraints,
            format!(
                r#"{{"version":"verify.constraint.v1","constraint_set_id":"tests.itself","bindings":[{{"name":"input","kind":"relation","key_fields":["id"]}}],"rules":[{{"id":"R","severity":"error","portability":"portable","check":{{"op":"predicate","binding":"input","expr":{{"{operator}":[{{"column":"v"}},985085759598861.9]}}}}}}]}}"#
            ),
        )
        .expect("artifact written");
        let output = verify(&[
            "run".as_ref(),
            constraints.as_os_str(),
            "--bind".as_ref(),
            format!("input={}", data.display()).as_ref(),
            "--json".as_ref(),
            "--no-witness".as_ref(),
        ]);
        let report: Value = serde_json::from_slice(&output.stdout).expect("JSON report");
        assert_eq!(report["outcome"], outcome, "{operator}");
    }
}
