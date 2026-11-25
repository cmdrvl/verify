//! Excel worksheet bindings (bd-l2l). The same dataset in CSV, Parquet and XLSX
//! (fixtures/inputs/xlsx/sample.*, copied from cmdrvl-tabular's conformance fixtures) must give
//! identical rule results, and a workbook with several sheets must refuse until one is selected.

use std::{
    path::Path,
    process::{Command, Output},
};

use serde_json::{Value, json};

const ROOT: &str = env!("CARGO_MANIFEST_DIR");

fn fixture(name: &str) -> String {
    format!("{ROOT}/../../fixtures/inputs/xlsx/{name}")
}

fn run(constraints: &Path, bind: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_verify"))
        .env_remove("VERIFY_REDACTION_KEY")
        .arg("run")
        .arg(constraints)
        .args(["--bind", bind, "--json", "--explicit", "--no-witness"])
        .output()
        .expect("verify run should execute")
}

fn report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        json!({"unparsed": String::from_utf8_lossy(&output.stdout), "stderr": String::from_utf8_lossy(&output.stderr)})
    })
}

fn write_constraints(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("rules.verify.json");
    std::fs::write(
        &path,
        json!({
            "version": "verify.constraint.v1",
            "constraint_set_id": "fixtures.xlsx.parity",
            "bindings": [{"name": "input", "kind": "relation", "key_fields": ["id"]}],
            "rules": [
                {"id": "ID_UNIQUE", "severity": "error", "portability": "portable",
                 "check": {"op": "unique", "binding": "input", "columns": ["id"]}},
                {"id": "AMOUNT_OVER_ONE", "severity": "error", "portability": "portable",
                 "check": {"op": "predicate", "binding": "input",
                           "expr": {"gt": [{"column": "amount"}, 1]}}},
                {"id": "SMALL_AMOUNTS", "severity": "warn", "portability": "batch_only",
                 "check": {"op": "query_zero_rows", "bindings": ["input"],
                           "query": "SELECT id AS key__id, region AS value FROM input WHERE amount < 1"}}
            ]
        })
        .to_string(),
    )
    .expect("write constraints");
    path
}

struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "verify-xlsx-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn identical_rules_give_identical_results_over_csv_parquet_and_xlsx() {
    let scratch = Scratch::new("parity");
    let constraints = write_constraints(&scratch.0);
    let csv = run(&constraints, &format!("input={}", fixture("sample.csv")));
    let parquet = run(
        &constraints,
        &format!("input={}", fixture("sample.parquet")),
    );
    let xlsx = run(
        &constraints,
        &format!("input={}#loans", fixture("sample.xlsx")),
    );

    for (label, output) in [("csv", &csv), ("parquet", &parquet), ("xlsx", &xlsx)] {
        assert_eq!(
            output.status.code(),
            Some(1),
            "{label}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let (csv, parquet, xlsx) = (report(&csv), report(&parquet), report(&xlsx));
    // The fixture really fails the rules, so the comparison is not vacuous.
    let violations = |rule: &str| {
        csv["results"]
            .as_array()
            .and_then(|results| results.iter().find(|r| r["rule_id"] == rule))
            .and_then(|r| r["violation_count"].as_u64())
    };
    assert!(violations("AMOUNT_OVER_ONE").unwrap_or(0) > 0, "{csv}");
    assert!(violations("SMALL_AMOUNTS").unwrap_or(0) > 0, "{csv}");
    assert_eq!(violations("ID_UNIQUE"), Some(0), "{csv}");

    assert_eq!(csv["results"], parquet["results"], "csv vs parquet");
    assert_eq!(csv["results"], xlsx["results"], "csv vs xlsx");
    assert_eq!(csv["outcome"], xlsx["outcome"]);
    assert_eq!(csv["summary"], xlsx["summary"]);
}

#[test]
fn a_multi_sheet_workbook_without_a_selector_refuses_deterministically() {
    let scratch = Scratch::new("ambiguous");
    let constraints = write_constraints(&scratch.0);
    let bind = format!("input={}", fixture("sample.xlsx"));
    let first = run(&constraints, &bind);
    assert_eq!(first.status.code(), Some(2));
    let refusal = report(&first);
    assert_eq!(refusal["refusal"]["code"], "E_FORMAT_DETECT");
    let detail = refusal["refusal"]["detail"]["detail"]
        .as_str()
        .unwrap_or_default();
    assert!(detail.contains("E_AMBIGUOUS_SHEET"), "{detail}");
    assert!(
        detail.contains("loans") && detail.contains("notes"),
        "{detail}"
    );
    // Deterministic: the same refusal byte for byte.
    assert_eq!(first.stdout, run(&constraints, &bind).stdout);
}

#[test]
fn an_unknown_sheet_and_a_selector_on_a_non_workbook_refuse() {
    let scratch = Scratch::new("selectors");
    let constraints = write_constraints(&scratch.0);

    let unknown = run(
        &constraints,
        &format!("input={}#nope", fixture("sample.xlsx")),
    );
    assert_eq!(unknown.status.code(), Some(2));
    let refusal = report(&unknown);
    assert_eq!(refusal["refusal"]["code"], "E_FORMAT_DETECT");
    assert!(
        refusal["refusal"]["detail"]["detail"]
            .as_str()
            .is_some_and(|text| text.contains("E_SHEET_NOT_FOUND")),
        "{refusal}"
    );

    // `#Sheet` after a CSV is not a selector: the path is used as written and does not exist.
    let csv = run(
        &constraints,
        &format!("input={}#loans", fixture("sample.csv")),
    );
    assert_eq!(csv.status.code(), Some(2));
    assert_eq!(report(&csv)["refusal"]["code"], "E_IO");
}

#[test]
fn a_sheet_can_be_selected_by_index_and_other_sheets_are_independent() {
    let scratch = Scratch::new("index");
    let constraints = write_constraints(&scratch.0);
    let by_name = report(&run(
        &constraints,
        &format!("input={}#loans", fixture("sample.xlsx")),
    ));
    let by_index = report(&run(
        &constraints,
        &format!("input={}#0", fixture("sample.xlsx")),
    ));
    assert_eq!(by_name["results"], by_index["results"]);
    // The `notes` sheet has different columns, so the same rules cannot run on it.
    let notes = run(
        &constraints,
        &format!("input={}#notes", fixture("sample.xlsx")),
    );
    assert_eq!(
        notes.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&notes.stdout)
    );
}

#[test]
fn xlsx_default_output_is_redacted_like_every_other_format() {
    let scratch = Scratch::new("redact");
    let constraints = write_constraints(&scratch.0);
    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .env_remove("VERIFY_REDACTION_KEY")
        .arg("run")
        .arg(&constraints)
        .args([
            "--bind",
            &format!("input={}#loans", fixture("sample.xlsx")),
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("verify run");
    assert_eq!(output.status.code(), Some(1));
    let text = String::from_utf8_lossy(&output.stdout);
    for raw in ["East", "West", "North"] {
        assert!(!text.contains(raw), "raw region {raw} leaked: {text}");
    }
    assert!(text.contains("redacted:ref:"), "{text}");
}
