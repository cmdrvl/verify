//! bd-c3y: a DECIMAL beyond 96 bits or scale 28 ended the process with no report.
//!
//! Every case is a real Parquet binding read by the built binary.

use std::{error::Error, path::Path, process::Command};

use duckdb::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// Writes rows `(id, v, w)` to Parquet; `values` are SQL expressions for `v`.
fn parquet(dir: &Path, name: &str, kind: &str, values: &[String]) -> TestResult<String> {
    let path = dir.join(format!("{name}.parquet"));
    let connection = Connection::open_in_memory()?;
    let rows = values
        .iter()
        .enumerate()
        .map(|(row, value)| format!("('r{row}', CAST({value} AS {kind}), {row})"))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE TABLE t (id VARCHAR, v {kind}, w INTEGER); INSERT INTO t VALUES {rows}; \
         COPY t TO '{}' (FORMAT PARQUET);",
        path.display()
    ))?;
    Ok(path.to_string_lossy().into_owned())
}

fn binding(name: &str, key: &str) -> Value {
    json!({"name": name, "kind": "relation", "key_fields": [key]})
}

/// One rule over `input` (and `other`, bound to the same file).
fn run(
    dir: &Path,
    data: &str,
    bindings: Value,
    portability: &str,
    check: Value,
) -> TestResult<Run> {
    let artifact = dir.join("rule.verify.json");
    std::fs::write(
        &artifact,
        serde_json::to_vec(&json!({
            "version": "verify.constraint.v1",
            "constraint_set_id": "tests.decimal_limits",
            "bindings": bindings.clone(),
            "rules": [{"id": "r", "severity": "error", "portability": portability, "check": check}],
        }))?,
    )?;
    // ubs:ignore — the binary under test.
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    command.arg("run").arg(&artifact);
    for declared in bindings.as_array().expect("bindings") {
        let name = declared["name"].as_str().expect("name");
        command.arg("--bind").arg(format!("{name}={data}"));
    }
    let output = command
        .args(["--json", "--explicit", "--no-witness"])
        .output()?;
    Ok(Run {
        code: output.status.code(),
        report: serde_json::from_slice(&output.stdout).ok(),
    })
}

#[derive(Debug)]
struct Run {
    code: Option<i32>,
    report: Option<Value>,
}

impl Run {
    fn report(&self) -> &Value {
        self.report.as_ref().expect("a report")
    }

    fn refusal(&self) -> &Value {
        &self.report()["refusal"]
    }

    fn status(&self) -> &str {
        self.report()["results"][0]["status"].as_str().unwrap_or("")
    }

    fn first_value(&self) -> &Value {
        &self.report()["results"][0]["affected"][0]["value"]
    }
}

fn one() -> Value {
    json!([binding("input", "id")])
}

fn two() -> Value {
    json!([binding("input", "id"), binding("other", "id")])
}

/// Every route by which a rule can make verify read column `v`.
fn paths() -> Vec<(&'static str, Value, &'static str, Value)> {
    let same = |column: &str| {
        json!({"op": "predicate", "binding": "input",
        "expr": {"eq": [{"column": column}, {"binding": "other", "column": column}]}})
    };
    vec![
        (
            "not_null",
            one(),
            "portable",
            json!({"op": "not_null", "binding": "input", "columns": ["v"]}),
        ),
        (
            "predicate",
            one(),
            "portable",
            json!({"op": "predicate", "binding": "input",
            "expr": {"gt": [{"column": "v"}, -1]}}),
        ),
        (
            "aggregate",
            one(),
            "portable",
            json!({"op": "aggregate_compare", "binding": "input",
            "aggregate": {"max": "v"}, "compare": {"gt": -1}}),
        ),
        (
            "unique",
            one(),
            "portable",
            json!({"op": "unique", "binding": "input", "columns": ["v"]}),
        ),
        (
            "foreign_key",
            two(),
            "portable",
            json!({"op": "foreign_key", "binding": "input",
            "columns": ["v"], "ref_binding": "other", "ref_columns": ["v"]}),
        ),
        (
            "portable key",
            json!([binding("input", "v")]),
            "portable",
            json!({"op": "not_null", "binding": "input", "columns": ["id"]}),
        ),
        ("qualified operand", two(), "batch_only", same("v")),
        (
            "qualified key",
            json!([binding("input", "v"), binding("other", "v")]),
            "batch_only",
            same("w"),
        ),
        ("query result", one(), "batch_only", query_check("v")),
    ]
}

fn query_check(expression: &str) -> Value {
    json!({"op": "query_zero_rows", "bindings": ["input"], "query": format!(
        "SELECT 'input' AS binding, 'v' AS field, {expression} AS value, id AS key__id FROM input")})
}

fn path_named(name: &str) -> (Value, &'static str, Value) {
    let (_, bindings, portability, check) = paths()
        .into_iter()
        .find(|path| path.0 == name)
        .expect("known path");
    (bindings, portability, check)
}

fn on_path(dir: &Path, data: &str, name: &str) -> TestResult<Run> {
    let (bindings, portability, check) = path_named(name);
    run(dir, data, bindings, portability, check)
}

const BEYOND: &[(&str, &str, &str)] = &[
    (
        "over_96_bits",
        "DECIMAL(38,1)",
        "'9999999999999999999999999999.5'",
    ),
    (
        "negative_over_96_bits",
        "DECIMAL(38,1)",
        "'-9999999999999999999999999999.5'",
    ),
    (
        "just_over_96_bits",
        "DECIMAL(38,1)",
        "'7922816251426433759354395033.6'",
    ),
    ("half_scale_29", "DECIMAL(38,29)", "'0.5'"),
    ("half_scale_30", "DECIMAL(38,30)", "'0.5'"),
    ("list", "DECIMAL(38,30)[]", "['0.5']"),
    ("struct", "STRUCT(a DECIMAL(38,30))", "{'a': '0.5'}"),
    ("array", "DECIMAL(38,30)[1]", "['0.5']"),
    (
        "map_value",
        "MAP(VARCHAR, DECIMAL(38,30))",
        "map {'k': '0.5'}",
    ),
    (
        "map_key",
        "MAP(DECIMAL(38,30), VARCHAR)",
        "map {'0.5': 'k'}",
    ),
];

#[test]
fn no_value_beyond_the_limits_ends_the_process() -> TestResult {
    let dir = TempDir::new()?;
    let mut aborted = Vec::new();
    for (name, kind, value) in BEYOND {
        let data = parquet(dir.path(), name, kind, &[(*value).to_owned()])?;
        for (label, bindings, portability, check) in paths() {
            let outcome = run(dir.path(), &data, bindings, portability, check)?;
            if !matches!(outcome.code, Some(0..=2)) || outcome.report.is_none() {
                aborted.push(format!("{name} / {label}: {:?}", outcome.code));
            }
        }
    }
    // A union cannot be written to Parquet; a query result is the only way to meet one.
    let data = parquet(dir.path(), "plain", "INTEGER", &["1".to_owned()])?;
    let union = "CAST(union_value(n := CAST('0.5' AS DECIMAL(38,30))) AS UNION(n DECIMAL(38,30), s VARCHAR))";
    let outcome = run(dir.path(), &data, one(), "batch_only", query_check(union))?;
    if outcome.report.is_none() {
        aborted.push(format!("union / query result: {:?}", outcome.code));
    }
    assert!(
        aborted.is_empty(),
        "{} ended without a report: {aborted:#?}",
        aborted.len()
    );
    Ok(())
}

#[test]
fn a_value_that_loses_no_digit_is_read_as_that_number() -> TestResult {
    let dir = TempDir::new()?;
    for (name, kind, value, number) in [
        ("half", "DECIMAL(38,30)", "'0.5'", "0.5"),
        ("tenth", "DECIMAL(38,38)", "'0.1'", "0.1"),
        (
            "wide_integer",
            "DECIMAL(38,1)",
            "'10000000000000000000000000000.0'",
            "1e28",
        ),
        ("zero", "DECIMAL(38,29)", "'0'", "0.0"),
        ("negative", "DECIMAL(38,30)", "'-0.25'", "-0.25"),
    ] {
        let data = parquet(dir.path(), name, kind, &[value.to_owned()])?;
        let literal: Value = serde_json::from_str(number)?;
        let equal = json!({"op": "predicate", "binding": "input",
            "expr": {"eq": [{"column": "v"}, literal]}});
        let outcome = run(dir.path(), &data, one(), "portable", equal)?;
        assert_eq!(outcome.status(), "pass", "{name}: {outcome:?}");
        let shown = on_path(dir.path(), &data, "query result")?;
        assert_eq!(
            shown.first_value().as_f64(),
            literal.as_f64(),
            "{name}: {shown:?}"
        );
        for path in [
            "unique",
            "foreign_key",
            "portable key",
            "qualified operand",
            "qualified key",
        ] {
            let outcome = on_path(dir.path(), &data, path)?;
            assert_eq!(outcome.status(), "pass", "{name} / {path}: {outcome:?}");
        }
    }
    Ok(())
}

#[test]
fn a_value_that_would_lose_a_digit_is_refused_with_the_existing_code() -> TestResult {
    let dir = TempDir::new()?;
    let text = "9999999999999999999999999999.5";
    let data = parquet(dir.path(), "lossy", "DECIMAL(38,1)", &[format!("'{text}'")])?;
    for (path, code) in [
        ("predicate", "E_SQL_ERROR"),
        ("unique", "E_SQL_ERROR"),
        ("portable key", "E_SQL_ERROR"),
        ("qualified operand", "E_BAD_EXPR"),
        ("qualified key", "E_KEY_INVALID"),
    ] {
        let outcome = on_path(dir.path(), &data, path)?;
        assert_eq!(outcome.refusal()["code"], code, "{path}: {outcome:?}");
        let detail = outcome.refusal()["detail"].to_string();
        assert!(detail.contains("interim limit"), "{path}: {detail}");
    }
    let shown = on_path(dir.path(), &data, "query result")?;
    assert_eq!(shown.first_value(), &json!(text), "{shown:?}");
    Ok(())
}

#[test]
fn nested_values_and_nulls_behave_as_inside_the_limits() -> TestResult {
    let dir = TempDir::new()?;
    for (name, kind, value) in [
        ("list", "DECIMAL(38,$s)[]", "['0.5']"),
        ("struct", "STRUCT(a DECIMAL(38,$s))", "{'a': '0.5'}"),
        ("array", "DECIMAL(38,$s)[1]", "['0.5']"),
        ("map", "MAP(VARCHAR, DECIMAL(38,$s))", "map {'k': '0.5'}"),
        ("null_list", "DECIMAL(38,$s)[]", "NULL"),
        ("null_scalar", "DECIMAL(38,$s)", "NULL"),
    ] {
        let inside = parquet(
            dir.path(),
            &format!("{name}_28"),
            &kind.replace("$s", "28"),
            &[value.to_owned()],
        )?;
        let beyond = parquet(
            dir.path(),
            &format!("{name}_30"),
            &kind.replace("$s", "30"),
            &[value.to_owned()],
        )?;
        // The query path renders contents; every rule path decides by type and nullness.
        for (path, ..) in paths().into_iter().filter(|path| path.0 != "query result") {
            let a = on_path(dir.path(), &inside, path)?;
            let b = on_path(dir.path(), &beyond, path)?;
            assert_eq!(a.code, b.code, "{name} / {path}");
            // A refusal made from the column type names it, scale included.
            let inside_refusal = a.report()["refusal"]
                .to_string()
                .replace("decimal(38,28)", "decimal(38,30)");
            assert_eq!(
                serde_json::from_str::<Value>(&inside_refusal)?,
                b.report()["refusal"],
                "{name} / {path}"
            );
            assert_eq!(
                a.report()["results"],
                b.report()["results"],
                "{name} / {path}"
            );
        }
    }
    Ok(())
}

/// Doubles near 1.5e15 are 0.25 apart, so `.2` and `.3` (and `.7` and `.8`) are
/// two equally short texts for one double. Formatters break that tie differently,
/// and only the text the report prints may be accepted as losing no digit.
#[test]
fn a_halfway_value_is_never_reported_with_another_digit() -> TestResult {
    let dir = TempDir::new()?;
    let data = parquet(dir.path(), "plain", "INTEGER", &["1".to_owned()])?;
    let texts: Vec<String> = (0..32_u64)
        .flat_map(|n| {
            [2, 3, 7, 8]
                .map(|tenth| format!("{}.{tenth}{}", 1_536_945_785_199_992 + n, "0".repeat(17)))
        })
        .collect();
    let query = texts
        .iter()
        .enumerate()
        .map(|(index, text)| {
            format!(
                "SELECT 'input' AS binding, 'v' AS field, CAST('{text}' AS DECIMAL(38,18)) AS value, \
                 '{index:04}' AS key__id"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let check = json!({"op": "query_zero_rows", "bindings": ["input"], "query": query});
    let outcome = run(dir.path(), &data, one(), "batch_only", check)?;
    let affected = outcome.report()["results"][0]["affected"]
        .as_array()
        .expect("affected")
        .clone();
    assert_eq!(affected.len(), texts.len());
    let mut changed = Vec::new();
    let mut as_number = 0;
    for entry in affected {
        let index: usize = entry["key"]["id"].as_str().expect("id").parse()?;
        let text = &texts[index];
        let kept = match &entry["value"] {
            // Reading the report and printing the number again gives the report's text.
            Value::Number(number) => {
                as_number += 1;
                digits_and_exponent(&number.to_string()) == digits_and_exponent(text)
            }
            Value::String(shown) => shown == text,
            _ => false,
        };
        if !kept {
            changed.push(format!("stored {text}, reported {}", entry["value"]));
        }
    }
    assert!(
        changed.is_empty(),
        "{} values changed: {:#?}",
        changed.len(),
        &changed[..changed.len().min(8)]
    );
    assert!(as_number > 0, "no halfway value was read as a number");
    Ok(())
}

/// DuckDB's own text-to-double and double-to-text, which share no code with
/// serde_json or rust_decimal: no digit is lost when the shortest text of the
/// nearest double is the stored decimal.
fn duckdb_says_lossless(connection: &Connection, text: &str) -> TestResult<bool> {
    let shortest: String = connection.query_row(
        &format!("SELECT CAST(CAST('{text}' AS DOUBLE) AS VARCHAR)"),
        [],
        |row| row.get(0),
    )?;
    Ok(digits_and_exponent(text) == digits_and_exponent(&shortest))
}

/// `(negative, significant digits, power of ten of the last digit)`.
fn digits_and_exponent(text: &str) -> (bool, String, i64) {
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    let negative = mantissa.starts_with('-');
    let (whole, fraction) = mantissa
        .trim_start_matches('-')
        .split_once('.')
        .unwrap_or((mantissa.trim_start_matches('-'), ""));
    let mut digits = format!("{whole}{fraction}")
        .trim_start_matches('0')
        .to_owned();
    let mut power = exponent.parse::<i64>().expect("exponent") - fraction.len() as i64;
    while digits.ends_with('0') {
        digits.pop();
        power += 1;
    }
    if digits.is_empty() {
        return (false, digits, 0);
    }
    (negative, digits, power)
}

struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[test]
fn seeded_values_beyond_the_limits_agree_with_an_independent_oracle() -> TestResult {
    let dir = TempDir::new()?;
    let oracle = Connection::open_in_memory()?;
    let mut rng = SplitMix(0xC3);
    // One query result holds all values, so each one is judged on its own.
    let mut selects = Vec::new();
    let mut texts = Vec::new();
    for index in 0..600 {
        // Few significant digits (mostly lossless) or many (mostly not).
        let significant = if index % 2 == 0 {
            1 + rng.below(16)
        } else {
            17 + rng.below(20)
        };
        let mut digits = (1 + rng.below(9)).to_string();
        while (digits.len() as u64) < significant {
            digits.push_str(&rng.below(10).to_string());
        }
        let scale = 29 + rng.below(10) as usize;
        let zeros = rng.below((38 - digits.len().min(37)) as u64) as usize;
        let unscaled = format!("{digits}{}", "0".repeat(zeros.min(38 - digits.len())));
        let padded = format!("{unscaled:0>width$}", width = scale + 1);
        let (whole, fraction) = padded.split_at(padded.len() - scale);
        let sign = if rng.below(4) == 0 { "-" } else { "" };
        let text = format!("{sign}{whole}.{fraction}");
        selects.push(format!(
            "SELECT 'input' AS binding, 'v' AS field, CAST('{text}' AS DECIMAL(38,{scale})) AS value, \
             '{index:04}' AS key__id"
        ));
        texts.push(text);
    }
    let data = parquet(dir.path(), "plain", "INTEGER", &["1".to_owned()])?;
    // UNION ALL would unify the scales, so each scale needs its own rule.
    let mut wrong = Vec::new();
    let (mut as_number, mut as_text) = (0, 0);
    for scale in 29..39 {
        let marker = format!("DECIMAL(38,{scale}))");
        let group: Vec<usize> = (0..texts.len())
            .filter(|i| selects[*i].contains(&marker))
            .collect();
        let query = group
            .iter()
            .map(|i| selects[*i].clone())
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let check = json!({"op": "query_zero_rows", "bindings": ["input"], "query": query});
        let outcome = run(dir.path(), &data, one(), "batch_only", check)?;
        let affected = outcome.report()["results"][0]["affected"]
            .as_array()
            .expect("affected")
            .clone();
        assert_eq!(affected.len(), group.len(), "scale {scale}");
        for entry in affected {
            let index: usize = entry["key"]["id"].as_str().expect("id").parse()?;
            let text = &texts[index];
            let lossless = duckdb_says_lossless(&oracle, text)?;
            let right = match &entry["value"] {
                Value::Number(number) => {
                    as_number += 1;
                    let expected: f64 = text.parse()?;
                    lossless
                        && number.as_f64().map(f64::to_bits) == Some(expected.to_bits())
                        && digits_and_exponent(&number.to_string()) == digits_and_exponent(text)
                }
                Value::String(shown) => {
                    as_text += 1;
                    !lossless && shown == text
                }
                _ => false,
            };
            if !right {
                wrong.push(format!(
                    "{text}: {} (oracle lossless {lossless})",
                    entry["value"]
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong: {:#?}",
        wrong.len(),
        &wrong[..wrong.len().min(8)]
    );
    assert!(
        as_number > 100 && as_text > 100,
        "numbers {as_number}, text {as_text}"
    );
    Ok(())
}
