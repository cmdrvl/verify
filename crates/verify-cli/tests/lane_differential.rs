//! Seeded differential tests across verify's two lanes (bd-5ps).
//!
//! In `verify run`, unique, not_null, portable predicate, row_count, aggregate_compare and
//! foreign_key are all decided by verify-engine over rows DuckDB loaded. Comparing that with the
//! embedded executor would compare one evaluator with itself, so each arm here names what differs:
//!
//! * arm P: one predicate AST through the portable evaluator and, with some column references
//!   qualified to an identical `twin` binding, through the DuckDB binding-qualified evaluator.
//!   Both share `verify_engine::scalar`, so a scalar bug is invisible to this arm.
//! * arm S: verify-engine against DuckDB's own semantics, through `query_zero_rows` and a
//!   test-authored SQL transcription of the documented rule.
//! * arm I: the same artifact through the embedded executor and through the `verify run` binary.
//!   One evaluator; only value ingestion differs.
//! * arm T: DuckDB native temporal comparisons against the portable engine over fixed-width ISO
//!   strings.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use duckdb::Connection;
use serde_json::{Value, json};
use tempfile::tempdir;
use verify_core::{
    constraint::{ConstraintSet, Rule},
    report::{Outcome, RuleResult},
    validation::routes_to_portable_engine,
};
use verify_duckdb::{
    BatchBindingInput, BatchBindingLimits, evaluate_binding_predicate, execute_query_rules,
    prepare_batch_context,
};
use verify_engine::{
    Relation,
    embedded::{EmbeddedBinding, EmbeddedBindings, EmbeddedExecutor},
    portable_row,
};

// ---------------------------------------------------------------------------
// Seeded generation
// ---------------------------------------------------------------------------

/// SplitMix64; the case seed alone reproduces a case.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

fn case_seed(arm: u64, case: u64) -> u64 {
    arm.wrapping_mul(0x1_0000_0000).wrapping_add(case)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int,
    Double,
    Decimal,
    Text,
    Bool,
}

impl Kind {
    fn sql_type(self) -> &'static str {
        match self {
            Self::Int => "BIGINT",
            Self::Double => "DOUBLE",
            Self::Decimal => "DECIMAL(38,10)",
            Self::Text => "VARCHAR",
            Self::Bool => "BOOLEAN",
        }
    }

    fn category(self) -> &'static str {
        match self {
            Self::Int | Self::Double | Self::Decimal => "number",
            Self::Text => "string",
            Self::Bool => "boolean",
        }
    }
}

const INTS: &[i64] = &[
    -3,
    -1,
    0,
    1,
    2,
    9,
    10,
    100,
    9_007_199_254_740_991,
    9_007_199_254_740_992,
    9_007_199_254_740_993,
    i64::MAX,
    i64::MIN,
];
const DOUBLES: &[&str] = &[
    "0.0",
    "-0.0",
    "0.25",
    "0.5",
    "1.0",
    "2.0",
    "9.0",
    "10.0",
    "-1.5",
    "100.25",
    "5e-324",
    "1.7976931348623157e308",
];
const DECIMALS: &[&str] = &[
    "0",
    "0.1",
    "0.2",
    "0.3",
    "1.00",
    "9",
    "10",
    "100.25",
    "-1.5",
    "12345678901234567.89",
    "12345678901234567.88",
];
const TEXTS: &[&str] = &[
    "",
    " ",
    "\t\n",
    "\u{a0}",
    "\u{3000}",
    "\u{200b}",
    "a",
    "A",
    " a ",
    "\u{e9}",
    "e\u{301}",
    "\u{1d518}",
    "9",
    "10",
    "007",
    "NaN",
    "Infinity",
    "-0",
    "2024-01-31",
    "null",
    "it's",
];

fn number(text: &str) -> Value {
    serde_json::from_str(text).expect("numeric pool entry is JSON")
}

fn pool_value(rng: &mut Rng, kind: Kind) -> Value {
    match kind {
        Kind::Int => json!(*rng.pick(INTS)),
        Kind::Double => number(rng.pick(DOUBLES)),
        Kind::Decimal => number(rng.pick(DECIMALS)),
        Kind::Text => json!(*rng.pick(TEXTS)),
        Kind::Bool => json!(rng.chance(50)),
    }
}

/// One cell: the exact SQL text DuckDB stores, and the JSON value an embedded caller passes.
#[derive(Debug, Clone)]
struct Cell {
    sql: String,
    json: Value,
    /// The source spelling, before any numeric parsing. Empty for null.
    text: String,
}

fn sql_string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn null_cell() -> Cell {
    Cell {
        sql: "NULL".to_owned(),
        json: Value::Null,
        text: String::new(),
    }
}

fn cell(kind: Kind, text: &str) -> Cell {
    let json = match kind {
        Kind::Text => json!(text),
        Kind::Bool => json!(text == "true"),
        _ => number(text),
    };
    Cell {
        sql: format!("CAST({} AS {})", sql_string(text), kind.sql_type()),
        json,
        text: text.to_owned(),
    }
}

fn random_cell(rng: &mut Rng, kind: Kind) -> Cell {
    match kind {
        Kind::Int => cell(kind, &rng.pick(INTS).to_string()),
        Kind::Double => cell(kind, rng.pick(DOUBLES)),
        Kind::Decimal => cell(kind, rng.pick(DECIMALS)),
        Kind::Text => cell(kind, rng.pick(TEXTS)),
        Kind::Bool => cell(kind, if rng.chance(50) { "true" } else { "false" }),
    }
}

#[derive(Debug, Clone)]
struct Table {
    columns: Vec<(&'static str, Kind)>,
    rows: Vec<Vec<Cell>>,
}

impl Table {
    fn kind(&self, name: &str) -> Kind {
        self.columns
            .iter()
            .find(|(column, _)| *column == name)
            .map(|(_, kind)| *kind)
            .expect("known column")
    }

    fn index(&self, name: &str) -> usize {
        self.columns
            .iter()
            .position(|(column, _)| *column == name)
            .expect("known column")
    }

    fn json_rows(&self) -> Vec<BTreeMap<String, Value>> {
        self.rows
            .iter()
            .map(|row| {
                self.columns
                    .iter()
                    .zip(row)
                    .map(|((name, _), cell)| ((*name).to_owned(), cell.json.clone()))
                    .collect()
            })
            .collect()
    }

    fn write_parquet(&self, path: &Path) {
        let connection = Connection::open_in_memory().expect("DuckDB");
        let schema = self
            .columns
            .iter()
            .map(|(name, kind)| format!("\"{name}\" {}", kind.sql_type()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("CREATE TABLE t ({schema});");
        for row in &self.rows {
            let values = row
                .iter()
                .map(|cell| cell.sql.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!("INSERT INTO t VALUES ({values});"));
        }
        sql.push_str(&format!(
            "COPY t TO {} (FORMAT PARQUET);",
            sql_string(&path.to_string_lossy())
        ));
        connection.execute_batch(&sql).expect("parquet written");
    }

    fn show(&self) -> String {
        let names = self
            .columns
            .iter()
            .map(|(name, kind)| format!("{name}:{}", kind.sql_type()))
            .collect::<Vec<_>>()
            .join(", ");
        let rows = self.json_rows();
        format!(
            "[{names}] rows={}",
            serde_json::to_string(&rows).expect("rows serialize")
        )
    }
}

/// `id` is a unique non-null VARCHAR key; the other columns are random and nullable.
fn random_table(rng: &mut Rng, columns: &[(&'static str, Kind)]) -> Table {
    let row_count = if rng.chance(8) { 0 } else { 1 + rng.below(5) };
    let null_percent: Vec<u64> = columns
        .iter()
        .map(|_| *rng.pick(&[0, 0, 0, 20, 20, 100]))
        .collect();
    let mut all_columns = vec![("id", Kind::Text)];
    all_columns.extend_from_slice(columns);
    let mut rows = Vec::new();
    for index in 0..row_count {
        let mut row = vec![cell(Kind::Text, &format!("r{index}"))];
        for ((_, kind), nulls) in columns.iter().zip(&null_percent) {
            row.push(if rng.chance(*nulls) {
                null_cell()
            } else {
                random_cell(rng, *kind)
            });
        }
        // A copied row creates duplicate tuples for the uniqueness rules.
        if index > 0 && rng.chance(25) {
            let source = rng.below(rows.len());
            let copied: Vec<Cell> = Vec::clone(&rows[source]);
            row.truncate(1);
            row.extend(copied.into_iter().skip(1));
        }
        rows.push(row);
    }
    Table {
        columns: all_columns,
        rows,
    }
}

// ---------------------------------------------------------------------------
// Artifacts and outcomes
// ---------------------------------------------------------------------------

fn artifact(bindings: &[&str], portability: &str, check: Value) -> ConstraintSet {
    let bindings: Vec<Value> = bindings
        .iter()
        .map(|name| json!({ "name": name, "kind": "relation", "key_fields": ["id"] }))
        .collect();
    serde_json::from_value(json!({
        "version": "verify.constraint.v1",
        "constraint_set_id": "tests.lane_differential",
        "bindings": bindings,
        "rules": [{
            "id": "RULE",
            "severity": "error",
            "portability": portability,
            "check": check,
        }],
    }))
    .expect("generated artifact parses")
}

fn only_rule(constraints: &ConstraintSet) -> &Rule {
    &constraints.rules[0]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    /// Pass or fail with the sorted failing keys (one entry per violation).
    Decided {
        pass: bool,
        failing: Vec<String>,
    },
    Refused(String),
}

fn code_text<T: serde::Serialize>(code: &T) -> String {
    serde_json::to_value(code)
        .expect("code serializes")
        .as_str()
        .expect("code is a string")
        .to_owned()
}

fn decided(result: &RuleResult) -> Verdict {
    let mut failing: Vec<String> = result
        .affected
        .iter()
        .map(|entry| serde_json::to_string(&entry.key).expect("key serializes"))
        .collect();
    failing.sort();
    assert_eq!(result.violation_count, failing.len(), "count matches list");
    let pass = serde_json::to_value(result.status).expect("status") == json!("pass");
    assert_eq!(pass, failing.is_empty(), "status matches list");
    Verdict::Decided { pass, failing }
}

fn embedded_bindings(tables: &[(&str, &Table)]) -> EmbeddedBindings {
    tables
        .iter()
        .map(|(name, table)| {
            (
                (*name).to_owned(),
                EmbeddedBinding::new(
                    format!("embedded://{name}"),
                    format!("sha256:{name}"),
                    Relation::new(vec!["id".to_owned()], table.json_rows()),
                ),
            )
        })
        .collect()
}

fn portable_verdict(constraints: &ConstraintSet, tables: &[(&str, &Table)]) -> Verdict {
    let report = EmbeddedExecutor::evaluate(constraints, "sha256:x", &embedded_bindings(tables));
    if report.outcome == Outcome::Refusal {
        let refusal = report.refusal.expect("refusal payload");
        Verdict::Refused(code_text(&refusal.code))
    } else {
        decided(&report.results[0])
    }
}

// ---------------------------------------------------------------------------
// Arm P: one predicate AST, two evaluators
// ---------------------------------------------------------------------------

const PREDICATE_COLUMNS: &[(&str, Kind)] = &[
    ("n1", Kind::Int),
    ("n2", Kind::Int),
    ("d1", Kind::Double),
    ("m1", Kind::Decimal),
    ("s1", Kind::Text),
    ("s2", Kind::Text),
    ("b1", Kind::Bool),
];

fn random_column(rng: &mut Rng) -> (&'static str, Kind) {
    *rng.pick(PREDICATE_COLUMNS)
}

fn random_literal(rng: &mut Rng, like: Kind) -> Value {
    if rng.chance(5) {
        return Value::Null;
    }
    let kind = if rng.chance(8) {
        *rng.pick(&[Kind::Int, Kind::Text, Kind::Bool])
    } else {
        like
    };
    pool_value(rng, kind)
}

fn same_category_column(rng: &mut Rng, like: Kind) -> &'static str {
    if rng.chance(8) {
        return random_column(rng).0;
    }
    let candidates: Vec<&'static str> = PREDICATE_COLUMNS
        .iter()
        .filter(|(_, kind)| kind.category() == like.category())
        .map(|(name, _)| *name)
        .collect();
    rng.pick(&candidates)
}

fn random_expression(rng: &mut Rng, depth: usize) -> Value {
    let choice = if depth == 0 {
        rng.below(6)
    } else {
        rng.below(9)
    };
    let (name, kind) = random_column(rng);
    let column = json!({ "column": name });
    match choice {
        0..=1 => {
            let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
            let other = if rng.chance(30) {
                json!({ "column": same_category_column(rng, kind) })
            } else {
                random_literal(rng, kind)
            };
            let operands = if rng.chance(20) {
                json!([other, column])
            } else {
                json!([column, other])
            };
            let mut object = serde_json::Map::new();
            object.insert(operator.to_owned(), operands);
            Value::Object(object)
        }
        2 => {
            let set: Vec<Value> = (0..rng.below(4))
                .map(|_| random_literal(rng, kind))
                .collect();
            json!({ "in": [column, set] })
        }
        3 => json!({ "is_null": column }),
        4 => json!({ "is_blank": column }),
        5 => column,
        6 => json!({ "not": random_expression(rng, depth - 1) }),
        _ => {
            let parts: Vec<Value> = (0..1 + rng.below(3))
                .map(|_| random_expression(rng, depth - 1))
                .collect();
            if choice == 7 {
                json!({ "and": parts })
            } else {
                json!({ "or": parts })
            }
        }
    }
}

/// Qualify a seeded non-empty subset of the column references to `twin`.
fn qualify_to_twin(rng: &mut Rng, expression: &Value) -> Value {
    fn count(value: &Value) -> usize {
        match value {
            Value::Object(fields) if fields.contains_key("column") => 1,
            Value::Object(fields) => fields.values().map(count).sum(),
            Value::Array(items) => items.iter().map(count).sum(),
            _ => 0,
        }
    }
    fn rewrite(value: &mut Value, chosen: &[bool], next: &mut usize) {
        match value {
            Value::Object(fields) if fields.contains_key("column") => {
                if chosen[*next] {
                    fields.insert("binding".to_owned(), json!("twin"));
                }
                *next += 1;
            }
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    rewrite(child, chosen, next);
                }
            }
            Value::Array(items) => {
                for child in items {
                    rewrite(child, chosen, next);
                }
            }
            _ => {}
        }
    }
    let total = count(expression);
    assert!(total > 0, "every generated expression references a column");
    let mut chosen: Vec<bool> = (0..total).map(|_| rng.chance(50)).collect();
    let forced = rng.below(total);
    chosen[forced] = true;
    let mut rewritten = expression.clone();
    rewrite(&mut rewritten, &chosen, &mut 0);
    rewritten
}

fn references_column(expression: &Value) -> bool {
    match expression {
        Value::Object(fields) => {
            fields.contains_key("column") || fields.values().any(references_column)
        }
        Value::Array(items) => items.iter().any(references_column),
        _ => false,
    }
}

fn twin_artifact(expression: &Value) -> ConstraintSet {
    artifact(
        &["input", "twin"],
        "batch_only",
        json!({ "op": "predicate", "binding": "input", "expr": expression }),
    )
}

fn duckdb_predicate_verdict(constraints: &ConstraintSet, parquet: &Path) -> Verdict {
    let context = prepare_batch_context(
        constraints,
        vec![
            BatchBindingInput::new("input", parquet),
            BatchBindingInput::new("twin", parquet),
        ],
        BatchBindingLimits::default(),
    )
    .expect("typed parquet loads");
    match evaluate_binding_predicate(
        only_rule(constraints),
        context.connection(),
        context.bindings(),
    ) {
        Ok(result) => decided(&result),
        Err(error) => Verdict::Refused(code_text(&error.refusal_code())),
    }
}

const PREDICATE_CASES: u64 = 1500;

#[test]
fn arm_p_predicate_verdicts_and_failing_rows_match_across_evaluators() {
    let directory = tempdir().expect("scratch");
    let mut mismatches = Vec::new();
    let mut tally = BTreeMap::<&str, usize>::new();
    let mut known = KnownHits::default();
    for case in 0..PREDICATE_CASES {
        let seed = case_seed(0xD1, case);
        let mut rng = Rng::new(seed);
        let table = random_table(&mut rng, PREDICATE_COLUMNS);
        let expression = loop {
            let candidate = random_expression(&mut rng, 2);
            if references_column(&candidate) {
                break candidate;
            }
        };
        let qualified = qualify_to_twin(&mut rng, &expression);

        let portable = artifact(
            &["input"],
            "portable",
            json!({ "op": "predicate", "binding": "input", "expr": expression }),
        );
        let batch = twin_artifact(&qualified);

        // The two artifacts cannot be served by one evaluator.
        assert!(
            routes_to_portable_engine(only_rule(&portable)),
            "seed {seed:#x}"
        );
        assert!(
            !routes_to_portable_engine(only_rule(&batch)),
            "seed {seed:#x}"
        );
        let refused = portable_verdict(&batch, &[("input", &table), ("twin", &table)]);
        assert_eq!(
            refused,
            Verdict::Refused("E_BATCH_ONLY_RULE".to_owned()),
            "seed {seed:#x}"
        );
        let relations = BTreeMap::from([(
            "input".to_owned(),
            Relation::new(vec!["id".to_owned()], table.json_rows()),
        )]);
        assert!(
            matches!(
                portable_row::evaluate_rule(only_rule(&batch), &relations),
                Err(portable_row::EngineError::UnsupportedOp(_))
            ),
            "seed {seed:#x}"
        );

        let path = directory.path().join(format!("p{case}.parquet"));
        table.write_parquet(&path);
        let left = portable_verdict(&portable, &[("input", &table)]);
        let right = duckdb_predicate_verdict(&batch, &path);
        fs::remove_file(&path).expect("own scratch parquet");

        *tally
            .entry(match &left {
                Verdict::Decided { pass: true, .. } => "pass",
                Verdict::Decided { .. } => "fail",
                Verdict::Refused(_) => "refusal",
            })
            .or_default() += 1;
        if left != right {
            if matches!(left, Verdict::Decided { .. })
                && right == Verdict::Refused("E_BAD_EXPR".to_owned())
                && has_column_type_conflict(&expression, &table)
            {
                known.record(&[BEAD_COLUMN_TYPE_PREFLIGHT]);
            } else {
                mismatches.push(format!(
                    "seed {seed:#x} expr={qualified} table={}\n  portable={left:?}\n  duckdb={right:?}",
                    table.show()
                ));
            }
        }
    }
    eprintln!("arm P portable outcomes: {tally:?}");
    report_mismatches("arm P", &mismatches);
    known.assert_reached("arm P", &[BEAD_COLUMN_TYPE_PREFLIGHT]);
}

fn report_mismatches(arm: &str, mismatches: &[String]) {
    for line in mismatches {
        eprintln!("{arm} MISMATCH {line}");
    }
    assert!(
        mismatches.is_empty(),
        "{arm}: {} lane mismatches (listed above with seeds)",
        mismatches.len()
    );
}

// ---------------------------------------------------------------------------
// Arm S: verify-engine against DuckDB's own semantics
// ---------------------------------------------------------------------------

/// Rust's `char::is_whitespace` (Unicode White_Space), spelled for DuckDB's RE2.
const BLANK_PATTERN: &str = r"[\x{9}-\x{D}\x{20}\x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]*";

fn duckdb_query_verdict(bindings: &[(&str, &Path)], query: &str) -> Verdict {
    let names: Vec<&str> = bindings.iter().map(|(name, _)| *name).collect();
    let constraints = artifact(
        &names,
        "batch_only",
        json!({ "op": "query_zero_rows", "bindings": names, "query": query }),
    );
    assert!(!routes_to_portable_engine(only_rule(&constraints)));
    let context = prepare_batch_context(
        &constraints,
        bindings
            .iter()
            .map(|(name, path)| BatchBindingInput::new(*name, *path))
            .collect(),
        BatchBindingLimits::default(),
    )
    .expect("typed parquet loads");
    // The error's Debug form carries the failing SQL.
    let results = execute_query_rules(context.connection(), &constraints.rules)
        .expect("oracle SQL runs; a failure here is a test bug");
    decided(&results[0])
}

/// SQL literal with the same mathematical value as a JSON literal compared with `kind`.
fn sql_literal(value: &Value, kind: Kind) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(flag) => flag.to_string().to_uppercase(),
        Value::Number(n) => match kind {
            Kind::Double => format!("CAST('{n}' AS DOUBLE)"),
            _ => n.to_string(),
        },
        other => sql_string(other.as_str().expect("predicate literals are scalar")),
    }
}

fn sql_operator(operator: &str) -> &'static str {
    [
        ("eq", "IS NOT DISTINCT FROM"),
        ("ne", "IS DISTINCT FROM"),
        ("gt", ">"),
        ("gte", ">="),
        ("lt", "<"),
        ("lte", "<="),
    ]
    .into_iter()
    .find_map(|(name, sql)| (name == operator).then_some(sql))
    .expect("known comparison operator")
}

struct OracleCase {
    label: &'static str,
    tables: Vec<(&'static str, Table)>,
    check: Value,
    query: String,
    /// Row-level rules compare failing keys; relation-level rules compare pass/fail only.
    keyed: bool,
}

fn run_oracle_case(directory: &Path, case: &OracleCase) -> (Verdict, Verdict) {
    let constraints = artifact(
        &case
            .tables
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        "portable",
        case.check.clone(),
    );
    assert!(routes_to_portable_engine(only_rule(&constraints)));
    let paths: Vec<(&str, PathBuf)> = case
        .tables
        .iter()
        .map(|(name, table)| {
            let path = directory.join(format!("{name}.parquet"));
            table.write_parquet(&path);
            (*name, path)
        })
        .collect();
    let tables: Vec<(&str, &Table)> = case
        .tables
        .iter()
        .map(|(name, table)| (*name, table))
        .collect();
    let mut left = portable_verdict(&constraints, &tables);
    let bound: Vec<(&str, &Path)> = paths
        .iter()
        .map(|(name, path)| (*name, path.as_path()))
        .collect();
    let mut right = duckdb_query_verdict(&bound, &case.query);
    if !case.keyed {
        for verdict in [&mut left, &mut right] {
            if let Verdict::Decided { failing, .. } = verdict {
                failing.clear();
            }
        }
    }
    (left, right)
}

const ORACLE_COLUMNS: &[(&str, Kind)] = &[
    ("n1", Kind::Int),
    ("d1", Kind::Double),
    ("m1", Kind::Decimal),
    ("s1", Kind::Text),
    ("b1", Kind::Bool),
];

fn without_nulls(rng: &mut Rng, table: &mut Table) {
    let kinds: Vec<Kind> = table.columns.iter().map(|(_, kind)| *kind).collect();
    for row in &mut table.rows {
        for (cell, kind) in row.iter_mut().zip(&kinds) {
            if cell.json.is_null() {
                *cell = random_cell(rng, *kind);
            }
        }
    }
}

fn unique_case(rng: &mut Rng) -> OracleCase {
    let table = random_table(rng, ORACLE_COLUMNS);
    let mut columns = vec![rng.pick(ORACLE_COLUMNS).0];
    if rng.chance(30) {
        let second = rng.pick(ORACLE_COLUMNS).0;
        if second != columns[0] {
            columns.push(second);
        }
    }
    let partition = columns.join(", ");
    OracleCase {
        label: "unique",
        check: json!({ "op": "unique", "binding": "input", "columns": columns }),
        query: format!(
            "SELECT id AS key__id FROM (SELECT id, count(*) OVER (PARTITION BY {partition}) AS n FROM input) WHERE n > 1"
        ),
        tables: vec![("input", table)],
        keyed: true,
    }
}

fn not_null_case(rng: &mut Rng) -> OracleCase {
    let table = random_table(rng, ORACLE_COLUMNS);
    let mut columns = vec![rng.pick(ORACLE_COLUMNS).0];
    if rng.chance(40) {
        let second = rng.pick(ORACLE_COLUMNS).0;
        if second != columns[0] {
            columns.push(second);
        }
    }
    let parts: Vec<String> = columns
        .iter()
        .map(|column| {
            let blank = if table.kind(column) == Kind::Text {
                format!(" OR regexp_full_match({column}, '{BLANK_PATTERN}')")
            } else {
                String::new()
            };
            format!("SELECT id AS key__id FROM input WHERE {column} IS NULL{blank}")
        })
        .collect();
    OracleCase {
        label: "not_null",
        check: json!({ "op": "not_null", "binding": "input", "columns": columns }),
        query: parts.join(" UNION ALL "),
        tables: vec![("input", table)],
        keyed: true,
    }
}

/// The plan does not say whether a null reference resolves against a null parent; the oracle
/// uses the protocol's null equality (null equals null), which is what the engine does.
fn foreign_key_case(rng: &mut Rng) -> OracleCase {
    let child_kind = *rng.pick(&[
        Kind::Int,
        Kind::Double,
        Kind::Decimal,
        Kind::Text,
        Kind::Bool,
    ]);
    // Same family, but sometimes a different physical numeric type on the parent side.
    let parent_kind = if child_kind.category() == "number" && rng.chance(35) {
        *rng.pick(&[Kind::Int, Kind::Double, Kind::Decimal])
    } else {
        child_kind
    };
    // Small values only when types are mixed: DuckDB widens to DOUBLE and would itself round.
    let mixed = child_kind != parent_kind;
    let small = |rng: &mut Rng, kind: Kind| match kind {
        Kind::Double => cell(kind, rng.pick(&["1.0", "2.0", "0.5", "0.0"])),
        Kind::Decimal => cell(kind, rng.pick(&["1", "2", "0.5", "0"])),
        _ => cell(kind, rng.pick(&["1", "2", "0"])),
    };
    let build = |rng: &mut Rng, kind: Kind, prefix: &str, count: usize| {
        let rows = (0..count)
            .map(|index| {
                let value = if rng.chance(12) {
                    null_cell()
                } else if mixed {
                    small(rng, kind)
                } else {
                    random_cell(rng, kind)
                };
                vec![cell(Kind::Text, &format!("{prefix}{index}")), value]
            })
            .collect();
        Table {
            columns: vec![("id", Kind::Text), ("ref", kind)],
            rows,
        }
    };
    let child_rows = rng.below(5);
    let child = build(rng, child_kind, "c", child_rows);
    let parent_rows = rng.below(5);
    let parent = build(rng, parent_kind, "p", parent_rows);
    OracleCase {
        label: "foreign_key",
        check: json!({
            "op": "foreign_key", "binding": "input", "columns": ["ref"],
            "ref_binding": "parent", "ref_columns": ["ref"],
        }),
        query: "SELECT c.id AS key__id FROM input AS c WHERE NOT EXISTS (SELECT 1 FROM parent AS p WHERE p.ref IS NOT DISTINCT FROM c.ref)".to_owned(),
        tables: vec![("input", child), ("parent", parent)],
        keyed: true,
    }
}

fn comparison_sql(
    observed: &str,
    operator: &str,
    expected: &str,
    tolerance: Option<&str>,
) -> String {
    match (operator, tolerance) {
        ("eq", Some(tolerance)) => format!("abs(({observed}) - ({expected})) <= {tolerance}"),
        ("ne", Some(tolerance)) => format!("abs(({observed}) - ({expected})) > {tolerance}"),
        _ => format!("({observed}) {} ({expected})", sql_operator(operator)),
    }
}

fn comparison_json(operator: &str, expected: &Value, tolerance: Option<&str>) -> Value {
    let mut compare = serde_json::Map::new();
    compare.insert(operator.to_owned(), expected.clone());
    if let Some(tolerance) = tolerance {
        compare.insert("tolerance".to_owned(), number(tolerance));
    }
    Value::Object(compare)
}

fn row_count_case(rng: &mut Rng) -> OracleCase {
    let table = random_table(rng, &[("n1", Kind::Int)]);
    let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
    let expected = json!(table.rows.len() as i64 + rng.below(5) as i64 - 2);
    let tolerance = if matches!(operator, "eq" | "ne") && rng.chance(60) {
        Some(*rng.pick(&["0", "0.999999", "1", "1.000001", "2"]))
    } else {
        None
    };
    let observed = "SELECT count(*) FROM input";
    let condition = comparison_sql(observed, operator, &expected.to_string(), tolerance);
    OracleCase {
        label: "row_count",
        check: json!({
            "op": "row_count", "binding": "input",
            "compare": comparison_json(operator, &expected, tolerance),
        }),
        query: format!("SELECT 1 AS marker WHERE NOT coalesce({condition}, false)"),
        tables: vec![("input", table)],
        keyed: false,
    }
}

/// Numbers of different digit counts and both signs, each exact in f64 and none `-0.0`, so
/// text order and numeric order disagree often and no other filed defect can appear.
fn digit_count_value(rng: &mut Rng, kind: Kind) -> &'static str {
    match kind {
        Kind::Int => rng.pick(&[
            "0", "1", "2", "9", "10", "11", "99", "100", "101", "999", "1000", "123456", "-1",
            "-2", "-9", "-10", "-99", "-100", "-1000",
        ]),
        Kind::Double => rng.pick(&[
            "0.0",
            "1.5",
            "2.0",
            "9.5",
            "10.0",
            "10.25",
            "100.0",
            "999.5",
            "1000.0",
            "-1.5",
            "-9.0",
            "-10.0",
            "-100.25",
            "0.0000152587890625",
            "1e21",
        ]),
        _ => rng.pick(&[
            "0", "0.5", "2", "9", "10", "10.25", "99.5", "100", "1000", "-1.5", "-2", "-10",
            "-100.25",
        ]),
    }
}

fn min_max_case(rng: &mut Rng) -> OracleCase {
    let function = *rng.pick(&["min", "max"]);
    let kind = *rng.pick(&[Kind::Int, Kind::Double, Kind::Decimal]);
    let row_count = 2 + rng.below(5);
    // At least two rows whose magnitudes have different digit counts.
    let values = loop {
        let values: Vec<&str> = (0..row_count)
            .map(|_| digit_count_value(rng, kind))
            .collect();
        let digits = |text: &str| {
            text.trim_start_matches('-')
                .split(['.', 'e'])
                .next()
                .map_or(0, str::len)
        };
        if values.iter().any(|v| digits(v) != digits(values[0])) {
            break values;
        }
    };
    let table = Table {
        columns: vec![("id", Kind::Text), ("v", kind)],
        rows: values
            .iter()
            .enumerate()
            .map(|(index, text)| vec![cell(Kind::Text, &format!("r{index}")), cell(kind, text)])
            .collect(),
    };
    let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
    let expected = table.rows[rng.below(table.rows.len())][1].json.clone();
    let observed = format!("SELECT {function}(v) FROM input");
    let condition = comparison_sql(&observed, operator, &sql_literal(&expected, kind), None);
    let mut aggregate = serde_json::Map::new();
    aggregate.insert(function.to_owned(), json!("v"));
    OracleCase {
        label: "aggregate_compare",
        check: json!({
            "op": "aggregate_compare", "binding": "input", "aggregate": aggregate,
            "compare": comparison_json(operator, &expected, None),
        }),
        query: format!("SELECT 1 AS marker WHERE NOT coalesce({condition}, false)"),
        tables: vec![("input", table)],
        keyed: false,
    }
}

/// Aggregates are generated over non-empty, null-free columns: DuckDB's own aggregates skip
/// nulls, so it is no oracle there. The `min`/`max` refusals have their own tests.
fn aggregate_case(rng: &mut Rng) -> OracleCase {
    let function = *rng.pick(&["sum", "avg", "min", "max"]);
    let column = if matches!(function, "sum" | "avg") {
        *rng.pick(&["n1", "d1", "m1"])
    } else {
        rng.pick(ORACLE_COLUMNS).0
    };
    let mut table = random_table(rng, ORACLE_COLUMNS);
    while table.rows.is_empty() {
        table = random_table(rng, ORACLE_COLUMNS);
    }
    without_nulls(rng, &mut table);
    let kind = table.kind(column);
    // avg over wide integers or decimals is DOUBLE in DuckDB too, so keep it to exact inputs.
    if function == "avg" || (function == "sum" && kind == Kind::Int && rng.chance(50)) {
        let index = table.index(column);
        for row in &mut table.rows {
            let small = *rng.pick(&["1", "2", "3", "10"]);
            row[index] = cell(kind, small);
        }
    }
    // DuckDB itself raises an error when a DOUBLE sum overflows; keep the oracle defined.
    if function == "sum" && kind == Kind::Double {
        let index = table.index(column);
        for row in &mut table.rows {
            if row[index].json.as_f64().is_some_and(|v| v.abs() > 1e300) {
                row[index] = cell(kind, "100.25");
            }
        }
    }
    if function == "sum" && kind == Kind::Int {
        // Keep the exact sum inside i64 so that it can be written as a literal.
        let index = table.index(column);
        for row in &mut table.rows {
            if row[index]
                .json
                .as_i64()
                .is_some_and(|v| v.unsigned_abs() > (1 << 60))
            {
                row[index] = cell(kind, "9007199254740992");
            }
        }
    }
    let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
    // The expected value is usually one near the observed boundary: a cell, or the exact sum.
    let index = table.index(column);
    let expected = if function == "sum" && rng.chance(60) {
        exact_sum(&table, column)
    } else if rng.chance(75) {
        table.rows[rng.below(table.rows.len())][index].json.clone()
    } else {
        pool_value(rng, kind)
    };
    let tolerance = if matches!(operator, "eq" | "ne") && expected.is_number() && rng.chance(30) {
        Some(*rng.pick(&["0", "0.25", "1", "0.01"]))
    } else {
        None
    };
    let observed = format!("SELECT {function}({column}) FROM input");
    // Integer differences can leave BIGINT, so the oracle subtracts in HUGEINT.
    let literal = if kind == Kind::Int && tolerance.is_some() {
        format!("CAST({} AS HUGEINT)", sql_literal(&expected, kind))
    } else {
        sql_literal(&expected, kind)
    };
    let condition = comparison_sql(&observed, operator, &literal, tolerance);
    let mut aggregate = serde_json::Map::new();
    aggregate.insert(function.to_owned(), json!(column));
    OracleCase {
        label: "aggregate_compare",
        check: json!({
            "op": "aggregate_compare", "binding": "input", "aggregate": aggregate,
            "compare": comparison_json(operator, &expected, tolerance),
        }),
        query: format!("SELECT 1 AS marker WHERE NOT coalesce({condition}, false)"),
        tables: vec![("input", table)],
        keyed: false,
    }
}

/// The mathematically exact sum of a null-free numeric column, as DuckDB computes it.
fn exact_sum(table: &Table, column: &str) -> Value {
    let kind = table.kind(column);
    let index = table.index(column);
    let connection = Connection::open_in_memory().expect("DuckDB");
    let values = table
        .rows
        .iter()
        .map(|row| format!("({})", row[index].sql))
        .collect::<Vec<_>>()
        .join(", ");
    let text: String = connection
        .query_row(
            &format!("SELECT CAST(sum(v) AS VARCHAR) FROM (VALUES {values}) AS t(v)"),
            [],
            |row| row.get(0),
        )
        .expect("exact sum");
    if kind == Kind::Decimal && text.contains('.') {
        number(text.trim_end_matches('0').trim_end_matches('.'))
    } else {
        number(&text)
    }
}

fn row_predicate_case(rng: &mut Rng) -> OracleCase {
    let (column, kind) = *rng.pick(ORACLE_COLUMNS);
    let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
    let mut table = random_table(rng, ORACLE_COLUMNS);
    let ordering = !matches!(operator, "eq" | "ne");
    if ordering {
        without_nulls(rng, &mut table);
    }
    let literal = if !table.rows.is_empty() && rng.chance(60) {
        let index = table.index(column);
        let from_cell = table.rows[rng.below(table.rows.len())][index].json.clone();
        if from_cell.is_null() {
            pool_value(rng, kind)
        } else {
            from_cell
        }
    } else {
        pool_value(rng, kind)
    };
    // Literals near a wide value: one unit away in the last integer place.
    let literal = match literal.as_i64() {
        Some(value) if kind == Kind::Int && rng.chance(30) => json!(value.saturating_sub(1)),
        _ => literal,
    };
    let mut expression = serde_json::Map::new();
    expression.insert(operator.to_owned(), json!([{ "column": column }, literal]));
    OracleCase {
        label: "predicate",
        check: json!({ "op": "predicate", "binding": "input", "expr": expression }),
        query: format!(
            // The literal is parenthesized: verify's read-only check reads a bare
            // `IS DISTINCT FROM 'text'` as a file scan.
            "SELECT id AS key__id FROM input WHERE NOT coalesce({column} {} ({}), false)",
            sql_operator(operator),
            sql_literal(&literal, kind)
        ),
        tables: vec![("input", table)],
        keyed: true,
    }
}

fn run_oracle_arm(
    arm: u64,
    cases: u64,
    generate: fn(&mut Rng) -> OracleCase,
    expected_divergences: &[&'static str],
) {
    let directory = tempdir().expect("scratch");
    let mut known = KnownHits::default();
    let mut mismatches = Vec::new();
    let mut tally = BTreeMap::<&str, usize>::new();
    let mut label = "";
    for case_number in 0..cases {
        let seed = case_seed(arm, case_number);
        let mut rng = Rng::new(seed);
        let case = generate(&mut rng);
        label = case.label;
        let (left, right) = run_oracle_case(directory.path(), &case);
        *tally
            .entry(match &right {
                Verdict::Decided { pass: true, .. } => "pass",
                Verdict::Decided { .. } => "fail",
                Verdict::Refused(_) => "refusal",
            })
            .or_default() += 1;
        if left != right {
            let triggers = oracle_defect_triggers(&case);
            if !triggers.is_empty() && current_engine_model(&case).as_ref() == Some(&left) {
                known.record(&triggers);
                continue;
            }
            let tables: Vec<String> = case
                .tables
                .iter()
                .map(|(name, table)| format!("{name}={}", table.show()))
                .collect();
            mismatches.push(format!(
                "seed {seed:#x} check={} {}\n  portable={left:?}\n  duckdb={right:?}",
                case.check,
                tables.join(" ")
            ));
        }
    }
    eprintln!("arm S {label} DuckDB outcomes: {tally:?}");
    report_mismatches(&format!("arm S {label}"), &mismatches);
    known.assert_reached(&format!("arm S {label}"), expected_divergences);
}

#[test]
fn arm_s_unique_matches_duckdb() {
    run_oracle_arm(
        0x51,
        400,
        unique_case,
        &[BEAD_F64_ROUNDING, BEAD_NUMBER_TEXT_IDENTITY],
    );
}

#[test]
fn arm_s_not_null_matches_duckdb() {
    run_oracle_arm(0x52, 400, not_null_case, &[]);
}

#[test]
fn arm_s_foreign_key_matches_duckdb() {
    run_oracle_arm(
        0x53,
        400,
        foreign_key_case,
        &[BEAD_F64_ROUNDING, BEAD_NUMBER_TEXT_IDENTITY],
    );
}

#[test]
fn arm_s_row_count_matches_duckdb() {
    run_oracle_arm(0x54, 250, row_count_case, &[]);
}

#[test]
fn arm_s_aggregate_compare_matches_duckdb() {
    run_oracle_arm(
        0x55,
        600,
        aggregate_case,
        &[BEAD_F64_ROUNDING, BEAD_SIGNED_ZERO_ORDER],
    );
}

/// No case here can trigger a filed defect, so the lanes must agree on every one.
#[test]
fn arm_s_min_max_orders_numbers_by_value() {
    run_oracle_arm(0x57, 600, min_max_case, &[]);
}

/// Both paths over the same rows: the embedded executor and `verify run` over typed Parquet.
fn min_max_in_both_paths(
    table: &Table,
    function: &str,
    operator: &str,
    expected: Value,
) -> (Verdict, String) {
    let check = aggregate(function, operator, expected);
    let embedded = portable_verdict(
        &artifact(&["input"], "portable", check.clone()),
        &[("input", table)],
    );
    (embedded, repro_outcome(&[("input", table)], check))
}

/// A null, or a column that is all null, has no `min` or `max`. Batch reports the refusal as
/// `E_BAD_CONSTRAINTS` where the plan says `E_BAD_EXPR`; that is bd-uj2.
#[test]
fn min_max_over_nulls_refuse_in_both_paths() {
    for values in [
        vec![None, Some("5")],
        vec![Some("5"), None],
        vec![None, None],
    ] {
        let table = single_column(Kind::Int, &values);
        for function in ["min", "max"] {
            // Both operators passed or failed on some of these before bd-nx1.
            for (operator, expected) in [("ne", json!(7)), ("lte", json!(9))] {
                assert_eq!(
                    min_max_in_both_paths(&table, function, operator, expected),
                    (
                        Verdict::Refused("E_BAD_EXPR".to_owned()),
                        "E_BAD_CONSTRAINTS".to_owned()
                    ),
                    "{function} {operator} over {values:?}"
                );
            }
        }
    }
}

/// An empty CSV cell is the empty string, not null, and it makes the whole column strings, so
/// the null rule never applies: `min` selects `""` and `max` orders the digits as text.
#[test]
fn min_max_over_a_csv_column_with_an_empty_cell_order_strings() {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.csv");
    fs::write(&path, "id,v\nr0,\nr1,9\nr2,10\n").expect("csv written");
    let outcome = |check: Value| {
        let constraints = artifact(&["input"], "portable", check);
        essence_text(&cli_report(
            directory.path(),
            &constraints,
            &[("input", &path)],
        ))
    };
    assert_eq!(outcome(aggregate("min", "eq", json!(""))), "PASS");
    assert_eq!(outcome(aggregate("max", "eq", json!("9"))), "PASS");
    // A numeric threshold cannot be ordered against a string, so this refuses.
    assert_eq!(
        outcome(aggregate("max", "lte", json!(9))),
        "E_BAD_CONSTRAINTS"
    );
}

/// Every function, every operator and every threshold type that does not match the observed
/// value refuses in both paths. Batch says `E_BAD_CONSTRAINTS` where the plan says
/// `E_BAD_EXPR`; that is bd-uj2.
#[test]
fn wrong_typed_thresholds_refuse_in_both_paths() {
    let numbers = single_column(Kind::Int, &[Some("9"), Some("10")]);
    let strings = single_column(Kind::Text, &[Some("9"), Some("10")]);
    let booleans = single_column(Kind::Bool, &[Some("true"), Some("false")]);
    let row_count = json!({ "op": "row_count", "binding": "input" });
    let over = |function: &str| {
        let mut check = aggregate(function, "eq", Value::Null);
        check.as_object_mut().expect("check").remove("compare");
        check
    };
    // (observed column, rule without its comparison, thresholds of another type)
    let number_thresholds = || {
        vec![
            json!("10"),
            json!(true),
            Value::Null,
            json!([10]),
            json!({}),
        ]
    };
    let mut cases = vec![(&numbers, row_count, number_thresholds())];
    for function in ["min", "max", "sum", "avg"] {
        cases.push((&numbers, over(function), number_thresholds()));
    }
    for function in ["min", "max"] {
        cases.push((
            &strings,
            over(function),
            vec![json!(10), json!(true), Value::Null, json!(["10"])],
        ));
        cases.push((
            &booleans,
            over(function),
            vec![json!(0), json!("false"), Value::Null, json!([false])],
        ));
    }

    for (table, check, thresholds) in cases {
        for threshold in thresholds {
            for operator in ["eq", "ne", "gt", "gte", "lt", "lte"] {
                let mut compare = serde_json::Map::new();
                compare.insert(operator.to_owned(), threshold.clone());
                let mut check = check.clone();
                check["compare"] = Value::Object(compare);
                let embedded = portable_verdict(
                    &artifact(&["input"], "portable", check.clone()),
                    &[("input", table)],
                );
                let batch = repro_outcome(&[("input", table)], check.clone());
                assert_eq!(
                    (embedded, batch.as_str()),
                    (
                        Verdict::Refused("E_BAD_EXPR".to_owned()),
                        "E_BAD_CONSTRAINTS"
                    ),
                    "{check} over {}",
                    table.show()
                );
            }
        }
    }
}

/// Typed batch columns cannot mix scalar types, so only an embedded caller can.
#[test]
fn min_max_over_mixed_types_refuse_embedded() {
    for other in [json!("1"), json!(true)] {
        let mut table = single_column(Kind::Int, &[Some("10"), Some("9")]);
        table.rows[1][1].json = other.clone();
        for function in ["min", "max"] {
            assert_eq!(
                portable_verdict(
                    &artifact(&["input"], "portable", aggregate(function, "ne", json!(7))),
                    &[("input", &table)],
                ),
                Verdict::Refused("E_BAD_EXPR".to_owned()),
                "{function} over 10 and {other}"
            );
        }
    }
}

#[test]
fn arm_s_row_predicate_matches_duckdb() {
    run_oracle_arm(
        0x56,
        600,
        row_predicate_case,
        &[BEAD_F64_ROUNDING, BEAD_SIGNED_ZERO_ORDER],
    );
}

// ---------------------------------------------------------------------------
// Known divergences (bead ids). An entry matches only its own case shape AND the exact outcome
// of the defect; anything else fails, and an entry that no case reaches fails too, so a fix
// forces its removal. The correct behaviour of each is asserted by an ignored repro below.
// ---------------------------------------------------------------------------

const BEAD_F64_ROUNDING: &str = "bd-1t5";
const BEAD_SIGNED_ZERO_ORDER: &str = "bd-der";
const BEAD_NUMBER_TEXT_IDENTITY: &str = "bd-v3r";
const BEAD_COLUMN_TYPE_PREFLIGHT: &str = "bd-mhl";
const BEAD_RELATION_REFUSAL_CODE: &str = "bd-uj2";
const BEAD_ALL_NULL_JSON: &str = "bd-ffj";
const BEAD_JSON_STRING_COERCION: &str = "bd-yty";

fn text_identity(value: &Value) -> String {
    match value {
        Value::Null => "\0NULL".to_owned(),
        Value::String(s) => format!("S:{s}"),
        Value::Number(n) => format!("N:{n}"),
        Value::Bool(b) => format!("B:{b}"),
        other => format!("O:{other}"),
    }
}

/// How the engine compares today: numbers as f64, ordered by `total_cmp`.
fn current_order(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => Some(l.as_f64()?.total_cmp(&r.as_f64()?)),
        (Value::String(l), Value::String(r)) => Some(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

fn current_comparison(
    operator: &str,
    observed: &Value,
    expected: &Value,
    tolerance: Option<f64>,
) -> Option<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    if matches!(operator, "eq" | "ne") {
        let equal = match (observed, expected, tolerance) {
            (Value::Number(l), Value::Number(r), Some(t)) => (l.as_f64()? - r.as_f64()?).abs() <= t,
            (Value::Number(l), Value::Number(r), None) => l.as_f64()? == r.as_f64()?,
            _ => observed == expected,
        };
        return Some(equal == (operator == "eq"));
    }
    let order = current_order(observed, expected)?;
    Some(match operator {
        "gt" => order == Greater,
        "gte" => matches!(order, Greater | Equal),
        "lt" => order == Less,
        _ => matches!(order, Less | Equal),
    })
}

fn row_key(row: &[Cell]) -> String {
    serde_json::to_string(&Some(BTreeMap::from([("id", row[0].json.clone())])))
        .expect("key serializes")
}

fn verdict_of(mut failing: Vec<String>) -> Verdict {
    failing.sort();
    Verdict::Decided {
        pass: failing.is_empty(),
        failing,
    }
}

fn first_entry(value: &Value) -> (&str, &Value) {
    value
        .as_object()
        .expect("object")
        .iter()
        .find(|(name, _)| *name != "tolerance")
        .map(|(name, value)| (name.as_str(), value))
        .expect("one operator")
}

fn strings(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_str().expect("string"))
        .collect()
}

/// The documented `min`/`max` value of a null-free column of one physical type, written
/// independently of the engine: integers as integers, other numbers by value with `0.0`
/// preferred over `-0.0`, strings by code point, and `false` before `true`.
fn documented_extremum(
    function: &str,
    kind: Kind,
    values: impl Iterator<Item = Value>,
) -> Option<Value> {
    use std::cmp::Ordering;
    let order = |left: &Value, right: &Value| -> Option<Ordering> {
        match kind {
            Kind::Int => Some(left.as_i64()?.cmp(&right.as_i64()?)),
            Kind::Double | Kind::Decimal => left.as_f64()?.partial_cmp(&right.as_f64()?),
            Kind::Text => Some(left.as_str()?.cmp(right.as_str()?)),
            Kind::Bool => Some(left.as_bool()?.cmp(&right.as_bool()?)),
        }
    };
    let wanted = if function == "min" {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    let mut selected: Option<Value> = None;
    for value in values {
        let replace = match &selected {
            None => true,
            Some(current) => {
                let found = order(&value, current)?;
                found == wanted
                    || (found == Ordering::Equal
                        && is_negative_zero(current)
                        && !is_negative_zero(&value))
            }
        };
        if replace {
            selected = Some(value);
        }
    }
    selected
}

/// A model of the engine's present behaviour, used only to confirm that a mismatch is exactly
/// the filed defect and not something else in the same case.
fn current_engine_model(case: &OracleCase) -> Option<Verdict> {
    let input = &case.tables[0].1;
    let tuple = |table: &Table, row: &[Cell], columns: &[&str]| -> Vec<String> {
        columns
            .iter()
            .map(|column| text_identity(&row[table.index(column)].json))
            .collect()
    };
    match case.label {
        "unique" => {
            let columns = strings(&case.check["columns"]);
            let mut groups = BTreeMap::<Vec<String>, Vec<String>>::new();
            for row in &input.rows {
                groups
                    .entry(tuple(input, row, &columns))
                    .or_default()
                    .push(row_key(row));
            }
            Some(verdict_of(
                groups
                    .into_values()
                    .filter(|g| g.len() > 1)
                    .flatten()
                    .collect(),
            ))
        }
        "foreign_key" => {
            let parent = &case.tables[1].1;
            let known: Vec<Vec<String>> = parent
                .rows
                .iter()
                .map(|row| tuple(parent, row, &["ref"]))
                .collect();
            Some(verdict_of(
                input
                    .rows
                    .iter()
                    .filter(|row| !known.contains(&tuple(input, row, &["ref"])))
                    .map(|row| row_key(row))
                    .collect(),
            ))
        }
        "predicate" => {
            let (operator, operands) = first_entry(&case.check["expr"]);
            let index = input.index(operands[0]["column"].as_str()?);
            let mut failing = Vec::new();
            for row in &input.rows {
                if !current_comparison(operator, &row[index].json, &operands[1], None)? {
                    failing.push(row_key(row));
                }
            }
            Some(verdict_of(failing))
        }
        "aggregate_compare" => {
            let (function, column) = first_entry(&case.check["aggregate"]);
            let index = input.index(column.as_str()?);
            let values = input.rows.iter().map(|row| row[index].json.clone());
            let observed = match function {
                "sum" | "avg" => {
                    let mut sum = 0.0_f64;
                    for value in values {
                        sum += value.as_f64()?;
                    }
                    if function == "avg" {
                        sum /= input.rows.len() as f64;
                    }
                    Value::Number(serde_json::Number::from_f64(sum)?)
                }
                _ => documented_extremum(function, input.kind(column.as_str()?), values)?,
            };
            let compare = &case.check["compare"];
            let (operator, expected) = first_entry(compare);
            // A threshold of another type, null included, has no verdict.
            if std::mem::discriminant(&observed) != std::mem::discriminant(expected)
                || expected.is_null()
            {
                return Some(Verdict::Refused("E_BAD_EXPR".to_owned()));
            }
            let pass =
                current_comparison(operator, &observed, expected, compare["tolerance"].as_f64())?;
            Some(Verdict::Decided {
                pass,
                failing: Vec::new(),
            })
        }
        _ => None,
    }
}

/// True when f64 arithmetic cannot carry this stored number exactly.
fn beyond_f64(kind: Kind, cell: &Cell) -> bool {
    match kind {
        Kind::Int => cell
            .text
            .parse::<i128>()
            .is_ok_and(|v| v.unsigned_abs() >= (1 << 53)),
        Kind::Decimal => cell
            .text
            .parse::<f64>()
            .is_ok_and(|v| (v * 1024.0).fract() != 0.0 || v.abs() >= 9_007_199_254_740_992.0),
        _ => false,
    }
}

fn is_negative_zero(value: &Value) -> bool {
    value
        .as_f64()
        .is_some_and(|v| v == 0.0 && v.is_sign_negative())
}

/// The filed defects whose trigger this case actually contains.
fn oracle_defect_triggers(case: &OracleCase) -> Vec<&'static str> {
    let mut cells: Vec<(Kind, &Cell)> = Vec::new();
    let columns: Vec<String> = match case.label {
        "unique" => strings(&case.check["columns"])
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "foreign_key" => vec!["ref".to_owned()],
        "predicate" => vec![
            first_entry(&case.check["expr"]).1[0]["column"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ],
        "aggregate_compare" => vec![
            first_entry(&case.check["aggregate"])
                .1
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ],
        _ => return Vec::new(),
    };
    for (_, table) in &case.tables {
        for column in &columns {
            let index = table.index(column);
            cells.extend(
                table
                    .rows
                    .iter()
                    .map(|row| (table.kind(column), &row[index])),
            );
        }
    }
    let literal = match case.label {
        "predicate" => first_entry(&case.check["expr"]).1[1].clone(),
        "aggregate_compare" => first_entry(&case.check["compare"]).1.clone(),
        _ => Value::Null,
    };

    let mut triggers = Vec::new();
    let wide_literal = literal
        .as_i64()
        .is_some_and(|v| v.unsigned_abs() >= (1 << 53));
    if wide_literal || cells.iter().any(|(kind, cell)| beyond_f64(*kind, cell)) {
        triggers.push(BEAD_F64_ROUNDING);
    }
    let ordered = match case.label {
        "predicate" => !matches!(first_entry(&case.check["expr"]).0, "eq" | "ne"),
        "aggregate_compare" => !matches!(first_entry(&case.check["compare"]).0, "eq" | "ne"),
        _ => false,
    };
    if ordered
        && (is_negative_zero(&literal) || cells.iter().any(|(_, c)| is_negative_zero(&c.json)))
    {
        triggers.push(BEAD_SIGNED_ZERO_ORDER);
    }
    if matches!(case.label, "unique" | "foreign_key")
        && cells.iter().any(|(_, a)| {
            cells.iter().any(|(_, b)| {
                a.json.is_number()
                    && a.json.as_f64() == b.json.as_f64()
                    && text_identity(&a.json) != text_identity(&b.json)
            })
        })
    {
        triggers.push(BEAD_NUMBER_TEXT_IDENTITY);
    }
    triggers
}

/// Every listed bead must be the ONLY explanation of at least one case in its arm.
#[derive(Default)]
struct KnownHits {
    sole: BTreeMap<&'static str, usize>,
    shared: usize,
}

impl KnownHits {
    fn record(&mut self, triggers: &[&'static str]) {
        if let [only] = triggers {
            *self.sole.entry(only).or_default() += 1;
        } else {
            self.shared += 1;
        }
    }

    fn assert_reached(&self, arm: &str, expected: &[&'static str]) {
        eprintln!(
            "{arm} known divergences: sole={:?} multi-trigger={}",
            self.sole, self.shared
        );
        let reached: Vec<&str> = self.sole.keys().copied().collect();
        let mut expected = expected.to_vec();
        expected.sort_unstable();
        assert_eq!(
            reached, expected,
            "{arm}: the known-divergence list is stale (a fixed or unreached entry must go)"
        );
    }
}

fn operand_category(operand: &Value, table: &Table) -> &'static str {
    match operand {
        Value::Object(fields) => table
            .kind(fields["column"].as_str().expect("column name"))
            .category(),
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
    }
}

/// A comparison that is incomparable by COLUMN type, whatever the cells hold.
fn has_column_type_conflict(expression: &Value, table: &Table) -> bool {
    let Some(fields) = expression.as_object() else {
        return false;
    };
    let Some((operator, argument)) = fields.iter().next() else {
        return false;
    };
    match operator.as_str() {
        "eq" | "ne" => {
            let left = operand_category(&argument[0], table);
            let right = operand_category(&argument[1], table);
            left != right && left != "null" && right != "null"
        }
        "gt" | "gte" | "lt" | "lte" => {
            operand_category(&argument[0], table) != operand_category(&argument[1], table)
        }
        "in" => {
            let left = operand_category(&argument[0], table);
            argument[1].as_array().expect("set").iter().any(|member| {
                let right = operand_category(member, table);
                right != "null" && left != right
            })
        }
        "not" => has_column_type_conflict(argument, table),
        "and" | "or" => argument
            .as_array()
            .expect("parts")
            .iter()
            .any(|part| has_column_type_conflict(part, table)),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// CLI helpers (arm I and the minimized repros)
// ---------------------------------------------------------------------------

/// `VERIFY_DIFFERENTIAL_BIN` points the CLI cases at another build, such as a release.
fn verify_binary() -> PathBuf {
    std::env::var_os("VERIFY_DIFFERENTIAL_BIN").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_verify")),
        PathBuf::from,
    )
}

fn cli_report(directory: &Path, constraints: &ConstraintSet, inputs: &[(&str, &Path)]) -> Value {
    let compiled = directory.join("constraints.verify.json");
    fs::write(
        &compiled,
        serde_json::to_vec(constraints).expect("constraints serialize"),
    )
    .expect("constraints written");
    let mut command = Command::new(verify_binary()); // ubs:ignore — test binary path.
    command.arg("run").arg(&compiled);
    for (name, path) in inputs {
        command
            .arg("--bind")
            .arg(format!("{name}={}", path.display()));
    }
    let output = command
        .args(["--json", "--explicit", "--no-witness"])
        .output()
        .expect("verify runs");
    let report = serde_json::from_slice::<Value>(&output.stdout);
    assert!(
        report.is_ok(),
        "no JSON report ({report:?}); stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    report.expect("asserted above")
}

/// What both lanes must agree on: outcome, the full results, and any refusal code.
fn report_essence(report: &Value) -> Value {
    let mut results = report["results"].clone();
    normalize_numbers(&mut results);
    json!({
        "outcome": report["outcome"],
        "results": results,
        "refusal": report["refusal"]["code"],
    })
}

/// `10` and `10.0` are the same protocol number: an embedded caller may pass either for a
/// DECIMAL or DOUBLE column. Only integral values f64 holds exactly are rewritten.
fn normalize_numbers(value: &mut Value) {
    match value {
        Value::Number(n) if n.is_f64() => {
            let float = n.as_f64().expect("f64 number");
            if float.fract() == 0.0 && float.abs() < 9_007_199_254_740_992.0 {
                *value = json!(float as i64);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(normalize_numbers),
        Value::Object(fields) => fields.values_mut().for_each(normalize_numbers),
        _ => {}
    }
}

/// Text DuckDB's JSON reader turns into a temporal value.
fn reads_as_temporal(text: &str) -> bool {
    matches!(text, "Infinity" | "2024-01-31")
}

fn jsonl(table: &Table) -> String {
    table
        .json_rows()
        .iter()
        .map(|row| serde_json::to_string(row).expect("row serializes") + "\n")
        .collect()
}

// ---------------------------------------------------------------------------
// Arm I: embedded rows against `verify run` over the same logical data
// ---------------------------------------------------------------------------

fn ingestion_check(rng: &mut Rng, table: &Table) -> Value {
    let columns: Vec<(&str, Kind)> = table.columns[1..].to_vec();
    let (column, kind) = *rng.pick(&columns);
    match rng.below(5) {
        0 => json!({ "op": "unique", "binding": "input", "columns": [column] }),
        1 => json!({ "op": "not_null", "binding": "input", "columns": [column] }),
        2 => json!({ "op": "row_count", "binding": "input", "compare": { "gte": 2 } }),
        3 => {
            let function = if kind.category() == "number" {
                *rng.pick(&["sum", "min", "max"])
            } else {
                *rng.pick(&["min", "max"])
            };
            let mut aggregate = serde_json::Map::new();
            aggregate.insert(function.to_owned(), json!(column));
            json!({
                "op": "aggregate_compare", "binding": "input", "aggregate": aggregate,
                "compare": { "lte": pool_value(rng, kind) },
            })
        }
        _ => {
            let operator = *rng.pick(&["eq", "ne", "gte", "lt"]);
            let mut expression = serde_json::Map::new();
            expression.insert(
                operator.to_owned(),
                json!([{ "column": column }, pool_value(rng, kind)]),
            );
            json!({ "op": "predicate", "binding": "input", "expr": expression })
        }
    }
}

fn run_ingestion_arm(arm: u64, cases: u64, format: &str, expected_divergences: &[&'static str]) {
    let directory = tempdir().expect("scratch");
    let mut mismatches = Vec::new();
    let mut known = KnownHits::default();
    for case in 0..cases {
        let seed = case_seed(arm, case);
        let mut rng = Rng::new(seed);
        let columns = [*rng.pick(ORACLE_COLUMNS)];
        let mut table = random_table(&mut rng, &columns);
        // JSON and JSONL carry no schema, so zero rows have no columns to bind.
        if format == "jsonl" {
            while table.rows.is_empty() {
                table = random_table(&mut rng, &columns);
            }
        }
        let check = ingestion_check(&mut rng, &table);
        let constraints = artifact(&["input"], "portable", check.clone());
        let path = directory.path().join(format!("input.{format}"));
        if format == "jsonl" {
            fs::write(&path, jsonl(&table)).expect("jsonl written");
        } else {
            table.write_parquet(&path);
        }
        let embedded = EmbeddedExecutor::evaluate(
            &constraints,
            "sha256:x",
            &embedded_bindings(&[("input", &table)]),
        );
        let embedded = report_essence(&serde_json::to_value(embedded).expect("report"));
        let batch = report_essence(&cli_report(
            directory.path(),
            &constraints,
            &[("input", &path)],
        ));
        if embedded != batch {
            let values: Vec<&Value> = table.rows.iter().map(|row| &row[1].json).collect();
            let known_bead = if check["op"] == "aggregate_compare"
                && embedded["refusal"] == "E_BAD_EXPR"
                && batch["refusal"] == "E_BAD_CONSTRAINTS"
            {
                Some(BEAD_RELATION_REFUSAL_CODE)
            } else if format == "jsonl"
                && batch["refusal"] == "E_FORMAT_DETECT"
                && values.iter().all(|value| value.is_null())
            {
                Some(BEAD_ALL_NULL_JSON)
            } else if format == "jsonl"
                && batch["refusal"] == "E_SQL_ERROR"
                && values.iter().any(|value| value.is_string())
                && values
                    .iter()
                    .all(|value| value.is_null() || value.as_str().is_some_and(reads_as_temporal))
            {
                Some(BEAD_JSON_STRING_COERCION)
            } else {
                None
            };
            match known_bead {
                Some(bead) => known.record(&[bead]),
                None => mismatches.push(format!(
                    "seed {seed:#x} check={check} {}\n  embedded={embedded}\n  batch={batch}",
                    table.show()
                )),
            }
        }
    }
    report_mismatches(&format!("arm I {format}"), &mismatches);
    known.assert_reached(&format!("arm I {format}"), expected_divergences);
}

#[test]
fn arm_i_typed_parquet_batch_matches_embedded() {
    run_ingestion_arm(0x71, 300, "parquet", &[BEAD_RELATION_REFUSAL_CODE]);
}

#[test]
fn arm_i_jsonl_batch_matches_embedded() {
    run_ingestion_arm(
        0x72,
        300,
        "jsonl",
        &[
            BEAD_RELATION_REFUSAL_CODE,
            BEAD_ALL_NULL_JSON,
            BEAD_JSON_STRING_COERCION,
        ],
    );
}

/// CSV carries no types, so only the missingness contract is unambiguous there: null, empty and
/// whitespace-only all violate `not_null` in both lanes.
#[test]
fn arm_i_csv_missingness_matches_embedded() {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.csv");
    fs::write(
        &path,
        "id,s1\nr0,\nr1,\"\"\nr2,\"   \"\nr3,\"\u{a0}\"\nr4,\u{200b}\nr5,null\nr6,a\n",
    )
    .expect("csv written");
    let constraints = artifact(
        &["input"],
        "portable",
        json!({ "op": "not_null", "binding": "input", "columns": ["s1"] }),
    );
    let table = Table {
        columns: vec![("id", Kind::Text), ("s1", Kind::Text)],
        rows: [
            None,
            Some(""),
            Some("   "),
            Some("\u{a0}"),
            Some("\u{200b}"),
            Some("null"),
            Some("a"),
        ]
        .iter()
        .enumerate()
        .map(|(index, value)| {
            vec![
                cell(Kind::Text, &format!("r{index}")),
                value.map_or_else(null_cell, |text| cell(Kind::Text, text)),
            ]
        })
        .collect(),
    };
    let embedded = portable_verdict(&constraints, &[("input", &table)]);
    let report = cli_report(directory.path(), &constraints, &[("input", &path)]);
    let failing: Vec<String> = report["results"][0]["affected"]
        .as_array()
        .expect("affected")
        .iter()
        .map(|entry| entry["key"].to_string())
        .collect();
    assert_eq!(embedded, verdict_of(failing));
}

// ---------------------------------------------------------------------------
// Documented differences and unrepresentable values
// ---------------------------------------------------------------------------

/// The binding-qualified contract needs a unique non-null key; portable predicates do not.
/// These are why arm P generates unique non-null keys.
#[test]
fn binding_lane_refuses_duplicate_and_null_keys_as_documented() {
    let directory = tempdir().expect("scratch");
    let expression = json!({ "gt": [{ "column": "n1" }, 0] });
    let mut qualified = expression.clone();
    qualified["gt"][0]["binding"] = json!("twin");
    for (second_key, expected) in [
        (cell(Kind::Text, "r0"), "E_KEY_AMBIGUOUS"),
        (null_cell(), "E_KEY_INVALID"),
    ] {
        let table = Table {
            columns: vec![("id", Kind::Text), ("n1", Kind::Int)],
            rows: vec![
                vec![cell(Kind::Text, "r0"), cell(Kind::Int, "1")],
                vec![second_key, cell(Kind::Int, "1")],
            ],
        };
        let path = directory.path().join("keys.parquet");
        table.write_parquet(&path);
        let portable = artifact(
            &["input"],
            "portable",
            json!({ "op": "predicate", "binding": "input", "expr": expression }),
        );
        assert!(matches!(
            portable_verdict(&portable, &[("input", &table)]),
            Verdict::Decided { pass: true, .. }
        ));
        assert_eq!(
            duckdb_predicate_verdict(&twin_artifact(&qualified), &path),
            Verdict::Refused(expected.to_owned())
        );
    }
}

/// NaN, the infinities and integers past u64 have no protocol number. Neither lane may give a
/// verdict about them. (JSON cannot carry NaN or infinity, so there is no embedded side.)
#[test]
fn unrepresentable_numbers_never_get_a_verdict_in_either_lane() {
    for (sql_type, literal) in [
        ("DOUBLE", "'NaN'"),
        ("DOUBLE", "'Infinity'"),
        ("DOUBLE", "'-Infinity'"),
        ("FLOAT", "'NaN'"),
        ("HUGEINT", "'170141183460469231731687303715884105727'"),
    ] {
        let directory = tempdir().expect("scratch");
        let path = directory.path().join("input.parquet");
        // HUGEINT has no Parquet type of its own; DuckDB would store it as DOUBLE, so the
        // value is checked through the DuckDB binding lane's in-memory table instead.
        if sql_type == "HUGEINT" {
            let value =
                verify_duckdb::duckdb_value_to_protocol(duckdb::types::Value::HugeInt(i128::MAX));
            assert!(value.is_err(), "{literal}");
            continue;
        }
        Connection::open_in_memory()
            .expect("DuckDB")
            .execute_batch(&format!(
                "CREATE TABLE t (id VARCHAR, v {sql_type}); INSERT INTO t VALUES ('r0', CAST({literal} AS {sql_type})); COPY t TO {} (FORMAT PARQUET);",
                sql_string(&path.to_string_lossy())
            ))
            .expect("parquet written");
        for operator in ["eq", "ne", "gt", "lt"] {
            let mut expression = serde_json::Map::new();
            expression.insert(operator.to_owned(), json!([{ "column": "v" }, 0]));
            let portable = artifact(
                &["input"],
                "portable",
                json!({ "op": "predicate", "binding": "input", "expr": expression }),
            );
            let report = cli_report(directory.path(), &portable, &[("input", &path)]);
            assert_eq!(report["outcome"], "REFUSAL", "{literal} {operator}");

            let mut qualified = Value::Object(expression);
            qualified[operator][0]["binding"] = json!("twin");
            assert!(
                matches!(
                    duckdb_predicate_verdict(&twin_artifact(&qualified), &path),
                    Verdict::Refused(_)
                ),
                "{literal} {operator}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Minimized repros, all through the CLI. Each asserts the DOCUMENTED behaviour and is ignored
// until its bead is fixed. Check a release with:
//   VERIFY_DIFFERENTIAL_BIN=/path/to/verify cargo test -p verify-cli \
//     --test lane_differential repro_ -- --ignored
// ---------------------------------------------------------------------------

fn single_column(kind: Kind, values: &[Option<&str>]) -> Table {
    Table {
        columns: vec![("id", Kind::Text), ("v", kind)],
        rows: values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                vec![
                    cell(Kind::Text, &format!("r{index}")),
                    value.map_or_else(null_cell, |text| cell(kind, text)),
                ]
            })
            .collect(),
    }
}

/// Run one portable rule through `verify run` over typed Parquet.
fn repro_outcome(tables: &[(&str, &Table)], check: Value) -> String {
    let directory = tempdir().expect("scratch");
    let paths: Vec<(&str, PathBuf)> = tables
        .iter()
        .map(|(name, table)| {
            let path = directory.path().join(format!("{name}.parquet"));
            table.write_parquet(&path);
            (*name, path)
        })
        .collect();
    let constraints = artifact(
        &tables.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        "portable",
        check,
    );
    let bound: Vec<(&str, &Path)> = paths
        .iter()
        .map(|(name, path)| (*name, path.as_path()))
        .collect();
    essence_text(&cli_report(directory.path(), &constraints, &bound))
}

fn essence_text(report: &Value) -> String {
    match report["refusal"]["code"].as_str() {
        Some(code) => code.to_owned(),
        None => report["outcome"].as_str().expect("outcome").to_owned(),
    }
}

fn aggregate(function: &str, operator: &str, expected: Value) -> Value {
    let mut aggregate = serde_json::Map::new();
    aggregate.insert(function.to_owned(), json!("v"));
    let mut compare = serde_json::Map::new();
    compare.insert(operator.to_owned(), expected);
    json!({ "op": "aggregate_compare", "binding": "input", "aggregate": aggregate, "compare": compare })
}

fn predicate(operator: &str, literal: Value) -> Value {
    let mut expression = serde_json::Map::new();
    expression.insert(operator.to_owned(), json!([{ "column": "v" }, literal]));
    json!({ "op": "predicate", "binding": "input", "expr": expression })
}

#[test]
fn repro_max_of_9_and_10_is_10() {
    let table = single_column(Kind::Int, &[Some("9"), Some("10")]);
    let max = repro_outcome(&[("input", &table)], aggregate("max", "lte", json!(9)));
    let min = repro_outcome(&[("input", &table)], aggregate("min", "gte", json!(10)));
    assert_eq!((max.as_str(), min.as_str()), ("FAIL", "FAIL"));
}

#[test]
#[ignore = "bd-1t5: integers and decimals are rounded through f64"]
fn repro_wide_numbers_compare_exactly() {
    let wide = single_column(Kind::Int, &[Some("9007199254740993")]);
    let equality = repro_outcome(
        &[("input", &wide)],
        predicate("eq", json!(9_007_199_254_740_992_i64)),
    );
    let parts = single_column(Kind::Int, &[Some("9007199254740992"), Some("1")]);
    let sum = repro_outcome(
        &[("input", &parts)],
        aggregate("sum", "eq", json!(9_007_199_254_740_992_i64)),
    );
    let cents = single_column(Kind::Decimal, &[Some("0.1"), Some("0.2")]);
    let decimal_sum = repro_outcome(&[("input", &cents)], aggregate("sum", "gt", json!(0.3)));
    let child = single_column(Kind::Decimal, &[Some("12345678901234567.88")]);
    let parent = single_column(Kind::Decimal, &[Some("12345678901234567.89")]);
    let foreign_key = repro_outcome(
        &[("input", &child), ("parent", &parent)],
        json!({
            "op": "foreign_key", "binding": "input", "columns": ["v"],
            "ref_binding": "parent", "ref_columns": ["v"],
        }),
    );
    assert_eq!(
        [equality, sum, decimal_sum, foreign_key],
        ["FAIL", "FAIL", "FAIL", "FAIL"]
    );
}

#[test]
#[ignore = "bd-der: -0.0 is ordered below 0.0 although it equals it"]
fn repro_negative_zero_is_not_below_zero() {
    let table = single_column(Kind::Double, &[Some("-0.0")]);
    let below = repro_outcome(&[("input", &table)], predicate("lt", json!(0.0)));
    let at_least = repro_outcome(&[("input", &table)], predicate("gte", json!(0.0)));
    assert_eq!((below.as_str(), at_least.as_str()), ("FAIL", "PASS"));
}

#[test]
#[ignore = "bd-v3r: unique and foreign_key match numbers by their text"]
fn repro_equal_numbers_are_one_value_for_unique_and_foreign_key() {
    let zeros = single_column(Kind::Double, &[Some("0.0"), Some("-0.0")]);
    let unique = repro_outcome(
        &[("input", &zeros)],
        json!({ "op": "unique", "binding": "input", "columns": ["v"] }),
    );
    let child = single_column(Kind::Double, &[Some("1.0")]);
    let parent = single_column(Kind::Int, &[Some("1")]);
    let foreign_key = repro_outcome(
        &[("input", &child), ("parent", &parent)],
        json!({
            "op": "foreign_key", "binding": "input", "columns": ["v"],
            "ref_binding": "parent", "ref_columns": ["v"],
        }),
    );
    assert_eq!((unique.as_str(), foreign_key.as_str()), ("FAIL", "PASS"));
}

/// The same predicate through the binding-qualified lane of `verify run`.
fn repro_binding_outcome(table: &Table, expression: Value) -> String {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.parquet");
    table.write_parquet(&path);
    let report = cli_report(
        directory.path(),
        &twin_artifact(&expression),
        &[("input", &path), ("twin", &path)],
    );
    essence_text(&report)
}

#[test]
#[ignore = "bd-mhl: the binding-qualified lane refuses by column type, even for null cells"]
fn repro_null_against_another_type_is_unequal_in_both_lanes() {
    let table = single_column(Kind::Text, &[None]);
    let portable = repro_outcome(&[("input", &table)], predicate("eq", json!(5)));
    let qualified = repro_binding_outcome(
        &table,
        json!({ "eq": [{ "binding": "twin", "column": "v" }, 5] }),
    );
    assert_eq!(portable, "FAIL");
    assert_eq!(qualified, portable);
}

#[test]
#[ignore = "bd-1xg: the binding-qualified lane refuses presence checks on temporal columns"]
fn repro_temporal_presence_checks_work_in_both_lanes() {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.parquet");
    Connection::open_in_memory()
        .expect("DuckDB")
        .execute_batch(&format!(
            "CREATE TABLE t (id VARCHAR, v DATE); INSERT INTO t VALUES ('r0', DATE '2024-01-31'); COPY t TO {} (FORMAT PARQUET);",
            sql_string(&path.to_string_lossy())
        ))
        .expect("parquet written");
    let portable = artifact(
        &["input"],
        "portable",
        json!({ "op": "predicate", "binding": "input", "expr": { "not": { "is_null": { "column": "v" } } } }),
    );
    let portable = essence_text(&cli_report(
        directory.path(),
        &portable,
        &[("input", &path)],
    ));
    let qualified =
        twin_artifact(&json!({ "not": { "is_null": { "binding": "twin", "column": "v" } } }));
    let qualified = essence_text(&cli_report(
        directory.path(),
        &qualified,
        &[("input", &path), ("twin", &path)],
    ));
    assert_eq!(portable, "PASS");
    assert_eq!(qualified, portable);
}

#[test]
#[ignore = "bd-uj2: batch and embedded give different codes for the same aggregate refusal"]
fn repro_aggregate_refusal_code_matches_embedded() {
    let table = single_column(Kind::Text, &[Some("a")]);
    let check = aggregate("sum", "gt", json!(0));
    let embedded = portable_verdict(
        &artifact(&["input"], "portable", check.clone()),
        &[("input", &table)],
    );
    let batch = repro_outcome(&[("input", &table)], check);
    assert_eq!(embedded, Verdict::Refused(batch));
}

fn repro_jsonl_outcome(lines: &str, check: Value) -> String {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.jsonl");
    fs::write(&path, lines).expect("jsonl written");
    let constraints = artifact(&["input"], "portable", check);
    essence_text(&cli_report(
        directory.path(),
        &constraints,
        &[("input", &path)],
    ))
}

#[test]
#[ignore = "bd-ffj: an all-null JSON column is refused as nested data"]
fn repro_all_null_jsonl_column_is_accepted() {
    let outcome = repro_jsonl_outcome(
        "{\"id\":\"r0\",\"v\":null}\n",
        json!({ "op": "row_count", "binding": "input", "compare": { "eq": 1 } }),
    );
    assert_eq!(outcome, "PASS");
}

#[test]
#[ignore = "bd-yty: JSON strings are coerced to temporal types by the reader"]
fn repro_jsonl_strings_stay_strings() {
    let outcome = repro_jsonl_outcome(
        "{\"id\":\"r0\",\"v\":\"Infinity\"}\n",
        predicate("eq", json!("Infinity")),
    );
    assert_eq!(outcome, "PASS");
}

/// Two different strings must stay unequal when the reader takes both for timestamps.
#[test]
#[ignore = "bd-yty: JSON strings are coerced to temporal types by the reader"]
fn repro_distinct_jsonl_strings_are_not_equal_across_bindings() {
    let directory = tempdir().expect("scratch");
    let current = directory.path().join("input.jsonl");
    let prior = directory.path().join("twin.jsonl");
    fs::write(&current, "{\"id\":\"r0\",\"v\":\"Infinity\"}\n").expect("jsonl written");
    fs::write(&prior, "{\"id\":\"r0\",\"v\":\"infinity\"}\n").expect("jsonl written");
    let constraints = twin_artifact(&json!({
        "eq": [{ "column": "v" }, { "binding": "twin", "column": "v" }]
    }));
    let report = cli_report(
        directory.path(),
        &constraints,
        &[("input", &current), ("twin", &prior)],
    );
    assert_eq!(essence_text(&report), "FAIL");
}

#[test]
#[ignore = "bd-7k7: binding-qualified keys are matched after rounding to DOUBLE"]
fn repro_wide_integer_keys_do_not_match_a_neighbour() {
    let directory = tempdir().expect("scratch");
    let key_table = |key: &str| Table {
        columns: vec![("id", Kind::Int), ("v", Kind::Int)],
        rows: vec![vec![cell(Kind::Int, key), cell(Kind::Int, "1")]],
    };
    let current = directory.path().join("input.parquet");
    let prior = directory.path().join("twin.parquet");
    key_table("9007199254740993").write_parquet(&current);
    key_table("9007199254740992").write_parquet(&prior);
    let constraints = twin_artifact(&json!({
        "eq": [{ "column": "v" }, { "binding": "twin", "column": "v" }]
    }));
    let report = cli_report(
        directory.path(),
        &constraints,
        &[("input", &current), ("twin", &prior)],
    );
    assert_eq!(essence_text(&report), "E_KEY_UNMATCHED");
}

#[test]
#[ignore = "bd-wlx: `IS DISTINCT FROM 'text'` is refused as a file scan"]
fn repro_is_distinct_from_a_string_literal_is_a_read_only_query() {
    let directory = tempdir().expect("scratch");
    let path = directory.path().join("input.parquet");
    single_column(Kind::Text, &[Some("a")]).write_parquet(&path);
    let constraints = artifact(
        &["input"],
        "batch_only",
        json!({
            "op": "query_zero_rows", "bindings": ["input"],
            "query": "SELECT id AS key__id FROM input WHERE v IS DISTINCT FROM 'a'",
        }),
    );
    let report = cli_report(directory.path(), &constraints, &[("input", &path)]);
    assert_eq!(essence_text(&report), "PASS");
}

// ---------------------------------------------------------------------------
// Arm T: temporal comparisons
// ---------------------------------------------------------------------------

#[test]
fn arm_t_duckdb_temporal_comparisons_match_iso_string_order() {
    const POOLS: &[(&str, &[&str])] = &[
        (
            "DATE",
            &[
                "1900-02-28",
                "2024-01-31",
                "2024-02-01",
                "2024-02-29",
                "9999-12-31",
            ],
        ),
        ("TIME", &["00:00:00", "09:15:00", "10:00:00", "23:59:59"]),
        (
            "TIMESTAMP",
            &[
                "1969-12-31 23:59:59",
                "1970-01-01 00:00:00",
                "2024-01-31 00:00:00",
                "2024-01-31 09:15:00",
                "2024-02-01 00:00:00",
            ],
        ),
    ];
    let directory = tempdir().expect("scratch");
    let mut mismatches = Vec::new();
    for case in 0..300_u64 {
        let seed = case_seed(0x81, case);
        let mut rng = Rng::new(seed);
        let (sql_type, pool) = *rng.pick(POOLS);
        let row_count = 1 + rng.below(4);
        let mut text_rows = Vec::new();
        let mut sql_rows = Vec::new();
        for index in 0..row_count {
            let mut values = Vec::new();
            for _ in 0..2 {
                values.push(if rng.chance(15) {
                    None
                } else {
                    Some(*rng.pick(pool))
                });
            }
            let id = cell(Kind::Text, &format!("r{index}"));
            let as_text: Vec<Cell> = std::iter::once(id.clone())
                .chain(
                    values
                        .iter()
                        .map(|value| value.map_or_else(null_cell, |text| cell(Kind::Text, text))),
                )
                .collect();
            let literals: Vec<String> = std::iter::once(id.sql)
                .chain(values.iter().map(|value| {
                    value.map_or_else(
                        || "NULL".to_owned(),
                        |text| format!("CAST('{text}' AS {sql_type})"),
                    )
                }))
                .collect();
            text_rows.push(as_text);
            sql_rows.push(format!("({})", literals.join(", ")));
        }
        let text_table = Table {
            columns: vec![("id", Kind::Text), ("t1", Kind::Text), ("t2", Kind::Text)],
            rows: text_rows,
        };
        let path = directory.path().join("temporal.parquet");
        let connection = Connection::open_in_memory().expect("DuckDB");
        // Fixture DDL: the type, values and temp path all come from this test.
        connection // ubs:ignore — test-owned fixture SQL, no outside input.
            .execute_batch(&format!(
                "CREATE TABLE t (id VARCHAR, t1 {sql_type}, t2 {sql_type}); INSERT INTO t VALUES {}; COPY t TO {} (FORMAT PARQUET);",
                sql_rows.join(", "),
                sql_string(&path.to_string_lossy())
            ))
            .expect("temporal parquet");

        let operator = *rng.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]);
        let mut expression = serde_json::Map::new();
        expression.insert(
            operator.to_owned(),
            json!([{ "column": "t1" }, { "column": "t2" }]),
        );
        let expression = Value::Object(expression);
        let portable = artifact(
            &["input"],
            "portable",
            json!({ "op": "predicate", "binding": "input", "expr": expression }),
        );
        let mut qualified = expression.clone();
        qualified[operator][1]["binding"] = json!("twin");
        let batch = twin_artifact(&qualified);
        let left = portable_verdict(&portable, &[("input", &text_table)]);
        let right = duckdb_predicate_verdict(&batch, &path);
        if left != right {
            mismatches.push(format!(
                "seed {seed:#x} {sql_type} {operator} {}\n  portable-iso={left:?}\n  duckdb={right:?}",
                text_table.show()
            ));
        }
    }
    report_mismatches("arm T", &mismatches);
}
