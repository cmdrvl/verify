use std::collections::BTreeMap;

use duckdb::Connection;
use serde_json::{Value, json};
use verify_core::{
    constraint::{Check, Portability, Rule},
    order::sort_affected_entries,
    refusal::{Refusal, RefusalCode},
    report::{AffectedEntry, ResultStatus, RuleResult},
};

use crate::{
    BindingRegistry,
    fetch::{Cell, query_cells},
};

pub const QUERY_RULE_OP: &str = "query_zero_rows";

#[derive(Debug, Clone, Default)]
pub struct QueryRuleExecutor;

impl QueryRuleExecutor {
    pub fn evaluate_rule(
        rule: &Rule,
        connection: &Connection,
        bindings: &BindingRegistry,
    ) -> Result<RuleResult, QueryRuleError> {
        evaluate_rule(rule, connection, bindings)
    }
}

#[derive(Debug)]
pub enum QueryRuleError {
    MissingBinding {
        rule_id: String,
        binding: String,
    },
    SqlError {
        rule_id: String,
        bindings: Vec<String>,
        query: String,
        message: String,
    },
    BadConstraints {
        rule_id: String,
        detail: String,
    },
}

impl QueryRuleError {
    pub fn refusal_code(&self) -> RefusalCode {
        match self {
            Self::MissingBinding { .. } => RefusalCode::MissingBinding,
            Self::SqlError { .. } => RefusalCode::SqlError,
            Self::BadConstraints { .. } => RefusalCode::BadConstraints,
        }
    }

    pub fn to_refusal(&self) -> Refusal {
        Refusal::new(self.refusal_code(), self.to_string(), self.detail())
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::MissingBinding { rule_id, binding } => json!({
                "rule_id": rule_id,
                "binding": binding,
            }),
            Self::SqlError {
                rule_id,
                bindings,
                query,
                message,
            } => json!({
                "rule_id": rule_id,
                "bindings": bindings,
                "query": query,
                "message": message,
            }),
            Self::BadConstraints { rule_id, detail } => json!({
                "rule_id": rule_id,
                "detail": detail,
            }),
        }
    }
}

impl std::fmt::Display for QueryRuleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBinding { rule_id, binding } => write!(
                formatter,
                "query_zero_rows rule {rule_id} references missing binding {binding}"
            ),
            Self::SqlError {
                rule_id, message, ..
            } => {
                write!(formatter, "SQL error in rule {rule_id}: {message}")
            }
            Self::BadConstraints { rule_id, detail } => {
                write!(
                    formatter,
                    "query_zero_rows rule {rule_id} is invalid: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for QueryRuleError {}

pub fn evaluate_rule(
    rule: &Rule,
    connection: &Connection,
    bindings: &BindingRegistry,
) -> Result<RuleResult, QueryRuleError> {
    let Check::QueryZeroRows {
        bindings: declared_bindings,
        ..
    } = &rule.check
    else {
        return Err(QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
        });
    };

    if !matches!(rule.portability, Portability::BatchOnly) {
        return Err(QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: "query_zero_rows rules must declare portability=batch_only".to_owned(),
        });
    }

    let default_binding =
        declared_bindings
            .first()
            .ok_or_else(|| QueryRuleError::BadConstraints {
                rule_id: rule.id.clone(),
                detail: "query_zero_rows rule must declare at least one binding".to_owned(),
            })?;

    for binding in declared_bindings {
        if bindings.get(binding).is_none() {
            return Err(QueryRuleError::MissingBinding {
                rule_id: rule.id.clone(),
                binding: binding.clone(),
            });
        }
    }

    evaluate_query_rule(rule, connection, default_binding)
}

pub fn evaluate_query_rule(
    rule: &Rule,
    connection: &Connection,
    default_binding: &str,
) -> Result<RuleResult, QueryRuleError> {
    let query = match &rule.check {
        Check::QueryZeroRows { query, .. } => query,
        _ => {
            return Err(QueryRuleError::BadConstraints {
                rule_id: rule.id.clone(),
                detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
            });
        }
    };

    // Refuse before the connection ever sees the text. The connection is also
    // locked down (see `lock_down`); this check gives a typed, deterministic
    // refusal instead of depending on DuckDB's own permission errors.
    validate_read_only_query(query).map_err(|reason| QueryRuleError::BadConstraints {
        rule_id: rule.id.clone(),
        detail: format!("query is not read-only: {reason}"),
    })?;

    let mut statement = connection
        .prepare(query)
        .map_err(|error| sql_error(rule, error.to_string()))?;
    // The user's statement is executed as written; only the reading differs.
    let result =
        query_cells(&mut statement, []).map_err(|error| sql_error(rule, error.to_string()))?;

    let mut affected = Vec::new();
    for cells in result.rows {
        let mut values = BTreeMap::new();
        for (name, cell) in result.names.iter().zip(cells) {
            values.insert(name.clone(), cell_to_json(cell));
        }
        affected.push(row_to_affected_entry(&values, default_binding));
    }

    sort_affected_entries(&mut affected);
    let status = if affected.is_empty() {
        ResultStatus::Pass
    } else {
        ResultStatus::Fail
    };

    Ok(RuleResult {
        rule_id: rule.id.clone(),
        severity: rule.severity,
        status,
        violation_count: affected.len(),
        affected,
    })
}

/// First words a `query_zero_rows` statement may start with (`FROM t` is DuckDB's
/// FROM-first select).
const ALLOWED_FIRST_WORDS: &[&str] = &["SELECT", "WITH", "VALUES", "FROM"];

/// Words refused anywhere outside strings/comments/quoted identifiers: they start
/// DDL, DML, `COPY`, `ATTACH` or `PRAGMA`, and could hide behind a leading `WITH`.
const FORBIDDEN_ANYWHERE: &[&str] = &[
    "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "COPY", "ATTACH", "DETACH", "PRAGMA",
    "INSTALL",
];

/// Functions (when called) that read files, URLs or the environment. The locked
/// connection is the real barrier; this list gives an early typed refusal.
const FORBIDDEN_FUNCTIONS: &[&str] = &[
    "GLOB",
    "SNIFF_CSV",
    "GETENV",
    "PARQUET_SCAN",
    "PARQUET_METADATA",
    "PARQUET_SCHEMA",
    "PARQUET_FILE_METADATA",
    "PARQUET_KV_METADATA",
    "SQLITE_SCAN",
    "POSTGRES_SCAN",
    "MYSQL_SCAN",
    "ICEBERG_SCAN",
    "DELTA_SCAN",
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlToken {
    Word { upper: String, is_call: bool },
    SingleQuoted,
    Other,
}

/// Accept a single read-only query and refuse everything else, deterministically,
/// without touching a database. Strings, quoted identifiers and comments are skipped
/// so a literal like `'DROP TABLE'` is fine; an unquoted column that collides with a
/// refused keyword must be double-quoted.
///
/// Refused: more than one statement; a first word other than `SELECT`, `WITH`,
/// `VALUES` or `FROM` (so `COPY`, `ATTACH`, `DETACH`, `PRAGMA`, `LOAD`, `INSTALL`,
/// `SET`, DDL and DML never start); DDL/DML/`COPY`/`ATTACH`/`PRAGMA`/`INSTALL` words
/// anywhere (`WITH x AS (...) DELETE ...`); calls to `read_*` and other file,
/// network or environment functions; a quoted path in `FROM`/`JOIN`
/// (`FROM 'https://...'`, `FROM '/etc/x.csv'`).
pub fn validate_read_only_query(query: &str) -> Result<(), String> {
    let statements = tokenize_statements(query)?;
    let non_empty: Vec<&Vec<SqlToken>> = statements.iter().filter(|s| !s.is_empty()).collect();
    let [statement] = non_empty.as_slice() else {
        return Err(if non_empty.is_empty() {
            "the query is empty".to_owned()
        } else {
            "multiple statements; exactly one is allowed".to_owned()
        });
    };

    match statement.first() {
        Some(SqlToken::Word { upper, .. }) if ALLOWED_FIRST_WORDS.contains(&upper.as_str()) => {}
        Some(SqlToken::Word { upper, .. }) => {
            return Err(format!(
                "statement starts with {upper}; only SELECT, WITH, VALUES or FROM queries are allowed"
            ));
        }
        _ => return Err("statement does not start with a SELECT, WITH, VALUES or FROM".to_owned()),
    }

    let mut previous_word: Option<&str> = None;
    for token in statement.iter() {
        match token {
            SqlToken::Word { upper, is_call } => {
                if FORBIDDEN_ANYWHERE.contains(&upper.as_str()) {
                    return Err(format!("keyword {upper} is not allowed"));
                }
                if *is_call
                    && (upper.starts_with("READ_") || FORBIDDEN_FUNCTIONS.contains(&upper.as_str()))
                {
                    return Err(format!(
                        "function {upper} reads outside the bound relations"
                    ));
                }
                previous_word = Some(upper.as_str());
            }
            SqlToken::SingleQuoted => {
                if matches!(previous_word, Some("FROM" | "JOIN")) {
                    return Err(
                        "a quoted path or URL in FROM/JOIN is a file scan and is not allowed"
                            .to_owned(),
                    );
                }
                previous_word = None;
            }
            SqlToken::Other => previous_word = None,
        }
    }
    Ok(())
}

/// Split into statements on top-level `;`, skipping comments and quoted text.
fn tokenize_statements(query: &str) -> Result<Vec<Vec<SqlToken>>, String> {
    let chars: Vec<char> = query.chars().collect();
    let mut statements = vec![Vec::new()];
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        let next = chars.get(index + 1).copied();
        match current {
            c if c.is_whitespace() => index += 1,
            '-' if next == Some('-') => {
                while index < chars.len() && chars[index] != '\n' {
                    index += 1;
                }
            }
            '/' if next == Some('*') => {
                let mut depth = 1usize;
                index += 2;
                while depth > 0 {
                    match (chars.get(index), chars.get(index + 1)) {
                        (Some('/'), Some('*')) => {
                            depth += 1;
                            index += 2;
                        }
                        (Some('*'), Some('/')) => {
                            depth -= 1;
                            index += 2;
                        }
                        (Some(_), _) => index += 1,
                        (None, _) => return Err("unterminated /* comment".to_owned()),
                    }
                }
            }
            '\'' => {
                // `E'...'` strings treat a backslash as an escape.
                let escape_string = index > 0
                    && matches!(chars[index - 1], 'e' | 'E')
                    && (index < 2 || !is_word_char(chars[index - 2]));
                index = skip_quoted(&chars, index, '\'', escape_string)?;
                push(&mut statements, SqlToken::SingleQuoted);
            }
            '"' => {
                index = skip_quoted(&chars, index, '"', false)?;
                push(&mut statements, SqlToken::Other);
            }
            '$' if next == Some('$') || next.is_some_and(is_word_start) => {
                index = skip_dollar_quoted(&chars, index)?;
                push(&mut statements, SqlToken::SingleQuoted);
            }
            ';' => {
                statements.push(Vec::new());
                index += 1;
            }
            c if is_word_start(c) => {
                let start = index;
                while index < chars.len() && is_word_char(chars[index]) {
                    index += 1;
                }
                let upper: String = chars[start..index]
                    .iter()
                    .collect::<String>()
                    .to_uppercase();
                let mut lookahead = index;
                while lookahead < chars.len() && chars[lookahead].is_whitespace() {
                    lookahead += 1;
                }
                let is_call = chars.get(lookahead) == Some(&'(');
                push(&mut statements, SqlToken::Word { upper, is_call });
            }
            c if c.is_ascii_digit() => {
                // Numbers such as 1e5 or 0x1F are one token, not a word.
                while index < chars.len() && (is_word_char(chars[index]) || chars[index] == '.') {
                    index += 1;
                }
                push(&mut statements, SqlToken::Other);
            }
            _ => {
                index += 1;
                push(&mut statements, SqlToken::Other);
            }
        }
    }
    Ok(statements)
}

fn push(statements: &mut [Vec<SqlToken>], token: SqlToken) {
    if let Some(last) = statements.last_mut() {
        last.push(token);
    }
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// `start` is at the opening quote; returns the index after the closing quote.
fn skip_quoted(
    chars: &[char],
    start: usize,
    quote: char,
    backslash_escapes: bool,
) -> Result<usize, String> {
    let mut index = start + 1;
    while index < chars.len() {
        if backslash_escapes && chars[index] == '\\' {
            index += 2;
            continue;
        }
        if chars[index] == quote {
            if chars.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(format!("unterminated {quote}-quoted text"))
}

/// `start` is at the first `$` of `$$` or `$tag$`; returns the index after the closing tag.
fn skip_dollar_quoted(chars: &[char], start: usize) -> Result<usize, String> {
    let mut end_of_tag = start + 1;
    while end_of_tag < chars.len() && chars[end_of_tag] != '$' {
        if !is_word_char(chars[end_of_tag]) || chars[end_of_tag] == '$' {
            return Err("malformed dollar-quoted string".to_owned());
        }
        end_of_tag += 1;
    }
    if end_of_tag >= chars.len() {
        return Err("malformed dollar-quoted string".to_owned());
    }
    let tag: Vec<char> = chars[start..=end_of_tag].to_vec();
    let mut index = end_of_tag + 1;
    while index + tag.len() <= chars.len() {
        if chars[index..index + tag.len()] == tag[..] {
            return Ok(index + tag.len());
        }
        index += 1;
    }
    Err("unterminated dollar-quoted string".to_owned())
}

pub fn execute_query_rules(
    connection: &Connection,
    rules: &[Rule],
) -> Result<Vec<RuleResult>, QueryRuleError> {
    let mut results = Vec::new();
    for rule in rules {
        let Check::QueryZeroRows { bindings, .. } = &rule.check else {
            continue;
        };
        let default_binding = bindings.first().map(String::as_str).unwrap_or("unknown");
        results.push(evaluate_query_rule(rule, connection, default_binding)?);
    }
    Ok(results)
}

fn row_to_affected_entry(row: &BTreeMap<String, Value>, default_binding: &str) -> AffectedEntry {
    let binding = row
        .get("binding")
        .and_then(|value| value.as_str())
        .unwrap_or(default_binding)
        .to_owned();

    let field = row
        .get("field")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);

    let value = row.get("value").cloned();

    let mut key = BTreeMap::new();
    for (column_name, column_value) in row {
        if let Some(key_field) = column_name.strip_prefix("key__")
            && !key_field.is_empty()
        {
            key.insert(key_field.to_owned(), column_value.clone());
        }
    }

    AffectedEntry {
        binding,
        key: if key.is_empty() { None } else { Some(key) },
        field,
        value,
    }
}

/// A query result's value for the report; no verdict depends on it.
fn cell_to_json(cell: Cell) -> Value {
    match cell {
        Cell::Value(value) => duckdb_to_json(value),
        // A number if no digit is lost, otherwise its exact text, as a HugeInt is shown.
        Cell::WideDecimal(decimal) => decimal
            .lossless_number()
            .map_or_else(|| Value::String(decimal.text()), Value::Number),
        Cell::WideNested(nested) => Value::String(nested.debug_text()),
    }
}

fn duckdb_to_json(value: duckdb::types::Value) -> Value {
    match value {
        duckdb::types::Value::Null => Value::Null,
        duckdb::types::Value::Boolean(value) => Value::Bool(value),
        duckdb::types::Value::TinyInt(value) => Value::Number(value.into()),
        duckdb::types::Value::SmallInt(value) => Value::Number(value.into()),
        duckdb::types::Value::Int(value) => Value::Number(value.into()),
        duckdb::types::Value::BigInt(value) => Value::Number(value.into()),
        duckdb::types::Value::HugeInt(value) => Value::String(value.to_string()),
        duckdb::types::Value::UTinyInt(value) => Value::Number(value.into()),
        duckdb::types::Value::USmallInt(value) => Value::Number(value.into()),
        duckdb::types::Value::UInt(value) => Value::Number(value.into()),
        duckdb::types::Value::UBigInt(value) => Value::Number(value.into()),
        duckdb::types::Value::Float(value) => {
            serde_json::Number::from_f64(f64::from(value)).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Double(value) => {
            serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Decimal(value) => serde_json::from_str(&value.to_string())
            .unwrap_or_else(|_| Value::String(value.to_string())),
        duckdb::types::Value::Text(value) => Value::String(value),
        _ => Value::String(format!("{value:?}")),
    }
}

fn sql_error(rule: &Rule, message: String) -> QueryRuleError {
    match &rule.check {
        Check::QueryZeroRows { bindings, query } => QueryRuleError::SqlError {
            rule_id: rule.id.clone(),
            bindings: bindings.clone(),
            query: query.clone(),
            message,
        },
        _ => QueryRuleError::BadConstraints {
            rule_id: rule.id.clone(),
            detail: format!("expected {QUERY_RULE_OP}, got {}", rule.check.op()),
        },
    }
}

#[cfg(test)]
mod read_only_tests {
    use super::validate_read_only_query;

    fn refused(query: &str) -> String {
        validate_read_only_query(query).expect_err(&format!("query must be refused: {query}"))
    }

    #[test]
    fn hostile_statements_are_refused_with_a_reason() {
        let cases: &[(&str, &str)] = &[
            ("COPY (SELECT 1) TO '/tmp/exfil.csv'", "starts with COPY"),
            ("copy (select 1) to '/tmp/exfil.csv'", "starts with COPY"),
            ("/* hide */ CoPy (SELECT 1) TO 'x'", "starts with COPY"),
            ("ATTACH '/tmp/other.db' AS other", "starts with ATTACH"),
            ("DETACH other", "starts with DETACH"),
            ("INSTALL httpfs", "starts with INSTALL"),
            ("LOAD httpfs", "starts with LOAD"),
            ("SET enable_external_access = true", "starts with SET"),
            ("PRAGMA database_list", "starts with PRAGMA"),
            ("INSERT INTO t VALUES (1)", "starts with INSERT"),
            ("UPDATE t SET a = 1", "starts with UPDATE"),
            ("DELETE FROM t", "starts with DELETE"),
            ("CREATE TABLE t (a INT)", "starts with CREATE"),
            ("DROP TABLE t", "starts with DROP"),
            ("ALTER TABLE t ADD COLUMN b INT", "starts with ALTER"),
            ("EXPORT DATABASE '/tmp/dump'", "starts with EXPORT"),
            ("CALL pragma_version()", "starts with CALL"),
        ];
        for (query, reason) in cases {
            let message = refused(query);
            assert!(message.contains(reason), "{query}: {message}");
        }
    }

    #[test]
    fn multiple_statements_are_refused_even_when_the_first_is_a_select() {
        for query in [
            "SELECT 1; SELECT 2",
            "SELECT 1; DROP TABLE t",
            "SELECT 1; COPY (SELECT 1) TO '/tmp/x'",
            "SELECT 1 /* ; */ ; SELECT 2",
            "SELECT 1;;SELECT 2",
        ] {
            assert!(refused(query).contains("multiple statements"), "{query}");
        }
    }

    #[test]
    fn writes_hidden_behind_a_leading_with_are_refused() {
        for (query, word) in [
            ("WITH x AS (SELECT 1 AS a) DELETE FROM t", "DELETE"),
            (
                "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM x",
                "INSERT",
            ),
            ("WITH x AS (SELECT 1) SELECT * FROM x; ", ""),
        ] {
            if word.is_empty() {
                // Control: a trailing `;` after one read-only statement is fine.
                assert_eq!(validate_read_only_query(query), Ok(()), "{query}");
            } else {
                assert!(refused(query).contains(word), "{query}");
            }
        }
    }

    #[test]
    fn file_network_and_environment_reads_are_refused() {
        for query in [
            "SELECT * FROM read_csv_auto('/etc/passwd')",
            "SELECT * FROM READ_CSV ('/etc/passwd')",
            "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
            "SELECT * FROM read_json('https://example.com/x.json')",
            "SELECT * FROM read_text('/etc/hosts')",
            "SELECT * FROM glob('/*')",
            "SELECT getenv('HOME')",
            "SELECT * FROM parquet_scan('x.parquet')",
            "SELECT * FROM parquet_metadata('x.parquet')",
            "SELECT * FROM sniff_csv('x.csv')",
            "SELECT 1 WHERE EXISTS (SELECT * FROM read_blob('/etc/hosts'))",
        ] {
            assert!(
                refused(query).contains("reads outside the bound relations"),
                "{query}"
            );
        }
        for query in [
            "SELECT * FROM 'https://example.com/x.csv'",
            "SELECT * FROM '/etc/hosts.csv'",
            "SELECT * FROM a JOIN 's3://bucket/x.parquet' USING (id)",
            "FROM '/tmp/x.parquet' SELECT *",
            "SELECT * FROM $$/etc/x.csv$$",
        ] {
            assert!(refused(query).contains("file scan"), "{query}");
        }
    }

    #[test]
    fn empty_and_malformed_text_is_refused() {
        for query in ["", "   ", ";", "-- only a comment", "/* only a comment */"] {
            assert!(refused(query).contains("empty"), "{query:?}");
        }
        for query in [
            "SELECT 'open",
            "SELECT \"open",
            "SELECT 1 /* open",
            "SELECT $$open",
        ] {
            assert!(refused(query).contains("unterminated"), "{query}");
        }
    }

    #[test]
    fn ordinary_read_only_queries_still_pass() {
        for query in [
            "SELECT * FROM current WHERE balance < 0",
            "SELECT * FROM current;",
            "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
            "FROM current SELECT loan_id",
            "VALUES (1), (2)",
            "SELECT 'DROP TABLE x; COPY' AS note",
            "SELECT \"update\", \"read_csv\" FROM current",
            "SELECT read_date, load_id FROM current",
            "SELECT $$copy; attach$$ AS note",
            "SELECT E'it\\'s; COPY' AS note",
            "SELECT 1e5, 0x1F, 1.5",
            "SELECT 1 -- ; DROP TABLE t",
            "SELECT a.x FROM a JOIN b ON a.id = b.id WHERE a.s IN ('COPY', 'ATTACH')",
            "SELECT * FROM current c JOIN prior p ON c.id = p.id",
        ] {
            assert_eq!(validate_read_only_query(query), Ok(()), "{query}");
        }
    }
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;
    use serde_json::json;
    use verify_core::constraint::{Check, Portability, Rule, Severity};
    use verify_core::report::ResultStatus;

    use super::{evaluate_query_rule, execute_query_rules};

    fn setup_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory DuckDB opens");
        conn.execute_batch(
            "CREATE TABLE property (property_id TEXT, tenant_id TEXT);
             INSERT INTO property VALUES ('P-001', 'T-001');
             INSERT INTO property VALUES ('P-002', 'T-002');
             INSERT INTO property VALUES ('P-003', 'T-999');
             CREATE TABLE tenants (tenant_id TEXT, name TEXT);
             INSERT INTO tenants VALUES ('T-001', 'Acme');
             INSERT INTO tenants VALUES ('T-002', 'Beta');",
        )
        .expect("test data inserts");
        conn
    }

    fn make_query_rule(id: &str, query: &str) -> Rule {
        Rule {
            id: id.to_owned(),
            severity: Severity::Error,
            portability: Portability::BatchOnly,
            check: Check::QueryZeroRows {
                bindings: vec!["property".to_owned(), "tenants".to_owned()],
                query: query.to_owned(),
            },
        }
    }

    #[test]
    fn zero_rows_means_pass() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "NO_VIOLATIONS",
            "SELECT 'property' AS binding FROM property WHERE 1 = 0",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Pass));
        assert_eq!(result.violation_count, 0);
        assert!(result.affected.is_empty());
    }

    #[test]
    fn returned_rows_mean_fail_with_localization() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "ORPHAN_CHECK",
            "SELECT 'property' AS binding, 'tenant_id' AS field, property.tenant_id AS value, property.property_id AS key__property_id FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE tenants.tenant_id IS NULL",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.violation_count, 1);

        let affected = &result.affected[0];
        assert_eq!(affected.binding, "property");
        assert_eq!(affected.field.as_deref(), Some("tenant_id"));
        assert_eq!(affected.value, Some(json!("T-999")));

        let key = affected.key.as_ref().expect("key present");
        assert_eq!(key.get("property_id"), Some(&json!("P-003")));
    }

    #[test]
    fn missing_binding_column_uses_default() {
        let conn = setup_connection();
        let rule = make_query_rule(
            "NO_BINDING_COL",
            "SELECT property_id AS key__property_id FROM property WHERE tenant_id = 'T-999'",
        );

        let result = evaluate_query_rule(&rule, &conn, "property").expect("executes");
        assert!(matches!(result.status, ResultStatus::Fail));
        assert_eq!(result.affected[0].binding, "property");
    }

    #[test]
    fn sql_error_returns_query_rule_error() {
        let conn = setup_connection();
        let rule = make_query_rule("BAD_SQL", "SELECT * FROM nonexistent_table");

        let error = evaluate_query_rule(&rule, &conn, "property").expect_err("should fail");
        assert!(error.to_string().contains("BAD_SQL"));
    }

    #[test]
    fn unsupported_check_type_returns_error() {
        let rule = Rule {
            id: "NOT_QUERY".to_owned(),
            severity: Severity::Error,
            portability: Portability::Portable,
            check: Check::Unique {
                binding: "input".to_owned(),
                columns: vec!["id".to_owned()],
            },
        };

        let conn = Connection::open_in_memory().expect("opens");
        let error = evaluate_query_rule(&rule, &conn, "input").expect_err("should fail");
        assert!(error.to_string().contains("query_zero_rows"));
    }

    #[test]
    fn multiple_violations_each_become_affected_entry() {
        let conn = Connection::open_in_memory().expect("opens");
        conn.execute_batch(
            "CREATE TABLE data (id TEXT, status TEXT);
             INSERT INTO data VALUES ('A', 'bad');
             INSERT INTO data VALUES ('B', 'bad');
             INSERT INTO data VALUES ('C', 'good');",
        )
        .expect("inserts");

        let rule = make_query_rule(
            "STATUS_CHECK",
            "SELECT 'data' AS binding, id AS key__id, 'status' AS field, status AS value FROM data WHERE status = 'bad'",
        );

        let result = evaluate_query_rule(&rule, &conn, "data").expect("executes");
        assert_eq!(result.violation_count, 2);
        assert_eq!(result.affected.len(), 2);

        let ids: Vec<_> = result
            .affected
            .iter()
            .map(|affected| {
                affected
                    .key
                    .as_ref()
                    .and_then(|key| key.get("id"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("?")
            })
            .collect();
        assert_eq!(ids, vec!["A", "B"]);
    }

    #[test]
    fn sql_error_maps_to_e_sql_error_refusal() {
        let conn = setup_connection();
        let rule = make_query_rule("BAD_SQL", "SELECT * FROM nonexistent_table");

        let error = evaluate_query_rule(&rule, &conn, "property").expect_err("should fail");
        assert_eq!(
            error.refusal_code(),
            verify_core::refusal::RefusalCode::SqlError
        );

        let refusal = error.to_refusal();
        assert_eq!(refusal.code, verify_core::refusal::RefusalCode::SqlError);
        assert_eq!(refusal.next_step, "Fix the query-backed rule.");

        let detail = error.detail();
        assert_eq!(detail["rule_id"], "BAD_SQL");
    }

    #[test]
    fn execute_query_rules_processes_only_query_rules() {
        let conn = setup_connection();
        let rules = vec![
            Rule {
                id: "NOT_QUERY".to_owned(),
                severity: Severity::Error,
                portability: Portability::Portable,
                check: Check::NotNull {
                    binding: "property".to_owned(),
                    columns: vec!["property_id".to_owned()],
                },
            },
            make_query_rule(
                "ORPHAN_CHECK",
                "SELECT 'property' AS binding FROM property LEFT JOIN tenants ON property.tenant_id = tenants.tenant_id WHERE tenants.tenant_id IS NULL",
            ),
            make_query_rule(
                "ALL_PASS",
                "SELECT 'property' AS binding FROM property WHERE 1 = 0",
            ),
        ];

        let results = execute_query_rules(&conn, &rules).expect("executes");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].rule_id, "ORPHAN_CHECK");
        assert!(matches!(results[0].status, ResultStatus::Fail));
        assert_eq!(results[1].rule_id, "ALL_PASS");
        assert!(matches!(results[1].status, ResultStatus::Pass));
    }
}
