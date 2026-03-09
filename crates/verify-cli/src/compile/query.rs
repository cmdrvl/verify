use std::{fs, path::Path};

use serde_json::json;
use verify_core::{
    CONSTRAINT_VERSION,
    constraint::{Binding, BindingKind, Check, ConstraintSet, Portability, Rule, Severity},
    refusal::RefusalCode,
};

pub fn is_query_authoring(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
}

#[derive(Debug)]
pub enum CompileError {
    Io(std::io::Error),
    BadAuthoring {
        message: String,
        detail: serde_json::Value,
    },
}

impl CompileError {
    pub fn render(&self, path: &Path) -> String {
        match self {
            Self::Io(error) => {
                format!(
                    "{}: failed to read {}: {error}",
                    refusal_code(RefusalCode::Io),
                    path.display()
                )
            }
            Self::BadAuthoring { message, detail } => {
                format!(
                    "{}: {message}\ndetail: {}",
                    refusal_code(RefusalCode::BadAuthoring),
                    serde_json::to_string(detail).expect("bad authoring detail should serialize")
                )
            }
        }
    }
}

pub fn compile_from_path(path: &Path) -> Result<ConstraintSet, CompileError> {
    let source = fs::read_to_string(path).map_err(CompileError::Io)?;
    compile_source(&source)
}

pub fn compile_source(source: &str) -> Result<ConstraintSet, CompileError> {
    let authoring = SqlAuthoring::parse(source)?;
    authoring.compile()
}

#[cfg(test)]
pub fn scaffold_surface(check: bool) -> &'static str {
    if check {
        "compile --check batch SQL authoring"
    } else {
        "compile batch SQL authoring"
    }
}

#[derive(Debug)]
struct SqlAuthoring {
    constraint_set_id: String,
    rule_id: String,
    severity: Severity,
    bindings: Vec<String>,
    query: String,
}

impl SqlAuthoring {
    fn parse(source: &str) -> Result<Self, CompileError> {
        let mut constraint_set_id = None;
        let mut rule_id = None;
        let mut severity = None;
        let mut bindings = None;

        let mut query_lines = Vec::new();
        let mut header_complete = false;

        for line in source.lines() {
            let trimmed = line.trim();

            if !header_complete && trimmed.is_empty() {
                continue;
            }

            if !header_complete && let Some((key, value)) = parse_metadata_line(trimmed)? {
                match key {
                    "constraint_set_id" => {
                        insert_unique_metadata(&mut constraint_set_id, key, value.to_owned())?
                    }
                    "rule_id" => insert_unique_metadata(&mut rule_id, key, value.to_owned())?,
                    "severity" => {
                        let parsed = parse_severity(value)?;
                        insert_unique_metadata(&mut severity, key, parsed)?
                    }
                    "bindings" => {
                        let parsed = parse_bindings(value)?;
                        insert_unique_metadata(&mut bindings, key, parsed)?
                    }
                    other => {
                        return Err(bad_authoring(
                            "SQL authoring declared unsupported verify metadata",
                            json!({ "metadata": other }),
                        ));
                    }
                }
                continue;
            }

            header_complete = true;
            if !trimmed.is_empty() {
                query_lines.push(trimmed);
            }
        }

        let query = normalize_query(&query_lines.join("\n"));
        if query.is_empty() {
            return Err(bad_authoring(
                "SQL authoring must contain a query body",
                json!({ "field": "query" }),
            ));
        }

        Ok(Self {
            constraint_set_id: require_metadata(constraint_set_id, "constraint_set_id")?,
            rule_id: require_metadata(rule_id, "rule_id")?,
            severity: require_metadata(severity, "severity")?,
            bindings: require_metadata(bindings, "bindings")?,
            query,
        })
    }

    fn compile(self) -> Result<ConstraintSet, CompileError> {
        let key_binding = query_binding_name(&self.query)
            .filter(|binding| self.bindings.iter().any(|candidate| candidate == binding))
            .unwrap_or_else(|| self.bindings[0].clone());
        let key_fields = query_key_fields(&self.query);

        let bindings = self
            .bindings
            .iter()
            .map(|name| Binding {
                name: name.clone(),
                kind: BindingKind::Relation,
                key_fields: if *name == key_binding {
                    key_fields.clone()
                } else {
                    Vec::new()
                },
            })
            .collect();

        Ok(ConstraintSet {
            version: CONSTRAINT_VERSION.to_owned(),
            constraint_set_id: self.constraint_set_id,
            bindings,
            rules: vec![Rule {
                id: self.rule_id,
                severity: self.severity,
                portability: Portability::BatchOnly,
                check: Check::QueryZeroRows {
                    bindings: self.bindings,
                    query: self.query,
                },
            }],
        })
    }
}

fn parse_metadata_line(line: &str) -> Result<Option<(&str, &str)>, CompileError> {
    if line.is_empty() {
        return Ok(None);
    }

    let Some(content) = line.strip_prefix("--") else {
        return Ok(None);
    };

    let content = content.trim();
    if content.is_empty() {
        return Ok(None);
    }

    let Some(content) = content.strip_prefix("verify.") else {
        return Err(bad_authoring(
            "SQL authoring metadata comments must use the `verify.` prefix",
            json!({ "line": line }),
        ));
    };

    let Some((key, value)) = content.split_once(':') else {
        return Err(bad_authoring(
            "SQL authoring metadata comments must use `-- verify.<field>: <value>`",
            json!({ "line": line }),
        ));
    };

    Ok(Some((key.trim(), value.trim())))
}

fn parse_severity(value: &str) -> Result<Severity, CompileError> {
    match value {
        "error" => Ok(Severity::Error),
        "warn" => Ok(Severity::Warn),
        _ => Err(bad_authoring(
            "SQL authoring severity must be `error` or `warn`",
            json!({ "severity": value }),
        )),
    }
}

fn parse_bindings(value: &str) -> Result<Vec<String>, CompileError> {
    let bindings = value
        .split(',')
        .map(str::trim)
        .filter(|binding| !binding.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if bindings.is_empty() {
        return Err(bad_authoring(
            "SQL authoring must declare at least one binding",
            json!({ "field": "bindings" }),
        ));
    }

    let mut seen = std::collections::BTreeSet::new();
    for binding in &bindings {
        if !seen.insert(binding.clone()) {
            return Err(bad_authoring(
                "SQL authoring declared duplicate bindings",
                json!({ "binding": binding }),
            ));
        }
    }

    Ok(bindings)
}

fn normalize_query(source: &str) -> String {
    let trimmed = source.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut normalized = String::new();
    let mut in_single_quote = false;
    let mut pending_space = false;

    for character in trimmed.chars() {
        match character {
            '\'' => {
                if pending_space && !normalized.is_empty() {
                    normalized.push(' ');
                    pending_space = false;
                }
                in_single_quote = !in_single_quote;
                normalized.push(character);
            }
            _ if character.is_whitespace() && !in_single_quote => {
                pending_space = true;
            }
            _ => {
                if pending_space && !normalized.is_empty() {
                    normalized.push(' ');
                    pending_space = false;
                }
                normalized.push(character);
            }
        }
    }

    normalized
}

fn query_binding_name(query: &str) -> Option<String> {
    let lower = query.to_ascii_lowercase();
    let marker = " as binding";
    let binding_index = lower.find(marker)?;
    let prefix = query[..binding_index].trim_end();
    let literal = prefix.strip_suffix('\'')?;
    let start = literal.rfind('\'')?;
    Some(literal[start + 1..].to_owned())
}

fn query_key_fields(query: &str) -> Vec<String> {
    let lower = query.to_ascii_lowercase();
    let marker = " as key__";
    let mut fields = Vec::new();
    let mut search_from = 0;

    while let Some(relative_index) = lower[search_from..].find(marker) {
        let field_start = search_from + relative_index + marker.len();
        let field_end = query[field_start..]
            .find(|character: char| !matches!(character, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_'))
            .map(|offset| field_start + offset)
            .unwrap_or(query.len());

        if field_end > field_start {
            let field = query[field_start..field_end].to_owned();
            if !fields.contains(&field) {
                fields.push(field);
            }
        }

        search_from = field_end;
    }

    fields
}

fn insert_unique_metadata<T>(
    slot: &mut Option<T>,
    field: &str,
    value: T,
) -> Result<(), CompileError> {
    if slot.is_some() {
        return Err(bad_authoring(
            "SQL authoring declared duplicate metadata",
            json!({ "field": field }),
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn require_metadata<T>(value: Option<T>, field: &str) -> Result<T, CompileError> {
    value.ok_or_else(|| {
        bad_authoring(
            "SQL authoring is missing required metadata",
            json!({ "field": field }),
        )
    })
}

fn bad_authoring(message: &str, detail: serde_json::Value) -> CompileError {
    CompileError::BadAuthoring {
        message: message.to_owned(),
        detail,
    }
}

fn refusal_code(code: RefusalCode) -> &'static str {
    match code {
        RefusalCode::Io => "E_IO",
        RefusalCode::BadAuthoring => "E_BAD_AUTHORING",
        _ => unreachable!("query compile only renders IO and bad authoring refusals"),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;
    use verify_core::{
        constraint::{Check, ConstraintSet, Portability, Severity},
        refusal::RefusalCode,
    };

    use super::{
        CompileError, compile_source, is_query_authoring, normalize_query, query_binding_name,
        query_key_fields, scaffold_surface,
    };

    #[test]
    fn detects_sql_authoring_by_extension() {
        assert!(is_query_authoring(Path::new("rules.sql")));
        assert!(is_query_authoring(Path::new("RULES.SQL")));
        assert!(!is_query_authoring(Path::new("rules.yaml")));
    }

    #[test]
    fn scaffold_surface_tracks_check_mode() {
        assert_eq!(scaffold_surface(false), "compile batch SQL authoring");
        assert_eq!(
            scaffold_surface(true),
            "compile --check batch SQL authoring"
        );
    }

    #[test]
    fn compiles_fixture_sql_authoring_into_batch_only_rule() -> Result<(), String> {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/query_rules/orphan_rows.sql"
        ));
        let expected: ConstraintSet = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/query_rules/orphan_rows.verify.json"
        )))
        .expect("expected fixture parses");

        let compiled = compile_source(source).expect("fixture SQL authoring compiles");

        assert_eq!(compiled, expected);
        assert_eq!(compiled.rules.len(), 1);
        assert!(matches!(compiled.rules[0].severity, Severity::Error));
        assert!(matches!(
            compiled.rules[0].portability,
            Portability::BatchOnly
        ));
        let (bindings, query) = match &compiled.rules[0].check {
            Check::QueryZeroRows { bindings, query } => (bindings, query),
            other => return Err(format!("expected query_zero_rows check, got {other:?}")),
        };
        assert_eq!(bindings, &vec!["property".to_owned(), "tenants".to_owned()]);
        assert!(!query.ends_with(';'));
        Ok(())
    }

    #[test]
    fn rejects_missing_required_metadata() -> Result<(), String> {
        let error = compile_source(
            r#"
-- verify.constraint_set_id: fixtures.query_rules.orphan_rows
-- verify.severity: error
SELECT 1
"#,
        )
        .expect_err("missing rule metadata should fail");

        match error {
            CompileError::BadAuthoring { message, detail } => {
                assert!(message.contains("missing required metadata"));
                assert_eq!(detail["field"], "rule_id");
                Ok(())
            }
            other => Err(format!("expected bad authoring error, got {other:?}")),
        }
    }

    #[test]
    fn renders_bad_authoring_with_refusal_code() {
        let error = CompileError::BadAuthoring {
            message: "query metadata is invalid".to_owned(),
            detail: json!({ "field": "bindings" }),
        };

        let rendered = error.render(Path::new("rules.sql"));
        assert!(rendered.contains("E_BAD_AUTHORING"));
        assert!(rendered.contains(r#""field":"bindings""#));
    }

    #[test]
    fn normalizes_query_whitespace_and_trailing_semicolon() {
        let normalized = normalize_query(
            "SELECT\n  'property' AS binding,\n  property.property_id AS key__property_id\nFROM property ;\n",
        );

        assert_eq!(
            normalized,
            "SELECT 'property' AS binding, property.property_id AS key__property_id FROM property"
        );
    }

    #[test]
    fn extracts_binding_and_key_aliases_from_query() {
        let query = "SELECT 'property' AS binding, property.property_id AS key__property_id, property.region_id AS key__region_id FROM property";

        assert_eq!(query_binding_name(query).as_deref(), Some("property"));
        assert_eq!(
            query_key_fields(query),
            vec!["property_id".to_owned(), "region_id".to_owned()]
        );
    }

    #[test]
    fn refusal_code_helper_matches_protocol_spelling() {
        assert_eq!(super::refusal_code(RefusalCode::Io), "E_IO");
        assert_eq!(
            super::refusal_code(RefusalCode::BadAuthoring),
            "E_BAD_AUTHORING"
        );
    }
}
