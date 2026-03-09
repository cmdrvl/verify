use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use verify_core::{
    constraint::{
        Aggregate, Binding, BindingKind, Check, Comparison, ConstraintSet, Portability,
        PredicateExpression, Rule, Severity,
    },
    refusal::RefusalCode,
};

#[cfg(test)]
pub fn scaffold_surface(check: bool) -> &'static str {
    if check {
        "compile --check portable authoring"
    } else {
        "compile portable authoring"
    }
}

#[derive(Debug)]
pub enum CompileError {
    Io(std::io::Error),
    BadAuthoring { message: String, detail: Value },
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
    let authoring: PortableAuthoring =
        serde_yaml::from_str(source).map_err(|error| CompileError::BadAuthoring {
            message: format!("portable authoring could not be parsed: {error}"),
            detail: json!({
                "status": "parse_error",
            }),
        })?;

    authoring.compile()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableAuthoring {
    constraint_set_id: String,
    bindings: BTreeMap<String, PortableBindingAuthoring>,
    rules: Vec<PortableRuleAuthoring>,
}

impl PortableAuthoring {
    fn compile(self) -> Result<ConstraintSet, CompileError> {
        require_non_empty("constraint_set_id", &self.constraint_set_id)?;

        if self.bindings.is_empty() {
            return Err(bad_authoring(
                "portable authoring must declare at least one binding",
                json!({"field": "bindings"}),
            ));
        }

        if self.rules.is_empty() {
            return Err(bad_authoring(
                "portable authoring must declare at least one rule",
                json!({"field": "rules"}),
            ));
        }

        let binding_names = self.bindings.keys().cloned().collect::<BTreeSet<_>>();
        let bindings = self
            .bindings
            .into_iter()
            .map(|(name, binding)| binding.compile(name))
            .collect::<Result<Vec<_>, _>>()?;

        let mut seen_rule_ids = BTreeSet::new();
        let mut rules = Vec::with_capacity(self.rules.len());
        for rule in self.rules {
            if !seen_rule_ids.insert(rule.id.clone()) {
                return Err(bad_authoring(
                    "portable authoring contains duplicate rule ids",
                    json!({"rule_id": rule.id}),
                ));
            }
            rules.push(rule.compile(&binding_names)?);
        }

        Ok(ConstraintSet {
            version: verify_core::CONSTRAINT_VERSION.to_owned(),
            constraint_set_id: self.constraint_set_id,
            bindings,
            rules,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PortableBindingAuthoring {
    #[serde(default)]
    key_fields: Vec<String>,
}

impl PortableBindingAuthoring {
    fn compile(self, name: String) -> Result<Binding, CompileError> {
        require_non_empty("binding name", &name)?;
        ensure_optional_named_list("key_fields", &self.key_fields)?;

        Ok(Binding {
            name,
            kind: BindingKind::Relation,
            key_fields: self.key_fields,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableRuleAuthoring {
    id: String,
    severity: Severity,
    #[serde(default)]
    portability: Option<Portability>,
    binding: Option<String>,
    op: String,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    ref_binding: Option<String>,
    #[serde(default)]
    ref_columns: Vec<String>,
    #[serde(default)]
    bindings: Vec<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    expr: Option<Value>,
    #[serde(default)]
    compare: Option<Comparison>,
    #[serde(default)]
    aggregate: Option<Aggregate>,
}

impl PortableRuleAuthoring {
    fn compile(self, binding_names: &BTreeSet<String>) -> Result<Rule, CompileError> {
        require_non_empty("rule id", &self.id)?;

        if let Some(portability) = self.portability
            && portability != Portability::Portable
        {
            return Err(bad_authoring(
                "portable authoring cannot emit batch-only rules",
                json!({
                    "rule_id": self.id,
                    "portability": portability,
                }),
            ));
        }

        let check = match self.op.as_str() {
            "unique" => Check::Unique {
                binding: self.require_binding(binding_names)?,
                columns: self.require_columns()?,
            },
            "not_null" => Check::NotNull {
                binding: self.require_binding(binding_names)?,
                columns: self.require_columns()?,
            },
            "predicate" => Check::Predicate {
                binding: self.require_binding(binding_names)?,
                expr: self.require_expr()?,
            },
            "row_count" => Check::RowCount {
                binding: self.require_binding(binding_names)?,
                compare: self.require_compare()?,
            },
            "aggregate_compare" => Check::AggregateCompare {
                binding: self.require_binding(binding_names)?,
                aggregate: self.require_aggregate()?,
                compare: self.require_compare()?,
            },
            "foreign_key" => Check::ForeignKey {
                binding: self.require_binding(binding_names)?,
                columns: self.require_columns()?,
                ref_binding: self.require_ref_binding(binding_names)?,
                ref_columns: self.require_ref_columns()?,
            },
            "query_zero_rows" => {
                return Err(bad_authoring(
                    "portable authoring does not support query_zero_rows; use SQL authoring",
                    json!({
                        "rule_id": self.id,
                        "bindings": self.bindings,
                        "query": self.query,
                    }),
                ));
            }
            _ => {
                return Err(bad_authoring(
                    "portable authoring declared an unsupported op",
                    json!({
                        "rule_id": self.id,
                        "op": self.op,
                    }),
                ));
            }
        };

        Ok(Rule {
            id: self.id,
            severity: self.severity,
            portability: Portability::Portable,
            check,
        })
    }

    fn require_binding(&self, binding_names: &BTreeSet<String>) -> Result<String, CompileError> {
        let binding = self.binding.clone().ok_or_else(|| {
            bad_authoring(
                "portable rule is missing the required binding field",
                json!({
                    "rule_id": self.id,
                    "op": self.op,
                }),
            )
        })?;

        require_declared_binding(binding_names, &self.id, "binding", &binding)?;
        Ok(binding)
    }

    fn require_ref_binding(
        &self,
        binding_names: &BTreeSet<String>,
    ) -> Result<String, CompileError> {
        let ref_binding = self.ref_binding.clone().ok_or_else(|| {
            bad_authoring(
                "foreign_key rule is missing ref_binding",
                json!({"rule_id": self.id}),
            )
        })?;

        require_declared_binding(binding_names, &self.id, "ref_binding", &ref_binding)?;
        Ok(ref_binding)
    }

    fn require_columns(&self) -> Result<Vec<String>, CompileError> {
        ensure_named_list("columns", &self.columns)?;
        Ok(self.columns.clone())
    }

    fn require_ref_columns(&self) -> Result<Vec<String>, CompileError> {
        ensure_named_list("ref_columns", &self.ref_columns)?;
        Ok(self.ref_columns.clone())
    }

    fn require_expr(&self) -> Result<PredicateExpression, CompileError> {
        let expr = self.expr.clone().ok_or_else(|| {
            bad_authoring(
                "predicate rule is missing expr",
                json!({"rule_id": self.id}),
            )
        })?;
        let normalized = normalize_predicate_aliases(expr);

        serde_json::from_value(normalized).map_err(|error| {
            bad_authoring(
                &format!("predicate expression is invalid: {error}"),
                json!({"rule_id": self.id}),
            )
        })
    }

    fn require_compare(&self) -> Result<Comparison, CompileError> {
        let compare = self.compare.clone().ok_or_else(|| {
            bad_authoring(
                "rule is missing compare",
                json!({"rule_id": self.id, "op": self.op}),
            )
        })?;
        validate_comparison(&self.id, &compare)?;
        Ok(compare)
    }

    fn require_aggregate(&self) -> Result<Aggregate, CompileError> {
        let aggregate = self.aggregate.clone().ok_or_else(|| {
            bad_authoring(
                "aggregate_compare rule is missing aggregate",
                json!({"rule_id": self.id}),
            )
        })?;
        validate_aggregate(&self.id, &aggregate)?;
        Ok(aggregate)
    }
}

fn normalize_predicate_aliases(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(normalize_predicate_aliases)
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let normalized_key = match key.as_str() {
                        "blank" => "is_blank".to_owned(),
                        "null" => "is_null".to_owned(),
                        _ => key,
                    };
                    (normalized_key, normalize_predicate_aliases(value))
                })
                .collect::<Map<String, Value>>(),
        ),
        scalar => scalar,
    }
}

fn validate_comparison(rule_id: &str, compare: &Comparison) -> Result<(), CompileError> {
    let configured = [
        compare.eq.is_some(),
        compare.ne.is_some(),
        compare.gt.is_some(),
        compare.gte.is_some(),
        compare.lt.is_some(),
        compare.lte.is_some(),
    ]
    .into_iter()
    .filter(|is_some| *is_some)
    .count();

    if configured != 1 {
        return Err(bad_authoring(
            "comparison must declare exactly one comparator",
            json!({"rule_id": rule_id}),
        ));
    }

    Ok(())
}

fn validate_aggregate(rule_id: &str, aggregate: &Aggregate) -> Result<(), CompileError> {
    let configured = [
        aggregate.sum.as_ref(),
        aggregate.avg.as_ref(),
        aggregate.min.as_ref(),
        aggregate.max.as_ref(),
    ]
    .into_iter()
    .flatten()
    .count();

    if configured != 1 {
        return Err(bad_authoring(
            "aggregate must declare exactly one aggregate operator",
            json!({"rule_id": rule_id}),
        ));
    }

    if let Some(column) = aggregate
        .sum
        .as_ref()
        .or(aggregate.avg.as_ref())
        .or(aggregate.min.as_ref())
        .or(aggregate.max.as_ref())
    {
        require_non_empty("aggregate column", column)?;
    }

    Ok(())
}

fn require_declared_binding(
    binding_names: &BTreeSet<String>,
    rule_id: &str,
    field: &str,
    binding: &str,
) -> Result<(), CompileError> {
    require_non_empty(field, binding)?;

    if !binding_names.contains(binding) {
        return Err(bad_authoring(
            "rule references a binding that is not declared in bindings",
            json!({
                "rule_id": rule_id,
                "field": field,
                "binding": binding,
            }),
        ));
    }

    Ok(())
}

fn ensure_named_list(field: &str, values: &[String]) -> Result<(), CompileError> {
    if values.is_empty() {
        return Err(bad_authoring(
            &format!("{field} must contain at least one value"),
            json!({"field": field}),
        ));
    }

    let mut seen = BTreeSet::new();
    for value in values {
        require_non_empty(field, value)?;
        if !seen.insert(value) {
            return Err(bad_authoring(
                &format!("{field} contains duplicate values"),
                json!({"field": field, "value": value}),
            ));
        }
    }

    Ok(())
}

fn ensure_optional_named_list(field: &str, values: &[String]) -> Result<(), CompileError> {
    if values.is_empty() {
        return Ok(());
    }

    ensure_named_list(field, values)
}

fn require_non_empty(field: &str, value: &str) -> Result<(), CompileError> {
    if value.trim().is_empty() {
        return Err(bad_authoring(
            &format!("{field} must not be empty"),
            json!({"field": field}),
        ));
    }

    Ok(())
}

fn bad_authoring(message: &str, detail: Value) -> CompileError {
    CompileError::BadAuthoring {
        message: message.to_owned(),
        detail,
    }
}

fn refusal_code(code: RefusalCode) -> String {
    serde_json::to_string(&code)
        .expect("refusal code should serialize")
        .trim_matches('"')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use verify_core::constraint::{Check, ConstraintSet};

    use super::{CompileError, compile_source, scaffold_surface};

    #[test]
    fn scaffold_surface_tracks_check_mode() {
        assert_eq!(scaffold_surface(false), "compile portable authoring");
        assert_eq!(scaffold_surface(true), "compile --check portable authoring");
    }

    #[test]
    fn compiles_yaml_fixture_into_expected_constraint_set() {
        const AUTHORING: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/arity1/not_null_loans.yaml"
        ));
        const EXPECTED: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/arity1/not_null_loans.verify.json"
        ));

        let compiled = compile_source(AUTHORING).expect("fixture authoring compiles");
        let expected: ConstraintSet =
            serde_json::from_str(EXPECTED).expect("compiled fixture parses");

        assert_eq!(compiled, expected);
    }

    #[test]
    fn compiles_predicate_aliases_into_protocol_expression() -> Result<(), String> {
        const AUTHORING: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/predicate_grammar/blank_or_member.yaml"
        ));

        let compiled = compile_source(AUTHORING).expect("predicate fixture compiles");

        let expr = match &compiled.rules[0].check {
            Check::Predicate { expr, .. } => expr,
            other => return Err(format!("expected predicate check, got {other:?}")),
        };
        let serialized = serde_json::to_value(expr).expect("expr serializes");
        assert_eq!(
            serialized,
            json!({
                "or": [
                    {
                        "in": [
                            { "column": "occupancy_status" },
                            ["owner", "investor"]
                        ]
                    },
                    {
                        "is_blank": {
                            "column": "occupancy_status"
                        }
                    }
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn compiles_json_authoring_input() {
        let compiled = compile_source(
            r#"{
                "constraint_set_id": "json.portable.not_null",
                "bindings": {
                    "input": {
                        "key_fields": ["loan_id"]
                    }
                },
                "rules": [
                    {
                        "id": "INPUT_LOAN_ID_PRESENT",
                        "severity": "error",
                        "binding": "input",
                        "op": "not_null",
                        "columns": ["loan_id"]
                    }
                ]
            }"#,
        )
        .expect("json authoring should compile");

        assert_eq!(compiled.constraint_set_id, "json.portable.not_null");
        assert_eq!(compiled.bindings[0].name, "input");
        assert!(matches!(compiled.rules[0].check, Check::NotNull { .. }));
    }

    #[test]
    fn rejects_batch_only_rules_in_portable_authoring() -> Result<(), String> {
        let error = compile_source(
            r#"
constraint_set_id: invalid.query_zero_rows
bindings:
  input: {}
rules:
  - id: QUERY_ASSERTION
    severity: error
    op: query_zero_rows
    bindings: [input]
    query: select 1
"#,
        )
        .expect_err("query_zero_rows should stay on the SQL authoring path");

        match error {
            CompileError::BadAuthoring { message, .. } => {
                assert!(message.contains("query_zero_rows"));
                Ok(())
            }
            other => Err(format!("expected bad authoring error, got {other:?}")),
        }
    }

    #[test]
    fn rejects_rules_that_reference_undeclared_bindings() -> Result<(), String> {
        let error = compile_source(
            r#"
constraint_set_id: invalid.missing_binding
bindings:
  input: {}
rules:
  - id: PROPERTY_LOAN_EXISTS
    severity: error
    binding: property
    op: not_null
    columns: [loan_id]
"#,
        )
        .expect_err("undeclared bindings should be rejected");

        match error {
            CompileError::BadAuthoring { detail, .. } => {
                assert_eq!(detail["binding"], "property");
                Ok(())
            }
            other => Err(format!("expected bad authoring error, got {other:?}")),
        }
    }
}
