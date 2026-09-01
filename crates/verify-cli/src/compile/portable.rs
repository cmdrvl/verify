use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};
use verify_core::{
    constraint::{
        Aggregate, Binding, BindingKind, Check, Comparison, ConstraintSet, Portability,
        PredicateExpression, Rule, Severity,
    },
    refusal::RefusalCode,
    validation::{
        ConstraintValidationError, ConstraintValidationReason, analyze_predicate,
        validate_predicate_key_fields,
    },
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

        for (name, binding) in &self.bindings {
            binding.validate(name)?;
        }

        let binding_names = self.bindings.keys().cloned().collect::<BTreeSet<_>>();
        let key_fields_by_binding = self
            .bindings
            .iter()
            .map(|(name, binding)| (name.clone(), binding.key_fields.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut seen_rule_ids = BTreeSet::new();
        let mut rules = Vec::with_capacity(self.rules.len());
        for rule in self.rules {
            if !seen_rule_ids.insert(rule.id.clone()) {
                return Err(bad_authoring(
                    "portable authoring contains duplicate rule ids",
                    json!({"rule_id": rule.id}),
                ));
            }
            rules.push(rule.compile(&binding_names, &key_fields_by_binding)?);
        }

        if let Some((binding, _)) = key_fields_by_binding
            .iter()
            .find(|(_, key_fields)| key_fields.as_ref().is_some_and(Vec::is_empty))
        {
            return Err(bad_authoring(
                "binding key_fields must not be explicitly empty",
                json!({
                    "reason": ConstraintValidationReason::EmptyKeyFields,
                    "binding": binding,
                    "key_fields": [],
                }),
            ));
        }

        let bindings = self
            .bindings
            .into_iter()
            .map(|(name, binding)| binding.compile(name))
            .collect();

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
    #[serde(default, deserialize_with = "deserialize_optional_key_fields")]
    key_fields: Option<Vec<String>>,
}

impl PortableBindingAuthoring {
    fn validate(&self, name: &str) -> Result<(), CompileError> {
        require_non_empty("binding name", name)?;
        if let Some(key_fields) = &self.key_fields
            && !key_fields.is_empty()
        {
            ensure_named_list("key_fields", key_fields)?;
        }

        Ok(())
    }

    fn compile(self, name: String) -> Binding {
        Binding {
            name,
            kind: BindingKind::Relation,
            key_fields: self.key_fields.unwrap_or_default(),
        }
    }
}

fn deserialize_optional_key_fields<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer).map(Some)
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
    fn compile(
        self,
        binding_names: &BTreeSet<String>,
        key_fields_by_binding: &BTreeMap<String, Option<Vec<String>>>,
    ) -> Result<Rule, CompileError> {
        require_non_empty("rule id", &self.id)?;

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
                binding: self.require_predicate_binding()?,
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

        let analysis = match &check {
            Check::Predicate { binding, expr } => Some(analyze_predicate(binding, expr)),
            _ => None,
        };
        let derived_portability = analysis.as_ref().map_or(Portability::Portable, |analysis| {
            analysis.derived_portability
        });
        let participating_bindings = analysis.as_ref().map_or_else(
            || check_binding_names(&check),
            |analysis| analysis.participating_bindings.clone(),
        );

        if let Some(declared_portability) = self.portability
            && declared_portability != derived_portability
        {
            return Err(bad_authoring(
                "declared rule portability does not match its derived semantics",
                json!({
                    "reason": ConstraintValidationReason::PortabilityMismatch,
                    "rule_id": self.id,
                    "declared_portability": declared_portability,
                    "derived_portability": derived_portability,
                    "participating_bindings": participating_bindings,
                }),
            ));
        }

        if let Some(analysis) = &analysis {
            if let Some(binding) = analysis
                .participating_bindings
                .iter()
                .find(|binding| !binding_names.contains(binding.as_str()))
            {
                return Err(bad_authoring(
                    "predicate expression references an undeclared binding",
                    json!({
                        "reason": ConstraintValidationReason::UndeclaredReference,
                        "rule_id": self.id,
                        "binding": binding,
                        "participating_bindings": analysis.participating_bindings,
                    }),
                ));
            }

            if derived_portability == Portability::BatchOnly {
                validate_predicate_key_fields(
                    &self.id,
                    &analysis.anchor_binding,
                    &analysis.participating_bindings,
                    key_fields_by_binding,
                )
                .map_err(validation_bad_authoring)?;
            }
        }

        Ok(Rule {
            id: self.id,
            severity: self.severity,
            portability: derived_portability,
            check,
        })
    }

    fn require_predicate_binding(&self) -> Result<String, CompileError> {
        let binding = self.binding.clone().ok_or_else(|| {
            bad_authoring(
                "portable rule is missing the required binding field",
                json!({
                    "rule_id": self.id,
                    "op": self.op,
                }),
            )
        })?;
        require_non_empty("binding", &binding)?;
        Ok(binding)
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

fn check_binding_names(check: &Check) -> Vec<String> {
    let names = match check {
        Check::Unique { binding, .. }
        | Check::NotNull { binding, .. }
        | Check::Predicate { binding, .. }
        | Check::RowCount { binding, .. }
        | Check::AggregateCompare { binding, .. } => vec![binding.clone()],
        Check::ForeignKey {
            binding,
            ref_binding,
            ..
        } => vec![binding.clone(), ref_binding.clone()],
        Check::QueryZeroRows { bindings, .. } => bindings.clone(),
    };
    names
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn validation_bad_authoring(error: ConstraintValidationError) -> CompileError {
    bad_authoring(
        &format!("predicate authoring is invalid: {}", error.reason.as_str()),
        error.detail,
    )
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
    use serde_json::{Value, json};
    use verify_core::constraint::{Check, ConstraintSet, Portability};

    use super::{CompileError, compile_source, scaffold_surface};

    fn bad_authoring_detail(source: &str, expectation: &str) -> Result<Value, String> {
        match compile_source(source) {
            Err(CompileError::BadAuthoring { detail, .. }) => Ok(detail),
            Err(error) => Err(format!("{expectation}; received {error:?}")),
            Ok(_) => Err(format!("{expectation}; compilation succeeded")),
        }
    }

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
    fn compiles_binding_qualified_yaml_and_json_with_inferred_portability() {
        const AUTHORING: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/binding_qualified/maturity_date_immutable.yaml"
        ));
        const EXPECTED: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json"
        ));

        let yaml_compiled = compile_source(AUTHORING).expect("YAML fixture should compile");
        let expected: ConstraintSet =
            serde_json::from_str(EXPECTED).expect("compiled fixture should parse");
        assert_eq!(yaml_compiled, expected);
        assert_eq!(yaml_compiled.rules[0].portability, Portability::BatchOnly);

        let json_compiled = compile_source(
            r#"{
                "constraint_set_id": "fixtures.binding_qualified.maturity_date_immutable",
                "bindings": {
                    "current": {"key_fields": ["loan_id", "tranche_id"]},
                    "prior": {"key_fields": ["asset_number", "class_code"]}
                },
                "rules": [{
                    "id": "MATURITY_DATE_IMMUTABLE",
                    "severity": "error",
                    "portability": "batch_only",
                    "binding": "current",
                    "op": "predicate",
                    "expr": {"eq": [
                        {"column": "maturity_date"},
                        {"binding": "prior", "column": "maturity_date"}
                    ]}
                }]
            }"#,
        )
        .expect("JSON authoring should compile");
        assert_eq!(json_compiled, expected);
    }

    #[test]
    fn binding_qualified_compilation_is_byte_deterministic() {
        const AUTHORING: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/binding_qualified/maturity_date_immutable.yaml"
        ));

        let first = compile_source(AUTHORING).expect("first compilation should succeed");
        let second = compile_source(AUTHORING).expect("second compilation should succeed");

        assert_eq!(
            serde_json::to_vec(&first).expect("first artifact should serialize"),
            serde_json::to_vec(&second).expect("second artifact should serialize")
        );
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

    #[test]
    fn explicit_anchor_reference_remains_portable_and_is_preserved() -> Result<(), String> {
        let compiled = compile_source(
            r#"
constraint_set_id: explicit.anchor.predicate
bindings:
  input: {}
rules:
  - id: VALUE_MATCHES_EXPECTED
    severity: error
    portability: portable
    binding: input
    op: predicate
    expr:
      eq:
        - { binding: input, column: value }
        - { column: expected }
"#,
        )
        .expect("same-anchor reference should compile");

        assert_eq!(compiled.rules[0].portability, Portability::Portable);
        let Check::Predicate { expr, .. } = &compiled.rules[0].check else {
            return Err("compiled rule should remain a predicate".to_owned());
        };
        assert_eq!(
            serde_json::to_value(expr).expect("expression should serialize")["eq"][0]["binding"],
            "input"
        );
        Ok(())
    }

    #[test]
    fn rejects_both_declared_portability_mismatches() -> Result<(), String> {
        let cross_declared_portable = r#"
constraint_set_id: invalid.cross_portability
bindings:
  current: { key_fields: [id] }
  prior: { key_fields: [id] }
rules:
  - id: VALUE_IMMUTABLE
    severity: error
    portability: portable
    binding: current
    op: predicate
    expr:
      eq:
        - { column: value }
        - { binding: prior, column: value }
"#;
        let anchor_declared_batch = r#"
constraint_set_id: invalid.anchor_portability
bindings:
  input: {}
rules:
  - id: VALUE_PRESENT
    severity: error
    portability: batch_only
    binding: input
    op: predicate
    expr: { column: value }
"#;

        for source in [cross_declared_portable, anchor_declared_batch] {
            let detail = bad_authoring_detail(source, "portability mismatch should refuse")?;
            assert_eq!(detail["reason"], "portability_mismatch");
        }
        Ok(())
    }

    #[test]
    fn rejects_nested_undeclared_binding_reference() -> Result<(), String> {
        let detail = bad_authoring_detail(
            r#"
constraint_set_id: invalid.nested_binding
bindings:
  current: { key_fields: [id] }
rules:
  - id: VALUE_PRESENT
    severity: error
    portability: batch_only
    binding: current
    op: predicate
    expr:
      or:
        - { is_null: { column: value } }
        - { not: { is_blank: { binding: missing, column: value } } }
"#,
            "nested undeclared reference should refuse",
        )?;
        assert_eq!(detail["reason"], "undeclared_reference");
        assert_eq!(detail["binding"], "missing");
        Ok(())
    }

    #[test]
    fn rejects_every_binding_qualified_key_shape_defect() -> Result<(), String> {
        let missing = r#"
constraint_set_id: invalid.missing_keys
bindings:
  current: { key_fields: [id] }
  prior: {}
rules:
  - id: VALUE_IMMUTABLE
    severity: error
    binding: current
    op: predicate
    expr: { eq: [{ column: value }, { binding: prior, column: value }] }
"#;
        let empty = r#"
constraint_set_id: invalid.empty_keys
bindings:
  current: { key_fields: [id] }
  prior: { key_fields: [] }
rules:
  - id: VALUE_IMMUTABLE
    severity: error
    binding: current
    op: predicate
    expr: { eq: [{ column: value }, { binding: prior, column: value }] }
"#;
        let arity = r#"
constraint_set_id: invalid.key_arity
bindings:
  current: { key_fields: [id, part] }
  prior: { key_fields: [id] }
rules:
  - id: VALUE_IMMUTABLE
    severity: error
    binding: current
    op: predicate
    expr: { eq: [{ column: value }, { binding: prior, column: value }] }
"#;

        for (source, reason) in [
            (missing, "missing_key_fields"),
            (empty, "empty_key_fields"),
            (arity, "key_arity_mismatch"),
        ] {
            let detail = bad_authoring_detail(source, "invalid key shape should refuse")?;
            assert_eq!(detail["reason"], reason);
            assert_eq!(detail["rule_id"], "VALUE_IMMUTABLE");
        }
        Ok(())
    }

    #[test]
    fn explicit_empty_key_fields_are_rejected_even_without_a_cross_binding_rule()
    -> Result<(), String> {
        let detail = bad_authoring_detail(
            r#"
constraint_set_id: invalid.unused_empty_keys
bindings:
  input: { key_fields: [] }
rules:
  - id: VALUE_PRESENT
    severity: error
    binding: input
    op: not_null
    columns: [value]
"#,
            "explicit empty key fields must not normalize to omission",
        )?;
        assert_eq!(detail["reason"], "empty_key_fields");
        assert_eq!(detail["binding"], "input");
        Ok(())
    }
}
