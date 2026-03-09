use std::collections::BTreeSet;
use std::path::PathBuf;

use clap::Args;
use serde_json::json;
use verify_core::constraint::{Check, ConstraintSet};

#[derive(Debug, Clone, Args)]
pub struct ValidateArgs {
    #[arg(value_name = "COMPILED_CONSTRAINTS")]
    pub compiled_constraints: PathBuf,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: ValidateArgs) -> Result<(), String> {
    let bytes = std::fs::read(&args.compiled_constraints).map_err(|error| {
        format!(
            "failed to read {}: {error}",
            args.compiled_constraints.display()
        )
    })?;

    let constraints: ConstraintSet = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "invalid compiled constraints {}: {error}",
            args.compiled_constraints.display()
        )
    })?;

    if constraints.version != verify_core::CONSTRAINT_VERSION {
        return Err(format!(
            "unexpected constraint version: expected {}, got {}",
            verify_core::CONSTRAINT_VERSION,
            constraints.version
        ));
    }

    let binding_names: BTreeSet<&str> = constraints
        .bindings
        .iter()
        .map(|b| b.name.as_str())
        .collect();
    for rule in &constraints.rules {
        check_rule_bindings(&rule.id, &rule.check, &binding_names)?;
    }

    if args.json {
        println!(
            "{}",
            json!({
                "valid": true,
                "constraint_set_id": constraints.constraint_set_id,
                "bindings": constraints.bindings.len(),
                "rules": constraints.rules.len(),
            })
        );
    }

    Ok(())
}

fn check_rule_bindings(
    rule_id: &str,
    check: &Check,
    declared: &BTreeSet<&str>,
) -> Result<(), String> {
    let referenced = check_binding_names(check);
    for name in referenced {
        if !declared.contains(name) {
            return Err(format!(
                "rule {rule_id} references undeclared binding: {name}"
            ));
        }
    }
    Ok(())
}

fn check_binding_names(check: &Check) -> Vec<&str> {
    match check {
        Check::Unique { binding, .. }
        | Check::NotNull { binding, .. }
        | Check::Predicate { binding, .. }
        | Check::RowCount { binding, .. }
        | Check::AggregateCompare { binding, .. } => vec![binding.as_str()],
        Check::ForeignKey {
            binding,
            ref_binding,
            ..
        } => vec![binding.as_str(), ref_binding.as_str()],
        Check::QueryZeroRows { bindings, .. } => bindings.iter().map(String::as_str).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ValidateArgs, execute};

    #[test]
    fn validates_good_arity1_fixture() {
        let result = execute(ValidateArgs {
            compiled_constraints: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/constraints/arity1/not_null_loans.verify.json"
            )
            .into(),
            json: false,
        });

        assert!(result.is_ok());
    }

    #[test]
    fn validates_good_arity_n_fixture() {
        let result = execute(ValidateArgs {
            compiled_constraints: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/constraints/arity_n/foreign_key_property_tenants.verify.json"
            )
            .into(),
            json: false,
        });

        assert!(result.is_ok());
    }

    #[test]
    fn validates_good_query_rules_fixture() {
        let result = execute(ValidateArgs {
            compiled_constraints: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/constraints/query_rules/orphan_rows.verify.json"
            )
            .into(),
            json: false,
        });

        assert!(result.is_ok());
    }

    #[test]
    fn rejects_missing_file() {
        let error = execute(ValidateArgs {
            compiled_constraints: "nonexistent.verify.json".into(),
            json: false,
        })
        .expect_err("missing file should fail");

        assert!(error.contains("failed to read"));
    }

    #[test]
    fn json_mode_prints_summary() {
        let result = execute(ValidateArgs {
            compiled_constraints: concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/constraints/arity1/not_null_loans.verify.json"
            )
            .into(),
            json: true,
        });

        assert!(result.is_ok());
    }
}
