use std::path::PathBuf;
use std::{fs, path::Path};

use clap::Args;

pub mod portable;
pub mod query;

#[derive(Debug, Clone, Args)]
pub struct CompileArgs {
    #[arg(
        value_name = "AUTHORING",
        required_unless_present = "schema",
        conflicts_with = "schema"
    )]
    pub authoring: Option<PathBuf>,
    #[arg(long = "out", alias = "output", conflicts_with_all = ["check", "schema"])]
    pub output: Option<PathBuf>,
    #[arg(long, conflicts_with = "schema")]
    pub check: bool,
    #[arg(long)]
    pub schema: bool,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: CompileArgs) -> Result<(), String> {
    if args.schema {
        println!("{}", crate::render::constraint_schema());
        return Ok(());
    }

    let authoring = args
        .authoring
        .as_deref()
        .expect("clap should require AUTHORING unless --schema is used");

    let compiled = if query::is_query_authoring(authoring) {
        query::compile_from_path(authoring).map_err(|error| error.render(authoring))?
    } else {
        portable::compile_from_path(authoring).map_err(|error| error.render(authoring))?
    };

    if args.check {
        return Ok(());
    }

    let rendered = serde_json::to_string_pretty(&compiled)
        .map_err(|error| format!("failed to serialize compiled constraints: {error}"))?;

    if let Some(output) = args.output {
        write_output(&output, &rendered)
    } else {
        println!("{rendered}");
        Ok(())
    }
}

fn write_output(path: &Path, rendered: &str) -> Result<(), String> {
    let mut payload = rendered.to_owned();
    payload.push('\n');
    fs::write(path, payload).map_err(|error| format!("failed to write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{CompileArgs, execute, portable, query};

    #[test]
    fn yaml_authoring_routes_to_portable_surface() {
        let surface = if query::is_query_authoring(Path::new("rules.yaml")) {
            query::scaffold_surface(false)
        } else {
            portable::scaffold_surface(false)
        };

        assert_eq!(surface, "compile portable authoring");
    }

    #[test]
    fn sql_authoring_routes_to_query_surface() {
        let surface = if query::is_query_authoring(Path::new("rules.sql")) {
            query::scaffold_surface(true)
        } else {
            portable::scaffold_surface(true)
        };

        assert_eq!(surface, "compile --check batch SQL authoring");
    }

    #[test]
    fn compile_check_validates_portable_fixture_without_writing() {
        let authoring = PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/arity1/not_null_loans.yaml"
        ));

        let result = execute(CompileArgs {
            authoring: Some(authoring),
            output: None,
            check: true,
            schema: false,
            json: false,
        });

        assert!(result.is_ok());
    }

    #[test]
    fn compile_check_surfaces_bad_portable_authoring() {
        let authoring = PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/query_rules/orphan_rows.sql"
        ));

        let result = execute(CompileArgs {
            authoring: Some(authoring),
            output: None,
            check: true,
            schema: false,
            json: false,
        });

        assert!(result.is_ok());
    }
}
