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
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};

    use super::{CompileArgs, execute, portable, query};

    fn reserve_temp_directory(stem: &str) -> io::Result<PathBuf> {
        for suffix in 0..1_024 {
            let candidate = std::env::temp_dir().join(format!("{stem}-{suffix}"));
            match fs::create_dir(&candidate) {
                Ok(()) => return Ok(candidate),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not reserve a temporary test directory",
        ))
    }

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

    #[test]
    fn invalid_predicate_authoring_does_not_write_an_artifact() {
        let directory = reserve_temp_directory("verify-invalid-predicate")
            .expect("temporary directory should be reserved");
        let authoring = directory.join("authoring.yaml");
        let output = directory.join("compiled.verify.json");
        fs::write(
            &authoring,
            r#"
constraint_set_id: invalid.cross_binding_predicate
bindings:
  old: { key_fields: [id] }
  new: { key_fields: [id] }
rules:
  - id: VALUE_IMMUTABLE
    severity: error
    portability: portable
    binding: new
    op: predicate
    expr:
      eq:
        - { binding: old, column: value }
        - { binding: new, column: value }
"#,
        )
        .expect("temporary authoring should be written");

        let result = execute(CompileArgs {
            authoring: Some(authoring.clone()),
            output: Some(output.clone()),
            check: false,
            schema: false,
            json: false,
        });

        let output_was_written = output.exists();
        fs::remove_file(authoring).ok();
        fs::remove_file(output).ok();
        fs::remove_dir(directory).ok();

        let error = result.expect_err("invalid authoring must be rejected");
        assert!(error.contains("E_BAD_AUTHORING"));
        assert!(
            !output_was_written,
            "no compiled artifact should be written"
        );
    }

    #[test]
    fn binding_qualified_fixture_compiles_to_identical_output_bytes_twice() {
        let directory = reserve_temp_directory("verify-binding-qualified-compile")
            .expect("temporary directory should be reserved");
        let authoring = PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/authoring/binding_qualified/maturity_date_immutable.yaml"
        ));
        let first = directory.join("first.verify.json");
        let second = directory.join("second.verify.json");

        for output in [&first, &second] {
            execute(CompileArgs {
                authoring: Some(authoring.clone()),
                output: Some(output.clone()),
                check: false,
                schema: false,
                json: false,
            })
            .expect("binding-qualified fixture should compile");
        }

        let first_bytes = fs::read(&first).expect("first artifact should be readable");
        let second_bytes = fs::read(&second).expect("second artifact should be readable");
        fs::remove_file(first).ok();
        fs::remove_file(second).ok();
        fs::remove_dir(directory).ok();

        assert_eq!(first_bytes, second_bytes);
    }
}
