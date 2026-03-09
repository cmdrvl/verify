#![forbid(unsafe_code)]

use std::{ffi::OsString, process::ExitCode};

mod compile;
mod render;
#[allow(clippy::result_large_err)]
mod run;
mod validate;
mod witness;

use clap::{ArgAction, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "verify",
    about = "Deterministic constraint evaluation for the epistemic spine.",
    version,
    propagate_version = true
)]
struct Cli {
    #[arg(long, action = ArgAction::SetTrue, global = true)]
    schema: bool,
    #[arg(long, action = ArgAction::SetTrue, global = true)]
    describe: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(run::RunArgs),
    Compile(compile::CompileArgs),
    Validate(validate::ValidateArgs),
    Witness(witness::WitnessArgs),
    #[command(name = run::SHORTCUT_SUBCOMMAND, hide = true)]
    Shortcut(run::ShortcutArgs),
}

#[derive(Debug, PartialEq, Eq)]
struct DispatchOutcome {
    exit_code: u8,
    stdout: Option<String>,
    stderr: Option<String>,
}

fn main() -> ExitCode {
    emit(dispatch(Cli::parse_from(normalize_shortcut_args(
        std::env::args_os(),
    ))))
}

fn emit(outcome: DispatchOutcome) -> ExitCode {
    if let Some(stdout) = outcome.stdout {
        println!("{stdout}");
    }

    if let Some(stderr) = outcome.stderr {
        eprintln!("{stderr}");
    }

    ExitCode::from(outcome.exit_code)
}

fn normalize_shortcut_args<I>(args: I) -> Vec<OsString>
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let mut normalized = args.into_iter().map(Into::into).collect::<Vec<_>>();

    let first = normalized.get(1).and_then(|value| value.to_str());
    let looks_like_shortcut = matches!(
        first,
        Some(candidate)
            if !candidate.starts_with('-')
                && !matches!(
                    candidate,
                    "run" | "compile" | "validate" | "witness" | "help" | run::SHORTCUT_SUBCOMMAND
                )
    );

    if looks_like_shortcut {
        normalized.insert(1, OsString::from(run::SHORTCUT_SUBCOMMAND));
    }

    normalized
}

fn refusal_outcome(message: String) -> DispatchOutcome {
    DispatchOutcome {
        exit_code: 2,
        stdout: None,
        stderr: Some(message),
    }
}

fn run_exit_code(exit: run::RunExit) -> u8 {
    exit.exit_code()
}

fn from_run(result: run::RunCommandResult) -> DispatchOutcome {
    DispatchOutcome {
        exit_code: run_exit_code(result.exit),
        stdout: result.stdout,
        stderr: result.stderr,
    }
}

fn from_witness(result: witness::WitnessCommandResult) -> DispatchOutcome {
    DispatchOutcome {
        exit_code: result.exit_code,
        stdout: result.stdout,
        stderr: result.stderr,
    }
}

fn from_command(result: Result<(), String>) -> DispatchOutcome {
    match result {
        Ok(()) => DispatchOutcome {
            exit_code: 0,
            stdout: None,
            stderr: None,
        },
        Err(message) => refusal_outcome(message),
    }
}

fn dispatch(cli: Cli) -> DispatchOutcome {
    if cli.schema {
        return DispatchOutcome {
            exit_code: 0,
            stdout: Some(render::report_schema().to_owned()),
            stderr: None,
        };
    }

    if cli.describe {
        return refusal_outcome(render::scaffold_message("describe", false));
    }

    match cli.command {
        Some(Command::Run(args)) => from_run(run::execute(args)),
        Some(Command::Compile(args)) => from_command(compile::execute(args)),
        Some(Command::Validate(args)) => from_command(validate::execute(args)),
        Some(Command::Witness(args)) => from_witness(witness::execute(args)),
        Some(Command::Shortcut(args)) => from_run(run::execute_shortcut(args)),
        None => refusal_outcome(render::scaffold_message("root", false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn normalizes_root_shortcut_into_hidden_subcommand() {
        let normalized =
            normalize_shortcut_args(["verify", "fixtures/input.csv", "--rules", "authoring.yaml"]);

        assert_eq!(
            normalized,
            vec![
                OsString::from("verify"),
                OsString::from(run::SHORTCUT_SUBCOMMAND),
                OsString::from("fixtures/input.csv"),
                OsString::from("--rules"),
                OsString::from("authoring.yaml"),
            ]
        );
    }

    #[test]
    fn preserves_explicit_subcommands() {
        let normalized =
            normalize_shortcut_args(["verify", "run", "compiled.json", "--bind", "input=data.csv"]);

        assert_eq!(
            normalized,
            vec![
                OsString::from("verify"),
                OsString::from("run"),
                OsString::from("compiled.json"),
                OsString::from("--bind"),
                OsString::from("input=data.csv"),
            ]
        );
    }

    #[test]
    fn dispatches_run_io_error_to_stderr_in_human_mode() {
        let cli =
            Cli::try_parse_from(["verify", "run", "compiled.json", "--bind", "input=data.csv"])
                .expect("run shell should parse");

        let outcome = dispatch(cli);

        assert_eq!(outcome.exit_code, 2);
        assert!(outcome.stdout.is_none());
        assert!(
            outcome
                .stderr
                .as_deref()
                .is_some_and(|msg| msg.contains("compiled.json"))
        );
    }

    #[test]
    fn dispatches_run_io_error_to_stdout_in_json_mode() {
        let cli = Cli::try_parse_from([
            "verify",
            "run",
            "compiled.json",
            "--bind",
            "input=data.csv",
            "--json",
        ])
        .expect("json run shell should parse");

        let outcome = dispatch(cli);

        assert_eq!(outcome.exit_code, 2);
        assert!(outcome.stderr.is_none());
        assert!(
            outcome
                .stdout
                .as_deref()
                .is_some_and(|stdout| stdout.contains("\"outcome\":\"REFUSAL\"")
                    && stdout.contains("\"code\":\"E_IO\""))
        );
    }

    #[test]
    fn parses_arity_one_shortcut_into_hidden_subcommand() -> Result<(), String> {
        let cli = Cli::try_parse_from(normalize_shortcut_args([
            "verify",
            "fixtures/input.csv",
            "--rules",
            "authoring.yaml",
            "--key",
            "loan_id",
            "--max-rows",
            "1000",
            "--no-witness",
        ]))
        .expect("shortcut shell should parse");

        match cli.command {
            Some(Command::Shortcut(args)) => {
                assert_eq!(args.dataset, std::path::PathBuf::from("fixtures/input.csv"));
                assert_eq!(args.rules, std::path::PathBuf::from("authoring.yaml"));
                assert_eq!(args.key.as_deref(), Some("loan_id"));
                assert_eq!(args.common.max_rows, Some(1000));
                assert!(args.common.no_witness);
                Ok(())
            }
            other => Err(format!("expected hidden shortcut command, got {other:?}")),
        }
    }

    #[test]
    fn maps_run_outcomes_to_exit_codes() {
        assert_eq!(run_exit_code(run::RunExit::Pass), 0);
        assert_eq!(run_exit_code(run::RunExit::Fail), 1);
        assert_eq!(run_exit_code(run::RunExit::Refusal), 2);
    }
}
