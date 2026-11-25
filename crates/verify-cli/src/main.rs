#![forbid(unsafe_code)]

use std::{ffi::OsString, process::ExitCode};

mod compile;
mod doctor;
mod paths;
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
    #[arg(long = "robot-triage", action = ArgAction::SetTrue)]
    robot_triage: bool,
    #[arg(long, action = ArgAction::SetTrue, global = true)]
    schema: bool,
    #[arg(long, action = ArgAction::SetTrue, global = true)]
    describe: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Emit the machine-readable CLI capabilities contract")]
    Capabilities(doctor::TopLevelCapabilitiesArgs),
    #[command(about = "Print agent-facing operational guidance")]
    RobotDocs(doctor::TopLevelRobotDocsArgs),
    Run(run::RunArgs),
    Compile(compile::CompileArgs),
    Validate(validate::ValidateArgs),
    Witness(witness::WitnessArgs),
    Doctor(doctor::DoctorArgs),
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
    let args: Vec<OsString> = std::env::args_os().collect();
    if let Some(message) = typo_hint(&args) {
        return emit(refusal_outcome(message));
    }
    emit(dispatch(Cli::parse_from(normalize_shortcut_args(args))))
}

/// Subcommands a first positional argument can be a typo of.
const KNOWN_COMMANDS: &[&str] = &[
    "run",
    "compile",
    "validate",
    "witness",
    "doctor",
    "capabilities",
    "robot-docs",
];

/// `true` when `a` and `b` differ by exactly one insertion, deletion,
/// substitution or adjacent transposition.
fn one_typo_apart(a: &str, b: &str) -> bool {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if a == b {
        return false;
    }
    match a.len().abs_diff(b.len()) {
        0 => {
            let diffs: Vec<usize> = (0..a.len()).filter(|&i| a[i] != b[i]).collect();
            diffs.len() == 1
                || (diffs.len() == 2
                    && diffs[1] == diffs[0] + 1
                    && a[diffs[0]] == b[diffs[1]]
                    && a[diffs[1]] == b[diffs[0]])
        }
        1 => {
            let (long, short) = if a.len() > b.len() {
                (&a, &b)
            } else {
                (&b, &a)
            };
            let skip = (0..short.len())
                .find(|&i| long[i] != short[i])
                .unwrap_or(short.len());
            long[skip + 1..] == short[skip..]
        }
        _ => false,
    }
}

/// The root shortcut (`verify <DATASET> --rules ...`) treats any unknown first
/// word as a dataset path, so a mistyped subcommand such as `verify docter`
/// would otherwise fail with an unrelated missing-argument error. If the word
/// is one typo away from a subcommand and is not an existing path, refuse with
/// a hint instead.
fn typo_hint(args: &[OsString]) -> Option<String> {
    let first = args.get(1)?.to_str()?;
    if first.starts_with('-')
        || KNOWN_COMMANDS.contains(&first)
        || std::path::Path::new(first).exists()
    {
        return None;
    }
    let suggestion = KNOWN_COMMANDS
        .iter()
        .find(|command| one_typo_apart(first, command))?;
    Some(format!(
        "verify: unknown command `{first}`; did you mean `{suggestion}`? (`{first}` is not an existing dataset path either; see `verify --help`)"
    ))
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
                    "run"
                        | "compile"
                        | "validate"
                        | "witness"
                        | "doctor"
                        | "capabilities"
                        | "robot-docs"
                        | "help"
                        | run::SHORTCUT_SUBCOMMAND
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

fn from_doctor(result: doctor::DoctorCommandResult) -> DispatchOutcome {
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
        return DispatchOutcome {
            exit_code: 0,
            stdout: Some(render::operator_contract().to_owned()),
            stderr: None,
        };
    }

    if cli.robot_triage {
        return from_doctor(doctor::execute_top_level_robot_triage());
    }

    match cli.command {
        Some(Command::Capabilities(args)) => {
            from_doctor(doctor::execute_top_level_capabilities(args))
        }
        Some(Command::RobotDocs(args)) => from_doctor(doctor::execute_top_level_robot_docs(args)),
        Some(Command::Run(args)) => from_run(run::execute(args)),
        Some(Command::Compile(args)) => from_command(compile::execute(args)),
        Some(Command::Validate(args)) => from_command(validate::execute(args)),
        Some(Command::Witness(args)) => from_witness(witness::execute(args)),
        Some(Command::Doctor(args)) => from_doctor(doctor::execute(args)),
        Some(Command::Shortcut(args)) => from_run(run::execute_shortcut(args)),
        None => refusal_outcome(root_guidance()),
    }
}

fn root_guidance() -> String {
    [
        "verify: no command or dataset was provided.",
        "For agent discovery, run `verify --robot-triage`, `verify capabilities --json`, or `verify robot-docs guide`.",
        "To evaluate constraints, run `verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH>` or `verify <DATASET> --rules <AUTHORING>`.",
        "For help with verify, run `verify --help`.",
    ]
    .join(" ")
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

    fn args(list: &[&str]) -> Vec<OsString> {
        list.iter().map(OsString::from).collect()
    }

    #[test]
    fn one_typo_covers_substitution_insertion_deletion_transposition() {
        assert!(one_typo_apart("docter", "doctor"));
        assert!(one_typo_apart("runn", "run"));
        assert!(one_typo_apart("rn", "run"));
        assert!(one_typo_apart("wtiness", "witness"));
        assert!(!one_typo_apart("doctor", "doctor"));
        assert!(!one_typo_apart("dctr", "doctor"));
        assert!(!one_typo_apart("dataset", "doctor"));
    }

    #[test]
    fn typo_hint_names_the_subcommand_but_never_hijacks_real_paths() {
        let hint = typo_hint(&args(&["verify", "docter"])).expect("typo should be hinted");
        assert!(hint.contains("did you mean `doctor`"), "{hint}");
        // A real subcommand, a flag, an existing path and a far-off word are all left alone.
        assert_eq!(typo_hint(&args(&["verify", "doctor"])), None);
        assert_eq!(typo_hint(&args(&["verify", "--json"])), None);
        assert_eq!(typo_hint(&args(&["verify", "Cargo.toml"])), None);
        assert_eq!(typo_hint(&args(&["verify", "input.csv"])), None);
        assert_eq!(typo_hint(&args(&["verify"])), None);
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
    fn preserves_doctor_subcommand() {
        let normalized = normalize_shortcut_args(["verify", "doctor", "health", "--json"]);

        assert_eq!(
            normalized,
            vec![
                OsString::from("verify"),
                OsString::from("doctor"),
                OsString::from("health"),
                OsString::from("--json"),
            ]
        );
    }

    #[test]
    fn preserves_top_level_agent_subcommands() {
        assert_eq!(
            normalize_shortcut_args(["verify", "capabilities", "--json"]),
            vec![
                OsString::from("verify"),
                OsString::from("capabilities"),
                OsString::from("--json"),
            ]
        );
        assert_eq!(
            normalize_shortcut_args(["verify", "robot-docs", "guide"]),
            vec![
                OsString::from("verify"),
                OsString::from("robot-docs"),
                OsString::from("guide"),
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
