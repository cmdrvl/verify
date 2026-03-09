#![forbid(unsafe_code)]

mod compile;
mod render;
mod run;
mod validate;
mod witness;

use std::process::ExitCode;

use clap::{ArgAction, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "verify",
    about = "Deterministic constraint evaluation for the epistemic spine."
)]
struct Cli {
    #[arg(long, action = ArgAction::SetTrue)]
    schema: bool,
    #[arg(long, action = ArgAction::SetTrue)]
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
}

fn main() -> ExitCode {
    match dispatch(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

fn dispatch(cli: Cli) -> Result<(), String> {
    if cli.schema {
        return Err(render::scaffold_message("schema", false));
    }

    if cli.describe {
        return Err(render::scaffold_message("describe", false));
    }

    match cli.command {
        Some(Command::Run(args)) => run::execute(args),
        Some(Command::Compile(args)) => compile::execute(args),
        Some(Command::Validate(args)) => validate::execute(args),
        Some(Command::Witness(args)) => witness::execute(args),
        None => Err(render::scaffold_message("root", false)),
    }
}
