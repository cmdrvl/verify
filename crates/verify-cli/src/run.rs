use std::path::PathBuf;

use clap::{ArgAction, Args};

use crate::render;

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[arg(value_name = "COMPILED_CONSTRAINTS")]
    pub compiled_constraints: Option<PathBuf>,
    #[arg(long = "bind", action = ArgAction::Append, value_name = "NAME=PATH")]
    pub binds: Vec<String>,
    #[arg(long = "lock", action = ArgAction::Append, value_name = "LOCK")]
    pub locks: Vec<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: RunArgs) -> Result<(), String> {
    Err(render::scaffold_message("run", args.json))
}
