use std::path::PathBuf;

use clap::Args;

use crate::render;

#[derive(Debug, Clone, Args)]
pub struct ValidateArgs {
    #[arg(value_name = "COMPILED_CONSTRAINTS")]
    pub compiled_constraints: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: ValidateArgs) -> Result<(), String> {
    Err(render::scaffold_message("validate", args.json))
}
