use clap::Args;

use crate::render;

#[derive(Debug, Clone, Args)]
pub struct WitnessArgs {
    #[arg(value_name = "ACTION")]
    pub action: Option<String>,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: WitnessArgs) -> Result<(), String> {
    Err(render::scaffold_message("witness", args.json))
}
