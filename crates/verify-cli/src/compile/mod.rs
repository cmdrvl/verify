use std::path::PathBuf;

use clap::Args;

pub mod portable;
pub mod query;

#[derive(Debug, Clone, Args)]
pub struct CompileArgs {
    #[arg(value_name = "AUTHORING")]
    pub authoring: Option<PathBuf>,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub check: bool,
    #[arg(long)]
    pub schema: bool,
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: CompileArgs) -> Result<(), String> {
    let surface = format!(
        "compile ({} + {})",
        portable::scaffold_surface(),
        query::scaffold_surface()
    );
    Err(crate::render::scaffold_message(&surface, args.json))
}
