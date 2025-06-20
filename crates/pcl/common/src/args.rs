use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser, Clone, Default)]
pub struct CliArgs {
    #[clap(short, long)]
    pub json: bool,
    #[clap(hide = true)]
    pub config_dir: Option<PathBuf>,
}

impl CliArgs {
    pub fn json_output(&self) -> bool {
        self.json
    }
}
