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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{
        Path,
        PathBuf,
    };

    #[test]
    fn parses_json_flag() {
        let args = CliArgs::try_parse_from(["cli", "--json"]).expect("should parse");
        assert!(args.json_output());
    }

    #[test]
    fn config_dir_can_be_overridden() {
        let args = CliArgs {
            config_dir: Some(PathBuf::from("/tmp/pcl")),
            ..Default::default()
        };
        assert_eq!(args.config_dir.as_deref(), Some(Path::new("/tmp/pcl")));
    }
}
