use clap::Parser;
use pcl_common::args::CliArgs;
use pcl_core::{
    DEFAULT_DAPP_URL,
    apply::ApplyArgs,
    auth::AuthCommand,
    config::ConfigArgs,
};
use pcl_phoundry::{
    build::BuildArgs,
    phorge_test::PhorgeTest,
};
use std::sync::OnceLock;

fn version_message() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            format!(
                "{}\nCommit: {}\nBuild Timestamp: {}\nDefault Dapp URL: {}",
                env!("CARGO_PKG_VERSION"),
                env!("VERGEN_GIT_SHA"),
                env!("VERGEN_BUILD_TIMESTAMP"),
                DEFAULT_DAPP_URL,
            )
        })
        .as_str()
}

#[derive(Parser)]
#[command(
    name = "pcl",
    version = version_message(),
    long_version = version_message(),
    about = "The Credible CLI for the Credible Layer"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    #[command(flatten)]
    pub args: CliArgs,
}

#[derive(clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Commands {
    #[command(name = "test")]
    Test(PhorgeTest),
    #[command(name = "apply")]
    Apply(ApplyArgs),
    Auth(AuthCommand),
    #[command(about = "Manage configuration")]
    Config(ConfigArgs),
    #[command(name = "build")]
    Build(BuildArgs),
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_config_show_command() {
        let cli = Cli::try_parse_from(["pcl", "config", "show"]).unwrap();
        assert!(matches!(cli.command, Commands::Config(_)));
    }

    #[test]
    fn parses_apply_command() {
        let cli =
            Cli::try_parse_from(["pcl", "apply", "--root", "./testdata/mock-protocol"]).unwrap();
        match cli.command {
            Commands::Apply(args) => {
                assert_eq!(
                    args.root,
                    std::path::PathBuf::from("./testdata/mock-protocol")
                );
                assert_eq!(
                    args.config,
                    std::path::PathBuf::from("assertions/credible.toml")
                );
                assert!(!args.json);
                assert!(!args.yes);
            }
            _ => panic!("expected apply command"),
        }
    }

    #[test]
    fn parses_apply_command_with_custom_config() {
        let cli = Cli::try_parse_from([
            "pcl",
            "apply",
            "--root",
            "./testdata/mock-protocol",
            "-c",
            "custom/path/credible.toml",
        ])
        .unwrap();
        match cli.command {
            Commands::Apply(args) => {
                assert_eq!(
                    args.config,
                    std::path::PathBuf::from("custom/path/credible.toml")
                );
            }
            _ => panic!("expected apply command"),
        }
    }
}
