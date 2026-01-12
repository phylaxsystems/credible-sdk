use clap::Parser;
use pcl_common::args::CliArgs;
use pcl_core::{
    DEFAULT_DA_URL,
    DEFAULT_DAPP_URL,
    assertion_da::DaStoreArgs,
    assertion_submission::DappSubmitArgs,
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
                "{}\nCommit: {}\nBuild Timestamp: {}\nDefault DA URL: {}\nDefault Dapp URL: {}",
                env!("CARGO_PKG_VERSION"),
                env!("VERGEN_GIT_SHA"),
                env!("VERGEN_BUILD_TIMESTAMP"),
                DEFAULT_DA_URL,
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
    #[command(name = "store")]
    Store(DaStoreArgs),
    #[command(name = "submit")]
    Submit(DappSubmitArgs),
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
    fn parses_store_command_with_assertion_flag() {
        let cli = Cli::try_parse_from(["pcl", "--json", "store", "-a", "TestAssertion()"]).unwrap();
        assert!(cli.args.json_output());
        match cli.command {
            Commands::Store(args) => {
                assert_eq!(args.assertion_specs.len(), 1);
                assert_eq!(args.assertion_specs[0].assertion_name, "TestAssertion");
                assert!(args.positional_assertions.is_empty());
            }
            _ => panic!("expected store command"),
        }
    }

    #[test]
    fn parses_store_command_with_positional_specs() {
        let cli =
            Cli::try_parse_from(["pcl", "store", "TestAssertion", "OtherAssertion(arg1,arg2)"])
                .unwrap();

        match cli.command {
            Commands::Store(args) => {
                assert!(args.assertion_specs.is_empty());
                assert_eq!(args.positional_assertions.len(), 2);
                assert_eq!(args.positional_assertions[0], "TestAssertion");
                assert_eq!(args.positional_assertions[1], "OtherAssertion(arg1,arg2)");
            }
            _ => panic!("expected store command"),
        }
    }

    #[test]
    fn parses_submit_command_with_project_name() {
        let cli = Cli::try_parse_from([
            "pcl",
            "submit",
            "-a",
            "TestAssertion(arg)",
            "--project-name",
            "demo",
        ])
        .unwrap();

        match cli.command {
            Commands::Submit(args) => {
                assert_eq!(args.project_name.as_deref(), Some("demo"));
                let keys = args.assertion_keys.expect("assertion keys");
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].assertion_name, "TestAssertion");
                assert_eq!(keys[0].constructor_args, vec!["arg"]);
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn parses_config_show_command() {
        let cli = Cli::try_parse_from(["pcl", "config", "show"]).unwrap();
        assert!(matches!(cli.command, Commands::Config(_)));
    }
}
