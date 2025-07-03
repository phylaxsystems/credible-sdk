use clap::{Parser, command};
use color_eyre::Result;
use color_eyre::eyre::Report;
use pcl_common::args::CliArgs;
use pcl_core::{
    assertion_da::DaStoreArgs,
    assertion_submission::DappSubmitArgs,
    auth::AuthCommand,
    config::{CliConfig, ConfigArgs},
};
use pcl_phoundry::{build::BuildArgs, phorge_test::PhorgeTest};
use serde_json::json;

const VERSION_MESSAGE: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "\nCommit: ",
    env!("VERGEN_GIT_SHA"),
    "\nBuild Timestamp: ",
    env!("VERGEN_BUILD_TIMESTAMP"),
    "\nDefault DA URL: ",
    pcl_core::default_da_url!(),
);

#[derive(Parser)]
#[command(
    name = "pcl",
    version = VERSION_MESSAGE,
    about = "The Credible CLI for the Credible Layer"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[command(flatten)]
    args: CliArgs,
}

#[derive(clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
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

#[tokio::main]
async fn main() -> Result<()> {
    // Configure color_eyre to hide location information and backtrace messages
    color_eyre::config::HookBuilder::default()
        .display_location_section(true)
        .display_env_section(false)
        .install()?;

    let cli = Cli::parse();
    let mut config = CliConfig::read_from_file(&cli.args).unwrap_or_default();

    // TODO(Odysseas): Convert these commands to return strings to print for json output
    // We can also use something similar like the shell macro from Foundry
    // where a global static lazy is used to signal to every print statement
    // whether it should be a noop or print to stdout/stderr.

    let result = async {
        match cli.command {
            Commands::Test(phorge) => {
                phorge.run().await?;
            }
            Commands::Store(store) => {
                store.run(&cli.args, &mut config).await?;
            }
            Commands::Submit(submit) => {
                submit.run(&cli.args, &mut config).await?;
            }
            Commands::Auth(auth_cmd) => {
                auth_cmd.run(&mut config).await?;
            }
            Commands::Config(config_cmd) => {
                config_cmd.run(&mut config)?;
            }
            Commands::Build(build_cmd) => {
                build_cmd.run()?;
            }
        };
        config.write_to_file(&cli.args)?;
        Ok::<_, Report>(())
    }
    .await;

    if let Err(err) = result {
        if cli.args.json_output() {
            eprintln!(
                "{}",
                json!({
                    "status": "error",
                    "error": {
                        "message": err.to_string(),
                    }
                })
            );
            std::process::exit(1);
        } else {
            return Err(err);
        }
    }

    Ok(())
}

//TODO(GREG): Add integration tests that run cli with all the commands and confirm the output is as
//expected.
//This serves the purpose of forced testing of cli args and output testing. For example
//conflicting short args can fall through CI without tests like this.
//Consider adding unit tests with dapp and da mocks for a quicker 0-1 than running
//the dapp in CI.
