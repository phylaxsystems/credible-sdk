mod cli;

use crate::cli::{
    Cli,
    Commands,
};
use clap::Parser;
use color_eyre::{
    Result,
    eyre::Report,
};
use pcl_core::config::CliConfig;
use serde_json::json;

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
        }
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
