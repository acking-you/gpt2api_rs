//! Binary entrypoint for `gpt2api-rs`.

use anyhow::Result;
use clap::Parser;
use gpt2api_rs::cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(_) => Ok(()),
        Command::Admin(_) => Ok(()),
    }
}
