//! Binary entrypoint for `gpt2api-rs`.

use anyhow::Result;
use clap::Parser;
use gpt2api_rs::{
    app::build_router_for_tests,
    cli::{Cli, Command},
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(_) => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:8787").await?;
            axum::serve(listener, build_router_for_tests()).await?;
            Ok(())
        }
        Command::Admin(_) => Ok(()),
    }
}
