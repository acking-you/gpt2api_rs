//! Binary entrypoint for `gpt2api-rs`.

use anyhow::Result;
use clap::Parser;
use gpt2api_rs::{
    admin_client,
    app::build_router_with_state,
    cli::{
        AdminCommand, AdminResource, AdminRootCommand, Cli, Command, KeyCommand, ServeCommand,
        UsageCommand,
    },
    config::ResolvedPaths,
    http::admin_api::AdminState,
    storage::Storage,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(command) => {
            let router = build_runtime_router(&command).await?;
            let listener = tokio::net::TcpListener::bind(&command.listen).await?;
            axum::serve(listener, router).await?;
            Ok(())
        }
        Command::Admin(command) => run_admin_command(command).await,
    }
}

/// Bootstraps local storage and constructs the runtime router.
async fn build_runtime_router(command: &ServeCommand) -> Result<axum::Router> {
    let paths = ResolvedPaths::new(command.storage_dir.clone());
    let storage = Storage::open(&paths).await?;
    let admin_state =
        AdminState { admin_token: command.admin_token.clone(), storage: Some(storage) };
    Ok(build_router_with_state(admin_state))
}

async fn run_admin_command(command: AdminRootCommand) -> Result<()> {
    match command.resource {
        AdminResource::Accounts(AdminCommand::List) => {
            let accounts =
                admin_client::list_accounts(&command.base_url, &command.admin_token).await?;
            print_payload(&accounts, command.json)?;
        }
        AdminResource::Keys(KeyCommand::List) => {
            let keys = admin_client::list_keys(&command.base_url, &command.admin_token).await?;
            print_payload(&keys, command.json)?;
        }
        AdminResource::Usage(UsageCommand::List { limit }) => {
            let usage =
                admin_client::list_usage(&command.base_url, &command.admin_token, limit).await?;
            print_payload(&usage, command.json)?;
        }
    }

    Ok(())
}

fn print_payload<T>(value: &T, json: bool) -> Result<()>
where
    T: serde::Serialize + std::fmt::Debug,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{value:#?}");
    }
    Ok(())
}
