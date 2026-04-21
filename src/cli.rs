//! CLI definitions for the `gpt2api-rs` binary.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Top-level command line arguments.
#[derive(Debug, Parser)]
#[command(name = "gpt2api-rs")]
pub struct Cli {
    /// Selected top-level command.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the HTTP service.
    Serve(ServeCommand),
    /// Call admin REST APIs.
    Admin(AdminRootCommand),
}

/// Arguments for `serve`.
#[derive(Debug, clap::Args)]
pub struct ServeCommand {
    /// Listen address for the HTTP server.
    #[arg(long, default_value = "127.0.0.1:8787")]
    pub listen: String,
    /// Root directory that owns all service state.
    #[arg(long)]
    pub storage_dir: PathBuf,
    /// Bearer token used by admin APIs.
    #[arg(long)]
    pub admin_token: String,
}

/// Arguments for `admin`.
#[derive(Debug, clap::Args)]
pub struct AdminRootCommand {
    /// Base URL of the running service.
    #[arg(long)]
    pub base_url: String,
    /// Bearer token used by admin APIs.
    #[arg(long)]
    pub admin_token: String,
    /// Emit machine-readable JSON output.
    #[arg(long, default_value_t = false, global = true)]
    pub json: bool,
    /// Selected admin resource.
    #[command(subcommand)]
    pub resource: AdminResource,
}

/// Admin resource group.
#[derive(Debug, Subcommand)]
pub enum AdminResource {
    /// Account operations.
    #[command(subcommand)]
    Accounts(AdminCommand),
}

/// Account commands.
#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// List imported accounts.
    List,
}
