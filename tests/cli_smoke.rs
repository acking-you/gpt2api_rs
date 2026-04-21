//! CLI smoke tests for the `gpt2api-rs` binary.

use clap::Parser;
use gpt2api_rs::cli::{AdminCommand, AdminResource, Cli, Command};

/// Parses the `serve` command shape.
#[test]
fn parse_serve_command() {
    let cli = Cli::parse_from([
        "gpt2api-rs",
        "serve",
        "--listen",
        "127.0.0.1:8787",
        "--storage-dir",
        "/tmp/gpt2api",
        "--admin-token",
        "secret",
    ]);

    match cli.command {
        Command::Serve(cmd) => {
            assert_eq!(cmd.listen, "127.0.0.1:8787");
            assert_eq!(cmd.storage_dir.to_string_lossy(), "/tmp/gpt2api");
            assert_eq!(cmd.admin_token, "secret");
        }
        other => panic!("expected serve command, got {other:?}"),
    }
}

/// Parses the `admin accounts list` command shape.
#[test]
fn parse_admin_accounts_list_command() {
    let cli = Cli::parse_from([
        "gpt2api-rs",
        "admin",
        "--base-url",
        "http://127.0.0.1:8787",
        "--admin-token",
        "secret",
        "accounts",
        "list",
        "--json",
    ]);

    match cli.command {
        Command::Admin(cmd) => {
            assert_eq!(cmd.base_url, "http://127.0.0.1:8787");
            assert_eq!(cmd.admin_token, "secret");
            assert!(cmd.json);
            assert!(matches!(cmd.resource, AdminResource::Accounts(AdminCommand::List)));
        }
        other => panic!("expected admin command, got {other:?}"),
    }
}
