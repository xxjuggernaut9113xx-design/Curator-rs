use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "curator-server",
    about = "Curator Server: local and Tailnet media backend"
)]
struct Cli {
    #[arg(long)]
    docs: bool,
    /// Used by service definitions to make the selected mode explicit.
    #[arg(long)]
    background: bool,
    #[arg(long, value_enum, global = true)]
    install_scope: Option<ScopeArg>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy a stopped Host library into an empty machine-wide Server data
    /// directory. Run from an elevated all-users Server installation.
    ImportHost {
        #[arg(long, value_name = "HOST_DATA_DIR")]
        from: PathBuf,
    },
    /// Record P-HAR setup intent without downloading packages or models.
    /// Installers use this only when their opt-in checkbox was selected.
    PharIntent {
        #[arg(long, value_parser = clap::value_parser!(bool), action = clap::ArgAction::Set)]
        enabled: bool,
        #[arg(long, value_enum)]
        backend: Option<BackendArg>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ScopeArg {
    CurrentUser,
    AllUsers,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BackendArg {
    Auto,
    Cuda,
    Rocm,
}

impl From<BackendArg> for curator::phar::PharBackend {
    fn from(value: BackendArg) -> Self {
        match value {
            BackendArg::Auto => Self::Auto,
            BackendArg::Cuda => Self::Cuda,
            BackendArg::Rocm => Self::Rocm,
        }
    }
}

impl From<ScopeArg> for curator::edition::InstallScope {
    fn from(value: ScopeArg) -> Self {
        match value {
            ScopeArg::CurrentUser => Self::CurrentUser,
            ScopeArg::AllUsers => Self::AllUsers,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.docs {
        print!("{}", curator::DOCS_TEXT);
        return Ok(());
    }

    if let Some(command) = cli.command {
        match command {
            Command::ImportHost { from } => {
                let scope = cli.install_scope.unwrap_or(ScopeArg::AllUsers);
                anyhow::ensure!(
                    matches!(scope, ScopeArg::AllUsers),
                    "Host import is intentionally limited to an all-users Server destination."
                );
                let config =
                    curator::config::load_config_for(curator::edition::InstallScope::AllUsers);
                let destination = curator::config::resolve_data_dir_for(
                    &config,
                    curator::edition::InstallScope::AllUsers,
                    cli.data_dir.as_deref(),
                );
                let report = curator::migration::import_host_library(&from, &destination)?;
                curator::migration::configure_all_users_server(&report.destination_data_dir)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            Command::PharIntent { enabled, backend } => {
                let scope = cli.install_scope.unwrap_or(ScopeArg::CurrentUser).into();
                let config = curator::config::load_config_for(scope);
                let data_dir =
                    curator::config::resolve_data_dir_for(&config, scope, cli.data_dir.as_deref());
                let status = curator::phar::record_install_intent(
                    &data_dir,
                    scope,
                    enabled,
                    backend.map(Into::into),
                )?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
        }
        return Ok(());
    }

    let mut options = curator::edition::InitializeOptions::server();
    if let Some(scope) = cli.install_scope {
        options.install_scope = scope.into();
    }
    options.data_dir_override = cli.data_dir;
    let state = curator::initialize_with_options(options).await?;
    tracing::info!(
        edition = state.edition.as_str(),
        install_scope = state.install_scope.as_str(),
        background = cli.background,
        "Curator Server started"
    );
    curator::remote::start_http_server(&state).await?;
    let _ = tokio::signal::ctrl_c().await;
    curator::shutdown(&state).await;
    Ok(())
}
