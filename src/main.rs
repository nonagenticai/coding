#![forbid(unsafe_code)]
//! Binary entry point. No mode guesses a port or a database:
//! - no arguments: serve the HTTP API (what the container entrypoint runs);
//! - `serve`: the same, spelled out;
//! - `worker`: run the job worker loop;
//! - `check`: pack this working tree, run it in a kata Pod, print the verdict and exit with it;
//! - `hook install|uninstall`: a git hook that runs `check` (fails open);
//! - `--print-openapi`: write the OpenAPI JSON to stdout and exit without reading any env or
//!   touching a database (the parity tooling runs it in a bare workspace).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use sqlx::PgPool;
use tokio::net::TcpListener;

// The crate's own imports stay in their own group so a rename never reorders them.
use coding::client::{self, CheckArgs};
use coding::config::{Config, WorkerConfig};
use coding::domain::JobKind;
use coding::error::StartupError;
use coding::{AppState, app, db, openapi, telemetry, worker};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Print the OpenAPI document as JSON to stdout and exit.
    #[arg(long)]
    print_openapi: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the HTTP API. This is also the default when no subcommand is given.
    Serve,
    /// Run the job worker loop (claim, heartbeat, execute, report).
    Worker,
    /// Check this repository's working tree in a kata Pod; exit with the verdict.
    Check(CheckCmd),
    /// Install or remove a git hook that runs `coding check`.
    Hook(HookCmd),
}

/// The job kinds a check can ask for. All four are executable by the runner today; anything else
/// would answer `unsupported`, which is a worse way to learn the kind is not implemented.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum KindArg {
    /// Formatting, clippy and the test suite (the pre-push gate).
    Check,
    /// Formatting and clippy only (the pre-commit gate).
    Lint,
    Test,
    Build,
}

impl From<KindArg> for JobKind {
    fn from(kind: KindArg) -> Self {
        match kind {
            KindArg::Check => JobKind::Check,
            KindArg::Lint => JobKind::Lint,
            KindArg::Test => JobKind::Test,
            KindArg::Build => JobKind::Build,
        }
    }
}

#[derive(Debug, clap::Args)]
struct CheckCmd {
    /// The app's base URL. Falls back to CODING_URL.
    #[arg(long)]
    url: Option<String>,
    /// The tenant to attribute the job to. Falls back to CODING_TENANT, then `local`.
    #[arg(long)]
    tenant: Option<String>,
    #[arg(long, value_enum, default_value_t = KindArg::Check)]
    kind: KindArg,
    /// How long the job may run inside the Pod, in seconds.
    #[arg(long, default_value_t = 1800)]
    timeout: u32,
    /// Print the job JSON instead of the human verdict.
    #[arg(long)]
    json: bool,
    /// Pack the tree and print the manifest; upload nothing.
    #[arg(long)]
    dry_run: bool,
    /// The repository directory to pack; the current directory by default.
    #[arg(long)]
    dir: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct HookCmd {
    #[command(subcommand)]
    action: HookAction,
}

#[derive(Debug, Subcommand)]
enum HookAction {
    /// Write the hook (refuses to replace one it did not write without --force).
    Install {
        /// Which git hook to write.
        #[arg(long, value_enum, default_value_t = HookName::PrePush)]
        hook: HookName,
        /// The job kind the hook runs. Default: check for pre-push, lint for pre-commit.
        #[arg(long, value_enum)]
        kind: Option<KindArg>,
        /// Replace a hook that coding did not write.
        #[arg(long)]
        force: bool,
        /// The repository directory; the current directory by default.
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Remove the hook, if coding wrote it.
    Uninstall {
        #[arg(long, value_enum, default_value_t = HookName::PrePush)]
        hook: HookName,
        #[arg(long)]
        dir: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HookName {
    PrePush,
    PreCommit,
}

impl HookName {
    fn kind(self) -> client::hook::HookKind {
        match self {
            Self::PrePush => client::hook::HookKind::PrePush,
            Self::PreCommit => client::hook::HookKind::PreCommit,
        }
    }

    fn default_job(self) -> JobKind {
        match self {
            Self::PrePush => JobKind::Check,
            Self::PreCommit => JobKind::Lint,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.print_openapi {
        return match openapi().to_pretty_json() {
            Ok(json) => {
                println!("{json}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: cannot serialise OpenAPI: {e}");
                ExitCode::FAILURE
            }
        };
    }
    let command = cli.command.unwrap_or(Command::Serve);
    // The client modes run before tracing is initialised: their output is for a human at a
    // terminal, and a log line in the middle of a verdict is noise.
    let command = match command {
        Command::Check(cmd) => {
            let code = client::check(CheckArgs {
                url: cmd.url,
                tenant: cmd.tenant,
                kind: cmd.kind.into(),
                timeout_seconds: cmd.timeout,
                json: cmd.json,
                dry_run: cmd.dry_run,
                dir: cmd.dir,
            })
            .await;
            return ExitCode::from(code);
        }
        Command::Hook(cmd) => return run_hook(cmd),
        other => other,
    };
    telemetry::init_tracing();
    let result = match command {
        Command::Serve => serve().await,
        Command::Worker => worker_main().await,
        Command::Check(_) | Command::Hook(_) => unreachable!("handled before telemetry"),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "startup failed");
            ExitCode::FAILURE
        }
    }
}

fn run_hook(cmd: HookCmd) -> ExitCode {
    let dir = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("coding hook: cannot read the working directory: {error}");
            return ExitCode::from(client::EXIT_USAGE);
        }
    };
    let outcome = match cmd.action {
        HookAction::Install {
            hook,
            kind,
            force,
            dir: flag_dir,
        } => {
            let dir = flag_dir.unwrap_or(dir);
            let kind = kind
                .map(JobKind::from)
                .unwrap_or_else(|| hook.default_job());
            client::hook::install(&dir, hook.kind(), kind, force)
                .map(|path| format!("installed {}", path.display()))
        }
        HookAction::Uninstall {
            hook,
            dir: flag_dir,
        } => {
            let dir = flag_dir.unwrap_or(dir);
            client::hook::uninstall(&dir, hook.kind())
                .map(|path| format!("removed {}", path.display()))
        }
    };
    match outcome {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("coding hook: {message}");
            ExitCode::from(client::EXIT_USAGE)
        }
    }
}

/// Connect and migrate. The migrator takes a session-level advisory lock, which is unsafe
/// through pgbouncer's transaction pooling — hence the optional direct URL.
async fn connect_and_migrate(
    database_url: &str,
    migrate_url: Option<&str>,
) -> Result<PgPool, StartupError> {
    let pool = db::connect(database_url).await?;
    match migrate_url {
        Some(url) => {
            let direct = db::connect(url).await?;
            db::migrate(&direct).await?;
            direct.close().await;
        }
        None => db::migrate(&pool).await?,
    }
    tracing::info!("migrations applied");
    Ok(pool)
}

async fn serve() -> Result<(), StartupError> {
    let cfg = Config::from_env()?;
    let pool = connect_and_migrate(&cfg.database_url, cfg.database_migrate_url.as_deref()).await?;

    let listener = TcpListener::bind(cfg.bind_addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(
        listener,
        app(AppState {
            pool,
            default_tenant: cfg.default_tenant.clone(),
        }),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

async fn worker_main() -> Result<(), StartupError> {
    let cfg = WorkerConfig::from_env()?;
    let pool = connect_and_migrate(&cfg.database_url, cfg.database_migrate_url.as_deref()).await?;
    tracing::info!(worker_id = %cfg.worker_id, "worker mode");
    worker::run(pool, cfg.worker_id, shutdown_signal()).await?;
    Ok(())
}

/// SIGTERM (Kubernetes) or Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
    tracing::info!("shutdown signal received");
}
