//! `tropel-agent` — connect to a `tropel-controller`, run this worker's
//! segment of the job, and ship the raw metrics snapshot back.
//!
//! Usage:
//!   tropel-agent --controller <host:port>

use clap::Parser;
use std::path::PathBuf;
use tropel_core::{Result, TropelError};
use tropel_distributed::resolve_token;

#[derive(Parser)]
#[command(name = "tropel-agent", about = "Distributed load-test worker")]
struct Args {
    /// Controller address (host:port).
    #[arg(long, short = 'C', default_value = "127.0.0.1:17890")]
    controller: String,
    /// Shared auth token (or set TROPEL_TOKEN). Must match the controller's.
    #[arg(long)]
    token: Option<String>,
    /// Read the shared auth token from this file.
    #[arg(long)]
    token_file: Option<PathBuf>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    if args.controller.is_empty() {
        return Err(TropelError::Config(
            "--controller must not be empty".into(),
        ));
    }
    // An agent runs a FULL load-test engine, not just a socket loop — the
    // old hardcoded `#[tokio::main(worker_threads = 2)]` starved it. Build a
    // runtime that scales with available parallelism instead (backlog
    // line 119).
    let rt = tropel_distributed::build_runtime().map_err(TropelError::Io)?;
    rt.block_on(async {
        let token = resolve_token(args.token, args.token_file)?;
        tropel_distributed::run_agent(&args.controller, &token).await
    })
}
