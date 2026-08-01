//! `tropel-agent` — connect to a `tropel-controller`, run this worker's
//! segment of the job, and ship the raw metrics snapshot back.
//!
//! Usage:
//!   tropel-agent --controller <host:port>

use clap::Parser;
use tropel_core::{Result, TropelError};

#[derive(Parser)]
#[command(name = "tropel-agent", about = "Distributed load-test worker")]
struct Args {
    /// Controller address (host:port).
    #[arg(long, short = 'C', default_value = "127.0.0.1:17890")]
    controller: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    if args.controller.is_empty() {
        return Err(TropelError::Config(
            "--controller must not be empty".into(),
        ));
    }
    tropel_distributed::run_agent(&args.controller).await
}
