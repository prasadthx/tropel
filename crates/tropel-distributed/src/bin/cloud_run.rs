//! `tropel-cloud-run` — single-binary distributed load testing.
//!
//! Subcommands:
//!   controller  Run the controller (wait for N agents, merge losslessly).
//!   agent       Run a worker that connects to a controller.
//!   local       Run controller + N agents in one process (CI/laptop mode).
//!   k8s         Generate Kubernetes manifests for a cluster deployment.
//!
//! Examples:
//!   tropel-cloud-run local  --config job.json --agents 4
//!   tropel-cloud-run controller --config job.json --agents 4 --listen 0.0.0.0:17890
//!   tropel-cloud-run agent --controller controller-svc:17890
//!   tropel-cloud-run k8s --config job.json --agents 4 --image reg/tropel:v1 --namespace loadtest

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::net::TcpListener;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};

#[derive(Parser)]
#[command(
    name = "tropel-cloud-run",
    about = "Distributed load-testing in one binary (cloud-run mode)"
)]
struct Args {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the controller: wait for N agents, dispatch segments, merge losslessly.
    Controller {
        /// Job config JSON (a full JobConfig).
        #[arg(long, short = 'c')]
        config: PathBuf,
        /// Number of agent workers to expect.
        #[arg(long, default_value_t = 1)]
        agents: u32,
        /// Listen address for agents.
        #[arg(long, default_value = "127.0.0.1:17890")]
        listen: String,
    },
    /// Run a worker that connects to a controller and ships its snapshot back.
    Agent {
        /// Controller address (host:port).
        #[arg(long, short = 'C', default_value = "127.0.0.1:17890")]
        controller: String,
    },
    /// Run controller + N agents in this process (CI/laptop mode).
    Local {
        /// Job config JSON (a full JobConfig).
        #[arg(long, short = 'c')]
        config: PathBuf,
        /// Number of in-process agent workers.
        #[arg(long, default_value_t = 1)]
        agents: u32,
    },
    /// Generate Kubernetes manifests (ConfigMap + controller + agents).
    K8s {
        /// Job config JSON (a full JobConfig).
        #[arg(long, short = 'c')]
        config: PathBuf,
        /// Number of agent replicas.
        #[arg(long, default_value_t = 1)]
        agents: u32,
        /// Container image for both controller and agents.
        #[arg(long, default_value = "tropel:latest")]
        image: String,
        /// Kubernetes namespace for all objects.
        #[arg(long, default_value = "default")]
        namespace: String,
        /// Controller listen/service port.
        #[arg(long, default_value_t = 17890)]
        port: u16,
        /// Write manifests to this file instead of stdout.
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
    },
}

fn load_config(path: &PathBuf) -> Result<JobConfig> {
    let raw = std::fs::read_to_string(path).map_err(TropelError::Io)?;
    serde_json::from_str(&raw)
        .map_err(|e| TropelError::Parse(format!("invalid job config: {e}")))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    match args.command {
        Cmd::Controller { config, agents, listen } => {
            let config = load_config(&config)?;
            let listener = TcpListener::bind(&listen).await.map_err(TropelError::Io)?;
            tracing::info!("Controller listening on {listen}. Waiting for {agents} agent(s)...");
            let result = tropel_distributed::run_controller(listener, &config, agents).await?;
            tropel_distributed::report_and_thresholds(&config, &result).await
        }
        Cmd::Agent { controller } => {
            if controller.is_empty() {
                return Err(TropelError::Config("--controller must not be empty".into()));
            }
            tropel_distributed::run_agent(&controller).await
        }
        Cmd::Local { config, agents } => {
            let config = load_config(&config)?;
            tracing::info!("Cloud-run local mode: {agents} in-process agent(s)");
            let result = tropel_distributed::run_cloud(&config, agents).await?;
            tropel_distributed::report_and_thresholds(&config, &result).await
        }
        Cmd::K8s { config, agents, image, namespace, port, output } => {
            let config = load_config(&config)?;
            let yaml = tropel_distributed::generate_k8s_manifests(&config, agents, &image, &namespace, port)?;
            match output {
                Some(path) => {
                    std::fs::write(&path, yaml).map_err(TropelError::Io)?;
                    tracing::info!("Manifests written to {}", path.display());
                }
                None => println!("{yaml}"),
            }
            Ok(())
        }
    }
}
