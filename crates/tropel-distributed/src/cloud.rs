//! # Cloud-run mode + Kubernetes manifests
//!
//! The distributed controller/agent pair over TCP is the substrate; this
//! module layers two convenience surfaces on top:
//!
//! 1. **`run_cloud`** — a single-process "cloud-run" that binds a local
//!    controller listener and spawns `agents` in-process agent tasks,
//!    exactly like the e2e test does. Ideal for CI, laptops, and local
//!    smoke runs: one command, N logical workers, lossless central merge.
//! 2. **`generate_k8s_manifests`** — deterministic Kubernetes YAML for the
//!    same topology in a cluster: a ConfigMap carrying the job config, a
//!    controller **Job** + Service, and an agent **Indexed Job** with
//!    `completions/parallelism = agents`, plus a headless Service so each
//!    agent pod has a stable `tropel-agent-<i>.<ns>.svc` DNS name. Agents
//!    reach the controller through the controller Service DNS name, so no
//!    external address configuration is needed.
//!
//! Jobs (not Deployments/StatefulSets) are the right shape here: a load
//! test is **run-to-completion**. A Deployment would restart the exited
//! controller pod forever, and a StatefulSet's kubelet would keep re-running
//! finished agents in a loop. A Job runs each pod once and stays finished;
//! `completionMode: Indexed` plus the headless Service gives agents stable
//! per-pod identity without a StatefulSet.
//!
//! A full CRD-style operator (kube-rs) is deliberately NOT used: the
//! manifest generation is dependency-light, testable offline, and the
//! cluster topology is static per run.

use tokio::net::TcpListener;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_metrics::collector::MetricsResult;

use crate::controller::run_controller;
use crate::yaml::YamlDoc;

/// Run a distributed load test entirely in this process: bind a local
/// controller, spawn `agents` in-process agent workers over loopback TCP,
/// collect their snapshots, and return the losslessly merged result.
///
/// This is the "cloud-run" mode: `tropel-cloud-run local --config job.json
/// --agents N`. The caller (CLI) reports the merged result and evaluates
/// thresholds, mirroring the controller binary's tail.
pub async fn run_cloud(config: &JobConfig, agents: u32) -> Result<MetricsResult> {
    if agents == 0 {
        return Err(TropelError::Config("--agents must be >= 1".into()));
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(TropelError::Io)?;
    let addr = listener.local_addr().map_err(TropelError::Io)?;
    tracing::info!("Cloud-run: controller on {addr}, spawning {agents} in-process agent(s)");

    let mut handles = Vec::with_capacity(agents as usize);
    for i in 0..agents {
        let a = addr.to_string();
        handles.push(tokio::spawn(async move {
            tracing::debug!("cloud-run agent {i}: connecting to {a}");
            crate::agent::run_agent(&a).await
        }));
    }

    // The controller accepts each agent in order and waits for its snapshot
    // with a job-bounded timeout — a hung agent fails the run, not the host.
    // On error, abort the in-process agents so no detached tasks keep running
    // the load engine in the background before propagating.
    let merged = match run_controller(listener, config, agents).await {
        Ok(m) => m,
        Err(e) => {
            for h in &handles {
                h.abort();
            }
            return Err(e);
        }
    };

    for h in handles {
        h.await
            .map_err(|e| TropelError::Other(format!("agent task join failed: {e}")))??
    }
    tracing::info!("Cloud-run: all {agents} agent(s) finished — merged result ready");
    Ok(merged)
}

/// Render a complete Kubernetes manifest bundle for this job.
///
/// Topology (one YAML document per `---` separator, `kubectl apply -f -`
/// ready):
///
/// 1. `ConfigMap tropel-job` — the serialized job config (agents receive
///    their assignments from the controller over TCP, so only the
///    controller mounts this).
/// 2. `Job tropel-controller` — `completions: 1`, listens on
///    `0.0.0.0:<listen_port>`, mounts the job ConfigMap, runs
///    `cloud-run controller --config /etc/tropel/job.json --agents N`.
///    Run-to-completion: a finished Job is not restarted by kubelet (a
///    Deployment would re-run the finished controller forever).
/// 3. `Service tropel-controller` — ClusterIP so agents resolve the
///    controller by DNS name (`tropel-controller.<ns>.svc`).
/// 4. `Job tropel-agent` — **Indexed** Job (`completions/parallelism =
///    agents`, `completionMode: Indexed`), runs
///    `cloud-run agent --controller tropel-controller:<port>`. Each pod
///    runs its agent once and exits; the Job completes when all agents do.
/// 5. `Service tropel-agent` — headless (`clusterIP: None`) so each agent
///    pod owns a stable `tropel-agent-<i>.<ns>.svc` DNS name.
///
/// `image` defaults to `tropel:latest`. `namespace` defaults to `default`
/// and is applied to every object's metadata.
pub fn generate_k8s_manifests(
    config: &JobConfig,
    agents: u32,
    image: &str,
    namespace: &str,
    listen_port: u16,
) -> Result<String> {
    if agents == 0 {
        return Err(TropelError::Config("agents must be >= 1".into()));
    }
    let ns = if namespace.is_empty() { "default" } else { namespace };
    let img = if image.is_empty() { "tropel:latest" } else { image };

    let job_json = serde_json::to_string_pretty(config)
        .map_err(|e| TropelError::Parse(format!("serialize job config: {e}")))?;

    let mut y = YamlDoc::new();
    y.comment(&format!(
        "Generated by `tropel-cloud-run k8s` — {ns} / {agents} agent(s)"
    ));
    y.comment("Apply:  kubectl apply -f -   (or write to a file first)");

    // 1. ConfigMap tropel-job — the serialized job config.
    y.kv(0, "apiVersion", "v1");
    y.kv(0, "kind", "ConfigMap");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-job");
    y.kv(1, "namespace", ns);
    y.key(0, "data");
    y.block(1, "job.json", &job_json);
    y.separator();

    // 2. Controller Job — run-to-completion.
    y.kv(0, "apiVersion", "batch/v1");
    y.kv(0, "kind", "Job");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-controller");
    y.kv(1, "namespace", ns);
    y.key(0, "spec");
    y.kv_num(1, "completions", 1);
    y.kv_num(1, "backoffLimit", 0);
    y.key(1, "template");
    y.key(2, "metadata");
    y.key(3, "labels");
    y.kv(4, "app", "tropel-controller");
    y.key(2, "spec");
    y.kv(3, "restartPolicy", "Never");
    y.key(3, "containers");
    y.item_kv(4, "name", "controller");
    y.kv(5, "image", img);
    y.key(5, "args");
    y.item(6, "cloud-run");
    y.item(6, "controller");
    y.item(6, "--config");
    y.item(6, "/etc/tropel/job.json");
    y.item(6, "--agents");
    y.item(6, &agents.to_string());
    y.item(6, "--listen");
    y.item(6, &format!("0.0.0.0:{listen_port}"));
    y.key(5, "ports");
    y.item_kv(6, "name", "control");
    y.kv_num(7, "containerPort", listen_port);
    y.key(5, "volumeMounts");
    y.item_kv(6, "name", "job");
    y.kv(7, "mountPath", "/etc/tropel");
    // `readOnly: true` must stay an unquoted literal — `true` would
    // otherwise be quoted by the YAML-1.1 resolution guard.
    y.kv_plain(7, "readOnly", "true");
    y.key(2, "volumes");
    y.item_kv(3, "name", "job");
    y.key(4, "configMap");
    y.kv(5, "name", "tropel-job");
    y.separator();

    // 3. Controller Service — stable DNS for agents.
    y.kv(0, "apiVersion", "v1");
    y.kv(0, "kind", "Service");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-controller");
    y.kv(1, "namespace", ns);
    y.key(0, "spec");
    y.key(1, "selector");
    y.kv(2, "app", "tropel-controller");
    y.key(1, "ports");
    y.item_kv(2, "name", "control");
    y.kv_num(3, "port", listen_port);
    y.kv_num(3, "targetPort", listen_port);
    y.separator();

    // 4. Agent Indexed Job.
    y.kv(0, "apiVersion", "batch/v1");
    y.kv(0, "kind", "Job");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-agent");
    y.kv(1, "namespace", ns);
    y.key(0, "spec");
    y.kv_num(1, "completions", agents);
    y.kv_num(1, "parallelism", agents);
    y.kv(1, "completionMode", "Indexed");
    y.kv_num(1, "backoffLimit", 0);
    y.key(1, "template");
    y.key(2, "metadata");
    y.key(3, "labels");
    y.kv(4, "app", "tropel-agent");
    y.key(2, "spec");
    y.kv(3, "restartPolicy", "Never");
    y.key(3, "containers");
    y.item_kv(4, "name", "agent");
    y.kv(5, "image", img);
    y.key(5, "args");
    y.item(6, "cloud-run");
    y.item(6, "agent");
    y.item(6, "--controller");
    y.item(6, &format!("tropel-controller:{listen_port}"));
    y.separator();

    // 5. Agent headless Service — stable per-pod DNS without a StatefulSet.
    y.kv(0, "apiVersion", "v1");
    y.kv(0, "kind", "Service");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-agent");
    y.kv(1, "namespace", ns);
    y.key(0, "spec");
    y.kv(1, "clusterIP", "None");
    y.key(1, "selector");
    y.kv(2, "app", "tropel-agent");
    y.key(1, "ports");
    y.item_kv(2, "name", "control");
    y.kv_num(3, "port", listen_port);
    y.kv_num(3, "targetPort", listen_port);

    Ok(y.finish())
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener as TokioListener;
    use tropel_core::config::{ExecutionConfig, ThinkTimeConfig};
    use tropel_core::Result;

    /// Minimal HTTP/1.1 server answering every request with 200.
    async fn start_http_server() -> std::net::SocketAddr {
        let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        addr
    }

    fn write_collection(base: &str) -> String {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tropel-cloud-run-e2e-{}.json", std::process::id()));
        let json = format!(
            r#"{{"info":{{"_postman_id":"e2e","name":"cloud","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"}},"item":[{{"name":"r1","request":{{"method":"GET","url":"{base}/","header":[]}},"response":[]}}]}}"#
        );
        std::fs::File::create(&path).unwrap().write_all(json.as_bytes()).unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cloud_local_runs_and_merges() -> Result<()> {
        let srv = start_http_server().await;
        let coll = write_collection(&format!("http://{srv}"));

        let config = JobConfig {
            input: coll.clone(),
            input_type: Some("postman".into()),
            execution: ExecutionConfig::SharedIterations {
                iterations: 4,
                max_duration: Some("30s".into()),
                vus: 2,
                graceful_stop: Some("10s".into()),
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        };

        let merged = run_cloud(&config, 2).await?;
        assert_eq!(merged.http_reqs, 4, "merged http_reqs = 4: {}", merged.http_reqs);
        assert_eq!(merged.iterations, 4, "merged iterations = 4");
        let dur = merged.http_req_duration.expect("merged http_req_duration");
        assert_eq!(dur.count, 4);
        assert!(dur.max > 0);

        let _ = std::fs::remove_file(&coll);
        Ok(())
    }

    #[test]
    fn manifests_contain_full_topology() {
        let config = JobConfig {
            input: "coll.json".into(),
            input_type: Some("postman".into()),
            execution: ExecutionConfig::ConstantVus {
                vus: 5,
                duration: "10s".into(),
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        };
        let yaml = generate_k8s_manifests(&config, 3, "reg/tropel:v1", "loadtest", 17890).unwrap();

        for needle in [
            "kind: ConfigMap",
            "name: tropel-job",
            "kind: Job",
            "name: tropel-controller",
            "kind: Service",
            "name: tropel-agent",
            "completions: 3",
            "parallelism: 3",
            "completionMode: Indexed",
            "restartPolicy: Never",
            "clusterIP: None",
            "backoffLimit: 0",
            "image: reg/tropel:v1",
            "namespace: loadtest",
            "cloud-run",
            "controller",
            "agent",
            "tropel-controller:17890",
            "0.0.0.0:17890",
            "--agents",
            "\"3\"",
        ] {
            assert!(yaml.contains(needle), "manifest missing {needle:?}");
        }
        // Run-to-completion: Deployments/StatefulSets would make kubelet
        // re-run finished pods forever — a Job must not contain them.
        assert!(!yaml.contains("kind: Deployment"), "no Deployment allowed");
        assert!(!yaml.contains("kind: StatefulSet"), "no StatefulSet allowed");
        // The job config JSON must be embedded verbatim in the ConfigMap.
        assert!(yaml.contains("\"input\": \"coll.json\""));
        assert!(yaml.contains("\"type\": \"constant-vus\""));
        // ConfigMap is on the controller mount path.
        assert!(yaml.contains("mountPath: /etc/tropel"));
    }

    #[test]
    fn manifests_quote_hostile_values() {
        // Values with YAML-significant characters must be quoted, never
        // interpolated raw (namespace is user-controlled).
        let config = JobConfig::default();
        let yaml = generate_k8s_manifests(&config, 2, "img:v1", "my ns:qa#1", 9000).unwrap();
        // `:` + space, `#`, and the space are all quoted away.
        assert!(yaml.contains("namespace: \"my ns:qa#1\""));
        assert!(!yaml.contains("namespace: my ns:qa#1"));
        // Image with a quote is escaped, not a raw `"` breaking the doc.
        let yaml = generate_k8s_manifests(&config, 2, "img\"evil:latest", "ns", 9000).unwrap();
        assert!(yaml.contains("image: \"img\\\"evil:latest\""));
    }

    #[test]
    fn manifests_reject_zero_agents() {
        let config = JobConfig::default();
        assert!(generate_k8s_manifests(&config, 0, "", "default", 17890).is_err());
    }

    #[test]
    fn manifests_defaults() {
        let yaml = generate_k8s_manifests(&JobConfig::default(), 1, "", "", 17890).unwrap();
        assert!(yaml.contains("image: tropel:latest"));
        assert!(yaml.contains("namespace: default"));
    }
}
