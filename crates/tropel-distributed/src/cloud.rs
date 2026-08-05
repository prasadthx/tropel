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

use std::path::Path;
use tokio::net::TcpListener;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_metrics::collector::MetricsResult;

use crate::controller::run_controller;
use crate::yaml::YamlDoc;

/// Small non-zero Job retry budget. `backoffLimit: 0` fails the whole Job
/// on the FIRST transient pod failure (image-pull race, node scheduling,
/// a controller pod killed while agents reconnect) — with no kubelet
/// retry the entire run is lost. A small budget tolerates these while
/// still bounding how many times a genuinely broken pod restarts.
const JOB_BACKOFF_LIMIT: u64 = 3;

/// Run a distributed load test entirely in this process: bind a local
/// controller, spawn `agents` in-process agent workers over loopback TCP,
/// collect their snapshots, and return the losslessly merged result.
///
/// This is the "cloud-run" mode: `tropel-cloud-run local --config job.json
/// --agents N`. The caller (CLI) reports the merged result and evaluates
/// thresholds, mirroring the controller binary's tail.
pub async fn run_cloud(config: &JobConfig, agents: u32, token: &str) -> Result<MetricsResult> {
    if agents == 0 {
        return Err(TropelError::Config("--agents must be >= 1".into()));
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(TropelError::Io)?;
    let addr = listener.local_addr().map_err(TropelError::Io)?;
    tracing::info!("Cloud-run: controller on {addr}, spawning {agents} in-process agent(s)");

    let token = token.to_string();
    let mut handles = Vec::with_capacity(agents as usize);
    for i in 0..agents {
        let a = addr.to_string();
        let tok = token.clone();
        handles.push(tokio::spawn(async move {
            tracing::debug!("cloud-run agent {i}: connecting to {a}");
            crate::agent::run_agent(&a, &tok).await
        }));
    }

    // The controller accepts each agent in order and waits for its snapshot
    // with a job-bounded timeout — a hung agent fails the run, not the host.
    // On error, abort the in-process agents so no detached tasks keep running
    // the load engine in the background before propagating.
    let merged = match run_controller(listener, config, agents, &token).await {
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
/// 1. `ConfigMap tropel-job` — the serialized job config AND the shared
///    auth token. Agents receive their assignments from the controller over
///    TCP, but they mount the ConfigMap too: the token file must be readable
///    by agent pods so they can authenticate before the controller
///    dispatches the (credential-bearing) config.
/// 2. `Job tropel-controller` — `completions: 1`, listens on
///    `0.0.0.0:<listen_port>`, mounts the job ConfigMap (job.json + token),
///    runs `cloud-run controller --config /etc/tropel/job.json --agents N
///    --token-file /etc/tropel/token`.
///    Run-to-completion: a finished Job is not restarted by kubelet (a
///    Deployment would re-run the finished controller forever).
/// 3. `Service tropel-controller` — ClusterIP so agents resolve the
///    controller by DNS name (`tropel-controller.<ns>.svc`).
/// 4. `Job tropel-agent` — **Indexed** Job (`completions/parallelism =
///    agents`, `completionMode: Indexed`), runs
///    `cloud-run agent --controller tropel-controller:<port>
///    --token-file /etc/tropel/token`, mounting the same ConfigMap. Each pod
///    runs its agent once and exits; the Job completes when all agents do.
/// 5. `Service tropel-agent` — headless (`clusterIP: None`) so each agent
///    pod owns a stable `tropel-agent-<i>.<ns>.svc` DNS name.
///
/// `image` defaults to `tropel:latest`. `namespace` defaults to `default`
/// and is applied to every object's metadata.
///
/// The container command is emitted explicitly as `tropel-cloud-run` (the
/// image may or may not declare an ENTRYPOINT) and the args are its real
/// subcommands (`controller` / `agent`) — there is no `cloud-run`
/// subcommand on either binary.
pub fn generate_k8s_manifests(
    config: &JobConfig,
    agents: u32,
    image: &str,
    namespace: &str,
    listen_port: u16,
    token: &str,
) -> Result<String> {
    if agents == 0 {
        return Err(TropelError::Config("agents must be >= 1".into()));
    }
    let ns = if namespace.is_empty() { "default" } else { namespace };
    let img = if image.is_empty() { "tropel:latest" } else { image };

    // Agent pods have no access to the operator's filesystem: they resolve
    // the scenario from `config.input` (a local path) and the only shared
    // storage is this ConfigMap. So embed the input file's BYTES here and
    // rewrite the serialized job's `input` to the mount path. If the input
    // is not a readable local file (a URL, inline value, or empty), keep
    // the original field — the operator then has to provide it some other
    // way (sidecar, baked into the image).
    let mut effective = config.clone();
    let input_embed: Option<(String, String)> = match std::fs::read(&config.input) {
        Ok(bytes) => {
            let ext = Path::new(&config.input)
                .extension()
                .and_then(|e| e.to_str())
                .filter(|e| !e.is_empty())
                .unwrap_or("json");
            let key = format!("input.{ext}");
            effective.input = format!("/etc/tropel/{key}");
            Some((key, String::from_utf8_lossy(&bytes).into_owned()))
        }
        Err(_) => {
            // A manifest whose `input` points at a path agent pods cannot
            // read is exactly the "cannot start" bug — surface it instead
            // of silently emitting a broken bundle. (URLs/inline/empty
            // inputs keep the original field; the operator must provide
            // the scenario some other way.)
            if !config.input.is_empty() {
                tracing::warn!(
                    "k8s manifests: input '{}' is not a readable local file — \
                     agent pods will not be able to resolve it unless the \
                     scenario is provided another way (sidecar, baked image)",
                    config.input
                );
            }
            None
        }
    };

    let job_json = serde_json::to_string_pretty(&effective)
        .map_err(|e| TropelError::Parse(format!("serialize job config: {e}")))?;

    let mut y = YamlDoc::new();
    y.comment(&format!(
        "Generated by `tropel-cloud-run k8s` — {ns} / {agents} agent(s)"
    ));
    y.comment("Apply:  kubectl apply -f -   (or write to a file first)");

    // 1. ConfigMap tropel-job — the serialized job config AND the shared
    //    auth token (mounted into both controller and agent pods, passed as
    //    --token-file). The controller is reachable by anything in the
    //    cluster via its ClusterIP service, so agents must authenticate
    //    before the controller dispatches the (credential-bearing) config.
    y.kv(0, "apiVersion", "v1");
    y.kv(0, "kind", "ConfigMap");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-job");
    y.kv(1, "namespace", ns);
    y.key(0, "data");
    y.block(1, "job.json", &job_json);
    y.block(1, "token", token);
    if let Some((key, content)) = &input_embed {
        y.block(1, key, content);
    }
    y.separator();

    // 2. Controller Job — run-to-completion.
    y.kv(0, "apiVersion", "batch/v1");
    y.kv(0, "kind", "Job");
    y.key(0, "metadata");
    y.kv(1, "name", "tropel-controller");
    y.kv(1, "namespace", ns);
    y.key(0, "spec");
    y.kv_num(1, "completions", 1);
    y.kv_num(1, "backoffLimit", JOB_BACKOFF_LIMIT);
    y.key(1, "template");
    y.key(2, "metadata");
    y.key(3, "labels");
    y.kv(4, "app", "tropel-controller");
    y.key(2, "spec");
    y.kv(3, "restartPolicy", "Never");
    y.key(3, "containers");
    y.item_kv(4, "name", "controller");
    y.kv(5, "image", img);
    // Explicit command + real subcommand: `tropel-cloud-run controller`.
    y.key(5, "command");
    y.item(6, "tropel-cloud-run");
    y.key(5, "args");
    y.item(6, "controller");
    y.item(6, "--config");
    y.item(6, "/etc/tropel/job.json");
    y.item(6, "--agents");
    y.item(6, &agents.to_string());
    y.item(6, "--listen");
    y.item(6, &format!("0.0.0.0:{listen_port}"));
    y.item(6, "--token-file");
    y.item(6, "/etc/tropel/token");
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
    y.kv_num(1, "backoffLimit", JOB_BACKOFF_LIMIT);
    y.key(1, "template");
    y.key(2, "metadata");
    y.key(3, "labels");
    y.kv(4, "app", "tropel-agent");
    y.key(2, "spec");
    y.kv(3, "restartPolicy", "Never");
    y.key(3, "containers");
    y.item_kv(4, "name", "agent");
    y.kv(5, "image", img);
    // Explicit command + real subcommand: `tropel-cloud-run agent`.
    y.key(5, "command");
    y.item(6, "tropel-cloud-run");
    y.key(5, "args");
    y.item(6, "agent");
    y.item(6, "--controller");
    y.item(6, &format!("tropel-controller:{listen_port}"));
    y.item(6, "--token-file");
    y.item(6, "/etc/tropel/token");
    // The agent mounts the same ConfigMap (job config + token) so it can
    // authenticate before receiving its assignment.
    y.key(5, "volumeMounts");
    y.item_kv(6, "name", "job");
    y.kv(7, "mountPath", "/etc/tropel");
    y.kv_plain(7, "readOnly", "true");
    y.key(2, "volumes");
    y.item_kv(3, "name", "job");
    y.key(4, "configMap");
    y.kv(5, "name", "tropel-job");
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

        let merged = run_cloud(&config, 2, "test-token").await?;
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
        let yaml =
            generate_k8s_manifests(&config, 3, "reg/tropel:v1", "loadtest", 17890, "tok123").unwrap();

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
            "backoffLimit: 3",
            "image: reg/tropel:v1",
            "namespace: loadtest",
            // Explicit command + real subcommands (no `cloud-run` prefix).
            "- \"tropel-cloud-run\"",
            "- \"controller\"",
            "- \"agent\"",
            "tropel-controller:17890",
            "0.0.0.0:17890",
            "--agents",
            "\"3\"",
            "--token-file",
            "/etc/tropel/token",
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
        let yaml = generate_k8s_manifests(&config, 2, "img:v1", "my ns:qa#1", 9000, "tok").unwrap();
        // `:` + space, `#`, and the space are all quoted away.
        assert!(yaml.contains("namespace: \"my ns:qa#1\""));
        assert!(!yaml.contains("namespace: my ns:qa#1"));
        // Image with a quote is escaped, not a raw `"` breaking the doc.
        let yaml = generate_k8s_manifests(&config, 2, "img\"evil:latest", "ns", 9000, "tok").unwrap();
        assert!(yaml.contains("image: \"img\\\"evil:latest\""));
    }

    #[test]
    fn manifests_reject_zero_agents() {
        let config = JobConfig::default();
        assert!(generate_k8s_manifests(&config, 0, "", "default", 17890, "tok").is_err());
    }

    #[test]
    fn manifests_defaults() {
        let yaml = generate_k8s_manifests(&JobConfig::default(), 1, "", "", 17890, "tok").unwrap();
        assert!(yaml.contains("image: tropel:latest"));
        assert!(yaml.contains("namespace: default"));
    }

    #[test]
    fn manifests_embed_input_file_and_rewrite_input_path() {
        // Agent pods have no access to the operator's filesystem — they
        // resolve the scenario from `config.input` (a local path) and the
        // only shared storage is the ConfigMap. The input file's bytes must
        // ride in the ConfigMap AND the embedded job.json's `input` field
        // must point at the mount path, or every agent pod fails to parse.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tropel-k8s-input-{}.json", std::process::id()));
        std::fs::write(&path, r#"{"item":[{"request":{}}]}"#).unwrap();

        let config = JobConfig {
            input: path.to_string_lossy().to_string(),
            input_type: Some("postman".into()),
            ..Default::default()
        };
        let yaml = generate_k8s_manifests(&config, 2, "img:v1", "ns", 9000, "tok").unwrap();

        // The embedded job.json rewrites input to the mount path.
        assert!(yaml.contains("\"input\": \"/etc/tropel/input.json\""));
        // The file bytes themselves are in the ConfigMap under input.json.
        assert!(yaml.contains("input.json: |-"), "input bytes missing from ConfigMap");
        assert!(yaml.contains("{\"item\":[{\"request\":{}}]}"), "file contents missing");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn manifests_keep_unreadable_input_as_is() {
        // A non-file input (URL / inline / empty) can't be embedded — the
        // original `input` field must survive untouched so the operator can
        // provide the scenario another way (sidecar, baked image).
        let config = JobConfig {
            input: "https://example.com/coll.json".into(),
            ..Default::default()
        };
        let yaml = generate_k8s_manifests(&config, 1, "img:v1", "ns", 9000, "tok").unwrap();
        assert!(yaml.contains("\"input\": \"https://example.com/coll.json\""));
        assert!(!yaml.contains("input.json: |-"));
    }

    #[test]
    fn manifests_embed_token_and_mount_for_agents() {
        // The auth token must ride in the ConfigMap so BOTH controller and
        // agent pods can read it (agents authenticate before the controller
        // dispatches the credential-bearing config).
        let yaml =
            generate_k8s_manifests(&JobConfig::default(), 2, "img:v1", "ns", 9000, "secret123").unwrap();
        assert!(yaml.contains("token: |-"), "token block missing");
        assert!(yaml.contains("secret123"), "token value missing");
        // Both jobs mount /etc/tropel (job.json + token) and pass --token-file.
        assert_eq!(yaml.matches("--token-file").count(), 2, "both jobs need --token-file");
        assert_eq!(yaml.matches("mountPath: /etc/tropel").count(), 2, "both jobs mount the config");
    }
}
