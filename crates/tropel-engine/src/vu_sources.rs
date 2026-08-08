//! Per-iteration execution sources for the shared VU loop: the declarative
//! (Postman-style) [`ScenarioVuSource`] and the imperative (k6-style)
//! [`DriverVuSource`]. Split out of the former `vu_loop.rs` god-file; both
//! implement [`VuIterationSource`].

use crate::vu_loop::{VuIterationOutcome, VuIterationSource};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::types::{Sample, TagMap};
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::VUScheduler;
use tropel_ext::traits::{DriverHttpClient, DriverInstance, Protocol, VuContext};
use tropel_pm::bridge::SharedPmState;

// ── Scenario source: VURunner (Postman pm.* declarative execution) ──

pub(crate) struct ScenarioVuSource {
    pub(crate) runner: VURunner,
    pub(crate) pm_state: SharedPmState,
}

#[async_trait]
impl VuIterationSource for ScenarioVuSource {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome {
        let iter_result = self
            .runner
            .run_iteration(iteration_index, data_row, vu_env)
            .await;
        let abort_message = {
            let state = self.pm_state.lock().unwrap();
            if state.abort_requested {
                Some(
                    state
                        .abort_message
                        .clone()
                        .unwrap_or_else(|| "Test aborted by script".to_string()),
                )
            } else {
                None
            }
        };
        VuIterationOutcome {
            samples: iter_result.samples,
            abort_message,
            // Backlog line 98: prerequest/test script errors now surface as
            // failed checks inside samples AND count here so the run exits
            // non-zero when scripts keep failing.
            script_failures: iter_result.script_failures,
        }
    }
}

// ── Driver source: k6-style imperative driver instance ──

pub(crate) struct DriverVuSource {
    pub(crate) instance: Box<dyn DriverInstance>,
    pub(crate) http_client: Arc<dyn DriverHttpClient + Send + Sync>,
    pub(crate) executor_name: String,
    pub(crate) driver_id: String,
    pub(crate) vu_id: u32,
    pub(crate) sc_name: String,
    pub(crate) sched: Arc<VUScheduler>,
    /// The merged run env, cloned ONCE at construction. `VuContext.env` is
    /// only consumed by the k6 driver's sync_globals() to seed
    /// __ENV/__tropel_env, which happens on the FIRST iteration only — so
    /// ctx.env is populated exactly once and never deep-cloned per iteration.
    pub(crate) env: HashMap<String, String>,
    pub(crate) env_attached: bool,
    /// The script's `setup()` return value (serialized JSON), computed once
    /// per scenario by `Driver::setup` before VUs spawn and threaded into
    /// every VU's `VuContext.setup_data` so `export default function (data)`
    /// receives it (k6 lifecycle). `None` when the script declares no setup.
    pub(crate) setup_data: Option<String>,
    /// Registered protocols keyed by URL scheme (backlog line 230) — the
    /// driver-path twin of `VURunner.protocols`, so imperative scripts can
    /// reach third-party protocols, not just the declarative runner.
    pub(crate) protocols: Arc<HashMap<String, Arc<dyn Protocol>>>,
}

#[async_trait]
impl VuIterationSource for DriverVuSource {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        _vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome {
        let mut ctx = VuContext::new(self.vu_id, iteration_index, self.sc_name.clone());
        // Env is immutable for the whole run; sync_globals only reads it on the
        // first iteration (to seed __ENV/__tropel_env once). Attach the cached
        // copy once and skip the per-iteration deep clone. `_vu_env` is the
        // same map every call, so this is a strict optimization — not a
        // semantics change.
        if !self.env_attached {
            ctx.env = self.env.clone();
        }
        ctx.data_row = data_row;
        ctx.http_client = Some(self.http_client.clone());
        ctx.setup_data = self.setup_data.clone();
        ctx.protocols = self.protocols.clone();
        ctx.set_exec_context(
            self.executor_name.clone(),
            self.sched.total_iterations().await,
            self.sched.active_vus().await,
        );
        let result = self.instance.run_iteration(&mut ctx).await;
        // Latch `env_attached` ONLY after a successful iteration: if the first
        // run_iteration fails BEFORE sync_globals seeds __ENV/__tropel_env
        // (e.g. a bridge-registration error), the env must be re-attached on
        // the next attempt so the seed reads the real env — otherwise iteration
        // 2 would seed __ENV from an empty ctx.env for the whole run.
        if !self.env_attached && result.is_ok() {
            self.env_attached = true;
        }
        let mut script_failures = 0u64;
        if let Err(e) = result {
            tracing::warn!(
                "VU {} iteration {} failed: {}",
                self.vu_id,
                iteration_index,
                e
            );
            // Backlog line 98: a driver iteration that errored was only
            // warned — iterations still counted it and the run exited 0.
            // Record a failed check + count so the failure is visible and
            // drives a non-zero exit. Tag prefix matches the runner's
            // `script: <name>` convention (a driver iteration is a script
            // execution too).
            script_failures = 1;
            let mut tags = TagMap::with_capacity(1);
            tags.insert("check", format!("script: driver {} iteration", self.vu_id));
            ctx.samples.push(Sample {
                metric: "checks".into(),
                value: 0.0,
                tags: Arc::new(tags),
                timestamp: std::time::SystemTime::now(),
                sample_type: tropel_core::types::SampleType::Rate,
            });
        }
        let abort_message = if ctx.abort_requested {
            Some(format!(
                "Driver '{}' requested abort: {}",
                self.driver_id,
                ctx.abort_message
                    .clone()
                    .unwrap_or_else(|| "Test aborted by driver".to_string())
            ))
        } else {
            None
        };
        VuIterationOutcome {
            samples: std::mem::take(&mut ctx.samples),
            abort_message,
            script_failures,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tropel_core::config::{ExecutionConfig, ThinkTimeConfig};
    use tropel_core::types::{Request, Response};
    use tropel_core::Result;

    /// Stub protocol whose presence in the context proves the driver path
    /// received the registry's protocol map (backlog line 230).
    struct StubProtocol;
    #[async_trait]
    impl Protocol for StubProtocol {
        fn scheme(&self) -> &str {
            "stub"
        }
        async fn execute(
            &self,
            _req: &Request,
            _config: Option<&serde_json::Value>,
        ) -> Result<tropel_ext::traits::ProtocolOutcome> {
            Ok(tropel_ext::traits::ProtocolOutcome {
                samples: vec![],
                response: None,
            })
        }
    }

    /// Stub driver instance that records whether the VuContext it was handed
    /// carried the protocols map (backlog line 230).
    struct RecordingInstance {
        saw_protocols: Arc<AtomicBool>,
    }
    #[async_trait]
    impl DriverInstance for RecordingInstance {
        async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()> {
            // Check the SPECIFIC scheme, not just non-empty: a wrong-but-
            // populated map would slip past a `!is_empty()` assertion.
            self.saw_protocols
                .store(ctx.protocols.get("stub").is_some(), Ordering::SeqCst);
            Ok(())
        }
    }

    struct StubHttpClient;
    #[async_trait]
    impl DriverHttpClient for StubHttpClient {
        async fn execute(&self, _req: &Request) -> Result<Response> {
            Err(tropel_core::TropelError::Other("stub".into()))
        }
    }

    /// Backlog line 230: `run_driver_vus` used to take NO `protocols`
    /// argument, so a k6/WASM/third-party driver could never reach a
    /// registered protocol — only the declarative runner got the registry's
    /// scheme dispatch. The engine now threads the protocol map into every
    /// driver VU's `VuContext`; this pins that wiring at the `DriverVuSource`
    /// level (no full engine run needed).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_source_threads_protocols_into_vu_context() {
        let mut protocols: HashMap<String, Arc<dyn Protocol>> = HashMap::new();
        protocols.insert("stub".to_string(), Arc::new(StubProtocol));
        let protocols = Arc::new(protocols);

        let saw = Arc::new(AtomicBool::new(false));
        let sched = Arc::new(VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: ThinkTimeConfig::default(),
        }));

        let mut source = DriverVuSource {
            instance: Box::new(RecordingInstance {
                saw_protocols: saw.clone(),
            }),
            http_client: Arc::new(StubHttpClient),
            executor_name: "constant-vus".to_string(),
            driver_id: "stub".to_string(),
            vu_id: 0,
            sc_name: "scenario".to_string(),
            sched,
            env: HashMap::new(),
            env_attached: false,
            setup_data: None,
            protocols,
        };

        source.run_iteration(0, None, &HashMap::new()).await;
        assert!(
            saw.load(Ordering::SeqCst),
            "driver VuContext must receive the registry's protocol map (scheme 'stub')"
        );
    }
}
