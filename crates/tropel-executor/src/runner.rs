use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use tropel_core::scenario::Scenario;
use tropel_core::types::Sample;
use tropel_core::Result;
use tropel_pm::bridge::{PmState, SharedPmState};

/// Result of running a VU iteration.
#[derive(Debug, Default)]
pub struct IterationResult {
    pub samples: Vec<Sample>,
    pub iteration_index: u64,
}

/// Configuration for a VU runner.
#[derive(Clone)]
pub struct RunnerConfig {
    pub max_iterations: Option<u64>,
    pub max_duration: Option<Duration>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_iterations: None,
            max_duration: None,
        }
    }
}

/// Per-VU iteration runner.
pub struct VURunner {
    scenario: Arc<Scenario>,
    pm_state: SharedPmState,
    config: RunnerConfig,
}

impl VURunner {
    /// Create a new VU runner.
    pub fn new(scenario: Arc<Scenario>) -> Self {
        Self {
            scenario,
            pm_state: Arc::new(Mutex::new(PmState::new())),
            config: RunnerConfig::default(),
        }
    }

    /// Set the runner configuration.
    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.config = config;
        self
    }

    /// Access the PM state.
    pub fn pm_state(&self) -> &SharedPmState {
        &self.pm_state
    }

    /// Run a single iteration through the scenario items.
    pub async fn run_iteration(&self, iteration_index: u64, data_row: Option<std::collections::HashMap<String, serde_json::Value>>) -> IterationResult {
        let mut result = IterationResult {
            iteration_index,
            ..Default::default()
        };

        // Resolve variables for this iteration
        let scope = tropel_variables::VariableScope {
            data: data_row.unwrap_or_default(),
            env: {
                let state = self.pm_state.lock().await;
                state.environment.clone()
            },
            collection: {
                let state = self.pm_state.lock().await;
                state.collection_vars.clone()
            },
            globals: {
                let state = self.pm_state.lock().await;
                state.globals.clone()
            },
        };

        let resolver = tropel_variables::VariableResolver::new();

        // Walk through the scenario items in order
        let item_count = self.scenario.items.len();
        let mut current_index = 0usize;

        while current_index < item_count {
            // Check for setNextRequest override
            {
                let mut state = self.pm_state.lock().await;
                if let Some(next) = state.next_request.take() {
                    if next < item_count {
                        current_index = next;
                    } else {
                        break;
                    }
                }
            }

            let item = &self.scenario.items[current_index];

            // If this is a folder, process its children
            if item.items.is_empty() && item.request.is_some() {
                // Run prerequest script
                if let Some(script) = &item.prerequest {
                    if let Err(e) = self.run_script(script).await {
                        tracing::warn!("Prerequest script error: {}", e);
                    }
                }

                // Resolve URL variables
                let url = if let Some(req) = &item.request {
                    resolver.resolve_deep(&req.url, &scope, 5)
                } else {
                    String::new()
                };

                // TODO: Execute the request via the HTTP protocol
                // This is handled by the top-level executor which orchestrates
                // the HTTP protocol alongside the VU runner.

                // Run test script
                if let Some(script) = &item.test {
                    if let Err(e) = self.run_script(script).await {
                        tracing::warn!("Test script error: {}", e);
                    }
                }

                // Collect samples from PM state
                {
                    let mut state = self.pm_state.lock().await;
                    result.samples.append(&mut state.samples);
                }
            }

            current_index += 1;
        }

        result
    }

    /// Run a JavaScript script (async-safe stub for now).
    async fn run_script(&self, _code: &str) -> Result<()> {
        // TODO: Execute via tropel-js context
        // For now, we simulate the script execution
        Ok(())
    }

    /// Get the current PM state (for the orchestrator to inject response data).
    pub fn state_handle(&self) -> SharedPmState {
        self.pm_state.clone()
    }
}
