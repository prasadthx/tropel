use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tropel_collection::{collection_to_scenario, parse_collection_file};
use tropel_core::config::{ExecutionConfig, JobConfig, OutputConfig};
use tropel_core::scenario::Scenario;
use tropel_core::types::SampleType;
use tropel_core::{Result, TropelError};
use tropel_executor::scheduler::VUScheduler;
use tropel_ext::registry::ExtensionRegistry;
use tropel_http::client::HttpClient;
use tropel_http::protocol::HttpProtocol;
use tropel_metrics::collector::MetricsCollector;
use tropel_report::{create_reporter, Reporter};

/// The engine orchestrates a complete load test job.
pub struct Engine {
    extension_registry: ExtensionRegistry,
}

impl Engine {
    /// Create a new engine with the given extension registry.
    pub fn new(registry: ExtensionRegistry) -> Self {
        Self {
            extension_registry: registry,
        }
    }

    /// Create a new engine with an empty extension registry.
    pub fn default() -> Self {
        Self {
            extension_registry: ExtensionRegistry::new(),
        }
    }

    /// Run a complete load test job.
    pub async fn run(&self, config: &JobConfig) -> Result<EngineResult> {
        tracing::info!("Starting Tropel load test: {}", config.input);

        // Parse the input file
        let scenario = self.parse_input(&config.input, config).await?;
        let scenario = Arc::new(scenario);

        // Create HTTP client
        let http_client = HttpClient::new(&config.http)?;
        let http_protocol = Arc::new(HttpProtocol::new(&config.http)?);

        // Create metrics collector
        let metrics = Arc::new(MetricsCollector::new());

        // Create the executor
        let executor = VUScheduler::new(&config.execution);
        let stop_signal = executor.stop_signal();

        // Shared iteration counter for shared-iterations mode
        let shared_iterations = Arc::new(Mutex::new(0u64));
        let total_iterations = match &config.execution {
            ExecutionConfig::SharedIterations { iterations, .. } => *iterations,
            _ => u64::MAX,
        };

        // Number of VUs
        let vus = match &config.execution {
            ExecutionConfig::ConstantVus { vus, .. } => *vus,
            ExecutionConfig::RampingVus { start_vus, .. } => *start_vus,
            ExecutionConfig::SharedIterations { vus, .. } => *vus,
            ExecutionConfig::ConstantArrivalRate { pre_alloc_vus, .. } => *pre_alloc_vus,
        };

        // Run the executor
        let metrics_clone = metrics.clone();
        let scenario_clone = scenario.clone();
        let http_protocol_clone = http_protocol.clone();
        let stop_signal_clone = stop_signal.clone();
        let shared_iterations_clone = shared_iterations.clone();
        let env_clone = config.env.clone();

        executor.run(move |sched, vu_id| {
            let metrics = metrics_clone.clone();
            let scenario = scenario_clone.clone();
            let http = http_protocol_clone.clone();
            let stop = stop_signal_clone.clone();
            let shared_iters = shared_iterations_clone.clone();
            let total_iters = total_iterations;
            let vu_env = env_clone.clone();

            tokio::spawn(async move {
                let mut iteration_index = 0u64;

                loop {
                    // Check if we should stop
                    tokio::select! {
                        _ = stop.notified() => {
                            tracing::debug!("VU {} stopping (signal)", vu_id);
                            break;
                        }
                        _ = run_iteration(&scenario, &http, &metrics, vu_id, iteration_index, &vu_env) => {}
                    }

                    iteration_index += 1;

                    // Update iteration count
                    if total_iters != u64::MAX {
                        let mut count = shared_iters.lock().await;
                        *count += 1;
                        if *count >= total_iters {
                            break;
                        }
                    }

                    // Check scheduler
                    if sched.active_vus().await == 0 {
                        break;
                    }
                }
            })
        }).await?;

        // Collect and aggregate metrics
        let results = metrics.results().await;

        // Report results
        let reporters = self.create_reporters(&config.output);
        for reporter in &reporters {
            reporter.report(&results).await?;
        }

        Ok(EngineResult {
            metrics: results,
        })
    }

    /// Parse an input file into a Scenario.
    async fn parse_input(&self, input_path: &str, config: &JobConfig) -> Result<Scenario> {
        // Try to detect and parse using the appropriate adapter
        let collection = parse_collection_file(input_path)
            .map_err(|e| TropelError::Parse(format!("Failed to parse collection: {}", e)))?;

        let scenario = collection_to_scenario(collection, config.env.clone());
        Ok(scenario)
    }

    /// Create reporters based on config.
    fn create_reporters(&self, config: &OutputConfig) -> Vec<Box<dyn Reporter>> {
        let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();

        for name in &config.reporters {
            if let Some(reporter) = create_reporter(name) {
                reporters.push(reporter);
            } else {
                // Check extension registry for custom reporters
                if let Some(_ext) = self.extension_registry.get_output(name) {
                    // Extension reporters are not yet fully integrated here
                    tracing::warn!("Extension reporter '{}' not yet supported in engine runner", name);
                } else {
                    tracing::warn!("Unknown reporter: {}", name);
                }
            }
        }

        reporters
    }

    /// Get a reference to the extension registry.
    pub fn extension_registry(&self) -> &ExtensionRegistry {
        &self.extension_registry
    }
}

/// Result of a full engine run.
#[derive(Debug)]
pub struct EngineResult {
    pub metrics: tropel_metrics::collector::MetricsResult,
}

/// Run a single VU iteration.
async fn run_iteration(
    scenario: &Scenario,
    http: &HttpProtocol,
    metrics: &MetricsCollector,
    vu_id: u32,
    iteration: u64,
    env_vars: &HashMap<String, String>,
) {
    tracing::trace!("VU {} iteration {} starting", vu_id, iteration);

    let scope = tropel_variables::VariableScope {
        data: HashMap::new(),
        env: env_vars.clone(),
        collection: scenario.variables.iter().map(|(k, v)| {
            (k.clone(), v.clone())
        }).collect(),
        globals: HashMap::new(),
    };
    let resolver = tropel_variables::VariableResolver::new();

    // Walk through items in order
    for item in &scenario.items {
        let resolved_url = item.request.as_ref().map(|req| {
            resolver.resolve_deep(&req.url, &scope, 5)
        }).unwrap_or_default();

        // Execute via HTTP
        let sample = http.execute_item(item, &resolved_url, None).await;

        match sample {
            Ok(sample) => {
                // Collect the duration sample
                let tags = sample.tags.clone();
                metrics.record(&sample).await;

                // Also emit a counter
                let count_sample = tropel_core::types::Sample {
                    metric: "http_reqs".to_string(),
                    value: 1.0,
                    tags,
                    timestamp: std::time::SystemTime::now(),
                    sample_type: SampleType::Counter,
                };
                metrics.record(&count_sample).await;
            }
            Err(e) => {
                tracing::warn!("VU {} request '{}' failed: {}", vu_id, item.name, e);
                let err_tags = std::collections::HashMap::from([
                    ("url".to_string(), resolved_url),
                    ("method".to_string(), item.request.as_ref().map(|r| r.method.to_string()).unwrap_or_default()),
                    ("name".to_string(), item.name.clone()),
                    ("error".to_string(), e.to_string()),
                ]);
                let error_sample = tropel_core::types::Sample {
                    metric: "errors".to_string(),
                    value: 1.0,
                    tags: err_tags,
                    timestamp: std::time::SystemTime::now(),
                    sample_type: SampleType::Counter,
                };
                metrics.record(&error_sample).await;
            }
        }

        // Brief pause between requests within a VU
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    tracing::trace!("VU {} iteration {} finished", vu_id, iteration);
}
