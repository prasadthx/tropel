use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tropel_core::scenario::Scenario;
use tropel_core::types::{Sample, SampleType, AuthConfig};
use tropel_core::Result;
use tropel_http::protocol::HttpProtocol;
use tropel_js::JsContext;
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

/// Per-VU iteration runner with full HTTP/JS/PM integration.
pub struct VURunner {
    scenario: Arc<Scenario>,
    pm_state: SharedPmState,
    http: Arc<HttpProtocol>,
    config: RunnerConfig,
    js_ctx: Option<Arc<JsContext>>,
}

impl VURunner {
    /// Create a new VU runner.
    pub fn new(scenario: Arc<Scenario>, http: Arc<HttpProtocol>) -> Self {
        // Extract all item names in order for setNextRequest resolution
        let names: Vec<String> = scenario.items.iter().map(|item| item.name.clone()).collect();
        let pm_state = Arc::new(Mutex::new(PmState::new()));
        {
            let mut state = pm_state.lock().unwrap();
            state.set_request_names(names);
        }
        Self {
            scenario,
            pm_state,
            http,
            config: RunnerConfig::default(),
            js_ctx: None,
        }
    }

    /// Attach a JS context for script execution.
    pub fn with_js_context(mut self, js_ctx: Arc<JsContext>) -> Self {
        self.js_ctx = Some(js_ctx);
        self
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
    pub async fn run_iteration(
        &self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        env_vars: &HashMap<String, String>,
    ) -> IterationResult {
        let mut result = IterationResult {
            iteration_index,
            ..Default::default()
        };

        // Set iteration data in PM state for pm.iterationData access
        {
            let mut state = self.pm_state.lock().unwrap();
            state.set_iteration_data(data_row.clone());
        }

        // Build variable scope for this iteration
        let _scope = self.build_scope(data_row.clone(), env_vars).await;
        let resolver = tropel_variables::VariableResolver::new();

        // Walk through the scenario items in order
        let item_count = self.scenario.items.len();
        let mut current_index = 0usize;

        while current_index < item_count {
            // Check for setNextRequest override
            {
                let mut state = self.pm_state.lock().unwrap();
                if let Some(next) = state.next_request.take() {
                    if next < item_count {
                        current_index = next;
                    } else {
                        break;
                    }
                }
            }

            let item = &self.scenario.items[current_index];

            // Process leaf items and folders with children
            if item.items.is_empty() && item.request.is_some() {
                // Set request info in PM state
                {
                    let mut state = self.pm_state.lock().unwrap();
                    state.request = item.request.clone();
                    state.skip_tests = false;
                }

                // Run prerequest script
                if let Some(script) = &item.prerequest {
                    if let Err(e) = self.run_script(script).await {
                        tracing::warn!("VU {} prerequest script error: {}", iteration_index, e);
                    }
                }

                // Rebuild scope after prerequest script (may have changed env vars)
                let data_row_ref = data_row.as_ref();
                let scope = self.build_scope(data_row_ref.cloned(), env_vars).await;

                // Resolve variables across the entire request
                let request = item.request.as_ref().unwrap();
                let resolved_url = resolver.resolve_deep(&request.url, &scope, 5);

                // Resolve headers, query params, body
                let resolved_headers: HashMap<String, String> = request.headers.iter()
                    .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, &scope, 5)))
                    .collect();
                let resolved_query: HashMap<String, String> = request.query_params.iter()
                    .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, &scope, 5)))
                    .collect();
                let resolved_body = request.body.as_ref().map(|b| resolve_body(b, &resolver, &scope));

                // Build the fully resolved request
                let resolved_req = tropel_core::types::Request {
                    url: resolved_url.clone(),
                    method: request.method.clone(),
                    headers: resolved_headers,
                    query_params: resolved_query,
                    body: resolved_body,
                    auth: request.auth.clone(),
                    certificate: request.certificate.clone(),
                    follow_redirects: request.follow_redirects,
                    timeout: request.timeout,
                };

                // Build auth signer from request auth config, or use the scenario-level auth
                let auth_signer = resolved_req.auth.as_ref()
                    .or(self.scenario.auth.as_ref())
                    .and_then(|auth| build_auth_signer(auth));

                tracing::trace!("VU runner: executing request to {}", resolved_req.url);

                // Execute the request via HTTP protocol
                let http_result = self.http.execute_item_with_request(&resolved_req, auth_signer.as_deref()).await;

                tracing::trace!("VU runner: request to {} completed", resolved_req.url);

                match http_result {
                    Ok((sample, http_response)) => {
                        // Set response in PM state (directly from the returned value,
                        // not from a shared slot — avoids race conditions with other VUs)
                        {
                            let mut state = self.pm_state.lock().unwrap();
                            state.response = Some(http_response);
                        }

                        // Record duration sample
                        let tags = sample.tags.clone();
                        result.samples.push(sample);

                        // Also emit a counter
                        let count_sample = tropel_core::types::Sample {
                            metric: "http_reqs".to_string(),
                            value: 1.0,
                            tags,
                            timestamp: std::time::SystemTime::now(),
                            sample_type: SampleType::Counter,
                        };
                        result.samples.push(count_sample);
                    }
                    Err(e) => {
                        tracing::warn!("VU {} request '{}' failed: {}", iteration_index, item.name, e);
                        let err_tags = HashMap::from([
                            ("url".to_string(), resolved_url),
                            ("method".to_string(), request.method.to_string()),
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
                        result.samples.push(error_sample);
                    }
                }

                // Run test script
                if let Some(script) = &item.test {
                    if let Err(e) = self.run_script(script).await {
                        tracing::warn!("VU {} test script error: {}", iteration_index, e);
                    }
                }

                // Collect samples from PM state (checks, custom metrics)
                {
                    let mut state = self.pm_state.lock().unwrap();
                    result.samples.append(&mut state.samples);
                }
            }

            current_index += 1;
        }

        result
    }

    /// Build a variable scope from the current PM state + iteration data + env.
    async fn build_scope(
        &self,
        data_row: Option<HashMap<String, serde_json::Value>>,
        env_vars: &HashMap<String, String>,
    ) -> tropel_variables::VariableScope {
        let data = data_row.unwrap_or_default();
        let state = self.pm_state.lock().unwrap();
        tropel_variables::VariableScope {
            data,
            env: env_vars.clone(),
            collection: state.collection_vars.clone(),
            globals: state.globals.clone(),
        }
    }

    /// Run a JavaScript script via the tropel-js context.
    ///
    /// Uses the cached compilation path: the first call compiles the script
    /// to a Function and stores it in the JS global object; subsequent calls
    /// invoke the cached function directly, avoiding re-parsing.
    async fn run_script(&self, code: &str) -> Result<()> {
        if let Some(ctx) = &self.js_ctx {
            ctx.run_script_cached(code).await
                .map_err(|e| tropel_core::TropelError::Other(format!("Script error: {}", e)))?;
        } else {
            tracing::trace!("Script execution skipped (no JS context): {} chars", code.len());
        }
        Ok(())
    }

    /// Get the current PM state (for the orchestrator to inject response data).
    pub fn state_handle(&self) -> SharedPmState {
        self.pm_state.clone()
    }
}

/// Resolve variables in a request body.
fn resolve_body(
    body: &tropel_core::types::Body,
    resolver: &tropel_variables::VariableResolver,
    scope: &tropel_variables::VariableScope,
) -> tropel_core::types::Body {
    match body {
        tropel_core::types::Body::Raw(s) => {
            tropel_core::types::Body::Raw(resolver.resolve_deep(s, scope, 5))
        }
        tropel_core::types::Body::Json(val) => {
            // Resolve variables in JSON values by stringifying and re-parsing
            let s = serde_json::to_string(val).unwrap_or_default();
            let resolved = resolver.resolve_deep(&s, scope, 5);
            tropel_core::types::Body::Json(serde_json::from_str(&resolved).unwrap_or_else(|_| val.clone()))
        }
        tropel_core::types::Body::FormData(map) => {
            let resolved: HashMap<String, String> = map.iter()
                .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, scope, 5)))
                .collect();
            tropel_core::types::Body::FormData(resolved)
        }
        tropel_core::types::Body::UrlEncoded(map) => {
            let resolved: HashMap<String, String> = map.iter()
                .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, scope, 5)))
                .collect();
            tropel_core::types::Body::UrlEncoded(resolved)
        }
        tropel_core::types::Body::Binary(data) => {
            // Binary bodies can't have variables — pass through
            tropel_core::types::Body::Binary(data.clone())
        }
        tropel_core::types::Body::GraphQL { query, variables } => {
            let resolved_query = resolver.resolve_deep(query, scope, 5);
            let resolved_vars = variables.as_ref().map(|vars| {
                let s = serde_json::to_string(vars).unwrap_or_default();
                let resolved = resolver.resolve_deep(&s, scope, 5);
                serde_json::from_str(&resolved).unwrap_or_else(|_| vars.clone())
            });
            tropel_core::types::Body::GraphQL {
                query: resolved_query,
                variables: resolved_vars,
            }
        }
    }
}

/// Build an auth signer from an AuthConfig.
fn build_auth_signer(auth: &AuthConfig) -> Option<Box<dyn tropel_http::auth::AuthSigner>> {
    match auth {
        AuthConfig::Bearer { token } => {
            Some(Box::new(tropel_http::auth::BearerAuth::new(token)))
        }
        AuthConfig::Basic { username, password } => {
            Some(Box::new(tropel_http::auth::BasicAuth::new(username, password)))
        }
        AuthConfig::ApiKey { key, value, location } => {
            Some(Box::new(tropel_http::auth::ApiKeyAuth::new(key, value, location.clone())))
        }
        _ => {
            tracing::warn!("Auth type {:?} not yet implemented, sending without auth", auth);
            None
        }
    }
}
