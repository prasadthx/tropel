use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tropel_core::config::ExpectedStatus;
use tropel_core::scenario::Scenario;
use tropel_core::types::{Sample, SampleType, TagMap};
use tropel_core::Result;
use tropel_ext::traits::Protocol;
use tropel_http::client::HttpClient;
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
#[derive(Default)]
pub struct RunnerConfig {
    pub max_iterations: Option<u64>,
    pub max_duration: Option<Duration>,
}


/// Per-VU iteration runner with full HTTP/JS/PM integration.
///
/// Each VU owns its own `HttpClient` (own connection pool, cookie jar,
/// and discard_bodies setting) — eliminating connection contention and
/// the N1 race condition where VUs shared a response slot.
pub struct VURunner {
    scenario: Arc<Scenario>,
    pm_state: SharedPmState,
    client: HttpClient,
    config: RunnerConfig,
    js_ctx: Option<Box<JsContext>>,
    /// Registered protocols keyed by URL scheme (e.g. `grpc`, `ws`, or any
    /// third-party scheme), instantiated once per scenario from the
    /// extension registry and shared across VUs. Dispatch is generic: a
    /// URL's scheme is looked up here, so ANY registered protocol runs —
    /// not just hardcoded gRPC/WebSocket slots.
    protocols: Arc<HashMap<String, Arc<dyn Protocol>>>,
    /// Expected status codes/ranges that determine request success.
    /// Controls http_req_failed metric: 1.0 when status is NOT expected.
    expected_statuses: Vec<ExpectedStatus>,
    // ── Execution context (k6 exec.* API) ──
    /// Unique VU identifier.
    pub vu_id: u32,
    /// Name of the currently running scenario.
    pub scenario_name: String,
}

impl VURunner {
    /// Create a new VU runner with a dedicated HTTP client.
    pub fn new(
        scenario: Arc<Scenario>,
        client: HttpClient,
        vu_id: u32,
        scenario_name: String,
    ) -> Self {
        // Extract all item names in order for setNextRequest resolution
        let names: Vec<String> = scenario
            .items
            .iter()
            .map(|item| item.name.clone())
            .collect();
        let pm_state = Arc::new(Mutex::new(PmState::new()));
        {
            let mut state = pm_state.lock().unwrap();
            state.set_request_names(names);
            state.vu_id = vu_id;
            state.scenario_name = scenario_name.clone();
            // Seed collection variables from the scenario (the Postman
            // collection's top-level `variable` section lands in
            // scenario.variables) so `{{var}}` references in URLs, headers,
            // and bodies resolve. CLI env vars were already merged into
            // scenario.variables by the engine before this point.
            state.collection_vars.extend(scenario.variables.clone());
        }
        Self {
            scenario,
            pm_state,
            client,
            config: RunnerConfig::default(),
            js_ctx: None,
            protocols: Arc::new(HashMap::new()),
            // Default: 2xx-3xx = success (matches k6 behavior)
            expected_statuses: vec![ExpectedStatus::Range("200-399".to_string())],
            vu_id,
            scenario_name,
        }
    }

    /// Attach a JS context for script execution.
    pub fn with_js_context(mut self, js_ctx: Box<JsContext>) -> Self {
        self.js_ctx = Some(js_ctx);
        self
    }

    /// Attach the registry-instantiated protocol map so any non-HTTP URL
    /// scheme (`grpc`, `ws`, third-party) dispatches to its registered
    /// protocol instead of the HTTP client.
    pub fn with_protocols(mut self, protocols: Arc<HashMap<String, Arc<dyn Protocol>>>) -> Self {
        self.protocols = protocols;
        self
    }

    /// Set the runner configuration.
    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set expected status codes/ranges for http_req_failed evaluation.
    pub fn with_expected_statuses(mut self, expected: Vec<ExpectedStatus>) -> Self {
        self.expected_statuses = expected;
        self
    }

    /// Attach the execution-context info from the scheduler: the executor
    /// type name and shared handles to the scheduler's ACTIVE-VU and GLOBAL
    /// iteration counters. These back `exec.scenario.executor()`,
    /// `exec.instance.vusActive()`, and `exec.instance.iterationsCompleted()`
    /// (a total across ALL VUs, not just this one).
    pub fn with_exec_context(
        self,
        executor_name: String,
        active_vus: Arc<AtomicU32>,
        global_iterations: Arc<AtomicU64>,
    ) -> Self {
        {
            let mut state = self.pm_state.lock().unwrap();
            state.attach_exec_context(executor_name, active_vus, global_iterations);
        }
        self
    }

    /// Access the PM state.
    pub fn pm_state(&self) -> &SharedPmState {
        &self.pm_state
    }

    /// Run a single iteration through the scenario items.
    pub async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        env_vars: &HashMap<String, String>,
    ) -> IterationResult {
        let mut result = IterationResult {
            iteration_index,
            ..Default::default()
        };

        // Set iteration data and execution context in PM state.
        // vu_id and scenario_name are already set once in new() and never
        // change — only iteration_index is updated each iteration.
        {
            let mut state = self.pm_state.lock().unwrap();
            state.set_iteration_data(data_row.clone());
            state.iteration_index = iteration_index;
        }

        // Walk through the scenario items in order

        // Build variable scope for this iteration
        let _scope = self.build_scope(data_row.clone(), env_vars);
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

            // Process leaf items: execute the request (if present), then run scripts.
            // Items without a request (e.g. transpiled TS/ES module scripts) still
            // execute their prerequest and test scripts.
            if item.items.is_empty()
                && (item.request.is_some() || item.prerequest.is_some() || item.test.is_some())
            {
                // Set request info in PM state
                {
                    let mut state = self.pm_state.lock().unwrap();
                    state.request = item.request.clone();
                    state.skip_tests = false;
                    state.current_request_name = item.name.clone();
                }

                // Run prerequest script
                if let Some(script) = &item.prerequest {
                    let source_url = Some(format!("{}.prerequest.js", item.name));
                    if let Err(e) =
                        Self::run_script(&mut self.js_ctx, script, source_url).await
                    {
                        tracing::warn!("VU {} prerequest script error: {}", iteration_index, e);
                    }
                }

                // Rebuild scope after prerequest script (may have changed env vars)
                let data_row_ref = data_row.as_ref();
                let scope = self.build_scope(data_row_ref.cloned(), env_vars);

                // Execute HTTP request only if this item has one.
                // Script-only items (transpiled TS/ES module scripts) don't have
                // a request — they handle HTTP via pm.sendRequest internally.
                if let Some(request) = &item.request {
                    // Resolve variables across the entire request
                    let resolved_url = resolver.resolve_deep(&request.url, &scope, 5);

                    // Resolve headers, query params, body
                    let resolved_headers: HashMap<String, String> = request
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, &scope, 5)))
                        .collect();
                    let resolved_query: HashMap<String, String> = request
                        .query_params
                        .iter()
                        .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, &scope, 5)))
                        .collect();
                    let resolved_body = request
                        .body
                        .as_ref()
                        .map(|b| resolve_body(b, &resolver, &scope));

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
                        response_type: request.response_type,
                    };

                    // ── gRPC protocol dispatch (grpc:// or grpcs://) ──
                    // When the URL uses the gRPC scheme, dispatch to the
                    // registered protocol instead of the HTTP client. The
                    // protocol resolves its proto source from request
                    // headers / config / env and returns both the metric
                    // samples and a Response for pm.response.
                    // Scheme-driven dispatch: ANY registered protocol (gRPC,
                    // WebSocket, or a third-party one) runs when its scheme
                    // matches the URL. TLS-suffixed schemes (grpcs, wss) map
                    // to the base registration (grpc, ws) when not registered
                    // verbatim. The protocol returns both the metric samples
                    // and a Response for pm.response.
                    let scheme = resolved_url.split("://").next().unwrap_or("");
                    // http/https are the built-in HTTP path — never dispatch
                    // them to a registered protocol (also closes the latent
                    // https→http strip fallback foot-gun).
                    let is_http_scheme = matches!(scheme, "http" | "https");
                    let protocol = if is_http_scheme {
                        None
                    } else {
                        self.protocols
                            .get(scheme)
                            .or_else(|| self.protocols.get(scheme.strip_suffix('s').unwrap_or("")))
                            .cloned()
                    };
                    // A clearly non-HTTP scheme with no registered protocol:
                    // warn and SKIP (parity with the old 'no gRPC protocol
                    // registered — skipping' behavior) instead of producing a
                    // confusing reqwest error.
                    if protocol.is_none() && !is_http_scheme {
                        tracing::warn!(
                            "VU {}: {}:// URL '{}' but no protocol registered for scheme '{}' — skipping",
                            iteration_index,
                            scheme,
                            resolved_url,
                            scheme
                        );
                    }
                    if let Some(proto) = protocol {
                        let exec_start = Instant::now();
                        match proto.execute(&resolved_req, None).await {
                            Ok(outcome) => {
                                let duration = exec_start.elapsed();
                                tracing::trace!(
                                    "VU runner: {}:// call to {} completed in {:?}",
                                    scheme,
                                    resolved_req.url,
                                    duration
                                );
                                if let Some(resp) = outcome.response {
                                    let mut state = self.pm_state.lock().unwrap();
                                    state.response = Some(resp);
                                }
                                result.samples.extend(outcome.samples);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "VU {} {}:// request '{}' failed: {}",
                                    iteration_index,
                                    scheme,
                                    item.name,
                                    e
                                );
                                let err_tags = Arc::new(TagMap::from_pairs([
                                    ("url", resolved_url.clone()),
                                    ("method", request.method.to_string()),
                                    ("name", item.name.clone()),
                                    ("error", e.to_string()),
                                ]));
                                let now = std::time::SystemTime::now();
                                result.samples.push(tropel_core::types::Sample {
                                    metric: "errors".into(),
                                    value: 1.0,
                                    tags: err_tags,
                                    timestamp: now,
                                    sample_type: SampleType::Counter,
                                });
                            }
                        }
                    } else if !is_http_scheme {
                        // Warned above; skip — never send a non-HTTP scheme to
                        // the HTTP client (reqwest would fail confusingly).
                    } else {
                    // Build auth signer from request auth config, or use the scenario-level auth
                    let auth_signer = resolved_req
                        .auth
                        .as_ref()
                        .or(self.scenario.auth.as_ref())
                        .and_then(|auth| tropel_http::auth::build_auth_signer(auth));

                    // Execute the request directly via the per-VU HTTP client
                    tracing::trace!("VU runner: executing request to {}", resolved_req.url);

                    let exec_start = Instant::now();
                    let exec_result = self
                        .client
                        .execute(&resolved_req, auth_signer.as_deref())
                        .await;
                    let duration = exec_start.elapsed();

                    tracing::trace!(
                        "VU runner: request to {} completed in {:?}",
                        resolved_req.url,
                        duration
                    );

                    match exec_result {
                        Ok(http_response) => {
                            // Convert to core Response and store in PM state (by move — no shared slot)
                            let pm_response = tropel_core::types::Response::from(&http_response);
                            {
                                let mut state = self.pm_state.lock().unwrap();
                                state.response = Some(pm_response);
                            }

                            // Emit samples for EVERY redirect hop plus the final
                            // response (k6 parity: a 302 chain counts as hops + 1
                            // requests, not just the final — the earlier
                            // k6_sample_basic comparison showed 136 reqs for 68
                            // iterations while Tropel recorded 64). The final
                            // response's URL/status/body is what pm.response
                            // exposes; each hop gets its own sample set.
                            let chain = http_response
                                .redirects
                                .iter()
                                .chain(std::iter::once(&http_response));
                            for resp in chain {
                                // Build tags for all request-level metrics
                                let mut tags = TagMap::with_capacity(5);
                                tags.insert("url", resp.url.clone());
                                tags.insert("method", resolved_req.method.to_string());
                                tags.insert("status", resp.status_code.to_string());
                                tags.insert("name", resp.url.clone());
                                tags.insert("group", "http");
                                // Share one Arc so all ~12 per-request samples bump a
                                // refcount instead of copying the whole map.
                                let tags = Arc::new(tags);

                                let now = std::time::SystemTime::now();

                                // http_req_duration (Trend) — this hop's own time
                                result.samples.push(Sample {
                                    metric: "http_req_duration".into(),
                                    value: resp.response_time.as_micros() as f64,
                                    tags: tags.clone(),
                                    timestamp: now,
                                    sample_type: SampleType::Trend,
                                });

                                // http_reqs (Counter)
                                result.samples.push(Sample {
                                    metric: "http_reqs".into(),
                                    value: 1.0,
                                    tags: tags.clone(),
                                    timestamp: now,
                                    sample_type: SampleType::Counter,
                                });

                                // http_req_failed (Rate) — true when status not in expected list
                                let is_failed = !tropel_core::config::status_is_expected(
                                    resp.status_code,
                                    &self.expected_statuses,
                                );
                                result.samples.push(Sample {
                                    metric: "http_req_failed".into(),
                                    value: if is_failed { 1.0 } else { 0.0 },
                                    tags: tags.clone(),
                                    timestamp: now,
                                    sample_type: SampleType::Rate,
                                });

                                // data_received (Counter) — response body bytes
                                result.samples.push(Sample {
                                    metric: "data_received".into(),
                                    value: resp.size as f64,
                                    tags: tags.clone(),
                                    timestamp: now,
                                    sample_type: SampleType::Counter,
                                });

                                // data_sent (Counter) — request body bytes
                                result.samples.push(Sample {
                                    metric: "data_sent".into(),
                                    value: resp.request_body_size as f64,
                                    tags: tags.clone(),
                                    timestamp: now,
                                    sample_type: SampleType::Counter,
                                });

                                // ═══════════════════════════════════════
                                // HTTP sub-timing metrics (Trend, all in μs)
                                // ═══════════════════════════════════════
                                // These match k6's http_req_* sub-timing
                                // metrics. http_req_dns is a Tropel extra (k6
                                // folds DNS into http_req_blocked).
                                // blocked/dns/connecting are REAL (from
                                // reqwest's dns_resolver + connector_layer
                                // hooks); tls_handshaking/sending are always
                                // ZERO (folded into connecting / waiting by
                                // reqwest). waiting (TTFB) and receiving are
                                // always measured. Note: on a pooled keep-alive
                                // reuse no connector call happens, so
                                // blocked/dns/connecting are 0.
                                if let Some(timings) = &resp.timings {
                                    let sub_timing_metrics = [
                                        ("http_req_blocked", timings.blocked),
                                        ("http_req_dns", timings.dns),
                                        ("http_req_connecting", timings.connecting),
                                        ("http_req_tls_handshaking", timings.tls_handshaking),
                                        ("http_req_sending", timings.sending),
                                        ("http_req_waiting", timings.waiting),
                                        ("http_req_receiving", timings.receiving),
                                    ];
                                    let sub_tags = tags.clone();
                                    for (metric_name, dur) in &sub_timing_metrics {
                                        result.samples.push(Sample {
                                            metric: (*metric_name).into(),
                                            value: dur.as_micros() as f64,
                                            tags: sub_tags.clone(),
                                            timestamp: now,
                                            sample_type: SampleType::Trend,
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "VU {} request '{}' failed: {}",
                                iteration_index,
                                item.name,
                                e
                            );
                            let err_tags = Arc::new(TagMap::from_pairs([
                                ("url", resolved_url.clone()),
                                ("method", request.method.to_string()),
                                ("name", item.name.clone()),
                                ("error", e.to_string()),
                            ]));
                            let now = std::time::SystemTime::now();
                            result.samples.push(tropel_core::types::Sample {
                                metric: "errors".into(),
                                value: 1.0,
                                tags: err_tags.clone(),
                                timestamp: now,
                                sample_type: SampleType::Counter,
                            });
                            // Connection errors always count as failed requests
                            result.samples.push(tropel_core::types::Sample {
                                metric: "http_req_failed".into(),
                                value: 1.0,
                                tags: err_tags,
                                timestamp: now,
                                sample_type: SampleType::Rate,
                            });
                        }
                    }
                    }
                }

                // Run test script
                if let Some(script) = &item.test {
                    let source_url = Some(format!("{}.test.js", item.name));
                    if let Err(e) =
                        Self::run_script(&mut self.js_ctx, script, source_url).await
                    {
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
    ///
    /// Deliberately synchronous (`&self`, no `.await`): the `#[async_trait]`
    /// `VuIterationSource` future must be `Send`, and an async `&self` method
    /// would hold `&VURunner` across an await — `&VURunner: Send` requires
    /// `VURunner: Sync`, which the now-`!Sync` `JsContext` can't satisfy.
    fn build_scope(
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
    /// Uses the cached compilation path. The cached wrapper is an **async**
    /// function, so top-level `await` / `Promise` in user scripts is valid
    /// everywhere — there is no fragile substring sniffing to pick between
    /// sync and async paths. Any returned Promise is driven to completion and
    /// rejections surface as errors.
    ///
    /// `source_url` is an identifier shown in error messages and stack traces
    /// (e.g. `"prerequest.js"` or `"test.js"`). When omitted, errors show
    /// the raw source without a meaningful label.
    /// Run a script in the VU's JS context. Takes the context by itself (not
    /// `&mut self`) so callers holding an immutable borrow of another field
    /// (e.g. `&self.scenario.items[i]`) don't trip the borrow checker — the
    /// js_ctx field is disjoint from scenario data.
    async fn run_script(
        js_ctx: &mut Option<Box<JsContext>>,
        code: &str,
        source_url: Option<String>,
    ) -> Result<()> {
        if let Some(ctx) = js_ctx {
            ctx.run_script_cached(code, source_url)
                .await
                .map_err(|e| tropel_core::TropelError::Other(format!("Script error: {}", e)))?;
        } else {
            tracing::trace!(
                "Script execution skipped (no JS context): {} chars",
                code.len()
            );
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
            tropel_core::types::Body::Json(
                serde_json::from_str(&resolved).unwrap_or_else(|_| val.clone()),
            )
        }
        tropel_core::types::Body::FormData(map) => {
            let resolved: HashMap<String, String> = map
                .iter()
                .map(|(k, v)| (k.clone(), resolver.resolve_deep(v, scope, 5)))
                .collect();
            tropel_core::types::Body::FormData(resolved)
        }
        tropel_core::types::Body::UrlEncoded(map) => {
            let resolved: HashMap<String, String> = map
                .iter()
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


