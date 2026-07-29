use crate::client::HttpClient;
use crate::auth::AuthSigner;
use tropel_core::config::HttpConfig;
use tropel_core::types::*;
use tropel_core::scenario::ScenarioItem;
use tropel_core::Result;
use tropel_core::TropelError;

/// HTTP protocol executor.
pub struct HttpProtocol {
    client: HttpClient,
}

impl HttpProtocol {
    /// Create a new HTTP protocol executor.
    pub fn new(config: &HttpConfig) -> Result<Self> {
        let client = HttpClient::new(config)?;
        Ok(Self { client })
    }

    /// Execute a single request item and return a sample.
    pub async fn execute_item(
        &self,
        item: &ScenarioItem,
        resolved_url: &str,
        auth_signer: Option<&dyn AuthSigner>,
    ) -> Result<Sample> {
        let request = item.request.as_ref()
            .ok_or_else(|| TropelError::Http("Item has no request".into()))?;

        let start = std::time::Instant::now();

        // Build a resolved request with the substituted URL
        let resolved_req = Request {
            url: resolved_url.to_string(),
            method: request.method.clone(),
            headers: request.headers.clone(),
            query_params: request.query_params.clone(),
            body: request.body.clone(),
            auth: request.auth.clone(),
            certificate: request.certificate.clone(),
            follow_redirects: request.follow_redirects,
            timeout: request.timeout,
        };

        // Execute the request
        let response = self.client.execute(&resolved_req, auth_signer).await?;

        let duration = start.elapsed();

        // Build a sample from the response
        let mut tags = std::collections::HashMap::new();
        tags.insert("url".to_string(), resolved_url.to_string());
        tags.insert("method".to_string(), request.method.to_string());
        tags.insert("status_code".to_string(), response.status_code.to_string());
        tags.insert("name".to_string(), item.name.clone());
        tags.insert("group".to_string(), "http".to_string());

        let sample = Sample {
            metric: "http_req_duration".to_string(),
            value: duration.as_secs_f64() * 1000.0, // milliseconds
            tags: tags.clone(),
            timestamp: std::time::SystemTime::now(),
            sample_type: SampleType::Trend,
        };

        // Note: The caller is responsible for also emitting a counter sample.
        // We return only the duration sample; the executor collects the counter.
        // _ = Sample { metric: "http_reqs", value: 1.0, ... }
        Ok(sample)
    }

    /// Get the underlying HTTP client (for direct use by the PM bridge).
    pub fn client(&self) -> &HttpClient {
        &self.client
    }
}
