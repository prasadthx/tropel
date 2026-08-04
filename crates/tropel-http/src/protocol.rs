use crate::auth::AuthSigner;
use crate::client::HttpClient;
use tropel_core::config::HttpConfig;
use tropel_core::types::*;
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

    /// Execute a request item — resolves variables in URL only (legacy path).
    pub async fn execute_item(
        &self,
        item: &tropel_core::scenario::ScenarioItem,
        resolved_url: &str,
        auth_signer: Option<&dyn AuthSigner>,
    ) -> Result<Sample> {
        let request = item
            .request
            .as_ref()
            .ok_or_else(|| TropelError::Http("Item has no request".into()))?;

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
            response_type: request.response_type,
        };

        self.execute_item_with_request(&resolved_req, auth_signer)
            .await
            .map(|(sample, _)| sample)
    }

    /// Execute a fully-resolved request and return a duration sample along with
    /// the full response data. The response is returned directly to avoid a race
    /// condition where multiple VUs sharing the same HttpClient would overwrite
    /// each other's responses in a shared last_response slot.
    pub async fn execute_item_with_request(
        &self,
        resolved_req: &Request,
        auth_signer: Option<&dyn AuthSigner>,
    ) -> Result<(Sample, tropel_core::types::Response)> {
        let start = std::time::Instant::now();

        // Execute the request
        let response = self.client.execute(resolved_req, auth_signer).await?;

        let duration = start.elapsed();
        let http_response = tropel_core::types::Response::from(&response);

        // Build a sample with interned tags
        let mut tags = TagMap::with_capacity(5);
        tags.insert("url", resolved_req.url.clone());
        tags.insert("method", resolved_req.method.to_string());
        tags.insert("status_code", response.status_code.to_string());
        tags.insert("name", resolved_req.url.clone());
        tags.insert("group", "http");

        let sample = Sample {
            metric: "http_req_duration".into(),
            value: duration.as_micros() as f64, // microseconds (histogram records in μs)
            tags: std::sync::Arc::new(tags),
            timestamp: std::time::SystemTime::now(),
            sample_type: SampleType::Trend,
        };

        Ok((sample, http_response))
    }

    /// Get the underlying HTTP client (for direct use by the PM bridge).
    pub fn client(&self) -> &HttpClient {
        &self.client
    }
}
