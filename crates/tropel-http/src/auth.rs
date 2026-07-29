use tropel_core::Result;

/// Auth signer trait — signs/modifies a request before sending.
pub trait AuthSigner: Send + Sync {
    fn name(&self) -> &str;
    fn sign(&self, request: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder>;
}

/// Bearer token authentication.
pub struct BearerAuth {
    token: String,
}

impl BearerAuth {
    pub fn new(token: &str) -> Self {
        Self { token: token.to_string() }
    }
}

impl AuthSigner for BearerAuth {
    fn name(&self) -> &str {
        "bearer"
    }

    fn sign(&self, request: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        Ok(request.bearer_auth(&self.token))
    }
}

/// Basic authentication.
pub struct BasicAuth {
    username: String,
    password: String,
}

impl BasicAuth {
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl AuthSigner for BasicAuth {
    fn name(&self) -> &str {
        "basic"
    }

    fn sign(&self, request: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        Ok(request.basic_auth(&self.username, Some(&self.password)))
    }
}

/// API Key authentication (header or query).
pub struct ApiKeyAuth {
    key: String,
    value: String,
    location: tropel_core::types::ApiKeyLocation,
}

impl ApiKeyAuth {
    pub fn new(key: &str, value: &str, location: tropel_core::types::ApiKeyLocation) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_string(),
            location,
        }
    }
}

impl AuthSigner for ApiKeyAuth {
    fn name(&self) -> &str {
        "apikey"
    }

    fn sign(&self, request: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        match self.location {
            tropel_core::types::ApiKeyLocation::Header => {
                Ok(request.header(&self.key, &self.value))
            }
            tropel_core::types::ApiKeyLocation::Query => {
                Ok(request.query(&[(self.key.as_str(), self.value.as_str())]))
            }
        }
    }
}
