use crate::types::{AuthConfig, Request};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A protocol-agnostic scenario produced by an input adapter.
/// The executor iterates items in folder-order with setNextRequest flow control.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    /// Scenario metadata.
    pub info: ScenarioInfo,
    /// Top-level items (requests or folders).
    pub items: Vec<ScenarioItem>,
    /// Global variables.
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,
    /// Auth configuration applied to all requests.
    pub auth: Option<AuthConfig>,
}

/// Scenario metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioInfo {
    pub name: String,
    pub description: Option<String>,
    pub schema: Option<String>,
}

/// An item in a scenario — either a single request or a folder of items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioItem {
    pub id: String,
    pub name: String,
    pub request: Option<Request>,
    /// Pre-request script (JS code).
    pub prerequest: Option<String>,
    /// Test script (JS code, pm.test).
    pub test: Option<String>,
    /// Assertions (Postman-style).
    #[serde(default)]
    pub assertions: Vec<String>,
    /// Child items (for folders).
    #[serde(default)]
    pub items: Vec<ScenarioItem>,
}
