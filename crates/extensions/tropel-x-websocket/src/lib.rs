//! # tropel-x-websocket
//!
//! WebSocket protocol extension for Tropel.
//! This is a reference protocol extension implementing Protocol trait.

use async_trait::async_trait;
use tropel_sdk::{Protocol, ProtocolOutcome, Request, Result, TropelError};

/// WebSocket protocol executor (stub).
pub struct WebSocketProtocol;

#[async_trait]
impl Protocol for WebSocketProtocol {
    fn scheme(&self) -> &str {
        "ws"
    }

    async fn execute(
        &self,
        _req: &Request,
        _config: Option<&serde_json::Value>,
    ) -> Result<ProtocolOutcome> {
        Err(TropelError::Extension(
            "WebSocket protocol not yet implemented".into(),
        ))
    }
}
