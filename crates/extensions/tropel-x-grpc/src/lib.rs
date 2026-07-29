//! # tropel-x-grpc
//!
//! gRPC protocol extension for Tropel.
//! This is a reference protocol extension implementing Protocol trait.

use async_trait::async_trait;
use tropel_core::types::{Request, Sample};
use tropel_core::Result;
use tropel_core::TropelError;
use tropel_ext::traits::Protocol;

/// gRPC protocol executor (stub — requires tonic for full implementation).
pub struct GrpcProtocol;

#[async_trait]
impl Protocol for GrpcProtocol {
    fn scheme(&self) -> &str {
        "grpc"
    }

    async fn execute(&self, _req: &Request, _config: Option<&serde_json::Value>) -> Result<Sample> {
        let _start = std::time::Instant::now();

        // gRPC execution is not yet implemented
        // This would use tonic to execute gRPC calls

        Err(TropelError::Extension(
            "gRPC protocol not yet implemented — add tonic dependency and implement the executor".into()
        ))
    }
}

// Registration would use inventory:
// inventory::submit! {
//     tropel_ext::traits::ProtocolRegistration::new(|| Box::new(GrpcProtocol::default()))
// }
