use std::sync::atomic::AtomicBool;

use abash_core::{
    ExecutionRequest, ExecutionResult, SandboxConfig, SandboxError, SandboxExtensions,
    SessionBackend,
};

pub fn create_session(_config: SandboxConfig) -> Result<Box<dyn SessionBackend>, SandboxError> {
    Ok(Box::new(NsjailSession))
}

struct NsjailSession;

impl SessionBackend for NsjailSession {
    fn name(&self) -> &'static str {
        "nsjail"
    }

    fn run(
        &mut self,
        _request: ExecutionRequest,
        _config: &SandboxConfig,
        _cancel_flag: &AtomicBool,
        _extensions: Option<&dyn SandboxExtensions>,
    ) -> Result<ExecutionResult, SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            backend_message().to_string(),
        ))
    }
}

#[cfg(target_os = "linux")]
fn backend_message() -> &'static str {
    "real-shell execution is reserved for Linux nsjail integration and is not implemented in bootstrap"
}

#[cfg(not(target_os = "linux"))]
fn backend_message() -> &'static str {
    "real-shell execution is Linux-only and the nsjail backend is not available on this platform"
}
