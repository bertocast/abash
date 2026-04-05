use std::sync::{atomic::AtomicBool, Arc};

use abash_core::{
    ExecutionRequest, ExecutionResult, SandboxConfig, SandboxError, SandboxExtensions,
    SessionBackend,
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
mod linux_mounts;

#[cfg(target_os = "linux")]
pub fn create_session(config: SandboxConfig) -> Result<Box<dyn SessionBackend>, SandboxError> {
    linux::create_session(config)
}

#[cfg(not(target_os = "linux"))]
pub fn create_session(_config: SandboxConfig) -> Result<Box<dyn SessionBackend>, SandboxError> {
    Ok(Box::new(UnsupportedNsjailSession))
}

#[cfg(not(target_os = "linux"))]
struct UnsupportedNsjailSession;

#[cfg(not(target_os = "linux"))]
impl SessionBackend for UnsupportedNsjailSession {
    fn name(&self) -> &'static str {
        "nsjail"
    }

    fn run(
        &mut self,
        _request: ExecutionRequest,
        _config: &SandboxConfig,
        _cancel_flag: &AtomicBool,
        _extensions: Option<Arc<dyn SandboxExtensions>>,
    ) -> Result<ExecutionResult, SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            "real-shell execution is Linux-only and the nsjail backend is not available on this platform"
                .to_string(),
        ))
    }
}
