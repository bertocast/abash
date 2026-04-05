use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{atomic::AtomicBool, Arc};
use std::thread;
use std::time::{Duration, Instant};

use abash_core::{
    normalize_sandbox_path, resolve_sandbox_path, ExecutionMode, ExecutionRequest, ExecutionResult,
    SandboxConfig, SandboxError, SandboxExtensions, SessionBackend, TerminationReason,
};

use crate::linux_mounts::{
    base_metadata, build_nsjail_args, discover_nsjail_bin, ensure_path_in_mounts,
    ensure_within_root, path_is_within_root, resolve_mount, resolve_mounts, resolve_writable_roots,
    top_level_command_name, validate_existing_ancestor, validate_real_shell_config, ResolvedMount,
};

pub(crate) fn create_session(
    config: SandboxConfig,
) -> Result<Box<dyn SessionBackend>, SandboxError> {
    Ok(Box::new(NsjailSession::new(config)?))
}

struct NsjailSession {
    nsjail_bin: PathBuf,
    mounts: Vec<ResolvedMount>,
    writable_roots: Vec<String>,
}

impl NsjailSession {
    fn new(config: SandboxConfig) -> Result<Self, SandboxError> {
        validate_real_shell_config(&config)?;
        Ok(Self {
            nsjail_bin: discover_nsjail_bin()?,
            mounts: resolve_mounts(&config)?,
            writable_roots: resolve_writable_roots(&config)?,
        })
    }

    fn run_linux(
        &self,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
    ) -> Result<ExecutionResult, SandboxError> {
        if request.mode != ExecutionMode::Argv {
            return Err(SandboxError::UnsupportedFeature(
                "real-shell backend currently supports argv execution only".to_string(),
            ));
        }
        if request.network_enabled {
            return Err(SandboxError::UnsupportedFeature(
                "real-shell backend does not yet support network-enabled executions".to_string(),
            ));
        }
        if request.filesystem_mode != config.filesystem_mode {
            return Err(SandboxError::InvalidRequest(
                "real-shell backend does not support per-request filesystem overrides".to_string(),
            ));
        }

        let Some(command_name) = top_level_command_name(&request.argv) else {
            return Err(SandboxError::InvalidRequest(
                "argv execution requires at least one command".to_string(),
            ));
        };
        if !config.allowlisted_commands.contains(&command_name) {
            return Err(SandboxError::PolicyDenied(format!(
                "command is not allowlisted: {command_name}"
            )));
        }

        let cwd = if request.cwd.is_empty() {
            config.default_cwd.clone()
        } else {
            resolve_sandbox_path(&config.default_cwd, &request.cwd)?
        };
        ensure_path_in_mounts(&self.mounts, &cwd)?;

        let mut command = Command::new(&self.nsjail_bin);
        command.args(build_nsjail_args(
            &self.mounts,
            &self.writable_roots,
            &cwd,
            request.timeout_ms,
            &request.argv,
        ));
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.env_clear();
        command.env("PATH", "/usr/bin:/bin");
        command.env("HOME", "/tmp");
        command.env("USER", "abash");
        for (key, value) in &request.env {
            command.env(key, value);
        }

        let mut child = command.spawn().map_err(|error| {
            SandboxError::BackendFailure(format!("nsjail could not start: {error}"))
        })?;

        if !request.stdin.is_empty() {
            if let Some(mut child_stdin) = child.stdin.take() {
                use std::io::Write;
                child_stdin.write_all(&request.stdin).map_err(|error| {
                    SandboxError::BackendFailure(format!("real-shell stdin write failed: {error}"))
                })?;
            }
        }

        let started = Instant::now();
        loop {
            if cancel_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = child.kill();
                return Err(SandboxError::Cancellation(
                    "real-shell execution was cancelled".to_string(),
                ));
            }
            if request
                .timeout_ms
                .is_some_and(|limit| started.elapsed() > Duration::from_millis(limit))
            {
                let _ = child.kill();
                return Err(SandboxError::Timeout(
                    "real-shell execution timed out".to_string(),
                ));
            }
            if child
                .try_wait()
                .map_err(|error| {
                    SandboxError::BackendFailure(format!("real-shell wait failed: {error}"))
                })?
                .is_some()
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let output = child.wait_with_output().map_err(|error| {
            SandboxError::BackendFailure(format!("real-shell output collection failed: {error}"))
        })?;

        let mut metadata = base_metadata(config, request.metadata);
        metadata.insert("command".to_string(), command_name);
        metadata.insert("cwd".to_string(), cwd);

        Ok(ExecutionResult {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code: output.status.code().unwrap_or(1),
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn resolve_existing_host_path(
        &self,
        path: &str,
    ) -> Result<(ResolvedMount, String, PathBuf, PathBuf), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        let (mount, relative) = resolve_mount(&self.mounts, &normalized)?;
        let candidate = mount.host_path.join(&relative);
        let canonical = fs::canonicalize(&candidate).map_err(|error| {
            SandboxError::InvalidRequest(format!(
                "path does not exist or cannot be resolved: {error}"
            ))
        })?;
        ensure_within_root(&mount.host_path, &canonical)?;
        Ok((mount.clone(), normalized, candidate, canonical))
    }

    fn resolve_host_target(
        &self,
        path: &str,
        allow_create: bool,
    ) -> Result<(ResolvedMount, String, PathBuf), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        let (mount, relative) = resolve_mount(&self.mounts, &normalized)?;
        let candidate = mount.host_path.join(relative);
        if !allow_create && !candidate.exists() {
            return Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )));
        }
        validate_existing_ancestor(&mount.host_path, &candidate)?;
        Ok((mount.clone(), normalized, candidate))
    }

    fn ensure_writable(&self, sandbox_path: &str) -> Result<(), SandboxError> {
        if self.writable_roots.is_empty() {
            return Err(SandboxError::PolicyDenied(format!(
                "writes are not allowed outside configured writable roots: {sandbox_path}"
            )));
        }
        if self
            .writable_roots
            .iter()
            .any(|root| path_is_within_root(sandbox_path, root))
        {
            Ok(())
        } else {
            Err(SandboxError::PolicyDenied(format!(
                "writes are not allowed outside configured writable roots: {sandbox_path}"
            )))
        }
    }
}

impl SessionBackend for NsjailSession {
    fn name(&self) -> &'static str {
        "nsjail"
    }

    fn run(
        &mut self,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        _extensions: Option<Arc<dyn SandboxExtensions>>,
    ) -> Result<ExecutionResult, SandboxError> {
        self.run_linux(request, config, cancel_flag)
    }

    fn read_file(&mut self, path: &str) -> Result<Vec<u8>, SandboxError> {
        let (_, normalized, _, canonical) = self.resolve_existing_host_path(path)?;
        if canonical.is_dir() {
            return Err(SandboxError::InvalidRequest(format!(
                "path is a directory: {normalized}"
            )));
        }
        fs::read(&canonical)
            .map_err(|error| SandboxError::BackendFailure(format!("failed to read file: {error}")))
    }

    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        let (_, normalized, candidate) = self.resolve_host_target(path, true)?;
        self.ensure_writable(&normalized)?;
        if candidate.exists() && candidate.is_dir() {
            return Err(SandboxError::InvalidRequest(format!(
                "path is a directory: {normalized}"
            )));
        }
        if let Some(parent) = candidate.parent() {
            if create_parents {
                fs::create_dir_all(parent).map_err(|error| {
                    SandboxError::BackendFailure(format!(
                        "failed to create parent directories: {error}"
                    ))
                })?;
            } else if !parent.exists() {
                return Err(SandboxError::InvalidRequest(format!(
                    "parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }
        fs::write(&candidate, contents)
            .map_err(|error| SandboxError::BackendFailure(format!("failed to write file: {error}")))
    }

    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        let (_, normalized, candidate) = self.resolve_host_target(path, true)?;
        self.ensure_writable(&normalized)?;
        if parents {
            fs::create_dir_all(&candidate).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to create directory: {error}"))
            })?;
        } else {
            fs::create_dir(&candidate).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to create directory: {error}"))
            })?;
        }
        Ok(())
    }

    fn exists(&mut self, path: &str) -> Result<bool, SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        let Ok((mount, relative)) = resolve_mount(&self.mounts, &normalized) else {
            return Ok(false);
        };
        if normalized == mount.sandbox_path {
            return Ok(true);
        }
        let candidate = mount.host_path.join(relative);
        match fs::symlink_metadata(&candidate) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    let canonical = fs::canonicalize(&candidate).map_err(|error| {
                        SandboxError::PolicyDenied(format!(
                            "host-backed path could not be resolved safely: {error}"
                        ))
                    })?;
                    ensure_within_root(&mount.host_path, &canonical)?;
                }
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(SandboxError::BackendFailure(format!(
                "failed to inspect host path: {error}"
            ))),
        }
    }
}
