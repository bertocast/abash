use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use thiserror::Error;

mod network;

pub use network::{
    normalize_http_method, parse_network_policy_json, NetworkOriginPolicy, NetworkPolicy,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionMode {
    Argv,
    Script,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionProfile {
    Safe,
    Workspace,
    RealShell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilesystemMode {
    Memory,
    HostReadonly,
    HostCow,
    HostReadwrite,
}

impl FilesystemMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::HostReadonly => "host_readonly",
            Self::HostCow => "host_cow",
            Self::HostReadwrite => "host_readwrite",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    Persistent,
    PerExec,
}

impl SessionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::PerExec => "per_exec",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminationReason {
    Exited,
    Timeout,
    Cancelled,
    Denied,
    Unsupported,
    Failed,
}

impl TerminationReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exited => "exited",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Denied => "denied",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    PolicyDenied,
    Timeout,
    Cancellation,
    UnsupportedFeature,
    InternalError,
    BackendFailure,
    InvalidRequest,
    ClosedSession,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PolicyDenied => "policy_denied",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::UnsupportedFeature => "unsupported_feature",
            Self::InternalError => "internal_error",
            Self::BackendFailure => "backend_failure",
            Self::InvalidRequest => "invalid_request",
            Self::ClosedSession => "closed_session",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    pub timeout_ms: Option<u64>,
    pub max_output_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            timeout_ms: None,
            max_output_bytes: 65_536,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxConfig {
    pub profile: ExecutionProfile,
    pub filesystem_mode: FilesystemMode,
    pub session_state: SessionState,
    pub allowlisted_commands: BTreeSet<String>,
    pub default_cwd: String,
    pub workspace_root: Option<PathBuf>,
    pub writable_roots: BTreeSet<String>,
    pub network_policy: Option<NetworkPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRequest {
    pub mode: ExecutionMode,
    pub argv: Vec<String>,
    pub script: Option<String>,
    pub cwd: String,
    pub env: BTreeMap<String, String>,
    pub stdin: Vec<u8>,
    pub timeout_ms: Option<u64>,
    pub network_enabled: bool,
    pub filesystem_mode: FilesystemMode,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SanitizedError {
    pub kind: ErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
    pub termination_reason: TerminationReason,
    pub error: Option<SanitizedError>,
    pub metadata: BTreeMap<String, String>,
}

impl ExecutionResult {
    pub fn success(stdout: impl Into<Vec<u8>>, metadata: BTreeMap<String, String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: Vec::new(),
            exit_code: 0,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        }
    }

    pub fn failure(error: SandboxError, metadata: BTreeMap<String, String>) -> Self {
        let sanitized = error.sanitized();
        let termination_reason = error.termination_reason();
        Self {
            stdout: Vec::new(),
            stderr: sanitized.message.clone().into_bytes(),
            exit_code: 1,
            termination_reason,
            error: Some(sanitized),
            metadata,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SandboxError {
    #[error("policy denied: {0}")]
    PolicyDenied(String),
    #[error("execution timed out: {0}")]
    Timeout(String),
    #[error("execution cancelled: {0}")]
    Cancellation(String),
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("backend failure: {0}")]
    BackendFailure(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("sandbox is closed")]
    ClosedSession,
}

impl SandboxError {
    pub fn sanitized(&self) -> SanitizedError {
        SanitizedError {
            kind: self.kind(),
            message: match self {
                Self::PolicyDenied(message)
                | Self::Timeout(message)
                | Self::Cancellation(message)
                | Self::UnsupportedFeature(message)
                | Self::InternalError(message)
                | Self::BackendFailure(message)
                | Self::InvalidRequest(message) => message.clone(),
                Self::ClosedSession => "sandbox session is closed".to_string(),
            },
        }
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::PolicyDenied(_) => ErrorKind::PolicyDenied,
            Self::Timeout(_) => ErrorKind::Timeout,
            Self::Cancellation(_) => ErrorKind::Cancellation,
            Self::UnsupportedFeature(_) => ErrorKind::UnsupportedFeature,
            Self::InternalError(_) => ErrorKind::InternalError,
            Self::BackendFailure(_) => ErrorKind::BackendFailure,
            Self::InvalidRequest(_) => ErrorKind::InvalidRequest,
            Self::ClosedSession => ErrorKind::ClosedSession,
        }
    }

    pub fn termination_reason(&self) -> TerminationReason {
        match self {
            Self::PolicyDenied(_) => TerminationReason::Denied,
            Self::Timeout(_) => TerminationReason::Timeout,
            Self::Cancellation(_) => TerminationReason::Cancelled,
            Self::UnsupportedFeature(_) => TerminationReason::Unsupported,
            Self::InternalError(_)
            | Self::BackendFailure(_)
            | Self::InvalidRequest(_)
            | Self::ClosedSession => TerminationReason::Failed,
        }
    }
}

pub trait SessionBackend: Send {
    fn name(&self) -> &'static str;

    fn run(
        &mut self,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
    ) -> Result<ExecutionResult, SandboxError>;

    fn read_file(&mut self, _path: &str) -> Result<Vec<u8>, SandboxError> {
        Err(SandboxError::UnsupportedFeature(format!(
            "file reads are not supported by backend {}",
            self.name()
        )))
    }

    fn write_file(
        &mut self,
        _path: &str,
        _contents: Vec<u8>,
        _create_parents: bool,
    ) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(format!(
            "file writes are not supported by backend {}",
            self.name()
        )))
    }

    fn mkdir(&mut self, _path: &str, _parents: bool) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(format!(
            "directory creation is not supported by backend {}",
            self.name()
        )))
    }

    fn exists(&mut self, _path: &str) -> Result<bool, SandboxError> {
        Err(SandboxError::UnsupportedFeature(format!(
            "existence checks are not supported by backend {}",
            self.name()
        )))
    }

    fn close(&mut self) -> Result<(), SandboxError> {
        Ok(())
    }
}

pub trait SandboxExtensions: Send + Sync {
    fn exec_custom_command(
        &self,
        _request: &ExecutionRequest,
    ) -> Result<Option<ExecutionResult>, SandboxError> {
        Ok(None)
    }
}

pub struct SandboxSession {
    config: SandboxConfig,
    backend: Box<dyn SessionBackend>,
    extensions: Option<Arc<dyn SandboxExtensions>>,
    cancel_flag: Arc<AtomicBool>,
    closed: bool,
}

impl SandboxSession {
    pub fn new(
        config: SandboxConfig,
        backend: Box<dyn SessionBackend>,
        extensions: Option<Arc<dyn SandboxExtensions>>,
        cancel_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            backend,
            extensions,
            cancel_flag,
            closed: false,
        }
    }

    pub fn run(&mut self, request: ExecutionRequest) -> ExecutionResult {
        if self.closed {
            return ExecutionResult::failure(SandboxError::ClosedSession, self.base_metadata());
        }

        match self.backend.run(
            request,
            &self.config,
            self.cancel_flag.as_ref(),
            self.extensions.as_deref(),
        ) {
            Ok(result) => result,
            Err(error) => ExecutionResult::failure(error, self.base_metadata()),
        }
    }

    pub fn read_file(&mut self, path: &str) -> Result<Vec<u8>, SandboxError> {
        self.ensure_open()?;
        let resolved = resolve_sandbox_path(&self.config.default_cwd, path)?;
        self.backend.read_file(&resolved)
    }

    pub fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        self.ensure_open()?;
        let resolved = resolve_sandbox_path(&self.config.default_cwd, path)?;
        self.backend.write_file(&resolved, contents, create_parents)
    }

    pub fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        self.ensure_open()?;
        let resolved = resolve_sandbox_path(&self.config.default_cwd, path)?;
        self.backend.mkdir(&resolved, parents)
    }

    pub fn exists(&mut self, path: &str) -> Result<bool, SandboxError> {
        self.ensure_open()?;
        let resolved = resolve_sandbox_path(&self.config.default_cwd, path)?;
        self.backend.exists(&resolved)
    }

    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) -> Result<(), SandboxError> {
        if self.closed {
            return Ok(());
        }
        self.backend.close()?;
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), SandboxError> {
        if self.closed {
            Err(SandboxError::ClosedSession)
        } else {
            Ok(())
        }
    }

    pub fn base_metadata(&self) -> BTreeMap<String, String> {
        let mut metadata = BTreeMap::new();
        metadata.insert("backend".to_string(), self.backend.name().to_string());
        metadata.insert(
            "profile".to_string(),
            match self.config.profile {
                ExecutionProfile::Safe => "safe",
                ExecutionProfile::Workspace => "workspace",
                ExecutionProfile::RealShell => "real_shell",
            }
            .to_string(),
        );
        metadata.insert(
            "filesystem_mode".to_string(),
            self.config.filesystem_mode.as_str().to_string(),
        );
        metadata.insert(
            "session_state".to_string(),
            self.config.session_state.as_str().to_string(),
        );
        if self.config.workspace_root.is_some() {
            metadata.insert("workspace_mount".to_string(), "/workspace".to_string());
        }
        metadata
    }
}

pub trait SandboxFilesystem: Send {
    fn mode(&self) -> FilesystemMode;
    fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError>;
    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError>;
    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError>;
    fn delete_path(&mut self, path: &str, recursive: bool) -> Result<(), SandboxError>;
    fn exists(&self, path: &str) -> Result<bool, SandboxError>;
    fn is_dir(&self, path: &str) -> Result<bool, SandboxError>;
    fn get_mode_bits(&self, path: &str) -> Result<u32, SandboxError>;
    fn chmod(&mut self, path: &str, mode: u32) -> Result<(), SandboxError>;
    fn list_paths(&self) -> Result<Vec<String>, SandboxError>;
    fn read_link(&self, path: &str) -> Result<Option<String>, SandboxError>;
    fn create_symlink(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError>;
    fn create_hard_link(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError>;
}

pub fn default_cwd_for_mode(mode: &FilesystemMode) -> &'static str {
    match mode {
        FilesystemMode::Memory => "/",
        FilesystemMode::HostReadonly | FilesystemMode::HostCow | FilesystemMode::HostReadwrite => {
            "/workspace"
        }
    }
}

pub fn create_filesystem(
    config: &SandboxConfig,
) -> Result<Box<dyn SandboxFilesystem>, SandboxError> {
    match config.filesystem_mode {
        FilesystemMode::Memory => {
            if config.workspace_root.is_some() {
                return Err(SandboxError::InvalidRequest(
                    "memory filesystem mode must not configure a host workspace root".to_string(),
                ));
            }
            if !config.writable_roots.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "memory filesystem mode must not configure writable host roots".to_string(),
                ));
            }
            Ok(Box::new(MemoryFilesystem::new()))
        }
        FilesystemMode::HostReadonly => {
            validate_workspace_profile(config)?;
            if !config.writable_roots.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "host_readonly mode must not configure writable roots".to_string(),
                ));
            }
            Ok(Box::new(HostReadonlyFilesystem::new(
                resolve_workspace_root(config)?,
            )?))
        }
        FilesystemMode::HostCow => {
            validate_workspace_profile(config)?;
            if !config.writable_roots.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "host_cow mode must not configure writable roots".to_string(),
                ));
            }
            Ok(Box::new(HostCowFilesystem::new(resolve_workspace_root(
                config,
            )?)?))
        }
        FilesystemMode::HostReadwrite => {
            validate_workspace_profile(config)?;
            Ok(Box::new(HostReadwriteFilesystem::new(
                resolve_workspace_root(config)?,
                normalize_writable_roots(&config.writable_roots)?,
            )?))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct VirtualFilesystem {
    files: HashMap<String, Vec<u8>>,
    directories: HashSet<String>,
    modes: HashMap<String, u32>,
}

impl VirtualFilesystem {
    pub fn new() -> Self {
        let mut directories = HashSet::new();
        directories.insert("/".to_string());
        directories.insert("/workspace".to_string());
        let mut modes = HashMap::new();
        modes.insert("/".to_string(), 0o755);
        modes.insert("/workspace".to_string(), 0o755);
        Self {
            files: HashMap::new(),
            directories,
            modes,
        }
    }

    pub fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        if normalized == "/" || normalized == "/workspace" {
            return Ok(());
        }

        let parent = parent_dir(&normalized).ok_or_else(|| {
            SandboxError::InvalidRequest("cannot create a directory without a parent".to_string())
        })?;
        if parents {
            self.ensure_dir_chain(&normalized)?;
        } else if !self.directories.contains(&parent) {
            return Err(SandboxError::InvalidRequest(format!(
                "parent directory does not exist: {parent}"
            )));
        } else {
            self.directories.insert(normalized.clone());
            self.modes.entry(normalized).or_insert(0o755);
        }
        Ok(())
    }

    pub fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        if normalized == "/" || normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot write file contents to a directory path".to_string(),
            ));
        }
        if self.directories.contains(&normalized) {
            return Err(SandboxError::InvalidRequest(format!(
                "path is a directory: {normalized}"
            )));
        }

        let parent = parent_dir(&normalized).ok_or_else(|| {
            SandboxError::InvalidRequest("cannot write a file without a parent".to_string())
        })?;
        if create_parents {
            self.ensure_dir_chain(&parent)?;
        } else if !self.directories.contains(&parent) {
            return Err(SandboxError::InvalidRequest(format!(
                "parent directory does not exist: {parent}"
            )));
        }
        self.files.insert(normalized.clone(), contents);
        self.modes.insert(normalized, 0o644);
        Ok(())
    }

    pub fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        self.files.get(&normalized).cloned().ok_or_else(|| {
            SandboxError::InvalidRequest(format!("file does not exist: {normalized}"))
        })
    }

    pub fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        Ok(self.files.contains_key(&normalized) || self.directories.contains(&normalized))
    }

    pub fn create_symlink(&self, _source: &str, _target: &str) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            "symlinks are not supported in the bootstrap virtual filesystem".to_string(),
        ))
    }

    pub fn create_hard_link(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError> {
        let normalized_target = normalize_sandbox_path(target)?;
        let normalized_link = normalize_sandbox_path(link_path)?;
        if normalized_link == "/" || normalized_link == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot create a hard link at the sandbox root".to_string(),
            ));
        }
        if self.directories.contains(&normalized_target) {
            return Err(SandboxError::InvalidRequest(format!(
                "hard links are not allowed for directories: {normalized_target}"
            )));
        }
        let contents = self.files.get(&normalized_target).cloned().ok_or_else(|| {
            SandboxError::InvalidRequest(format!("path does not exist: {normalized_target}"))
        })?;
        if self.files.contains_key(&normalized_link) || self.directories.contains(&normalized_link)
        {
            return Err(SandboxError::InvalidRequest(format!(
                "path already exists: {normalized_link}"
            )));
        }
        let parent = Path::new(&normalized_link).parent().ok_or_else(|| {
            SandboxError::InvalidRequest("hard link destination must have a parent".to_string())
        })?;
        let parent = parent.to_string_lossy().to_string();
        if !self.directories.contains(&parent) {
            return Err(SandboxError::InvalidRequest(format!(
                "parent directory does not exist: {parent}"
            )));
        }

        self.files.insert(normalized_link.clone(), contents);
        let mode = self.mode_bits(&normalized_target)?;
        self.modes.insert(normalized_link, mode);
        Ok(())
    }

    pub fn delete_path(&mut self, path: &str, recursive: bool) -> Result<(), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        if normalized == "/" || normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot delete the sandbox root".to_string(),
            ));
        }

        if self.files.remove(&normalized).is_some() {
            return Ok(());
        }

        if !self.directories.contains(&normalized) {
            return Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )));
        }

        if !recursive {
            return Err(SandboxError::InvalidRequest(format!(
                "cannot remove directory without -r: {normalized}"
            )));
        }

        let prefix = format!("{normalized}/");
        self.files.retain(|path, _| !path.starts_with(&prefix));
        self.directories
            .retain(|path| path != &normalized && !path.starts_with(&prefix));
        self.modes
            .retain(|path, _| path != &normalized && !path.starts_with(&prefix));
        Ok(())
    }

    pub fn mode_bits(&self, path: &str) -> Result<u32, SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        if self.files.contains_key(&normalized) || self.directories.contains(&normalized) {
            Ok(*self
                .modes
                .get(&normalized)
                .unwrap_or(if self.directories.contains(&normalized) {
                    &0o755
                } else {
                    &0o644
                }))
        } else {
            Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )))
        }
    }

    pub fn chmod(&mut self, path: &str, mode: u32) -> Result<(), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        if self.files.contains_key(&normalized) || self.directories.contains(&normalized) {
            self.modes.insert(normalized, mode & 0o7777);
            Ok(())
        } else {
            Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )))
        }
    }

    fn ensure_dir_chain(&mut self, path: &str) -> Result<(), SandboxError> {
        let normalized = normalize_sandbox_path(path)?;
        let mut current = String::new();
        for segment in normalized.split('/').filter(|segment| !segment.is_empty()) {
            current.push('/');
            current.push_str(segment);
            self.directories.insert(current.clone());
            self.modes.entry(current.clone()).or_insert(0o755);
        }
        if normalized == "/" {
            self.directories.insert("/".to_string());
            self.modes.entry("/".to_string()).or_insert(0o755);
        }
        Ok(())
    }
}

struct MemoryFilesystem {
    virtual_fs: VirtualFilesystem,
}

impl MemoryFilesystem {
    fn new() -> Self {
        Self {
            virtual_fs: VirtualFilesystem::new(),
        }
    }
}

impl SandboxFilesystem for MemoryFilesystem {
    fn mode(&self) -> FilesystemMode {
        FilesystemMode::Memory
    }

    fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        self.virtual_fs.read_file(path)
    }

    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        self.virtual_fs.write_file(path, contents, create_parents)
    }

    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        self.virtual_fs.mkdir(path, parents)
    }

    fn delete_path(&mut self, path: &str, recursive: bool) -> Result<(), SandboxError> {
        self.virtual_fs.delete_path(path, recursive)
    }

    fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        self.virtual_fs.exists(path)
    }

    fn is_dir(&self, path: &str) -> Result<bool, SandboxError> {
        Ok(self
            .virtual_fs
            .directories
            .contains(&normalize_sandbox_path(path)?))
    }

    fn get_mode_bits(&self, path: &str) -> Result<u32, SandboxError> {
        self.virtual_fs.mode_bits(path)
    }

    fn chmod(&mut self, path: &str, mode: u32) -> Result<(), SandboxError> {
        self.virtual_fs.chmod(path, mode)
    }

    fn list_paths(&self) -> Result<Vec<String>, SandboxError> {
        let mut paths = self
            .virtual_fs
            .directories
            .iter()
            .cloned()
            .chain(self.virtual_fs.files.keys().cloned())
            .collect::<Vec<_>>();
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn read_link(&self, _path: &str) -> Result<Option<String>, SandboxError> {
        Ok(None)
    }

    fn create_symlink(&mut self, _target: &str, _link_path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            "symlinks are not supported in memory mode".to_string(),
        ))
    }

    fn create_hard_link(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError> {
        self.virtual_fs.create_hard_link(target, link_path)
    }
}

struct HostReadonlyFilesystem {
    root: PathBuf,
}

impl HostReadonlyFilesystem {
    fn new(root: PathBuf) -> Result<Self, SandboxError> {
        Ok(Self { root })
    }
}

impl SandboxFilesystem for HostReadonlyFilesystem {
    fn mode(&self) -> FilesystemMode {
        FilesystemMode::HostReadonly
    }

    fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        host_read_file(&self.root, path)
    }

    fn write_file(
        &mut self,
        _path: &str,
        _contents: Vec<u8>,
        _create_parents: bool,
    ) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow file writes".to_string(),
        ))
    }

    fn mkdir(&mut self, _path: &str, _parents: bool) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow directory creation".to_string(),
        ))
    }

    fn delete_path(&mut self, _path: &str, _recursive: bool) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow deletions".to_string(),
        ))
    }

    fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        host_exists(&self.root, path)
    }

    fn is_dir(&self, path: &str) -> Result<bool, SandboxError> {
        host_is_dir(&self.root, path)
    }

    fn get_mode_bits(&self, path: &str) -> Result<u32, SandboxError> {
        host_get_mode(&self.root, path)
    }

    fn chmod(&mut self, _path: &str, _mode: u32) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow chmod".to_string(),
        ))
    }

    fn list_paths(&self) -> Result<Vec<String>, SandboxError> {
        list_host_paths(&self.root)
    }

    fn read_link(&self, path: &str) -> Result<Option<String>, SandboxError> {
        host_read_link(&self.root, path)
    }

    fn create_symlink(&mut self, _target: &str, _link_path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow symlink creation".to_string(),
        ))
    }

    fn create_hard_link(&mut self, _target: &str, _link_path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::PolicyDenied(
            "host_readonly mode does not allow hard-link creation".to_string(),
        ))
    }
}

struct HostCowFilesystem {
    root: PathBuf,
    overlay_files: HashMap<String, Vec<u8>>,
    overlay_dirs: HashSet<String>,
    overlay_modes: HashMap<String, u32>,
}

impl HostCowFilesystem {
    fn new(root: PathBuf) -> Result<Self, SandboxError> {
        let mut overlay_dirs = HashSet::new();
        overlay_dirs.insert("/".to_string());
        overlay_dirs.insert("/workspace".to_string());
        let mut overlay_modes = HashMap::new();
        overlay_modes.insert("/".to_string(), 0o755);
        overlay_modes.insert("/workspace".to_string(), 0o755);
        Ok(Self {
            root,
            overlay_files: HashMap::new(),
            overlay_dirs,
            overlay_modes,
        })
    }

    fn ensure_overlay_dirs(&mut self, path: &str) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        let mut current = String::new();
        for segment in normalized.split('/').filter(|segment| !segment.is_empty()) {
            current.push('/');
            current.push_str(segment);
            self.overlay_dirs.insert(current.clone());
            self.overlay_modes.entry(current.clone()).or_insert(0o755);
        }
        Ok(())
    }
}

impl SandboxFilesystem for HostCowFilesystem {
    fn mode(&self) -> FilesystemMode {
        FilesystemMode::HostCow
    }

    fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if let Some(contents) = self.overlay_files.get(&normalized) {
            return Ok(contents.clone());
        }
        host_read_file(&self.root, &normalized)
    }

    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot write file contents to the workspace root".to_string(),
            ));
        }
        let parent = parent_dir(&normalized).ok_or_else(|| {
            SandboxError::InvalidRequest("cannot write a file without a parent".to_string())
        })?;
        if create_parents {
            self.ensure_overlay_dirs(&parent)?;
        } else if !self.overlay_dirs.contains(&parent) && !host_exists(&self.root, &parent)? {
            return Err(SandboxError::InvalidRequest(format!(
                "parent directory does not exist: {parent}"
            )));
        }
        self.overlay_files.insert(normalized.clone(), contents);
        self.overlay_modes.insert(normalized, 0o644);
        Ok(())
    }

    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if normalized == "/workspace" {
            return Ok(());
        }
        let parent = parent_dir(&normalized).ok_or_else(|| {
            SandboxError::InvalidRequest("cannot create a directory without a parent".to_string())
        })?;
        if parents {
            self.ensure_overlay_dirs(&normalized)?;
        } else if !self.overlay_dirs.contains(&parent) && !host_exists(&self.root, &parent)? {
            return Err(SandboxError::InvalidRequest(format!(
                "parent directory does not exist: {parent}"
            )));
        } else {
            self.overlay_dirs.insert(normalized.clone());
            self.overlay_modes.entry(normalized).or_insert(0o755);
        }
        Ok(())
    }

    fn delete_path(&mut self, path: &str, recursive: bool) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot delete the workspace root".to_string(),
            ));
        }

        if self.overlay_files.remove(&normalized).is_some() {
            return Ok(());
        }

        if self.overlay_dirs.contains(&normalized) && !host_exists(&self.root, &normalized)? {
            if !recursive {
                return Err(SandboxError::InvalidRequest(format!(
                    "cannot remove directory without -r: {normalized}"
                )));
            }
            let prefix = format!("{normalized}/");
            self.overlay_files
                .retain(|path, _| !path.starts_with(&prefix));
            self.overlay_dirs
                .retain(|path| path != &normalized && !path.starts_with(&prefix));
            self.overlay_modes
                .retain(|path, _| path != &normalized && !path.starts_with(&prefix));
            return Ok(());
        }

        if host_exists(&self.root, &normalized)? {
            return Err(SandboxError::UnsupportedFeature(
                "host_cow mode does not support deleting host-backed paths".to_string(),
            ));
        }

        Err(SandboxError::InvalidRequest(format!(
            "path does not exist: {normalized}"
        )))
    }

    fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if self.overlay_files.contains_key(&normalized) || self.overlay_dirs.contains(&normalized) {
            return Ok(true);
        }
        host_exists(&self.root, &normalized)
    }

    fn is_dir(&self, path: &str) -> Result<bool, SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if self.overlay_dirs.contains(&normalized) {
            return Ok(true);
        }
        if self.overlay_files.contains_key(&normalized) {
            return Ok(false);
        }
        host_is_dir(&self.root, &normalized)
    }

    fn get_mode_bits(&self, path: &str) -> Result<u32, SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if let Some(mode) = self.overlay_modes.get(&normalized) {
            return Ok(*mode);
        }
        if self.overlay_dirs.contains(&normalized) {
            return Ok(0o755);
        }
        if self.overlay_files.contains_key(&normalized) {
            return Ok(0o644);
        }
        host_get_mode(&self.root, &normalized)
    }

    fn chmod(&mut self, path: &str, mode: u32) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if self.overlay_files.contains_key(&normalized)
            || self.overlay_dirs.contains(&normalized)
            || host_exists(&self.root, &normalized)?
        {
            self.overlay_modes.insert(normalized, mode & 0o7777);
            Ok(())
        } else {
            Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )))
        }
    }

    fn list_paths(&self) -> Result<Vec<String>, SandboxError> {
        let mut paths = list_host_paths(&self.root)?;
        paths.extend(self.overlay_dirs.iter().cloned());
        paths.extend(self.overlay_files.keys().cloned());
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn read_link(&self, path: &str) -> Result<Option<String>, SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if self.overlay_files.contains_key(&normalized) || self.overlay_dirs.contains(&normalized) {
            return Ok(None);
        }
        host_read_link(&self.root, &normalized)
    }

    fn create_symlink(&mut self, _target: &str, _link_path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            "host_cow mode does not support creating symlinks".to_string(),
        ))
    }

    fn create_hard_link(&mut self, _target: &str, _link_path: &str) -> Result<(), SandboxError> {
        Err(SandboxError::UnsupportedFeature(
            "host_cow mode does not support hard-link creation".to_string(),
        ))
    }
}

struct HostReadwriteFilesystem {
    root: PathBuf,
    writable_roots: Vec<String>,
}

impl HostReadwriteFilesystem {
    fn new(root: PathBuf, writable_roots: Vec<String>) -> Result<Self, SandboxError> {
        if writable_roots.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "host_readwrite mode requires at least one writable root".to_string(),
            ));
        }
        Ok(Self {
            root,
            writable_roots,
        })
    }

    fn ensure_writable(&self, sandbox_path: &str) -> Result<(), SandboxError> {
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

impl SandboxFilesystem for HostReadwriteFilesystem {
    fn mode(&self) -> FilesystemMode {
        FilesystemMode::HostReadwrite
    }

    fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        host_read_file(&self.root, path)
    }

    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        self.ensure_writable(&normalized)?;
        let candidate = host_target_for_write(&self.root, &normalized, create_parents)?;
        if let Some(parent) = candidate.parent() {
            if create_parents {
                fs::create_dir_all(parent).map_err(|error| {
                    SandboxError::BackendFailure(format!("failed to create directories: {error}"))
                })?;
            }
        }
        fs::write(&candidate, contents)
            .map_err(|error| SandboxError::BackendFailure(format!("failed to write file: {error}")))
    }

    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        self.ensure_writable(&normalized)?;
        let candidate = host_target_for_directory(&self.root, &normalized, parents)?;
        let result = if parents {
            fs::create_dir_all(&candidate)
        } else {
            fs::create_dir(&candidate)
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(SandboxError::BackendFailure(format!(
                "failed to create directory: {error}"
            ))),
        }
    }

    fn delete_path(&mut self, path: &str, recursive: bool) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot delete the workspace root".to_string(),
            ));
        }
        self.ensure_writable(&normalized)?;

        let candidate = self.root.join(sandbox_to_workspace_relative(&normalized)?);
        validate_existing_ancestor(&self.root, &candidate, false)?;
        let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SandboxError::InvalidRequest(format!("path does not exist: {normalized}"))
            } else {
                SandboxError::BackendFailure(format!("failed to inspect path: {error}"))
            }
        })?;

        if metadata.is_dir() {
            if !recursive {
                return Err(SandboxError::InvalidRequest(format!(
                    "cannot remove directory without -r: {normalized}"
                )));
            }
            fs::remove_dir_all(&candidate).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to remove directory: {error}"))
            })
        } else {
            fs::remove_file(&candidate).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to remove file: {error}"))
            })
        }
    }

    fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        host_exists(&self.root, path)
    }

    fn is_dir(&self, path: &str) -> Result<bool, SandboxError> {
        host_is_dir(&self.root, path)
    }

    fn get_mode_bits(&self, path: &str) -> Result<u32, SandboxError> {
        host_get_mode(&self.root, path)
    }

    fn chmod(&mut self, path: &str, mode: u32) -> Result<(), SandboxError> {
        let normalized = normalize_workspace_path(path)?;
        if normalized == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot chmod the workspace root".to_string(),
            ));
        }
        self.ensure_writable(&normalized)?;
        host_set_mode(&self.root, &normalized, mode)
    }

    fn list_paths(&self) -> Result<Vec<String>, SandboxError> {
        list_host_paths(&self.root)
    }

    fn read_link(&self, path: &str) -> Result<Option<String>, SandboxError> {
        host_read_link(&self.root, path)
    }

    fn create_symlink(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError> {
        let normalized_link = normalize_workspace_path(link_path)?;
        if normalized_link == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot create a symlink at the workspace root".to_string(),
            ));
        }
        self.ensure_writable(&normalized_link)?;

        let link_candidate = host_target_for_write(&self.root, &normalized_link, false)?;
        if fs::symlink_metadata(&link_candidate).is_ok() {
            return Err(SandboxError::InvalidRequest(format!(
                "path already exists: {normalized_link}"
            )));
        }

        let normalized_target = normalize_workspace_path(target)?;
        let target_candidate = self
            .root
            .join(sandbox_to_workspace_relative(&normalized_target)?);
        let link_parent = link_candidate.parent().ok_or_else(|| {
            SandboxError::InvalidRequest("symlink destination must have a parent".to_string())
        })?;
        let relative_target = relative_host_path(link_parent, &target_candidate)?;

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&relative_target, &link_candidate).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to create symlink: {error}"))
            })?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = relative_target;
            Err(SandboxError::UnsupportedFeature(
                "symlink creation is supported only on unix hosts".to_string(),
            ))
        }
    }

    fn create_hard_link(&mut self, target: &str, link_path: &str) -> Result<(), SandboxError> {
        let normalized_link = normalize_workspace_path(link_path)?;
        if normalized_link == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "cannot create a hard link at the workspace root".to_string(),
            ));
        }
        self.ensure_writable(&normalized_link)?;

        let link_candidate = host_target_for_write(&self.root, &normalized_link, false)?;
        if fs::symlink_metadata(&link_candidate).is_ok() {
            return Err(SandboxError::InvalidRequest(format!(
                "path already exists: {normalized_link}"
            )));
        }

        let normalized_target = normalize_workspace_path(target)?;
        if normalized_target == "/workspace" {
            return Err(SandboxError::InvalidRequest(
                "hard links are not allowed for directories: /workspace".to_string(),
            ));
        }
        let raw_target = self
            .root
            .join(sandbox_to_workspace_relative(&normalized_target)?);
        validate_existing_ancestor(&self.root, &raw_target, false)?;
        let metadata = fs::symlink_metadata(&raw_target).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SandboxError::InvalidRequest(format!("path does not exist: {normalized_target}"))
            } else {
                SandboxError::BackendFailure(format!("failed to inspect path: {error}"))
            }
        })?;

        if metadata.is_dir() {
            return Err(SandboxError::InvalidRequest(format!(
                "hard links are not allowed for directories: {normalized_target}"
            )));
        }

        let target_candidate = if metadata.file_type().is_symlink() {
            let canonical = fs::canonicalize(&raw_target).map_err(|error| {
                SandboxError::PolicyDenied(format!(
                    "host-backed path could not be resolved safely: {error}"
                ))
            })?;
            ensure_within_root(&self.root, &canonical)?;
            canonical
        } else {
            raw_target
        };

        fs::hard_link(&target_candidate, &link_candidate).map_err(|error| {
            SandboxError::BackendFailure(format!("failed to create hard link: {error}"))
        })
    }
}

pub fn normalize_sandbox_path(path: &str) -> Result<String, SandboxError> {
    if path.contains('\0') {
        return Err(SandboxError::InvalidRequest(
            "sandbox paths must not contain NUL bytes".to_string(),
        ));
    }

    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(value) => parts.push(value.to_string_lossy().into_owned()),
            Component::ParentDir => {
                return Err(SandboxError::PolicyDenied(
                    "path traversal outside the sandbox root is blocked".to_string(),
                ))
            }
            Component::Prefix(_) => {
                return Err(SandboxError::PolicyDenied(
                    "host path prefixes are not allowed in sandbox paths".to_string(),
                ))
            }
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

pub fn resolve_sandbox_path(cwd: &str, path: &str) -> Result<String, SandboxError> {
    if path.is_empty() {
        return normalize_sandbox_path(cwd);
    }
    if path.starts_with('/') {
        return normalize_sandbox_path(path);
    }

    let base = normalize_sandbox_path(cwd)?;
    let joined = if base == "/" {
        format!("/{path}")
    } else {
        format!("{base}/{path}")
    };
    normalize_sandbox_path(&joined)
}

pub fn normalize_workspace_path(path: &str) -> Result<String, SandboxError> {
    let normalized = normalize_sandbox_path(path)?;
    if path_is_within_root(&normalized, "/workspace") {
        Ok(normalized)
    } else {
        Err(SandboxError::PolicyDenied(
            "host-backed filesystem access is restricted to /workspace".to_string(),
        ))
    }
}

fn validate_workspace_profile(config: &SandboxConfig) -> Result<(), SandboxError> {
    if config.profile != ExecutionProfile::Workspace {
        return Err(SandboxError::InvalidRequest(
            "host-backed filesystem modes require the workspace execution profile".to_string(),
        ));
    }
    Ok(())
}

fn resolve_workspace_root(config: &SandboxConfig) -> Result<PathBuf, SandboxError> {
    let root = config.workspace_root.as_ref().ok_or_else(|| {
        SandboxError::InvalidRequest(
            "host-backed filesystem modes require a workspace_root".to_string(),
        )
    })?;
    let canonical = fs::canonicalize(root).map_err(|error| {
        SandboxError::InvalidRequest(format!(
            "workspace_root must exist and be accessible: {error}"
        ))
    })?;
    if !canonical.is_dir() {
        return Err(SandboxError::InvalidRequest(
            "workspace_root must be a directory".to_string(),
        ));
    }
    Ok(canonical)
}

fn normalize_writable_roots(
    writable_roots: &BTreeSet<String>,
) -> Result<Vec<String>, SandboxError> {
    let mut normalized = writable_roots
        .iter()
        .map(|path| normalize_workspace_path(path))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn path_is_within_root(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn sandbox_to_workspace_relative(path: &str) -> Result<PathBuf, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        Ok(PathBuf::new())
    } else {
        Ok(PathBuf::from(normalized.trim_start_matches("/workspace/")))
    }
}

fn parent_dir(path: &str) -> Option<String> {
    let mut segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.pop().is_none() {
        return None;
    }
    if segments.is_empty() {
        Some("/".to_string())
    } else {
        Some(format!("/{}", segments.join("/")))
    }
}

fn host_exists(root: &Path, path: &str) -> Result<bool, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Ok(true);
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    match fs::symlink_metadata(&candidate) {
        Ok(_) => {
            let canonical = fs::canonicalize(&candidate).map_err(|error| {
                SandboxError::PolicyDenied(format!(
                    "host-backed path could not be resolved safely: {error}"
                ))
            })?;
            ensure_within_root(root, &canonical)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(SandboxError::BackendFailure(format!(
            "failed to inspect host path: {error}"
        ))),
    }
}

fn host_read_file(root: &Path, path: &str) -> Result<Vec<u8>, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Err(SandboxError::InvalidRequest(
            "cannot read the workspace root as a file".to_string(),
        ));
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    let canonical = fs::canonicalize(&candidate).map_err(|error| {
        SandboxError::InvalidRequest(format!(
            "file does not exist or cannot be resolved: {error}"
        ))
    })?;
    ensure_within_root(root, &canonical)?;
    if canonical.is_dir() {
        return Err(SandboxError::InvalidRequest(format!(
            "path is a directory: {normalized}"
        )));
    }
    fs::read(&canonical)
        .map_err(|error| SandboxError::BackendFailure(format!("failed to read file: {error}")))
}

fn host_is_dir(root: &Path, path: &str) -> Result<bool, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Ok(true);
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    match fs::symlink_metadata(&candidate) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                let canonical = fs::canonicalize(&candidate).map_err(|error| {
                    SandboxError::PolicyDenied(format!(
                        "host-backed path could not be resolved safely: {error}"
                    ))
                })?;
                ensure_within_root(root, &canonical)?;
                Ok(canonical.is_dir())
            } else {
                Ok(metadata.is_dir())
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            SandboxError::InvalidRequest(format!("path does not exist: {normalized}")),
        ),
        Err(error) => Err(SandboxError::BackendFailure(format!(
            "failed to inspect host path: {error}"
        ))),
    }
}

fn host_read_link(root: &Path, path: &str) -> Result<Option<String>, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Ok(None);
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    let metadata = match fs::symlink_metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SandboxError::InvalidRequest(format!(
                "path does not exist: {normalized}"
            )))
        }
        Err(error) => {
            return Err(SandboxError::BackendFailure(format!(
                "failed to inspect path: {error}"
            )))
        }
    };
    if !metadata.file_type().is_symlink() {
        return Ok(None);
    }

    let raw_target = fs::read_link(&candidate).map_err(|error| {
        SandboxError::BackendFailure(format!("failed to read symlink: {error}"))
    })?;
    let resolved = normalize_link_target(root, candidate.parent().unwrap_or(root), &raw_target)?;
    Ok(Some(resolved))
}

fn host_get_mode(root: &Path, path: &str) -> Result<u32, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Ok(0o755);
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            SandboxError::InvalidRequest(format!("path does not exist: {normalized}"))
        } else {
            SandboxError::BackendFailure(format!("failed to inspect path: {error}"))
        }
    })?;
    if metadata.file_type().is_symlink() {
        let canonical = fs::canonicalize(&candidate).map_err(|error| {
            SandboxError::PolicyDenied(format!(
                "host-backed path could not be resolved safely: {error}"
            ))
        })?;
        ensure_within_root(root, &canonical)?;
    }

    #[cfg(unix)]
    {
        Ok(metadata.permissions().mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err(SandboxError::UnsupportedFeature(
            "chmod is supported only on unix hosts".to_string(),
        ))
    }
}

fn host_set_mode(root: &Path, path: &str, mode: u32) -> Result<(), SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    validate_existing_ancestor(root, &candidate, false)?;
    let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            SandboxError::InvalidRequest(format!("path does not exist: {normalized}"))
        } else {
            SandboxError::BackendFailure(format!("failed to inspect path: {error}"))
        }
    })?;
    if metadata.file_type().is_symlink() {
        let canonical = fs::canonicalize(&candidate).map_err(|error| {
            SandboxError::PolicyDenied(format!(
                "host-backed path could not be resolved safely: {error}"
            ))
        })?;
        ensure_within_root(root, &canonical)?;
    }

    #[cfg(unix)]
    {
        let permissions = fs::Permissions::from_mode(mode & 0o7777);
        fs::set_permissions(&candidate, permissions).map_err(|error| {
            SandboxError::BackendFailure(format!("failed to set mode bits: {error}"))
        })
    }
    #[cfg(not(unix))]
    {
        let _ = candidate;
        Err(SandboxError::UnsupportedFeature(
            "chmod is supported only on unix hosts".to_string(),
        ))
    }
}

fn host_target_for_write(
    root: &Path,
    path: &str,
    create_parents: bool,
) -> Result<PathBuf, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Err(SandboxError::InvalidRequest(
            "cannot write file contents to the workspace root".to_string(),
        ));
    }

    let relative = sandbox_to_workspace_relative(&normalized)?;
    let candidate = root.join(relative);
    validate_existing_ancestor(root, &candidate, create_parents)?;

    if let Ok(metadata) = fs::symlink_metadata(&candidate) {
        if metadata.file_type().is_symlink() {
            let canonical = fs::canonicalize(&candidate).map_err(|error| {
                SandboxError::PolicyDenied(format!(
                    "symlink targets must remain inside the workspace root: {error}"
                ))
            })?;
            ensure_within_root(root, &canonical)?;
        } else if metadata.is_dir() {
            return Err(SandboxError::InvalidRequest(format!(
                "path is a directory: {normalized}"
            )));
        }
    }

    Ok(candidate)
}

fn host_target_for_directory(
    root: &Path,
    path: &str,
    parents: bool,
) -> Result<PathBuf, SandboxError> {
    let normalized = normalize_workspace_path(path)?;
    if normalized == "/workspace" {
        return Ok(root.to_path_buf());
    }
    let candidate = root.join(sandbox_to_workspace_relative(&normalized)?);
    validate_existing_ancestor(root, &candidate, parents)?;
    Ok(candidate)
}

fn list_host_paths(root: &Path) -> Result<Vec<String>, SandboxError> {
    let mut paths = vec!["/".to_string(), "/workspace".to_string()];
    let mut stack = vec![(root.to_path_buf(), "/workspace".to_string())];

    while let Some((host_dir, sandbox_dir)) = stack.pop() {
        let entries = fs::read_dir(&host_dir).map_err(|error| {
            SandboxError::BackendFailure(format!("failed to read directory entries: {error}"))
        })?;

        for entry in entries {
            let entry = entry.map_err(|error| {
                SandboxError::BackendFailure(format!("failed to read directory entry: {error}"))
            })?;
            let host_path = entry.path();
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let sandbox_path = if sandbox_dir == "/" {
                format!("/{file_name}")
            } else {
                format!("{sandbox_dir}/{file_name}")
            };

            let metadata = fs::symlink_metadata(&host_path).map_err(|error| {
                SandboxError::BackendFailure(format!("failed to inspect path: {error}"))
            })?;
            if metadata.file_type().is_symlink() {
                let canonical = fs::canonicalize(&host_path).map_err(|error| {
                    SandboxError::PolicyDenied(format!(
                        "host-backed path could not be resolved safely: {error}"
                    ))
                })?;
                ensure_within_root(root, &canonical)?;
                paths.push(sandbox_path);
                continue;
            }

            paths.push(sandbox_path.clone());

            if metadata.is_dir() {
                stack.push((host_path, sandbox_path));
            }
        }
    }

    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn validate_existing_ancestor(
    root: &Path,
    candidate: &Path,
    create_parents: bool,
) -> Result<(), SandboxError> {
    let parent = candidate.parent().ok_or_else(|| {
        SandboxError::InvalidRequest("path must have a parent directory".to_string())
    })?;
    if !create_parents && !parent.exists() {
        return Err(SandboxError::InvalidRequest(format!(
            "parent directory does not exist: {}",
            parent.display()
        )));
    }

    let mut existing = Some(parent);
    while let Some(current) = existing {
        if current.exists() {
            let canonical = fs::canonicalize(current).map_err(|error| {
                SandboxError::PolicyDenied(format!(
                    "host-backed path could not be resolved safely: {error}"
                ))
            })?;
            ensure_within_root(root, &canonical)?;
            return Ok(());
        }
        existing = current.parent();
    }

    Err(SandboxError::PolicyDenied(
        "host-backed path resolution escaped the configured workspace root".to_string(),
    ))
}

fn normalize_link_target(
    root: &Path,
    parent: &Path,
    target: &Path,
) -> Result<String, SandboxError> {
    let absolute = if target.is_absolute() {
        target.to_path_buf()
    } else {
        parent.join(target)
    };
    let normalized = normalize_host_path(&absolute).ok_or_else(|| {
        SandboxError::PolicyDenied("symlink target escapes the workspace root".to_string())
    })?;
    let relative = normalized.strip_prefix(root).map_err(|_| {
        SandboxError::PolicyDenied("symlink target escapes the workspace root".to_string())
    })?;
    if relative.as_os_str().is_empty() {
        Ok("/workspace".to_string())
    } else {
        Ok(format!("/workspace/{}", relative.display()))
    }
}

fn normalize_host_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
        }
    }
    Some(normalized)
}

fn relative_host_path(from_dir: &Path, to_path: &Path) -> Result<PathBuf, SandboxError> {
    let from = normalize_host_path(from_dir).ok_or_else(|| {
        SandboxError::PolicyDenied("symlink source escapes the workspace root".to_string())
    })?;
    let to = normalize_host_path(to_path).ok_or_else(|| {
        SandboxError::PolicyDenied("symlink target escapes the workspace root".to_string())
    })?;
    let from_components = from.components().collect::<Vec<_>>();
    let to_components = to.components().collect::<Vec<_>>();
    let mut shared = 0usize;

    while shared < from_components.len()
        && shared < to_components.len()
        && from_components[shared] == to_components[shared]
    {
        shared += 1;
    }

    let mut relative = PathBuf::new();
    for _ in shared..from_components.len() {
        relative.push("..");
    }
    for component in to_components.iter().skip(shared) {
        relative.push(component.as_os_str());
    }
    if relative.as_os_str().is_empty() {
        relative.push(".");
    }
    Ok(relative)
}

fn ensure_within_root(root: &Path, candidate: &Path) -> Result<(), SandboxError> {
    if candidate.starts_with(root) {
        Ok(())
    } else {
        Err(SandboxError::PolicyDenied(
            "host-backed path escaped the configured workspace root".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn tempdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn normalizes_relative_paths_into_sandbox_absolute_paths() {
        let normalized = normalize_sandbox_path("workspace/src/lib.rs").unwrap();
        assert_eq!(normalized, "/workspace/src/lib.rs");
    }

    #[test]
    fn resolves_relative_paths_against_cwd() {
        let resolved = resolve_sandbox_path("/workspace", "notes/todo.txt").unwrap();
        assert_eq!(resolved, "/workspace/notes/todo.txt");
    }

    #[test]
    fn blocks_parent_traversal() {
        let error = normalize_sandbox_path("../etc/passwd").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PolicyDenied);
    }

    #[test]
    fn symlink_creation_is_explicitly_unsupported() {
        let filesystem = VirtualFilesystem::new();
        let error = filesystem.create_symlink("/demo", "/escape").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedFeature);
    }

    #[test]
    fn failure_results_map_to_sanitized_error_kinds() {
        let result = ExecutionResult::failure(
            SandboxError::Timeout("time budget exceeded".to_string()),
            BTreeMap::new(),
        );
        assert_eq!(result.termination_reason, TerminationReason::Timeout);
        assert_eq!(result.error.unwrap().kind, ErrorKind::Timeout);
    }

    #[test]
    fn memory_filesystem_persists_within_session() {
        let mut filesystem = MemoryFilesystem::new();
        filesystem.mkdir("/workspace/data", true).unwrap();
        filesystem
            .write_file("/workspace/data/demo.txt", b"hello".to_vec(), false)
            .unwrap();
        assert_eq!(
            filesystem.read_file("/workspace/data/demo.txt").unwrap(),
            b"hello".to_vec()
        );
        assert!(filesystem.exists("/workspace/data").unwrap());
    }

    #[test]
    fn host_cow_reads_from_overlay_before_host() {
        let root = tempdir();
        fs::create_dir_all(root.path().join("notes")).unwrap();
        fs::write(root.path().join("notes/demo.txt"), b"host").unwrap();

        let mut filesystem =
            HostCowFilesystem::new(fs::canonicalize(root.path()).unwrap()).unwrap();
        filesystem
            .write_file("/workspace/notes/demo.txt", b"overlay".to_vec(), false)
            .unwrap();

        assert_eq!(
            filesystem.read_file("/workspace/notes/demo.txt").unwrap(),
            b"overlay".to_vec()
        );
        assert_eq!(
            fs::read(root.path().join("notes/demo.txt")).unwrap(),
            b"host".to_vec()
        );
    }

    #[test]
    fn host_readwrite_denies_writes_outside_writable_roots() {
        let root = tempdir();
        fs::create_dir_all(root.path().join("allowed")).unwrap();
        fs::create_dir_all(root.path().join("blocked")).unwrap();

        let mut filesystem = HostReadwriteFilesystem::new(
            fs::canonicalize(root.path()).unwrap(),
            vec!["/workspace/allowed".to_string()],
        )
        .unwrap();

        let error = filesystem
            .write_file("/workspace/blocked/demo.txt", b"nope".to_vec(), false)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PolicyDenied);
    }

    #[test]
    fn host_workspace_symlink_escape_is_denied() {
        let root = tempdir();
        let outside = tempdir();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();

        #[cfg(unix)]
        {
            let error = host_exists(&fs::canonicalize(root.path()).unwrap(), "/workspace/escape")
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::PolicyDenied);
        }
    }

    #[test]
    fn normalize_writable_roots_requires_workspace_prefix() {
        let roots = BTreeSet::from(["/tmp".to_string()]);
        let error = normalize_writable_roots(&roots).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PolicyDenied);
    }
}
