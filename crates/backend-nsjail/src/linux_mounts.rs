use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use abash_core::{
    normalize_sandbox_path, ExecutionProfile, FilesystemMode, HostMount, SandboxConfig,
    SandboxError,
};

#[derive(Clone, Debug)]
pub(super) struct ResolvedMount {
    pub(super) sandbox_path: String,
    pub(super) host_path: PathBuf,
}

pub(super) fn validate_real_shell_config(config: &SandboxConfig) -> Result<(), SandboxError> {
    if config.profile != ExecutionProfile::RealShell {
        return Err(SandboxError::InvalidRequest(
            "nsjail backend requires the real_shell execution profile".to_string(),
        ));
    }
    match config.filesystem_mode {
        FilesystemMode::HostReadonly | FilesystemMode::HostReadwrite => {}
        FilesystemMode::Memory | FilesystemMode::HostCow => {
            return Err(SandboxError::InvalidRequest(
                "real-shell backend currently requires host_readonly or host_readwrite filesystem mode"
                    .to_string(),
            ))
        }
    }
    if config.workspace_root.is_none() && config.host_mounts.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "real-shell backend requires at least one configured host mount".to_string(),
        ));
    }
    if matches!(config.filesystem_mode, FilesystemMode::HostReadonly)
        && !config.writable_roots.is_empty()
    {
        return Err(SandboxError::InvalidRequest(
            "host_readonly mode must not configure writable roots".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn discover_nsjail_bin() -> Result<PathBuf, SandboxError> {
    if let Ok(value) = std::env::var("ABASH_NSJAIL_BIN") {
        let path = PathBuf::from(value);
        if path.is_file() {
            return Ok(path);
        }
        return Err(SandboxError::UnsupportedFeature(
            "ABASH_NSJAIL_BIN must point to an existing nsjail binary".to_string(),
        ));
    }

    let path_var = std::env::var_os("PATH").ok_or_else(|| {
        SandboxError::UnsupportedFeature(
            "nsjail binary was not found in PATH; set ABASH_NSJAIL_BIN".to_string(),
        )
    })?;
    for entry in std::env::split_paths(&path_var) {
        let candidate = entry.join("nsjail");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(SandboxError::UnsupportedFeature(
        "nsjail binary was not found in PATH; set ABASH_NSJAIL_BIN".to_string(),
    ))
}

pub(super) fn resolve_mounts(config: &SandboxConfig) -> Result<Vec<ResolvedMount>, SandboxError> {
    let mut mounts = Vec::new();
    if let Some(root) = &config.workspace_root {
        mounts.push(ResolvedMount {
            sandbox_path: "/workspace".to_string(),
            host_path: canonicalize_host_root(root)?,
        });
    }
    for mount in &config.host_mounts {
        mounts.push(ResolvedMount {
            sandbox_path: normalize_mount_root(&mount.sandbox_path)?,
            host_path: canonicalize_host_root(&mount.host_path)?,
        });
    }
    mounts.sort_by(|left, right| left.sandbox_path.cmp(&right.sandbox_path));
    for window in mounts.windows(2) {
        let left = &window[0];
        let right = &window[1];
        if left.sandbox_path == right.sandbox_path {
            return Err(SandboxError::InvalidRequest(format!(
                "duplicate host mount path: {}",
                left.sandbox_path
            )));
        }
        if path_is_within_root(&right.sandbox_path, &left.sandbox_path) {
            return Err(SandboxError::InvalidRequest(format!(
                "nested host mounts are not supported: {} and {}",
                left.sandbox_path, right.sandbox_path
            )));
        }
    }
    Ok(mounts)
}

pub(super) fn resolve_writable_roots(config: &SandboxConfig) -> Result<Vec<String>, SandboxError> {
    if !matches!(config.filesystem_mode, FilesystemMode::HostReadwrite) {
        return Ok(Vec::new());
    }
    let mounts = resolve_mounts(config)?;
    let mut roots = config
        .writable_roots
        .iter()
        .map(|root| {
            let normalized = normalize_sandbox_path(root)?;
            ensure_path_in_mounts(&mounts, &normalized)?;
            Ok(normalized)
        })
        .collect::<Result<Vec<_>, SandboxError>>()?;
    roots.sort();
    roots.dedup();
    Ok(roots)
}

pub(super) fn build_nsjail_args(
    mounts: &[ResolvedMount],
    writable_roots: &[String],
    cwd: &str,
    timeout_ms: Option<u64>,
    argv: &[String],
) -> Vec<String> {
    let mut args = vec![
        "-Mo".to_string(),
        "--chroot".to_string(),
        "/".to_string(),
        "--cwd".to_string(),
        cwd.to_string(),
        "-R".to_string(),
        "/bin".to_string(),
        "-R".to_string(),
        "/usr".to_string(),
        "-R".to_string(),
        "/lib".to_string(),
        "-R".to_string(),
        "/lib64".to_string(),
        "-R".to_string(),
        "/etc".to_string(),
        "-R".to_string(),
        "/dev/null".to_string(),
        "-R".to_string(),
        "/dev/urandom".to_string(),
        "-R".to_string(),
        "/dev/random".to_string(),
        "-R".to_string(),
        "/dev/zero".to_string(),
        "-T".to_string(),
        "/tmp".to_string(),
    ];
    if let Some(limit) = timeout_ms {
        args.push("--time_limit".to_string());
        args.push(timeout_secs(limit).to_string());
    }
    for mount in mounts {
        args.push("-R".to_string());
        args.push(format!(
            "{}:{}",
            mount.host_path.display(),
            mount.sandbox_path
        ));
    }
    for root in writable_roots {
        if let Ok(host_path) = writable_root_host_path(mounts, root) {
            args.push("-B".to_string());
            args.push(format!("{}:{root}", host_path.display()));
        }
    }
    args.push("--".to_string());
    args.extend(argv.iter().cloned());
    args
}

fn timeout_secs(timeout_ms: u64) -> u64 {
    timeout_ms.saturating_add(999) / 1000
}

pub(super) fn top_level_command_name(argv: &[String]) -> Option<String> {
    argv.first().map(|value| {
        Path::new(value)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| value.clone())
    })
}

pub(super) fn resolve_mount<'a>(
    mounts: &'a [ResolvedMount],
    path: &str,
) -> Result<(&'a ResolvedMount, PathBuf), SandboxError> {
    let normalized = normalize_sandbox_path(path)?;
    let mount = mounts
        .iter()
        .find(|mount| path_is_within_root(&normalized, &mount.sandbox_path))
        .ok_or_else(|| {
            SandboxError::PolicyDenied(format!(
                "host-backed filesystem access is restricted to: {}",
                mounts
                    .iter()
                    .map(|mount| mount.sandbox_path.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    let relative = sandbox_to_mount_relative(&normalized, &mount.sandbox_path)?;
    Ok((mount, relative))
}

pub(super) fn ensure_path_in_mounts(
    mounts: &[ResolvedMount],
    path: &str,
) -> Result<(), SandboxError> {
    let _ = resolve_mount(mounts, path)?;
    Ok(())
}

pub(super) fn writable_root_host_path(
    mounts: &[ResolvedMount],
    sandbox_root: &str,
) -> Result<PathBuf, SandboxError> {
    let (mount, relative) = resolve_mount(mounts, sandbox_root)?;
    Ok(mount.host_path.join(relative))
}

pub(super) fn path_is_within_root(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn sandbox_to_mount_relative(path: &str, mount_path: &str) -> Result<PathBuf, SandboxError> {
    let normalized = normalize_sandbox_path(path)?;
    let mount_path = normalize_mount_root(mount_path)?;
    if normalized == mount_path {
        Ok(PathBuf::new())
    } else {
        let prefix = format!("{mount_path}/");
        let relative = normalized.strip_prefix(&prefix).ok_or_else(|| {
            SandboxError::PolicyDenied(format!(
                "path is not within configured host mount: {normalized}"
            ))
        })?;
        Ok(PathBuf::from(relative))
    }
}

fn canonicalize_host_root(root: &Path) -> Result<PathBuf, SandboxError> {
    let canonical = fs::canonicalize(root).map_err(|error| {
        SandboxError::InvalidRequest(format!("host mount must exist and be accessible: {error}"))
    })?;
    if !canonical.is_dir() {
        return Err(SandboxError::InvalidRequest(
            "host mount must be a directory".to_string(),
        ));
    }
    Ok(canonical)
}

fn normalize_mount_root(path: &str) -> Result<String, SandboxError> {
    let normalized = normalize_sandbox_path(path)?;
    if normalized == "/" {
        return Err(SandboxError::InvalidRequest(
            "host mount path must not be /".to_string(),
        ));
    }
    Ok(normalized)
}

pub(super) fn ensure_within_root(root: &Path, candidate: &Path) -> Result<(), SandboxError> {
    if candidate == root || candidate.starts_with(root) {
        Ok(())
    } else {
        Err(SandboxError::PolicyDenied(
            "host-backed path escaped its configured mount".to_string(),
        ))
    }
}

pub(super) fn validate_existing_ancestor(
    root: &Path,
    candidate: &Path,
) -> Result<(), SandboxError> {
    let mut current = Some(candidate);
    while let Some(path) = current {
        if path.exists() {
            let canonical = fs::canonicalize(path).map_err(|error| {
                SandboxError::PolicyDenied(format!(
                    "host-backed path could not be resolved safely: {error}"
                ))
            })?;
            ensure_within_root(root, &canonical)?;
            return Ok(());
        }
        current = path.parent();
    }
    Ok(())
}

pub(super) fn base_metadata(
    config: &SandboxConfig,
    mut metadata: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    metadata.insert("backend".to_string(), "nsjail".to_string());
    metadata.insert(
        "profile".to_string(),
        match config.profile {
            ExecutionProfile::Safe => "safe",
            ExecutionProfile::Workspace => "workspace",
            ExecutionProfile::RealShell => "real_shell",
        }
        .to_string(),
    );
    metadata.insert(
        "filesystem_mode".to_string(),
        match config.filesystem_mode {
            FilesystemMode::Memory => "memory",
            FilesystemMode::HostReadonly => "host_readonly",
            FilesystemMode::HostCow => "host_cow",
            FilesystemMode::HostReadwrite => "host_readwrite",
        }
        .to_string(),
    );
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_command_uses_path_basename() {
        assert_eq!(
            top_level_command_name(&["/bin/bash".to_string(), "-lc".to_string()]),
            Some("bash".to_string())
        );
    }

    #[test]
    fn build_nsjail_args_adds_rw_submounts_after_ro_mounts() {
        let mounts = vec![ResolvedMount {
            sandbox_path: "/workspace".to_string(),
            host_path: PathBuf::from("/tmp/workspace"),
        }];

        let args = build_nsjail_args(
            &mounts,
            &["/workspace/output".to_string()],
            "/workspace",
            Some(1200),
            &["bash".to_string(), "-lc".to_string(), "echo hi".to_string()],
        );

        assert!(args.windows(2).any(|pair| pair == ["--time_limit", "2"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-R", "/tmp/workspace:/workspace"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-B", "/tmp/workspace/output:/workspace/output"]));
        assert_eq!(args.last(), Some(&"echo hi".to_string()));
    }

    #[test]
    fn resolve_mounts_rejects_nested_sandbox_mounts() {
        let config = SandboxConfig {
            profile: ExecutionProfile::RealShell,
            filesystem_mode: FilesystemMode::HostReadonly,
            session_state: abash_core::SessionState::Persistent,
            allowlisted_commands: ["bash".to_string()].into_iter().collect(),
            default_cwd: "/workspace".to_string(),
            workspace_root: None,
            host_mounts: vec![
                HostMount {
                    sandbox_path: "/workspace".to_string(),
                    host_path: std::env::temp_dir(),
                },
                HostMount {
                    sandbox_path: "/workspace/sub".to_string(),
                    host_path: std::env::temp_dir(),
                },
            ],
            writable_roots: Default::default(),
            network_policy: None,
        };

        let error = resolve_mounts(&config).unwrap_err();
        assert!(error
            .sanitized()
            .message
            .contains("nested host mounts are not supported"));
    }
}
