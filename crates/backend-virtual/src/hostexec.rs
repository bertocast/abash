use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use abash_core::{SandboxError, SandboxFilesystem};

pub(crate) struct HostBridge {
    pub(crate) root: PathBuf,
    initial_paths: BTreeSet<String>,
}

impl HostBridge {
    pub(crate) fn new(filesystem: &mut dyn SandboxFilesystem) -> Result<Self, SandboxError> {
        let root = create_temp_root()?;
        fs::create_dir_all(root.join("workspace"))
            .map_err(io_error("failed to create temp workspace"))?;

        let initial_paths = filesystem
            .list_paths()?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut directories = initial_paths
            .iter()
            .filter(|path| **path == "/" || filesystem.is_dir(path).unwrap_or(false))
            .cloned()
            .collect::<Vec<_>>();
        directories.sort_by_key(|path| path.matches('/').count());

        for directory in directories {
            if directory == "/" {
                continue;
            }
            fs::create_dir_all(sandbox_path_to_host(&root, &directory))
                .map_err(io_error("failed to create temp directory"))?;
        }

        let mut files = initial_paths
            .iter()
            .filter(|path| **path != "/" && !filesystem.is_dir(path).unwrap_or(false))
            .cloned()
            .collect::<Vec<_>>();
        files.sort();

        for file in files {
            let host_path = sandbox_path_to_host(&root, &file);
            if let Some(parent) = host_path.parent() {
                fs::create_dir_all(parent).map_err(io_error("failed to create temp parent"))?;
            }
            let contents = filesystem.read_file(&file)?;
            fs::write(&host_path, contents).map_err(io_error("failed to write temp file"))?;
        }

        Ok(Self {
            root,
            initial_paths,
        })
    }

    pub(crate) fn map_sandbox_path(&self, sandbox_path: &str) -> PathBuf {
        sandbox_path_to_host(&self.root, sandbox_path)
    }

    pub(crate) fn sync_back(
        &self,
        filesystem: &mut dyn SandboxFilesystem,
    ) -> Result<(), SandboxError> {
        let final_paths = collect_host_paths(&self.root)?;

        let mut directories = final_paths
            .iter()
            .filter(|path| **path != "/" && is_host_dir(&self.root, path))
            .cloned()
            .collect::<Vec<_>>();
        directories.sort_by_key(|path| path.matches('/').count());
        for directory in directories {
            filesystem.mkdir(&directory, true)?;
        }

        let mut files = final_paths
            .iter()
            .filter(|path| **path != "/" && !is_host_dir(&self.root, path))
            .cloned()
            .collect::<Vec<_>>();
        files.sort();
        for file in files {
            let contents = fs::read(sandbox_path_to_host(&self.root, &file))
                .map_err(io_error("failed to read temp file"))?;
            filesystem.write_file(&file, contents, true)?;
        }

        for removed in self.initial_paths.difference(&final_paths) {
            if removed == "/" || removed == "/workspace" {
                continue;
            }
            filesystem.delete_path(removed, true)?;
        }

        Ok(())
    }

    pub(crate) fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(crate) fn bootstrap_dir(root: &Path, sandbox_cwd: &str) -> Result<PathBuf, SandboxError> {
    let dir = root.join("_abash_bootstrap");
    fs::create_dir_all(&dir).map_err(io_error("failed to create python bootstrap directory"))?;
    fs::write(
        dir.join("sitecustomize.py"),
        python_sitecustomize(sandbox_cwd),
    )
    .map_err(io_error("failed to write python bootstrap"))?;
    Ok(dir)
}

fn create_temp_root() -> Result<PathBuf, SandboxError> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            SandboxError::BackendFailure(format!("failed to create temp timestamp: {error}"))
        })?
        .as_nanos();
    let path = std::env::temp_dir().join(format!("abash-hostexec-{}-{suffix}", std::process::id()));
    Ok(path)
}

fn collect_host_paths(root: &Path) -> Result<BTreeSet<String>, SandboxError> {
    let mut paths = BTreeSet::from(["/".to_string(), "/workspace".to_string()]);
    let mut stack = vec![(root.to_path_buf(), "/".to_string())];

    while let Some((host_dir, sandbox_dir)) = stack.pop() {
        let entries = fs::read_dir(&host_dir).map_err(io_error("failed to read temp directory"))?;
        for entry in entries {
            let entry = entry.map_err(io_error("failed to inspect temp directory entry"))?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if sandbox_dir == "/" && file_name == "_abash_bootstrap" {
                continue;
            }
            let sandbox_path = if sandbox_dir == "/" {
                format!("/{file_name}")
            } else {
                format!("{sandbox_dir}/{file_name}")
            };
            paths.insert(sandbox_path.clone());
            if entry
                .file_type()
                .map_err(io_error("failed to inspect temp file type"))?
                .is_dir()
            {
                stack.push((entry.path(), sandbox_path));
            }
        }
    }

    Ok(paths)
}

fn is_host_dir(root: &Path, sandbox_path: &str) -> bool {
    fs::metadata(sandbox_path_to_host(root, sandbox_path))
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

fn sandbox_path_to_host(root: &Path, sandbox_path: &str) -> PathBuf {
    if sandbox_path == "/" {
        root.to_path_buf()
    } else {
        root.join(sandbox_path.trim_start_matches('/'))
    }
}

fn python_sitecustomize(sandbox_cwd: &str) -> String {
    format!(
        r#"
import builtins
import os
import pathlib

_ROOT = os.environ.get("ABASH_SANDBOX_ROOT", "")
_CWD = {sandbox_cwd:?}

def _map_path(value):
    try:
        path = os.fspath(value)
    except TypeError:
        return value
    if isinstance(path, str) and path.startswith("/"):
        return os.path.join(_ROOT, path.lstrip("/"))
    return value

_orig_open = builtins.open
builtins.open = lambda file, *args, **kwargs: _orig_open(_map_path(file), *args, **kwargs)

_orig_listdir = os.listdir
os.listdir = lambda path=".": _orig_listdir(_map_path(path))

_orig_mkdir = os.mkdir
os.mkdir = lambda path, mode=0o777, *, dir_fd=None: _orig_mkdir(_map_path(path), mode, dir_fd=dir_fd)

_orig_makedirs = os.makedirs
os.makedirs = lambda name, mode=0o777, exist_ok=False: _orig_makedirs(_map_path(name), mode, exist_ok=exist_ok)

_orig_remove = os.remove
os.remove = lambda path, *, dir_fd=None: _orig_remove(_map_path(path), dir_fd=dir_fd)
os.unlink = lambda path, *, dir_fd=None: _orig_remove(_map_path(path), dir_fd=dir_fd)

_orig_rmdir = os.rmdir
os.rmdir = lambda path, *, dir_fd=None: _orig_rmdir(_map_path(path), dir_fd=dir_fd)

_orig_stat = os.stat
os.stat = lambda path, *args, **kwargs: _orig_stat(_map_path(path), *args, **kwargs)

_orig_exists = os.path.exists
os.path.exists = lambda path: _orig_exists(_map_path(path))
os.path.isfile = lambda path: os.path.exists(path) and not os.path.isdir(path)
_orig_isdir = os.path.isdir
os.path.isdir = lambda path: _orig_isdir(_map_path(path))
_orig_getsize = os.path.getsize
os.path.getsize = lambda path: _orig_getsize(_map_path(path))

_orig_getcwd = os.getcwd
os.getcwd = lambda: _CWD

_orig_chdir = os.chdir
def _abash_chdir(path):
    global _CWD
    _orig_chdir(_map_path(path))
    if isinstance(path, str) and path.startswith("/"):
        _CWD = path
        return
    host_cwd = _orig_getcwd()
    if host_cwd.startswith(_ROOT):
        suffix = host_cwd[len(_ROOT):].replace("\\", "/")
        _CWD = suffix or "/"
    else:
        _CWD = host_cwd
os.chdir = _abash_chdir

_Path = pathlib.Path
_Path_open = _Path.open
_Path_stat = _Path.stat
_Path_exists = _Path.exists
_Path_is_dir = _Path.is_dir
_Path_is_file = _Path.is_file
_Path_iterdir = _Path.iterdir
_Path_mkdir = _Path.mkdir
_Path_unlink = _Path.unlink
_Path_rmdir = _Path.rmdir

def _abash_host_path(path_obj):
    rendered = str(path_obj)
    if rendered.startswith("/"):
        return type(path_obj)(_map_path(rendered))
    return path_obj

_Path.open = lambda self, *args, **kwargs: _Path_open(_abash_host_path(self), *args, **kwargs)
_Path.stat = lambda self, *args, **kwargs: _Path_stat(_abash_host_path(self), *args, **kwargs)
_Path.exists = lambda self: _Path_exists(_abash_host_path(self))
_Path.is_dir = lambda self: _Path_is_dir(_abash_host_path(self))
_Path.is_file = lambda self: _Path_is_file(_abash_host_path(self))
_Path.iterdir = lambda self: _Path_iterdir(_abash_host_path(self))
_Path.mkdir = lambda self, mode=0o777, parents=False, exist_ok=False: _Path_mkdir(_abash_host_path(self), mode=mode, parents=parents, exist_ok=exist_ok)
_Path.unlink = lambda self, missing_ok=False: _Path_unlink(_abash_host_path(self), missing_ok=missing_ok)
_Path.rmdir = lambda self: _Path_rmdir(_abash_host_path(self))
"#
    )
}

fn io_error(prefix: &'static str) -> impl Fn(std::io::Error) -> SandboxError {
    move |error| SandboxError::BackendFailure(format!("{prefix}: {error}"))
}
