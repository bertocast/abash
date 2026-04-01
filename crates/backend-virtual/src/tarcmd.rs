use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};

use abash_core::{resolve_sandbox_path, SandboxError, SandboxFilesystem};
use tar::{Archive, Builder, EntryType, Header};

use crate::{cp, gzipcmd};

pub(crate) enum Mode {
    Create,
    Extract,
    List,
}

pub(crate) struct Spec {
    pub(crate) mode: Mode,
    pub(crate) archive_path: Option<String>,
    pub(crate) member_paths: Vec<String>,
    pub(crate) directory: Option<String>,
    pub(crate) gzip: bool,
    pub(crate) to_stdout: bool,
}

pub(crate) fn parse(cwd: &str, args: &[String]) -> Result<Spec, SandboxError> {
    let mut create = false;
    let mut extract = false;
    let mut list = false;
    let mut archive_path = None;
    let mut directory = None;
    let mut gzip = false;
    let mut to_stdout = false;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        if arg == "--" {
            index += 1;
            break;
        }

        if !arg.starts_with('-') || arg == "-" {
            break;
        }

        if let Some(value) = arg.strip_prefix("--file=") {
            archive_path = Some(resolve_archive_path(cwd, value)?);
            index += 1;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--directory=") {
            directory = Some(resolve_sandbox_path(cwd, value)?);
            index += 1;
            continue;
        }

        match arg.as_str() {
            "-c" | "--create" => create = true,
            "-x" | "--extract" | "--get" => extract = true,
            "-t" | "--list" => list = true,
            "-z" | "--gzip" | "--gunzip" => gzip = true,
            "-O" | "--to-stdout" => to_stdout = true,
            "-v" | "--verbose" => {}
            "-a" | "--auto-compress" => {}
            "-f" | "--file" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "tar -f requires an archive path".to_string(),
                    ));
                };
                archive_path = Some(resolve_archive_path(cwd, value)?);
                index += 2;
                continue;
            }
            "-C" | "--directory" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "tar -C requires a directory".to_string(),
                    ));
                };
                directory = Some(resolve_sandbox_path(cwd, value)?);
                index += 2;
                continue;
            }
            _ if arg.starts_with('-') && !arg.starts_with("--") && arg.len() > 2 => {
                let chars = arg.chars().collect::<Vec<_>>();
                let mut inner = 1usize;
                while inner < chars.len() {
                    match chars[inner] {
                        'c' => create = true,
                        'x' => extract = true,
                        't' => list = true,
                        'z' => gzip = true,
                        'O' => to_stdout = true,
                        'v' | 'a' => {}
                        'f' => {
                            if inner + 1 < chars.len() {
                                let value = chars[inner + 1..].iter().collect::<String>();
                                archive_path = Some(resolve_archive_path(cwd, &value)?);
                                inner = chars.len();
                                continue;
                            }
                            let Some(value) = args.get(index + 1) else {
                                return Err(SandboxError::InvalidRequest(
                                    "tar -f requires an archive path".to_string(),
                                ));
                            };
                            archive_path = Some(resolve_archive_path(cwd, value)?);
                            index += 1;
                        }
                        'C' => {
                            if inner + 1 < chars.len() {
                                let value = chars[inner + 1..].iter().collect::<String>();
                                directory = Some(resolve_sandbox_path(cwd, &value)?);
                                inner = chars.len();
                                continue;
                            }
                            let Some(value) = args.get(index + 1) else {
                                return Err(SandboxError::InvalidRequest(
                                    "tar -C requires a directory".to_string(),
                                ));
                            };
                            directory = Some(resolve_sandbox_path(cwd, value)?);
                            index += 1;
                        }
                        _ => {
                            return Err(SandboxError::InvalidRequest(format!(
                                "tar flag is not supported: {arg}"
                            )))
                        }
                    }
                    inner += 1;
                }
            }
            _ => {
                return Err(SandboxError::InvalidRequest(format!(
                    "tar flag is not supported: {arg}"
                )))
            }
        }

        index += 1;
    }

    let mode_count = create as u8 + extract as u8 + list as u8;
    if mode_count != 1 {
        return Err(SandboxError::InvalidRequest(
            "tar requires exactly one of -c, -x, or -t".to_string(),
        ));
    }

    let mode = if create {
        Mode::Create
    } else if extract {
        Mode::Extract
    } else {
        Mode::List
    };

    Ok(Spec {
        mode,
        archive_path,
        member_paths: args[index..].to_vec(),
        directory,
        gzip,
        to_stdout,
    })
}

pub(crate) fn execute(
    filesystem: &mut dyn SandboxFilesystem,
    cwd: &str,
    spec: &Spec,
    stdin: &[u8],
) -> Result<Vec<u8>, SandboxError> {
    match spec.mode {
        Mode::Create => create_archive(filesystem, cwd, spec),
        Mode::List => list_archive(filesystem, spec, stdin),
        Mode::Extract => extract_archive(filesystem, cwd, spec, stdin),
    }
}

fn create_archive(
    filesystem: &mut dyn SandboxFilesystem,
    cwd: &str,
    spec: &Spec,
) -> Result<Vec<u8>, SandboxError> {
    if spec.member_paths.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "tar refuses to create an empty archive".to_string(),
        ));
    }

    let base_cwd = spec.directory.as_deref().unwrap_or(cwd);
    let mut archive = Builder::new(Vec::new());
    let mut listed_paths = filesystem.list_paths()?;
    listed_paths.sort();

    for original in &spec.member_paths {
        let resolved = resolve_sandbox_path(base_cwd, original)?;
        append_member(
            filesystem,
            &mut archive,
            &listed_paths,
            &resolved,
            &archive_name(original),
        )?;
    }

    let raw = archive
        .into_inner()
        .map_err(io_error("tar could not finalize archive"))?;
    let output = if should_use_gzip(spec) {
        gzipcmd::compress_bytes(&raw)?
    } else {
        raw
    };

    if let Some(path) = &spec.archive_path {
        filesystem.write_file(path, output, true)?;
        Ok(Vec::new())
    } else {
        Ok(output)
    }
}

fn list_archive(
    filesystem: &mut dyn SandboxFilesystem,
    spec: &Spec,
    stdin: &[u8],
) -> Result<Vec<u8>, SandboxError> {
    let input = archive_input(filesystem, spec, stdin)?;
    let mut archive = Archive::new(Cursor::new(input));
    let mut names = Vec::new();

    for entry in archive_entries(&mut archive)? {
        if !member_matches(&entry.path, &spec.member_paths) {
            continue;
        }
        names.push(entry.path);
    }

    if names.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(format!("{}\n", names.join("\n")).into_bytes())
    }
}

fn extract_archive(
    filesystem: &mut dyn SandboxFilesystem,
    cwd: &str,
    spec: &Spec,
    stdin: &[u8],
) -> Result<Vec<u8>, SandboxError> {
    let input = archive_input(filesystem, spec, stdin)?;
    let mut archive = Archive::new(Cursor::new(input));
    let destination = spec.directory.as_deref().unwrap_or(cwd);
    let mut stdout = Vec::new();

    for entry in archive_entries(&mut archive)? {
        if !member_matches(&entry.path, &spec.member_paths) {
            continue;
        }

        let safe_path = sanitize_member_path(&entry.path)?;
        match entry.kind {
            TarEntryKind::Directory => {
                if spec.to_stdout {
                    continue;
                }
                let target = resolve_sandbox_path(destination, &safe_path)?;
                filesystem.mkdir(&target, true)?;
            }
            TarEntryKind::Regular(contents) => {
                if spec.to_stdout {
                    stdout.extend_from_slice(&contents);
                    continue;
                }
                let target = resolve_sandbox_path(destination, &safe_path)?;
                filesystem.write_file(&target, contents, true)?;
            }
            TarEntryKind::Symlink(target_name) => {
                if spec.to_stdout {
                    continue;
                }
                sanitize_symlink_target(&target_name)?;
                let target = resolve_sandbox_path(destination, &safe_path)?;
                filesystem.create_symlink(&target_name, &target)?;
            }
        }
    }

    Ok(stdout)
}

fn append_member(
    filesystem: &mut dyn SandboxFilesystem,
    archive: &mut Builder<Vec<u8>>,
    listed_paths: &[String],
    source: &str,
    member_name: &str,
) -> Result<(), SandboxError> {
    if let Some(link_target) = filesystem.read_link(source)? {
        append_symlink(archive, member_name, &link_target)?;
        return Ok(());
    }

    if filesystem.is_dir(source)? {
        append_directory(archive, member_name)?;
        for descendant in cp::descendant_paths(source, listed_paths) {
            let suffix = descendant
                .strip_prefix(source)
                .unwrap_or(descendant.as_str())
                .trim_start_matches('/');
            let entry_name = if suffix.is_empty() {
                member_name.to_string()
            } else if member_name.is_empty() {
                suffix.to_string()
            } else {
                format!("{member_name}/{suffix}")
            };

            if let Some(link_target) = filesystem.read_link(&descendant)? {
                append_symlink(archive, &entry_name, &link_target)?;
            } else if filesystem.is_dir(&descendant)? {
                append_directory(archive, &entry_name)?;
            } else {
                let contents = filesystem.read_file(&descendant)?;
                append_file(archive, &entry_name, &contents)?;
            }
        }
        return Ok(());
    }

    let contents = filesystem.read_file(source)?;
    append_file(archive, member_name, &contents)
}

fn append_file(
    archive: &mut Builder<Vec<u8>>,
    path: &str,
    contents: &[u8],
) -> Result<(), SandboxError> {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_mode(0o644);
    header.set_size(contents.len() as u64);
    header
        .set_path(path)
        .map_err(|error| SandboxError::BackendFailure(format!("tar invalid path: {error}")))?;
    header.set_cksum();
    archive
        .append(&header, Cursor::new(contents))
        .map_err(io_error("tar could not append file"))
}

fn append_directory(archive: &mut Builder<Vec<u8>>, path: &str) -> Result<(), SandboxError> {
    if path.is_empty() {
        return Ok(());
    }

    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Directory);
    header.set_mode(0o755);
    header.set_size(0);
    header
        .set_path(path)
        .map_err(|error| SandboxError::BackendFailure(format!("tar invalid path: {error}")))?;
    header.set_cksum();
    archive
        .append(&header, std::io::empty())
        .map_err(io_error("tar could not append directory"))
}

fn append_symlink(
    archive: &mut Builder<Vec<u8>>,
    path: &str,
    target: &str,
) -> Result<(), SandboxError> {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Symlink);
    header.set_mode(0o777);
    header.set_size(0);
    header
        .set_path(path)
        .map_err(|error| SandboxError::BackendFailure(format!("tar invalid path: {error}")))?;
    header
        .set_link_name(target)
        .map_err(|error| SandboxError::BackendFailure(format!("tar invalid symlink: {error}")))?;
    header.set_cksum();
    archive
        .append(&header, std::io::empty())
        .map_err(io_error("tar could not append symlink"))
}

fn archive_input(
    filesystem: &mut dyn SandboxFilesystem,
    spec: &Spec,
    stdin: &[u8],
) -> Result<Vec<u8>, SandboxError> {
    let raw = if let Some(path) = &spec.archive_path {
        filesystem.read_file(path)?
    } else {
        stdin.to_vec()
    };

    if should_use_gzip(spec) {
        gzipcmd::decompress_bytes(&raw)
    } else {
        Ok(raw)
    }
}

fn should_use_gzip(spec: &Spec) -> bool {
    spec.gzip
        || spec
            .archive_path
            .as_deref()
            .is_some_and(|path| path.ends_with(".tar.gz") || path.ends_with(".tgz"))
}

fn resolve_archive_path(cwd: &str, value: &str) -> Result<String, SandboxError> {
    if value == "-" {
        Ok("-".to_string())
    } else {
        resolve_sandbox_path(cwd, value)
    }
}

fn archive_name(original: &str) -> String {
    if original == "." {
        return ".".to_string();
    }

    let path = Path::new(original);
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::CurDir => {}
            other => cleaned.push(other.as_os_str()),
        }
    }

    let rendered = cleaned.to_string_lossy().replace('\\', "/");
    if rendered.is_empty() {
        ".".to_string()
    } else {
        rendered
    }
}

fn member_matches(path: &str, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    filters.iter().any(|filter| {
        let normalized = archive_name(filter);
        path == normalized || path.strip_prefix(&(normalized.clone() + "/")).is_some()
    })
}

fn sanitize_member_path(path: &str) -> Result<String, SandboxError> {
    if path.starts_with('/') {
        return Err(SandboxError::BackendFailure(format!(
            "tar: {path}: absolute paths are not allowed"
        )));
    }

    let member = Path::new(path);
    if member
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(SandboxError::BackendFailure(format!(
            "tar: {path}: Path contains '..'"
        )));
    }

    let rendered = member
        .components()
        .filter_map(|component| match component {
            Component::CurDir => None,
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");

    if rendered.is_empty() {
        return Err(SandboxError::BackendFailure(
            "tar: empty archive member path".to_string(),
        ));
    }

    Ok(rendered)
}

fn sanitize_symlink_target(target: &str) -> Result<(), SandboxError> {
    let target_path = Path::new(target);
    if target_path.is_absolute()
        || target_path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(SandboxError::BackendFailure(format!(
            "tar: unsafe symlink target: {target}"
        )));
    }
    Ok(())
}

struct ParsedEntry {
    path: String,
    kind: TarEntryKind,
}

enum TarEntryKind {
    Directory,
    Regular(Vec<u8>),
    Symlink(String),
}

fn archive_entries<R: Read>(archive: &mut Archive<R>) -> Result<Vec<ParsedEntry>, SandboxError> {
    let mut parsed = Vec::new();
    for entry in archive
        .entries()
        .map_err(io_error("tar could not read archive"))?
    {
        let mut entry = entry.map_err(io_error("tar could not read archive entry"))?;
        let path = entry
            .path()
            .map_err(io_error("tar could not read archive path"))?
            .to_string_lossy()
            .replace('\\', "/");
        let kind = if entry.header().entry_type().is_dir() {
            TarEntryKind::Directory
        } else if entry.header().entry_type().is_symlink() {
            let Some(target) = entry
                .link_name()
                .map_err(io_error("tar could not read symlink target"))?
            else {
                return Err(SandboxError::BackendFailure(
                    "tar symlink entry is missing a target".to_string(),
                ));
            };
            TarEntryKind::Symlink(target.to_string_lossy().into_owned())
        } else {
            let mut contents = Vec::new();
            entry
                .read_to_end(&mut contents)
                .map_err(io_error("tar could not read archive contents"))?;
            TarEntryKind::Regular(contents)
        };
        parsed.push(ParsedEntry { path, kind });
    }
    Ok(parsed)
}

fn io_error(prefix: &'static str) -> impl Fn(std::io::Error) -> SandboxError {
    move |error| SandboxError::BackendFailure(format!("{prefix}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_flags() {
        let spec = parse(
            "/workspace",
            &[
                "-czf".to_string(),
                "demo.tar.gz".to_string(),
                "src".to_string(),
            ],
        )
        .unwrap();

        assert!(matches!(spec.mode, Mode::Create));
        assert!(spec.gzip);
        assert_eq!(spec.archive_path.as_deref(), Some("/workspace/demo.tar.gz"));
        assert_eq!(spec.member_paths, vec!["src".to_string()]);
    }

    #[test]
    fn strips_absolute_archive_names() {
        assert_eq!(archive_name("/workspace/demo.txt"), "workspace/demo.txt");
        assert_eq!(archive_name("./demo.txt"), "demo.txt");
    }

    #[test]
    fn blocks_parent_traversal_members() {
        let error = sanitize_member_path("../escape.txt").unwrap_err();
        assert!(error.sanitized().message.contains("Path contains '..'"));
    }
}
