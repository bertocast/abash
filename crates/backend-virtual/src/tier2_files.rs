use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn tree<F, G>(
    cwd: &str,
    args: &[String],
    mut list_paths: F,
    mut is_dir: G,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut() -> Result<Vec<String>, SandboxError>,
    G: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_tree_spec(cwd, args)?;
    let candidates = list_paths()?;
    let mut rendered = Vec::new();

    for (root_index, root) in spec.roots.iter().enumerate() {
        rendered.push(display_tree_path(root));
        let mut children = collect_children(&root.resolved, &candidates, spec.show_all);
        if let Some(limit) = spec.max_depth {
            children.retain(|child| depth_from_root(&child.path, &root.resolved) <= limit);
        }
        render_tree_children(&mut rendered, &children, &candidates, &mut is_dir)?;
        if root_index + 1 != spec.roots.len() {
            rendered.push(String::new());
        }
    }

    Ok(if rendered.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", rendered.join("\n")).into_bytes()
    })
}

pub(crate) fn stat<F, G, H>(
    cwd: &str,
    args: &[String],
    mut read_file: F,
    mut is_dir: G,
    mut read_link: H,
    candidates: &[String],
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    G: FnMut(&str) -> Result<bool, SandboxError>,
    H: FnMut(&str) -> Result<Option<String>, SandboxError>,
{
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "stat requires at least one path".to_string(),
        ));
    }

    let mut blocks = Vec::new();
    for arg in args {
        let resolved = resolve_sandbox_path(cwd, arg)?;
        let mut lines = vec![format!("File: {arg}")];
        if let Some(target) = read_link(&resolved)? {
            lines.push("Type: symbolic link".to_string());
            lines.push(format!("Target: {target}"));
        } else if is_dir(&resolved)? {
            let entry_count = count_children(&resolved, candidates);
            lines.push("Type: directory".to_string());
            lines.push(format!("Entries: {entry_count}"));
        } else {
            let size = read_file(&resolved)?.len();
            lines.push("Type: regular file".to_string());
            lines.push(format!("Size: {size}"));
        }
        blocks.push(lines.join("\n"));
    }

    Ok(format!("{}\n", blocks.join("\n\n")).into_bytes())
}

pub(crate) fn file<F, G, H>(
    cwd: &str,
    args: &[String],
    mut read_file: F,
    mut is_dir: G,
    mut read_link: H,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    G: FnMut(&str) -> Result<bool, SandboxError>,
    H: FnMut(&str) -> Result<Option<String>, SandboxError>,
{
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "file requires at least one path".to_string(),
        ));
    }

    let mut rendered = Vec::new();
    for arg in args {
        let resolved = resolve_sandbox_path(cwd, arg)?;
        let description = if let Some(target) = read_link(&resolved)? {
            format!("symbolic link to {target}")
        } else if is_dir(&resolved)? {
            "directory".to_string()
        } else {
            describe_bytes(&read_file(&resolved)?)
        };
        rendered.push(format!("{arg}: {description}"));
    }
    Ok(format!("{}\n", rendered.join("\n")).into_bytes())
}

pub(crate) fn readlink<H>(
    cwd: &str,
    args: &[String],
    mut read_link: H,
) -> Result<Vec<u8>, SandboxError>
where
    H: FnMut(&str) -> Result<Option<String>, SandboxError>,
{
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "readlink requires at least one path".to_string(),
        ));
    }
    let mut rendered = Vec::new();
    for arg in args {
        let resolved = resolve_sandbox_path(cwd, arg)?;
        let Some(target) = read_link(&resolved)? else {
            return Err(SandboxError::InvalidRequest(format!(
                "path is not a symbolic link: {resolved}"
            )));
        };
        rendered.push(target);
    }
    Ok(format!("{}\n", rendered.join("\n")).into_bytes())
}

pub(crate) fn ln_parse(cwd: &str, args: &[String]) -> Result<(String, String), SandboxError> {
    if args.len() != 3 || args[0] != "-s" {
        return Err(SandboxError::InvalidRequest(
            "ln currently supports only: ln -s TARGET LINK_NAME".to_string(),
        ));
    }
    Ok((
        resolve_sandbox_path(cwd, &args[1])?,
        resolve_sandbox_path(cwd, &args[2])?,
    ))
}

#[derive(Clone)]
struct TreeRoot {
    original: String,
    resolved: String,
}

struct TreeSpec {
    show_all: bool,
    max_depth: Option<usize>,
    roots: Vec<TreeRoot>,
}

#[derive(Clone)]
struct TreeChild {
    path: String,
    name: String,
}

fn parse_tree_spec(cwd: &str, args: &[String]) -> Result<TreeSpec, SandboxError> {
    let mut show_all = false;
    let mut max_depth = None;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-a" => {
                show_all = true;
                index += 1;
            }
            "-L" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "tree -L requires a non-negative integer".to_string(),
                    ));
                };
                max_depth = Some(value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest(
                        "tree -L requires a non-negative integer".to_string(),
                    )
                })?);
                index += 2;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "tree flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let mut roots = args[index..]
        .iter()
        .map(|arg| {
            Ok(TreeRoot {
                original: arg.clone(),
                resolved: resolve_sandbox_path(cwd, arg)?,
            })
        })
        .collect::<Result<Vec<_>, SandboxError>>()?;
    if roots.is_empty() {
        roots.push(TreeRoot {
            original: ".".to_string(),
            resolved: resolve_sandbox_path(cwd, ".")?,
        });
    }

    Ok(TreeSpec {
        show_all,
        max_depth,
        roots,
    })
}

fn collect_children(root: &str, candidates: &[String], show_all: bool) -> Vec<TreeChild> {
    let mut children = candidates
        .iter()
        .filter(|candidate| *candidate != root && candidate_within_root(candidate, root))
        .filter_map(|candidate| {
            let name = display_child_name(candidate);
            if !show_all && name.starts_with('.') {
                return None;
            }
            Some(TreeChild {
                path: candidate.clone(),
                name,
            })
        })
        .collect::<Vec<_>>();
    children.sort_by(|left, right| left.path.cmp(&right.path));
    children
}

fn render_tree_children<G>(
    rendered: &mut Vec<String>,
    children: &[TreeChild],
    candidates: &[String],
    is_dir: &mut G,
) -> Result<(), SandboxError>
where
    G: FnMut(&str) -> Result<bool, SandboxError>,
{
    for (index, child) in children.iter().enumerate() {
        let depth = child.path.matches('/').count().saturating_sub(1);
        let is_last = !children[index + 1..]
            .iter()
            .any(|next| depth_from_root(&next.path, parent_root(&child.path)) == 1);
        let prefix = if is_last { "└── " } else { "├── " };
        let indent = "│   ".repeat(depth.saturating_sub(1));
        let mut line = format!("{indent}{prefix}{}", child.name);
        if is_dir(&child.path)? {
            line.push('/');
        }
        rendered.push(line);
        let _ = candidates;
    }
    Ok(())
}

fn describe_bytes(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "empty".to_string();
    }
    if bytes.contains(&0) {
        return "data".to_string();
    }
    if let Ok(text) = std::str::from_utf8(bytes) {
        if text
            .chars()
            .all(|ch| ch.is_ascii_graphic() || ch.is_ascii_whitespace())
        {
            return "UTF-8 text".to_string();
        }
    }
    "data".to_string()
}

fn count_children(path: &str, candidates: &[String]) -> usize {
    candidates
        .iter()
        .filter(|candidate| {
            *candidate != path
                && candidate_within_root(candidate, path)
                && depth_from_root(candidate, path) == 1
        })
        .count()
}

fn candidate_within_root(candidate: &str, root: &str) -> bool {
    candidate == root
        || candidate
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn depth_from_root(candidate: &str, root: &str) -> usize {
    if candidate == root {
        return 0;
    }
    candidate
        .trim_start_matches(root)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

fn display_child_name(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn display_tree_path(root: &TreeRoot) -> String {
    if root.original.starts_with('/') {
        root.resolved.clone()
    } else {
        root.original.clone()
    }
}

fn parent_root(path: &str) -> &str {
    path.rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_detects_text() {
        let output = file(
            "/workspace",
            &["demo.txt".to_string()],
            |_| Ok(b"hello\n".to_vec()),
            |_| Ok(false),
            |_| Ok(None),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "demo.txt: UTF-8 text\n");
    }
}
