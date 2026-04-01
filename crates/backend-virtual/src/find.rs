use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn execute<F, G>(
    cwd: &str,
    args: &[String],
    mut list_paths: F,
    mut is_dir: G,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut() -> Result<Vec<String>, SandboxError>,
    G: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    let candidates = list_paths()?;
    let mut rendered = Vec::new();

    for candidate in candidates {
        if !spec
            .roots
            .iter()
            .any(|root| candidate_within_root(&candidate, &root.resolved))
        {
            continue;
        }

        if spec
            .max_depth
            .is_some_and(|limit| !within_depth_limit(&candidate, &spec.roots, limit))
        {
            continue;
        }

        if let Some(expected) = spec.path_type {
            let candidate_is_dir = is_dir(&candidate)?;
            if candidate_is_dir != matches!(expected, PathType::Directory) {
                continue;
            }
        }

        if let Some(pattern) = &spec.name_pattern {
            let name = path_name(&candidate);
            if !glob_matches(pattern, name) {
                continue;
            }
        }

        if let Some(root) = spec
            .roots
            .iter()
            .find(|root| candidate_within_root(&candidate, &root.resolved))
        {
            rendered.push(display_path(cwd, root, &candidate));
        }
    }

    if rendered.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(format!("{}\n", rendered.join("\n")).into_bytes())
    }
}

#[derive(Clone)]
struct FindRoot {
    original: String,
    resolved: String,
}

struct FindSpec {
    roots: Vec<FindRoot>,
    name_pattern: Option<String>,
    path_type: Option<PathType>,
    max_depth: Option<usize>,
}

#[derive(Clone, Copy)]
enum PathType {
    File,
    Directory,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<FindSpec, SandboxError> {
    let mut roots = Vec::new();
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        if arg.starts_with('-') {
            break;
        }
        roots.push(FindRoot {
            original: arg.clone(),
            resolved: resolve_sandbox_path(cwd, arg)?,
        });
        index += 1;
    }

    if roots.is_empty() {
        roots.push(FindRoot {
            original: ".".to_string(),
            resolved: resolve_sandbox_path(cwd, ".")?,
        });
    }

    let mut name_pattern = None;
    let mut path_type = None;
    let mut max_depth = None;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-name" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "find -name requires a pattern".to_string(),
                    ));
                };
                name_pattern = Some(value.clone());
                index += 2;
            }
            "-type" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "find -type requires f or d".to_string(),
                    ));
                };
                path_type = Some(parse_path_type(value)?);
                index += 2;
            }
            "-maxdepth" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "find -maxdepth requires a non-negative integer".to_string(),
                    ));
                };
                max_depth = Some(value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest(
                        "find -maxdepth requires a non-negative integer".to_string(),
                    )
                })?);
                index += 2;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(format!(
                    "find flag is not supported: {flag}"
                )))
            }
        }
    }

    Ok(FindSpec {
        roots,
        name_pattern,
        path_type,
        max_depth,
    })
}

fn parse_path_type(value: &str) -> Result<PathType, SandboxError> {
    match value {
        "f" => Ok(PathType::File),
        "d" => Ok(PathType::Directory),
        _ => Err(SandboxError::InvalidRequest(
            "find -type requires f or d".to_string(),
        )),
    }
}

fn candidate_within_root(candidate: &str, root: &str) -> bool {
    candidate == root
        || root == "/"
        || candidate
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn within_depth_limit(candidate: &str, roots: &[FindRoot], limit: usize) -> bool {
    roots.iter().any(|root| {
        if !candidate_within_root(candidate, &root.resolved) {
            return false;
        }
        depth_from_root(candidate, &root.resolved) <= limit
    })
}

fn depth_from_root(candidate: &str, root: &str) -> usize {
    if candidate == root {
        return 0;
    }

    let candidate_depth = candidate
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count();
    let root_depth = root
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count();
    candidate_depth.saturating_sub(root_depth)
}

fn path_name(path: &str) -> &str {
    if path == "/" {
        "/"
    } else {
        path.rsplit('/').next().unwrap_or(path)
    }
}

fn display_path(cwd: &str, root: &FindRoot, candidate: &str) -> String {
    if root.original.starts_with('/') {
        return candidate.to_string();
    }
    if candidate == root.resolved {
        return root.original.clone();
    }

    let relative = if cwd == "/" {
        candidate.trim_start_matches('/').to_string()
    } else {
        candidate
            .strip_prefix(&(cwd.to_string() + "/"))
            .unwrap_or(candidate)
            .to_string()
    };

    if root.original == "." {
        format!("./{relative}")
    } else {
        relative
    }
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    glob_matches_chars(
        &pattern.chars().collect::<Vec<_>>(),
        0,
        &text.chars().collect::<Vec<_>>(),
        0,
    )
}

fn glob_matches_chars(
    pattern: &[char],
    pattern_index: usize,
    text: &[char],
    text_index: usize,
) -> bool {
    if pattern_index == pattern.len() {
        return text_index == text.len();
    }

    match pattern[pattern_index] {
        '*' => {
            for index in text_index..=text.len() {
                if glob_matches_chars(pattern, pattern_index + 1, text, index) {
                    return true;
                }
            }
            false
        }
        '?' => {
            text_index < text.len()
                && glob_matches_chars(pattern, pattern_index + 1, text, text_index + 1)
        }
        '[' => {
            let Some((matched, next_index)) =
                match_bracket_class(pattern, pattern_index, text, text_index)
            else {
                return false;
            };
            matched && glob_matches_chars(pattern, next_index, text, text_index + 1)
        }
        ch => {
            text.get(text_index) == Some(&ch)
                && glob_matches_chars(pattern, pattern_index + 1, text, text_index + 1)
        }
    }
}

fn match_bracket_class(
    pattern: &[char],
    pattern_index: usize,
    text: &[char],
    text_index: usize,
) -> Option<(bool, usize)> {
    let text_char = *text.get(text_index)?;
    let mut index = pattern_index + 1;
    let mut matched = false;

    while index < pattern.len() {
        match pattern[index] {
            ']' => return Some((matched, index + 1)),
            start
                if index + 2 < pattern.len()
                    && pattern[index + 1] == '-'
                    && pattern[index + 2] != ']' =>
            {
                let end = pattern[index + 2];
                if start <= text_char && text_char <= end {
                    matched = true;
                }
                index += 3;
            }
            ch => {
                if ch == text_char {
                    matched = true;
                }
                index += 1;
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_current_directory_root() {
        let spec = parse_spec("/workspace", &[]).unwrap();
        assert_eq!(spec.roots[0].original, ".");
        assert_eq!(spec.roots[0].resolved, "/workspace");
    }

    #[test]
    fn filters_by_name_type_and_depth() {
        let output = execute(
            "/workspace",
            &[
                ".".to_string(),
                "-name".to_string(),
                "*.txt".to_string(),
                "-type".to_string(),
                "f".to_string(),
                "-maxdepth".to_string(),
                "1".to_string(),
            ],
            || {
                Ok(vec![
                    "/".to_string(),
                    "/workspace".to_string(),
                    "/workspace/docs".to_string(),
                    "/workspace/docs/demo.txt".to_string(),
                    "/workspace/demo.txt".to_string(),
                ])
            },
            |path| Ok(matches!(path, "/" | "/workspace" | "/workspace/docs")),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "./demo.txt\n");
    }

    #[test]
    fn relative_roots_preserve_relative_display_paths() {
        let root = FindRoot {
            original: "docs".to_string(),
            resolved: "/workspace/docs".to_string(),
        };

        assert_eq!(display_path("/workspace", &root, "/workspace/docs"), "docs");
        assert_eq!(
            display_path("/workspace", &root, "/workspace/docs/demo.txt"),
            "docs/demo.txt"
        );
    }
}
