use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct RmdirSpec {
    pub parents: bool,
    pub paths: Vec<String>,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<RmdirSpec, SandboxError> {
    let mut parents = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-p" => {
                parents = true;
                index += 1;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "rmdir flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    if index >= args.len() {
        return Err(SandboxError::InvalidRequest(
            "rmdir requires at least one path".to_string(),
        ));
    }

    Ok(RmdirSpec {
        parents,
        paths: args[index..]
            .iter()
            .map(|path| resolve_sandbox_path(cwd, path))
            .collect::<Result<Vec<_>, SandboxError>>()?,
    })
}

pub(crate) fn parent_path(path: &str) -> Option<String> {
    if path == "/" || path == "/workspace" {
        return None;
    }
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(0) => Some("/".to_string()),
        Some(index) => Some(trimmed[..index].to_string()),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_parent_flag() {
        let spec =
            parse_spec("/workspace", &["-p".to_string(), "demo/nested".to_string()]).unwrap();
        assert!(spec.parents);
        assert_eq!(spec.paths, vec!["/workspace/demo/nested".to_string()]);
    }

    #[test]
    fn stops_parent_walk_at_workspace() {
        assert_eq!(
            parent_path("/workspace/demo/nested"),
            Some("/workspace/demo".to_string())
        );
        assert_eq!(parent_path("/workspace"), None);
    }
}
