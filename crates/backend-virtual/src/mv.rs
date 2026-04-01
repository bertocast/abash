use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct MvSpec {
    pub sources: Vec<String>,
    pub destination: String,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<MvSpec, SandboxError> {
    if args.iter().any(|arg| arg.starts_with('-')) {
        return Err(SandboxError::InvalidRequest(
            "mv flags are not supported".to_string(),
        ));
    }
    if args.len() < 2 {
        return Err(SandboxError::InvalidRequest(
            "mv requires at least one source and one destination".to_string(),
        ));
    }

    let mut resolved = args
        .iter()
        .map(|path| resolve_sandbox_path(cwd, path))
        .collect::<Result<Vec<_>, SandboxError>>()?;
    let destination = resolved.pop().expect("destination required");

    Ok(MvSpec {
        sources: resolved,
        destination,
    })
}

pub(crate) fn path_within_root(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(&(root.to_string() + "/"))
            .is_some_and(|suffix| !suffix.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiple_sources() {
        let spec = parse_spec(
            "/workspace",
            &["a.txt".to_string(), "b.txt".to_string(), "dest".to_string()],
        )
        .unwrap();

        assert_eq!(
            spec.sources,
            vec![
                "/workspace/a.txt".to_string(),
                "/workspace/b.txt".to_string(),
            ]
        );
        assert_eq!(spec.destination, "/workspace/dest".to_string());
    }

    #[test]
    fn detects_descendant_paths() {
        assert!(path_within_root("/workspace/a/b", "/workspace/a"));
        assert!(!path_within_root("/workspace/ab", "/workspace/a"));
    }
}
