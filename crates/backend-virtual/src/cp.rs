use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct CpSpec {
    pub recursive: bool,
    pub sources: Vec<String>,
    pub destination: String,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<CpSpec, SandboxError> {
    let mut recursive = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if !flag.starts_with('-') || flag == "-" {
            break;
        }
        match flag.as_str() {
            "-r" | "-R" => recursive = true,
            _ if flag.starts_with('-') => {
                for ch in flag.chars().skip(1) {
                    match ch {
                        'r' | 'R' => recursive = true,
                        _ => {
                            return Err(SandboxError::InvalidRequest(format!(
                                "cp flag is not supported: {flag}"
                            )))
                        }
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    let remaining = &args[index..];
    if remaining.len() < 2 {
        return Err(SandboxError::InvalidRequest(
            "cp requires at least one source and one destination".to_string(),
        ));
    }

    let mut resolved = remaining
        .iter()
        .map(|path| resolve_sandbox_path(cwd, path))
        .collect::<Result<Vec<_>, SandboxError>>()?;
    let destination = resolved.pop().expect("destination required");

    Ok(CpSpec {
        recursive,
        sources: resolved,
        destination,
    })
}

pub(crate) fn path_basename(path: &str) -> Result<&str, SandboxError> {
    if path == "/" || path == "/workspace" {
        return Err(SandboxError::InvalidRequest(format!(
            "cp cannot use directory root as a copy leaf: {path}"
        )));
    }
    path.rsplit('/').next().ok_or_else(|| {
        SandboxError::InvalidRequest(format!("cp could not determine basename for: {path}"))
    })
}

pub(crate) fn join_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else {
        format!("{parent}/{child}")
    }
}

pub(crate) fn descendant_paths(source: &str, candidates: &[String]) -> Vec<String> {
    let prefix = format!("{source}/");
    let mut paths = candidates
        .iter()
        .filter(|candidate| candidate.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_recursive_spec() {
        let spec = parse_spec(
            "/workspace",
            &["-r".to_string(), "src".to_string(), "dest".to_string()],
        )
        .unwrap();

        assert!(spec.recursive);
        assert_eq!(spec.sources, vec!["/workspace/src".to_string()]);
        assert_eq!(spec.destination, "/workspace/dest".to_string());
    }

    #[test]
    fn descendant_paths_are_sorted() {
        let paths = descendant_paths(
            "/workspace/src",
            &[
                "/workspace/src/b.txt".to_string(),
                "/workspace/src".to_string(),
                "/workspace/src/a.txt".to_string(),
                "/workspace/other.txt".to_string(),
            ],
        );

        assert_eq!(
            paths,
            vec![
                "/workspace/src/a.txt".to_string(),
                "/workspace/src/b.txt".to_string(),
            ]
        );
    }
}
