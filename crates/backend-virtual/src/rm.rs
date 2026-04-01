use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct RmSpec {
    pub recursive: bool,
    pub force: bool,
    pub paths: Vec<String>,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<RmSpec, SandboxError> {
    let mut recursive = false;
    let mut force = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if !flag.starts_with('-') || flag == "-" {
            break;
        }
        match flag.as_str() {
            "-r" | "-R" => recursive = true,
            "-f" => force = true,
            _ if flag.starts_with('-') => {
                for ch in flag.chars().skip(1) {
                    match ch {
                        'r' | 'R' => recursive = true,
                        'f' => force = true,
                        _ => {
                            return Err(SandboxError::InvalidRequest(format!(
                                "rm flag is not supported: {flag}"
                            )))
                        }
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    let paths = args[index..]
        .iter()
        .map(|path| resolve_sandbox_path(cwd, path))
        .collect::<Result<Vec<_>, SandboxError>>()?;

    if paths.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "rm requires at least one path".to_string(),
        ));
    }

    Ok(RmSpec {
        recursive,
        force,
        paths,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_flags() {
        let spec = parse_spec("/workspace", &["-rf".to_string(), "demo.txt".to_string()]).unwrap();
        assert!(spec.recursive);
        assert!(spec.force);
        assert_eq!(spec.paths, vec!["/workspace/demo.txt".to_string()]);
    }

    #[test]
    fn force_ignores_missing_paths() {
        let spec =
            parse_spec("/workspace", &["-f".to_string(), "missing.txt".to_string()]).unwrap();

        assert!(spec.force);
        assert_eq!(spec.paths, vec!["/workspace/missing.txt".to_string()]);
    }

    #[test]
    fn directories_require_recursive_flag() {
        let spec = parse_spec("/workspace", &["docs".to_string()]).unwrap();
        assert!(!spec.recursive);
        assert_eq!(spec.paths, vec!["/workspace/docs".to_string()]);
    }
}
