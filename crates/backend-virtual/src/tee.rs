use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct TeeSpec {
    pub append: bool,
    pub paths: Vec<String>,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<TeeSpec, SandboxError> {
    let mut append = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if !flag.starts_with('-') || flag == "-" {
            break;
        }
        match flag.as_str() {
            "-a" => append = true,
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "tee flag is not supported: {flag}"
                )))
            }
            _ => {}
        }
        index += 1;
    }

    let paths = args[index..]
        .iter()
        .map(|path| resolve_sandbox_path(cwd, path))
        .collect::<Result<Vec<_>, SandboxError>>()?;

    Ok(TeeSpec { append, paths })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_append_spec() {
        let spec = parse_spec(
            "/workspace",
            &[
                "-a".to_string(),
                "log.txt".to_string(),
                "copy.txt".to_string(),
            ],
        )
        .unwrap();

        assert!(spec.append);
        assert_eq!(
            spec.paths,
            vec![
                "/workspace/log.txt".to_string(),
                "/workspace/copy.txt".to_string(),
            ]
        );
    }
}
