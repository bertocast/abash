use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct SplitSpec {
    pub line_count: usize,
    pub input: Option<String>,
    pub prefix: String,
}

pub(crate) fn parse_spec(cwd: &str, args: &[String]) -> Result<SplitSpec, SandboxError> {
    let mut line_count = 1000usize;
    let mut index = 0usize;

    if let Some(flag) = args.first() {
        if flag == "-l" {
            let Some(value) = args.get(1) else {
                return Err(SandboxError::InvalidRequest(
                    "split -l requires a positive integer".to_string(),
                ));
            };
            line_count = value.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest("split -l requires a positive integer".to_string())
            })?;
            if line_count == 0 {
                return Err(SandboxError::InvalidRequest(
                    "split -l requires a positive integer".to_string(),
                ));
            }
            index = 2;
        } else if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "split flag is not supported: {flag}"
            )));
        }
    }

    let remaining = &args[index..];
    let (input, prefix) = match remaining {
        [] => (None, resolve_sandbox_path(cwd, "x")?),
        [prefix] => (None, resolve_sandbox_path(cwd, prefix)?),
        [input, prefix] => (
            Some(resolve_sandbox_path(cwd, input)?),
            resolve_sandbox_path(cwd, prefix)?,
        ),
        _ => {
            return Err(SandboxError::InvalidRequest(
                "split supports only: split [-l N] [INPUT] [PREFIX]".to_string(),
            ))
        }
    };

    Ok(SplitSpec {
        line_count,
        input,
        prefix,
    })
}

pub(crate) fn suffix_for(index: usize) -> Result<String, SandboxError> {
    if index >= 26 * 26 {
        return Err(SandboxError::UnsupportedFeature(
            "split currently supports at most 676 output chunks".to_string(),
        ));
    }
    let first = ((index / 26) as u8 + b'a') as char;
    let second = ((index % 26) as u8 + b'a') as char;
    Ok(format!("{first}{second}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_suffixes() {
        assert_eq!(suffix_for(0).unwrap(), "aa");
        assert_eq!(suffix_for(27).unwrap(), "bb");
    }
}
