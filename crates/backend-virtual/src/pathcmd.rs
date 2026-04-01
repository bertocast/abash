use abash_core::SandboxError;

pub(crate) fn dirname(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "dirname requires at least one path".to_string(),
        ));
    }

    Ok(format!(
        "{}\n",
        args.iter()
            .map(|arg| dirname_one(arg))
            .collect::<Vec<_>>()
            .join("\n")
    )
    .into_bytes())
}

pub(crate) fn basename(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "basename requires at least one path".to_string(),
        ));
    }

    Ok(format!(
        "{}\n",
        args.iter()
            .map(|arg| basename_one(arg))
            .collect::<Vec<_>>()
            .join("\n")
    )
    .into_bytes())
}

fn dirname_one(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let trimmed = trim_trailing_slashes(path);
    if trimmed == "/" {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        Some(0) => "/".to_string(),
        Some(index) => trimmed[..index].to_string(),
        None => ".".to_string(),
    }
}

fn basename_one(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    let trimmed = trim_trailing_slashes(path);
    if trimmed == "/" {
        return "/".to_string();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

fn trim_trailing_slashes(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/"
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_dirnames() {
        let output =
            dirname(&["/workspace/docs/readme.txt".to_string(), "demo".to_string()]).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "/workspace/docs\n.\n");
    }

    #[test]
    fn renders_basenames() {
        let output = basename(&[
            "/workspace/docs/readme.txt".to_string(),
            "demo/".to_string(),
        ])
        .unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "readme.txt\ndemo\n");
    }
}
