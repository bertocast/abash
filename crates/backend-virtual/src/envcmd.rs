use std::collections::BTreeMap;

use abash_core::SandboxError;

#[derive(Debug)]
pub(crate) struct EnvSpec {
    pub clear_env: bool,
    pub assignments: BTreeMap<String, String>,
    pub command: Option<String>,
    pub args: Vec<String>,
}

pub(crate) fn parse_spec(args: &[String]) -> Result<EnvSpec, SandboxError> {
    let mut clear_env = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-i" => {
                clear_env = true;
                index += 1;
            }
            _ if flag.starts_with('-') && !flag.contains('=') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "env flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let mut assignments = BTreeMap::new();
    while let Some(token) = args.get(index) {
        let Some((name, value)) = token.split_once('=') else {
            break;
        };
        validate_name(name)?;
        assignments.insert(name.to_string(), value.to_string());
        index += 1;
    }

    let command = args.get(index).cloned();
    let command_args = if command.is_some() {
        args[index + 1..].to_vec()
    } else {
        Vec::new()
    };

    Ok(EnvSpec {
        clear_env,
        assignments,
        command,
        args: command_args,
    })
}

fn validate_name(name: &str) -> Result<(), SandboxError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "env assignment name must not be empty".to_string(),
        ));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(SandboxError::InvalidRequest(format!(
            "env assignment name is invalid: {name}"
        )));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(SandboxError::InvalidRequest(format!(
            "env assignment name is invalid: {name}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_clear_and_assignments() {
        let spec = parse_spec(&[
            "-i".to_string(),
            "FOO=bar".to_string(),
            "printenv".to_string(),
            "FOO".to_string(),
        ])
        .unwrap();

        assert!(spec.clear_env);
        assert_eq!(spec.assignments.get("FOO"), Some(&"bar".to_string()));
        assert_eq!(spec.command.as_deref(), Some("printenv"));
        assert_eq!(spec.args, vec!["FOO".to_string()]);
    }

    #[test]
    fn rejects_invalid_names() {
        let error = parse_spec(&["1BAD=value".to_string()]).unwrap_err();
        assert_eq!(error.kind(), abash_core::ErrorKind::InvalidRequest);
    }
}
