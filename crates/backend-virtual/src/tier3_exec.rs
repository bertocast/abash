use std::time::Duration;

use abash_core::SandboxError;

pub(crate) enum ShellProgram {
    Inline(String),
    File(String),
}

pub(crate) fn parse_timeout_ms(value: &str) -> Result<u64, SandboxError> {
    let seconds = value.parse::<f64>().map_err(|_| {
        SandboxError::InvalidRequest("timeout duration must be numeric seconds".to_string())
    })?;
    if seconds.is_sign_negative() {
        return Err(SandboxError::InvalidRequest(
            "timeout duration must be non-negative".to_string(),
        ));
    }
    Ok(Duration::from_secs_f64(seconds).as_millis() as u64)
}

pub(crate) fn parse_shell_program(
    args: &[String],
    command: &str,
) -> Result<ShellProgram, SandboxError> {
    match args {
        [] => Err(SandboxError::InvalidRequest(format!(
            "{command} requires -c <script> or a script path"
        ))),
        [flag, script] if flag == "-c" => Ok(ShellProgram::Inline(script.clone())),
        [flag] if flag.starts_with('-') => Err(SandboxError::InvalidRequest(format!(
            "{command} flag is not supported: {flag}"
        ))),
        [path] => Ok(ShellProgram::File(path.clone())),
        _ => Err(SandboxError::InvalidRequest(format!(
            "{command} currently supports only -c <script> or one script path"
        ))),
    }
}

pub(crate) fn render_time(stderr: &[u8], elapsed: Duration) -> Vec<u8> {
    let mut rendered = stderr.to_vec();
    rendered.extend_from_slice(format!("real {:.3}s\n", elapsed.as_secs_f64()).as_bytes());
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inline_and_file_programs() {
        assert!(matches!(
            parse_shell_program(&["-c".into(), "echo hi".into()], "bash").unwrap(),
            ShellProgram::Inline(_)
        ));
        assert!(matches!(
            parse_shell_program(&["/workspace/demo.sh".into()], "sh").unwrap(),
            ShellProgram::File(_)
        ));
    }
}
