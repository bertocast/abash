use abash_core::{resolve_sandbox_path, SandboxError};
use base64::Engine;

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut decode = false;
    let mut index = 0usize;
    if let Some(flag) = args.first() {
        match flag.as_str() {
            "-d" | "--decode" => {
                decode = true;
                index = 1;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "base64 flag is not supported: {flag}"
                )));
            }
            _ => {}
        }
    }

    let bytes = if index >= args.len() {
        stdin
    } else {
        let mut output = Vec::new();
        for path in &args[index..] {
            output.extend(read_file(&resolve_sandbox_path(cwd, path)?)?);
        }
        output
    };

    if decode {
        let text = String::from_utf8(bytes).map_err(|_| {
            SandboxError::InvalidRequest("base64 -d requires UTF-8 base64 text".to_string())
        })?;
        base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|error| SandboxError::InvalidRequest(format!("base64 decode failed: {error}")))
    } else {
        Ok(format!(
            "{}\n",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )
        .into_bytes())
    }
}
