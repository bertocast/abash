use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut show_offset = true;
    let mut index = 0usize;
    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-An" => {
                show_offset = false;
                index += 1;
            }
            "-tx1" => index += 1,
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "od flag is not supported: {flag}"
                )));
            }
            _ => break,
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

    let mut rendered = Vec::new();
    for (chunk_index, chunk) in bytes.chunks(16).enumerate() {
        let hex = chunk
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        if show_offset {
            rendered.push(format!("{:07o} {hex}", chunk_index * 16));
        } else {
            rendered.push(hex);
        }
    }

    Ok(if rendered.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", rendered.join("\n")).into_bytes()
    })
}
