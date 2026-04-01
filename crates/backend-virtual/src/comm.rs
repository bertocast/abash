use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    let left = read_lines(read_file(&spec.left)?)?;
    let right = read_lines(read_file(&spec.right)?)?;
    let mut rendered = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() || right_index < right.len() {
        match (left.get(left_index), right.get(right_index)) {
            (Some(left_line), Some(right_line)) if left_line == right_line => {
                if !spec.suppress3 {
                    rendered.push(prefixed_line(
                        right_line,
                        (!spec.suppress1 as usize) + (!spec.suppress2 as usize),
                    ));
                }
                left_index += 1;
                right_index += 1;
            }
            (Some(left_line), Some(right_line)) if left_line < right_line => {
                if !spec.suppress1 {
                    rendered.push(prefixed_line(left_line, 0));
                }
                left_index += 1;
            }
            (Some(_), Some(right_line)) => {
                if !spec.suppress2 {
                    rendered.push(prefixed_line(right_line, !spec.suppress1 as usize));
                }
                right_index += 1;
            }
            (Some(left_line), None) => {
                if !spec.suppress1 {
                    rendered.push(prefixed_line(left_line, 0));
                }
                left_index += 1;
            }
            (None, Some(right_line)) => {
                if !spec.suppress2 {
                    rendered.push(prefixed_line(right_line, !spec.suppress1 as usize));
                }
                right_index += 1;
            }
            (None, None) => break,
        }
    }

    Ok(if rendered.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", rendered.join("\n")).into_bytes()
    })
}

struct CommSpec {
    suppress1: bool,
    suppress2: bool,
    suppress3: bool,
    left: String,
    right: String,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<CommSpec, SandboxError> {
    let mut suppress1 = false;
    let mut suppress2 = false;
    let mut suppress3 = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-1" => suppress1 = true,
            "-2" => suppress2 = true,
            "-3" => suppress3 = true,
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "comm flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
        index += 1;
    }

    if args.len().saturating_sub(index) != 2 {
        return Err(SandboxError::InvalidRequest(
            "comm requires exactly two input files".to_string(),
        ));
    }

    Ok(CommSpec {
        suppress1,
        suppress2,
        suppress3,
        left: resolve_sandbox_path(cwd, &args[index])?,
        right: resolve_sandbox_path(cwd, &args[index + 1])?,
    })
}

fn read_lines(contents: Vec<u8>) -> Result<Vec<String>, SandboxError> {
    let text = String::from_utf8(contents).map_err(|_| {
        SandboxError::InvalidRequest("comm currently requires UTF-8 text input".to_string())
    })?;
    Ok(text.lines().map(ToString::to_string).collect())
}

fn prefixed_line(line: &str, prefix_tabs: usize) -> String {
    format!("{}{}", "\t".repeat(prefix_tabs), line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_columns() {
        let output = execute(
            "/workspace",
            &[
                "-3".to_string(),
                "left.txt".to_string(),
                "right.txt".to_string(),
            ],
            |path| match path {
                "/workspace/left.txt" => Ok(b"a\ncommon\n".to_vec()),
                "/workspace/right.txt" => Ok(b"b\ncommon\n".to_vec()),
                _ => unreachable!(),
            },
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "a\n\tb\n");
    }
}
