use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct DiffResult {
    pub output: Vec<u8>,
    pub identical: bool,
}

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    mut read_file: F,
) -> Result<DiffResult, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let (left_path, right_path) = parse_spec(cwd, args)?;
    let left = read_lines(read_file(&left_path)?)?;
    let right = read_lines(read_file(&right_path)?)?;

    if left == right {
        return Ok(DiffResult {
            output: Vec::new(),
            identical: true,
        });
    }

    let ops = diff_lines(&left, &right);
    let mut rendered = vec![
        format!("--- {}", args[0]),
        format!("+++ {}", args[1]),
        "@@".to_string(),
    ];
    for op in ops {
        match op {
            DiffOp::Equal(line) => rendered.push(format!(" {line}")),
            DiffOp::Remove(line) => rendered.push(format!("-{line}")),
            DiffOp::Add(line) => rendered.push(format!("+{line}")),
        }
    }

    Ok(DiffResult {
        output: format!("{}\n", rendered.join("\n")).into_bytes(),
        identical: false,
    })
}

enum DiffOp {
    Equal(String),
    Remove(String),
    Add(String),
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<(String, String), SandboxError> {
    if args.len() != 2 {
        return Err(SandboxError::InvalidRequest(
            "diff requires exactly two input files".to_string(),
        ));
    }
    Ok((
        resolve_sandbox_path(cwd, &args[0])?,
        resolve_sandbox_path(cwd, &args[1])?,
    ))
}

fn read_lines(contents: Vec<u8>) -> Result<Vec<String>, SandboxError> {
    let text = String::from_utf8(contents).map_err(|_| {
        SandboxError::InvalidRequest("diff currently requires UTF-8 text input".to_string())
    })?;
    Ok(text.lines().map(ToString::to_string).collect())
}

fn diff_lines(left: &[String], right: &[String]) -> Vec<DiffOp> {
    let mut dp = vec![vec![0usize; right.len() + 1]; left.len() + 1];
    for left_index in (0..left.len()).rev() {
        for right_index in (0..right.len()).rev() {
            dp[left_index][right_index] = if left[left_index] == right[right_index] {
                dp[left_index + 1][right_index + 1] + 1
            } else {
                dp[left_index + 1][right_index].max(dp[left_index][right_index + 1])
            };
        }
    }

    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut ops = Vec::new();

    while left_index < left.len() && right_index < right.len() {
        if left[left_index] == right[right_index] {
            ops.push(DiffOp::Equal(left[left_index].clone()));
            left_index += 1;
            right_index += 1;
        } else if dp[left_index + 1][right_index] >= dp[left_index][right_index + 1] {
            ops.push(DiffOp::Remove(left[left_index].clone()));
            left_index += 1;
        } else {
            ops.push(DiffOp::Add(right[right_index].clone()));
            right_index += 1;
        }
    }

    while left_index < left.len() {
        ops.push(DiffOp::Remove(left[left_index].clone()));
        left_index += 1;
    }
    while right_index < right.len() {
        ops.push(DiffOp::Add(right[right_index].clone()));
        right_index += 1;
    }

    ops
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_narrow_unified_diff() {
        let result = execute(
            "/workspace",
            &["a.txt".to_string(), "b.txt".to_string()],
            |path| match path {
                "/workspace/a.txt" => Ok(b"one\ntwo\n".to_vec()),
                "/workspace/b.txt" => Ok(b"one\nthree\n".to_vec()),
                _ => unreachable!(),
            },
        )
        .unwrap();

        assert!(!result.identical);
        assert_eq!(
            String::from_utf8(result.output).unwrap(),
            "--- a.txt\n+++ b.txt\n@@\n one\n-two\n+three\n"
        );
    }
}
