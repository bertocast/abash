use abash_core::SandboxError;

pub(crate) fn execute(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    let (start, step, end) = parse_bounds(args)?;
    let values = render_sequence(start, step, end);
    if values.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(format!("{}\n", values.join("\n")).into_bytes())
    }
}

fn parse_bounds(args: &[String]) -> Result<(i64, i64, i64), SandboxError> {
    match args {
        [end] => {
            let end = parse_i64(end, "seq expects integer arguments")?;
            Ok((1, 1, end))
        }
        [start, end] => {
            let start = parse_i64(start, "seq expects integer arguments")?;
            let end = parse_i64(end, "seq expects integer arguments")?;
            let step = if start <= end { 1 } else { -1 };
            Ok((start, step, end))
        }
        [start, step, end] => {
            let start = parse_i64(start, "seq expects integer arguments")?;
            let step = parse_i64(step, "seq step must be a non-zero integer")?;
            if step == 0 {
                return Err(SandboxError::InvalidRequest(
                    "seq step must be a non-zero integer".to_string(),
                ));
            }
            let end = parse_i64(end, "seq expects integer arguments")?;
            Ok((start, step, end))
        }
        [] => Err(SandboxError::InvalidRequest(
            "seq requires 1, 2, or 3 integer arguments".to_string(),
        )),
        _ => Err(SandboxError::InvalidRequest(
            "seq requires 1, 2, or 3 integer arguments".to_string(),
        )),
    }
}

fn parse_i64(value: &str, message: &str) -> Result<i64, SandboxError> {
    value
        .parse::<i64>()
        .map_err(|_| SandboxError::InvalidRequest(message.to_string()))
}

fn render_sequence(start: i64, step: i64, end: i64) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = start;

    if step > 0 {
        while current <= end {
            values.push(current.to_string());
            let Some(next) = current.checked_add(step) else {
                break;
            };
            current = next;
        }
    } else {
        while current >= end {
            values.push(current.to_string());
            let Some(next) = current.checked_add(step) else {
                break;
            };
            current = next;
        }
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_default_increment() {
        let rendered = execute(&["3".to_string()]).unwrap();
        assert_eq!(String::from_utf8(rendered).unwrap(), "1\n2\n3\n");
    }

    #[test]
    fn supports_explicit_step() {
        let rendered = execute(&["2".to_string(), "2".to_string(), "6".to_string()]).unwrap();
        assert_eq!(String::from_utf8(rendered).unwrap(), "2\n4\n6\n");
    }
}
