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
    let spec = parse_spec(cwd, args)?;
    let input = read_input(&spec.paths, stdin, &mut read_file)?;
    let rows = parse_rows(&input, spec.separator);
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let column_count = rows.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0usize; column_count];
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let rendered = rows
        .into_iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, cell)| {
                    if index + 1 == column_count || index + 1 == row.len() {
                        cell.clone()
                    } else {
                        format!("{cell:<width$}", width = widths[index])
                    }
                })
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>();

    Ok(format!("{}\n", rendered.join("\n")).into_bytes())
}

struct ColumnSpec {
    separator: Option<char>,
    paths: Vec<String>,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<ColumnSpec, SandboxError> {
    let mut separator = None;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-t" => index += 1,
            "-s" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "column -s requires a single-character separator".to_string(),
                    ));
                };
                separator = Some(parse_separator(value)?);
                index += 2;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "column flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    Ok(ColumnSpec {
        separator,
        paths: args[index..]
            .iter()
            .map(|path| resolve_sandbox_path(cwd, path))
            .collect::<Result<Vec<_>, SandboxError>>()?,
    })
}

fn parse_separator(value: &str) -> Result<char, SandboxError> {
    let mut chars = value.chars();
    let Some(separator) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "column -s requires a single-character separator".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(SandboxError::InvalidRequest(
            "column -s requires a single-character separator".to_string(),
        ));
    }
    Ok(separator)
}

fn read_input<F>(
    paths: &[String],
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<String, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let bytes = if paths.is_empty() {
        stdin
    } else {
        let mut combined = Vec::new();
        for path in paths {
            combined.extend(read_file(path)?);
        }
        combined
    };
    String::from_utf8(bytes).map_err(|_| {
        SandboxError::InvalidRequest("column currently requires UTF-8 text input".to_string())
    })
}

fn parse_rows(input: &str, separator: Option<char>) -> Vec<Vec<String>> {
    input
        .lines()
        .map(|line| match separator {
            Some(separator) => line.split(separator).map(ToString::to_string).collect(),
            None => line.split_whitespace().map(ToString::to_string).collect(),
        })
        .filter(|row: &Vec<String>| !row.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_columns() {
        let output = execute(
            "/workspace",
            &["-t".to_string()],
            b"a bb\nccc d\n".to_vec(),
            |_| unreachable!(),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "a    bb\nccc  d\n");
    }
}
