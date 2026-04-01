use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn rev<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let input = read_inputs(cwd, args, stdin, &mut read_file)?;
    Ok(render_lines(
        &input
            .lines()
            .map(|line| line.chars().rev().collect::<String>())
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn nl<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut index = 0usize;
    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-ba" => index += 1,
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "nl flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }
    let input = read_inputs(cwd, &args[index..], stdin, &mut read_file)?;
    Ok(render_lines(
        &input
            .lines()
            .enumerate()
            .map(|(line_number, line)| format!("{:>6}\t{line}", line_number + 1))
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn tac<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let input = read_inputs(cwd, args, stdin, &mut read_file)?;
    let mut lines = input.lines().map(ToString::to_string).collect::<Vec<_>>();
    lines.reverse();
    Ok(render_lines(&lines))
}

pub(crate) fn strings<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut min_len = 4usize;
    let mut index = 0usize;

    if let Some(flag) = args.first() {
        if flag == "-n" {
            let Some(value) = args.get(1) else {
                return Err(SandboxError::InvalidRequest(
                    "strings -n requires a positive integer".to_string(),
                ));
            };
            min_len = value.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest("strings -n requires a positive integer".to_string())
            })?;
            index = 2;
        } else if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "strings flag is not supported: {flag}"
            )));
        }
    }

    let bytes = read_input_bytes(cwd, &args[index..], stdin, &mut read_file)?;
    let mut current = String::new();
    let mut rendered = Vec::new();

    for byte in bytes {
        let ch = byte as char;
        if ch.is_ascii_graphic() || ch == ' ' {
            current.push(ch);
        } else if current.len() >= min_len {
            rendered.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.len() >= min_len {
        rendered.push(current);
    }

    Ok(render_lines(&rendered))
}

pub(crate) fn fold<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut width = 80usize;
    let mut index = 0usize;
    if let Some(flag) = args.first() {
        if flag == "-w" {
            let Some(value) = args.get(1) else {
                return Err(SandboxError::InvalidRequest(
                    "fold -w requires a positive integer".to_string(),
                ));
            };
            width = value.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest("fold -w requires a positive integer".to_string())
            })?;
            if width == 0 {
                return Err(SandboxError::InvalidRequest(
                    "fold -w requires a positive integer".to_string(),
                ));
            }
            index = 2;
        } else if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "fold flag is not supported: {flag}"
            )));
        }
    }

    let input = read_inputs(cwd, &args[index..], stdin, &mut read_file)?;
    let mut rendered = Vec::new();
    for line in input.lines() {
        let chars = line.chars().collect::<Vec<_>>();
        if chars.is_empty() {
            rendered.push(String::new());
            continue;
        }
        for chunk in chars.chunks(width) {
            rendered.push(chunk.iter().collect::<String>());
        }
    }
    Ok(render_lines(&rendered))
}

pub(crate) fn expand<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let (tabstop, paths) = parse_tabstop_args("expand", cwd, args)?;
    let input = read_inputs(cwd, &paths, stdin, &mut read_file)?;
    let rendered = input
        .lines()
        .map(|line| expand_tabs(line, tabstop))
        .collect::<Vec<_>>();
    Ok(render_lines(&rendered))
}

pub(crate) fn unexpand<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut all_spaces = false;
    let mut tabstop = 8usize;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-a" => {
                all_spaces = true;
                index += 1;
            }
            "-t" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "unexpand -t requires a positive integer".to_string(),
                    ));
                };
                tabstop = value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest(
                        "unexpand -t requires a positive integer".to_string(),
                    )
                })?;
                if tabstop == 0 {
                    return Err(SandboxError::InvalidRequest(
                        "unexpand -t requires a positive integer".to_string(),
                    ));
                }
                index += 2;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "unexpand flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let input = read_inputs(cwd, &args[index..], stdin, &mut read_file)?;
    let rendered = input
        .lines()
        .map(|line| unexpand_spaces(line, tabstop, all_spaces))
        .collect::<Vec<_>>();
    Ok(render_lines(&rendered))
}

fn parse_tabstop_args(
    command: &str,
    cwd: &str,
    args: &[String],
) -> Result<(usize, Vec<String>), SandboxError> {
    let mut tabstop = 8usize;
    let mut index = 0usize;
    if let Some(flag) = args.first() {
        if flag == "-t" {
            let Some(value) = args.get(1) else {
                return Err(SandboxError::InvalidRequest(format!(
                    "{command} -t requires a positive integer"
                )));
            };
            tabstop = value.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest(format!("{command} -t requires a positive integer"))
            })?;
            if tabstop == 0 {
                return Err(SandboxError::InvalidRequest(format!(
                    "{command} -t requires a positive integer"
                )));
            }
            index = 2;
        } else if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "{command} flag is not supported: {flag}"
            )));
        }
    }
    Ok((
        tabstop,
        args[index..]
            .iter()
            .map(|path| resolve_sandbox_path(cwd, path))
            .collect::<Result<Vec<_>, SandboxError>>()?,
    ))
}

fn read_inputs<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<String, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let bytes = read_input_bytes(cwd, args, stdin, read_file)?;
    String::from_utf8(bytes).map_err(|_| {
        SandboxError::InvalidRequest("command currently requires UTF-8 text input".to_string())
    })
}

fn read_input_bytes<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    if args.is_empty() {
        return Ok(stdin);
    }
    let mut output = Vec::new();
    for path in args {
        let resolved = resolve_sandbox_path(cwd, path)?;
        output.extend(read_file(&resolved)?);
    }
    Ok(output)
}

fn expand_tabs(line: &str, tabstop: usize) -> String {
    let mut rendered = String::new();
    let mut column = 0usize;
    for ch in line.chars() {
        if ch == '\t' {
            let spaces = tabstop - (column % tabstop);
            rendered.push_str(&" ".repeat(spaces));
            column += spaces;
        } else {
            rendered.push(ch);
            column += 1;
        }
    }
    rendered
}

fn unexpand_spaces(line: &str, tabstop: usize, all_spaces: bool) -> String {
    let chars = line.chars().collect::<Vec<_>>();
    let mut rendered = String::new();
    let mut column = 0usize;
    let mut index = 0usize;
    let mut can_convert = true;

    while index < chars.len() {
        let ch = chars[index];
        if ch != ' ' {
            if ch != '\t' {
                can_convert = false;
                column += 1;
            } else {
                column += tabstop - (column % tabstop);
            }
            rendered.push(ch);
            index += 1;
            continue;
        }

        if !all_spaces && !can_convert {
            rendered.push(' ');
            column += 1;
            index += 1;
            continue;
        }

        let mut run = 0usize;
        while index + run < chars.len() && chars[index + run] == ' ' {
            run += 1;
        }
        let mut remaining = run;
        while remaining > 0 {
            let next_tab = tabstop - (column % tabstop);
            if next_tab <= remaining && next_tab > 1 {
                rendered.push('\t');
                column += next_tab;
                remaining -= next_tab;
            } else {
                rendered.push(' ');
                column += 1;
                remaining -= 1;
            }
        }
        index += run;
    }

    rendered
}

fn render_lines(lines: &[String]) -> Vec<u8> {
    if lines.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", lines.join("\n")).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_and_unexpand_round_trip() {
        let expanded = expand(
            "/workspace",
            &["-t".to_string(), "4".to_string()],
            b"a\tb\n".to_vec(),
            |_| unreachable!(),
        )
        .unwrap();
        assert_eq!(String::from_utf8(expanded).unwrap(), "a   b\n");

        let unexpanded = unexpand(
            "/workspace",
            &["-t".to_string(), "4".to_string(), "-a".to_string()],
            b"a   b\n".to_vec(),
            |_| unreachable!(),
        )
        .unwrap();
        assert_eq!(String::from_utf8(unexpanded).unwrap(), "a\tb\n");
    }
}
