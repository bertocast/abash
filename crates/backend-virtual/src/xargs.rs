use abash_core::SandboxError;

pub(crate) struct XargsSpec {
    pub max_args: Option<usize>,
    pub command: String,
    pub initial_args: Vec<String>,
}

pub(crate) fn parse_spec(args: &[String]) -> Result<XargsSpec, SandboxError> {
    let mut max_args = None;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-n" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xargs -n requires a positive integer".to_string(),
                    ));
                };
                let parsed = value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest("xargs -n requires a positive integer".to_string())
                })?;
                if parsed == 0 {
                    return Err(SandboxError::InvalidRequest(
                        "xargs -n requires a positive integer".to_string(),
                    ));
                }
                max_args = Some(parsed);
                index += 2;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xargs flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let command = args
        .get(index)
        .cloned()
        .unwrap_or_else(|| "echo".to_string());
    let initial_args = if args.get(index).is_some() {
        args[index + 1..].to_vec()
    } else {
        Vec::new()
    };

    Ok(XargsSpec {
        max_args,
        command,
        initial_args,
    })
}

pub(crate) fn tokenize_input(stdin: &[u8]) -> Result<Vec<String>, SandboxError> {
    let input = String::from_utf8(stdin.to_vec()).map_err(|_| {
        SandboxError::InvalidRequest("xargs currently requires UTF-8 text input".to_string())
    })?;
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match quote {
            Some('\'') => {
                if ch == '\'' {
                    quote = None;
                } else {
                    current.push(ch);
                }
            }
            Some('"') => {
                if ch == '"' {
                    quote = None;
                } else if ch == '\\' {
                    escaped = true;
                } else {
                    current.push(ch);
                }
            }
            _ => match ch {
                '\'' | '"' => quote = Some(ch),
                '\\' => escaped = true,
                ch if ch.is_whitespace() => {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                }
                _ => current.push(ch),
            },
        }
    }

    if escaped || quote.is_some() {
        return Err(SandboxError::InvalidRequest(
            "xargs input contains an unterminated escape or quote".to_string(),
        ));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

pub(crate) fn build_invocations(spec: &XargsSpec, tokens: &[String]) -> Vec<Vec<String>> {
    let mut batches = Vec::new();

    if tokens.is_empty() {
        batches.push(spec.initial_args.clone());
        return batches;
    }

    match spec.max_args {
        Some(max_args) => {
            for chunk in tokens.chunks(max_args) {
                let mut args = spec.initial_args.clone();
                args.extend(chunk.iter().cloned());
                batches.push(args);
            }
        }
        None => {
            let mut args = spec.initial_args.clone();
            args.extend(tokens.iter().cloned());
            batches.push(args);
        }
    }

    batches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_quotes_and_batches() {
        let spec = parse_spec(&["-n".to_string(), "2".to_string(), "echo".to_string()]).unwrap();
        let tokens = tokenize_input(br#"a "two words" c"#).unwrap();
        let batches = build_invocations(&spec, &tokens);

        assert_eq!(
            batches,
            vec![
                vec!["a".to_string(), "two words".to_string()],
                vec!["c".to_string()],
            ]
        );
    }
}
