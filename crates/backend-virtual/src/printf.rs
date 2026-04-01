use abash_core::SandboxError;

pub(crate) fn execute(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    let Some(format) = args.first() else {
        return Err(SandboxError::InvalidRequest(
            "printf requires a format string".to_string(),
        ));
    };
    let tokens = parse_format(format)?;
    let placeholders = tokens
        .iter()
        .filter(|token| matches!(token, FormatToken::Placeholder))
        .count();

    let mut output = String::new();
    if placeholders == 0 {
        output.push_str(&render_once(&tokens, &[]));
    } else if args.len() == 1 {
        output.push_str(&render_once(&tokens, &[]));
    } else {
        let values = &args[1..];
        let mut index = 0usize;
        while index < values.len() {
            let end = (index + placeholders).min(values.len());
            output.push_str(&render_once(&tokens, &values[index..end]));
            index = end;
        }
    }

    Ok(output.into_bytes())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FormatToken {
    Literal(String),
    Placeholder,
}

fn parse_format(source: &str) -> Result<Vec<FormatToken>, SandboxError> {
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let chars = source.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        match chars[index] {
            '%' => {
                let Some(next) = chars.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "printf format string cannot end with %".to_string(),
                    ));
                };
                if !literal.is_empty() {
                    tokens.push(FormatToken::Literal(std::mem::take(&mut literal)));
                }
                match next {
                    '%' => literal.push('%'),
                    's' => tokens.push(FormatToken::Placeholder),
                    _ => {
                        return Err(SandboxError::InvalidRequest(format!(
                            "printf format specifier is not supported: %{next}"
                        )))
                    }
                }
                index += 2;
            }
            '\\' => {
                let Some(next) = chars.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "printf escape sequence is incomplete".to_string(),
                    ));
                };
                literal.push(match next {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '\\' => '\\',
                    '"' => '"',
                    '\'' => '\'',
                    _ => {
                        return Err(SandboxError::InvalidRequest(format!(
                            "printf escape sequence is not supported: \\{next}"
                        )))
                    }
                });
                index += 2;
            }
            ch => {
                literal.push(ch);
                index += 1;
            }
        }
    }

    if !literal.is_empty() {
        tokens.push(FormatToken::Literal(literal));
    }
    Ok(tokens)
}

fn render_once(tokens: &[FormatToken], values: &[String]) -> String {
    let mut output = String::new();
    let mut index = 0usize;
    for token in tokens {
        match token {
            FormatToken::Literal(text) => output.push_str(text),
            FormatToken::Placeholder => {
                if let Some(value) = values.get(index) {
                    output.push_str(value);
                }
                index += 1;
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_percent_s_and_repetition() {
        let rendered =
            execute(&["%s\\n".to_string(), "bert".to_string(), "ana".to_string()]).unwrap();

        assert_eq!(String::from_utf8(rendered).unwrap(), "bert\nana\n");
    }

    #[test]
    fn supports_literal_percent() {
        let rendered = execute(&["%% done\\n".to_string()]).unwrap();
        assert_eq!(String::from_utf8(rendered).unwrap(), "% done\n");
    }
}
