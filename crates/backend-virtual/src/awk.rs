use abash_core::SandboxError;

pub(crate) fn execute<F>(
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_invocation(args)?;
    let program = parse_program(&spec.program)?;
    let inputs = read_inputs(&spec.paths, stdin, &mut read_file)?;
    Ok(render_output(&run_program(
        &program,
        spec.delimiter,
        &inputs,
    )))
}

struct Invocation {
    delimiter: Option<char>,
    program: String,
    paths: Vec<String>,
}

struct AwkProgram {
    pattern: Option<AwkPattern>,
    print_exprs: Vec<AwkExpr>,
}

enum AwkPattern {
    Equals(AwkExpr, AwkExpr),
    Contains(AwkExpr, AwkExpr),
}

enum AwkExpr {
    EntireLine,
    Field(usize),
    Counter(AwkCounter),
    Literal(String),
}

enum AwkCounter {
    Nf,
    Nr,
    Fnr,
}

struct AwkInput {
    text: String,
}

struct AwkRecord<'a> {
    line: &'a str,
    fields: Vec<&'a str>,
    nr: usize,
    fnr: usize,
}

fn parse_invocation(args: &[String]) -> Result<Invocation, SandboxError> {
    let mut delimiter = None;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if flag == "-F" {
            let Some(value) = args.get(index + 1) else {
                return Err(SandboxError::InvalidRequest(
                    "awk -F requires a single-character delimiter".to_string(),
                ));
            };
            delimiter = Some(parse_delimiter(value)?);
            index += 2;
            continue;
        }
        if let Some(value) = flag.strip_prefix("-F") {
            if value.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "awk -F requires a single-character delimiter".to_string(),
                ));
            }
            delimiter = Some(parse_delimiter(value)?);
            index += 1;
            continue;
        }
        if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "awk flag is not supported: {flag}"
            )));
        }
        break;
    }

    let Some(program) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "awk requires a program string".to_string(),
        ));
    };

    Ok(Invocation {
        delimiter,
        program: program.clone(),
        paths: args[index + 1..].to_vec(),
    })
}

fn parse_delimiter(value: &str) -> Result<char, SandboxError> {
    let mut chars = value.chars();
    let Some(delimiter) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "awk delimiter must be a single character".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(SandboxError::InvalidRequest(
            "awk delimiter must be a single character".to_string(),
        ));
    }
    Ok(delimiter)
}

fn read_inputs<F>(
    paths: &[String],
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Vec<AwkInput>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    if paths.is_empty() {
        return Ok(vec![AwkInput {
            text: bytes_to_text(stdin, "awk currently requires UTF-8 text input")?,
        }]);
    }

    let mut inputs = Vec::new();
    for path in paths {
        inputs.push(AwkInput {
            text: bytes_to_text(read_file(path)?, "awk currently requires UTF-8 text input")?,
        });
    }
    Ok(inputs)
}

fn bytes_to_text(contents: Vec<u8>, message: &str) -> Result<String, SandboxError> {
    String::from_utf8(contents).map_err(|_| SandboxError::InvalidRequest(message.to_string()))
}

fn parse_program(source: &str) -> Result<AwkProgram, SandboxError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "awk program must not be empty".to_string(),
        ));
    }

    let (pattern_text, action_text) = split_program_parts(source)?;
    let action = action_text.trim();
    let Some(remainder) = action.strip_prefix("print") else {
        return Err(SandboxError::InvalidRequest(
            "awk currently supports only print actions".to_string(),
        ));
    };

    let print_exprs = parse_print_exprs(remainder.trim())?;
    Ok(AwkProgram {
        pattern: parse_pattern(pattern_text.trim())?,
        print_exprs,
    })
}

fn split_program_parts(source: &str) -> Result<(&str, &str), SandboxError> {
    let Some(open_brace) = find_char_outside_quotes(source, '{') else {
        return Ok(("", source));
    };
    let Some(close_brace) = find_last_char_outside_quotes(source, '}') else {
        return Err(SandboxError::InvalidRequest(
            "awk action block is missing a closing brace".to_string(),
        ));
    };
    if close_brace <= open_brace {
        return Err(SandboxError::InvalidRequest(
            "awk action block is malformed".to_string(),
        ));
    }
    if !source[close_brace + 1..].trim().is_empty() {
        return Err(SandboxError::InvalidRequest(
            "awk does not support trailing tokens after the action block".to_string(),
        ));
    }
    Ok((
        source[..open_brace].trim(),
        source[open_brace + 1..close_brace].trim(),
    ))
}

fn parse_pattern(source: &str) -> Result<Option<AwkPattern>, SandboxError> {
    if source.is_empty() {
        return Ok(None);
    }
    if let Some(index) = find_operator_outside_quotes(source, "==") {
        return Ok(Some(AwkPattern::Equals(
            parse_expr(source[..index].trim())?,
            parse_expr(source[index + 2..].trim())?,
        )));
    }
    if let Some(index) = find_operator_outside_quotes(source, "~") {
        return Ok(Some(AwkPattern::Contains(
            parse_expr(source[..index].trim())?,
            parse_expr(source[index + 1..].trim())?,
        )));
    }
    Err(SandboxError::InvalidRequest(
        "awk currently supports only == and ~ patterns".to_string(),
    ))
}

fn parse_print_exprs(source: &str) -> Result<Vec<AwkExpr>, SandboxError> {
    if source.is_empty() {
        return Ok(vec![AwkExpr::EntireLine]);
    }

    let mut exprs = Vec::new();
    for part in split_outside_quotes(source, ',')? {
        exprs.push(parse_expr(part.trim())?);
    }
    Ok(exprs)
}

fn parse_expr(source: &str) -> Result<AwkExpr, SandboxError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "awk expression must not be empty".to_string(),
        ));
    }
    if let Some(literal) = parse_string_literal(source)? {
        return Ok(AwkExpr::Literal(literal));
    }
    if source == "$0" {
        return Ok(AwkExpr::EntireLine);
    }
    if let Some(field) = source.strip_prefix('$') {
        let index = field.parse::<usize>().map_err(|_| {
            SandboxError::InvalidRequest(
                "awk field references must be $0 or positive integers".to_string(),
            )
        })?;
        return if index == 0 {
            Ok(AwkExpr::EntireLine)
        } else {
            Ok(AwkExpr::Field(index))
        };
    }
    match source {
        "NF" => Ok(AwkExpr::Counter(AwkCounter::Nf)),
        "NR" => Ok(AwkExpr::Counter(AwkCounter::Nr)),
        "FNR" => Ok(AwkExpr::Counter(AwkCounter::Fnr)),
        _ => Err(SandboxError::InvalidRequest(format!(
            "awk expression is not supported: {source}"
        ))),
    }
}

fn parse_string_literal(source: &str) -> Result<Option<String>, SandboxError> {
    if source.len() < 2 {
        return Ok(None);
    }
    let quote = source.chars().next().unwrap_or_default();
    if quote != '"' && quote != '\'' {
        return Ok(None);
    }
    if !source.ends_with(quote) {
        return Err(SandboxError::InvalidRequest(
            "awk string literal is unterminated".to_string(),
        ));
    }
    Ok(Some(source[1..source.len() - 1].to_string()))
}

fn find_char_outside_quotes(source: &str, target: char) -> Option<usize> {
    let mut active_quote = None::<char>;
    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == target => return Some(index),
            _ => {}
        }
    }
    None
}

fn find_last_char_outside_quotes(source: &str, target: char) -> Option<usize> {
    let mut result = None;
    let mut active_quote = None::<char>;
    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == target => result = Some(index),
            _ => {}
        }
    }
    result
}

fn find_operator_outside_quotes(source: &str, operator: &str) -> Option<usize> {
    let mut active_quote = None::<char>;
    let mut index = 0usize;
    while index < source.len() {
        let ch = source[index..].chars().next()?;
        match active_quote {
            Some(quote) if ch == quote => {
                active_quote = None;
                index += ch.len_utf8();
            }
            None if ch == '\'' || ch == '"' => {
                active_quote = Some(ch);
                index += ch.len_utf8();
            }
            None if source[index..].starts_with(operator) => return Some(index),
            _ => index += ch.len_utf8(),
        }
    }
    None
}

fn split_outside_quotes(source: &str, delimiter: char) -> Result<Vec<&str>, SandboxError> {
    let mut parts = Vec::new();
    let mut active_quote = None::<char>;
    let mut start = 0usize;

    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == delimiter => {
                parts.push(source[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if active_quote.is_some() {
        return Err(SandboxError::InvalidRequest(
            "awk string literal is unterminated".to_string(),
        ));
    }

    parts.push(source[start..].trim());
    Ok(parts)
}

fn run_program(program: &AwkProgram, delimiter: Option<char>, inputs: &[AwkInput]) -> Vec<String> {
    let mut output = Vec::new();
    let mut nr = 0usize;

    for input in inputs {
        let mut fnr = 0usize;
        for line in input.text.lines() {
            nr += 1;
            fnr += 1;
            let record = AwkRecord {
                line,
                fields: split_fields(line, delimiter),
                nr,
                fnr,
            };
            if program_matches(program, &record) {
                output.push(render_print(program, &record));
            }
        }
    }

    output
}

fn split_fields(line: &str, delimiter: Option<char>) -> Vec<&str> {
    match delimiter {
        Some(delimiter) => line.split(delimiter).collect(),
        None => line.split_whitespace().collect(),
    }
}

fn program_matches(program: &AwkProgram, record: &AwkRecord<'_>) -> bool {
    match &program.pattern {
        None => true,
        Some(AwkPattern::Equals(left, right)) => {
            eval_expr(left, record) == eval_expr(right, record)
        }
        Some(AwkPattern::Contains(left, right)) => {
            eval_expr(left, record).contains(&eval_expr(right, record))
        }
    }
}

fn render_print(program: &AwkProgram, record: &AwkRecord<'_>) -> String {
    program
        .print_exprs
        .iter()
        .map(|expr| eval_expr(expr, record))
        .collect::<Vec<_>>()
        .join(" ")
}

fn eval_expr(expr: &AwkExpr, record: &AwkRecord<'_>) -> String {
    match expr {
        AwkExpr::EntireLine => record.line.to_string(),
        AwkExpr::Field(index) => record
            .fields
            .get(index - 1)
            .copied()
            .unwrap_or_default()
            .to_string(),
        AwkExpr::Counter(AwkCounter::Nf) => record.fields.len().to_string(),
        AwkExpr::Counter(AwkCounter::Nr) => record.nr.to_string(),
        AwkExpr::Counter(AwkCounter::Fnr) => record.fnr.to_string(),
        AwkExpr::Literal(value) => value.clone(),
    }
}

fn render_output(lines: &[String]) -> Vec<u8> {
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
    fn parses_patterned_print_program() {
        let program = parse_program(r#"$2 == "core" { print $1, NR, FNR, NF }"#).unwrap();

        let AwkPattern::Equals(_, _) = program.pattern.expect("pattern") else {
            panic!("expected equals pattern");
        };
        assert_eq!(program.print_exprs.len(), 4);
    }

    #[test]
    fn executes_records_with_field_filters_and_counters() {
        let program = parse_program(r#"$2 == "core" { print $1, NR, FNR, NF }"#).unwrap();
        let inputs = vec![
            AwkInput {
                text: "bert,core\nana,product\n".to_string(),
            },
            AwkInput {
                text: "cami,core\n".to_string(),
            },
        ];

        let output = run_program(&program, Some(','), &inputs);
        assert_eq!(
            output,
            vec!["bert 1 1 2".to_string(), "cami 3 1 2".to_string()]
        );
    }

    #[test]
    fn print_without_arguments_defaults_to_entire_line() {
        let program = parse_program("{ print }").unwrap();
        let inputs = vec![AwkInput {
            text: "bert core\n".to_string(),
        }];

        let output = run_program(&program, None, &inputs);
        assert_eq!(output, vec!["bert core".to_string()]);
    }
}
