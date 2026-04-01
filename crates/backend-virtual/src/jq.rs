use abash_core::SandboxError;
use serde_json::{Deserializer, Value};

pub(crate) struct Execution {
    pub(crate) stdout: Vec<u8>,
    pub(crate) exit_code: i32,
}

pub(crate) fn execute<F>(
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Execution, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_invocation(args)?;
    let program = parse_program(&spec.filter)?;
    let inputs = read_inputs(&spec, stdin, &mut read_file)?;
    let outputs = run_program(&program, &inputs);
    Ok(Execution {
        exit_code: render_exit_code(&outputs, spec.exit_status),
        stdout: render_outputs(&outputs, &spec)?,
    })
}

struct Invocation {
    raw_output: bool,
    compact_output: bool,
    exit_status: bool,
    slurp: bool,
    null_input: bool,
    sort_keys: bool,
    filter: String,
    paths: Vec<String>,
}

struct Program {
    filter: Filter,
}

enum Filter {
    Path(PathExpr),
    Pipe(Box<Filter>, Box<Filter>),
    Comma(Vec<Filter>),
}

struct PathExpr {
    ops: Vec<PathOp>,
}

enum PathOp {
    Key(String),
    Index(isize),
    Slice(Option<isize>, Option<isize>),
    Iterate,
}

fn parse_invocation(args: &[String]) -> Result<Invocation, SandboxError> {
    let mut raw_output = false;
    let mut compact_output = false;
    let mut exit_status = false;
    let mut slurp = false;
    let mut null_input = false;
    let mut sort_keys = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-r" | "--raw-output" => raw_output = true,
            "-c" | "--compact-output" => compact_output = true,
            "-e" | "--exit-status" => exit_status = true,
            "-s" | "--slurp" => slurp = true,
            "-n" | "--null-input" => null_input = true,
            "-S" | "--sort-keys" => sort_keys = true,
            "-" => break,
            value if value.starts_with("--") => {
                return Err(SandboxError::InvalidRequest(format!(
                    "jq flag is not supported: {value}"
                )));
            }
            value if value.starts_with('-') && value.len() > 1 => {
                for short in value[1..].chars() {
                    match short {
                        'r' => raw_output = true,
                        'c' => compact_output = true,
                        'e' => exit_status = true,
                        's' => slurp = true,
                        'n' => null_input = true,
                        'S' => sort_keys = true,
                        other => {
                            return Err(SandboxError::InvalidRequest(format!(
                                "jq flag is not supported: -{other}"
                            )));
                        }
                    }
                }
            }
            _ => break,
        }
        index += 1;
    }

    let Some(filter) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "jq requires a filter string".to_string(),
        ));
    };

    Ok(Invocation {
        raw_output,
        compact_output,
        exit_status,
        slurp,
        null_input,
        sort_keys,
        filter: filter.clone(),
        paths: args[index + 1..].to_vec(),
    })
}

fn parse_program(source: &str) -> Result<Program, SandboxError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "jq filter must not be empty".to_string(),
        ));
    }

    Ok(Program {
        filter: parse_pipe_filter(source)?,
    })
}

fn split_top_level<'a>(source: &'a str, delimiter: char) -> Result<Vec<&'a str>, SandboxError> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut bracket_depth = 0usize;
    let mut active_quote = None::<char>;

    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == '[' => bracket_depth += 1,
            None if ch == ']' => {
                if bracket_depth == 0 {
                    return Err(SandboxError::InvalidRequest(
                        "jq filter has an unmatched closing bracket".to_string(),
                    ));
                }
                bracket_depth -= 1;
            }
            None if bracket_depth == 0 && ch == delimiter => {
                parts.push(source[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if active_quote.is_some() {
        return Err(SandboxError::InvalidRequest(
            "jq filter has an unterminated string literal".to_string(),
        ));
    }
    if bracket_depth != 0 {
        return Err(SandboxError::InvalidRequest(
            "jq filter has an unterminated bracket expression".to_string(),
        ));
    }

    parts.push(source[start..].trim());
    Ok(parts)
}

fn parse_path_expr(source: &str) -> Result<PathExpr, SandboxError> {
    if !source.starts_with('.') {
        return Err(SandboxError::InvalidRequest(format!(
            "jq filter segment is not supported: {source}"
        )));
    }

    let bytes = source.as_bytes();
    let mut index = 1usize;
    let mut ops = Vec::new();

    while index < bytes.len() {
        match bytes[index] {
            b'.' => {
                index += 1;
            }
            b'[' => {
                let end = source[index + 1..]
                    .find(']')
                    .map(|offset| index + 1 + offset)
                    .ok_or_else(|| {
                        SandboxError::InvalidRequest(
                            "jq bracket expression is unterminated".to_string(),
                        )
                    })?;
                let content = source[index + 1..end].trim();
                if content.is_empty() {
                    ops.push(PathOp::Iterate);
                } else if let Some((start, finish)) = parse_slice(content)? {
                    ops.push(PathOp::Slice(start, finish));
                } else {
                    let parsed = content.parse::<isize>().map_err(|_| {
                        SandboxError::InvalidRequest(format!(
                            "jq bracket expression is not supported: [{content}]"
                        ))
                    })?;
                    ops.push(PathOp::Index(parsed));
                }
                index = end + 1;
            }
            _ => {
                let start = index;
                while index < bytes.len() {
                    let ch = bytes[index] as char;
                    if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                        index += 1;
                    } else {
                        break;
                    }
                }
                if start == index {
                    return Err(SandboxError::InvalidRequest(format!(
                        "jq filter token is not supported near: {}",
                        &source[index..]
                    )));
                }
                ops.push(PathOp::Key(source[start..index].to_string()));
            }
        }
    }

    Ok(PathExpr { ops })
}

fn parse_comma_filter(source: &str) -> Result<Filter, SandboxError> {
    let parts = split_top_level(source, ',')?;
    if parts.len() == 1 {
        return parse_path_filter(parts[0].trim());
    }
    let mut filters = Vec::new();
    for part in parts {
        filters.push(parse_path_filter(part.trim())?);
    }
    Ok(Filter::Comma(filters))
}

fn parse_pipe_filter(source: &str) -> Result<Filter, SandboxError> {
    let parts = split_top_level(source, '|')?;
    let mut filter = parse_comma_filter(parts[0].trim())?;
    for part in parts.iter().skip(1) {
        filter = Filter::Pipe(Box::new(filter), Box::new(parse_comma_filter(part.trim())?));
    }
    Ok(filter)
}

fn parse_path_filter(source: &str) -> Result<Filter, SandboxError> {
    Ok(Filter::Path(parse_path_expr(source)?))
}

fn parse_slice(source: &str) -> Result<Option<(Option<isize>, Option<isize>)>, SandboxError> {
    let Some((left, right)) = source.split_once(':') else {
        return Ok(None);
    };
    Ok(Some((
        parse_optional_index(left.trim())?,
        parse_optional_index(right.trim())?,
    )))
}

fn parse_optional_index(source: &str) -> Result<Option<isize>, SandboxError> {
    if source.is_empty() {
        return Ok(None);
    }
    source
        .parse::<isize>()
        .map(Some)
        .map_err(|_| SandboxError::InvalidRequest(format!("jq slice bound is not valid: {source}")))
}

fn read_inputs<F>(
    spec: &Invocation,
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Vec<Value>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    if spec.null_input {
        return if spec.slurp {
            Ok(vec![Value::Array(Vec::new())])
        } else {
            Ok(vec![Value::Null])
        };
    }

    let mut values = Vec::new();
    if spec.paths.is_empty() {
        values.extend(parse_json_stream(stdin)?);
    } else {
        for path in &spec.paths {
            let bytes = if path == "-" {
                stdin.clone()
            } else {
                read_file(path)?
            };
            values.extend(parse_json_stream(bytes)?);
        }
    }

    if spec.slurp {
        Ok(vec![Value::Array(values)])
    } else {
        Ok(values)
    }
}

fn parse_json_stream(contents: Vec<u8>) -> Result<Vec<Value>, SandboxError> {
    let text = String::from_utf8(contents).map_err(|_| {
        SandboxError::InvalidRequest("jq currently requires UTF-8 JSON input".to_string())
    })?;
    let mut values = Vec::new();
    for value in Deserializer::from_str(&text).into_iter::<Value>() {
        values.push(value.map_err(|error| {
            SandboxError::InvalidRequest(format!("jq could not parse JSON input: {error}"))
        })?);
    }
    Ok(values)
}

fn run_program(program: &Program, inputs: &[Value]) -> Vec<Value> {
    let mut outputs = Vec::new();
    for input in inputs {
        outputs.extend(eval_filter(&program.filter, input));
    }
    outputs
}

fn eval_filter(filter: &Filter, input: &Value) -> Vec<Value> {
    match filter {
        Filter::Path(path) => apply_path(input, &path.ops),
        Filter::Pipe(left, right) => {
            let mut outputs = Vec::new();
            for value in eval_filter(left, input) {
                outputs.extend(eval_filter(right, &value));
            }
            outputs
        }
        Filter::Comma(filters) => {
            let mut outputs = Vec::new();
            for filter in filters {
                outputs.extend(eval_filter(filter, input));
            }
            outputs
        }
    }
}

fn apply_path(value: &Value, ops: &[PathOp]) -> Vec<Value> {
    let mut current = vec![value.clone()];
    for op in ops {
        let mut next = Vec::new();
        for value in current {
            next.extend(apply_op(&value, op));
        }
        current = next;
    }
    current
}

fn apply_op(value: &Value, op: &PathOp) -> Vec<Value> {
    match op {
        PathOp::Key(key) => vec![match value {
            Value::Object(map) => map.get(key).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        }],
        PathOp::Index(index) => vec![index_value(value, *index)],
        PathOp::Slice(start, finish) => vec![slice_value(value, *start, *finish)],
        PathOp::Iterate => match value {
            Value::Array(items) => items.clone(),
            Value::Object(map) => map.values().cloned().collect(),
            _ => Vec::new(),
        },
    }
}

fn index_value(value: &Value, index: isize) -> Value {
    match value {
        Value::Array(items) => normalized_index(items.len(), index)
            .and_then(|resolved| items.get(resolved).cloned())
            .unwrap_or(Value::Null),
        Value::String(text) => normalized_index(text.chars().count(), index)
            .and_then(|resolved| text.chars().nth(resolved))
            .map(|ch| Value::String(ch.to_string()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn slice_value(value: &Value, start: Option<isize>, finish: Option<isize>) -> Value {
    match value {
        Value::Array(items) => {
            let (start, finish) = normalized_bounds(items.len(), start, finish);
            Value::Array(items[start..finish].to_vec())
        }
        Value::String(text) => {
            let chars = text.chars().collect::<Vec<_>>();
            let (start, finish) = normalized_bounds(chars.len(), start, finish);
            Value::String(chars[start..finish].iter().collect())
        }
        _ => Value::Null,
    }
}

fn normalized_index(length: usize, index: isize) -> Option<usize> {
    if length == 0 {
        return None;
    }
    let length = length as isize;
    let resolved = if index < 0 { length + index } else { index };
    if resolved < 0 || resolved >= length {
        None
    } else {
        Some(resolved as usize)
    }
}

fn normalized_bounds(length: usize, start: Option<isize>, finish: Option<isize>) -> (usize, usize) {
    let length = length as isize;
    let start = normalize_bound(start.unwrap_or(0), length);
    let finish = normalize_bound(finish.unwrap_or(length), length);
    if finish < start {
        (start as usize, start as usize)
    } else {
        (start as usize, finish as usize)
    }
}

fn normalize_bound(bound: isize, length: isize) -> isize {
    let resolved = if bound < 0 { length + bound } else { bound };
    resolved.clamp(0, length)
}

fn render_outputs(values: &[Value], spec: &Invocation) -> Result<Vec<u8>, SandboxError> {
    let mut rendered = String::new();
    for value in values {
        if spec.raw_output && matches!(value, Value::String(_)) {
            rendered.push_str(value.as_str().unwrap_or_default());
        } else if spec.compact_output {
            rendered.push_str(&serde_json::to_string(value).map_err(|error| {
                SandboxError::BackendFailure(format!("jq could not render JSON output: {error}"))
            })?);
        } else {
            rendered.push_str(&serde_json::to_string_pretty(value).map_err(|error| {
                SandboxError::BackendFailure(format!("jq could not render JSON output: {error}"))
            })?);
        }
        rendered.push('\n');
    }
    let _ = spec.sort_keys;
    Ok(rendered.into_bytes())
}

fn render_exit_code(values: &[Value], exit_status: bool) -> i32 {
    if !exit_status {
        return 0;
    }
    match values.last() {
        Some(Value::Null) | Some(Value::Bool(false)) | None => 1,
        Some(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_path_pipeline_and_slice() {
        let program = parse_program(".items[] | .name, .[-2:]").unwrap();
        let Filter::Pipe(_, right) = program.filter else {
            panic!("expected pipe filter");
        };
        let Filter::Comma(filters) = right.as_ref() else {
            panic!("expected comma filter");
        };
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn evaluates_object_keys_indexes_and_iteration() {
        let program = parse_program(".items[] | .name").unwrap();
        let input = serde_json::json!({"items": [{"name": "bert"}, {"name": "ana"}]});

        let output = run_program(&program, &[input]);
        assert_eq!(
            output,
            vec![
                Value::String("bert".to_string()),
                Value::String("ana".to_string())
            ]
        );
    }

    #[test]
    fn slices_arrays_and_strings() {
        assert_eq!(
            slice_value(&serde_json::json!([0, 1, 2, 3, 4]), Some(1), Some(3)),
            serde_json::json!([1, 2])
        );
        assert_eq!(
            slice_value(&Value::String("hello".to_string()), Some(1), Some(4)),
            Value::String("ell".to_string())
        );
    }
}
