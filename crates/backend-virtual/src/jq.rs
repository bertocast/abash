use abash_core::SandboxError;
use serde_json::{Deserializer, Value};

use crate::jq_engine::{parse_program, run_program};

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
