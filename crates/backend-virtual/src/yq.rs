use abash_core::SandboxError;
use serde::Deserialize;
use serde_json::{Deserializer, Value};

use crate::jq;

pub(crate) fn execute<F>(
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<jq::Execution, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_invocation(args)?;
    let jq_args = to_jq_args(&spec);
    let jq_input = if spec.null_input {
        Vec::new()
    } else {
        transcode_inputs(&spec, stdin, &mut read_file)?
    };
    let result = jq::execute(&jq_args, jq_input, |_| {
        Err(SandboxError::BackendFailure(
            "yq internal file passthrough is unavailable".to_string(),
        ))
    })?;

    if spec.output_format == Format::Json || spec.raw_output {
        return Ok(result);
    }

    Ok(jq::Execution {
        stdout: render_yaml_output(result.stdout)?,
        exit_code: result.exit_code,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Yaml,
    Json,
}

struct Invocation {
    input_format: Format,
    output_format: Format,
    raw_output: bool,
    compact_output: bool,
    exit_status: bool,
    slurp: bool,
    null_input: bool,
    filter: String,
    paths: Vec<String>,
}

fn parse_invocation(args: &[String]) -> Result<Invocation, SandboxError> {
    let mut input_format = Format::Yaml;
    let mut output_format = Format::Yaml;
    let mut raw_output = false;
    let mut compact_output = false;
    let mut exit_status = false;
    let mut slurp = false;
    let mut null_input = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-p" | "--input-format" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "yq -p requires a format".to_string(),
                    ));
                };
                input_format = parse_format(value)?;
                index += 2;
                continue;
            }
            value if value.starts_with("--input-format=") => {
                input_format = parse_format(&value["--input-format=".len()..])?;
            }
            "-o" | "--output-format" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "yq -o requires a format".to_string(),
                    ));
                };
                output_format = parse_format(value)?;
                index += 2;
                continue;
            }
            value if value.starts_with("--output-format=") => {
                output_format = parse_format(&value["--output-format=".len()..])?;
            }
            "-r" | "--raw-output" => raw_output = true,
            "-c" | "--compact-output" => compact_output = true,
            "-e" | "--exit-status" => exit_status = true,
            "-s" | "--slurp" => slurp = true,
            "-n" | "--null-input" => null_input = true,
            "-" => break,
            value if value.starts_with("--") => {
                return Err(SandboxError::InvalidRequest(format!(
                    "yq flag is not supported: {value}"
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
                        other => {
                            return Err(SandboxError::InvalidRequest(format!(
                                "yq flag is not supported: -{other}"
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
            "yq requires a filter string".to_string(),
        ));
    };

    Ok(Invocation {
        input_format,
        output_format,
        raw_output,
        compact_output,
        exit_status,
        slurp,
        null_input,
        filter: filter.clone(),
        paths: args[index + 1..].to_vec(),
    })
}

fn parse_format(value: &str) -> Result<Format, SandboxError> {
    match value {
        "yaml" | "yml" => Ok(Format::Yaml),
        "json" => Ok(Format::Json),
        other => Err(SandboxError::InvalidRequest(format!(
            "yq format is not supported: {other}"
        ))),
    }
}

fn to_jq_args(spec: &Invocation) -> Vec<String> {
    let mut args = Vec::new();
    if spec.raw_output {
        args.push("-r".to_string());
    }
    if spec.compact_output {
        args.push("-c".to_string());
    }
    if spec.exit_status {
        args.push("-e".to_string());
    }
    if spec.slurp {
        args.push("-s".to_string());
    }
    if spec.null_input {
        args.push("-n".to_string());
    }
    args.push(spec.filter.clone());
    args
}

fn transcode_inputs<F>(
    spec: &Invocation,
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut output = Vec::new();
    if spec.paths.is_empty() {
        append_source_values(&mut output, &stdin, spec.input_format)?;
        return Ok(output);
    }

    for path in &spec.paths {
        let bytes = if path == "-" {
            stdin.clone()
        } else {
            read_file(path)?
        };
        append_source_values(&mut output, &bytes, spec.input_format)?;
    }
    Ok(output)
}

fn append_source_values(
    output: &mut Vec<u8>,
    bytes: &[u8],
    format: Format,
) -> Result<(), SandboxError> {
    match format {
        Format::Json => output.extend_from_slice(bytes),
        Format::Yaml => {
            let text = std::str::from_utf8(bytes).map_err(|_| {
                SandboxError::InvalidRequest("yq currently requires UTF-8 YAML input".to_string())
            })?;
            for document in serde_yaml::Deserializer::from_str(text) {
                let value = Value::deserialize(document).map_err(|error| {
                    SandboxError::InvalidRequest(format!("yq could not parse YAML input: {error}"))
                })?;
                let rendered = serde_json::to_string(&value).map_err(|error| {
                    SandboxError::BackendFailure(format!(
                        "yq could not transcode YAML to JSON: {error}"
                    ))
                })?;
                output.extend_from_slice(rendered.as_bytes());
                output.push(b'\n');
            }
        }
    }
    Ok(())
}

fn render_yaml_output(stdout: Vec<u8>) -> Result<Vec<u8>, SandboxError> {
    let text = String::from_utf8(stdout).map_err(|_| {
        SandboxError::BackendFailure("yq could not decode intermediate JSON output".to_string())
    })?;
    let mut rendered = String::new();
    for value in Deserializer::from_str(&text).into_iter::<Value>() {
        rendered.push_str(&render_yaml_value(&value.map_err(|error| {
            SandboxError::BackendFailure(format!(
                "yq could not parse intermediate JSON output: {error}"
            ))
        })?)?);
    }
    Ok(rendered.into_bytes())
}

fn render_yaml_value(value: &Value) -> Result<String, SandboxError> {
    Ok(match value {
        Value::Null => "null\n".to_string(),
        Value::Bool(flag) => format!("{flag}\n"),
        Value::Number(number) => format!("{number}\n"),
        Value::String(text) => format!("{text}\n"),
        Value::Array(_) | Value::Object(_) => {
            let mut rendered = serde_yaml::to_string(value).map_err(|error| {
                SandboxError::BackendFailure(format!("yq could not render YAML output: {error}"))
            })?;
            if let Some(stripped) = rendered.strip_prefix("---\n") {
                rendered = stripped.to_string();
            }
            if !rendered.ends_with('\n') {
                rendered.push('\n');
            }
            rendered
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_formats_and_flags() {
        let spec = parse_invocation(&[
            "-p".to_string(),
            "json".to_string(),
            "-o".to_string(),
            "yaml".to_string(),
            "-rce".to_string(),
            ".name".to_string(),
        ])
        .unwrap();

        assert_eq!(spec.input_format, Format::Json);
        assert_eq!(spec.output_format, Format::Yaml);
        assert!(spec.raw_output);
        assert!(spec.compact_output);
        assert!(spec.exit_status);
    }

    #[test]
    fn transcodes_yaml_documents_to_json_stream() {
        let mut output = Vec::new();
        append_source_values(&mut output, b"name: bert\n---\nname: ana\n", Format::Yaml).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("{\"name\":\"bert\"}"));
        assert!(text.contains("{\"name\":\"ana\"}"));
    }

    #[test]
    fn renders_yaml_scalars_without_json_quotes() {
        assert_eq!(
            render_yaml_value(&Value::String("bert".to_string())).unwrap(),
            "bert\n"
        );
    }
}
