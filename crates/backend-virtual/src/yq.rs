use abash_core::SandboxError;
use serde::Deserialize;
use serde_json::{Deserializer, Map, Number, Value};
use toml::Value as TomlValue;

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

    match spec.output_format {
        Format::Json => Ok(result),
        Format::Yaml => Ok(jq::Execution {
            stdout: render_yaml_output(result.stdout)?,
            exit_code: result.exit_code,
        }),
        Format::Toml => Ok(jq::Execution {
            stdout: render_toml_output(result.stdout)?,
            exit_code: result.exit_code,
        }),
        Format::Csv => Ok(jq::Execution {
            stdout: render_csv_output(result.stdout)?,
            exit_code: result.exit_code,
        }),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Yaml,
    Json,
    Toml,
    Csv,
}

struct Invocation {
    input_format: Format,
    input_format_explicit: bool,
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
    let mut input_format_explicit = false;
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
                input_format_explicit = true;
                index += 2;
                continue;
            }
            value if value.starts_with("--input-format=") => {
                input_format = parse_format(&value["--input-format=".len()..])?;
                input_format_explicit = true;
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
        input_format_explicit,
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
        "toml" => Ok(Format::Toml),
        "csv" | "tsv" => Ok(Format::Csv),
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
        let format = effective_input_format(spec, path);
        append_source_values(&mut output, &bytes, format)?;
    }
    Ok(output)
}

fn effective_input_format(spec: &Invocation, path: &str) -> Format {
    if spec.input_format_explicit || path == "-" {
        return spec.input_format;
    }

    if let Some((_, extension)) = path.rsplit_once('.') {
        match extension {
            "json" => return Format::Json,
            "toml" => return Format::Toml,
            "csv" | "tsv" => return Format::Csv,
            "yaml" | "yml" => return Format::Yaml,
            _ => {}
        }
    }

    spec.input_format
}

fn append_source_values(
    output: &mut Vec<u8>,
    bytes: &[u8],
    format: Format,
) -> Result<(), SandboxError> {
    match format {
        Format::Json => output.extend_from_slice(bytes),
        Format::Yaml => {
            let text = decode_utf8(bytes, "yq currently requires UTF-8 YAML input")?;
            for document in serde_norway::Deserializer::from_str(text) {
                let value = Value::deserialize(document).map_err(|error| {
                    SandboxError::InvalidRequest(format!("yq could not parse YAML input: {error}"))
                })?;
                append_json_value(output, &value)?;
            }
        }
        Format::Toml => {
            let text = decode_utf8(bytes, "yq currently requires UTF-8 TOML input")?;
            let value = toml::from_str::<TomlValue>(text).map_err(|error| {
                SandboxError::InvalidRequest(format!("yq could not parse TOML input: {error}"))
            })?;
            append_json_value(output, &toml_to_json(&value))?;
        }
        Format::Csv => {
            let text = decode_utf8(bytes, "yq currently requires UTF-8 CSV input")?;
            append_json_value(output, &csv_to_json(text)?)?;
        }
    }
    Ok(())
}

fn append_json_value(output: &mut Vec<u8>, value: &Value) -> Result<(), SandboxError> {
    let rendered = serde_json::to_string(value).map_err(|error| {
        SandboxError::BackendFailure(format!("yq could not transcode input to JSON: {error}"))
    })?;
    output.extend_from_slice(rendered.as_bytes());
    output.push(b'\n');
    Ok(())
}

fn decode_utf8<'a>(bytes: &'a [u8], message: &str) -> Result<&'a str, SandboxError> {
    std::str::from_utf8(bytes).map_err(|_| SandboxError::InvalidRequest(message.to_string()))
}

fn render_yaml_output(stdout: Vec<u8>) -> Result<Vec<u8>, SandboxError> {
    let values = parse_json_output(stdout)?;
    let mut rendered = String::new();
    for value in &values {
        rendered.push_str(&render_yaml_value(value)?);
    }
    Ok(rendered.into_bytes())
}

fn render_toml_output(stdout: Vec<u8>) -> Result<Vec<u8>, SandboxError> {
    let values = parse_json_output(stdout)?;
    let mut rendered = String::new();
    for value in &values {
        let toml_value = json_to_toml(value)?;
        let mut chunk = if matches!(toml_value, TomlValue::Table(_)) {
            toml::to_string_pretty(&toml_value).map_err(|error| {
                SandboxError::BackendFailure(format!("yq could not render TOML output: {error}"))
            })?
        } else {
            toml_value.to_string()
        };
        if !chunk.ends_with('\n') {
            chunk.push('\n');
        }
        rendered.push_str(&chunk);
    }
    Ok(rendered.into_bytes())
}

fn render_csv_output(stdout: Vec<u8>) -> Result<Vec<u8>, SandboxError> {
    let values = parse_json_output(stdout)?;
    let root = if values.len() == 1 {
        values
            .into_iter()
            .next()
            .unwrap_or(Value::Array(Vec::new()))
    } else {
        Value::Array(values)
    };
    Ok(format_csv(&csv_rows_from_json(&root)?).into_bytes())
}

fn parse_json_output(stdout: Vec<u8>) -> Result<Vec<Value>, SandboxError> {
    let text = String::from_utf8(stdout).map_err(|_| {
        SandboxError::BackendFailure("yq could not decode intermediate JSON output".to_string())
    })?;
    let mut values = Vec::new();
    for value in Deserializer::from_str(&text).into_iter::<Value>() {
        values.push(value.map_err(|error| {
            SandboxError::BackendFailure(format!(
                "yq could not parse intermediate JSON output: {error}"
            ))
        })?);
    }
    Ok(values)
}

fn render_yaml_value(value: &Value) -> Result<String, SandboxError> {
    Ok(match value {
        Value::Null => "null\n".to_string(),
        Value::Bool(flag) => format!("{flag}\n"),
        Value::Number(number) => format!("{number}\n"),
        Value::String(text) => format!("{text}\n"),
        Value::Array(_) | Value::Object(_) => {
            let mut rendered = serde_norway::to_string(value).map_err(|error| {
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

fn csv_to_json(input: &str) -> Result<Value, SandboxError> {
    let delimiter = detect_csv_delimiter(input);
    let records = parse_records(input, delimiter)?;
    if records.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }

    let headers = records[0].clone();
    let width = headers.len();
    let mut rows = Vec::new();
    for mut row in records.into_iter().skip(1) {
        if row.len() < width {
            row.resize(width, String::new());
        } else if row.len() > width {
            row.truncate(width);
        }
        let mut object = Map::new();
        for (index, header) in headers.iter().enumerate() {
            object.insert(header.clone(), csv_field_to_json(&row[index]));
        }
        rows.push(Value::Object(object));
    }
    Ok(Value::Array(rows))
}

fn detect_csv_delimiter(input: &str) -> char {
    let first_line = input.lines().next().unwrap_or_default();
    let candidates = [',', '\t', ';'];
    candidates
        .into_iter()
        .max_by_key(|delimiter| first_line.matches(*delimiter).count())
        .filter(|delimiter| first_line.contains(*delimiter))
        .unwrap_or(',')
}

fn parse_records(input: &str, delimiter: char) -> Result<Vec<Vec<String>>, SandboxError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quotes = false;
    let mut saw_any = false;

    while let Some(ch) = chars.next() {
        saw_any = true;
        if in_quotes {
            match ch {
                '"' => {
                    if chars.peek() == Some(&'"') {
                        field.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                }
                _ => field.push(ch),
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            '\n' => {
                row.push(std::mem::take(&mut field));
                if !(row.len() == 1 && row[0].is_empty() && records.is_empty()) {
                    records.push(std::mem::take(&mut row));
                } else {
                    row.clear();
                }
            }
            '\r' => {}
            value if value == delimiter => {
                row.push(std::mem::take(&mut field));
            }
            _ => field.push(ch),
        }
    }

    if in_quotes {
        return Err(SandboxError::InvalidRequest(
            "yq CSV input has an unterminated quoted field".to_string(),
        ));
    }

    if saw_any && (!field.is_empty() || !row.is_empty()) {
        row.push(field);
        records.push(row);
    }

    Ok(records)
}

fn csv_field_to_json(value: &str) -> Value {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Value::Bool(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Value::Bool(false)
    } else if let Ok(number) = trimmed.parse::<i64>() {
        Value::Number(number.into())
    } else if let Ok(number) = trimmed.parse::<f64>() {
        Number::from_f64(number)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string()))
    } else {
        Value::String(value.to_string())
    }
}

fn csv_rows_from_json(value: &Value) -> Result<Vec<Vec<String>>, SandboxError> {
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                return Ok(Vec::new());
            }
            if items.iter().all(Value::is_object) {
                let mut headers = Vec::new();
                for item in items {
                    let Value::Object(object) = item else {
                        unreachable!();
                    };
                    for key in object.keys() {
                        if !headers.contains(key) {
                            headers.push(key.clone());
                        }
                    }
                }
                let mut rows = vec![headers.clone()];
                for item in items {
                    let Value::Object(object) = item else {
                        unreachable!();
                    };
                    rows.push(
                        headers
                            .iter()
                            .map(|key| csv_scalar_string(object.get(key).unwrap_or(&Value::Null)))
                            .collect(),
                    );
                }
                Ok(rows)
            } else {
                let mut rows = vec![vec!["value".to_string()]];
                rows.extend(items.iter().map(|item| vec![csv_scalar_string(item)]));
                Ok(rows)
            }
        }
        Value::Object(object) => {
            let headers = object.keys().cloned().collect::<Vec<_>>();
            let row = headers
                .iter()
                .map(|key| csv_scalar_string(object.get(key).unwrap_or(&Value::Null)))
                .collect::<Vec<_>>();
            Ok(vec![headers, row])
        }
        _ => Ok(vec![
            vec!["value".to_string()],
            vec![csv_scalar_string(value)],
        ]),
    }
}

fn format_csv(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut rendered = String::new();
    for row in rows {
        rendered.push_str(
            &row.iter()
                .map(|value| quote_csv_field(value))
                .collect::<Vec<_>>()
                .join(","),
        );
        rendered.push('\n');
    }
    rendered
}

fn quote_csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn csv_scalar_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn toml_to_json(value: &TomlValue) -> Value {
    match value {
        TomlValue::String(text) => Value::String(text.clone()),
        TomlValue::Integer(number) => Value::Number((*number).into()),
        TomlValue::Float(number) => Number::from_f64(*number)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        TomlValue::Boolean(flag) => Value::Bool(*flag),
        TomlValue::Datetime(datetime) => Value::String(datetime.to_string()),
        TomlValue::Array(items) => Value::Array(items.iter().map(toml_to_json).collect()),
        TomlValue::Table(table) => Value::Object(
            table
                .iter()
                .map(|(key, value)| (key.clone(), toml_to_json(value)))
                .collect(),
        ),
    }
}

fn json_to_toml(value: &Value) -> Result<TomlValue, SandboxError> {
    match value {
        Value::Null => Err(SandboxError::InvalidRequest(
            "yq cannot render null as TOML".to_string(),
        )),
        Value::Bool(flag) => Ok(TomlValue::Boolean(*flag)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(TomlValue::Integer(integer))
            } else if let Some(unsigned) = number.as_u64() {
                Ok(TomlValue::Integer(unsigned as i64))
            } else if let Some(float) = number.as_f64() {
                Ok(TomlValue::Float(float))
            } else {
                Err(SandboxError::InvalidRequest(
                    "yq could not render number as TOML".to_string(),
                ))
            }
        }
        Value::String(text) => Ok(TomlValue::String(text.clone())),
        Value::Array(items) => Ok(TomlValue::Array(
            items
                .iter()
                .map(json_to_toml)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(object) => {
            let mut table = toml::map::Map::new();
            for (key, value) in object {
                table.insert(key.clone(), json_to_toml(value)?);
            }
            Ok(TomlValue::Table(table))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_formats_and_flags() {
        let spec = parse_invocation(&[
            "-p".to_string(),
            "toml".to_string(),
            "-o".to_string(),
            "csv".to_string(),
            "-rce".to_string(),
            ".name".to_string(),
        ])
        .unwrap();

        assert_eq!(spec.input_format, Format::Toml);
        assert_eq!(spec.output_format, Format::Csv);
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
    fn transcodes_toml_and_csv_inputs() {
        let mut toml_output = Vec::new();
        append_source_values(
            &mut toml_output,
            b"[package]\nname = \"abash\"\nversion = \"0.1.0\"\n",
            Format::Toml,
        )
        .unwrap();
        assert!(String::from_utf8(toml_output)
            .unwrap()
            .contains("\"package\""));

        let mut csv_output = Vec::new();
        append_source_values(&mut csv_output, b"name,age\nbert,34\n", Format::Csv).unwrap();
        assert_eq!(
            String::from_utf8(csv_output).unwrap(),
            "[{\"age\":34,\"name\":\"bert\"}]\n"
        );
    }

    #[test]
    fn renders_yaml_scalars_without_json_quotes() {
        assert_eq!(
            render_yaml_value(&Value::String("bert".to_string())).unwrap(),
            "bert\n"
        );
    }

    #[test]
    fn renders_csv_from_arrays_of_objects() {
        let stdout = br#"[{"name":"bert","age":34},{"name":"ana","age":29}]"#.to_vec();
        assert_eq!(
            String::from_utf8(render_csv_output(stdout).unwrap()).unwrap(),
            "age,name\n34,bert\n29,ana\n"
        );
    }
}
