use std::collections::BTreeSet;

use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct XanResult {
    pub output: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<XanResult, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let command = parse_command(args)?;
    match command {
        XanCommand::Help => Ok(text_result(help_text())),
        XanCommand::Headers { just_names, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let output = if just_names {
                if table.headers.is_empty() {
                    Vec::new()
                } else {
                    format!("{}\n", table.headers.join("\n")).into_bytes()
                }
            } else if table.headers.is_empty() {
                Vec::new()
            } else {
                let lines = table
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(index, value)| format!("{index}: {value}"))
                    .collect::<Vec<_>>();
                format!("{}\n", lines.join("\n")).into_bytes()
            };
            Ok(XanResult {
                output,
                exit_code: 0,
            })
        }
        XanCommand::Count { input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            Ok(text_result(format!("{}\n", table.rows.len())))
        }
        XanCommand::Head { limit, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let rows = table.rows.into_iter().take(limit).collect::<Vec<_>>();
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
        XanCommand::Tail { limit, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let keep = table.rows.len().saturating_sub(limit);
            let rows = table.rows.into_iter().skip(keep).collect::<Vec<_>>();
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
        XanCommand::Slice {
            start,
            end,
            len,
            input,
        } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let start_index = start.unwrap_or(0).min(table.rows.len());
            let end_index = if let Some(length) = len {
                start_index.saturating_add(length).min(table.rows.len())
            } else {
                end.unwrap_or(table.rows.len()).min(table.rows.len())
            };
            let rows = table.rows[start_index..end_index].to_vec();
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
        XanCommand::Reverse { input } => {
            let mut table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            table.rows.reverse();
            Ok(text_result(format_csv(&table.headers, &table.rows)))
        }
        XanCommand::Select { spec, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let columns = resolve_column_spec(&spec, &table.headers)?;
            let headers = columns
                .iter()
                .map(|index| table.headers[*index].clone())
                .collect::<Vec<_>>();
            let rows = table
                .rows
                .iter()
                .map(|row| columns.iter().map(|index| row[*index].clone()).collect())
                .collect::<Vec<Vec<String>>>();
            Ok(text_result(format_csv(&headers, &rows)))
        }
        XanCommand::Drop { spec, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let removed = resolve_column_spec(&spec, &table.headers)?
                .into_iter()
                .collect::<BTreeSet<_>>();
            let kept = table
                .headers
                .iter()
                .enumerate()
                .filter_map(|(index, _)| (!removed.contains(&index)).then_some(index))
                .collect::<Vec<_>>();
            let headers = kept
                .iter()
                .map(|index| table.headers[*index].clone())
                .collect::<Vec<_>>();
            let rows = table
                .rows
                .iter()
                .map(|row| kept.iter().map(|index| row[*index].clone()).collect())
                .collect::<Vec<Vec<String>>>();
            Ok(text_result(format_csv(&headers, &rows)))
        }
        XanCommand::Rename {
            names,
            select,
            input,
        } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let replacements = names
                .split(',')
                .map(|value| value.trim().to_string())
                .collect::<Vec<_>>();
            if replacements.is_empty() || replacements.iter().all(String::is_empty) {
                return Err(SandboxError::InvalidRequest(
                    "xan rename requires at least one new column name".to_string(),
                ));
            }

            let mut headers = table.headers.clone();
            if let Some(spec) = select {
                let columns = resolve_column_spec(&spec, &table.headers)?;
                if columns.len() != replacements.len() {
                    return Err(SandboxError::InvalidRequest(
                        "xan rename selected columns and new names must have the same length"
                            .to_string(),
                    ));
                }
                for (index, replacement) in columns.into_iter().zip(replacements) {
                    headers[index] = replacement;
                }
            } else {
                for (index, replacement) in replacements.into_iter().enumerate() {
                    if index >= headers.len() {
                        break;
                    }
                    headers[index] = replacement;
                }
            }
            Ok(text_result(format_csv(&headers, &table.rows)))
        }
        XanCommand::Enum { column, input } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let mut headers = vec![column.clone()];
            headers.extend(table.headers.clone());
            let rows = table
                .rows
                .iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut next = vec![index.to_string()];
                    next.extend(row.clone());
                    next
                })
                .collect::<Vec<Vec<String>>>();
            Ok(text_result(format_csv(&headers, &rows)))
        }
        XanCommand::Search {
            pattern,
            select,
            invert,
            ignore_case,
            input,
        } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let columns = match select {
                Some(spec) => resolve_column_spec(&spec, &table.headers)?,
                None => (0..table.headers.len()).collect(),
            };
            let matcher = SearchMatcher::new(&pattern, ignore_case);
            let rows = table
                .rows
                .iter()
                .filter(|row| {
                    let matched = columns.iter().any(|index| matcher.matches(&row[*index]));
                    if invert {
                        !matched
                    } else {
                        matched
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
        XanCommand::Sort {
            select,
            numeric,
            reverse,
            input,
        } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let column = if let Some(spec) = select {
                let columns = resolve_column_spec(&spec, &table.headers)?;
                *columns.first().ok_or_else(|| {
                    SandboxError::InvalidRequest(
                        "xan sort requires at least one column".to_string(),
                    )
                })?
            } else {
                0
            };
            let mut rows = table.rows.clone();
            rows.sort_by(|left, right| {
                let ordering = if numeric {
                    parse_number(&left[column]).total_cmp(&parse_number(&right[column]))
                } else {
                    left[column].cmp(&right[column])
                };
                if reverse {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
        XanCommand::Filter {
            expression,
            invert,
            limit,
            input,
        } => {
            let table = read_table(cwd, input.as_deref(), stdin, &mut read_file)?;
            let expression = parse_filter_expression(&expression, &table.headers)?;
            let mut rows = Vec::new();
            for row in &table.rows {
                let matched = expression.matches(row);
                if invert ^ matched {
                    rows.push(row.clone());
                    if limit.is_some_and(|value| rows.len() >= value) {
                        break;
                    }
                }
            }
            Ok(text_result(format_csv(&table.headers, &rows)))
        }
    }
}

enum XanCommand {
    Help,
    Headers {
        just_names: bool,
        input: Option<String>,
    },
    Count {
        input: Option<String>,
    },
    Head {
        limit: usize,
        input: Option<String>,
    },
    Tail {
        limit: usize,
        input: Option<String>,
    },
    Slice {
        start: Option<usize>,
        end: Option<usize>,
        len: Option<usize>,
        input: Option<String>,
    },
    Reverse {
        input: Option<String>,
    },
    Select {
        spec: String,
        input: Option<String>,
    },
    Drop {
        spec: String,
        input: Option<String>,
    },
    Rename {
        names: String,
        select: Option<String>,
        input: Option<String>,
    },
    Enum {
        column: String,
        input: Option<String>,
    },
    Search {
        pattern: String,
        select: Option<String>,
        invert: bool,
        ignore_case: bool,
        input: Option<String>,
    },
    Sort {
        select: Option<String>,
        numeric: bool,
        reverse: bool,
        input: Option<String>,
    },
    Filter {
        expression: String,
        invert: bool,
        limit: Option<usize>,
        input: Option<String>,
    },
}

#[derive(Clone)]
struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn parse_command(args: &[String]) -> Result<XanCommand, SandboxError> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Ok(XanCommand::Help);
    };
    if matches!(subcommand, "--help" | "-h") {
        return Ok(XanCommand::Help);
    }

    match subcommand {
        "headers" => parse_headers(&args[1..]),
        "count" => parse_count(&args[1..]),
        "head" => parse_head(&args[1..]),
        "tail" => parse_tail(&args[1..]),
        "slice" => parse_slice(&args[1..]),
        "reverse" => parse_reverse(&args[1..]),
        "select" => parse_select(&args[1..]),
        "drop" => parse_drop(&args[1..]),
        "rename" => parse_rename(&args[1..]),
        "enum" => parse_enum(&args[1..]),
        "search" => parse_search(&args[1..]),
        "sort" => parse_sort(&args[1..]),
        "filter" => parse_filter(&args[1..]),
        value => Err(SandboxError::InvalidRequest(format!(
            "xan subcommand is not supported: {value}"
        ))),
    }
}

fn parse_headers(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut just_names = false;
    let mut input = None;
    for arg in args {
        match arg.as_str() {
            "-j" | "--just-names" => just_names = true,
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan headers flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => input = Some(arg.clone()),
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan headers accepts at most one input".to_string(),
                ))
            }
        }
    }
    Ok(XanCommand::Headers { just_names, input })
}

fn parse_count(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut input = None;
    for arg in args {
        match arg.as_str() {
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan count flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => input = Some(arg.clone()),
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan count accepts at most one input".to_string(),
                ))
            }
        }
    }
    Ok(XanCommand::Count { input })
}

fn parse_head(args: &[String]) -> Result<XanCommand, SandboxError> {
    parse_head_tail(args, true)
}

fn parse_tail(args: &[String]) -> Result<XanCommand, SandboxError> {
    parse_head_tail(args, false)
}

fn parse_head_tail(args: &[String], head: bool) -> Result<XanCommand, SandboxError> {
    let mut limit = 10usize;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-n" | "-l" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan head/tail missing value for limit".to_string(),
                    ));
                };
                limit = parse_usize_flag("xan head/tail limit", value)?;
                index += 2;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan {} flag is not supported: {arg}",
                    if head { "head" } else { "tail" }
                )))
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan {} accepts at most one input",
                    if head { "head" } else { "tail" }
                )))
            }
        }
    }

    Ok(if head {
        XanCommand::Head { limit, input }
    } else {
        XanCommand::Tail { limit, input }
    })
}

fn parse_slice(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut start = None;
    let mut end = None;
    let mut len = None;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-s" | "--start" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan slice missing value for -s".to_string(),
                    ));
                };
                start = Some(parse_usize_flag("xan slice start", value)?);
                index += 2;
            }
            "-e" | "--end" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan slice missing value for -e".to_string(),
                    ));
                };
                end = Some(parse_usize_flag("xan slice end", value)?);
                index += 2;
            }
            "-l" | "--len" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan slice missing value for -l".to_string(),
                    ));
                };
                len = Some(parse_usize_flag("xan slice len", value)?);
                index += 2;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan slice flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan slice accepts at most one input".to_string(),
                ))
            }
        }
    }

    Ok(XanCommand::Slice {
        start,
        end,
        len,
        input,
    })
}

fn parse_reverse(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut input = None;
    for arg in args {
        match arg.as_str() {
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan reverse flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => input = Some(arg.clone()),
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan reverse accepts at most one input".to_string(),
                ))
            }
        }
    }
    Ok(XanCommand::Reverse { input })
}

fn parse_select(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut spec = None;
    let mut input = None;
    for arg in args {
        match arg.as_str() {
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan select flag is not supported: {arg}"
                )))
            }
            _ if spec.is_none() => spec = Some(arg.clone()),
            _ if input.is_none() => input = Some(arg.clone()),
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan select accepts one column spec and one input".to_string(),
                ))
            }
        }
    }
    let spec = spec.ok_or_else(|| {
        SandboxError::InvalidRequest("xan select requires a column spec".to_string())
    })?;
    Ok(XanCommand::Select { spec, input })
}

fn parse_drop(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut spec = None;
    let mut input = None;
    for arg in args {
        match arg.as_str() {
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan drop flag is not supported: {arg}"
                )))
            }
            _ if spec.is_none() => spec = Some(arg.clone()),
            _ if input.is_none() => input = Some(arg.clone()),
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan drop accepts one column spec and one input".to_string(),
                ))
            }
        }
    }
    let spec = spec.ok_or_else(|| {
        SandboxError::InvalidRequest("xan drop requires a column spec".to_string())
    })?;
    Ok(XanCommand::Drop { spec, input })
}

fn parse_rename(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut names = None;
    let mut select = None;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-s" | "--select" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan rename missing value for -s".to_string(),
                    ));
                };
                select = Some(value.clone());
                index += 2;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan rename flag is not supported: {arg}"
                )))
            }
            _ if names.is_none() => {
                names = Some(arg.clone());
                index += 1;
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan rename accepts one name list and one input".to_string(),
                ))
            }
        }
    }

    let names = names
        .ok_or_else(|| SandboxError::InvalidRequest("xan rename requires new names".to_string()))?;
    Ok(XanCommand::Rename {
        names,
        select,
        input,
    })
}

fn parse_enum(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut column = "index".to_string();
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-c" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan enum missing value for -c".to_string(),
                    ));
                };
                column = value.clone();
                index += 2;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan enum flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan enum accepts at most one input".to_string(),
                ))
            }
        }
    }
    Ok(XanCommand::Enum { column, input })
}

fn parse_search(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut pattern = None;
    let mut select = None;
    let mut invert = false;
    let mut ignore_case = false;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-s" | "--select" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan search missing value for -s".to_string(),
                    ));
                };
                select = Some(value.clone());
                index += 2;
            }
            "-v" | "--invert" => {
                invert = true;
                index += 1;
            }
            "-i" | "--ignore-case" => {
                ignore_case = true;
                index += 1;
            }
            "-r" | "--regex" => {
                index += 1;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan search flag is not supported: {arg}"
                )))
            }
            _ if pattern.is_none() => {
                pattern = Some(arg.clone());
                index += 1;
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan search accepts one pattern and one input".to_string(),
                ))
            }
        }
    }

    let pattern = pattern
        .ok_or_else(|| SandboxError::InvalidRequest("xan search requires a pattern".to_string()))?;
    Ok(XanCommand::Search {
        pattern,
        select,
        invert,
        ignore_case,
        input,
    })
}

fn parse_sort(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut select = None;
    let mut numeric = false;
    let mut reverse = false;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-s" | "--select" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan sort missing value for -s".to_string(),
                    ));
                };
                select = Some(value.clone());
                index += 2;
            }
            "-N" | "--numeric" => {
                numeric = true;
                index += 1;
            }
            "-R" | "-r" | "--reverse" => {
                reverse = true;
                index += 1;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan sort flag is not supported: {arg}"
                )))
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan sort accepts at most one input".to_string(),
                ))
            }
        }
    }

    Ok(XanCommand::Sort {
        select,
        numeric,
        reverse,
        input,
    })
}

fn parse_filter(args: &[String]) -> Result<XanCommand, SandboxError> {
    let mut expression = None;
    let mut invert = false;
    let mut limit = None;
    let mut input = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-v" | "--invert" => {
                invert = true;
                index += 1;
            }
            "-l" | "--limit" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "xan filter missing value for -l".to_string(),
                    ));
                };
                limit = Some(value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest("xan filter limit must be a number".to_string())
                })?);
                index += 2;
            }
            "--help" | "-h" => return Ok(XanCommand::Help),
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "xan filter flag is not supported: {arg}"
                )))
            }
            _ if expression.is_none() => {
                expression = Some(arg.clone());
                index += 1;
            }
            _ if input.is_none() => {
                input = Some(arg.clone());
                index += 1;
            }
            _ => {
                return Err(SandboxError::InvalidRequest(
                    "xan filter accepts one expression and one input".to_string(),
                ))
            }
        }
    }

    let expression = expression.ok_or_else(|| {
        SandboxError::InvalidRequest("xan filter requires an expression".to_string())
    })?;
    Ok(XanCommand::Filter {
        expression,
        invert,
        limit,
        input,
    })
}

fn help_text() -> String {
    [
        "xan - narrow CSV toolkit",
        "",
        "Usage:",
        "  xan headers [-j] [FILE]",
        "  xan count [FILE]",
        "  xan head [-n N] [FILE]",
        "  xan tail [-n N] [FILE]",
        "  xan slice [-s START] [-e END] [-l LEN] [FILE]",
        "  xan reverse [FILE]",
        "  xan select COLS [FILE]",
        "  xan drop COLS [FILE]",
        "  xan rename NAMES [-s COLS] [FILE]",
        "  xan enum [-c NAME] [FILE]",
        "  xan search [-s COLS] [-v] [-i] [-r] PATTERN [FILE]",
        "  xan sort [-s COL] [-N] [-R] [FILE]",
        "  xan filter [-v] [-l N] EXPR [FILE]",
        "",
        "Current scope:",
        "  headers, count, head, tail, slice, reverse, select, drop, rename, enum, search, sort, filter",
    ]
    .join("\n")
}

fn parse_usize_flag(name: &str, value: &str) -> Result<usize, SandboxError> {
    value
        .parse::<usize>()
        .map_err(|_| SandboxError::InvalidRequest(format!("{name} must be a number")))
}

fn read_table<F>(
    cwd: &str,
    input: Option<&str>,
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Table, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let bytes = if let Some(path) = input {
        let resolved = resolve_sandbox_path(cwd, path)?;
        read_file(&resolved)?
    } else {
        stdin
    };
    let text = String::from_utf8(bytes).map_err(|_| {
        SandboxError::InvalidRequest("xan currently requires UTF-8 CSV input".to_string())
    })?;
    parse_csv(&text)
}

fn parse_csv(input: &str) -> Result<Table, SandboxError> {
    let records = parse_records(input)?;
    if records.is_empty() {
        return Ok(Table {
            headers: Vec::new(),
            rows: Vec::new(),
        });
    }

    let headers = records[0].clone();
    let width = headers.len();
    let rows = records
        .into_iter()
        .skip(1)
        .map(|mut row| {
            if row.len() < width {
                row.resize(width, String::new());
            } else if row.len() > width {
                row.truncate(width);
            }
            row
        })
        .collect::<Vec<_>>();

    Ok(Table { headers, rows })
}

fn parse_records(input: &str) -> Result<Vec<Vec<String>>, SandboxError> {
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
            ',' => {
                row.push(std::mem::take(&mut field));
            }
            '\n' => {
                row.push(std::mem::take(&mut field));
                if !(row.len() == 1 && row[0].is_empty() && records.is_empty()) {
                    records.push(std::mem::take(&mut row));
                } else {
                    row.clear();
                }
            }
            '\r' => {}
            _ => field.push(ch),
        }
    }

    if in_quotes {
        return Err(SandboxError::InvalidRequest(
            "xan CSV input has an unterminated quoted field".to_string(),
        ));
    }

    if saw_any && (!field.is_empty() || !row.is_empty()) {
        row.push(field);
        records.push(row);
    }

    Ok(records)
}

fn format_csv(headers: &[String], rows: &[Vec<String>]) -> Vec<u8> {
    if headers.is_empty() {
        return Vec::new();
    }

    let mut rendered = String::new();
    rendered.push_str(
        &headers
            .iter()
            .map(|value| quote_csv_field(value))
            .collect::<Vec<_>>()
            .join(","),
    );
    rendered.push('\n');

    for row in rows {
        rendered.push_str(
            &row.iter()
                .map(|value| quote_csv_field(value))
                .collect::<Vec<_>>()
                .join(","),
        );
        rendered.push('\n');
    }

    rendered.into_bytes()
}

fn quote_csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn resolve_column_spec(spec: &str, headers: &[String]) -> Result<Vec<usize>, SandboxError> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();

    for raw_token in spec.split(',') {
        let token = raw_token.trim();
        if token.is_empty() {
            continue;
        }

        if let Some(negated) = token.strip_prefix('!') {
            let removals = resolve_column_token(negated, headers)?;
            if selected.is_empty() {
                selected.extend(0..headers.len());
                seen.extend(0..headers.len());
            }
            selected.retain(|index| !removals.contains(index));
            seen = selected.iter().copied().collect();
            continue;
        }

        for index in resolve_column_token(token, headers)? {
            if seen.insert(index) {
                selected.push(index);
            }
        }
    }

    if selected.is_empty() {
        return Err(SandboxError::InvalidRequest(format!(
            "xan column spec did not match any columns: {spec}"
        )));
    }

    Ok(selected)
}

fn resolve_column_token(token: &str, headers: &[String]) -> Result<Vec<usize>, SandboxError> {
    if token == "*" {
        return Ok((0..headers.len()).collect());
    }

    if let Some((start, end)) = token.split_once(':') {
        return resolve_column_range(start, end, headers);
    }

    if contains_header_glob(token) {
        let matches = headers
            .iter()
            .enumerate()
            .filter_map(|(index, header)| header_glob_matches(token, header).then_some(index))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(SandboxError::InvalidRequest(format!(
                "xan column spec did not match any columns: {token}"
            )));
        }
        return Ok(matches);
    }

    resolve_column_ref(token, headers)
        .map(|index| vec![index])
        .ok_or_else(|| SandboxError::InvalidRequest(format!("xan column was not found: {token}")))
}

fn resolve_column_range(
    start: &str,
    end: &str,
    headers: &[String],
) -> Result<Vec<usize>, SandboxError> {
    let start_index = if start.is_empty() {
        Some(0)
    } else {
        resolve_column_ref(start, headers)
    };
    let end_index = if end.is_empty() {
        headers.len().checked_sub(1)
    } else {
        resolve_column_ref(end, headers)
    };

    let Some(start_index) = start_index else {
        return Err(SandboxError::InvalidRequest(format!(
            "xan range start was not found: {start}"
        )));
    };
    let Some(end_index) = end_index else {
        return Err(SandboxError::InvalidRequest(format!(
            "xan range end was not found: {end}"
        )));
    };

    let indices = if start_index <= end_index {
        (start_index..=end_index).collect()
    } else {
        (end_index..=start_index).rev().collect()
    };
    Ok(indices)
}

fn resolve_column_ref(token: &str, headers: &[String]) -> Option<usize> {
    token
        .parse::<usize>()
        .ok()
        .filter(|index| *index < headers.len())
        .or_else(|| headers.iter().position(|header| header == token))
}

fn contains_header_glob(value: &str) -> bool {
    value.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn header_glob_matches(pattern: &str, text: &str) -> bool {
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let text_chars = text.chars().collect::<Vec<_>>();
    let mut memo = vec![vec![None; text_chars.len() + 1]; pattern_chars.len() + 1];
    header_glob_matches_inner(&pattern_chars, &text_chars, 0, 0, &mut memo)
}

fn header_glob_matches_inner(
    pattern: &[char],
    text: &[char],
    pattern_index: usize,
    text_index: usize,
    memo: &mut [Vec<Option<bool>>],
) -> bool {
    if let Some(value) = memo[pattern_index][text_index] {
        return value;
    }

    let value = if pattern_index == pattern.len() {
        text_index == text.len()
    } else {
        match pattern[pattern_index] {
            '*' => {
                header_glob_matches_inner(pattern, text, pattern_index + 1, text_index, memo)
                    || (text_index < text.len()
                        && header_glob_matches_inner(
                            pattern,
                            text,
                            pattern_index,
                            text_index + 1,
                            memo,
                        ))
            }
            '?' => {
                text_index < text.len()
                    && header_glob_matches_inner(
                        pattern,
                        text,
                        pattern_index + 1,
                        text_index + 1,
                        memo,
                    )
            }
            other => {
                text_index < text.len()
                    && other == text[text_index]
                    && header_glob_matches_inner(
                        pattern,
                        text,
                        pattern_index + 1,
                        text_index + 1,
                        memo,
                    )
            }
        }
    };

    memo[pattern_index][text_index] = Some(value);
    value
}

struct SearchMatcher {
    pattern: String,
    starts_with: bool,
    ends_with: bool,
    ignore_case: bool,
}

impl SearchMatcher {
    fn new(pattern: &str, ignore_case: bool) -> Self {
        let starts_with = pattern.starts_with('^');
        let ends_with = pattern.ends_with('$') && pattern.len() > usize::from(starts_with);
        let trimmed = if let Some(value) = pattern.strip_prefix('^') {
            value
        } else {
            pattern
        };
        let trimmed = if let Some(value) = trimmed.strip_suffix('$') {
            value
        } else {
            trimmed
        };
        let pattern = if ignore_case {
            trimmed.to_lowercase()
        } else {
            trimmed.to_string()
        };
        Self {
            pattern,
            starts_with,
            ends_with,
            ignore_case,
        }
    }

    fn matches(&self, value: &str) -> bool {
        let haystack = if self.ignore_case {
            value.to_lowercase()
        } else {
            value.to_string()
        };
        match (self.starts_with, self.ends_with) {
            (true, true) => haystack == self.pattern,
            (true, false) => haystack.starts_with(&self.pattern),
            (false, true) => haystack.ends_with(&self.pattern),
            (false, false) => haystack.contains(&self.pattern),
        }
    }
}

struct FilterExpression {
    column: usize,
    operator: FilterOperator,
    value: String,
}

enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl FilterExpression {
    fn matches(&self, row: &[String]) -> bool {
        let left = &row[self.column];
        match self.operator {
            FilterOperator::Eq => left == &self.value,
            FilterOperator::Ne => left != &self.value,
            FilterOperator::Gt => parse_number(left) > parse_number(&self.value),
            FilterOperator::Ge => parse_number(left) >= parse_number(&self.value),
            FilterOperator::Lt => parse_number(left) < parse_number(&self.value),
            FilterOperator::Le => parse_number(left) <= parse_number(&self.value),
        }
    }
}

fn parse_filter_expression(
    source: &str,
    headers: &[String],
) -> Result<FilterExpression, SandboxError> {
    let operators = [
        (" eq ", FilterOperator::Eq),
        (" ne ", FilterOperator::Ne),
        (">=", FilterOperator::Ge),
        ("<=", FilterOperator::Le),
        ("==", FilterOperator::Eq),
        ("!=", FilterOperator::Ne),
        (">", FilterOperator::Gt),
        ("<", FilterOperator::Lt),
    ];

    for (needle, operator) in operators {
        if let Some(index) = source.find(needle) {
            let left = source[..index].trim();
            let right = source[index + needle.len()..].trim();
            let column = resolve_column_ref(left, headers).ok_or_else(|| {
                SandboxError::InvalidRequest(format!("xan filter column was not found: {left}"))
            })?;
            return Ok(FilterExpression {
                column,
                operator,
                value: unquote(right),
            });
        }
    }

    Err(SandboxError::InvalidRequest(format!(
        "xan filter expression is not supported: {source}"
    )))
}

fn unquote(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn parse_number(value: &str) -> f64 {
    value.trim().parse::<f64>().unwrap_or(f64::NAN)
}

fn text_result(output: impl Into<Vec<u8>>) -> XanResult {
    XanResult {
        output: output.into(),
        exit_code: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_with_quotes_and_newlines() {
        let table = parse_csv("name,note\nbert,\"hi,there\"\nana,\"a\nb\"\n").unwrap();
        assert_eq!(table.headers, vec!["name", "note"]);
        assert_eq!(table.rows[0], vec!["bert", "hi,there"]);
        assert_eq!(table.rows[1], vec!["ana", "a\nb"]);
    }

    #[test]
    fn column_spec_supports_globs_ranges_and_negation() {
        let headers = vec![
            "name".to_string(),
            "vec_1".to_string(),
            "vec_2".to_string(),
            "tail".to_string(),
        ];
        let indices = resolve_column_spec("name,vec_*,:tail,!vec_1", &headers).unwrap();
        assert_eq!(indices, vec![0, 2, 3]);
    }

    #[test]
    fn filter_expression_supports_numeric_and_string_ops() {
        let headers = vec!["name".to_string(), "age".to_string()];
        let numeric = parse_filter_expression("age >= 30", &headers).unwrap();
        let stringy = parse_filter_expression("name eq \"bert\"", &headers).unwrap();
        let row = vec!["bert".to_string(), "32".to_string()];
        assert!(numeric.matches(&row));
        assert!(stringy.matches(&row));
    }
}
