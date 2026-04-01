use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct RgResult {
    pub output: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute<F, G, H>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
    mut list_paths: G,
    mut is_dir: H,
) -> Result<RgResult, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    G: FnMut() -> Result<Vec<String>, SandboxError>,
    H: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;

    if spec.paths.is_empty() && !stdin.is_empty() {
        return search_text("(stdin)", stdin, &spec);
    }

    let candidates = list_paths()?;
    let mut rendered = Vec::new();
    let mut matched_files = Vec::new();

    let roots = if spec.paths.is_empty() {
        vec![SearchRoot {
            original: ".".to_string(),
            resolved: resolve_sandbox_path(cwd, ".")?,
        }]
    } else {
        spec.paths.clone()
    };

    for root in &roots {
        if !is_dir(&root.resolved)? {
            let file_matches = search_file(
                &display_path(cwd, root, &root.resolved),
                read_file(&root.resolved)?,
                &spec,
            )?;
            if !file_matches.is_empty() {
                matched_files.push(display_path(cwd, root, &root.resolved));
                rendered.extend(file_matches);
            }
            continue;
        }

        for candidate in &candidates {
            if candidate == &root.resolved || !candidate_within_root(candidate, &root.resolved) {
                continue;
            }
            if is_dir(candidate)? {
                continue;
            }
            let display = display_path(cwd, root, candidate);
            let file_matches = search_file(&display, read_file(candidate)?, &spec)?;
            if !file_matches.is_empty() {
                matched_files.push(display);
                rendered.extend(file_matches);
            }
        }
    }

    matched_files.sort();
    matched_files.dedup();

    let output = if spec.files_with_matches {
        matched_files
    } else {
        rendered
    };

    Ok(RgResult {
        exit_code: if output.is_empty() { 1 } else { 0 },
        output: if output.is_empty() {
            Vec::new()
        } else {
            format!("{}\n", output.join("\n")).into_bytes()
        },
    })
}

#[derive(Clone)]
struct SearchRoot {
    original: String,
    resolved: String,
}

struct RgSpec {
    pattern: String,
    line_numbers: bool,
    files_with_matches: bool,
    ignore_case: bool,
    paths: Vec<SearchRoot>,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<RgSpec, SandboxError> {
    let mut line_numbers = false;
    let mut files_with_matches = false;
    let mut ignore_case = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-n" => {
                line_numbers = true;
                index += 1;
            }
            "-l" => {
                files_with_matches = true;
                index += 1;
            }
            "-i" => {
                ignore_case = true;
                index += 1;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "rg flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let Some(pattern) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "rg requires a literal pattern".to_string(),
        ));
    };

    let paths = args[index + 1..]
        .iter()
        .map(|path| {
            Ok(SearchRoot {
                original: path.clone(),
                resolved: resolve_sandbox_path(cwd, path)?,
            })
        })
        .collect::<Result<Vec<_>, SandboxError>>()?;

    Ok(RgSpec {
        pattern: pattern.clone(),
        line_numbers,
        files_with_matches,
        ignore_case,
        paths,
    })
}

fn search_text(label: &str, input: Vec<u8>, spec: &RgSpec) -> Result<RgResult, SandboxError> {
    let matches = search_file(label, input, spec)?;
    Ok(RgResult {
        exit_code: if matches.is_empty() { 1 } else { 0 },
        output: if matches.is_empty() {
            Vec::new()
        } else if spec.files_with_matches {
            format!("{label}\n").into_bytes()
        } else {
            format!("{}\n", matches.join("\n")).into_bytes()
        },
    })
}

fn search_file(label: &str, input: Vec<u8>, spec: &RgSpec) -> Result<Vec<String>, SandboxError> {
    let text = String::from_utf8(input).map_err(|_| {
        SandboxError::InvalidRequest("rg currently requires UTF-8 text input".to_string())
    })?;
    let pattern = if spec.ignore_case {
        spec.pattern.to_lowercase()
    } else {
        spec.pattern.clone()
    };
    let mut matches = Vec::new();

    for (index, line) in text.lines().enumerate() {
        let haystack = if spec.ignore_case {
            line.to_lowercase()
        } else {
            line.to_string()
        };
        if haystack.contains(&pattern) {
            if spec.files_with_matches {
                return Ok(vec![label.to_string()]);
            }
            if label == "(stdin)" {
                if spec.line_numbers {
                    matches.push(format!("{}:{line}", index + 1));
                } else {
                    matches.push(line.to_string());
                }
            } else if spec.line_numbers {
                matches.push(format!("{label}:{}:{line}", index + 1));
            } else {
                matches.push(format!("{label}:{line}"));
            }
        }
    }

    Ok(matches)
}

fn candidate_within_root(candidate: &str, root: &str) -> bool {
    candidate == root
        || candidate
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn display_path(cwd: &str, root: &SearchRoot, candidate: &str) -> String {
    if root.original.starts_with('/') {
        return candidate.to_string();
    }
    if candidate == root.resolved {
        return root.original.clone();
    }
    if cwd == "/" {
        return candidate.trim_start_matches('/').to_string();
    }
    candidate
        .strip_prefix(&(cwd.to_string() + "/"))
        .unwrap_or(candidate)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn searches_stdin_with_line_numbers() {
        let result = execute(
            "/workspace",
            &["-n".to_string(), "bert".to_string()],
            b"ana\nberto\n".to_vec(),
            |_| unreachable!(),
            || unreachable!(),
            |_| unreachable!(),
        )
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(String::from_utf8(result.output).unwrap(), "2:berto\n");
    }
}
