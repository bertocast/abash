use abash_core::{resolve_sandbox_path, ErrorKind, SandboxError};
use regex::{Regex, RegexBuilder};

pub(crate) struct GrepResult {
    pub output: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute<F, G, H>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
    mut list_paths: G,
    mut is_dir: H,
) -> Result<GrepResult, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    G: FnMut() -> Result<Vec<String>, SandboxError>,
    H: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    if spec.paths.is_empty() {
        return search_stdin(stdin, &spec);
    }

    let mut search_files = Vec::new();
    let mut stderr = String::new();
    let mut had_error = false;
    let mut show_filenames = false;
    let candidates = if spec.recursive {
        let mut paths = list_paths()?;
        paths.sort();
        paths
    } else {
        Vec::new()
    };

    for root in &spec.paths {
        match is_dir(&root.resolved) {
            Ok(true) => {
                if !spec.recursive {
                    had_error = true;
                    stderr.push_str(&format!(
                        "grep: {}: Is a directory\n",
                        shell_label(&root.original)
                    ));
                    continue;
                }
                show_filenames = true;
                for candidate in &candidates {
                    if !candidate_within_root(candidate, &root.resolved)
                        || candidate == &root.resolved
                    {
                        continue;
                    }
                    if is_dir(candidate)? {
                        continue;
                    }
                    search_files.push(SearchFile {
                        display: display_path(cwd, root, candidate),
                        resolved: candidate.clone(),
                    });
                }
            }
            Ok(false) => search_files.push(SearchFile {
                display: display_path(cwd, root, &root.resolved),
                resolved: root.resolved.clone(),
            }),
            Err(error) if error.kind() == ErrorKind::InvalidRequest => {
                had_error = true;
                stderr.push_str(&format!(
                    "grep: {}: No such file or directory\n",
                    shell_label(&root.original)
                ));
            }
            Err(error) => return Err(error),
        }
    }

    if search_files.len() > 1 {
        show_filenames = true;
    }

    let mut stdout = Vec::new();
    let mut any_match = false;
    for file in &search_files {
        match read_file(&file.resolved) {
            Ok(contents) => {
                let result = search_text(&file.display, contents, &spec, show_filenames)?;
                any_match |= result.matched;
                if spec.files_with_matches {
                    if result.matched {
                        stdout.push(file.display.clone());
                    }
                } else if spec.count_only {
                    stdout.push(render_count(&file.display, result.count, show_filenames));
                } else {
                    stdout.extend(result.lines);
                }
            }
            Err(error) if error.kind() == ErrorKind::InvalidRequest => {
                had_error = true;
                stderr.push_str(&format!(
                    "grep: {}: No such file or directory\n",
                    shell_label(&file.display)
                ));
            }
            Err(error) => return Err(error),
        }
    }

    Ok(GrepResult {
        output: render_lines(stdout),
        stderr: stderr.into_bytes(),
        exit_code: if had_error {
            2
        } else if any_match {
            0
        } else {
            1
        },
    })
}

#[derive(Clone)]
struct SearchRoot {
    original: String,
    resolved: String,
}

#[derive(Clone)]
struct SearchFile {
    display: String,
    resolved: String,
}

enum MatchMode {
    Regex,
    Fixed,
}

struct GrepSpec {
    matcher: Matcher,
    line_numbers: bool,
    inverted: bool,
    count_only: bool,
    files_with_matches: bool,
    recursive: bool,
    paths: Vec<SearchRoot>,
}

enum Matcher {
    Regex(Regex),
    Fixed { needle: String, ignore_case: bool },
}

impl Matcher {
    fn is_match(&self, line: &str) -> bool {
        match self {
            Self::Regex(regex) => regex.is_match(line),
            Self::Fixed {
                needle,
                ignore_case,
            } => {
                if *ignore_case {
                    line.to_lowercase().contains(needle)
                } else {
                    line.contains(needle)
                }
            }
        }
    }
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<GrepSpec, SandboxError> {
    let mut line_numbers = false;
    let mut inverted = false;
    let mut ignore_case = false;
    let mut count_only = false;
    let mut files_with_matches = false;
    let mut recursive = false;
    let mut mode = MatchMode::Regex;
    let mut index = 0usize;
    while let Some(arg) = args.get(index) {
        if arg == "--" {
            index += 1;
            break;
        }
        if !arg.starts_with('-') || arg == "-" {
            break;
        }
        for flag in arg[1..].chars() {
            match flag {
                'n' => line_numbers = true,
                'v' => inverted = true,
                'i' => ignore_case = true,
                'c' => count_only = true,
                'l' => files_with_matches = true,
                'r' => recursive = true,
                'E' => mode = MatchMode::Regex,
                'F' => mode = MatchMode::Fixed,
                _ => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "grep flag is not supported: -{flag}"
                    )));
                }
            }
        }
        index += 1;
    }

    let Some(pattern) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "grep requires a pattern".to_string(),
        ));
    };

    let matcher = match mode {
        MatchMode::Regex => Matcher::Regex(
            RegexBuilder::new(pattern)
                .case_insensitive(ignore_case)
                .build()
                .map_err(|error| {
                    SandboxError::InvalidRequest(format!("grep regex is invalid: {error}"))
                })?,
        ),
        MatchMode::Fixed => Matcher::Fixed {
            needle: if ignore_case {
                pattern.to_lowercase()
            } else {
                pattern.clone()
            },
            ignore_case,
        },
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

    Ok(GrepSpec {
        matcher,
        line_numbers,
        inverted,
        count_only,
        files_with_matches,
        recursive,
        paths,
    })
}

fn search_stdin(stdin: Vec<u8>, spec: &GrepSpec) -> Result<GrepResult, SandboxError> {
    let result = search_text("(stdin)", stdin, spec, false)?;
    let output = if spec.files_with_matches {
        if result.matched {
            b"(stdin)\n".to_vec()
        } else {
            Vec::new()
        }
    } else if spec.count_only {
        render_lines(vec![result.count.to_string()])
    } else {
        render_lines(result.lines)
    };
    Ok(GrepResult {
        output,
        stderr: Vec::new(),
        exit_code: if result.matched { 0 } else { 1 },
    })
}

struct SearchResult {
    lines: Vec<String>,
    matched: bool,
    count: usize,
}

fn search_text(
    label: &str,
    input: Vec<u8>,
    spec: &GrepSpec,
    show_filenames: bool,
) -> Result<SearchResult, SandboxError> {
    let text = String::from_utf8(input).map_err(|_| {
        SandboxError::InvalidRequest("grep currently requires UTF-8 text input".to_string())
    })?;

    let mut lines = Vec::new();
    let mut count = 0usize;
    for (index, line) in text.lines().enumerate() {
        let matched = spec.matcher.is_match(line);
        if matched != spec.inverted {
            count += 1;
            if spec.count_only || spec.files_with_matches {
                continue;
            }
            lines.push(render_match_line(
                label,
                index + 1,
                line,
                spec.line_numbers,
                show_filenames,
            ));
        }
    }

    Ok(SearchResult {
        lines,
        matched: count > 0,
        count,
    })
}

fn render_match_line(
    label: &str,
    line_number: usize,
    line: &str,
    line_numbers: bool,
    show_filenames: bool,
) -> String {
    match (show_filenames, line_numbers) {
        (true, true) => format!("{label}:{line_number}:{line}"),
        (true, false) => format!("{label}:{line}"),
        (false, true) => format!("{line_number}:{line}"),
        (false, false) => line.to_string(),
    }
}

fn render_count(label: &str, count: usize, show_filenames: bool) -> String {
    if show_filenames {
        format!("{label}:{count}")
    } else {
        count.to_string()
    }
}

fn render_lines(lines: Vec<String>) -> Vec<u8> {
    if lines.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", lines.join("\n")).into_bytes()
    }
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

fn shell_label(path: &str) -> &str {
    if path.is_empty() {
        "."
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn searches_stdin_with_regex_and_numbers() {
        let result = execute(
            "/workspace",
            &["-n".to_string(), "be.".to_string()],
            b"ana\nbeta\n".to_vec(),
            |_| unreachable!(),
            || unreachable!(),
            |_| unreachable!(),
        )
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(String::from_utf8(result.output).unwrap(), "2:beta\n");
    }

    #[test]
    fn fixed_string_mode_treats_pattern_literally() {
        let result = execute(
            "/workspace",
            &["-F".to_string(), "a.*".to_string()],
            b"a.*\nalpha\n".to_vec(),
            |_| unreachable!(),
            || unreachable!(),
            |_| unreachable!(),
        )
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(String::from_utf8(result.output).unwrap(), "a.*\n");
    }
}
