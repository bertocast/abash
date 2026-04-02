use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) struct DuResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute<F, G, H>(
    cwd: &str,
    args: &[String],
    mut exists: F,
    mut read_file: G,
    mut is_dir: H,
    candidates: &[String],
) -> Result<DuResult, SandboxError>
where
    F: FnMut(&str) -> Result<bool, SandboxError>,
    G: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    H: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    let mut stdout_lines = Vec::new();
    let mut stderr_lines = Vec::new();
    let mut grand_total = 0usize;

    for target in &spec.targets {
        if !exists(&target.resolved)? {
            stderr_lines.push(format!(
                "du: cannot access '{}': No such file or directory",
                target.original
            ));
            continue;
        }

        let result = accumulate_target(
            &target.resolved,
            &display_target(target),
            &spec,
            0,
            &mut read_file,
            &mut is_dir,
            candidates,
        )?;
        stdout_lines.extend(result.lines);
        grand_total += result.total_size;
    }

    if spec.grand_total {
        stdout_lines.push(format!(
            "{}\ttotal",
            render_size(grand_total, spec.human_readable)
        ));
    }

    let had_errors = !stderr_lines.is_empty();
    Ok(DuResult {
        stdout: render_lines(stdout_lines),
        stderr: render_lines(stderr_lines),
        exit_code: if had_errors { 1 } else { 0 },
    })
}

#[derive(Clone)]
struct Target {
    original: String,
    resolved: String,
}

struct DuSpec {
    all_files: bool,
    human_readable: bool,
    summarize: bool,
    grand_total: bool,
    max_depth: Option<usize>,
    targets: Vec<Target>,
}

struct DuAccumulation {
    lines: Vec<String>,
    total_size: usize,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<DuSpec, SandboxError> {
    let mut all_files = false;
    let mut human_readable = false;
    let mut summarize = false;
    let mut grand_total = false;
    let mut max_depth = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-a" => {
                all_files = true;
                index += 1;
            }
            "-h" => {
                human_readable = true;
                index += 1;
            }
            "-s" => {
                summarize = true;
                index += 1;
            }
            "-c" => {
                grand_total = true;
                index += 1;
            }
            "--help" | "-help" => {
                return Err(SandboxError::InvalidRequest(
                    "du help is not implemented; supported flags: -a -h -s -c --max-depth=N"
                        .to_string(),
                ))
            }
            _ if arg.starts_with("--max-depth=") => {
                let value = arg.trim_start_matches("--max-depth=");
                max_depth = Some(value.parse::<usize>().map_err(|_| {
                    SandboxError::InvalidRequest(
                        "du --max-depth requires a non-negative integer".to_string(),
                    )
                })?);
                index += 1;
            }
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "du flag is not supported: {arg}"
                )));
            }
            _ => break,
        }
    }

    let mut targets = args[index..]
        .iter()
        .map(|arg| {
            Ok(Target {
                original: arg.clone(),
                resolved: resolve_sandbox_path(cwd, arg)?,
            })
        })
        .collect::<Result<Vec<_>, SandboxError>>()?;

    if targets.is_empty() {
        targets.push(Target {
            original: ".".to_string(),
            resolved: resolve_sandbox_path(cwd, ".")?,
        });
    }

    Ok(DuSpec {
        all_files,
        human_readable,
        summarize,
        grand_total,
        max_depth,
        targets,
    })
}

fn accumulate_target<G, H>(
    resolved: &str,
    display: &str,
    spec: &DuSpec,
    depth: usize,
    read_file: &mut G,
    is_dir: &mut H,
    candidates: &[String],
) -> Result<DuAccumulation, SandboxError>
where
    G: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
    H: FnMut(&str) -> Result<bool, SandboxError>,
{
    if !is_dir(resolved)? {
        let size = read_file(resolved)?.len();
        let lines = if spec.all_files || depth == 0 {
            vec![format!(
                "{}\t{display}",
                render_size(size, spec.human_readable)
            )]
        } else {
            Vec::new()
        };
        return Ok(DuAccumulation {
            lines,
            total_size: size,
        });
    }

    let mut lines = Vec::new();
    let mut total_size = 0usize;
    let mut children = direct_children(resolved, candidates);
    children.sort();

    for child in children {
        let child_display = child_display_path(display, resolved, &child);
        let child_result = accumulate_target(
            &child,
            &child_display,
            spec,
            depth + 1,
            read_file,
            is_dir,
            candidates,
        )?;
        total_size += child_result.total_size;
        if !spec.summarize && (spec.max_depth.is_none() || depth + 1 <= spec.max_depth.unwrap()) {
            lines.extend(child_result.lines);
        }
    }

    if spec.summarize || spec.max_depth.is_none() || depth <= spec.max_depth.unwrap() {
        lines.push(format!(
            "{}\t{display}",
            render_size(total_size, spec.human_readable)
        ));
    }

    Ok(DuAccumulation { lines, total_size })
}

fn direct_children(root: &str, candidates: &[String]) -> Vec<String> {
    candidates
        .iter()
        .filter(|candidate| candidate.as_str() != root && candidate_within_root(candidate, root))
        .filter(|candidate| depth_from_root(candidate, root) == 1)
        .cloned()
        .collect()
}

fn candidate_within_root(candidate: &str, root: &str) -> bool {
    candidate == root
        || candidate
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn depth_from_root(candidate: &str, root: &str) -> usize {
    if candidate == root {
        return 0;
    }
    candidate
        .trim_start_matches(root)
        .trim_start_matches('/')
        .split('/')
        .count()
}

fn child_display_path(display_root: &str, resolved_root: &str, child: &str) -> String {
    let suffix = child
        .strip_prefix(resolved_root)
        .unwrap_or(child)
        .trim_start_matches('/');
    if display_root == "." {
        suffix.to_string()
    } else if display_root == "/" {
        format!("/{suffix}")
    } else {
        format!("{display_root}/{suffix}")
    }
}

fn display_target(target: &Target) -> String {
    if target.original == "." {
        ".".to_string()
    } else {
        target.original.clone()
    }
}

fn render_size(size: usize, human_readable: bool) -> String {
    if !human_readable {
        return size.to_string();
    }

    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut value = size as f64;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{size}B")
    } else if value >= 10.0 || value.fract() == 0.0 {
        format!("{value:.0}{}", UNITS[unit_index])
    } else {
        format!("{value:.1}{}", UNITS[unit_index])
    }
}

fn render_lines(lines: Vec<String>) -> Vec<u8> {
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
    fn renders_human_sizes() {
        assert_eq!(render_size(12, true), "12B");
        assert_eq!(render_size(1024, true), "1K");
        assert_eq!(render_size(1536, true), "1.5K");
    }

    #[test]
    fn finds_direct_children_only() {
        let children = direct_children(
            "/workspace",
            &[
                "/".to_string(),
                "/workspace".to_string(),
                "/workspace/a".to_string(),
                "/workspace/a/b".to_string(),
                "/workspace/c".to_string(),
            ],
        );
        assert_eq!(
            children,
            vec!["/workspace/a".to_string(), "/workspace/c".to_string()]
        );
    }
}
