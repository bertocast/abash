use abash_core::{resolve_sandbox_path, SandboxError};

pub(crate) fn execute<F, G>(
    cwd: &str,
    args: &[String],
    mut list_paths: F,
    mut is_dir: G,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut() -> Result<Vec<String>, SandboxError>,
    G: FnMut(&str) -> Result<bool, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    let candidates = list_paths()?;
    let mut rendered = Vec::new();

    for target in &spec.targets {
        if is_dir(&target.resolved)? {
            rendered.extend(list_directory(
                target,
                &candidates,
                spec.show_all,
                spec.long_format,
                &mut is_dir,
            )?);
        } else if spec.show_all || !is_hidden(path_name(&target.resolved)) {
            rendered.push(render_entry(
                &display_file_target(target),
                false,
                spec.long_format,
            ));
        }
    }

    if rendered.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(format!("{}\n", rendered.join("\n")).into_bytes())
    }
}

#[derive(Clone)]
struct LsTarget {
    original: String,
    resolved: String,
}

struct LsSpec {
    show_all: bool,
    long_format: bool,
    targets: Vec<LsTarget>,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<LsSpec, SandboxError> {
    let mut show_all = false;
    let mut long_format = false;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-a" => {
                show_all = true;
                index += 1;
            }
            "-l" => {
                long_format = true;
                index += 1;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "ls flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let mut targets = args[index..]
        .iter()
        .map(|arg| {
            Ok(LsTarget {
                original: arg.clone(),
                resolved: resolve_sandbox_path(cwd, arg)?,
            })
        })
        .collect::<Result<Vec<_>, SandboxError>>()?;

    if targets.is_empty() {
        targets.push(LsTarget {
            original: ".".to_string(),
            resolved: resolve_sandbox_path(cwd, ".")?,
        });
    }

    Ok(LsSpec {
        show_all,
        long_format,
        targets,
    })
}

fn list_directory(
    target: &LsTarget,
    candidates: &[String],
    show_all: bool,
    long_format: bool,
    is_dir: &mut impl FnMut(&str) -> Result<bool, SandboxError>,
) -> Result<Vec<String>, SandboxError> {
    let mut entries = candidates
        .iter()
        .filter_map(|candidate| immediate_child_name(candidate, &target.resolved))
        .filter(|name| show_all || !is_hidden(name))
        .collect::<Vec<_>>();
    entries.sort();
    entries.dedup();

    entries
        .into_iter()
        .map(|name| -> Result<String, SandboxError> {
            let child_path = child_path(&target.resolved, &name);
            Ok(render_entry(&name, is_dir(&child_path)?, long_format))
        })
        .collect()
}

fn display_file_target(target: &LsTarget) -> String {
    if target.original.starts_with('/') {
        target.resolved.clone()
    } else {
        target.original.clone()
    }
}

fn render_entry(name: &str, is_dir: bool, long_format: bool) -> String {
    if long_format {
        format!("{} {name}", if is_dir { "d" } else { "-" })
    } else {
        name.to_string()
    }
}

fn immediate_child_name<'a>(candidate: &'a str, parent: &str) -> Option<String> {
    if candidate == parent {
        return None;
    }

    let suffix = if parent == "/" {
        candidate.strip_prefix('/')?
    } else {
        candidate.strip_prefix(&(parent.to_string() + "/"))?
    };
    if suffix.is_empty() || suffix.contains('/') {
        return None;
    }
    Some(suffix.to_string())
}

fn child_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else {
        format!("{parent}/{child}")
    }
}

fn path_name(path: &str) -> &str {
    if path == "/" {
        "/"
    } else {
        path.rsplit('/').next().unwrap_or(path)
    }
}

fn is_hidden(name: &str) -> bool {
    name.starts_with('.') && name != "." && name != ".."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_current_directory_target() {
        let spec = parse_spec("/workspace", &[]).unwrap();
        assert_eq!(spec.targets[0].original, ".");
        assert_eq!(spec.targets[0].resolved, "/workspace");
    }

    #[test]
    fn lists_immediate_children_only() {
        let output = execute(
            "/workspace",
            &["/workspace".to_string()],
            || {
                Ok(vec![
                    "/".to_string(),
                    "/workspace".to_string(),
                    "/workspace/docs".to_string(),
                    "/workspace/docs/readme.txt".to_string(),
                    "/workspace/demo.txt".to_string(),
                ])
            },
            |path| Ok(matches!(path, "/" | "/workspace" | "/workspace/docs")),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "demo.txt\ndocs\n");
    }

    #[test]
    fn long_format_marks_directories() {
        let output = execute(
            "/workspace",
            &["-l".to_string(), "/workspace".to_string()],
            || {
                Ok(vec![
                    "/".to_string(),
                    "/workspace".to_string(),
                    "/workspace/docs".to_string(),
                    "/workspace/demo.txt".to_string(),
                ])
            },
            |path| Ok(matches!(path, "/" | "/workspace" | "/workspace/docs")),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "- demo.txt\nd docs\n");
    }
}
