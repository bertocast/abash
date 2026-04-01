use std::collections::{BTreeMap, BTreeSet};

use abash_core::SandboxError;

pub(crate) fn cd(
    current_cwd: &str,
    default_cwd: &str,
    args: &[String],
) -> Result<String, SandboxError> {
    match args {
        [] => Ok(default_cwd.to_string()),
        [path] => join_path(current_cwd, path),
        _ => Err(SandboxError::InvalidRequest(
            "cd accepts at most one path".to_string(),
        )),
    }
}

pub(crate) fn export(
    args: &[String],
    env: &mut BTreeMap<String, String>,
    exported_env: &mut BTreeMap<String, String>,
) -> Result<Vec<u8>, SandboxError> {
    if args.is_empty() {
        return Ok(render_env(exported_env));
    }

    for arg in args {
        if arg.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "export flag is not supported: {arg}"
            )));
        }

        if let Some((name, value)) = parse_assignment(arg)? {
            env.insert(name.clone(), value.clone());
            exported_env.insert(name, value);
            continue;
        }

        let value = env.get(arg).cloned().unwrap_or_default();
        env.insert(arg.clone(), value.clone());
        exported_env.insert(arg.clone(), value);
    }

    Ok(Vec::new())
}

pub(crate) fn alias(
    args: &[String],
    aliases: &mut BTreeMap<String, Vec<String>>,
) -> Result<Vec<u8>, SandboxError> {
    if args.is_empty() {
        return Ok(render_aliases(aliases, None));
    }

    let mut queried = Vec::new();
    for arg in args {
        if arg.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "alias flag is not supported: {arg}"
            )));
        }
        if let Some((name, value)) = arg.split_once('=') {
            validate_name("alias", name)?;
            aliases.insert(name.to_string(), split_alias_words(value));
        } else {
            queried.push(arg.clone());
        }
    }

    Ok(render_aliases(
        aliases,
        if queried.is_empty() {
            None
        } else {
            Some(&queried)
        },
    ))
}

pub(crate) fn unalias(
    args: &[String],
    aliases: &mut BTreeMap<String, Vec<String>>,
) -> Result<(), SandboxError> {
    if args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "unalias requires at least one alias name".to_string(),
        ));
    }
    if args.len() == 1 && args[0] == "-a" {
        aliases.clear();
        return Ok(());
    }
    for arg in args {
        if arg.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "unalias flag is not supported: {arg}"
            )));
        }
        aliases.remove(arg);
    }
    Ok(())
}

pub(crate) fn render_history(entries: &[String]) -> Vec<u8> {
    if entries.is_empty() {
        return Vec::new();
    }
    let lines = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| format!("{:>5}  {entry}", index + 1))
        .collect::<Vec<_>>();
    format!("{}\n", lines.join("\n")).into_bytes()
}

pub(crate) fn help(allowlisted_commands: &BTreeSet<String>) -> Vec<u8> {
    let mut commands = allowlisted_commands.iter().cloned().collect::<Vec<_>>();
    commands.sort();
    let mut lines = vec!["available commands:".to_string()];
    lines.extend(commands);
    format!("{}\n", lines.join("\n")).into_bytes()
}

pub(crate) fn clear(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    if !args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "clear does not accept arguments".to_string(),
        ));
    }
    Ok(b"\x1b[H\x1b[2J".to_vec())
}

pub(crate) fn whoami(
    args: &[String],
    env: &BTreeMap<String, String>,
) -> Result<Vec<u8>, SandboxError> {
    if !args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "whoami does not accept arguments".to_string(),
        ));
    }
    Ok(format!(
        "{}\n",
        env.get("USER")
            .cloned()
            .or_else(|| env.get("LOGNAME").cloned())
            .unwrap_or_else(|| "sandbox".to_string())
    )
    .into_bytes())
}

pub(crate) fn hostname(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    if !args.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "hostname does not accept arguments".to_string(),
        ));
    }
    Ok(b"abash\n".to_vec())
}

fn join_path(current_cwd: &str, path: &str) -> Result<String, SandboxError> {
    if path.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "cd path must not be empty".to_string(),
        ));
    }
    if path.starts_with('/') {
        return Ok(path.to_string());
    }
    if current_cwd == "/" {
        return Ok(format!("/{path}"));
    }
    Ok(format!("{current_cwd}/{path}"))
}

fn parse_assignment(arg: &str) -> Result<Option<(String, String)>, SandboxError> {
    let Some((name, value)) = arg.split_once('=') else {
        return Ok(None);
    };
    validate_name("export", name)?;
    Ok(Some((name.to_string(), value.to_string())))
}

fn validate_name(command: &str, name: &str) -> Result<(), SandboxError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(SandboxError::InvalidRequest(format!(
            "{command} name must not be empty"
        )));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(SandboxError::InvalidRequest(format!(
            "{command} name must match [A-Za-z_][A-Za-z0-9_]*"
        )));
    }
    Ok(())
}

fn render_env(env: &BTreeMap<String, String>) -> Vec<u8> {
    if env.is_empty() {
        return Vec::new();
    }
    let mut lines = env
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    lines.sort();
    format!("{}\n", lines.join("\n")).into_bytes()
}

fn render_aliases(aliases: &BTreeMap<String, Vec<String>>, names: Option<&[String]>) -> Vec<u8> {
    let selected = match names {
        None => aliases
            .iter()
            .map(|(name, value)| render_alias(name, value))
            .collect::<Vec<_>>(),
        Some(names) => names
            .iter()
            .filter_map(|name| aliases.get(name).map(|value| render_alias(name, value)))
            .collect::<Vec<_>>(),
    };
    if selected.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", selected.join("\n")).into_bytes()
    }
}

fn render_alias(name: &str, value: &[String]) -> String {
    format!("{name}='{}'", value.join(" "))
}

fn split_alias_words(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_updates_current_and_exported_env() {
        let mut env = BTreeMap::from([("USER".to_string(), "berto".to_string())]);
        let mut exported = BTreeMap::new();
        export(
            &["TEAM=core".to_string(), "USER".to_string()],
            &mut env,
            &mut exported,
        )
        .unwrap();

        assert_eq!(env["TEAM"], "core");
        assert_eq!(exported["TEAM"], "core");
        assert_eq!(exported["USER"], "berto");
    }

    #[test]
    fn alias_renders_sorted_values() {
        let mut aliases = BTreeMap::new();
        alias(
            &["ll=ls -l".to_string(), "g=grep -n".to_string()],
            &mut aliases,
        )
        .unwrap();

        let rendered = String::from_utf8(alias(&[], &mut aliases).unwrap()).unwrap();
        assert_eq!(rendered, "g='grep -n'\nll='ls -l'\n");
    }
}
