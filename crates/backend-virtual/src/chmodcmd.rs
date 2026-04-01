use abash_core::{resolve_sandbox_path, SandboxError};

use crate::cp;

pub(crate) struct Spec {
    pub(crate) recursive: bool,
    pub(crate) verbose: bool,
    pub(crate) mode: ModeSpec,
    pub(crate) targets: Vec<String>,
}

pub(crate) enum ModeSpec {
    Numeric(u32),
    Symbolic(String),
}

pub(crate) fn parse(cwd: &str, args: &[String]) -> Result<Spec, SandboxError> {
    if args.len() < 2 {
        return Err(SandboxError::InvalidRequest(
            "chmod requires MODE and at least one target".to_string(),
        ));
    }

    let mut recursive = false;
    let mut verbose = false;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        if arg == "--" {
            index += 1;
            break;
        }
        if !arg.starts_with('-') || looks_like_mode(arg) {
            break;
        }
        match arg.as_str() {
            "-R" | "--recursive" => recursive = true,
            "-v" | "--verbose" => verbose = true,
            _ if arg.starts_with('-') && arg.chars().skip(1).all(|ch| matches!(ch, 'R' | 'v')) => {
                recursive |= arg.contains('R');
                verbose |= arg.contains('v');
            }
            _ => {
                return Err(SandboxError::InvalidRequest(format!(
                    "chmod flag is not supported: {arg}"
                )))
            }
        }
        index += 1;
    }

    if args.len().saturating_sub(index) < 2 {
        return Err(SandboxError::InvalidRequest(
            "chmod requires MODE and at least one target".to_string(),
        ));
    }

    let mode = parse_mode_spec(&args[index])?;
    let targets = args[index + 1..]
        .iter()
        .map(|path| resolve_sandbox_path(cwd, path))
        .collect::<Result<Vec<_>, SandboxError>>()?;

    Ok(Spec {
        recursive,
        verbose,
        mode,
        targets,
    })
}

pub(crate) fn resolve_mode(spec: &ModeSpec, current: u32) -> Result<u32, SandboxError> {
    match spec {
        ModeSpec::Numeric(value) => Ok(value & 0o7777),
        ModeSpec::Symbolic(symbolic) => parse_symbolic_mode(symbolic, current),
    }
}

pub(crate) fn descendant_targets(root: &str, candidates: &[String]) -> Vec<String> {
    cp::descendant_paths(root, candidates)
}

fn parse_mode_spec(value: &str) -> Result<ModeSpec, SandboxError> {
    if value.chars().all(|ch| matches!(ch, '0'..='7')) {
        let parsed = u32::from_str_radix(value, 8)
            .map_err(|_| SandboxError::InvalidRequest(format!("chmod invalid mode: {value}")))?;
        return Ok(ModeSpec::Numeric(parsed));
    }

    parse_symbolic_mode(value, 0o644)?;
    Ok(ModeSpec::Symbolic(value.to_string()))
}

fn parse_symbolic_mode(value: &str, current: u32) -> Result<u32, SandboxError> {
    let mut mode = current & 0o7777;

    for clause in value.split(',') {
        let mut chars = clause.chars().peekable();
        let mut who = String::new();
        while matches!(chars.peek(), Some('u' | 'g' | 'o' | 'a')) {
            who.push(chars.next().expect("peeked"));
        }
        if who.is_empty() {
            who.push('a');
        }

        let Some(op) = chars.next() else {
            return Err(SandboxError::InvalidRequest(format!(
                "chmod invalid mode: {value}"
            )));
        };
        if !matches!(op, '+' | '-' | '=') {
            return Err(SandboxError::InvalidRequest(format!(
                "chmod invalid mode: {value}"
            )));
        }

        let perms = chars.collect::<String>();
        if perms.is_empty()
            || !perms
                .chars()
                .all(|ch| matches!(ch, 'r' | 'w' | 'x' | 'X' | 's' | 't'))
        {
            return Err(SandboxError::InvalidRequest(format!(
                "chmod invalid mode: {value}"
            )));
        }

        let who = if who == "a" {
            "ugo".to_string()
        } else {
            who.replace('a', "ugo")
        };

        let mut perm_bits = 0u32;
        if perms.contains('r') {
            perm_bits |= 0o4;
        }
        if perms.contains('w') {
            perm_bits |= 0o2;
        }
        if perms.contains('x') || perms.contains('X') {
            perm_bits |= 0o1;
        }

        let mut special_bits = 0u32;
        if perms.contains('s') {
            if who.contains('u') {
                special_bits |= 0o4000;
            }
            if who.contains('g') {
                special_bits |= 0o2000;
            }
        }
        if perms.contains('t') {
            special_bits |= 0o1000;
        }

        for actor in who.chars() {
            let shift = match actor {
                'u' => 6,
                'g' => 3,
                'o' => 0,
                _ => continue,
            };
            let bits = perm_bits << shift;
            match op {
                '+' => mode |= bits,
                '-' => mode &= !bits,
                '=' => {
                    mode &= !(0o7 << shift);
                    mode |= bits;
                }
                _ => unreachable!(),
            }
        }

        match op {
            '+' => mode |= special_bits,
            '-' => mode &= !special_bits,
            '=' => {
                if perms.contains('s') {
                    if who.contains('u') {
                        mode &= !0o4000;
                        mode |= special_bits & 0o4000;
                    }
                    if who.contains('g') {
                        mode &= !0o2000;
                        mode |= special_bits & 0o2000;
                    }
                }
                if perms.contains('t') {
                    mode &= !0o1000;
                    mode |= special_bits & 0o1000;
                }
            }
            _ => unreachable!(),
        }
    }

    Ok(mode & 0o7777)
}

fn looks_like_mode(value: &str) -> bool {
    if value.chars().all(|ch| matches!(ch, '0'..='7')) {
        return true;
    }
    value.chars().all(|ch| {
        matches!(
            ch,
            'u' | 'g' | 'o' | 'a' | '+' | '-' | '=' | ',' | 'r' | 'w' | 'x' | 'X' | 's' | 't'
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_flags_and_numeric_mode() {
        let spec = parse(
            "/workspace",
            &["-Rv".to_string(), "755".to_string(), "demo.txt".to_string()],
        )
        .unwrap();

        assert!(spec.recursive);
        assert!(spec.verbose);
        assert!(matches!(spec.mode, ModeSpec::Numeric(0o755)));
        assert_eq!(spec.targets, vec!["/workspace/demo.txt".to_string()]);
    }

    #[test]
    fn applies_symbolic_modes() {
        let mode = resolve_mode(&ModeSpec::Symbolic("g-w,o+x".to_string()), 0o664).unwrap();
        assert_eq!(mode, 0o645);
    }
}
