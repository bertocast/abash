use std::collections::BTreeSet;

pub(crate) struct WhichResult {
    pub output: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute(args: &[String], allowlisted: &BTreeSet<String>) -> WhichResult {
    let mut found = Vec::new();
    let mut missing = false;

    for name in args {
        if allowlisted.contains(name) {
            found.push(name.clone());
        } else {
            missing = true;
        }
    }

    WhichResult {
        output: if found.is_empty() {
            Vec::new()
        } else {
            format!("{}\n", found.join("\n")).into_bytes()
        },
        exit_code: if missing || found.is_empty() { 1 } else { 0 },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn returns_found_commands_and_nonzero_when_any_missing() {
        let allowlisted = ["echo".to_string(), "grep".to_string()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let result = execute(&["echo".to_string(), "missing".to_string()], &allowlisted);

        assert_eq!(result.exit_code, 1);
        assert_eq!(String::from_utf8(result.output).unwrap(), "echo\n");
    }
}
