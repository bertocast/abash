use abash_core::{resolve_sandbox_path, SandboxError};
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256};

pub(crate) enum HashKind {
    Md5,
    Sha1,
    Sha256,
}

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    stdin: Vec<u8>,
    kind: HashKind,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let mut rendered = Vec::new();

    if args.is_empty() {
        rendered.push(format!("{}  -", digest_hex(&stdin, &kind)));
    } else {
        for path in args {
            let resolved = resolve_sandbox_path(cwd, path)?;
            let bytes = read_file(&resolved)?;
            rendered.push(format!("{}  {path}", digest_hex(&bytes, &kind)));
        }
    }

    Ok(format!("{}\n", rendered.join("\n")).into_bytes())
}

fn digest_hex(bytes: &[u8], kind: &HashKind) -> String {
    match kind {
        HashKind::Md5 => format!("{:x}", Md5::digest(bytes)),
        HashKind::Sha1 => format!("{:x}", Sha1::digest(bytes)),
        HashKind::Sha256 => format!("{:x}", Sha256::digest(bytes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_md5() {
        let output = execute(
            "/workspace",
            &[],
            b"abc".to_vec(),
            HashKind::Md5,
            |_| unreachable!(),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "900150983cd24fb0d6963f7d28e17f72  -\n"
        );
    }
}
