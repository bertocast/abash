use std::io::{Read, Write};

use abash_core::SandboxError;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

pub(crate) struct Spec {
    pub(crate) stdout: bool,
    pub(crate) decompress: bool,
    pub(crate) keep: bool,
    pub(crate) force: bool,
    pub(crate) suffix: String,
    pub(crate) paths: Vec<String>,
}

pub(crate) fn parse(args: &[String]) -> Result<Spec, SandboxError> {
    let mut stdout = false;
    let mut decompress = false;
    let mut keep = false;
    let mut force = false;
    let mut suffix = ".gz".to_string();
    let mut paths = Vec::new();
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-c" | "--stdout" | "--to-stdout" => stdout = true,
            "-d" | "--decompress" | "--uncompress" => decompress = true,
            "-k" | "--keep" => keep = true,
            "-f" | "--force" => force = true,
            "-S" | "--suffix" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "gzip -S requires a suffix".to_string(),
                    ));
                };
                suffix = value.clone();
                index += 2;
                continue;
            }
            "--help" => {
                return Err(SandboxError::UnsupportedFeature(
                    "gzip help is not yet wired".to_string(),
                ));
            }
            value if value.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "gzip flag is not supported: {value}"
                )));
            }
            _ => paths.push(arg.clone()),
        }
        index += 1;
    }

    Ok(Spec {
        stdout,
        decompress,
        keep,
        force,
        suffix,
        paths,
    })
}

pub(crate) fn compress_bytes(input: &[u8]) -> Result<Vec<u8>, SandboxError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(input)
        .map_err(io_error("gzip could not compress input"))?;
    encoder
        .finish()
        .map_err(io_error("gzip could not finalize compressed data"))
}

pub(crate) fn decompress_bytes(input: &[u8]) -> Result<Vec<u8>, SandboxError> {
    let mut decoder = GzDecoder::new(input);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|_| {
        SandboxError::BackendFailure("gzip input is not in gzip format".to_string())
    })?;
    Ok(output)
}

pub(crate) fn compressed_path(path: &str, suffix: &str) -> Result<String, SandboxError> {
    if path.ends_with(suffix) {
        return Err(SandboxError::InvalidRequest(format!(
            "gzip input already has {suffix} suffix"
        )));
    }
    Ok(format!("{path}{suffix}"))
}

pub(crate) fn decompressed_path(path: &str, suffix: &str) -> Result<String, SandboxError> {
    path.strip_suffix(suffix)
        .map(ToString::to_string)
        .ok_or_else(|| {
            SandboxError::InvalidRequest(format!("gzip unknown suffix for path: {path}"))
        })
}

fn io_error(prefix: &'static str) -> impl Fn(std::io::Error) -> SandboxError {
    move |error| SandboxError::BackendFailure(format!("{prefix}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_compressed_bytes() {
        let compressed = compress_bytes(b"hello").unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(decompressed, b"hello");
    }
}
