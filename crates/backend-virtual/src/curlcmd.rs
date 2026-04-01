use abash_core::SandboxError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CurlSpec {
    pub method: String,
    pub url: String,
    pub body: Option<Vec<u8>>,
    pub output_path: Option<String>,
    pub follow_redirects: bool,
    pub include_headers: bool,
    pub head_only: bool,
}

pub(crate) fn parse_spec(args: &[String], stdin: Vec<u8>) -> Result<CurlSpec, SandboxError> {
    let mut method = None;
    let mut data = None;
    let mut output_path = None;
    let mut follow_redirects = false;
    let mut include_headers = false;
    let mut head_only = false;
    let mut url = None;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-X" | "--request" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "curl -X requires an HTTP method".to_string(),
                    ));
                };
                method = Some(value.clone());
                index += 2;
            }
            "-d" | "--data" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "curl -d requires a request body".to_string(),
                    ));
                };
                data = Some(if value == "@-" {
                    stdin.clone()
                } else {
                    value.as_bytes().to_vec()
                });
                index += 2;
            }
            "-o" | "--output" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "curl -o requires a file path".to_string(),
                    ));
                };
                output_path = Some(value.clone());
                index += 2;
            }
            "-L" | "--location" => {
                follow_redirects = true;
                index += 1;
            }
            "-i" | "--include" => {
                include_headers = true;
                index += 1;
            }
            "-I" | "--head" => {
                head_only = true;
                method = Some("HEAD".to_string());
                index += 1;
            }
            _ if arg.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "curl flag is not supported: {arg}"
                )));
            }
            _ => {
                if url.is_some() {
                    return Err(SandboxError::InvalidRequest(
                        "curl currently supports exactly one URL".to_string(),
                    ));
                }
                url = Some(arg.clone());
                index += 1;
            }
        }
    }

    let Some(mut url) = url else {
        return Err(SandboxError::InvalidRequest(
            "curl requires a URL".to_string(),
        ));
    };
    if !url.contains("://") {
        url = format!("https://{url}");
    }

    Ok(CurlSpec {
        method: method.unwrap_or_else(|| {
            if data.is_some() {
                "POST".to_string()
            } else {
                "GET".to_string()
            }
        }),
        url,
        body: data,
        output_path,
        follow_redirects,
        include_headers,
        head_only,
    })
}

pub(crate) fn render_response(
    spec: &CurlSpec,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> Vec<u8> {
    let mut output = Vec::new();
    if spec.include_headers || spec.head_only {
        output.extend_from_slice(format!("HTTP/1.1 {status}\r\n").as_bytes());
        for (name, value) in headers {
            output.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        output.extend_from_slice(b"\r\n");
    }
    if !spec.head_only {
        output.extend_from_slice(body);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_https_and_post_for_data() {
        let spec = parse_spec(
            &[
                "-d".to_string(),
                "hello".to_string(),
                "example.com".to_string(),
            ],
            Vec::new(),
        )
        .unwrap();

        assert_eq!(spec.method, "POST");
        assert_eq!(spec.url, "https://example.com");
        assert_eq!(spec.body, Some(b"hello".to_vec()));
    }
}
