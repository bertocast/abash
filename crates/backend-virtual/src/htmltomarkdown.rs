use abash_core::{resolve_sandbox_path, SandboxError};
use turndown::{CodeBlockStyle, HeadingStyle, RuleFilter, Turndown, TurndownOptions};

pub(crate) struct HtmlToMarkdownResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

pub(crate) fn execute<F>(
    cwd: &str,
    args: &[String],
    stdin: &[u8],
    mut read_file: F,
) -> Result<HtmlToMarkdownResult, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_spec(cwd, args)?;
    if spec.show_help {
        return Ok(HtmlToMarkdownResult {
            stdout: help_text().into_bytes(),
            stderr: Vec::new(),
            exit_code: 0,
        });
    }

    let input = match &spec.input {
        Input::Stdin => String::from_utf8_lossy(stdin).into_owned(),
        Input::File { original, resolved } => match read_file(resolved) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => {
                return Ok(HtmlToMarkdownResult {
                    stdout: Vec::new(),
                    stderr: format!("html-to-markdown: {original}: No such file or directory\n")
                        .into_bytes(),
                    exit_code: 1,
                })
            }
        },
    };

    if input.trim().is_empty() {
        return Ok(HtmlToMarkdownResult {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: 0,
        });
    }

    let mut options = TurndownOptions::default();
    options.bullet_list_marker = spec.bullet.clone();
    options.code_block_style = CodeBlockStyle::Fenced;
    options.fence = spec.code_fence.clone();
    options.hr = spec.horizontal_rule.clone();
    options.heading_style = spec.heading_style.clone();

    let mut turndown = Turndown::with_options(options);
    turndown.remove(RuleFilter::String("footer".to_string()));

    let markdown = turndown.convert(&input).trim().to_string();
    let stdout = if markdown.is_empty() {
        Vec::new()
    } else {
        format!("{markdown}\n").into_bytes()
    };

    Ok(HtmlToMarkdownResult {
        stdout,
        stderr: Vec::new(),
        exit_code: 0,
    })
}

#[derive(Clone)]
enum Input {
    Stdin,
    File { original: String, resolved: String },
}

struct HtmlToMarkdownSpec {
    bullet: String,
    code_fence: String,
    horizontal_rule: String,
    heading_style: HeadingStyle,
    show_help: bool,
    input: Input,
}

fn parse_spec(cwd: &str, args: &[String]) -> Result<HtmlToMarkdownSpec, SandboxError> {
    let mut bullet = "-".to_string();
    let mut code_fence = "```".to_string();
    let mut horizontal_rule = "---".to_string();
    let mut heading_style = HeadingStyle::Atx;
    let mut show_help = false;
    let mut files = Vec::new();
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "--help" => {
                show_help = true;
                index += 1;
            }
            "-b" | "--bullet" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "html-to-markdown --bullet requires a marker".to_string(),
                    ));
                };
                bullet = parse_bullet(value)?;
                index += 2;
            }
            "-c" | "--code" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "html-to-markdown --code requires a fence".to_string(),
                    ));
                };
                code_fence = parse_code_fence(value)?;
                index += 2;
            }
            "-r" | "--hr" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "html-to-markdown --hr requires a string".to_string(),
                    ));
                };
                horizontal_rule = value.clone();
                index += 2;
            }
            _ if arg.starts_with("--bullet=") => {
                bullet = parse_bullet(arg.trim_start_matches("--bullet="))?;
                index += 1;
            }
            _ if arg.starts_with("--code=") => {
                code_fence = parse_code_fence(arg.trim_start_matches("--code="))?;
                index += 1;
            }
            _ if arg.starts_with("--hr=") => {
                horizontal_rule = arg.trim_start_matches("--hr=").to_string();
                index += 1;
            }
            _ if arg.starts_with("--heading-style=") => {
                heading_style = parse_heading_style(arg.trim_start_matches("--heading-style="))?;
                index += 1;
            }
            "-" => {
                files.push(arg.clone());
                index += 1;
            }
            _ if arg.starts_with("--") || (arg.starts_with('-') && arg != "-") => {
                return Err(SandboxError::InvalidRequest(format!(
                    "html-to-markdown flag is not supported: {arg}"
                )));
            }
            _ => {
                files.push(arg.clone());
                index += 1;
            }
        }
    }

    if files.len() > 1 {
        return Err(SandboxError::InvalidRequest(
            "html-to-markdown currently accepts at most one input file".to_string(),
        ));
    }

    let input = match files.pop() {
        None => Input::Stdin,
        Some(path) if path == "-" => Input::Stdin,
        Some(path) => Input::File {
            original: path.clone(),
            resolved: resolve_sandbox_path(cwd, &path)?,
        },
    };

    Ok(HtmlToMarkdownSpec {
        bullet,
        code_fence,
        horizontal_rule,
        heading_style,
        show_help,
        input,
    })
}

fn parse_bullet(value: &str) -> Result<String, SandboxError> {
    if matches!(value, "-" | "+" | "*") {
        Ok(value.to_string())
    } else {
        Err(SandboxError::InvalidRequest(
            "html-to-markdown bullet must be one of: -, +, *".to_string(),
        ))
    }
}

fn parse_code_fence(value: &str) -> Result<String, SandboxError> {
    if matches!(value, "```" | "~~~") {
        Ok(value.to_string())
    } else {
        Err(SandboxError::InvalidRequest(
            "html-to-markdown code fence must be ``` or ~~~".to_string(),
        ))
    }
}

fn parse_heading_style(value: &str) -> Result<HeadingStyle, SandboxError> {
    match value {
        "atx" => Ok(HeadingStyle::Atx),
        "setext" => Ok(HeadingStyle::Setext),
        _ => Err(SandboxError::InvalidRequest(
            "html-to-markdown heading style must be atx or setext".to_string(),
        )),
    }
}

fn help_text() -> String {
    [
        "html-to-markdown",
        "convert HTML to Markdown",
        "",
        "Usage:",
        "  html-to-markdown [OPTION]... [FILE]",
        "",
        "Options:",
        "  -b, --bullet=CHAR          bullet marker for unordered lists (-, +, or *)",
        "  -c, --code=FENCE          code fence for blocks (``` or ~~~)",
        "  -r, --hr=STRING           horizontal rule string (default: ---)",
        "      --heading-style=STYLE heading style: atx or setext",
        "      --help                display this help and exit",
        "",
        "Notes:",
        "  Reads from FILE or stdin.",
        "  Strips script, style, and footer elements.",
        "  Narrow parity command; not full turndown CLI coverage.",
        "",
        "Examples:",
        "  echo '<h1>Hello</h1>' | html-to-markdown",
        "  curl -s https://example.com | html-to-markdown",
        "  html-to-markdown page.html",
        "",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_long_flags_and_file_input() {
        let spec = parse_spec(
            "/workspace",
            &[
                "--bullet=+".to_string(),
                "--code=~~~".to_string(),
                "--hr=***".to_string(),
                "--heading-style=setext".to_string(),
                "page.html".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(spec.bullet, "+");
        assert_eq!(spec.code_fence, "~~~");
        assert_eq!(spec.horizontal_rule, "***");
        assert_eq!(spec.heading_style, HeadingStyle::Setext);
        match spec.input {
            Input::File { original, resolved } => {
                assert_eq!(original, "page.html");
                assert_eq!(resolved, "/workspace/page.html");
            }
            Input::Stdin => panic!("expected file input"),
        }
    }

    #[test]
    fn converts_and_strips_removed_elements() {
        let result = execute(
            "/workspace",
            &[],
            b"<h1>Title</h1><style>.x{}</style><p>Hello <strong>world</strong></p><script>alert(1)</script><footer>bye</footer>",
            |_| unreachable!(),
        )
        .unwrap();

        let stdout = String::from_utf8(result.stdout).unwrap();
        assert_eq!(stdout, "# Title\n\nHello **world**\n");
    }
}
