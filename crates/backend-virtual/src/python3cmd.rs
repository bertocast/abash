use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use abash_core::{ExecutionResult, SandboxError, SandboxFilesystem, TerminationReason};

use crate::hostexec::{bootstrap_dir, HostBridge};

pub(crate) struct Spec {
    pub(crate) code: Option<String>,
    pub(crate) module: Option<String>,
    pub(crate) script_file: Option<String>,
    pub(crate) show_version: bool,
    pub(crate) script_args: Vec<String>,
}

pub(crate) fn parse(args: &[String]) -> Result<Spec, ExecutionResult> {
    let mut spec = Spec {
        code: None,
        module: None,
        script_file: None,
        show_version: false,
        script_args: Vec::new(),
    };

    if args.is_empty() {
        return Ok(spec);
    }

    let index = 0usize;
    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-c" => {
                let Some(code) = args.get(index + 1) else {
                    return Err(cli_error(
                        "python3: option requires an argument -- 'c'\n",
                        2,
                    ));
                };
                spec.code = Some(code.clone());
                spec.script_args = args[index + 2..].to_vec();
                return Ok(spec);
            }
            "-m" => {
                let Some(module) = args.get(index + 1) else {
                    return Err(cli_error(
                        "python3: option requires an argument -- 'm'\n",
                        2,
                    ));
                };
                spec.module = Some(module.clone());
                spec.script_args = args[index + 2..].to_vec();
                return Ok(spec);
            }
            "--version" | "-V" => {
                spec.show_version = true;
                return Ok(spec);
            }
            value if value.starts_with('-') && value != "-" => {
                return Err(cli_error(
                    format!("python3: unrecognized option '{value}'\n"),
                    2,
                ));
            }
            "--" => {
                if let Some(script) = args.get(index + 1) {
                    spec.script_file = Some(script.clone());
                    spec.script_args = args[index + 2..].to_vec();
                }
                return Ok(spec);
            }
            value => {
                spec.script_file = Some(value.to_string());
                spec.script_args = args[index + 1..].to_vec();
                return Ok(spec);
            }
        }
    }

    Ok(spec)
}

pub(crate) fn execute(
    filesystem: &mut dyn SandboxFilesystem,
    cwd: &str,
    env: &BTreeMap<String, String>,
    stdin: &[u8],
    args: &[String],
    timeout_ms: Option<u64>,
    cancel_flag: &AtomicBool,
    metadata: BTreeMap<String, String>,
) -> Result<ExecutionResult, SandboxError> {
    let spec = match parse(args) {
        Ok(spec) => spec,
        Err(result) => return Ok(with_metadata(result, metadata)),
    };

    if spec.show_version {
        return run_host_python(
            filesystem,
            cwd,
            env,
            stdin,
            spec,
            timeout_ms,
            cancel_flag,
            metadata,
        );
    }

    if spec.code.is_none()
        && spec.module.is_none()
        && spec.script_file.is_none()
        && stdin.is_empty()
    {
        return Ok(with_metadata(
            cli_error(
                "python3: no input provided (use -c CODE, -m MODULE, or provide a script file)\n",
                2,
            ),
            metadata,
        ));
    }

    run_host_python(
        filesystem,
        cwd,
        env,
        stdin,
        spec,
        timeout_ms,
        cancel_flag,
        metadata,
    )
}

fn run_host_python(
    filesystem: &mut dyn SandboxFilesystem,
    cwd: &str,
    env: &BTreeMap<String, String>,
    stdin: &[u8],
    spec: Spec,
    timeout_ms: Option<u64>,
    cancel_flag: &AtomicBool,
    metadata: BTreeMap<String, String>,
) -> Result<ExecutionResult, SandboxError> {
    let bridge = HostBridge::new(filesystem)?;
    let bootstrap = bootstrap_dir(&bridge.root, cwd)?;
    let host_cwd = bridge.map_sandbox_path(cwd);

    let mut command = Command::new("python3");
    command.current_dir(&host_cwd);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.env("ABASH_SANDBOX_ROOT", &bridge.root);
    command.env("ABASH_SANDBOX_CWD", cwd);
    command.env("PYTHONUNBUFFERED", "1");
    let pythonpath = match std::env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{}:{}", bootstrap.display(), existing),
        _ => bootstrap.display().to_string(),
    };
    command.env("PYTHONPATH", pythonpath);

    for (key, value) in env {
        command.env(key, value);
    }

    let mut stdin_bytes = Vec::new();
    if spec.show_version {
        command.arg("--version");
    } else if let Some(code) = spec.code {
        command.arg("-c").arg(code);
    } else if let Some(module) = spec.module {
        command.arg("-m").arg(module);
    } else if let Some(script_file) = spec.script_file {
        if script_file == "-" {
            command.arg("-");
            stdin_bytes.extend_from_slice(stdin);
        } else {
            let mapped = bridge.map_sandbox_path(&script_file);
            if !mapped.exists() {
                bridge.cleanup();
                return Ok(with_metadata(
                    cli_error(
                        format!(
                            "python3: can't open file '{script_file}': [Errno 2] No such file or directory\n"
                        ),
                        2,
                    ),
                    metadata,
                ));
            }
            command.arg(mapped);
        }
    } else {
        command.arg("-");
        stdin_bytes.extend_from_slice(stdin);
    }

    for arg in spec.script_args {
        command.arg(arg);
    }

    let mut child = command.spawn().map_err(|error| {
        SandboxError::BackendFailure(format!("python3 could not start: {error}"))
    })?;

    if !stdin_bytes.is_empty() {
        if let Some(mut child_stdin) = child.stdin.take() {
            use std::io::Write;
            child_stdin.write_all(&stdin_bytes).map_err(|error| {
                SandboxError::BackendFailure(format!("python3 stdin write failed: {error}"))
            })?;
        }
    }

    let started = Instant::now();
    loop {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = child.kill();
            bridge.cleanup();
            return Err(SandboxError::Cancellation(
                "python3 execution was cancelled".to_string(),
            ));
        }
        if timeout_ms.is_some_and(|limit| started.elapsed() > Duration::from_millis(limit)) {
            let _ = child.kill();
            bridge.cleanup();
            return Err(SandboxError::Timeout(
                "python3 execution timed out".to_string(),
            ));
        }
        if child
            .try_wait()
            .map_err(|error| SandboxError::BackendFailure(format!("python3 wait failed: {error}")))?
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let output = child.wait_with_output().map_err(|error| {
        SandboxError::BackendFailure(format!("python3 output collection failed: {error}"))
    })?;
    bridge.sync_back(filesystem)?;
    bridge.cleanup();

    Ok(ExecutionResult {
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.status.code().unwrap_or(1),
        termination_reason: TerminationReason::Exited,
        error: None,
        metadata,
    })
}

fn cli_error(stderr: impl Into<Vec<u8>>, exit_code: i32) -> ExecutionResult {
    ExecutionResult {
        stdout: Vec::new(),
        stderr: stderr.into(),
        exit_code,
        termination_reason: TerminationReason::Exited,
        error: None,
        metadata: BTreeMap::new(),
    }
}

fn with_metadata(
    mut result: ExecutionResult,
    metadata: BTreeMap<String, String>,
) -> ExecutionResult {
    result.metadata = metadata;
    result
}
